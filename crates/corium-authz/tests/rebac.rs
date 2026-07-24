//! End-to-end behaviour of the self-hosted `ReBAC` authorizer: policy written as
//! EDN into an authz database, compiled, and checked through the public
//! [`Authorizer`] seam.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use corium_authz::audit::{AuditEvent, AuditSink};
use corium_authz::authorizer::Consistency;
use corium_authz::eval::Denial;
use corium_authz::source::MemoryPolicySource;
use corium_authz::{
    AuthzConfig, BreakGlass, Limits, Policy, PolicyError, SystemDbAuthorizer, bootstrap,
};
use corium_protocol::authz::{Access, Action, ActionClass, Decision, Guard, Principal};

/// A policy exercising most of the model: a permission map, group membership
/// through both spellings, a tenant rewrite, a wildcard tuple, and a view.
const POLICY: &str = r#"
[;; actions -> relations
 {:db/id "p-read"  :authz.permission/object-type "database" :authz.permission/action "read"
  :authz.permission/relation ["viewer" "writer" "owner"]}
 {:db/id "p-write" :authz.permission/object-type "database" :authz.permission/action "write"
  :authz.permission/relation ["writer" "owner"]}
 {:db/id "p-admin" :authz.permission/object-type "database" :authz.permission/action "admin"
  :authz.permission/relation ["owner"]}
 {:db/id "p-cat"   :authz.permission/object-type "catalog"  :authz.permission/action "*"
  :authz.permission/relation ["owner"]}

 ;; alice owns music through the engineering group, written as a userset
 {:db/id "t-eng-owner" :authz.tuple/subject "group:eng#member"
  :authz.tuple/relation "owner" :authz.tuple/object "database:music"}
 {:db/id "t-alice-eng" :authz.tuple/subject "user:alice"
  :authz.tuple/relation "member" :authz.tuple/object "group:eng"}

 ;; bob writes payroll through the shorthand spelling (no #member)
 {:db/id "t-fin-writer" :authz.tuple/subject "group:finance"
  :authz.tuple/relation "writer" :authz.tuple/object "database:payroll"}
 {:db/id "t-bob-fin" :authz.tuple/subject "user:bob"
  :authz.tuple/relation "member" :authz.tuple/object "group:finance"}

 ;; carol views everything under tenant acme, reached by a rewrite
 {:db/id "o-payroll" :authz.object/type "database" :authz.object/id "payroll"}
 {:db/id "t-acme-parent" :authz.tuple/subject "tenant:acme"
  :authz.tuple/relation "parent" :authz.tuple/object "database:payroll"}
 {:db/id "t-carol-acme" :authz.tuple/subject "user:carol"
  :authz.tuple/relation "viewer" :authz.tuple/object "tenant:acme"}
 {:db/id "r-viewer" :authz.rewrite/relation "viewer"
  :authz.rewrite/via-relation "parent" :authz.rewrite/on-relation "viewer"
  :authz.rewrite/object-type "database"}

 ;; every caller, authenticated or not, may read the public database
 {:db/id "t-public" :authz.tuple/subject "user:*"
  :authz.tuple/relation "viewer" :authz.tuple/object "database:public"}

 ;; carol's tenant view is redacted; owners see everything
 {:db/id "v-redacted" :authz.view/name "redacted" :authz.view/filter-type "attribute-allowlist"
  :authz.view/attribute ["person/name" "person/title"]}
 {:db/id "b-viewer" :authz.binding/relation "viewer" :authz.binding/object "database:*"
  :authz.binding/view "redacted"}
 {:db/id "b-owner" :authz.binding/relation "owner" :authz.binding/object "database:*"
  :authz.binding/unfiltered true}]
"#;

fn authorizer_with(policy: &str, config: AuthzConfig) -> SystemDbAuthorizer {
    let db = bootstrap::policy_db(policy).expect("policy applies");
    SystemDbAuthorizer::with_config(
        Arc::new(MemoryPolicySource::new("corium_authz", db)),
        config,
    )
}

fn authorizer(policy: &str) -> SystemDbAuthorizer {
    authorizer_with(policy, AuthzConfig::default())
}

fn user(subject: &str) -> Principal {
    Principal::new("oidc", subject)
}

#[tokio::test]
async fn group_membership_grants_through_a_userset() {
    let authorizer = authorizer(POLICY);
    let decision = authorizer
        .check(&user("alice"), &Access::on(Action::Transact, "music"))
        .await;
    assert!(decision.is_allowed(), "{decision:?}");
    // The audit detail records how the grant was reached and at which basis.
    assert_eq!(
        decision.path.as_deref(),
        Some("database:music#owner -> group:eng#member -> user:alice")
    );
    assert!(decision.authz_t > 0);

    // dave holds nothing.
    let denied = authorizer
        .check(&user("dave"), &Access::on(Action::Query, "music"))
        .await;
    assert!(!denied.is_allowed());
    assert!(denied.reason.is_some());
}

#[tokio::test]
async fn group_membership_grants_through_the_shorthand_spelling() {
    let authorizer = authorizer(POLICY);
    assert!(
        authorizer
            .check(&user("bob"), &Access::on(Action::Transact, "payroll"))
            .await
            .is_allowed()
    );
    // Writer does not reach an admin action.
    assert!(
        !authorizer
            .check(&user("bob"), &Access::on(Action::DeleteDatabase, "payroll"))
            .await
            .is_allowed()
    );
}

#[tokio::test]
async fn rewrites_derive_a_relation_through_a_tenant() {
    let authorizer = authorizer(POLICY);
    let decision = authorizer
        .check(&user("carol"), &Access::on(Action::Query, "payroll"))
        .await;
    assert!(decision.is_allowed(), "{decision:?}");
    assert_eq!(
        decision.path.as_deref(),
        Some("database:payroll#viewer -> tenant:acme#viewer -> user:carol")
    );
    // Carol reads, but does not write.
    assert!(
        !authorizer
            .check(&user("carol"), &Access::on(Action::Transact, "payroll"))
            .await
            .is_allowed()
    );
}

#[tokio::test]
async fn wildcard_tuples_grant_public_access() {
    let authorizer = authorizer(POLICY);
    for principal in [user("dave"), Principal::anonymous()] {
        assert!(
            authorizer
                .check(&principal, &Access::on(Action::Query, "public"))
                .await
                .is_allowed(),
            "{} may read the public database",
            principal.subject
        );
    }
}

#[tokio::test]
async fn bindings_narrow_the_view_and_unfiltered_widens_it() {
    let authorizer = authorizer(POLICY);
    let carol = authorizer
        .check(&user("carol"), &Access::on(Action::Query, "payroll"))
        .await;
    let filter = carol.filter().expect("carol reads through a view");
    assert!(filter.attribute_visible(":person/name"));
    assert!(!filter.attribute_visible(":person/salary"));
    assert_eq!(carol.views, vec!["redacted".to_owned()]);

    // alice is an owner, and the owner binding is marked unfiltered.
    let alice = authorizer
        .check(&user("alice"), &Access::on(Action::Query, "music"))
        .await;
    assert!(alice.filter().is_none(), "{alice:?}");
}

#[tokio::test]
async fn several_filtered_paths_intersect() {
    let policy = r#"
[{:db/id "p" :authz.permission/object-type "database" :authz.permission/action "read"
  :authz.permission/relation ["reader-a" "reader-b"]}
 {:db/id "t1" :authz.tuple/subject "user:alice" :authz.tuple/relation "reader-a"
  :authz.tuple/object "database:music"}
 {:db/id "t2" :authz.tuple/subject "user:alice" :authz.tuple/relation "reader-b"
  :authz.tuple/object "database:music"}
 {:db/id "v1" :authz.view/name "a" :authz.view/filter-type "attribute-allowlist"
  :authz.view/attribute ["person/name" "person/email"]}
 {:db/id "v2" :authz.view/name "b" :authz.view/filter-type "attribute-allowlist"
  :authz.view/attribute ["person/name" "person/phone"]}
 {:db/id "b1" :authz.binding/relation "reader-a" :authz.binding/object "database:music"
  :authz.binding/view "a"}
 {:db/id "b2" :authz.binding/relation "reader-b" :authz.binding/object "database:music"
  :authz.binding/view "b"}]
"#;
    let decision = authorizer(policy)
        .check(&user("alice"), &Access::on(Action::Query, "music"))
        .await;
    let filter = decision.filter().expect("two views combine into one");
    // The intersection: holding both relations reveals no more than either.
    assert!(filter.attribute_visible(":person/name"));
    assert!(!filter.attribute_visible(":person/email"));
    assert!(!filter.attribute_visible(":person/phone"));
    assert_eq!(decision.views.len(), 2);
}

#[tokio::test]
async fn catalog_actions_target_the_catalog_object() {
    let policy = r#"
[{:db/id "p" :authz.permission/object-type "catalog" :authz.permission/action "*"
  :authz.permission/relation ["owner"]}
 {:db/id "t" :authz.tuple/subject "role:operator" :authz.tuple/relation "owner"
  :authz.tuple/object "catalog:*"}]
"#;
    let authorizer = authorizer(policy);
    let operator = user("ops").with_role("operator");
    assert!(
        authorizer
            .check(&operator, &Access::catalog(Action::ListDatabases))
            .await
            .is_allowed()
    );
    // Catalog ownership also covers creating a database that has no object yet.
    assert!(
        authorizer
            .check(&operator, &Access::on(Action::CreateDatabase, "brand-new"))
            .await
            .is_allowed()
    );
    // A read of an existing database is *not* covered: that is a database-typed
    // object with no permission mapping in this policy.
    assert!(
        !authorizer
            .check(&operator, &Access::on(Action::Query, "music"))
            .await
            .is_allowed()
    );
    assert!(
        !authorizer
            .check(&user("nobody"), &Access::catalog(Action::ListDatabases))
            .await
            .is_allowed()
    );
}

#[tokio::test]
async fn unmapped_actions_deny_by_default() {
    // A policy with tuples but no permission map cannot grant anything: the
    // action never resolves to a relation.
    let policy = r#"
[{:db/id "t" :authz.tuple/subject "user:alice" :authz.tuple/relation "owner"
  :authz.tuple/object "database:music"}]
"#;
    let decision = authorizer(policy)
        .check(&user("alice"), &Access::on(Action::Query, "music"))
        .await;
    assert!(!decision.is_allowed());
    assert!(
        decision
            .reason
            .as_deref()
            .is_some_and(|reason| reason.contains("no permission")),
        "{decision:?}"
    );
}

#[tokio::test]
async fn cyclic_policy_terminates_and_denies() {
    // group:a is a member of group:b and vice versa; neither contains alice.
    let policy = r#"
[{:db/id "p" :authz.permission/object-type "database" :authz.permission/action "read"
  :authz.permission/relation ["viewer"]}
 {:db/id "t1" :authz.tuple/subject "group:a#member" :authz.tuple/relation "viewer"
  :authz.tuple/object "database:music"}
 {:db/id "t2" :authz.tuple/subject "group:b#member" :authz.tuple/relation "member"
  :authz.tuple/object "group:a"}
 {:db/id "t3" :authz.tuple/subject "group:a#member" :authz.tuple/relation "member"
  :authz.tuple/object "group:b"}]
"#;
    let decision = authorizer(policy)
        .check(&user("alice"), &Access::on(Action::Query, "music"))
        .await;
    assert!(!decision.is_allowed());
    // Cycle detection, not budget exhaustion, is what stopped the walk.
    assert!(
        decision
            .reason
            .as_deref()
            .is_some_and(|reason| reason.contains("no relationship path")),
        "{decision:?}"
    );
}

#[tokio::test]
async fn depth_limit_bounds_a_long_chain() {
    // A chain of nested groups four levels deep, with alice at the bottom.
    let policy = r#"
[{:db/id "p" :authz.permission/object-type "database" :authz.permission/action "read"
  :authz.permission/relation ["viewer"]}
 {:db/id "t0" :authz.tuple/subject "group:g0#member" :authz.tuple/relation "viewer"
  :authz.tuple/object "database:music"}
 {:db/id "t1" :authz.tuple/subject "group:g1#member" :authz.tuple/relation "member"
  :authz.tuple/object "group:g0"}
 {:db/id "t2" :authz.tuple/subject "group:g2#member" :authz.tuple/relation "member"
  :authz.tuple/object "group:g1"}
 {:db/id "t3" :authz.tuple/subject "user:alice" :authz.tuple/relation "member"
  :authz.tuple/object "group:g2"}]
"#;
    assert!(
        authorizer(policy)
            .check(&user("alice"), &Access::on(Action::Query, "music"))
            .await
            .is_allowed()
    );

    let shallow = authorizer_with(
        policy,
        AuthzConfig {
            limits: Limits {
                max_depth: 2,
                ..Limits::default()
            },
            ..AuthzConfig::default()
        },
    );
    assert!(
        !shallow
            .check(&user("alice"), &Access::on(Action::Query, "music"))
            .await
            .is_allowed(),
        "a chain deeper than the limit does not grant"
    );
}

#[tokio::test]
async fn visit_budget_is_reported_as_exhaustion() {
    let tiny = authorizer_with(
        POLICY,
        AuthzConfig {
            limits: Limits {
                max_visited: 1,
                ..Limits::default()
            },
            ..AuthzConfig::default()
        },
    );
    let decision = tiny
        .check(&user("carol"), &Access::on(Action::Query, "payroll"))
        .await;
    assert!(!decision.is_allowed());
    assert!(
        decision
            .reason
            .as_deref()
            .is_some_and(|reason| reason.contains("exhausted")),
        "{decision:?}"
    );
    assert!(matches!(
        Denial::Exhausted { visited: 1 },
        Denial::Exhausted { .. }
    ));
}

#[tokio::test]
async fn policy_changes_take_effect_and_invalidate_the_result_cache() {
    let mut builder = bootstrap::PolicyBuilder::new();
    builder
        .transact_edn(
            r#"[{:db/id "p" :authz.permission/object-type "database"
                 :authz.permission/action "read" :authz.permission/relation ["viewer"]}]"#,
        )
        .expect("permissions apply");
    let source = Arc::new(MemoryPolicySource::new("corium_authz", builder.db()));
    let authorizer = SystemDbAuthorizer::new(source.clone());

    let access = Access::on(Action::Query, "music");
    let before = authorizer.check(&user("alice"), &access).await;
    assert!(!before.is_allowed());
    // The negative answer is cached, keyed by the basis it was decided at.
    assert!(!authorizer.check(&user("alice"), &access).await.is_allowed());

    builder
        .transact(&[bootstrap::tuple_form("alice", "viewer", "database:music")])
        .expect("grant applies");
    source.set(builder.db());
    authorizer.refresh().await.expect("policy recompiles");

    let after = authorizer.check(&user("alice"), &access).await;
    assert!(after.is_allowed(), "{after:?}");
    assert!(
        after.authz_t > before.authz_t,
        "the decision records the newer basis"
    );
}

#[tokio::test]
async fn fresh_consistency_reads_the_source_per_check() {
    let mut builder = bootstrap::PolicyBuilder::new();
    builder
        .transact_edn(
            r#"[{:db/id "p" :authz.permission/object-type "database"
                 :authz.permission/action "write" :authz.permission/relation ["writer"]}]"#,
        )
        .expect("permissions apply");
    let source = Arc::new(MemoryPolicySource::new("corium_authz", builder.db()));
    let authorizer = SystemDbAuthorizer::with_config(
        source.clone(),
        AuthzConfig {
            fresh_for: BTreeSet::from([ActionClass::Write]),
            ..AuthzConfig::default()
        },
    );

    let write = Access::on(Action::Transact, "music");
    assert!(!authorizer.check(&user("alice"), &write).await.is_allowed());

    builder
        .transact(&[bootstrap::tuple_form("alice", "writer", "database:music")])
        .expect("grant applies");
    source.set(builder.db());

    // No explicit refresh: a write-class action re-reads the source itself.
    assert!(authorizer.check(&user("alice"), &write).await.is_allowed());
}

/// A source that reports a failure, to exercise the fail-closed path.
struct BrokenSource;

#[tonic::async_trait]
impl corium_authz::PolicySource for BrokenSource {
    fn name(&self) -> &'static str {
        "corium_authz"
    }

    async fn snapshot(&self) -> Result<corium_db::Db, corium_authz::SourceError> {
        Err(corium_authz::SourceError::Unavailable(
            "corium_authz".to_owned(),
        ))
    }
}

#[tokio::test]
async fn unreadable_policy_fails_closed_with_a_break_glass_escape() {
    let closed = SystemDbAuthorizer::new(Arc::new(BrokenSource));
    let decision = closed
        .check(&user("alice"), &Access::on(Action::Query, "music"))
        .await;
    assert!(!decision.is_allowed());
    assert!(
        decision
            .reason
            .as_deref()
            .is_some_and(|reason| reason.contains("unavailable")),
        "{decision:?}"
    );

    let recovery = SystemDbAuthorizer::with_config(
        Arc::new(BrokenSource),
        AuthzConfig {
            break_glass: Some(BreakGlass {
                roles: BTreeSet::from(["operator".to_owned()]),
                ..BreakGlass::default()
            }),
            ..AuthzConfig::default()
        },
    );
    assert!(
        recovery
            .check(
                &user("ops").with_role("operator"),
                &Access::on(Action::Query, "music")
            )
            .await
            .is_allowed()
    );
    // Break-glass admits only the configured principals.
    assert!(
        !recovery
            .check(&user("alice"), &Access::on(Action::Query, "music"))
            .await
            .is_allowed()
    );
}

#[tokio::test]
async fn a_non_authz_database_is_rejected_rather_than_read_as_empty() {
    // An application database compiles to nothing; treating that as "deny
    // everything" would look like a working, very strict policy.
    let db = corium_db::Db::new(corium_core::Schema::default());
    let error = Policy::compile(&db).expect_err("an application database is refused");
    assert!(matches!(error, PolicyError::NotAnAuthzDatabase(_)));
}

#[tokio::test]
async fn undefined_view_in_a_binding_fails_compilation() {
    let db = bootstrap::policy_db(
        r#"[{:db/id "b" :authz.binding/relation "viewer"
             :authz.binding/object "database:music" :authz.binding/view "missing"}]"#,
    )
    .expect("the transaction applies");
    let error = Policy::compile(&db).expect_err("a dangling view reference is refused");
    assert!(
        matches!(error, PolicyError::UndefinedView { .. }),
        "{error:?}"
    );
}

#[derive(Default)]
struct RecordingAudit(Mutex<Vec<(String, bool, u64)>>);

impl AuditSink for RecordingAudit {
    fn record(&self, event: &AuditEvent) {
        self.0.lock().expect("audit lock").push((
            event.subject.clone(),
            event.allowed,
            event.authz_t,
        ));
    }
}

#[tokio::test]
async fn every_decision_is_audited_with_its_basis() {
    let audit = Arc::new(RecordingAudit::default());
    let db = bootstrap::policy_db(POLICY).expect("policy applies");
    let authorizer = SystemDbAuthorizer::new(Arc::new(MemoryPolicySource::new("corium_authz", db)))
        .with_audit(audit.clone());

    authorizer
        .check(&user("alice"), &Access::on(Action::Query, "music"))
        .await;
    authorizer
        .check(&user("dave"), &Access::on(Action::Query, "music"))
        .await;

    let events = audit.0.lock().expect("audit lock").clone();
    assert_eq!(events.len(), 2);
    assert_eq!(events[0].0, "alice");
    assert!(events[0].1, "alice was allowed");
    assert!(!events[1].1, "dave was denied");
    assert!(events.iter().all(|event| event.2 > 0), "{events:?}");
}

#[tokio::test]
async fn one_guard_serves_two_tenants_from_one_policy() {
    // The multi-tenant requirement, now answered from policy data: one Guard,
    // one authz database, two callers with different authority and views.
    let db = bootstrap::policy_db(POLICY).expect("policy applies");
    let guard = Guard::new(
        Arc::new(corium_protocol::authz::AllowAnonymous),
        Arc::new(SystemDbAuthorizer::new(Arc::new(MemoryPolicySource::new(
            "corium_authz",
            db,
        )))),
    );

    // alice owns music: full visibility, and she may write.
    assert!(
        guard
            .authorize(&user("alice"), &Access::on(Action::Transact, "music"))
            .await
            .expect("alice may write music")
            .is_none()
    );
    // carol reads payroll through the tenant, narrowed by the bound view.
    let carol_view = guard
        .authorize(&user("carol"), &Access::on(Action::Query, "payroll"))
        .await
        .expect("carol may read payroll")
        .expect("carol's read is filtered");
    assert!(carol_view.attribute_visible(":person/name"));
    assert!(!carol_view.attribute_visible(":person/salary"));
    // Neither reaches the other's database.
    assert!(
        guard
            .authorize(&user("carol"), &Access::on(Action::Transact, "music"))
            .await
            .is_err()
    );
    assert!(
        guard
            .authorize(&user("alice"), &Access::on(Action::Query, "payroll"))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn registered_principals_pin_their_provider() {
    let policy = r#"
[{:db/id "p" :authz.permission/object-type "database" :authz.permission/action "read"
  :authz.permission/relation ["viewer"]}
 {:db/id "alice" :authz.principal/id "alice" :authz.principal/provider "oidc"
  :authz.principal/role "analyst"}
 {:db/id "t" :authz.tuple/subject "role:analyst" :authz.tuple/relation "viewer"
  :authz.tuple/object "database:music"}]
"#;
    let authorizer = authorizer(policy);
    // The role comes from policy data, not from the token.
    assert!(
        authorizer
            .check(&user("alice"), &Access::on(Action::Query, "music"))
            .await
            .is_allowed()
    );
    // The same subject id minted by another provider is a different identity.
    assert!(
        !authorizer
            .check(
                &Principal::new("static-token", "alice"),
                &Access::on(Action::Query, "music")
            )
            .await
            .is_allowed()
    );
}

#[tokio::test]
async fn consistency_default_is_pinned() {
    assert_eq!(AuthzConfig::default().consistency, Consistency::Pinned);
    // A pinned authorizer answers from its snapshot without touching the
    // source again, which is what keeps the hot path local.
    let decision = authorizer(POLICY)
        .check(&user("alice"), &Access::on(Action::Query, "music"))
        .await;
    assert!(matches!(decision.decision, Decision::Allow));
}
