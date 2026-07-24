//! [`SystemDbAuthorizer`]: the [`Authorizer`] that answers from Corium's own
//! authorization database.
//!
//! The request path is deliberately local and lock-light: authenticate in the
//! interceptor (already done by the time this runs), read the current compiled
//! policy snapshot, walk it, decide. Compilation happens when the authz basis
//! `t` advances — on a background refresh task where one is spawned, or lazily
//! on the first check otherwise — never per request.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use corium_protocol::authz::{Access, ActionClass, Authorizer, Decision, Principal, ViewFilter};

use crate::audit::{AuditEvent, AuditSink, TracingAudit};
use crate::eval::{self, Denial, Limits, Outcome};
use crate::model::{ObjectRef, action_name};
use crate::policy::{Policy, PolicyError};
use crate::source::{PolicySource, SourceError};
use crate::subject::{self, SubjectMapping};

/// How fresh the policy behind a decision must be.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Consistency {
    /// Decide from the latest compiled snapshot this process holds.
    ///
    /// The fast path: no I/O, and policy changes reach the process through the
    /// refresh task. Propagation across a fleet is eventually consistent.
    #[default]
    Pinned,
    /// Re-read the source before deciding, so the decision reflects every
    /// policy change committed before the request arrived.
    Fresh,
}

/// Who may still act when the authorization database cannot be read.
///
/// This exists for operator recovery — restoring a corrupted authz database,
/// reaching a cluster whose policy store is down — and nothing else. Every
/// break-glass grant is audited as a denial-turned-grant at `warn`.
#[derive(Clone, Debug, Default)]
pub struct BreakGlass {
    /// Principal subjects admitted while the policy is unavailable.
    pub subjects: BTreeSet<String>,
    /// Roles admitted while the policy is unavailable.
    pub roles: BTreeSet<String>,
}

impl BreakGlass {
    /// Whether `principal` is admitted by this configuration.
    #[must_use]
    pub fn admits(&self, principal: &Principal) -> bool {
        self.subjects.contains(&principal.subject)
            || principal.roles.iter().any(|role| self.roles.contains(role))
    }
}

/// Tuning for [`SystemDbAuthorizer`].
#[derive(Clone, Debug)]
pub struct AuthzConfig {
    /// Search bounds for one check.
    pub limits: Limits,
    /// How a principal's claims become subjects.
    pub mapping: SubjectMapping,
    /// Default consistency for a decision.
    pub consistency: Consistency,
    /// Action classes that always re-read the source, whatever `consistency`
    /// says. Empty by default; setting `[Admin]` makes control-plane changes
    /// wait for the newest policy while reads stay on the pinned snapshot.
    pub fresh_for: BTreeSet<ActionClass>,
    /// Relations a plain (non-userset) subject object is expanded through, so
    /// `group:eng writer database:music` grants everyone with `member` on
    /// `group:eng`.
    pub expand_relations: Vec<String>,
    /// The object catalog-wide actions target.
    pub catalog_object: ObjectRef,
    /// Entries kept in the check-result cache; 0 disables it.
    pub check_cache_capacity: usize,
    /// Operator recovery access when policy cannot be read.
    pub break_glass: Option<BreakGlass>,
    /// How often the refresh task polls when the source has no change signal.
    pub refresh_interval: Duration,
}

impl Default for AuthzConfig {
    fn default() -> Self {
        Self {
            limits: Limits::default(),
            mapping: SubjectMapping::default(),
            consistency: Consistency::default(),
            fresh_for: BTreeSet::new(),
            expand_relations: vec!["member".to_owned()],
            catalog_object: ObjectRef::new("catalog", "*"),
            check_cache_capacity: 4_096,
            break_glass: None,
            refresh_interval: crate::source::DEFAULT_POLL_INTERVAL,
        }
    }
}

/// A decision, with everything an audit line or a `corium authz check` needs.
#[derive(Clone)]
pub struct AuthzDecision {
    /// The decision itself.
    pub decision: Decision,
    /// Authz database basis the decision was made against.
    pub authz_t: u64,
    /// Target object the check ran against.
    pub object: String,
    /// Relationship path that granted the access.
    pub path: Option<String>,
    /// Views the decision was narrowed through.
    pub views: Vec<String>,
    /// Why it was denied, when it was.
    pub reason: Option<String>,
}

impl AuthzDecision {
    /// Whether the access was permitted.
    #[must_use]
    pub fn is_allowed(&self) -> bool {
        !matches!(self.decision, Decision::Deny(_))
    }

    /// The view filter, when the decision narrows visibility.
    #[must_use]
    pub fn filter(&self) -> Option<Arc<dyn ViewFilter>> {
        match &self.decision {
            Decision::AllowFiltered(filter) => Some(Arc::clone(filter)),
            _ => None,
        }
    }
}

impl std::fmt::Debug for AuthzDecision {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("AuthzDecision")
            .field("decision", &self.decision)
            .field("authz_t", &self.authz_t)
            .field("object", &self.object)
            .field("path", &self.path)
            .field("views", &self.views)
            .field("reason", &self.reason)
            .finish()
    }
}

/// Answers authorization from a Corium database holding `ReBAC` policy.
pub struct SystemDbAuthorizer {
    source: Arc<dyn PolicySource>,
    config: AuthzConfig,
    policy: RwLock<Option<Arc<Policy>>>,
    checks: Mutex<CheckCache>,
    audit: Arc<dyn AuditSink>,
}

impl SystemDbAuthorizer {
    /// Builds an authorizer over `source` with default tuning.
    #[must_use]
    pub fn new(source: Arc<dyn PolicySource>) -> Self {
        Self::with_config(source, AuthzConfig::default())
    }

    /// Builds an authorizer over `source`.
    #[must_use]
    pub fn with_config(source: Arc<dyn PolicySource>, config: AuthzConfig) -> Self {
        let capacity = config.check_cache_capacity;
        Self {
            source,
            config,
            policy: RwLock::new(None),
            checks: Mutex::new(CheckCache::new(capacity)),
            audit: Arc::new(TracingAudit),
        }
    }

    /// Routes audit events to `audit` instead of `tracing` (builder style).
    #[must_use]
    pub fn with_audit(mut self, audit: Arc<dyn AuditSink>) -> Self {
        self.audit = audit;
        self
    }

    /// The tuning in force.
    #[must_use]
    pub fn config(&self) -> &AuthzConfig {
        &self.config
    }

    /// The compiled policy this process is deciding from, if it has one.
    #[must_use]
    pub fn current_policy(&self) -> Option<Arc<Policy>> {
        self.policy
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }

    /// Reads the source and compiles a new snapshot when its basis has moved.
    ///
    /// Safe to call from anywhere: it is what the refresh task loops on, what
    /// a `Fresh` check calls, and what a caller uses to fail startup early on a
    /// misconfigured authz database.
    ///
    /// # Errors
    /// Returns [`RefreshError`] when the snapshot cannot be read or compiled.
    /// The last good policy is kept: a policy that stops compiling does not
    /// silently become "deny everything" while the previous one still applies.
    pub async fn refresh(&self) -> Result<Arc<Policy>, RefreshError> {
        let snapshot = self.source.snapshot().await?;
        if let Some(current) = self.current_policy()
            && current.basis_t() == snapshot.basis_t()
        {
            return Ok(current);
        }
        let policy = Arc::new(Policy::compile(&snapshot)?);
        tracing::debug!(
            target: "corium_authz",
            source = self.source.name(),
            authz_t = policy.basis_t(),
            stats = ?policy.stats(),
            "compiled authorization policy"
        );
        *self
            .policy
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::clone(&policy));
        self.checks
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retarget(policy.basis_t());
        Ok(policy)
    }

    /// Spawns the background task that keeps the compiled snapshot current.
    ///
    /// The task waits on the source's change signal (a basis watch, a
    /// tx-report broadcast, or a poll), recompiles off the request path, and
    /// swaps the result in. It logs and retries on failure rather than
    /// exiting, so a transient store outage does not permanently freeze policy.
    pub fn spawn_refresh(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let authorizer = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                let basis_t = authorizer
                    .current_policy()
                    .map_or(0, |policy| policy.basis_t());
                if let Err(error) = authorizer.refresh().await {
                    tracing::warn!(
                        target: "corium_authz",
                        source = authorizer.source.name(),
                        %error,
                        "cannot refresh authorization policy; keeping the last good snapshot"
                    );
                    tokio::time::sleep(authorizer.config.refresh_interval).await;
                    continue;
                }
                authorizer.source.changed(basis_t).await;
            }
        })
    }

    /// Runs one check, returning the decision with its audit detail.
    pub async fn check(&self, principal: &Principal, access: &Access) -> AuthzDecision {
        let decision = self.decide(principal, access).await;
        self.audit.record(&AuditEvent::new(
            principal,
            access,
            &decision,
            self.source.name(),
        ));
        decision
    }

    async fn decide(&self, principal: &Principal, access: &Access) -> AuthzDecision {
        let fresh = self.config.consistency == Consistency::Fresh
            || self
                .config
                .fresh_for
                .contains(&ActionClass::of(access.action));
        let policy = if fresh {
            match self.refresh().await {
                Ok(policy) => policy,
                // A `Fresh` check that cannot reach the source must not fall
                // back to a pinned snapshot: the caller asked for the newest
                // policy precisely because a stale answer is not good enough.
                Err(error) => return self.unavailable(principal, access, &error.to_string()),
            }
        } else {
            match self.current_policy() {
                Some(policy) => policy,
                None => match self.refresh().await {
                    Ok(policy) => policy,
                    Err(error) => {
                        return self.unavailable(principal, access, &error.to_string());
                    }
                },
            }
        };

        let objects = self.target_objects(&policy, access);
        let object_label = objects
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let key = CacheKey {
            principal: subject::fingerprint(principal, &self.config.mapping),
            action: action_name(access.action),
            object: object_label.clone(),
        };
        if self.config.check_cache_capacity > 0
            && let Some(cached) = self
                .checks
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(policy.basis_t(), &key)
        {
            return cached;
        }

        let subjects = subject::subjects_of(principal, &policy, &self.config.mapping);
        let object_type = objects
            .first()
            .map_or_else(|| "*".to_owned(), |object| object.kind.clone());
        let mut relations = BTreeSet::new();
        for object in &objects {
            relations.extend(policy.relations_for(&object.kind, access.action));
        }
        let query = eval::Query {
            subjects,
            relations,
            action: action_name(access.action).to_owned(),
            objects,
            expand_relations: self.config.expand_relations.clone(),
            limits: self.config.limits,
        };
        let outcome = eval::check(&policy, &query);
        let decision = Self::render(&outcome, policy.basis_t(), object_label, &object_type);
        if self.config.check_cache_capacity > 0 {
            self.checks
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .put(policy.basis_t(), key, decision.clone());
        }
        decision
    }

    /// The objects an access targets.
    ///
    /// A database-scoped action targets `database:<name>` plus any object
    /// registered against that database (a tenant, say). An admin action also
    /// targets the catalog object, so catalog-level ownership covers creating
    /// and deleting databases that have no object of their own yet.
    fn target_objects(&self, policy: &Policy, access: &Access) -> Vec<ObjectRef> {
        let mut objects = Vec::new();
        match &access.database {
            Some(database) => {
                objects.push(ObjectRef::new("database", database.clone()));
                objects.extend(policy.objects_for_database(database).iter().cloned());
                if ActionClass::of(access.action) == ActionClass::Admin {
                    objects.push(self.config.catalog_object.clone());
                }
            }
            None => objects.push(self.config.catalog_object.clone()),
        }
        objects
    }

    fn render(outcome: &Outcome, authz_t: u64, object: String, object_type: &str) -> AuthzDecision {
        match outcome {
            Outcome::Allow { matches } => AuthzDecision {
                decision: Decision::Allow,
                authz_t,
                object,
                path: matches.first().map(eval::Match::render_path),
                views: Vec::new(),
                reason: None,
            },
            Outcome::AllowFiltered {
                matches,
                filter,
                views,
            } => AuthzDecision {
                decision: Decision::AllowFiltered(Arc::clone(filter)),
                authz_t,
                object,
                path: matches.first().map(eval::Match::render_path),
                views: views.clone(),
                reason: None,
            },
            Outcome::Deny(denial) => {
                let reason = match denial {
                    Denial::NoPermission { action, .. } => format!(
                        "no permission maps action {action:?} on object type {object_type:?}"
                    ),
                    other => other.to_string(),
                };
                AuthzDecision {
                    decision: Decision::Deny(reason.clone()),
                    authz_t,
                    object,
                    path: None,
                    views: Vec::new(),
                    reason: Some(reason),
                }
            }
        }
    }

    /// The fail-closed answer when policy cannot be read, with the break-glass
    /// escape for operator recovery.
    fn unavailable(&self, principal: &Principal, access: &Access, error: &str) -> AuthzDecision {
        let authz_t = self.current_policy().map_or(0, |policy| policy.basis_t());
        if let Some(break_glass) = &self.config.break_glass
            && break_glass.admits(principal)
        {
            tracing::warn!(
                target: "corium_authz::audit",
                subject = %principal.subject,
                provider = %principal.provider,
                action = action_name(access.action),
                source = self.source.name(),
                %error,
                "break-glass: allowing access while the authorization database is unavailable"
            );
            return AuthzDecision {
                decision: Decision::Allow,
                authz_t,
                object: access
                    .database
                    .clone()
                    .unwrap_or_else(|| "catalog".to_owned()),
                path: Some("break-glass".to_owned()),
                views: Vec::new(),
                reason: None,
            };
        }
        let reason = format!("authorization policy is unavailable: {error}");
        AuthzDecision {
            decision: Decision::Deny(reason.clone()),
            authz_t,
            object: access
                .database
                .clone()
                .unwrap_or_else(|| "catalog".to_owned()),
            path: None,
            views: Vec::new(),
            reason: Some(reason),
        }
    }
}

#[tonic::async_trait]
impl Authorizer for SystemDbAuthorizer {
    async fn authorize(&self, principal: &Principal, access: &Access) -> Decision {
        self.check(principal, access).await.decision
    }
}

/// Failure to load or compile a policy snapshot.
#[derive(Clone, Debug, thiserror::Error)]
pub enum RefreshError {
    /// The snapshot could not be read.
    #[error(transparent)]
    Source(#[from] SourceError),
    /// The snapshot is not a well-formed policy.
    #[error(transparent)]
    Policy(#[from] PolicyError),
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct CacheKey {
    principal: String,
    action: &'static str,
    object: String,
}

/// Positive and negative check results for one authz basis.
///
/// Keying by basis `t` is what makes invalidation free: a policy change moves
/// the basis, and every entry under the old one is dropped wholesale.
struct CheckCache {
    basis_t: u64,
    capacity: usize,
    entries: HashMap<CacheKey, AuthzDecision>,
}

impl CheckCache {
    fn new(capacity: usize) -> Self {
        Self {
            basis_t: 0,
            capacity,
            entries: HashMap::new(),
        }
    }

    fn retarget(&mut self, basis_t: u64) {
        if self.basis_t != basis_t {
            self.basis_t = basis_t;
            self.entries.clear();
        }
    }

    fn get(&mut self, basis_t: u64, key: &CacheKey) -> Option<AuthzDecision> {
        self.retarget(basis_t);
        self.entries.get(key).cloned()
    }

    fn put(&mut self, basis_t: u64, key: CacheKey, decision: AuthzDecision) {
        self.retarget(basis_t);
        // Bulk eviction rather than an LRU: entries are cheap, uniform, and
        // die at the next policy change anyway, so the extra bookkeeping of a
        // recency order would cost more than the occasional refill.
        if self.entries.len() >= self.capacity {
            self.entries.clear();
        }
        self.entries.insert(key, decision);
    }
}
