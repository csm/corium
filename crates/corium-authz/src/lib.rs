//! Self-hosted relationship-based authorization (`ReBAC`) over a Corium
//! database.
//!
//! `corium-protocol`'s [`Authorizer`](corium_protocol::authz::Authorizer) seam
//! was built so a deployment could point authorization at an external policy
//! service. This crate is the answer for deployments that would rather not run
//! one: Corium's own data model — entities, reference attributes, immutable
//! history — is a good fit for the tuple shape `ReBAC` needs, so the policy
//! lives in an ordinary Corium database and the check is a bounded in-memory
//! graph walk over a compiled snapshot of it.
//!
//! ```text
//! Check(subject, action, object, database, at_authz_t)
//!   action  -> required relation(s)          (permission map)
//!   subject --relation/derived--> object     (tuples + rewrites)
//!   optional binding -> ViewFilter           (bindings + views)
//! ```
//!
//! # The pieces
//!
//! * [`schema`] — the reserved attributes of the authz database, and the EDN
//!   an operator installs to create one.
//! * [`model`] — the policy vocabulary: objects, tuples, permissions,
//!   rewrites, views, bindings. Relation names are *data*, not Rust enums.
//! * [`policy::Policy`] — an immutable compiled snapshot keyed by the authz
//!   database's basis `t`, with the indexes a check needs.
//! * [`eval`] — the bounded, cycle-safe relationship search.
//! * [`subject`] — how a request [`Principal`](corium_protocol::authz::Principal)
//!   becomes the subjects the search starts from.
//! * [`source::PolicySource`] — where snapshots come from; each surface
//!   supplies the database value it already holds.
//! * [`SystemDbAuthorizer`] — the `Authorizer` itself: snapshot caching,
//!   consistency, result caching, fail-closed behaviour, break-glass, audit.
//! * [`bootstrap`] — building an authz database in process, and the EDN an
//!   operator's grant/revoke sends to a real one.
//!
//! # Fail closed
//!
//! Every failure that leaves the authorizer without a policy — a missing authz
//! database, an unreadable snapshot, a policy that does not compile — denies.
//! The one exception is an explicit [`BreakGlass`] configuration for operator
//! recovery, and it is audited at `warn` every time it grants. A snapshot that
//! stops compiling never *replaces* the last good one either: the process
//! keeps deciding from the policy it already has.
//!
//! # Example
//!
//! ```
//! use std::sync::Arc;
//!
//! use corium_authz::{SystemDbAuthorizer, bootstrap, source::MemoryPolicySource};
//! use corium_protocol::authz::{Access, Action, Authorizer, Decision, Principal};
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() {
//! let db = bootstrap::policy_db(r#"
//! [{:db/id "read" :authz.permission/object-type "database"
//!   :authz.permission/action "read" :authz.permission/relation ["viewer"]}
//!  {:db/id "t1" :authz.tuple/subject "group:eng#member"
//!   :authz.tuple/relation "viewer" :authz.tuple/object "database:music"}
//!  {:db/id "t2" :authz.tuple/subject "user:alice"
//!   :authz.tuple/relation "member" :authz.tuple/object "group:eng"}]
//! "#).expect("policy compiles");
//!
//! let authorizer = SystemDbAuthorizer::new(Arc::new(MemoryPolicySource::new("authz", db)));
//! let alice = Principal::new("oidc", "alice");
//! let bob = Principal::new("oidc", "bob");
//!
//! assert!(matches!(
//!     authorizer.authorize(&alice, &Access::on(Action::Query, "music")).await,
//!     Decision::Allow
//! ));
//! assert!(matches!(
//!     authorizer.authorize(&bob, &Access::on(Action::Query, "music")).await,
//!     Decision::Deny(_)
//! ));
//! # }
//! ```

pub mod audit;
pub mod authorizer;
pub mod bootstrap;
pub mod eval;
pub mod model;
pub mod policy;
pub mod schema;
pub mod source;
pub mod subject;
pub mod view;

pub use authorizer::{
    AuthzConfig, AuthzDecision, BreakGlass, Consistency, RefreshError, SystemDbAuthorizer,
};
pub use eval::{Denial, Limits, Match, Outcome};
pub use model::{ObjectRef, SubjectRef};
pub use policy::{Policy, PolicyError, PolicyStats};
pub use schema::DEFAULT_AUTHZ_DB;
pub use source::{MemoryPolicySource, PolicySource, SourceError};
pub use subject::SubjectMapping;
