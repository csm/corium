//! Audit events for authorization decisions.
//!
//! Every decision is a natural audit record, and the pieces a reviewer needs
//! are exactly what the check already has: who asked, what they asked for,
//! what was decided, the authz basis `t` the decision was made against, the
//! relationship path that granted it, and the view filter it was narrowed
//! through. Recording `authz_t` is what makes a decision reproducible — the
//! authz database read `as-of` that `t` yields the policy that produced it.

use corium_protocol::authz::{Access, Principal};

use crate::model::action_name;

/// One authorization decision, as recorded.
#[derive(Clone, Debug)]
pub struct AuditEvent {
    /// Subject of the principal that asked.
    pub subject: String,
    /// Provider that vouched for it.
    pub provider: String,
    /// Action name.
    pub action: &'static str,
    /// Target database, when the action names one.
    pub database: Option<String>,
    /// Target object the check ran against.
    pub object: String,
    /// Whether the access was permitted.
    pub allowed: bool,
    /// Reason, when denied.
    pub reason: Option<String>,
    /// Relationship path that granted the access.
    pub path: Option<String>,
    /// Views the decision was narrowed through.
    pub views: Vec<String>,
    /// Authz database basis the decision was made against.
    pub authz_t: u64,
    /// Name of the authz database.
    pub source: String,
}

impl AuditEvent {
    /// Builds an event from a decision.
    #[must_use]
    pub fn new(
        principal: &Principal,
        access: &Access,
        decision: &crate::AuthzDecision,
        source: &str,
    ) -> Self {
        Self {
            subject: principal.subject.clone(),
            provider: principal.provider.clone(),
            action: action_name(access.action),
            database: access.database.clone(),
            object: decision.object.clone(),
            allowed: decision.is_allowed(),
            reason: decision.reason.clone(),
            path: decision.path.clone(),
            views: decision.views.clone(),
            authz_t: decision.authz_t,
            source: source.to_owned(),
        }
    }
}

/// Receives audit events.
pub trait AuditSink: Send + Sync + 'static {
    /// Records one decision.
    fn record(&self, event: &AuditEvent);
}

/// The default sink: `tracing`, denials at `info` and grants at `debug`, both
/// under the `corium_authz::audit` target so a deployment can route them.
#[derive(Clone, Copy, Debug, Default)]
pub struct TracingAudit;

impl AuditSink for TracingAudit {
    fn record(&self, event: &AuditEvent) {
        if event.allowed {
            tracing::debug!(
                target: "corium_authz::audit",
                subject = %event.subject,
                provider = %event.provider,
                action = event.action,
                object = %event.object,
                path = event.path.as_deref().unwrap_or(""),
                views = ?event.views,
                authz_t = event.authz_t,
                source = %event.source,
                "authorization allowed"
            );
        } else {
            tracing::info!(
                target: "corium_authz::audit",
                subject = %event.subject,
                provider = %event.provider,
                action = event.action,
                object = %event.object,
                reason = event.reason.as_deref().unwrap_or(""),
                authz_t = event.authz_t,
                source = %event.source,
                "authorization denied"
            );
        }
    }
}

/// Discards every event.
#[derive(Clone, Copy, Debug, Default)]
pub struct NullAudit;

impl AuditSink for NullAudit {
    fn record(&self, _event: &AuditEvent) {}
}
