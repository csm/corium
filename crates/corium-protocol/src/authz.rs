//! Spike: request-scoped identity and authorization for the network surfaces.
//!
//! The shipped [`auth`](crate::auth) module authenticates a *connection*: its
//! [`Authenticator`](crate::auth::Authenticator) returns a bare `bool`, so a
//! request either passes the bearer-token gate or it does not, and no identity
//! survives into the handler. That is enough for a single trusted operator, but
//! it cannot express three things this spike explores:
//!
//! 1. **Optional, layered enforcement.** A surface may run wide open, require
//!    only authentication, or require authentication *and* per-operation
//!    authorization — chosen at deploy time without recompiling.
//! 2. **External identity providers on a shared, public system.** A transactor
//!    or peer server exposed to many tenants must accept tokens minted by
//!    someone else (an OIDC issuer, an mTLS CA) and turn them into a local
//!    [`Principal`], not compare them to a single static secret.
//! 3. **Many users, different views, one server.** Because identity is bound to
//!    the *request* (not the connection), one hosted peer can answer two
//!    callers concurrently and give each a different authorization decision and
//!    a different [`ViewFilter`] over the same database.
//!
//! The model is deliberately transport-adjacent: [`Guard`] bundles a chosen
//! ([`IdentityProvider`], [`Authorizer`]) pair, and [`IdentityInterceptor`] is
//! the only tonic-facing piece. A deployment that wants none of this constructs
//! [`Guard::disabled`] and behaves exactly as today.
//!
//! Authn is **synchronous** and authz is **asynchronous**, on purpose:
//! authenticating a request is local verification (a token compare, a signature
//! check against cached keys, an mTLS subject) and it runs inside the
//! synchronous tonic [`Interceptor`]; authorizing one may consult an external
//! policy oracle (`OpenFGA` / `Auth0 FGA` answer a per-decision network `Check`), and
//! it runs handler-side where awaiting is free. A provider that genuinely needs
//! I/O (OIDC token introspection) caches results and refreshes out of band, so
//! [`IdentityProvider::authenticate`] stays a cheap synchronous lookup.
//!
//! See `docs/design/auth.md` for how this maps onto the RPC surface and the
//! migration away from the bool authenticator.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Arc;

use tonic::metadata::MetadataMap;
use tonic::service::Interceptor;
use tonic::{Request, Status};

/// An authenticated (or anonymous) caller, carried in request extensions.
///
/// A principal is what an [`IdentityProvider`] produces from credentials and
/// what an [`Authorizer`] consumes. It is intentionally a flat bag of strings
/// so that identities from different providers (static tokens, OIDC claims,
/// mTLS subjects) share one shape; richer typing can come later without
/// changing the seam.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Principal {
    /// Stable subject identifier, unique within `provider` (e.g. an OIDC
    /// `sub`, a certificate SAN, or a static-token label).
    pub subject: String,
    /// Name of the [`IdentityProvider`] that vouched for this principal.
    pub provider: String,
    /// Roles used by role-based authorizers.
    pub roles: BTreeSet<String>,
    /// Arbitrary provider claims (e.g. `tenant`, `email`, `scope`).
    pub claims: BTreeMap<String, String>,
}

impl Principal {
    /// The unauthenticated caller. Present whenever a surface does not require
    /// authentication, so handlers never deal with an absent identity.
    #[must_use]
    pub fn anonymous() -> Self {
        Self {
            subject: "anonymous".to_owned(),
            provider: "anonymous".to_owned(),
            roles: BTreeSet::new(),
            claims: BTreeMap::new(),
        }
    }

    /// Builds a principal with `subject` vouched for by `provider`.
    #[must_use]
    pub fn new(provider: impl Into<String>, subject: impl Into<String>) -> Self {
        Self {
            subject: subject.into(),
            provider: provider.into(),
            roles: BTreeSet::new(),
            claims: BTreeMap::new(),
        }
    }

    /// Adds a role (builder style).
    #[must_use]
    pub fn with_role(mut self, role: impl Into<String>) -> Self {
        self.roles.insert(role.into());
        self
    }

    /// Adds a claim (builder style).
    #[must_use]
    pub fn with_claim(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.claims.insert(key.into(), value.into());
        self
    }

    /// Whether this is the anonymous principal.
    #[must_use]
    pub fn is_anonymous(&self) -> bool {
        self.provider == "anonymous"
    }

    /// Whether the principal holds `role`.
    #[must_use]
    pub fn has_role(&self, role: &str) -> bool {
        self.roles.contains(role)
    }

    /// Reads a claim by key.
    #[must_use]
    pub fn claim(&self, key: &str) -> Option<&str> {
        self.claims.get(key).map(String::as_str)
    }
}

/// Raw credentials extracted from a request, before authentication.
///
/// Only the bearer token is populated by [`Credentials::from_metadata`] today;
/// `client_cert_subject` is the documented seam for mTLS, which tonic exposes
/// on the request extensions inside the service (not in the interceptor), so a
/// full wiring fills it in there. Keeping both here lets one provider consume
/// either without a second extraction path.
#[derive(Clone, Debug, Default)]
pub struct Credentials {
    /// Value after `Bearer ` in the `authorization` metadata, if any.
    pub bearer: Option<String>,
    /// Subject (SAN/CN) of a verified client certificate, if mTLS is in use.
    pub client_cert_subject: Option<String>,
}

impl Credentials {
    /// Extracts credentials from gRPC request metadata.
    #[must_use]
    pub fn from_metadata(metadata: &MetadataMap) -> Self {
        let bearer = metadata
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .map(str::to_owned);
        Self {
            bearer,
            client_cert_subject: None,
        }
    }

    /// Whether any credential material was presented.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.bearer.is_none() && self.client_cert_subject.is_none()
    }
}

/// Failure of authentication or authorization.
#[derive(Clone, Debug, thiserror::Error)]
pub enum AuthError {
    /// Credentials were required, missing, or invalid.
    #[error("unauthenticated: {0}")]
    Unauthenticated(String),
    /// The principal is known but not permitted the requested access.
    #[error("forbidden: {0}")]
    Forbidden(String),
}

impl AuthError {
    /// Maps onto the gRPC status a handler should return.
    #[must_use]
    pub fn to_status(&self) -> Status {
        match self {
            Self::Unauthenticated(message) => Status::unauthenticated(message.clone()),
            Self::Forbidden(message) => Status::permission_denied(message.clone()),
        }
    }
}

/// Turns credentials into a [`Principal`], or declines.
///
/// The three-way return is what makes providers composable on a shared system:
/// `Ok(Some(p))` accepts, `Ok(None)` *abstains* (these are not my credentials —
/// let the next provider try), and `Err` *rejects* (these look like mine but are
/// invalid — stop and fail). A provider that abstains on absent credentials lets
/// an anonymous fallback take over when the surface allows it.
pub trait IdentityProvider: Send + Sync + 'static {
    /// A short name recorded in [`Principal::provider`] and logs.
    fn name(&self) -> &str;

    /// Authenticates `credentials`.
    ///
    /// # Errors
    /// Returns [`AuthError::Unauthenticated`] when credentials are addressed to
    /// this provider but fail verification.
    fn authenticate(&self, credentials: &Credentials) -> Result<Option<Principal>, AuthError>;
}

/// Accepts everyone as [`Principal::anonymous`]. Used when a surface does not
/// require authentication.
#[derive(Clone, Debug, Default)]
pub struct AllowAnonymous;

impl IdentityProvider for AllowAnonymous {
    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "anonymous"
    }

    fn authenticate(&self, _credentials: &Credentials) -> Result<Option<Principal>, AuthError> {
        Ok(Some(Principal::anonymous()))
    }
}

/// Maps a fixed set of bearer tokens to fixed principals.
///
/// The successor to [`StaticToken`](crate::auth::StaticToken): instead of one
/// shared secret gating the whole server, each token names a distinct principal,
/// so a small multi-user deployment gets per-caller identity and authorization
/// without an external issuer.
#[derive(Clone, Debug, Default)]
pub struct StaticTokens {
    tokens: BTreeMap<String, Principal>,
}

impl StaticTokens {
    /// An empty table; every request abstains.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers `token` as `principal`.
    #[must_use]
    pub fn with(mut self, token: impl Into<String>, principal: Principal) -> Self {
        self.tokens.insert(token.into(), principal);
        self
    }
}

impl IdentityProvider for StaticTokens {
    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "static-token"
    }

    fn authenticate(&self, credentials: &Credentials) -> Result<Option<Principal>, AuthError> {
        // Abstain on an unknown token rather than reject: an opaque token this
        // table does not hold may still be a valid OIDC/mTLS credential a later
        // provider recognizes. The all-abstained outcome is the [`Guard`]'s call.
        Ok(credentials
            .bearer
            .as_ref()
            .and_then(|token| self.tokens.get(token).cloned()))
    }
}

/// Verifies an opaque bearer token minted by an external issuer.
///
/// This is the seam for OIDC/JWT and similar: a real implementation validates a
/// signature against a JWKS (or introspects the token with the issuer) and maps
/// verified claims onto a [`Principal`]. The spike ships only the trait so the
/// dependency choice and network calls stay out of this crate; a concrete
/// verifier lives behind a feature flag in a later milestone.
pub trait TokenVerifier: Send + Sync + 'static {
    /// Verifies `token` and derives a principal from its claims.
    ///
    /// # Errors
    /// Returns [`AuthError::Unauthenticated`] for a malformed, expired, or
    /// untrusted token.
    fn verify(&self, token: &str) -> Result<Principal, AuthError>;
}

/// Adapts a [`TokenVerifier`] into an [`IdentityProvider`].
pub struct ExternalTokens<V: TokenVerifier> {
    name: String,
    verifier: V,
}

impl<V: TokenVerifier> ExternalTokens<V> {
    /// Wraps `verifier`, labelling produced principals with `name`.
    pub fn new(name: impl Into<String>, verifier: V) -> Self {
        Self {
            name: name.into(),
            verifier,
        }
    }
}

impl<V: TokenVerifier> IdentityProvider for ExternalTokens<V> {
    fn name(&self) -> &str {
        &self.name
    }

    fn authenticate(&self, credentials: &Credentials) -> Result<Option<Principal>, AuthError> {
        match &credentials.bearer {
            None => Ok(None),
            Some(token) => self.verifier.verify(token).map(Some),
        }
    }
}

/// Tries a list of providers in order, so one shared system can accept several
/// issuers at once (static tokens for internal tools, OIDC for humans, mTLS for
/// services). The first provider to accept wins; the first to reject stops the
/// chain. If every provider abstains, the composite abstains too — whether that
/// becomes anonymous access or a rejection is the [`Guard`]'s policy.
#[derive(Clone)]
pub struct CompositeProvider {
    providers: Vec<Arc<dyn IdentityProvider>>,
}

impl CompositeProvider {
    /// Builds a chain of providers, tried in order.
    #[must_use]
    pub fn new(providers: Vec<Arc<dyn IdentityProvider>>) -> Self {
        Self { providers }
    }
}

impl IdentityProvider for CompositeProvider {
    #[allow(clippy::unnecessary_literal_bound)]
    fn name(&self) -> &str {
        "composite"
    }

    fn authenticate(&self, credentials: &Credentials) -> Result<Option<Principal>, AuthError> {
        for provider in &self.providers {
            if let Some(principal) = provider.authenticate(credentials)? {
                return Ok(Some(principal));
            }
        }
        Ok(None)
    }
}

/// The operation a request wants to perform, for authorization.
///
/// One variant per public RPC family; the mapping from concrete RPCs is in
/// `docs/design/auth.md`. [`Action::is_write`] and [`Action::is_admin`] let
/// coarse policies grant by category instead of enumerating every action.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Action {
    /// Datalog query against a database view.
    Query,
    /// Pull expression against a database view.
    Pull,
    /// Raw datom access against a database view.
    Datoms,
    /// Transaction-log range read.
    TxRange,
    /// Live subscription to a database.
    Subscribe,
    /// Stats / status read.
    Inspect,
    /// Submit a transaction.
    Transact,
    /// Create a database.
    CreateDatabase,
    /// Delete a database.
    DeleteDatabase,
    /// Fork a database.
    ForkDatabase,
    /// List databases in the catalog.
    ListDatabases,
    /// Garbage-collect deleted databases or old segments.
    GarbageCollect,
    /// Request or reconfigure indexing.
    ManageIndex,
}

impl Action {
    /// Whether the action mutates database state.
    #[must_use]
    pub fn is_write(self) -> bool {
        matches!(self, Self::Transact)
    }

    /// Whether the action is a catalog/administration operation.
    #[must_use]
    pub fn is_admin(self) -> bool {
        matches!(
            self,
            Self::CreateDatabase
                | Self::DeleteDatabase
                | Self::ForkDatabase
                | Self::GarbageCollect
                | Self::ManageIndex
        )
    }
}

/// A specific access: an [`Action`] on an optional target database.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Access {
    /// The operation being attempted.
    pub action: Action,
    /// The target database, or `None` for catalog-wide operations.
    pub database: Option<String>,
}

impl Access {
    /// An action targeting `database`.
    #[must_use]
    pub fn on(action: Action, database: impl Into<String>) -> Self {
        Self {
            action,
            database: Some(database.into()),
        }
    }

    /// A catalog-wide action with no specific database.
    #[must_use]
    pub fn catalog(action: Action) -> Self {
        Self {
            action,
            database: None,
        }
    }
}

/// Restricts what a principal may see within a database it is allowed to read.
///
/// This is the "different views, one server" seam. An [`Authorizer`] can return
/// [`Decision::AllowFiltered`] with a filter, and the query/datom paths consult
/// it before returning facts. The spike defines attribute-level visibility (the
/// cheapest useful cut, enforceable in the peer's datom scan); entity- or
/// value-predicate filtering is a documented extension of the same trait.
pub trait ViewFilter: Send + Sync + fmt::Debug {
    /// Whether datoms of `attribute` (a keyword ident like `:person/email`) are
    /// visible to the principal.
    fn attribute_visible(&self, attribute: &str) -> bool;
}

/// A [`ViewFilter`] that hides every attribute outside an allowlist.
#[derive(Clone, Debug)]
pub struct AttributeAllowlist {
    allowed: BTreeSet<String>,
}

impl AttributeAllowlist {
    /// Builds an allowlist from attribute idents.
    pub fn new(attributes: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            allowed: attributes.into_iter().map(Into::into).collect(),
        }
    }
}

impl ViewFilter for AttributeAllowlist {
    fn attribute_visible(&self, attribute: &str) -> bool {
        self.allowed.contains(attribute)
    }
}

/// The outcome of an authorization check.
#[derive(Clone)]
pub enum Decision {
    /// Permit the access with full visibility.
    Allow,
    /// Permit the access, but only through `filter`.
    AllowFiltered(Arc<dyn ViewFilter>),
    /// Refuse the access; the string explains why.
    Deny(String),
}

impl fmt::Debug for Decision {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Allow => formatter.write_str("Allow"),
            Self::AllowFiltered(filter) => write!(formatter, "AllowFiltered({filter:?})"),
            Self::Deny(reason) => write!(formatter, "Deny({reason:?})"),
        }
    }
}

/// Decides whether a [`Principal`] may perform an [`Access`].
///
/// This is **async**, unlike [`IdentityProvider`], because the interesting
/// authorizers call out to an external policy oracle — a relationship-based
/// service such as `OpenFGA` / `Auth0 FGA` answers a per-decision `Check(user,
/// relation, object)` over the network, and modelling that as a blocking call
/// would stall a runtime thread. Local authorizers ([`AllowAll`],
/// [`PolicyAuthorizer`]) simply return without awaiting. Authorization also runs
/// handler-side, inside the async RPC, so `.await` costs nothing structurally
/// there — whereas authn runs in the synchronous tonic interceptor.
#[tonic::async_trait]
pub trait Authorizer: Send + Sync + 'static {
    /// Renders a [`Decision`] for `principal` attempting `access`.
    async fn authorize(&self, principal: &Principal, access: &Access) -> Decision;
}

/// Permits every access. Used when a surface requires authentication but not
/// authorization (or none at all, paired with [`AllowAnonymous`]).
#[derive(Clone, Debug, Default)]
pub struct AllowAll;

#[tonic::async_trait]
impl Authorizer for AllowAll {
    async fn authorize(&self, _principal: &Principal, _access: &Access) -> Decision {
        Decision::Allow
    }
}

/// A grant held by a role: which actions, over which databases, with what view.
#[derive(Clone, Debug)]
pub struct Grant {
    /// Actions this grant permits. Empty means "any action".
    pub actions: BTreeSet<ActionClass>,
    /// Databases this grant covers. Empty means "any database".
    pub databases: BTreeSet<String>,
    /// Optional view restriction applied when the grant permits a read.
    pub view: Option<Arc<dyn ViewFilter>>,
}

/// A coarse class of action, so grants need not list every [`Action`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ActionClass {
    /// Any read (`Query`, `Pull`, `Datoms`, `TxRange`, `Subscribe`, `Inspect`).
    Read,
    /// Any write (`Transact`).
    Write,
    /// Any catalog/administration action.
    Admin,
}

impl ActionClass {
    /// The class an [`Action`] falls into.
    #[must_use]
    pub fn of(action: Action) -> Self {
        if action.is_admin() {
            Self::Admin
        } else if action.is_write() {
            Self::Write
        } else {
            Self::Read
        }
    }
}

impl Grant {
    /// A grant permitting `actions` over `databases`.
    #[must_use]
    pub fn new(
        actions: impl IntoIterator<Item = ActionClass>,
        databases: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            actions: actions.into_iter().collect(),
            databases: databases.into_iter().map(Into::into).collect(),
            view: None,
        }
    }

    /// Attaches a view restriction (builder style).
    #[must_use]
    pub fn with_view(mut self, view: Arc<dyn ViewFilter>) -> Self {
        self.view = Some(view);
        self
    }

    fn covers(&self, access: &Access) -> bool {
        let action_ok =
            self.actions.is_empty() || self.actions.contains(&ActionClass::of(access.action));
        let database_ok = self.databases.is_empty()
            || access
                .database
                .as_ref()
                .is_some_and(|database| self.databases.contains(database));
        action_ok && database_ok
    }
}

/// A role-based authorizer: a principal's roles select [`Grant`]s, and the
/// union of matching grants decides the access. Deny-by-default — an access no
/// grant covers is refused.
#[derive(Clone, Default)]
pub struct PolicyAuthorizer {
    roles: BTreeMap<String, Vec<Grant>>,
}

impl PolicyAuthorizer {
    /// An empty policy that denies everything.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Grants `grant` to holders of `role`.
    #[must_use]
    pub fn grant(mut self, role: impl Into<String>, grant: Grant) -> Self {
        self.roles.entry(role.into()).or_default().push(grant);
        self
    }
}

#[tonic::async_trait]
impl Authorizer for PolicyAuthorizer {
    async fn authorize(&self, principal: &Principal, access: &Access) -> Decision {
        let mut matched = false;
        let mut view: Option<Arc<dyn ViewFilter>> = None;
        for role in &principal.roles {
            let Some(grants) = self.roles.get(role) else {
                continue;
            };
            for grant in grants {
                if grant.covers(access) {
                    matched = true;
                    // A read gets the narrowest requirement the matching grants
                    // impose; the first view wins for the spike. An unfiltered
                    // matching grant widens to full visibility.
                    match &grant.view {
                        Some(filter) if view.is_none() => view = Some(Arc::clone(filter)),
                        Some(_) => {}
                        None => return Decision::Allow,
                    }
                }
            }
        }
        if !matched {
            return Decision::Deny(format!(
                "principal {:?} has no grant for {:?}",
                principal.subject, access
            ));
        }
        match view {
            Some(filter) => Decision::AllowFiltered(filter),
            None => Decision::Allow,
        }
    }
}

/// A chosen ([`IdentityProvider`], [`Authorizer`]) pair — the policy a surface
/// enforces. One `Guard` serves every request on a surface; identity is derived
/// per request, so a single guard handles many concurrent callers.
///
/// `allow_anonymous` decides what happens when no provider recognizes a
/// request's credentials: `true` admits it as [`Principal::anonymous`] (auth is
/// optional), `false` rejects it (auth is required). Authorization still runs on
/// the resulting principal, so "optional authn + strict authz" is expressible —
/// anonymous simply holds no roles.
#[derive(Clone)]
pub struct Guard {
    identity: Arc<dyn IdentityProvider>,
    authorizer: Arc<dyn Authorizer>,
    allow_anonymous: bool,
}

impl Guard {
    /// Builds a guard that *requires* authentication (`allow_anonymous = false`).
    #[must_use]
    pub fn new(identity: Arc<dyn IdentityProvider>, authorizer: Arc<dyn Authorizer>) -> Self {
        Self {
            identity,
            authorizer,
            allow_anonymous: false,
        }
    }

    /// Sets whether unrecognized/absent credentials fall back to anonymous
    /// (builder style).
    #[must_use]
    pub fn allow_anonymous(mut self, allow: bool) -> Self {
        self.allow_anonymous = allow;
        self
    }

    /// The wide-open policy: anonymous is accepted and everything is allowed.
    /// Behaviourally identical to running with no auth.
    #[must_use]
    pub fn disabled() -> Self {
        Self::new(Arc::new(AllowAnonymous), Arc::new(AllowAll)).allow_anonymous(true)
    }

    /// Whether this guard enforces nothing (anonymous provider + allow-all), so
    /// callers can skip the identity plumbing on hot paths if they wish.
    #[must_use]
    pub fn is_disabled(&self) -> bool {
        self.identity.name() == "anonymous"
    }

    /// Authenticates a request's credentials into a principal.
    ///
    /// # Errors
    /// Propagates a provider's [`AuthError`], or returns
    /// [`AuthError::Unauthenticated`] when no provider accepts and anonymous
    /// access is not allowed.
    pub fn authenticate(&self, credentials: &Credentials) -> Result<Principal, AuthError> {
        match self.identity.authenticate(credentials)? {
            Some(principal) => Ok(principal),
            None if self.allow_anonymous => Ok(Principal::anonymous()),
            None if credentials.is_empty() => Err(AuthError::Unauthenticated(
                "authentication is required".to_owned(),
            )),
            None => Err(AuthError::Unauthenticated(
                "no provider accepted the presented credentials".to_owned(),
            )),
        }
    }

    /// Authorizes `access` for `principal`, returning any view restriction.
    ///
    /// Async because [`Authorizer`] may consult an external policy oracle; local
    /// authorizers resolve without awaiting.
    ///
    /// # Errors
    /// Returns [`AuthError::Forbidden`] when the authorizer denies the access.
    pub async fn authorize(
        &self,
        principal: &Principal,
        access: &Access,
    ) -> Result<Option<Arc<dyn ViewFilter>>, AuthError> {
        match self.authorizer.authorize(principal, access).await {
            Decision::Allow => Ok(None),
            Decision::AllowFiltered(filter) => Ok(Some(filter)),
            Decision::Deny(reason) => Err(AuthError::Forbidden(reason)),
        }
    }
}

/// tonic interceptor that authenticates each request and stashes the resulting
/// [`Principal`] in the request extensions. Because it runs per request, two
/// callers on the same connection get their own identities — the basis for
/// multi-tenant serving. Handlers then call [`principal`] and [`Guard::authorize`]
/// once they know the concrete [`Access`].
#[derive(Clone)]
pub struct IdentityInterceptor {
    guard: Guard,
}

impl IdentityInterceptor {
    /// Wraps `guard` for use with `InterceptedService`.
    #[must_use]
    pub fn new(guard: Guard) -> Self {
        Self { guard }
    }
}

impl Interceptor for IdentityInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        let credentials = Credentials::from_metadata(request.metadata());
        let principal = self
            .guard
            .authenticate(&credentials)
            .map_err(|error| error.to_status())?;
        request.extensions_mut().insert(principal);
        Ok(request)
    }
}

/// Reads the [`Principal`] an [`IdentityInterceptor`] attached to a request,
/// defaulting to anonymous when no interceptor ran (e.g. embedded transport).
#[must_use]
pub fn principal<T>(request: &Request<T>) -> Principal {
    request
        .extensions()
        .get::<Principal>()
        .cloned()
        .unwrap_or_else(Principal::anonymous)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn creds(bearer: &str) -> Credentials {
        Credentials {
            bearer: Some(bearer.to_owned()),
            client_cert_subject: None,
        }
    }

    #[tokio::test]
    async fn disabled_guard_allows_anonymous_everything() {
        let guard = Guard::disabled();
        assert!(guard.is_disabled());
        let principal = guard.authenticate(&Credentials::default()).unwrap();
        assert!(principal.is_anonymous());
        assert!(
            guard
                .authorize(&principal, &Access::on(Action::Transact, "people"))
                .await
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn static_tokens_map_to_distinct_principals() {
        let provider = StaticTokens::new()
            .with(
                "reader-secret",
                Principal::new("static-token", "reader").with_role("reader"),
            )
            .with(
                "writer-secret",
                Principal::new("static-token", "writer").with_role("writer"),
            );

        let reader = provider.authenticate(&creds("reader-secret")).unwrap();
        assert_eq!(reader.unwrap().subject, "reader");
        let writer = provider.authenticate(&creds("writer-secret")).unwrap();
        assert!(writer.unwrap().has_role("writer"));

        // Absent and unknown tokens both abstain, so the table composes ahead
        // of other issuers; the accept/reject call is the Guard's.
        assert!(
            provider
                .authenticate(&Credentials::default())
                .unwrap()
                .is_none()
        );
        assert!(provider.authenticate(&creds("bogus")).unwrap().is_none());
    }

    struct FakeJwt;
    impl TokenVerifier for FakeJwt {
        fn verify(&self, token: &str) -> Result<Principal, AuthError> {
            // Stand-in for signature+claim validation: "oidc:<sub>:<tenant>".
            let mut parts = token.split(':');
            match (parts.next(), parts.next(), parts.next()) {
                (Some("oidc"), Some(sub), Some(tenant)) => Ok(Principal::new("oidc", sub)
                    .with_role("reader")
                    .with_claim("tenant", tenant)),
                _ => Err(AuthError::Unauthenticated("bad token".to_owned())),
            }
        }
    }

    #[test]
    fn external_token_verifier_seam_produces_principal() {
        let provider = ExternalTokens::new("oidc", FakeJwt);
        let principal = provider
            .authenticate(&creds("oidc:alice:acme"))
            .unwrap()
            .unwrap();
        assert_eq!(principal.provider, "oidc");
        assert_eq!(principal.claim("tenant"), Some("acme"));
        assert!(matches!(
            provider.authenticate(&creds("garbage")),
            Err(AuthError::Unauthenticated(_))
        ));
    }

    #[test]
    fn composite_provider_tries_in_order_and_abstains() {
        let statics = StaticTokens::new().with(
            "svc-secret",
            Principal::new("static-token", "svc").with_role("writer"),
        );
        let provider = CompositeProvider::new(vec![
            Arc::new(statics),
            Arc::new(ExternalTokens::new("oidc", FakeJwt)),
        ]);

        // First provider accepts.
        assert_eq!(
            provider
                .authenticate(&creds("svc-secret"))
                .unwrap()
                .unwrap()
                .subject,
            "svc"
        );
        // Static table abstains, so the request falls through to OIDC.
        assert_eq!(
            provider
                .authenticate(&creds("oidc:bob:beta"))
                .unwrap()
                .unwrap()
                .provider,
            "oidc"
        );
        // Absent credentials: both abstain, composite abstains.
        assert!(
            provider
                .authenticate(&Credentials::default())
                .unwrap()
                .is_none()
        );

        // A guard requiring auth turns the abstain into a rejection; the same
        // provider under an anonymous-allowing guard yields anonymous.
        let strict = Guard::new(Arc::new(provider.clone()), Arc::new(AllowAll));
        assert!(matches!(
            strict.authenticate(&Credentials::default()),
            Err(AuthError::Unauthenticated(_))
        ));
        let open = Guard::new(Arc::new(provider), Arc::new(AllowAll)).allow_anonymous(true);
        assert!(
            open.authenticate(&Credentials::default())
                .unwrap()
                .is_anonymous()
        );
    }

    fn sample_policy() -> PolicyAuthorizer {
        PolicyAuthorizer::new()
            .grant("reader", Grant::new([ActionClass::Read], ["people"]))
            .grant(
                "writer",
                Grant::new([ActionClass::Read, ActionClass::Write], ["people"]),
            )
            .grant(
                "admin",
                Grant::new([ActionClass::Admin], Vec::<String>::new()),
            )
    }

    #[tokio::test]
    async fn policy_authorizer_enforces_actions_and_databases() {
        let policy = sample_policy();
        let reader = Principal::new("t", "r").with_role("reader");
        let writer = Principal::new("t", "w").with_role("writer");
        let admin = Principal::new("t", "a").with_role("admin");

        // Reader may query people but not transact, and not touch other dbs.
        assert!(matches!(
            policy
                .authorize(&reader, &Access::on(Action::Query, "people"))
                .await,
            Decision::Allow
        ));
        assert!(matches!(
            policy
                .authorize(&reader, &Access::on(Action::Transact, "people"))
                .await,
            Decision::Deny(_)
        ));
        assert!(matches!(
            policy
                .authorize(&reader, &Access::on(Action::Query, "secrets"))
                .await,
            Decision::Deny(_)
        ));

        // Writer may transact people; admin may create any database.
        assert!(matches!(
            policy
                .authorize(&writer, &Access::on(Action::Transact, "people"))
                .await,
            Decision::Allow
        ));
        assert!(matches!(
            policy
                .authorize(&admin, &Access::catalog(Action::CreateDatabase))
                .await,
            Decision::Allow
        ));
        // Admin grant is admin-only: no read of people.
        assert!(matches!(
            policy
                .authorize(&admin, &Access::on(Action::Query, "people"))
                .await,
            Decision::Deny(_)
        ));
    }

    #[tokio::test]
    async fn per_principal_view_filter_gives_different_views() {
        // Two tenants read the same database through different attribute views.
        let acme_view: Arc<dyn ViewFilter> = Arc::new(AttributeAllowlist::new([
            ":person/name",
            ":person/acme-note",
        ]));
        let beta_view: Arc<dyn ViewFilter> = Arc::new(AttributeAllowlist::new([
            ":person/name",
            ":person/beta-note",
        ]));
        let policy = PolicyAuthorizer::new()
            .grant(
                "acme",
                Grant::new([ActionClass::Read], ["people"]).with_view(acme_view),
            )
            .grant(
                "beta",
                Grant::new([ActionClass::Read], ["people"]).with_view(beta_view),
            );
        let guard = Guard::new(Arc::new(AllowAnonymous), Arc::new(policy));

        let acme = Principal::new("oidc", "alice").with_role("acme");
        let beta = Principal::new("oidc", "bob").with_role("beta");
        let acme_filter = guard
            .authorize(&acme, &Access::on(Action::Query, "people"))
            .await
            .unwrap()
            .expect("acme is filtered");
        let beta_filter = guard
            .authorize(&beta, &Access::on(Action::Query, "people"))
            .await
            .unwrap()
            .expect("beta is filtered");

        assert!(acme_filter.attribute_visible(":person/name"));
        assert!(acme_filter.attribute_visible(":person/acme-note"));
        assert!(!acme_filter.attribute_visible(":person/beta-note"));

        assert!(beta_filter.attribute_visible(":person/beta-note"));
        assert!(!beta_filter.attribute_visible(":person/acme-note"));
    }

    #[test]
    fn interceptor_attaches_principal_to_extensions() {
        let guard = Guard::new(
            Arc::new(StaticTokens::new().with(
                "reader-secret",
                Principal::new("static-token", "reader").with_role("reader"),
            )),
            Arc::new(AllowAll),
        );
        let mut interceptor = IdentityInterceptor::new(guard);

        let mut request = Request::new(());
        request
            .metadata_mut()
            .insert("authorization", "Bearer reader-secret".parse().unwrap());
        let request = interceptor.call(request).unwrap();
        assert_eq!(principal(&request).subject, "reader");

        // A bad token is rejected before reaching any handler.
        let mut bad = Request::new(());
        bad.metadata_mut()
            .insert("authorization", "Bearer nope".parse().unwrap());
        assert_eq!(
            interceptor.call(bad).unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
    }

    #[tokio::test]
    async fn one_guard_serves_two_tenants_with_different_authority() {
        // A single shared guard: static-token issuer + role policy, no anonymous.
        let provider = CompositeProvider::new(vec![Arc::new(
            StaticTokens::new()
                .with(
                    "acme-writer",
                    Principal::new("static-token", "acme-svc").with_role("acme"),
                )
                .with(
                    "beta-reader",
                    Principal::new("static-token", "beta-user").with_role("beta"),
                ),
        )]);
        let policy = PolicyAuthorizer::new()
            .grant(
                "acme",
                Grant::new([ActionClass::Read, ActionClass::Write], ["acme-db"]),
            )
            .grant("beta", Grant::new([ActionClass::Read], ["beta-db"]));
        let guard = Guard::new(Arc::new(provider), Arc::new(policy));

        let acme = guard.authenticate(&creds("acme-writer")).unwrap();
        let beta = guard.authenticate(&creds("beta-reader")).unwrap();

        // acme may write its own db; beta may not read acme's db.
        assert!(
            guard
                .authorize(&acme, &Access::on(Action::Transact, "acme-db"))
                .await
                .is_ok()
        );
        assert!(
            guard
                .authorize(&beta, &Access::on(Action::Query, "acme-db"))
                .await
                .is_err()
        );
        assert!(
            guard
                .authorize(&beta, &Access::on(Action::Query, "beta-db"))
                .await
                .is_ok()
        );
        // beta is read-only even on its own db.
        assert!(
            guard
                .authorize(&beta, &Access::on(Action::Transact, "beta-db"))
                .await
                .is_err()
        );
    }

    /// Stand-in for an external relationship-based oracle (`OpenFGA` / `Auth0 FGA`):
    /// `authorize` awaits a "network" `Check` before deciding. Demonstrates that
    /// the async `Authorizer` seam is dyn-compatible and composes with `Guard`.
    struct FakeOracle {
        allowed: BTreeSet<(String, Action)>,
    }

    #[tonic::async_trait]
    impl Authorizer for FakeOracle {
        async fn authorize(&self, principal: &Principal, access: &Access) -> Decision {
            // Simulate the round-trip to the policy service.
            tokio::task::yield_now().await;
            let key = (principal.subject.clone(), access.action);
            if self.allowed.contains(&key) {
                Decision::Allow
            } else {
                Decision::Deny("oracle check failed".to_owned())
            }
        }
    }

    #[tokio::test]
    async fn external_async_oracle_authorizer() {
        let oracle = FakeOracle {
            allowed: [("alice".to_owned(), Action::Query)].into_iter().collect(),
        };
        let guard = Guard::new(Arc::new(AllowAnonymous), Arc::new(oracle));
        let alice = Principal::new("oidc", "alice");
        let bob = Principal::new("oidc", "bob");

        assert!(
            guard
                .authorize(&alice, &Access::on(Action::Query, "people"))
                .await
                .is_ok()
        );
        assert!(
            guard
                .authorize(&alice, &Access::on(Action::Transact, "people"))
                .await
                .is_err()
        );
        assert!(
            guard
                .authorize(&bob, &Access::on(Action::Query, "people"))
                .await
                .is_err()
        );
    }
}
