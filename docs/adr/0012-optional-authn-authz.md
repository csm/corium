# ADR-0012: Optional, request-scoped authentication and authorization

**Status:** Accepted (2026-07-21); implemented in
[`corium-protocol::authz`](../../crates/corium-protocol/src/authz.rs) with the
design in [`docs/design/auth.md`](../design/auth.md). The transactor and peer
server enforce it; [ADR-0014](0014-self-hosted-rebac-authz.md) fills in the
`Authorizer` seam with a self-hosted relationship policy.

## Context

The network surfaces ship with connection-level bearer-token auth
([ADR-0006](0006-grpc-protocol.md)): `auth::Authenticator` returns `bool`, one
shared secret gates an endpoint, and no identity reaches the handler. That
suits a single-operator deployment but cannot express three things we want to
leave the door open for: (1) enforcement that is *optional and layered* per
surface (open / authn / authn+authz) as a deployment choice; (2) *external*
identity providers (OIDC, mTLS) on a shared, public system, where a credential
is minted elsewhere and must be verified and mapped to a local identity; and
(3) *many users with different views on one server*, which requires identity to
be per-request, not per-connection, and authorization that can narrow which
facts a caller sees. Baking authz in later as connection-scoped would repeat
the HA mistake ADR-0010 avoided — a connection-bound assumption that forces a
rewrite.

## Decision

Model auth as a request-scoped layer of small traits in `corium-protocol`, kept
separate from the shipped `auth` module:

- **`IdentityProvider`** (authn) turns `Credentials` into a `Principal` with a
  three-way result — accept / abstain / reject — so providers compose:
  static tokens, an `ExternalTokens` adapter over a `TokenVerifier` seam
  (OIDC/JWT/mTLS, implemented later behind a feature flag), and a
  `CompositeProvider` chain.
- **`Authorizer`** (authz) maps `(Principal, Access)` to a `Decision` of
  `Allow` / `AllowFiltered(ViewFilter)` / `Deny`. `PolicyAuthorizer` is a
  deny-by-default role→grant policy; `ViewFilter` is the per-principal view
  seam (attribute-level in the spike). This trait is **async** so it can
  consult an external policy oracle (OpenFGA / Auth0 FGA `Check`);
  `IdentityProvider` stays **sync** because authn is local verification that
  runs in the synchronous tonic interceptor.
- **`Guard`** bundles a chosen provider + authorizer and owns the
  anonymous-vs-reject decision. `Guard::disabled()` is the default and is
  behaviourally identical to today's no-auth path.
- **`IdentityInterceptor`** authenticates per request and stashes the
  `Principal` in request extensions; handlers authorize once they know the
  concrete action.

The wire protocol is unchanged: credentials stay `authorization: Bearer …`
metadata, failures stay `UNAUTHENTICATED` / `PERMISSION_DENIED`. The bool
`Authenticator` remains until servers adopt `Guard`; the migration is
mechanical.

## Consequences

- Identity is per request, so one transactor or peer server serves many
  tenants concurrently, each with its own authorization and view — no
  connection-bound assumption to unwind later.
- Optionality is a `Guard` choice, not a build flag: open, authn-only, and
  authn+authz are all the same code path with different policy objects; the
  single-process embedded transport keeps running with `Guard::disabled`.
- The `TokenVerifier` seam fixes the external-provider boundary now while
  deferring the crypto/JWKS dependency and its network calls out of the lowest
  wire crate.
- The async `Authorizer` supports a networked policy oracle without blocking a
  runtime thread and needs no extra seam trait (an oracle implements
  `Authorizer` directly). The cost is a boxed future per decision — via
  `#[tonic::async_trait]`, already used across the servers — even for local
  authorizers; acceptable since authz runs once per RPC, not in a tight loop.
- Real per-tenant *view* filtering beyond attribute visibility needs a hook in
  `corium-query` (executor predicate + query-cache key), which is the largest
  deferred piece; the spike proves the seam without paying for it.
- Two overlapping auth surfaces (`auth` bool + `authz` principal) exist until
  the servers migrate — a deliberate, temporary cost the design doc tracks.
