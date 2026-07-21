# Authentication & Authorization (spike)

Status: **spike / design exploration.** The prototype lives in
[`corium-protocol::authz`](../../crates/corium-protocol/src/authz.rs) with unit
tests; it is not yet wired into the servers. This document records the model, the
target integration, and the open questions. ADR-0012 records the decision to
build authz as a request-scoped, optional layer.

## Problem

The network surfaces — the transactor (`Transactor` + `Catalog` services) and
the peer server (`PeerServerService`) — ship with connection-level bearer-token
auth ([`corium-protocol::auth`](../../crates/corium-protocol/src/auth.rs)): an
`Authenticator` returns `bool`, one shared secret gates the whole endpoint, and
no identity reaches the handler. That is a correct v1 for a single-operator
deployment and nothing here removes it.

Three requirements push past it:

1. **Optional, layered enforcement.** Each surface can run wide open, require
   authentication only, or require authentication *and* per-operation
   authorization — a deployment choice, not a compile-time one.
2. **External identity providers on a shared, public system.** A transactor or
   peer server may be exposed to callers whose credentials are minted elsewhere
   (an OIDC issuer, an mTLS CA). Auth must *verify* such a credential and map it
   onto a local identity, not compare it to one static secret.
3. **Many users, different views, one server.** One transactor or peer server
   may serve several tenants at once, each seeing a different slice of the same
   database. Identity therefore has to be **per request**, not per connection,
   and authorization has to be able to narrow *what facts a caller sees*, not
   just *whether a call is allowed*.

## Model

Four small traits plus a `Guard` that bundles a chosen policy and an interceptor
that applies it. **Authn is synchronous, authz is asynchronous** — see
[Sync authn, async authz](#sync-authn-async-authz) for why.

```
credentials ──▶ IdentityProvider ──▶ Principal ──▶ Authorizer ──▶ Decision
 (metadata)      (authn: who?)        (identity)    (authz: may?)   Allow /
                                                                    AllowFiltered(ViewFilter) /
                                                                    Deny
```

### Principal — request-scoped identity

A `Principal` is a flat bag: `subject`, the `provider` that vouched for it,
`roles`, and free-form `claims` (e.g. `tenant`, `email`, `scope`). Every
provider produces the same shape, so the authorizer does not care whether an
identity came from a static token, an OIDC `sub`, or a certificate SAN. The
unauthenticated caller is a real value, `Principal::anonymous()`, so handlers
never juggle an `Option` — "auth is off" just means everyone is anonymous.

### IdentityProvider — authentication (optional)

`authenticate(&Credentials) -> Result<Option<Principal>, AuthError>` is
deliberately three-way, which is what makes providers **composable** on a shared
system:

- `Ok(Some(p))` — **accept**: these credentials are valid, here is the identity.
- `Ok(None)` — **abstain**: not my credentials; let the next provider try.
- `Err(_)` — **reject**: addressed to me but invalid (bad signature, expired);
  stop the chain.

Shipped providers:

| Provider | Role |
|---|---|
| `AllowAnonymous` | accepts everyone as anonymous (authn off) |
| `StaticTokens` | maps a fixed token → a fixed `Principal`; **abstains** on unknown tokens so it can sit ahead of other issuers |
| `ExternalTokens<V: TokenVerifier>` | adapts a `TokenVerifier` (the OIDC/JWT/mTLS seam) into a provider |
| `CompositeProvider` | tries a list in order; first accept wins, first reject stops |

`StaticTokens` *abstaining* rather than rejecting on an unknown token is the key
composition rule the spike's tests pinned down: it lets `[static-tokens, oidc]`
accept an internal service token *and* fall through to an OIDC token on the same
endpoint. The "nobody accepted" outcome — anonymous or rejection — is not the
provider's call; it belongs to the `Guard`.

`TokenVerifier` is a bare trait on purpose. A real JWT/OIDC verifier pulls in a
crypto/JWKS dependency and makes network calls to fetch keys; keeping that out
of `corium-protocol` (the lowest networked layer) means the seam is fixed now
and the dependency lands later, behind a feature flag, without disturbing the
wire crate. mTLS is the same shape: tonic surfaces the peer certificate on the
request, a verifier maps its SAN/CN to a `Principal`, and
`Credentials::client_cert_subject` carries it.

### Authorizer — authorization (optional)

`async fn authorize(&Principal, &Access) -> Decision`. An `Access` is an
`Action` (one per RPC family) on an optional target `database`. A `Decision` is:

- `Allow` — permit with full visibility.
- `AllowFiltered(Arc<dyn ViewFilter>)` — permit, but restrict what is returned.
- `Deny(reason)` — refuse.

Shipped authorizers: `AllowAll` (authz off) and `PolicyAuthorizer`, a
deny-by-default role→grant policy. A `Grant` names action *classes*
(`Read`/`Write`/`Admin`, so policies need not enumerate every action), a set of
databases (empty = any), and an optional `ViewFilter`.

### Sync authn, async authz

The two traits differ in sync-vs-async on purpose, following where the work
actually happens:

- **`IdentityProvider::authenticate` is synchronous.** The common providers are
  local verification: compare a static token, check a JWT signature against
  cached keys, read an mTLS subject. It also runs inside the tonic
  `Interceptor`, whose `call` is synchronous — keeping authn sync lets it stay
  in the interceptor and attach the `Principal` before dispatch. The one authn
  case that is genuinely networked, OIDC token *introspection*, is handled the
  way production middleware does: verify against cached state and refresh out of
  band, so `authenticate` remains a cheap lookup. If a deployment truly needs
  per-request introspection with no cache, authn moves out of the interceptor
  into an async `tower` layer or the handler prologue — but that is the
  exception, not the default.
- **`Authorizer::authorize` is asynchronous.** The interesting authorizers call
  an external policy oracle: a relationship-based service such as **OpenFGA** or
  **Auth0 FGA** answers a per-decision `Check(user, relation, object)` over the
  network, and modelling that as a blocking call would stall a runtime worker.
  Authorization already runs handler-side inside the async RPC, so `.await`
  costs nothing structurally. Local authorizers (`AllowAll`, `PolicyAuthorizer`)
  simply return without awaiting; the async signature does not make them slower.

Because `Authorizer` is itself the async seam, an external oracle needs no extra
trait — it implements `Authorizer` directly and awaits inside. The spike's
`external_async_oracle_authorizer` test demonstrates a `Check`-style oracle
plugged into a `Guard` via `Arc<dyn Authorizer>`.

### ViewFilter — different views, one server

This is how two tenants read the same database and see different facts. When an
authorizer returns `AllowFiltered`, the read paths consult the filter before
returning data. The spike defines the cheapest useful cut — **attribute-level
visibility** (`AttributeAllowlist`), enforceable directly in the peer's datom
scan — and leaves entity-level and value-predicate filtering as further
implementations of the same trait. Filtering *what a legitimate reader sees* is
distinct from *whether the read is allowed*; both live in the authorizer so the
policy is in one place.

### Guard — the chosen policy

`Guard` bundles an `IdentityProvider` and an `Authorizer` and owns the
"nobody authenticated" decision via `allow_anonymous`:

- `Guard::disabled()` — anonymous provider + allow-all; behaviourally identical
  to running with no auth. This is the default every surface keeps.
- `Guard::new(provider, authorizer)` — requires authentication (unrecognized or
  absent credentials are rejected).
- `.allow_anonymous(true)` — optional authn: unrecognized credentials fall back
  to anonymous, and the authorizer still runs (anonymous simply holds no roles,
  so "public read, authenticated write" is just a policy).

One `Guard` serves every request on a surface; because identity is derived per
request, a single guard handles many concurrent callers with different authority
— the multi-tenant requirement, demonstrated in
`one_guard_serves_two_tenants_with_different_authority`.

## Integration with the RPC surface

`IdentityInterceptor` replaces `AuthInterceptor` at the tonic layer. It runs per
request, authenticates the metadata, and — crucially — inserts the `Principal`
into the request **extensions**, where the handler reads it with
`authz::principal(&request)`. The interceptor does authn only; authorization
happens in the handler, which is the first place that knows the concrete
`Action` and database.

The proposed handler shape (peer server `query` as the example):

```rust
let principal = authz::principal(&request);
let access = Access::on(Action::Query, &spec.db);
let view = self.guard.authorize(&principal, &access).await?; // Option<Arc<dyn ViewFilter>>
// ... run the query, then apply `view` to the returned datoms/rows ...
```

Action mapping:

| RPC | Action |
|---|---|
| `PeerServerService.Query` / `Pull` / `Datoms` | `Query` / `Pull` / `Datoms` |
| `PeerServerService.TxRange` / `Subscribe` / `DbStats` | `TxRange` / `Subscribe` / `Inspect` |
| `PeerServerService.Transact`, `TransactorService.Transact` | `Transact` |
| `Catalog.CreateDatabase` / `DeleteDatabase` / `ForkDatabase` | `CreateDatabase` / … |
| `Catalog.ListDatabases` | `ListDatabases` (catalog-wide, no database) |
| `Catalog.GcDeletedDatabases` | `GarbageCollect` |
| `Catalog.RequestIndex` / `SetIndexPolicy` | `ManageIndex` |

Enforcement points differ by surface:

- **Peer server** is where per-view authz matters most: queries, pulls, and
  datom scans run server-side against local `Db` values, so a `ViewFilter` can
  be applied to results before they leave the process.
- **Transactor** authz is coarser — mostly action/database gating on transact
  and catalog operations. It has no query surface to filter.
- **Embedded transport** (`corium-peer` → `corium-transactor` in-process) runs
  no interceptor; `authz::principal` returns anonymous, and a `Guard::disabled`
  keeps the single-process path exactly as it is today.

## Migration from the bool authenticator

The shipped `auth::Authenticator`/`StaticToken`/`AuthInterceptor` stay until the
servers adopt `Guard`. The migration is mechanical: `StaticToken(Some(secret))`
becomes a `Guard::new(StaticTokens::with(secret, principal), AllowAll)`, and
`StaticToken(None)` becomes `Guard::disabled()`. The CLI's `--serve-token`
maps to the former; new flags (`--auth-config`, an OIDC issuer URL, a token
file) select richer guards. The wire protocol does not change — credentials
still travel as `authorization: Bearer …` metadata and the same gRPC status
codes (`UNAUTHENTICATED`, `PERMISSION_DENIED`) are returned — so existing thin
clients keep working; only what the server does with the token changes.

## Open questions

- **Where does policy live?** For a shared public system the role→grant map and
  view definitions probably want to be data, not code — a config file, or
  eventually facts in a control database the transactor reads. The spike hard-codes
  policies in Rust; externalizing them is the next design step.
- **View filtering in the query engine.** `AttributeAllowlist` is enforceable in
  the datom scan, but a `ViewFilter` that hides *entities* or filters by *value*
  needs a hook inside `corium-query` (a predicate injected into the executor),
  and interacts with the query cache (cache keys must include the filter). This
  is the largest downstream change and is out of scope for the spike.
- **Auditing.** Every authz decision is a natural audit event; the `Principal`
  and `Access` are exactly what a log line needs. Not prototyped.
- **mTLS subject extraction.** Needs the tonic peer-certificate plumbing to fill
  `Credentials::client_cert_subject` inside the service; the field exists, the
  wiring does not.
- **Token caching.** OIDC verification (JWKS fetch, signature check) should be
  cached per token/expiry so it stays off the hot path; the `TokenVerifier` impl
  owns this.

## What the spike delivers

- `corium-protocol::authz`: `Principal`, `Credentials`, `IdentityProvider`
  (+ `AllowAnonymous`, `StaticTokens`, `ExternalTokens`, `CompositeProvider`),
  `TokenVerifier`, `Action`/`Access`, `Authorizer` (+ `AllowAll`,
  `PolicyAuthorizer`/`Grant`), `ViewFilter` (+ `AttributeAllowlist`), `Decision`,
  `Guard`, and `IdentityInterceptor`.
- Unit tests covering each requirement: optional-off, distinct static-token
  identities, the external-verifier seam, provider composition, role/database
  enforcement, an async external-oracle authorizer, per-principal views,
  interceptor extension propagation, and one guard serving two tenants with
  different authority.
- No change to the shipped `auth` module, the wire protocol, or server wiring.
