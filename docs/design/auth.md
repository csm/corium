# Authentication & Authorization

Status: **wired in (permissive default).** The model lives in
[`corium-protocol::authz`](../../crates/corium-protocol/src/authz.rs); the
transactor and peer server enforce it through an
[`IdentityInterceptor`](../../crates/corium-protocol/src/authz.rs) plus a
per-handler authorization check, and the CLI builds the [`Guard`] each server
runs from its flags. A concrete OIDC/JWT identity provider ships in
[`corium-protocol::oidc`](../../crates/corium-protocol/src/oidc.rs) behind the
`oidc` feature. Authorization is permit-all ([`AllowAll`]) by default — the seam
is in place for a richer authorizer later. ADR-0012 records the decision to
build authz as a request-scoped, optional layer. This document records the
model, the wiring, and the open questions.

## Defaults: frictionless local, one shared secret

Every `corium` CLI program defaults its bearer token to a single shared
development secret, `corium_protocol::auth::DEFAULT_DEV_TOKEN`, so a local
database needs no auth configuration: start a transactor, create a database, and
open a console without passing a credential flag. A server started with no auth
flags recognizes that token *and* also admits anonymous callers, so nothing has
to line up for local use.

- **Client token** (`--token`, or `CORIUM_TOKEN`): defaults to the shared
  secret; `--token ""` connects anonymously; set a different value everywhere to
  use another secret.
- **Server auth** (`ServeFlags`): permissive by default (recognizes the shared
  secret, admits anonymous). `--serve-token <secret>` (or `CORIUM_SERVE_TOKEN`)
  requires that exact token and rejects everything else; `--require-auth`
  requires the shared secret without changing it; `--serve-open` disables auth;
  `--oidc-issuer <url>` (with `--oidc-audience`) adds OIDC and switches to strict
  mode.

The shared secret exists to make experimentation frictionless, **not** to
secure anything: never expose a surface that accepts it to a network anyone else
can reach.

## Problem

The network surfaces — the transactor (`Transactor` + `Catalog` services) and
the peer server (`PeerServerService`) — ship with connection-level
bearer-token auth
([`corium-protocol::auth`](../../crates/corium-protocol/src/auth.rs)): an
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

- `Ok(Some(p))` — **accept**: these credentials are valid, here is the
  identity.
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
endpoint. The "nobody accepted" outcome — anonymous or rejection — is not
the provider's call; it belongs to the `Guard`.

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
- `AllowFiltered(Arc<dyn ViewFilter>)` — permit, but restrict what is
  returned.
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

- `Guard::disabled()` — anonymous provider + allow-all; behaviourally
  identical to running with no auth. This is the default every surface keeps.
- `Guard::new(provider, authorizer)` — requires authentication (unrecognized
  or absent credentials are rejected).
- `.allow_anonymous(true)` — optional authn: unrecognized credentials fall
  back to anonymous, and the authorizer still runs (anonymous simply holds no
  roles, so "public read, authenticated write" is just a policy).

One `Guard` serves every request on a surface; because identity is derived per
request, a single guard handles many concurrent callers with different
authority — the multi-tenant requirement, demonstrated in
`one_guard_serves_two_tenants_with_different_authority`.

## Integration with the RPC surface

`IdentityInterceptor` replaces `AuthInterceptor` at the tonic layer. It runs
per request, authenticates the metadata, and — crucially — inserts the
`Principal` into the request **extensions**, where the handler reads it with
`authz::principal(&request)`. The interceptor does authn only; authorization
happens in the handler, which is the first place that knows the concrete
`Action` and database.

The proposed handler shape (peer server `query` as the example):

```rust
let principal = authz::principal(&request);
let access = Access::on(Action::Query, &spec.db);
// view: Option<Arc<dyn ViewFilter>>
let view = self.guard.authorize(&principal, &access).await?;
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

Done for the servers: `corium-transactor::server::serve` and
`corium-peer::server::{serve, serve_service}` now take a `Guard` and install an
`IdentityInterceptor` instead of the bool `AuthInterceptor`. `StaticToken(None)`
became `Guard::disabled()`, and `--serve-token <secret>` became a
`Guard::new(StaticTokens::with(secret, principal), AllowAll)` (anonymous
disabled). The client side still attaches the token with
`auth::TokenInterceptor`, and the shipped `auth::Authenticator`/`StaticToken`
types remain for out-of-tree embedders — the servers no longer use them.

The wire protocol did not change — credentials still travel as
`authorization: Bearer …` metadata and the same gRPC status codes
(`UNAUTHENTICATED`, `PERMISSION_DENIED`) come back — so existing thin clients
keep working; only what the server does with the token changed.


## Self-hosted ReBAC authorization database

Corium's data model is itself a good fit for relationship-based authorization
(ReBAC): subjects, resources, groups, tenants, and permissions are naturally
represented as entities and reference attributes. Rather than requiring every
deployment to run an external policy service such as OpenFGA, Corium should be
able to host its own small **system authorization database** and answer authz
questions by querying that database from the transactor and peer servers.
External authorizers remain possible through the `Authorizer` trait, but the
first-class self-hosted path keeps single-cluster deployments operationally
simple.

### System database

A deployment may mark one database as the authorization database (for example,
`_corium/authz`). It is an ordinary Corium database with a reserved schema and
normal transaction history, but it is opened through a control-plane handle with
additional safeguards:

- Only operators or principals granted `authz/admin` may transact policy data.
- User transactions to application databases cannot modify the system authz
  database by accident; catalog configuration names it explicitly.
- Policy reads are snapshot reads. An authz decision records the authz basis `t`
  it used so decisions can be audited and reproduced.
- The authz database can be backed up, restored, replicated, and inspected with
  the same machinery as any other Corium database.

The system database should stay intentionally small. It contains identities,
resource relationships, permission derivation rules, and view-filter bindings —
not high-volume application facts. Application databases may mirror stable
resource identifiers into authz tuples when needed, but the hot data path should
not require joining against arbitrary application facts for every request.

### Schema shape

The initial schema should be tuple-oriented so it can express common ReBAC
models directly and can be imported from or exported to OpenFGA-like systems if
needed:

| Entity | Required attributes | Meaning |
|---|---|---|
| Principal | `:authz.principal/id`, `:authz.principal/provider`, optional `:authz.principal/role` | Local subject produced by `IdentityProvider` mapping. |
| Object | `:authz.object/type`, `:authz.object/id`, optional `:authz.object/database` | Protected database, tenant, collection, entity, or synthetic control-plane object. |
| Tuple | `:authz.tuple/subject`, `:authz.tuple/relation`, `:authz.tuple/object` | Relationship fact such as `alice member group:eng` or `group:eng writer db:music`. |
| Permission | `:authz.permission/object-type`, `:authz.permission/action`, `:authz.permission/relation` | Maps a Corium `Action` to one or more relations that satisfy it. |
| Rewrite | `:authz.rewrite/relation`, `:authz.rewrite/via-relation`, `:authz.rewrite/on-relation` | Optional derived relation edge, e.g. `viewer` through `parent` to a tenant. |
| View | `:authz.view/name`, `:authz.view/filter-type`, filter parameters | A reusable `ViewFilter` definition returned with `AllowFiltered`. |
| Binding | `:authz.binding/relation`, `:authz.binding/object`, `:authz.binding/view` | Attaches a view filter to a successful relation. |

The tuple model intentionally mirrors the check shape:

```text
Check(subject, action, object, database, at_authz_t)
  action -> required relation(s)
  subject --relation/derived-relation--> object
  optional binding -> ViewFilter
```

Concrete relation names are data, not Rust enums. The built-in action classes
(`Read`, `Write`, `Admin`) remain the coarse API contract, while deployments can
define relations such as `owner`, `writer`, `viewer`, `member`, `parent`, and
`impersonator` in policy data.

### Query and evaluation model

The self-hosted implementation should be an `Authorizer` named something like
`SystemDbAuthorizer`. It receives `(Principal, Access)` and evaluates a bounded
ReBAC query against a snapshot of the authz database:

1. Map the request `Principal` to one or more subject object ids. OIDC groups or
   static roles may be materialized as tuples, but request claims can also add
   ephemeral subjects (for example, `provider:oidc/group:eng`) for the duration
   of the check.
2. Map `Access` to a target object. Database-scoped actions target
   `database:<name>`; catalog-wide actions target reserved objects such as
   `catalog:*`; future entity-level checks can target `entity:<db>/<eid>`.
3. Resolve the configured action to candidate relations, then search
   relationship tuples and rewrites with bounded recursion. The evaluator must
   enforce maximum depth, maximum visited nodes, and cycle detection.
4. Return `Allow`, `AllowFiltered(view)`, or `Deny`. If multiple successful
   paths produce filters, combine them conservatively: the effective filter is
   the intersection of visibility constraints, unless a policy explicitly marks
   one relation as unfiltered.

The first production version can support database-level checks and
attribute-allowlist views only. Entity-level checks and predicate views need the
query-engine hooks already called out below.

### Caching and invalidation

Because policy data is expected to be much smaller and less frequently changed
than application data, peer servers and transactors should cache aggressively:

- Maintain an in-memory, immutable compiled policy snapshot keyed by authz
  database basis `t`. The compiled form should include tuple indexes by
  `(object, relation)`, subject memberships, action-to-relation mappings, and
  prebuilt `ViewFilter` values.
- Subscribe to the authz database log or poll its latest basis `t`. When `t`
  advances, build the next compiled snapshot off the request path and atomically
  swap it in.
- Negative and positive check-result caches may be layered on top, keyed by
  `(principal fingerprint, action, object, authz_t)`. Including `authz_t` makes
  invalidation trivial: advancing policy basis naturally misses old entries.
- Fail closed when the configured authz database cannot be loaded, except for an
  explicit break-glass configuration intended only for operator recovery.

This keeps the common path local and lock-free: authenticate the request, read
the current compiled authz snapshot, perform a bounded graph walk in memory, and
return a decision.

### Consistency choices

Authorization consistency should be explicit per deployment or per surface:

- **Pinned snapshot (default):** all checks use the latest compiled authz basis
  known to that process. This is fastest and gives monotonic local updates after
  cache refresh, but propagation to all peers is eventually consistent.
- **Require fresh basis:** sensitive control-plane operations may require the
  server to observe at least a specified authz `t` before deciding, returning a
  retryable error if it is behind.
- **Transactor-owned checks:** writes can be authorized by the transactor using
  its local authz snapshot, making write admission consistent at the serialization
  point even if read-serving peers lag slightly.

Audit entries should include the principal, action, target, decision, authz
basis `t`, matched relation path when available, and filter name.

### External compatibility

Self-hosted ReBAC does not remove the external-oracle seam. Deployments can
still implement `Authorizer` with OpenFGA/Auth0 FGA, or run a hybrid mode where
Corium's system database is the source of truth and a background exporter feeds
an external service. Keeping the schema tuple-shaped makes that bridge
straightforward while allowing Corium-only deployments to avoid another
service, another consistency boundary, and another cache layer.

## Open questions

- **Bootstrapping and operating the system authz database.** The ReBAC section
  proposes a control database, but the concrete bootstrap flow is still open:
  how an operator creates the first `authz/admin`, how break-glass access is
  represented, and how catalog configuration pins the database identity across
  backup/restore and fork operations.
- **View filtering in the query engine.** `AttributeAllowlist` is enforceable
  in the datom scan, but a `ViewFilter` that hides *entities* or filters by
  *value* needs a hook inside `corium-query` (a predicate in the executor),
  and interacts with the query cache (cache keys must include the filter). This
  is the largest downstream change and is out of scope for the spike.
- **Auditing.** Every authz decision is a natural audit event; the `Principal`
  and `Access` are exactly what a log line needs. Not prototyped.
- **mTLS subject extraction.** Needs the tonic peer-certificate plumbing to
  fill `Credentials::client_cert_subject` inside the service; the field exists,
  the wiring does not.
- **Token caching.** OIDC verification (JWKS fetch, signature check) should be
  cached per token/expiry so it stays off the hot path; the `TokenVerifier`
  impl owns this.

## What is implemented

- `corium-protocol::authz`: `Principal`, `Credentials`, `IdentityProvider`
  (+ `AllowAnonymous`, `StaticTokens`, `ExternalTokens`, `CompositeProvider`),
  `TokenVerifier`, `Action`/`Access`, `Authorizer` (+ `AllowAll`,
  `PolicyAuthorizer`/`Grant`), `ViewFilter` (+ `AttributeAllowlist`),
  `Decision`, `Guard`, and `IdentityInterceptor`.
- `corium-protocol::oidc` (feature `oidc`): `OidcVerifier`, a concrete
  `TokenVerifier` that validates RSA-signed (`RS256`/`RS384`/`RS512`) JWTs
  against a JWKS and maps `sub`/roles/claims onto a `Principal`, with issuer,
  audience, and expiry checks. `OidcVerifier::from_discovery` (feature
  `oidc-discovery`) fetches the issuer's JWKS over HTTP; `from_jwks_json` builds
  it offline. Unit tests sign and verify real tokens against an embedded test
  key and cover tampering, expiry, wrong issuer/audience, and unsupported
  algorithms.
- Server wiring: the transactor (`Transactor` + `Catalog`) and peer server
  authenticate every request in the interceptor and authorize the concrete
  `Access` in each handler. Authorization defaults to `AllowAll`.
- CLI: a shared development token defaulted across every program, `ServeFlags`
  that build the server `Guard` (`--serve-token`, `--require-auth`,
  `--serve-open`, `--oidc-issuer`/`--oidc-audience`/`--oidc-jwks-file`), and
  `CORIUM_TOKEN` / `CORIUM_SERVE_TOKEN` environment overrides.
- Unit tests covering each model requirement: optional-off, distinct
  static-token identities, the external-verifier seam, provider composition,
  role/database enforcement, an async external-oracle authorizer, per-principal
  views, interceptor extension propagation, and one guard serving two tenants
  with different authority.
