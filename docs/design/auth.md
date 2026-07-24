# Authentication & Authorization

Status: **wired in (permissive default), with a self-hosted authorizer.** The
model lives in [`corium-protocol::authz`](../../crates/corium-protocol/src/authz.rs);
the transactor and peer server enforce it through an
[`IdentityInterceptor`](../../crates/corium-protocol/src/authz.rs) plus a
per-handler authorization check, and the CLI builds the [`Guard`] each server
runs from its flags. A concrete OIDC/JWT identity provider ships in
[`corium-protocol::oidc`](../../crates/corium-protocol/src/oidc.rs) behind the
`oidc` feature. Authorization is permit-all ([`AllowAll`]) by default;
`--authz-db <name>` switches it to the relationship-based (ReBAC) authorizer in
[`corium-authz`](../../crates/corium-authz/src/lib.rs), which answers from
policy stored in an ordinary Corium database. ADR-0012 records the decision to
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

Status: **implemented** in [`corium-authz`](../../crates/corium-authz/src/lib.rs).

Corium's data model is itself a good fit for relationship-based authorization
(ReBAC): subjects, resources, groups, tenants, and permissions are naturally
represented as entities and reference attributes. Rather than requiring every
deployment to run an external policy service such as OpenFGA, Corium hosts its
own small **system authorization database** and answers authz questions from a
compiled snapshot of it, in process, on the transactor and peer servers.
External authorizers remain possible through the `Authorizer` trait; the
self-hosted path keeps single-cluster deployments operationally simple.

The check has the shape the tuple model implies:

```text
Check(subject, action, object, database, at_authz_t)
  action  -> required relation(s)         (permission map)
  subject --relation/derived--> object    (tuples + rewrites)
  optional binding -> ViewFilter          (bindings + views)
```

### System database

One database holds policy — `corium_authz` by default (`DEFAULT_AUTHZ_DB`;
database names are restricted to `[A-Za-z0-9_-]`, so the reserved name spells
`_corium/authz` as `corium_authz`). It is an ordinary Corium database with a
reserved schema and normal transaction history:

- Policy reads are snapshot reads. Every decision records the authz basis `t`
  it was made against, so a decision can be audited and reproduced by reading
  the authz database `as-of` that `t`.
- The authz database is backed up, restored, forked, replicated, and inspected
  with exactly the same machinery as any other database — that is the point of
  keeping it ordinary.
- Because it *is* a database, access to it is itself authorized by the policy
  it contains: `corium authz init` grants the administrator `owner` on
  `database:*`, which covers `database:corium_authz`. Nothing else can read or
  write policy.

Keep it small. It holds identities, relationships, permission derivation rules,
and view bindings — not application facts. Application databases may mirror
stable resource identifiers into authz tuples, but the hot path never joins
against arbitrary application data.

### Schema shape

The schema is tuple-oriented so it expresses common ReBAC models directly and
can be imported from, or exported to, an OpenFGA-like system. Every attribute
is a string (or boolean), and policy entities refer to each other by *name*
rather than by entity ref, so a policy fragment transacts without first
resolving what it points at.

| Entity | Attributes | Meaning |
|---|---|---|
| Principal | `:authz.principal/id`, `:authz.principal/provider`, `:authz.principal/role` | Registers a subject id, optionally pinned to the provider that may vouch for it, with roles granted by policy. |
| Object | `:authz.object/type`, `:authz.object/id`, `:authz.object/database` | Names a protected object; `:authz.object/database` makes it an additional target for accesses on that Corium database. |
| Tuple | `:authz.tuple/subject`, `:authz.tuple/relation`, `:authz.tuple/object` | The relationship fact, e.g. `user:alice member group:eng`. |
| Permission | `:authz.permission/object-type`, `:authz.permission/action`, `:authz.permission/relation` | Maps an action (or action class) on an object type onto the relations that satisfy it. |
| Rewrite | `:authz.rewrite/relation`, `:authz.rewrite/via-relation`, `:authz.rewrite/on-relation`, `:authz.rewrite/object-type` | Derived relation: `relation` holds on an object when `on-relation` holds on whatever its `via-relation` points at. |
| View | `:authz.view/name`, `:authz.view/filter-type`, `:authz.view/attribute` | A reusable `ViewFilter` (`attribute-allowlist` / `attribute-denylist`). |
| Binding | `:authz.binding/relation`, `:authz.binding/object`, `:authz.binding/view`, `:authz.binding/unfiltered` | Attaches a view to a successful relation, or marks that relation as granting full visibility. |

Names follow a `type:id` convention. `type:*` is a wildcard: `database:*` is
every database, `user:*` is every caller (including anonymous),
`authenticated:*` is every caller who authenticated. A subject may also name a
**userset**, `object#relation` — `group:eng#member` is "everyone with `member`
on `group:eng`" — and the shorthand `group:eng` expands through the configured
membership relations (`member` by default) so both spellings work.

Concrete relation names are data, not Rust enums. The built-in action classes
(`Read`, `Write`, `Admin`) remain the coarse API contract, and a permission may
name an exact action (`query`, `transact`, `create-database`), a class
(`read`, `write`, `admin`), or `*`.

### Query and evaluation model

[`SystemDbAuthorizer`](../../crates/corium-authz/src/authorizer.rs) implements
`Authorizer`. It receives `(Principal, Access)` and evaluates a bounded ReBAC
query against a compiled snapshot:

1. **Subjects.** The principal becomes a set of subject objects:
   `user:<id>` and the provider-qualified `user:<provider>/<id>`,
   `authenticated:<id>` when not anonymous, `role:<r>` for each role (from the
   credential *and* from a principal registration), and claim-derived subjects
   (`groups` → `group:eng`, `tenant` → `tenant:acme`). A principal registered
   against a different provider does not get the bare `user:<id>` form, so one
   issuer cannot mint an identity another issuer's tuples were written for.
2. **Objects.** A database-scoped access targets `database:<name>` plus any
   object registered against that database; a catalog-wide access targets
   `catalog:*`. An *admin* action on a database also targets the catalog
   object, so catalog ownership covers creating and deleting databases that
   have no object of their own yet.
3. **Relations.** The permission map resolves the action to candidate
   relations, widening from the exact action name to its class to `*`, each
   against the object's own type and `*`.
4. **Search.** A breadth-first walk over `(relation, object)` goals follows
   tuples, usersets, group expansion, and rewrites. It is bounded by maximum
   depth (8), maximum visited goals (10,000), and a visited set that doubles as
   cycle detection — policy data is user-authored, so a `parent` loop must cost
   a bounded amount, not hang a request. Exhausting the budget is reported
   distinctly from "no path", because it is an operational signal rather than a
   policy answer.
5. **Decision.** `Allow`, `AllowFiltered(view)`, or `Deny(reason)`. When
   several relations succeed, their bound views **intersect** — holding one
   more relation never reveals more than holding it alone — unless a binding is
   explicitly marked `:authz.binding/unfiltered`, the documented escape hatch
   for relations like `owner` that must see everything.

Denials carry the reason (`no permission maps action …`, `no relationship path
grants …`), and grants carry the matched path
(`database:music#writer -> group:eng#member -> user:bob`), which is what
`corium authz check` prints and what the audit line records.

The first version supports database- and catalog-level checks with
attribute-level views. Entity-level checks and predicate views need the
query-engine hooks called out under [Open questions](#open-questions).

### Caching and invalidation

Policy is small and changes rarely, so the surfaces cache aggressively:

- An immutable compiled snapshot ([`Policy`](../../crates/corium-authz/src/policy.rs))
  keyed by the authz database's basis `t`, holding tuples indexed by
  `(object, relation)`, the action-to-relation map, rewrites, and prebuilt
  `ViewFilter` values.
- A refresh task waits on the source's change signal — the transactor's basis
  watch, a peer connection's tx-report broadcast — recompiles **off the request
  path**, and swaps the new snapshot in atomically. A snapshot that fails to
  compile never replaces the last good one: the process keeps deciding from the
  policy it has, and logs.
- A check-result cache keyed by `(principal fingerprint, action, object,
  authz_t)`. Including `authz_t` makes invalidation free — advancing the policy
  basis drops every entry under the old one.
- **Fail closed.** No policy means no access. The only exception is an explicit
  break-glass configuration (`--authz-break-glass-role`) for operator recovery,
  which applies when the policy cannot be *read* — never to override a policy
  that denies — and is audited at `warn` every time it grants.

The common path is therefore local and lock-light: authenticate in the
interceptor, read the current snapshot pointer, walk it in memory, decide.

### Consistency choices

- **Pinned snapshot (default).** Checks use the newest compiled basis this
  process holds. Fastest; propagation across a fleet is eventually consistent
  (in practice, as fast as the change signal — the end-to-end test asserts a
  new grant takes effect on a running transactor without a restart).
- **Require fresh basis.** `--authz-fresh-writes` makes write and admin actions
  re-read the source before deciding, so a control-plane change is never
  decided from a stale snapshot. Reads stay pinned. A fresh check that cannot
  reach the source denies rather than falling back to the pinned snapshot —
  the caller asked for the newest policy precisely because stale is not good
  enough.
- **Transactor-owned checks.** The transactor authorizes writes from its own
  local snapshot of the database it leads, so write admission is consistent at
  the serialization point even when read-serving peers lag.

Audit events ([`corium_authz::audit`](../../crates/corium-authz/src/audit.rs))
carry the principal, provider, action, target object, decision, authz basis
`t`, matched relation path, and view names. The default sink is `tracing` under
the `corium_authz::audit` target — denials at `info`, grants at `debug` — and
`AuditSink` lets a deployment route them elsewhere.

### Operating it

```sh
# 1. Create the policy database (against a transactor started without --authz-db).
corium authz init --admin alice --provider oidc

# 2. Write policy.
corium authz grant alice owner catalog:*
corium authz grant 'group:eng#member' writer database:music
corium authz grant bob member group:eng

# 3. Ask what it decides, before enforcing it.
corium authz check bob transact --database music
# {:decision :allow … :path "database:music#writer -> group:eng#member -> user:bob"}

# 4. Enforce it.
corium transactor  --data-dir … --authz-db corium_authz
corium peer-server --db music    --authz-db corium_authz
```

`corium authz init` installs the reserved schema and the default permission map
(`read`→`viewer`/`writer`/`owner`, `write`→`writer`/`owner`, `admin`→`owner`,
and the same for `catalog`), then grants a first administrator `owner` on
`catalog:*` and `database:*`. That administrator defaults to the identity a
`--serve-token` client presents (`operator`, pinned to `static-token`), so the
CLI that created the database can still administer it once enforcement is on;
`--admin`/`--provider` point it at a real identity, and `--no-admin` grants
nobody anything. `corium authz status` prints the compiled basis and entity
counts; `corium authz revoke` retracts a tuple.

Bootstrap is deliberately a two-step: create the policy, *then* enable
`--authz-db`. A server started with `--authz-db` before the database exists does
not fail to start — it denies every request (fail closed), logs the remedy, and
recovers on its own once the database appears.

### External compatibility

Self-hosted ReBAC does not remove the external-oracle seam. Deployments can
still implement `Authorizer` with OpenFGA/Auth0 FGA, or run a hybrid mode where
Corium's system database is the source of truth and a background exporter feeds
an external service. Keeping the schema tuple-shaped makes that bridge
straightforward while allowing Corium-only deployments to avoid another
service, another consistency boundary, and another cache layer.


## Open questions

- **View filtering in the query engine.** This is now the gap that matters: an
  authorizer *can* return `AllowFiltered`, but no read path applies a filter
  yet, so the peer server rejects a filtered decision with `UNIMPLEMENTED`
  rather than returning unfiltered data. Attribute-level visibility is
  enforceable in the datom scan; a `ViewFilter` that hides *entities* or
  filters by *value* needs a hook inside `corium-query` (a predicate in the
  executor) and interacts with the query cache (cache keys must include the
  filter). Until then, policies that bind views are expressible and testable
  but not servable.
- **Pinning the authz database across fork and restore.** The database is named
  by configuration (`--authz-db`), not recorded in the catalog, so a restore
  under a different name or a fork of the policy database is an operator
  concern rather than something the catalog enforces.
- **Cross-process policy propagation lag.** Each surface refreshes from its own
  change signal, so two peers can briefly decide from different bases.
  `--authz-fresh-writes` closes this for control-plane actions; a
  `require at least authz_t` request option for readers is not implemented.
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
- `corium-authz`: the self-hosted ReBAC authorizer. `schema` (the reserved
  attributes and the EDN that installs them), `model` (objects, tuples,
  permissions, rewrites, views, bindings), `policy::Policy` (the compiled
  snapshot and its indexes), `eval` (the bounded, cycle-safe search),
  `subject` (principal → subjects), `view` (filters and their conservative
  combination), `source::PolicySource` (+ `MemoryPolicySource`), `audit`
  (`AuditEvent`/`AuditSink`/`TracingAudit`), `bootstrap` (in-process policy
  databases and the grant/revoke transaction forms), and `SystemDbAuthorizer`
  with snapshot caching, a refresh task, result caching, consistency modes,
  fail-closed behaviour, and break-glass.
- Policy sources per surface: `corium_transactor::authz::NodePolicySource`
  (the node's own `DbState`, woken by its basis watch) and
  `corium_peer::authz::ConnectionPolicySource` (a second peer connection, woken
  by its tx-report broadcast).
- Server wiring: the transactor (`Transactor` + `Catalog`) and peer server
  authenticate every request in the interceptor and authorize the concrete
  `Access` in each handler. Authorization defaults to `AllowAll`, or to the
  relationship policy when `--authz-db` names one.
- CLI: a shared development token defaulted across every program, `ServeFlags`
  that build the server `Guard` (`--serve-token`, `--require-auth`,
  `--serve-open`, `--oidc-issuer`/`--oidc-audience`/`--oidc-jwks-file`,
  `--authz-db`, `--authz-fresh-writes`, `--authz-break-glass-role`,
  `--authz-max-depth`), `CORIUM_TOKEN` / `CORIUM_SERVE_TOKEN` /
  `CORIUM_AUTHZ_DB` environment overrides, and
  `corium authz init|grant|revoke|check|status`.
- Unit tests covering each model requirement: optional-off, distinct
  static-token identities, the external-verifier seam, provider composition,
  role/database enforcement, an async external-oracle authorizer, per-principal
  views, interceptor extension propagation, and one guard serving two tenants
  with different authority.
- ReBAC tests: 20 behavioural tests over the compiled policy (usersets, group
  shorthand, rewrites, wildcards, view intersection and the unfiltered escape,
  unmapped actions, cycles, depth and visit budgets, cache invalidation, fresh
  consistency, fail-closed with break-glass, provider pinning, audit), plus two
  end-to-end tests that enforce a policy over a live transactor's gRPC surface
  and a peer server, including a grant taking effect without a restart.
