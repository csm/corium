# ADR-0014: Self-hosted relationship-based authorization in a Corium database

**Status:** Accepted (2026-07-24); implemented in
[`corium-authz`](../../crates/corium-authz/src/lib.rs) with the design in
[`docs/design/auth.md`](../design/auth.md). Extends
[ADR-0012](0012-optional-authn-authz.md), which fixed the request-scoped authz
seam this fills in.

## Context

ADR-0012 gave the network surfaces a per-request `Principal` and an async
`Authorizer` that maps `(Principal, Access)` onto
`Allow` / `AllowFiltered(ViewFilter)` / `Deny`. It deliberately shipped only
two authorizers: permit-all, and a role→grant table configured in Rust. Neither
scales past a single operator — a role table cannot express "everyone in the
engineering group writes the databases owned by their tenant" without
enumerating it — and changing either means restarting a server.

The obvious next step is a relationship-based (ReBAC) model in the Zanzibar
tradition: relationship tuples plus derivation rules, with a `Check` that walks
them. The question is *where the tuples live*. Running an external policy
service (OpenFGA, Auth0 FGA) is a supported answer — the async `Authorizer` was
shaped for it — but it costs a second service, a second consistency boundary,
and a second cache layer for every deployment, including the single-cluster
ones that make up most of them.

Corium is already a database whose model is a close fit: tuples are entities,
policy is small and read-mostly, snapshots are immutable values, every change
gets a monotonic basis `t`, and history is retained. Storing policy in Corium
means backup, restore, fork, `as-of`, and the log API apply to policy for free.

## Decision

Store relationship policy in an ordinary Corium database — `corium_authz` by
default — with a reserved, tuple-oriented schema, and answer checks from a
compiled in-memory snapshot of it.

- **Schema is data, not Rust.** Relation names (`owner`, `writer`, `viewer`,
  `member`, `parent`) are strings a deployment invents. The fixed vocabulary is
  the existing `Action`/`ActionClass` API contract, which policy maps onto
  relations through permission entities. The tuple shape stays flat and
  string-keyed so it can be imported from, or exported to, an OpenFGA-like
  service.
- **The authz database is an ordinary database.** No control-plane handle, no
  special storage path: it is created with `CreateDatabase`, edited with
  transactions, and read through the same snapshot machinery. Access to it is
  governed by the policy it contains.
- **Compile, then walk.** Each surface compiles the snapshot into an immutable
  `Policy` keyed by its basis `t` and answers checks with a breadth-first walk
  bounded by depth, visited goals, and a visited set that doubles as cycle
  detection. Compilation happens off the request path, driven by the source's
  change signal; the request path touches only in-memory indexes.
- **Every decision names its basis.** `authz_t` appears in the decision, the
  audit event, and the check-result cache key — one number that makes
  decisions reproducible and invalidation free.
- **Fail closed, with a narrow escape.** No readable policy means no access.
  Break-glass admits configured roles *only* while policy is unreadable, never
  to override a deny, and is audited at `warn` on every use.
- **Each surface supplies its own snapshot.** The transactor reads the database
  it already leads (so write admission is decided at the serialization point);
  a peer server reads a second peer connection. Both are `PolicySource` impls,
  so an embedded or test deployment can hand over a database value directly.

## Consequences

- A single-cluster deployment gets ReBAC with no extra service, no extra
  consistency boundary, and no extra cache: `corium authz init`, some grants,
  and `--authz-db` on the servers.
- Policy is versioned, auditable, and reproducible by construction. "Why was
  this allowed at 14:03?" is answered by reading the authz database `as-of` the
  `authz_t` in the audit line.
- Propagation between processes is eventually consistent by default. That is
  the right trade for reads; control-plane actions can demand a fresh basis
  (`--authz-fresh-writes`), and the transactor's own snapshot makes write
  admission consistent where it is serialized.
- Policy authors can write pathological data (deep chains, cycles). The bounds
  make that a bounded cost and a distinguishable outcome (`Exhausted`) rather
  than a hang, at the price of policies deeper than the limit silently not
  granting — the limit is configurable and logged.
- Relation names being data means typos are not compile errors. `corium authz
  check` exists to make the answer — and the matched path — inspectable before
  enforcement is turned on.
- `AllowFiltered` decisions are expressible but not yet servable: no read path
  applies a `ViewFilter`, so a peer server rejects a filtered decision with
  `UNIMPLEMENTED` rather than returning unfiltered data. That keeps the failure
  safe, and leaves the query-engine hook (ADR-0012's largest deferred piece) as
  the remaining work.
- The external-oracle path survives untouched: `Authorizer` is still the seam,
  and a deployment can point it at OpenFGA instead, or export Corium's tuples
  to one.
