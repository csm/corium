# Clojurust Integration

Clojurust (`cljrs`) plays two roles, both at the boundary of the engine
(ADR-0002): the idiomatic client language, and the execution environment for
user database functions. The engine core never depends on cljrs types.

## Value conversion

`corium-cljrs` implements bidirectional conversion between `corium_core::Value`
/ datom structures and `cljrs-value` types via the `FromValue`/`IntoValue`
marshalling traits from `cljrs-interop`:

| Corium | cljrs |
|---|---|
| Keyword (interned) | keyword |
| Str / Bool / Long / Double / BigInt / BigDec | string / bool / long / double / bigint / bigdec |
| Instant | `#inst` tagged value |
| Uuid | `#uuid` tagged value |
| Ref / EntityId | long (plus `Entity` wrapper type) |
| Bytes | byte array |
| Datom | seqable/indexed `[e a v tx added]` value |
| tx-data, query forms, pull patterns | plain EDN collections |

Conversion happens once per API call at the edges; query execution, index
access, and transaction expansion all run on engine-native types. EDN *text*
parsing everywhere in Corium (CLI, query strings, config) uses `cljrs-reader`,
so there is exactly one EDN implementation in the system.

## cljrs client API

A `corium.api` namespace mirroring Datomic's peer API shapes, exported via
`#[cljrs_interop::export]`:

```clojure
(require '[corium.api :as d])

(def conn (d/connect "corium://transactor:4335/mydb"))
(d/transact conn [{:person/name "Rich" :person/langs #{:clojure :rust}}])

(def db (d/db conn))
(d/q '[:find ?n :where [?e :person/name ?n]] db)
(d/pull db '[* {:person/friends [:person/name]}] [:person/email "x@y.z"])
(d/entity db [:person/email "x@y.z"])
(d/as-of db t) (d/history db) (d/since db t)
(d/tx-range conn t1 t2)
(d/tx-report-queue conn)
(d/sync conn t)
```

The cljrs API binds to `corium-peer` (a real peer in the cljrs process), not
to the thin-client protocol — Clojurust programs get local query execution,
which is the point of the pairing.

## Database functions (ADR-0008)

User transaction functions are entities: `:db/ident`, `:db/fn` whose value is
the function's code (stored as a string datom; compiled+cached per schema
basis). Invocation `[:my/fn arg…]` during transaction expansion calls the
function with `(db-in-transaction, args…)`; it returns tx-data that is
recursively expanded. Classic uses: domain-checked upserts, counters,
invariant enforcement.

Execution environment — a **sandboxed cljrs interpreter** hosted by the
transactor (`corium-cljrs::sandbox`):

- **Namespace allowlist**: `clojure.core` pure subset + `corium.api` read-only
  db operations (`q`, `pull`, `entity`, `datoms` against the in-transaction
  db). No I/O namespaces, no `eval`ing new definitions outside the fn, no
  interop escape into arbitrary Rust, no atoms/agents/futures (no side
  channels, keeps functions pure and replayable).
- **Fuel budget**: interpreter step limit and allocation cap per invocation;
  wall-clock deadline as backstop. Exceeding ⇒ transaction aborts with a
  clear error. Budgets are transactor config.
- **Determinism**: given `(db, args)`, a db function must produce identical
  tx-data on every run — enforced by the environment shape (no clock, no
  randomness in the allowlist). This keeps the log the source of truth: the
  log records the *expanded* datoms, so replay never re-runs functions.
- Built-ins `:db/cas`, `:db/retractEntity`, and future `:db/fn`-shaped
  primitives are native Rust in `corium-tx`, not sandboxed code.

The sandbox needs cljrs to support a restricted environment (custom namespace
resolution + step-limited interpretation). **Open item for M5**: verify
`cljrs-interp` exposes fuel/step hooks; if not, contribute that upstream or
run the interpreter on a watchdog thread with namespace restriction only —
tracked as a risk in the roadmap.

Query predicate/function clauses reuse the same sandbox host post-v1
(query-engine.md), with read-only db access and the same fuel discipline.
