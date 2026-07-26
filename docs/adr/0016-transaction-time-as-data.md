# ADR-0016: Transaction by time as data

**Status:** Accepted (2026-07-25); implemented in
[`corium-db::bootstrap`](../../crates/corium-db/src/bootstrap.rs) with the
model in [`docs/design/data-model.md`](../design/data-model.md) and the views
in [`docs/design/time-model.md`](../design/time-model.md). Completes the time
model of [ADR-0005](0005-full-time-model.md).

## Context

ADR-0005 shipped the full time model — `as-of`, `since`, `history`, the log
API, tx-reports — but transaction *time* was only half present. Every datom
carried its `tx`, and `t` ordered transactions, yet the wall-clock instant
lived exclusively as a field on the log record (`TxRecord::tx_instant`) and a
high-water mark on the database root. That left three gaps:

- **Not queryable.** `[?tx :db/txInstant ?inst]` matched nothing. A query could
  reach a datom's transaction but not the time it was committed, so "what did
  this look like last Tuesday" was unanswerable in Datalog, and audit questions
  needed the log API and a join done by hand in application code.
- **No instant-named views.** `docs/design/time-model.md` promised
  `as-of(instant)` and `since(instant)`; nothing implemented them, because
  there was nothing indexed to resolve an instant against.
- **No transaction metadata.** Datomic's reserved `"datomic.tx"` tempid had no
  analogue, so the who/why of a change had nowhere to live next to the when.

Corium is being evaluated for bitemporal extensions, where valid time is
compared against transaction time. Comparing against an axis the query language
cannot see is not possible, so this is a prerequisite for that work as well as
being worth having on its own.

## Decision

Make transaction time an ordinary datom.

1. **Bootstrap attribute.** The engine installs `:db/txInstant` in every
   database (`:db.part/db` sequence 50, as in Datomic; sequences below the
   first user-installable id are reserved for the engine). It is an `instant`,
   cardinality one, AVET-indexed, with history.
2. **Every commit asserts it.** All commit paths — sync, async, group-commit
   batch — settle the instant and materialize the datom before the log append,
   so the log record, the tx-report, peers' live indexes, and published
   snapshots carry one representation. The stamp stays `max(now, last + 1)`.
3. **Transaction data may supply it.** Asserting `:db/txInstant` against the
   transaction entity dates a commit explicitly (how a backfilled import keeps
   original timestamps); an instant that does not advance the clock is
   rejected, because monotonicity is what makes instant resolution meaningful.
4. **Reserved tempid.** `"datomic.tx"` (and `:db/current-tx` at the EDN
   boundary) names the transaction entity, allocating nothing and never
   upserting through a unique-identity attribute.
5. **Instant-named views.** `as-of`/`since` accept a wall-clock instant on
   every surface: Rust, cljrs, the wire (`DbViewSpec`), the console, and the
   SQL shell. An instant resolves to the last transaction committed at or
   before it; an instant older than the database resolves to basis 0.
6. **Old logs keep working.** Replay synthesizes the datom from the record's
   timestamp field when the datom set lacks one, so existing databases gain
   instant-named views without a rewrite.

## Consequences

- Transaction entities are now ordinary entities with at least one datom.
  Datom and entity counts include them, `d/datoms` returns them, and SQL's wide
  projection gains a `db` table over transactions. This is Datomic's behavior
  and the honest reading of "everything is datoms", but it does change counts
  that tests and dashboards may have treated as user data.
- One extra datom per transaction, in a partition of its own. Publication
  dirties the chunk holding the transaction region in addition to the chunks
  the transaction's own datoms touch — an O(1) addition per commit, since both
  regions grow at their tail.
- Instant resolution is O(log n) in both directions and view-independent: the
  `t ↔ instant` correspondence is carried on the database value, so deriving a
  view never truncates the clock it resolves against. Its memory cost is two
  ordered-map entries per transaction, against a value that already holds every
  datom.
- A current-state snapshot published before this change has no `:db/txInstant`
  datoms; a peer bootstrapped from one resolves instants only within the
  replayed tail until the next publication.
- The wasm engine (ephemeral, in-browser, no clock) still commits without
  instants: its transactions carry no `:db/txInstant`, and instant-named views
  there resolve to basis 0. `t`-named views are unaffected.
- Valid time remains a modeling concern for the user — this ADR makes
  transaction time first class, not bitemporality. It does, however, remove the
  reason bitemporal work could not start: both axes can now be compared in a
  query.
