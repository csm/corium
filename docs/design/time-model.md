# Time Model

Every datom carries its transaction; nothing is overwritten. The database
value handed to queries is a *view* defined by a basis filter, and all views
are cheap (they wrap the same trees with a filter/merge policy — no copying).

## Database views

Given a connection whose latest known basis is `t-now`:

| View | Meaning | Implementation |
|---|---|---|
| `db()` | current facts as of `t-now` | current indexes + live tail |
| `as-of(db, t)` | facts as they stood at basis `t` | current+history indexes, filter `tx ≤ t`, replay retraction pairs to reconstruct then-current set |
| `since(db, t)` | only facts added after `t` | filter `tx > t` on current indexes |
| `history(db)` | all assertions **and** retractions ever | history indexes ∪ current; datoms expose `added` |
| `sync(t)` / `sync()` | future that completes when the peer's basis ≥ `t` (or ≥ transactor's latest) | tx-report stream bookkeeping |

Rules baked into the index design to make these work:

- **Current indexes** hold live datoms only.
- **History indexes** hold every assertion and retraction (except
  `:db/noHistory` attributes), sorted with `tx` as the final key component so
  as-of reconstruction is a bounded backward scan within an `[e a v]` group.
- `as-of` and `history` views disable upsert/uniqueness shortcuts in query
  planning (uniqueness holds only for the current view).
- A transactor's indexing job maintains current and history trees in the same
  pass; there is no separate "history build".

## Log API

`tx-range(from-t, to-t)` streams `(t, tx-instant, [datom])` straight from the
log chunk tree — available on any peer without touching the covering indexes.
`t → tx-instant` resolution and `as-of(instant)` / `since(instant)` variants
binary-search the log by `:db/txInstant` (monotonic by construction).

## tx-report queue

Peers receive a stream of tx-reports:

```rust
pub struct TxReport {
    pub basis_t_before: T,
    pub basis_t_after: T,
    pub tx_data: Vec<Datom>,   // including the tx entity's own datoms
    pub tempids: Map<TempId, EntityId>,  // present for the submitting peer
}
```

The peer applies `tx_data` to its live index (advancing its basis) and then
offers the report to user-registered listeners (`tx-report-queue` in the Rust
and cljrs APIs). This one stream serves three needs: keeping peer basis
current, `sync`, and user change-feeds (materialized views, cache
invalidation, reactive systems).

Delivery guarantee: reports arrive in t-order with no gaps for a connected
peer; on reconnect the peer declares its basis and the transactor (or the log)
backfills the gap. Reports are not durable per-consumer state — a consumer
needing exactly-once must track its own high-water `t`, for which `tx-range`
provides replay.

## Excision

Explicitly out of scope for v1 (it is the one operation that violates
immutability and complicates segment sharing). The plan reserves design space:
excision would be implemented as a filter set stored in the DbRoot applied at
segment-read time, with physical rewrite happening lazily during normal
re-indexing — never as an in-place mutation. Revisit after M6.
