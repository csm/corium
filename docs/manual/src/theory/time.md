# Time and database values

## The database value

A peer holds a database value. The value is immutable. A query runs against
one value, so the answer cannot change while the query runs.

A time view wraps the same datoms with a different fold policy. A view never
copies a fact.

## The views

Given a connection whose latest known basis is `t-now`:

| View | Meaning |
|---|---|
| `db()` | Current facts at `t-now`. |
| `as-of(t)` | Facts as they stood at basis `t`. |
| `since(t)` | Only facts added after `t`. |
| `history()` | Every assertion and every retraction ever recorded. |
| `sync(t)` | Completes when the basis of the peer reaches `t`. |

`as-of` and `since` also accept a wall-clock instant. Corium resolves the
instant to the last transaction committed at or before it. An instant older
than the database resolves to basis 0.

The `as-of` and `history` views disable the uniqueness shortcuts of the
planner. Uniqueness holds only in the current view.

## Naming a view by wall clock

The `t` to instant correspondence is part of the database value, because every
commit asserts `:db/txInstant`. Resolution in both directions is O(log n).

A derived view keeps the whole correspondence. An instant therefore means the
same thing whatever value it starts from.

Five surfaces accept an instant.

- `Db::as_of_instant` and `Db::since_instant` in Rust.
- `d/as-of` and `d/since` in the Clojure API.
- `as_of_instant` and `since_instant` in `DbViewSpec` on the wire.
- `:as-of <timestamp>` and `:since <timestamp>` in the console.
- `\as-of <timestamp>` and `\since <timestamp>` in the SQL shell.

## The cost of a view today

> **Partly implemented.** The design opens a view by descent through the
> segment tree. The implementation folds the view in memory from the recorded
> log. The costs below are what a peer pays now.

- `as-of` folds the log up to its basis.
- `history` folds the whole log.
- `since` folds the whole log and then filters. It narrows before it projects,
  so the floor is applied while the index is built.

The result is cached in the database value, and it is shared by the clones of
that value. A view that selects exactly the datoms of an already-folded view
reuses that fold. A genuinely distinct view pays a full fold on first read.

The operational rule: a report that opens many distinct historical views is
expensive on a peer. Reuse one view where possible.

## Transaction reports

A peer receives a stream of transaction reports. Each report holds the basis
before, the basis after, the datoms, and the tempid map for the submitting
peer.

The peer applies the datoms to its live index and then offers the report to
registered listeners. One stream serves three needs: keeping the peer basis
current, `sync`, and application change feeds.

Reports arrive in `t` order with no gaps for a connected peer. After a
reconnect the peer declares its basis, and the transactor backfills the gap.

Reports are not durable per consumer. A consumer that needs exactly-once
delivery must track its own high-water `t`. `tx-range` replays the gap.

## Reading the log directly

`tx-range(from-t, to-t)` streams transactions from the log tree. It is
available on any peer, and it does not touch the covering indexes. Use it for
audit, for replay, and for change-data capture.
