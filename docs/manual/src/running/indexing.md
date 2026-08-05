# Index publication

The transactor republishes the covering indexes in the background. A cold peer
then bootstraps from a snapshot instead of replaying the whole log.

Each index is published as content-defined leaf chunks under a small manifest.
A publication uploads only the chunks that the changes landed in. Building the
snapshot still costs processor time in proportion to the database, and pacing
bounds that cost.

## Indexing is never a durability requirement

The log append is the commit point. The transactor serves from its in-memory
value whatever the index lag is.

Deferred publication has two costs, and no risk.

- Cold-peer bootstrap gets slower, because the log tail after the published
  basis is replayed.
- A backup is less fresh, because it reads through the published basis.

## Pacing

| Flag | Default | Effect |
|---|---|---|
| `--index-interval-ms` | 5000 | Base interval between publications. |
| `--index-backoff` | 4 | Minimum wait before the next publication, as a multiple `n` of the duration of the last one. `0` disables it. |
| `--index-tail-threshold` | 0 | Defer a due publication while fewer than this many datoms are pending. `0` publishes any pending work. |
| `--index-tail-deadline-ms` | 60000 | Longest that a below-threshold tail defers publication. |

The backoff bounds indexing to at most `1/(1+n)` of wall-clock time and of
storage bandwidth as publications get slower. With the default of 4, indexing
takes at most one fifth of the time.

The tail threshold makes small writes coalesce. Without it, a trickle of
transactions rewrites the indexes on every interval.

## Runtime overrides

All four values can be changed per database while the transactor runs. Omitted
flags are unchanged. An override lasts until the process restarts.

```sh
corium db index-policy people --interval-ms 60000 --tail-threshold 1000000
```

Read the current policy back with no flags:

```sh
corium db index-policy people
```

The command prints one EDN map:

```clojure
{:db "people" :interval-ms 60000 :backoff 4 :tail-threshold 1000000 :tail-deadline-ms 60000}
```

## Publish now

```sh
corium db request-index people
```

This request bypasses pacing entirely. Use it after a bulk load, and before a
backup, when the snapshot must be current.

## Bulk loading

For a bulk load, follow this procedure.

1. Raise the tail threshold, for example to one million datoms:
   `corium db index-policy <db> --tail-threshold 1000000`.
2. Run the load. The backoff keeps the indexing duty cycle bounded as the
   database grows.
3. Watch `:index-lag` in `corium db stats`, or the metrics endpoint.
4. Run `corium db request-index <db>` when the load is complete.
5. Restore the normal policy, or restart the transactor.

Without step 4, the final tail publishes within the tail deadline of the last
transaction.

> **Partly implemented.** On the native backends the per-transaction log
> objects are not yet sealed into chunks. Replay cost and list cost therefore
> grow with the tail since the last publication. A long deferral on
> `postgres`, `turso`, or `s3` makes recovery and cold bootstrap slower than
> the same deferral on `fs`.

## Watching the lag

Three surfaces report index lag.

- `corium db stats <db>` prints `:index-basis-t` and `:index-lag`.
- The `Metrics` panel of [`corium tui`](../surfaces/tui.md) plots index lag.
- The metrics endpoint exposes `corium_transactor_index_duration_seconds`. See
  [monitoring](../operations/monitoring.md).

A lag that grows without limit means that publication cannot keep up. Lower
`--index-backoff`, or give the transactor faster storage.
