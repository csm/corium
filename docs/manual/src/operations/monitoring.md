# Monitoring

Three surfaces report the state of a Corium system: the metrics endpoint,
`corium db stats`, and the tracing log.

## The metrics endpoint

Pass `--metrics-listen` to a transactor or a peer server:

```sh
corium transactor --data-dir /srv/corium --metrics-listen 127.0.0.1:9464
```

The endpoint serves Prometheus text at `/metrics`.

> **CAUTION: Keep the metrics listener on a private operations network.** The
> endpoint has no bearer-token authentication.

### Transactor metrics

| Metric | Type | Meaning |
|---|---|---|
| `corium_transactor_transactions_total` | Counter | Committed transactions. |
| `corium_transactor_transaction_failures_total` | Counter | Rejected transactions. |
| `corium_transactor_transaction_latency_seconds` | Histogram | Commit latency. |
| `corium_transactor_queue_depth` | Gauge | Commit queue depth. |
| `corium_transactor_index_duration_seconds` | Histogram | Index publication duration. |
| `corium_transactor_gc_runs_total` | Counter | Collection runs. |
| `corium_transactor_gc_swept_blobs_total` | Counter | Blobs deleted. |
| `corium_transactor_gc_retained_blobs_total` | Counter | Unreachable blobs kept by the window. |
| `corium_keys_unavailable` | Gauge | Nodes that cannot load a key manifest change. |

### Peer server metrics

| Metric | Type | Meaning |
|---|---|---|
| `corium_peer_queries_total` | Counter | Queries served. |
| `corium_peer_query_latency_seconds` | Histogram | Query latency. |
| `corium_peer_query_fuel_spent_total` | Counter | Datoms touched. |

### Segment cache metrics

A peer server with `--segment-cache-dir` adds these.

| Metric | Labels | Meaning |
|---|---|---|
| `corium_peer_segment_cache_requests_total` | `result`, `tier` | Hits and misses per tier. |
| `corium_peer_segment_cache_native_fetches_total` | `result` | Fetches that went to storage. |
| `corium_peer_segment_cache_bytes_read_total` | `source` | Bytes read per source. |
| `corium_peer_segment_cache_admissions_total` | `result` | Admissions and rejections. |
| `corium_peer_segment_cache_used_bytes` | `tier` | Bytes in use. |
| `corium_peer_segment_cache_capacity_bytes` | `tier` | Configured capacity. |

## On-demand statistics

```sh
corium db stats people
```

The command prints the basis, the index basis, the index lag, the counts, and
the transactor counters. See [the database catalog](../running/catalog.md).

The transactor `Status` call carries the same data. The `Metrics` panel of
[`corium tui`](../surfaces/tui.md) samples it live, and it is the only surface
that shows lease ownership.

## Auditing schema changes

Every applied schema update records its requester, its digests, its observed
basis, and its acknowledgements on the transaction entity. Those are ordinary
attributes, so the schema history of a database is a query.

```clojure
[:find ?when ?who ?tool
 :where [?tx :db.schemaUpdate/requester ?who]
        [?tx :db.schemaUpdate/tool ?tool]
        [?tx :db/txInstant ?when]]
```

```text
[[#inst 1785899778642 "static-token:operator" "corium-cli/0.1.0"]]
```

The requester is the authenticated principal. A caller never supplies it.

An ordinary transaction cannot write `:db.schemaUpdate/*`, so no transaction
can claim to have been a schema update. See
[schema management](../running/schema.md#the-audit-trail).

## Logging

Tracing is human-readable by default. `--log-format json` writes structured
logs, and `RUST_LOG` filters them.

```sh
RUST_LOG=corium_transactor=info,corium_peer=warn \
  corium --log-format json transactor --data-dir /srv/corium
```

`--log-format` is a global flag, so it comes before the subcommand.

Useful targets:

| Target | Content |
|---|---|
| `corium_transactor` | Commit pipeline, indexing, leases, garbage collection. |
| `corium_peer` | Connection, subscription, failover. |
| `corium_authz::audit` | Every authorization decision, with its basis. Denials at `info`, grants at `debug`. |

Log lines worth an alert:

- `standby took over write lease` — a failover happened.
- `deposed` — this member lost the lease. It stands down on its own.
- `standing by; lease held elsewhere` — normal on a standby at startup.

## What to alert on

| Signal | Condition | Meaning |
|---|---|---|
| Index lag | Grows without limit | Publication cannot keep up. |
| `corium_transactor_queue_depth` | Stays high | The write path is saturated. |
| `corium_transactor_transaction_failures_total` | Rises sharply | Validation errors, or a fenced writer. |
| `corium_keys_unavailable` | Above zero | A node cannot load a key manifest change. |
| `corium_transactor_gc_retained_blobs_total` | Grows steadily | Storage is not being reclaimed. |
| Lease owner | Changes unexpectedly | An unplanned failover. |

Peer memory is not exported as a metric. Watch the resident memory of the
process, because a peer holds the whole history. See
[indexes and storage](../theory/indexes.md).
