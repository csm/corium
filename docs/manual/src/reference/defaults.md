# Default values

## Network

| Setting | Default |
|---|---|
| Transactor listen address | `127.0.0.1:4334` |
| Peer server listen address | `127.0.0.1:4336` |
| PostgreSQL server listen address | `127.0.0.1:5432` |
| Metrics listen address | Disabled |
| Client transactor endpoint | `http://127.0.0.1:4334` |

## Storage

| Setting | Default |
|---|---|
| `--store` | `fs` |
| `--data-dir` | None. Required. |
| `--turso-path` | `<data-dir>/store.db` |
| `--s3-prefix` | Bucket root |
| S3 read-only role duration | 900 seconds |

## Lease and availability

| Setting | Default |
|---|---|
| `--lease-ttl-ms` | 5000 |
| `--lease-wait-ms` | 15000 |
| `--heartbeat-ms` | 10000 |
| `--ha` | Off |
| `--owner` | `transactor-$HOSTNAME` |
| Peer reconnect backoff | 100 ms to 5 s |
| Peer failover timeout | 30 s |

## Index publication

| Setting | Default |
|---|---|
| `--index-interval-ms` | 5000 |
| `--index-backoff` | 4 |
| `--index-tail-threshold` | 0 |
| `--index-tail-deadline-ms` | 60000 |

## Garbage collection

| Setting | Default |
|---|---|
| `--gc-interval` | `1h` |
| `--gc-window` | `72h` |
| `corium gc --window` | `72h` |

## Query and function budgets

| Setting | Default |
|---|---|
| `--max-fuel` (peer server) | 10000000 |
| `--db-fn-fuel` | 1000000 |
| `--db-fn-memory-bytes` | 16777216 (16 MiB) |
| `--authz-max-depth` | 8 |

## Segment cache

| Setting | Default |
|---|---|
| `--segment-cache-dir` | Disabled |
| `--segment-cache-capacity` | None. Required with the directory. |
| `--segment-cache-memory` | 64 MiB, or the capacity when smaller |

## Security

| Setting | Default |
|---|---|
| Authentication | Permissive: development token accepted, anonymous admitted |
| Authorization | Permit-all |
| TLS | Off |
| Encryption at rest | Off |
| Authorization database name | `corium_authz` |
| `authz init --admin` | `operator` |
| `authz init --provider` | `static-token` |
| Attribute protection | Off |
| `--key-policy` | `strict` when authentication is configured, `server-wide` when it is not |
| `on-missing-key` (protection class) | `redact` |
| `legacy-plaintext` (protection class) | `redact` |
| `scope` (protection class) | `attribute` |

## Schema

| Setting | Default |
|---|---|
| `corium schema update` mode | Read-only. `--apply` is needed to write. |
| Permitted execution class | `additive` only |
| `--prune` | Off. Absent attributes are reported as unmanaged. |
| Exit code with changes planned | 0, or 2 with `--detailed-exit-code` |

## Interactive surfaces

| Setting | Default |
|---|---|
| `corium tui --refresh-ms` | 2000, minimum 250 |
| `corium log --from` | 0 |
| `corium log --to` | 0, meaning open-ended |
| `corium postgres-server` write mode | Read-only |

## Duration and size formats

A duration accepts one of the suffixes `ms`, `s`, `m`, `h`, and `d`, for
example `1h`, `30m`, `72h`. A bare number is seconds. `off` disables the
scheduled collection duty.

A byte size accepts `B`, `KiB`, `MiB`, `GiB`, `TiB`, `kB`, `MB`, `GB`, and
`TB`. A bare number is bytes.
