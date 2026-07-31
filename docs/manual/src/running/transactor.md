# The transactor

The transactor owns writes, logs, indexing, leases, and scheduled garbage
collection. One process serves every database in its catalog.

## Start a transactor

```sh
corium transactor --data-dir /srv/corium --listen 0.0.0.0:4334
```

The process prints the databases that it serves and the databases for which it
stands by. It exits with an error when it cannot acquire a lease and `--ha` is
not set.

Every flag below has an equivalent key in the
[configuration file](config-file.md). A flag on the command line always wins.

## Identity and network

| Flag | Default | Effect |
|---|---|---|
| `--data-dir <path>` | None. Required. | Data directory for the filesystem store and for logs. |
| `--listen <addr>` | `127.0.0.1:4334` | gRPC listen address. |
| `--owner <id>` | `transactor-$HOSTNAME` | Stable identity in lease records. Set it. |
| `--advertise <url>` | None | Client endpoint that peers use to find the lease holder. |
| `--metrics-listen <addr>` | None | Prometheus endpoint at `/metrics`. |

Set `--owner` to a stable value per member, for example the host name. A
restarted member then re-acquires its own unexpired lease at once. A service
manager usually does not export `HOSTNAME`, so the default becomes
`transactor-local` on every member.

> **CAUTION: Keep the metrics listener on a private network.** The endpoint
> has no bearer-token authentication.

## Storage selection

`--store` picks the backend: `mem`, `fs`, `postgres`, `turso`, or `s3`. The
default is `fs`. Each backend has its own flags and its own Cargo feature. See
[storage backends](storage.md).

## Lease and availability

| Flag | Default | Effect |
|---|---|---|
| `--ha` | Off | Stand by when another transactor holds the lease, instead of failing at startup. |
| `--lease-ttl-ms <n>` | 5000 | Failover detection bound. Renewals run at one third of this value. |
| `--lease-wait-ms <n>` | 15000 | How long startup waits for a held lease before it gives up. Ignored with `--ha`, which waits without limit. |
| `--heartbeat-ms <n>` | 10000 | Subscription heartbeat interval. |

A lower time-to-live gives faster takeover. It also costs more root-store
traffic, and it tolerates shorter pauses on the active member. See
[high availability](../availability/high-availability.md).

## Index publication pacing

| Flag | Default | Effect |
|---|---|---|
| `--index-interval-ms <n>` | 5000 | Base interval between publications. |
| `--index-backoff <n>` | 4 | Minimum wait before the next publication, as a multiple of the duration of the last one. `0` disables it. |
| `--index-tail-threshold <n>` | 0 | Defer publication while fewer than this many datoms are pending. `0` publishes any pending work. |
| `--index-tail-deadline-ms <n>` | 60000 | Longest that a small tail defers publication. |

These four values can also be changed per database at runtime. See
[index publication](indexing.md).

## Garbage collection

| Flag | Default | Effect |
|---|---|---|
| `--gc-interval <duration>` | `1h` | Interval of the scheduled sweep. `off` disables it. |
| `--gc-window <duration>` | `72h` | Retain unreachable blobs for at least this long. |

Collection is serialized with index publication. See
[garbage collection](../availability/gc.md).

## Database functions

| Flag | Default | Effect |
|---|---|---|
| `--db-fn-fuel <n>` | 1000000 | Execution credits per `:db/fn` call. |
| `--db-fn-memory-bytes <n>` | 16777216 | Managed memory per `:db/fn` call. |

User `:db/fn` code runs on the transactor in a restricted Clojure interpreter.
The interpreter has no input or output access. These two budgets bound a
runaway function. Both flags need the `cljrs` feature, which is on by default.

## Authentication, authorization, and TLS

| Flag | Effect |
|---|---|
| `--serve-token <secret>` | Require this exact bearer token. Strict mode. |
| `--require-auth` | Require the shared development token. Reject anonymous callers. |
| `--serve-open` | Accept every request as anonymous. |
| `--oidc-issuer <url>` | Accept tokens signed by this issuer. Strict mode. |
| `--authz-db <name>` | Authorize every request against this policy database. |
| `--tls-cert <pem>`, `--tls-key <pem>` | Serve TLS. Both are required together. |

The default is permissive. The server recognizes the shared development token,
and it also admits anonymous callers. Any of `--serve-token`, `--require-auth`,
or `--oidc-issuer` switches the server to strict mode.

Read [authentication and TLS](../security/authentication.md) before you expose
a transactor outside a private network.

## Encryption keys

`--storage-key <uri>` names a key-encryption key that this process can
resolve. The flag is repeatable, because one transactor hosts databases under
different keys.

The process resolves every named key at startup. A misconfigured process
therefore fails at startup and names the key. See
[encryption at rest](../security/encryption.md).

## Logging

Tracing is human-readable by default. `--log-format json` writes structured
logs. `RUST_LOG` filters them.

```sh
RUST_LOG=corium_transactor=debug,corium_peer=info \
  corium --log-format json transactor --data-dir /srv/corium
```

`--log-format` is a global flag. It comes before the subcommand.

## A production example

```sh
corium transactor \
  --config /etc/corium/transactor.edn \
  --listen 0.0.0.0:4334 \
  --advertise http://txor-a.internal:4334 \
  --owner txor-a \
  --ha \
  --metrics-listen 127.0.0.1:9464 \
  --serve-token "$CORIUM_SERVE_TOKEN" \
  --authz-db corium_authz \
  --tls-cert /etc/corium/tls/server.pem \
  --tls-key /etc/corium/tls/server.key
```

The configuration file holds the storage selection and the read-only discovery
credentials. The command line holds the identity of the member.
