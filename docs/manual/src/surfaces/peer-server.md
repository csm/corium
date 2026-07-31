# Peer server and thin clients

A peer server is a peer hosted as a standalone process. It exposes query,
pull, transact, datom scans, and transaction ranges over gRPC.

Use it for a language that has no peer library. For a language with a peer
library, embed the peer instead. An embedded peer queries in-process.

## Start a peer server

```sh
corium peer-server --db people --listen 0.0.0.0:4336 \
  --transactor http://127.0.0.1:4334
```

One process hosts one database. Run one process per database.

| Flag | Default | Effect |
|---|---|---|
| `--db <name>` | None. Required. | Database to host. |
| `--listen <addr>` | `127.0.0.1:4336` | gRPC listen address. |
| `--max-fuel <n>` | 10000000 | Ceiling on datoms touched per query. |
| `--metrics-listen <addr>` | None | Prometheus endpoint at `/metrics`. |

The peer server takes the same [connection flags](../running/catalog.md) as
the other client commands, and the same
[serving flags](../security/authentication.md) as the transactor.

## Query fuel

Fuel bounds a runaway query. A client can request less fuel. `fuel = 0`
requests the server default. The server clamps every request to `--max-fuel`.

An exhausted budget returns `INVALID_ARGUMENT`.

## Storage bootstrap

By default the peer server replays the log from basis 0 at startup. On a large
database that is slow.

```sh
corium peer-server --db people --peer-bootstrap \
  --storage-key file:/etc/corium/storage.key
```

`--peer-bootstrap` reads the published snapshot from storage and subscribes
from the index basis. The peer needs network reach to the storage backend, and
a build with the matching storage feature.

The transactor supplies the connection details through its `GetStorageInfo`
call, using the read-only credential that you configured. See
[storage backends](../running/storage.md).

## Segment cache

A peer server can keep a local SSD cache of segments.

| Flag | Default | Effect |
|---|---|---|
| `--segment-cache-dir <path>` | None | Dedicated directory for the cache. |
| `--segment-cache-capacity <size>` | None. Required with the directory. | Disk capacity, for example `256GiB`. |
| `--segment-cache-memory <size>` | 64MiB, or the capacity when smaller | Memory front tier. |

A size accepts the suffixes `B`, `KiB`, `MiB`, `GiB`, `TiB`, `kB`, `MB`, `GB`,
and `TB`.

The cache requires `--peer-bootstrap`. Without it, the process fails at
startup with a clear message.

Give the cache a dedicated directory. Corium manages the contents.

## Failover

Pass every transactor endpoint, active first:

```sh
corium peer-server --db people \
  --transactor http://txor-a:4334,http://txor-b:4334
```

The peer rotates the list on failure. A standby rejects subscriptions with a
`standby` status, which the peer treats as a reason to try the next endpoint.
See [high availability](../availability/high-availability.md).

## The thin-client contract

The wire contract is documented in
[thin-client-protocol.md](https://github.com/csm/corium/blob/main/docs/thin-client-protocol.md).
The canonical schema is `crates/corium-protocol/proto/corium.proto`.

Six rules matter to an operator.

- Every transact and subscribe request sends `protocol_version = 1`. A
  mismatch is `FAILED_PRECONDITION`, never a silent downgrade.
- Malformed input is `INVALID_ARGUMENT`. An unknown database or entity is
  `NOT_FOUND`. Upstream loss is `UNAVAILABLE`.
- Query results stream in chunks. A client concatenates them and stops at
  `last = true`.
- `Subscribe.from_basis_t` is exclusive. The server backfills every later
  transaction without gaps, then continues live.
- The subscription handshake advertises the heartbeat interval. A client
  treats silence for a few multiples of it as a dead upstream.
- `Transact` gives read-your-writes on the serving peer before it responds.

A client is conformant when it reproduces the behavioral corpus in
`tests/conformance`.

## Language clients

| Client | Location |
|---|---|
| Rust | `corium-peer`, `corium-client` |
| Python | [`clients/python`](https://github.com/csm/corium/blob/main/clients/python/README.md) |
| Clojure | `corium-cljrs`, the `corium.api` namespace |

The Python client offers `LocalPeer`, which embeds a full peer, and
`RemotePeer`, which connects to a peer server. Both satisfy one protocol.
