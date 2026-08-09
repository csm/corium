# Installation

## Toolchain

Corium builds with a stable Rust toolchain, version 1.88 or newer. It uses
edition 2024.

Build the CLI:

```sh
cargo build -p corium-cli --release
```

The binary is `target/release/corium`. Copy it to a directory on the path of
the operator, for example `/usr/local/bin/corium`.

Run the test suite before you promote a build:

```sh
./scripts/test-rust.sh
```

## Cargo features

Optional backends and authentication methods are Cargo features of
`corium-cli`. A feature that is not compiled in makes its flags fail at
startup with a clear error.

| Feature | Default | Enables |
|---|---|---|
| `cljrs` | Yes | The `:db/fn` Clojure transaction-function runtime. |
| `postgres` | No | `--store postgres`. |
| `turso` | No | `--store turso`. |
| `s3` | No | `--store s3`. |
| `oidc` | No | OIDC bearer tokens with a JWKS file. |
| `oidc-discovery` | No | OIDC, and JWKS fetch from the issuer. |

Build a production binary with the backends that you deploy:

```sh
cargo build -p corium-cli --release --features postgres,s3,oidc-discovery
```

A backend can also be loaded at run time instead of compiled in. Build the
driver crate on its own, and give the transactor its library path:

```sh
cargo build -p corium-store-turso --release
```

Do not enable the `static-link` feature when you build a loadable library.
That feature is for a host that links the driver in. See
[storage plugins](storage.md#storage-plugins).

## Workspace build note

`corium-cljrs` and the MusicBrainz example are excluded from the default
workspace members. A `--workspace` build unifies the Clojure runtime into
`no-gc` mode and degrades their garbage-collection semantics.

The repository test script runs the workspace and those two crates in
separate Cargo invocations:

```sh
./scripts/test-rust.sh
```

## What a deployment needs

A minimal deployment has one transactor process and one storage backend.

Add a peer server only when a client language has no peer library. Add a
PostgreSQL wire server only when a SQL client must reach the data.

| Process | Default port |
|---|---|
| `corium transactor` | 4334 |
| `corium peer-server` | 4336 |
| `corium postgres-server` | 5432 |
| Metrics endpoint | None. Set `--metrics-listen`. |

## Directory layout of the `fs` store

The filesystem store keeps two directories under `--data-dir`.

| Path | Content |
|---|---|
| `<data-dir>/store` | Blobs and root records. |
| `<data-dir>/logs` | Versioned transaction log files. |

Back up the data directory as a unit, or use
[`corium backup`](../availability/backup.md). Do not edit files in either
directory by hand.

## Process supervision

Run the transactor under a supervisor, such as systemd. Two rules apply.

- Give the transactor a stable `--owner` value. A restarted member re-acquires
  its own unexpired lease at once.
- Stop the transactor with `SIGINT`, which `Ctrl-C` sends. The transactor
  releases its leases on the way out. A standby then takes over without
  waiting for the lease to expire.

> **Partly implemented.** The transactor and the peer server listen for
> `SIGINT` only. `SIGTERM` kills the process, which leaves the lease held
> until it expires. A shutdown by `SIGTERM` is safe, because takeover is
> ordinary crash recovery, but failover then costs one full lease
> time-to-live.

For systemd, set the stop signal explicitly:

```ini
[Service]
ExecStart=/usr/local/bin/corium transactor --config /etc/corium/transactor.edn
KillSignal=SIGINT
Restart=on-failure
```
