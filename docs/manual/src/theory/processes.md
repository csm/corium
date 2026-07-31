# Processes and roles

Corium separates three roles. Early versions can run them in one process, but
no part of the core assumes that they share a process.

```text
                    ┌────────────────────┐
   transact ───────►│     Transactor     │──── append ────► ┌─────────────┐
                    │  (single writer)   │──── segments ──► │   Storage   │
                    │  tx pipeline       │                  │   service   │
                    │  indexing job      │                  │ (blob store │
                    └─────────┬──────────┘                  │  + roots)   │
                              │ tx-report stream            └──────┬──────┘
              ┌───────────────┼───────────────┐                    │
              ▼               ▼               ▼            read segments
        ┌──────────┐    ┌──────────┐    ┌───────────┐              │
        │   Peer   │    │   Peer   │    │Peer server│ ◄────────────┘
        │ (in-proc │    │          │    │ (hosts db │
        │  query)  │    │          │    │ for thin  │
        └──────────┘    └──────────┘    │  clients) │
                                        └─────┬─────┘
                                              │ gRPC query/transact
                                        ┌─────┴─────┐
                                        │Thin client│ (any language)
                                        └───────────┘
```

## Storage service

The storage service is passive. It has a blob store for immutable segments and
a root store for named pointers.

Five backends exist: `mem`, `fs`, `postgres`, `turso`, and `s3`. See
[storage backends](../running/storage.md).

## Transactor

The transactor is the single writer for a database. It serializes
transactions, validates them, appends to the log, acknowledges the caller, and
streams reports to peers.

A background job publishes fresh index trees. Exactly one transactor holds the
write lease for a database at a time.

One transactor process serves many databases. It can be active for some
databases and standby for others.

## Peer

A peer is a library in the application process. It keeps a live connection for
transaction reports, reads segments from storage through a cache, and merges
them into an immutable database value.

All query execution happens on the peer: Datalog, pull, the entity API, index
scans, and time views. Getting a database value never blocks on the
transactor.

Peer state is either immutable or disposable. A peer crash loses nothing.

## Peer server and thin clients

The peer server is a peer hosted as a standalone process. It exposes query,
pull, and transact over gRPC for languages without the peer library.

One peer server hosts one database. See
[peer server and thin clients](../surfaces/peer-server.md).

## Operator service

> **Not implemented.** An operator peer service is specified. The design runs
> backup, restore, fork, garbage collection, index publication, and the
> encryption migrations as resumable, auditable jobs, behind an API and a web
> interface. The CLI runs all of those duties in-process today. See
> [operator-service.md](https://github.com/csm/corium/blob/main/docs/design/operator-service.md).

## Transactor fleet

> **Not implemented.** A fleet topology is specified. The design places many
> databases across many machines behind one client address, with the same
> lease and failover guarantees. The implemented topology is an active and
> standby pair. See
> [transactor-fleet.md](https://github.com/csm/corium/blob/main/docs/design/transactor-fleet.md).

## Which process needs what

| Process | Needs transactor address | Needs storage credentials | Needs storage key |
|---|---|---|---|
| `corium transactor` | No | Yes | Yes, for encrypted databases |
| `corium peer-server` | Yes | Only with `--peer-bootstrap` | Yes, for encrypted databases |
| `corium console`, `tui`, `sql` | Yes | Only with `--peer-bootstrap` | Yes, for encrypted databases |
| `corium postgres-server` | Yes | No | No |
| Thin client | No, it uses the peer server | No | No |
| `corium backup` | Yes | Yes | Not supported yet |
| `corium restore`, offline `gc`, `log` | No | Local data directory | Yes, for encrypted databases |

A thin client receives plaintext over TLS. It never holds a storage key.
