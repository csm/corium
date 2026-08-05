# Forking a database

A fork creates a new database that duplicates an existing one at a transaction
basis. The result is a sandbox wound back to a point in time.

Use a fork to debug against real data, or to try an alternative approach,
without touching the original.

Unlike backup and restore, forking is online. It runs against the live
transactor through the catalog service.

## Commands

```sh
corium db fork people people-debug --as-of 1234
corium db fork people people-scratch
```

Without `--as-of`, the fork is taken at the current basis.

The command prints one EDN map:

```clojure
{:db "people-debug" :forked-from "people" :basis-t 1234 :created true}
```

## What a fork copies

The fork copies only the transaction-log prefix through the requested basis.
Every `t` up to the basis of the source names a transaction, so any value in
range is exact.

Schema metadata is shared. Index segments deduplicate by content address in
the blob store, so a fork is cheap in storage.

The new database replays that prefix, publishes its own indexes, and then
transacts completely independently of its source.

## Rules

- A basis ahead of the source is refused.
- An existing target is never overwritten. The command prints
  `:created false`, and nothing is changed.
- The fork is a full database. It accepts writes, it needs its own backups,
  and garbage collection covers it.

## When not to fork

A read-only view of a point in time does not need a fork. A peer gets one
locally with `as-of`, at no storage cost.

Fork only when the sandbox must accept writes.

## Cleaning up

Delete a finished fork like any other database:

```sh
corium db delete people-debug
```

Blobs shared with the source stay reachable from the source root. Garbage
collection sweeps only what no live root reaches. See
[garbage collection](gc.md).
