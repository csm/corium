# Runbooks

Each runbook is a procedure. Read the whole procedure before you start it.

## Planned failover

Use this before maintenance on the active member.

1. Stop the active member with `Ctrl-C`, or with `SIGINT`. It releases its
   leases on the way out.
2. Watch the log of the standby for `standby took over write lease`. Takeover
   happens within one third of the lease time-to-live.
3. Do the maintenance.
4. Start the member again with the same `--owner` and `--ha`. It rejoins as
   standby.

If the supervisor sends `SIGTERM`, the process dies without releasing the
lease. Takeover then costs one full lease time-to-live.

## Crashed active

Nothing is required for service. The standby takes over within the
time-to-live plus one third.

1. Confirm the takeover. Watch the basis advance with `corium db stats`, and
   read the lease owner in the `Metrics` panel of `corium tui`.
2. Start the crashed member again under its supervisor with `--ha`. It rejoins
   as standby.
3. Investigate the crash afterward, not before.

## Split-brain suspicion

Both members print ownership messages in their logs.

This is not possible for durable state. The root record is owned by exactly
one lease version, and every publish and acknowledgement is fenced by it.

1. A member that logs `deposed` is the loser. It stands down on its own.
2. Trust the root record, not the process logs. `corium tui` reads the lease
   owner from the `Status` call.
3. Take no other action.

## Both members down

1. Start either member. Prefer the one with the newest data-directory
   modification times if storage is not shared.
2. The member waits out any unexpired lease. Without `--ha` it waits up to
   `--lease-wait-ms`. With `--ha` it waits without limit.
3. It recovers by log replay, and it serves.
4. Start the second member. It becomes standby.

## Recovery from a backup

1. Stop the affected transactor, and preserve its data directory. Do not
   delete it.
2. Restore the newest backup into an empty directory, or under a new name:
   `corium restore <file> --data-dir <empty-dir> --as-db <name>`.
3. Start a transactor on the restored directory.
4. Wait until `:index-lag` in `corium db stats` reaches zero.
5. Compare the basis, the datom count, the entity count, and the attribute
   count with the backup report.
6. Run a known query, and compare the result.
7. Redirect peers only after those checks pass.

## Transactor will not start

Read the error first. Four causes are common.

| Error names | Cause | Fix |
|---|---|---|
| A lease holder | Another transactor holds the lease. | Add `--ha` to stand by, or stop the other member. |
| A storage key | The process cannot resolve a named KEK. | Give it a `--storage-key` that resolves. |
| A Cargo feature | The binary lacks the storage or OIDC feature. | Rebuild with the feature. |
| A missing data directory | `--data-dir` is absent. | Pass it, or set `:data-dir` in the configuration file. |

The process fails at startup for a key it cannot resolve. That is by design.
It does not fail later at the first read.

## Writes refuse with `FAILED_PRECONDITION`

Two causes give this status.

**A standby.** The message names the current lease holder. Point the client at
that endpoint, or pass the whole endpoint list.

**A fenced key.** Run `corium keys status <db>`. If `:keys-fenced` is `true`,
the manifest opened an epoch that this node cannot load.

1. Give the transactor a `--storage-key` that resolves the KEK that the
   manifest names.
2. Restart the transactor.
3. Confirm that `:keys-fenced` is `false`.

See [encryption at rest](../security/encryption.md).

## An ambiguous transaction

The result of a `transact` call whose connection died mid-request is unknown.
The transaction is committed, or it is absent.

1. Do not resubmit yet.
2. Run `sync` on the connection.
3. Read the data back and decide from the result.
4. Resubmit only if the write is absent.

## Changing the schema of a live database

Writes continue throughout. Only a blocked change stops the procedure.

1. Edit the schema file. Keep every attribute that must survive, because a
   file that omits one reports it as unmanaged.
2. Plan it: `corium schema update <db> --schema <file>`. Nothing is written.
3. Read every execution class, every count, and every acknowledgement code.
4. Run the invocation that the last line of the plan prints, and add the path
   of the schema file.
5. Confirm the new schema generation in the output of the apply.
6. Compare `:attributes` in `corium db stats` with the expected count.

A plan is invalidated by a schema change or a failed condition, never by
ordinary data writes. Re-plan if the digest is refused.

See [schema management](../running/schema.md#updating-an-installed-schema).

## A schema plan is blocked or refused

Read the reason that the plan prints under the change.

| Reason | Meaning | Fix |
|---|---|---|
| `value-type-mutation` | A value type cannot change in place. | Follow the replacement-attribute recipe that the plan prints. |
| `unique-duplicates` | Duplicate values exist. | Retract the duplicates, then plan again. |
| `cardinality-conflicts` | An entity holds several values where the file asks for one. | Choose a winner per entity and retract the rest. |
| `ever-protected` | The attribute has been protected at some time. | It can never gain `index` or `unique`. Drop them from the file. |
| `protection-conflict` | The declaration combines protection with `index`, `unique`, or `ref`. | Drop the conflicting property from the declaration. |

An apply can also fail after a clean plan.

| Error code | Meaning | Fix |
|---|---|---|
| `plan-mismatch` | The schema changed between the plan and the apply. | Plan again and read the new plan. |
| `allow-required` | A change needs `--allow <class>`. | Add the exact allowance the plan names. |
| `ack-required` | A change needs `--ack <code>`. | Add the exact code the plan names. |

## Every request is denied

The policy denies, or the policy is unreadable.

1. Run `corium authz status`. A missing basis means the policy is unreadable.
2. If it is unreadable, a break-glass role admits an operator. See
   [authorization](../security/authorization.md).
3. If the policy denies, stop the transactor.
4. Start it again without `--authz-db`.
5. Fix the tuples with `corium authz grant`. Test each one with
   `corium authz check`.
6. Restart with `--authz-db`.

## Index lag grows without limit

1. Read `:index-lag` in `corium db stats`, and the publication duration in the
   metrics endpoint.
2. Lower `--index-backoff`, so publication takes a larger share of wall-clock
   time.
3. Lower `--index-tail-threshold` if a large threshold defers the work.
4. If neither helps, the storage backend is the limit. Give it faster storage,
   or reduce the write rate.

Index lag never risks durability. It lengthens cold-peer bootstrap and it
makes a backup less fresh.

## A peer uses too much memory

A peer holds every datom that it has seen, including retractions.

1. Confirm the cause. Memory tracks total history, not the size of the live
   database.
2. Restart the peer with `--peer-bootstrap`, so it starts from the published
   snapshot rather than replaying the log from basis 0.
3. Avoid opening many distinct time views in one process. Each distinct view
   costs a fold of the whole history.
4. Split the workload across more peer processes.

See [indexes and storage](../theory/indexes.md).

## Backing up an encrypted database

`corium backup` refuses an encrypted database.

1. Stop the transactor, or accept a crash-consistent copy.
2. Copy the underlying storage with its own tool. Use a filesystem snapshot,
   a PostgreSQL dump, or S3 replication.
3. Copy the KEK separately, and keep it in a different system.
4. Test the restore path on a separate host before you rely on it.

## Storage is full

1. Run a manual sweep: `corium gc --transactor <url> --window 72h`.
2. If that reclaims little, read
   `corium_transactor_gc_retained_blobs_total`. A large retained count means
   that the window is holding the blobs.
3. Lower `--gc-window` only when no reader holds a root older than the new
   window.
4. Delete finished forks and staging clones with `corium db delete`.

## Emergency: recreate a database from the log

The log is the source of truth. A data directory with an intact log recovers
by replay.

1. Stop every transactor that touches the directory.
2. Preserve a copy of the whole directory.
3. Start one transactor on the directory. Startup replays the log tail after
   the last published index basis.
4. Compare `corium db stats` with the last known values.

Never edit or delete files under `<data-dir>/logs` by hand. Old lease-version
files are inert history that readers must merge.
