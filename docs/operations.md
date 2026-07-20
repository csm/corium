# Operations guide

## Processes and logging

`corium transactor` owns writes, logs, indexing, leases, and scheduled GC.
`corium peer-server` hosts peer-local queries for thin clients. Both accept
TLS and bearer-token flags documented by `corium <command> --help`.

The transactor's blob and root storage is selected with `--store`: `fs` (the
default, under `--data-dir`), `mem` (in-memory and ephemeral — a single
process, everything lost on exit; for demos and tests, not production),
`postgres` (shared PostgreSQL blobs and roots at `--postgres-url`, requiring a
build with `--features postgres`), or `turso` (blobs and roots in an
embeddable-SQLite Turso database at `--turso-path`, requiring a build with
`--features turso`). The transaction log is appended synchronously by the
commit pipeline, so it stays on the local filesystem under `--data-dir` for
`fs`, `postgres`, and `turso`, and in memory for `mem`. A database-backed
transactor therefore still needs a writable data directory for its logs.
Backup, restore, and offline GC operate on the filesystem data directory and
therefore apply to `fs` (and only the local logs of a database-backed
transactor).

The PostgreSQL backend creates `corium_blobs` and `corium_roots` in the
connection's current schema and uses the platform certificate store for TLS.
For example:

```sh
cargo run -p corium-cli --features postgres -- \
  transactor --store postgres \
  --postgres-url 'postgresql://corium@db.example/corium?sslmode=require' \
  --data-dir /srv/corium
```

Tracing is human-readable by default. Use `--log-format json` for structured
logs and `RUST_LOG` for filtering:

```sh
RUST_LOG=corium_transactor=debug,corium_peer=info \
  corium --log-format json transactor --data-dir /srv/corium
```

Pass `--metrics-listen 127.0.0.1:9464` to a transactor or peer server to
serve Prometheus text at `/metrics`. Keep this listener on a private
operations network; it has no application bearer-token authentication.
Transactor metrics cover transaction count/failures/latency, commit queue
depth, indexing duration, and GC. Peer metrics cover query count/latency and
query fuel spent. `corium db stats` and the transactor `Status` RPC provide
basis, index lag, counts, queue depth, and GC counters on demand.

## Query console

```sh
corium console people --transactor http://127.0.0.1:4334
```

Enter an EDN Datalog query or use:

```clojure
(pull [:person/name :person/age] 1000)
```

Console commands are:

```text
:basis
:as-of 10
:since 10
:history on
:history off
:current
:schema
:schema person/name
:stats
:timing on
:watch
:quit
```

`:watch` tails live transaction reports until Ctrl-C. The reproducible M6
smoke script is [m6-console.txt](demo/m6-console.txt).

## High availability (active/standby)

One transactor holds the write lease per database; a warm standby polls the
lease and takes over when it lapses. Start both members identically with
`--ha`, pointing at the same (shared) data directory, each advertising its
own client endpoint:

```sh
corium transactor --data-dir /srv/corium --ha \
  --owner txor-a --advertise http://txor-a:4334 --listen 0.0.0.0:4334
corium transactor --data-dir /srv/corium --ha \
  --owner txor-b --advertise http://txor-b:4334 --listen 0.0.0.0:4334
```

Whichever starts first becomes active; the other stands by, rescans the
catalog every lease-renewal interval (so databases created on the active are
picked up), and rejects client work with a `standby` FAILED_PRECONDITION
naming the current lease holder. Give `--owner` a stable identity per
member: a restarted member re-acquires its own unexpired lease immediately.

Peers list both endpoints and fail over automatically:

```sh
corium peer-server --db people \
  --transactor http://txor-a:4334,http://txor-b:4334
```

Library peers pass the same list via `ConnectConfig::with_failover`; peers
with storage credentials can also rediscover the current holder's advertised
endpoint from the database root (`corium db stats` prints it, and
`SegmentSource::lease_holder_endpoint` reads it directly).

### Failover behavior and guarantees

- Takeover is ordinary crash recovery: the standby acquires the lapsed
  lease (which atomically fences the deposed writer), replays the log tail,
  and serves. No acknowledged transaction is ever lost or duplicated, and a
  deposed transactor can never publish — these properties are enforced by a
  post-append ownership check before every acknowledgement and exercised by
  the M7 simulation and integration batteries.
- Writes are unavailable from the crash until takeover: at worst one lease
  TTL (the active's last renewal has to expire) plus one standby poll
  interval (TTL/3) plus reconnect backoff.
- Peer subscriptions reconnect and backfill gaplessly. `transact` calls
  that fail before reaching the commit point (standby rejection, connection
  refused) are retried transparently within `failover_timeout`. A call whose
  connection died mid-request is ambiguous — the transaction may or may not
  have committed — and surfaces an error, exactly like a transactor crash
  between durability and reply; on such an error, `sync` and check before
  resubmitting.
- A deposed member (GC pause, partition) refuses further work and returns
  to standby on its own; no operator action is needed.

### Tuning

| Knob | Default | Effect |
|---|---|---|
| `--lease-ttl-ms` | 5000 | Failover detection bound; renewals run at TTL/3. Lower = faster takeover, more root-store traffic, less tolerance for GC/IO pauses on the active. |
| `--heartbeat-ms` | 10000 | Subscription heartbeats; peers presume the transactor dead after 3 missed intervals and fail over even when TCP has not noticed (partitions). Keep at or below the lease TTL for prompt peer failover. |
| Peer `reconnect_min`/`reconnect_max` | 100ms/5s | Reconnect backoff while rotating endpoints. |
| Peer `failover_timeout` | 30s | How long safe-to-retry transact failures ride out a takeover. |

The lease lives in the same CAS-fenced root record as the published
indexes, so the root store is the single arbiter; clock skew between members
only shifts detection latency, never safety. The transaction log is written
as per-lease-version files (`<db>.v<N>.log`); readers merge them and old
files are inert history — never edit or delete them by hand.

### HA runbook

Planned failover (maintenance on the active):
1. Stop the active gracefully (Ctrl-C). It releases its leases on the way
   out, so the standby takes over on its next poll (within TTL/3) with no
   expiry wait.
2. Watch the standby's log for `standby took over write lease`, or poll
   `corium db stats` until `:lease-owner` names the standby.
3. Do the maintenance; restart the member with the same `--owner` and
   `--ha`. It rejoins as standby.

Crashed active:
1. Nothing is required for service: the standby takes over within
   TTL + TTL/3. Confirm via `:lease-owner`/`:lease-owner-endpoint` in
   `corium db stats` and basis progress.
2. Restart the crashed member under its supervisor with `--ha`; it rejoins
   as standby. Investigate the crash afterwards, not before.

Split brain suspicion (both members claim ownership in their logs):
- Not possible for durable state: the root record is owned by exactly one
  lease version and every publish/ack is fenced by it. A member logging
  `deposed` messages is the loser and will stand down; trust
  `corium db stats` (which reads the root record), not process logs.

Both members down:
1. Start either member (prefer the one with newest data-directory mtimes if
   storage is not shared). It waits out any unexpired lease (up to
   `--lease-wait-ms` without `--ha`, indefinitely with it) and recovers by
   log replay.
2. Start the second member; it becomes standby.

Storage requirements: both members must see the same blob/root store and
log directory (shared filesystem in v1). The store is the source of truth;
never run members against diverged copies of a data directory.

## Backup and restore

Backup and restore are offline in v1: stop the transactor that owns the data
directory first. A backup contains the durable log, schema/naming metadata,
the database root, and every immutable blob reachable from that root.

```sh
corium backup --data-dir /srv/corium people /backups/people
```

Run the same command with the same destination for an incremental refresh.
Only hashes absent from the destination are copied; the report prints
`:copied-blobs` and `:reused-blobs`. The log and small root/manifest files are
refreshed atomically each time. Do not combine different source databases in
one backup directory.

Restore refuses to overwrite a database. Restoring under a new name creates
a clone:

```sh
corium restore /backups/people --data-dir /srv/corium-restored --as-db people
corium restore /backups/people --data-dir /srv/corium --as-db people-staging
```

After restore, start the target transactor and compare `corium db stats` with
the backup report's basis. The manifest and database root carry a format
version; a newer unsupported format fails clearly before publication.

## Garbage collection

The transactor runs GC hourly by default and retains unreachable blobs for 72
hours. Tune with `--gc-interval 1h` and `--gc-window 72h`, or disable the
scheduled duty with `--gc-interval off`. GC is serialized with index
publication.

Manual online and offline collection use the same retention rule:

```sh
corium gc --transactor http://127.0.0.1:4334 --window 72h
corium gc --data-dir /srv/corium --window 72h
```

Use a zero window only when no stale root or in-flight reader can exist.

## Recovery checklist

1. Stop the affected transactor and preserve its data directory.
2. Restore the newest backup into an empty directory/name.
3. Start a transactor on the restored directory and wait for index lag to
   reach zero.
4. Compare basis, datom/entity/attribute counts, and a known query result.
5. Redirect peers only after those checks pass.
