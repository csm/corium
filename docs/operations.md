# Operations guide

> **Proposed:** the duties below that the CLI currently implements in-process —
> backup, restore, fork, GC, index publication, and the encryption migrations —
> are specified to move behind the
> [operator peer service](design/operator-service.md) as resumable, auditable
> jobs, with the CLI becoming a client of them. Every command in this guide
> keeps working with no service configured. Nothing here has changed yet.

## Processes and logging

`corium transactor` owns writes, logs, indexing, leases, and scheduled GC.
`corium peer-server` hosts peer-local queries for thin clients. Both accept
TLS and bearer-token flags documented by `corium <command> --help`.

The transactor's statically linked blob, root, and transaction-log storage is
selected with `--store`: `fs` (the default, under `--data-dir`), `mem` (in-memory and
ephemeral — a single process, everything lost on exit; for demos and tests,
not production), `postgres` (shared PostgreSQL storage at `--postgres-url`,
requiring a build with `--features postgres`), `turso` (an
embeddable-SQLite Turso database at `--turso-path`, requiring a build with
`--features turso`), or `s3` (shared S3 or S3-compatible storage at
`--s3-bucket`/`--s3-prefix`, requiring a build with `--features s3`). `mem`
keeps its log in the process-shared in-memory registry, `fs` keeps using
versioned log files under `--data-dir`, and `postgres`, `turso`, and `s3`
store versioned logs natively in the same backend as their blobs and roots.
Online backup reads every durable backend; restore and offline GC write a
filesystem data directory.

A separately built driver can be selected without rebuilding the transactor:

```sh
corium transactor \
  --store-plugin /opt/corium/plugins/libcorium_store_turso.so \
  --store 'turso:{"path":"/srv/corium/store.db"}' \
  --data-dir /srv/corium
```

`--store-plugin` is repeatable. `CORIUM_STORE_PLUGINS` may additionally name a
path-separator-delimited list of library files or directories. Corium does not
scan the working directory. Loading a storage plugin executes native
code inside the transactor and gives that code the backend configuration,
which may contain credentials. Install libraries and their containing
directories with the same ownership and write restrictions as the transactor
binary. Never load a plugin from a user-writable directory.

Before deployment, run the live conformance checks against a disposable
backend namespace:

```sh
corium store verify turso '{"path":"/tmp/corium-plugin-check.db"}' \
  --store-plugin /opt/corium/plugins/libcorium_store_turso.so
```

The loader rejects an incompatible Corium ABI generation or `abi_stable` type
layout and keeps accepted libraries loaded for the lifetime of the process.

`GetStorageInfo` never returns a service backend's primary write credential.
For PostgreSQL, configure a separate database role that has `SELECT` on
`corium_blobs` and `corium_roots`, then pass its URL with
`--postgres-read-only-url` (or `CORIUM_POSTGRES_READ_ONLY_URL`). If it is
missing, storage-aware peer bootstrap and online backup fail explicitly
instead of falling back to `--postgres-url`. Local filesystem and Turso
stores need no separate credential configuration.

The PostgreSQL backend creates `corium_blobs` and `corium_roots` in the
connection's current schema and stores transaction-log objects as fenced
root records with `log:` names. It uses the platform certificate store for TLS.
For example:

```sh
cargo run -p corium-cli --features postgres -- \
  transactor --store postgres \
  --postgres-url 'postgresql://corium@db.example/corium?sslmode=require' \
  --postgres-read-only-url \
    'postgresql://corium_reader@db.example/corium?sslmode=require' \
  --data-dir /srv/corium
```

The S3 backend stores blobs under `{prefix}blobs/` and roots — including
versioned transaction-log objects with `log:` names — under `{prefix}roots/`
in the target bucket, and fences root publication with S3 conditional writes
(`If-None-Match`/`If-Match`), so the bucket (or S3-compatible substitute)
must support them. The bucket itself is not created automatically — provision
it beforehand, since bucket creation
involves region and ownership choices `corium` should not make for you.
The transactor's primary credentials still come from the standard AWS
configuration chain (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
`AWS_PROFILE`, instance/task roles, etc.). Region and endpoint may come from
that chain or `--s3-region`/`--s3-endpoint-url`. Storage discovery uses
separate read-only credentials in one of two forms:

- Static keys supplied with `--s3-read-only-access-key-id` and
  `--s3-read-only-secret-access-key` (plus an optional session token). The
  operator must restrict these keys to reads of the Corium prefix.
- Short-lived AWS STS credentials generated on every `GetStorageInfo` call
  with `--s3-read-only-role-arn`. Corium attaches a session policy limited to
  `s3:GetObject` and prefix-scoped `s3:ListBucket`, so the generated token
  cannot write even if the role has broader permissions. The transactor's AWS
  identity must be allowed to assume the role.

`--s3-region` and `--s3-endpoint-url` are advertised with either form. A custom
endpoint implies path-style S3 addressing. For example:

```sh
AWS_REGION=us-east-1 \
cargo run -p corium-cli --features s3 -- \
  transactor --store s3 \
  --s3-bucket corium-prod --s3-prefix corium/ \
  --s3-region us-east-1 \
  --s3-read-only-role-arn arn:aws:iam::123456789012:role/corium-reader \
  --data-dir /srv/corium
```

Storage selection and discovery credentials can instead live in one EDN file;
explicit flags override file values:

```clojure
{:store :s3
 :data-dir "/srv/corium"
 :s3-bucket "corium-prod"
 :s3-prefix "corium/"
 :s3-region "us-east-1"
 :s3-read-only-role-arn "arn:aws:iam::123456789012:role/corium-reader"
 :s3-read-only-role-duration-seconds 900}
```

```sh
corium transactor --config /etc/corium/transactor.edn
```

Static secret fields are also available through
`CORIUM_S3_READ_ONLY_ACCESS_KEY_ID`,
`CORIUM_S3_READ_ONLY_SECRET_ACCESS_KEY`, and
`CORIUM_S3_READ_ONLY_SESSION_TOKEN`; prefer those or a protected config file
over putting secrets directly in a process argument.

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
query fuel spent. The proposed optional peer SSD segment cache adds bounded
usage, hit/miss, native-fetch, admission, eviction, and corruption metrics to
this same endpoint; its configuration and exact metric contract are in
[peer-segment-cache.md](design/peer-segment-cache.md). `corium db stats` and
the transactor `Status` RPC provide basis, index lag, counts, queue depth, and
GC counters on demand.

## Schema updates

`corium schema update` compares a schema file with the schema installed in a
database and prints what would change, what it would cost, and what it would
mean. It is read-only unless `--apply` is given:

```sh
corium schema update people --schema schema.toml
```

The plan is computed against one immutable database value, so every count in
it is measured at one basis and one schema. Each difference is reported as a
property-level change with an execution class:

| Class | Meaning |
|---|---|
| `additive` | No existing fact is inspected or rewritten (a new attribute, or cardinality one → many). |
| `validate-reindex` | Existing facts stay valid, but a bounded scan, constraint validation, or covering-index rebuild is needed (adding `index` or `unique`, changing uniqueness mode, toggling `isComponent`, retirement). |
| `rewrite` | Current facts must change first (collapsing cardinality where entities hold several values). |
| `destructive` | Information or historical interpretation would be lost (changing an attribute's value type in place). This command can never run one; the plan prints a replacement-attribute recipe instead. |

Risk is reported separately from the class: an AVET backfill is expensive but
semantically harmless, while a metadata-only `isComponent` flip can change the
meaning of every live reference. Changes whose *meaning* changes carry a
stable acknowledgement code — `component-enable`, `component-disable`,
`unique-mode-change`, `no-history-enable`, `no-history-disable`,
`retire-live-attribute`, `protection-forward-only` — printed next to the
change and passed back with `--ack`.

A file manages the declarations it contains. Installed attributes it does not
mention are reported as `unmanaged` and left alone; `--prune` turns them into
retirement requests instead. Retirement refuses new assertions while keeping
the ident, its metadata, and its history readable — it is not deletion, and
`schema update` has no hard-delete or excision facility. Idents match exactly:
a removed ident and an added ident are two changes, never an inferred rename.
Engine attributes such as `:db/txInstant` are never managed by a file.

| Flag | Effect |
|---|---|
| `--prune` | Retire installed attributes the file omits. Part of the plan digest. |
| `--json` | Emit the versioned machine contract instead of the human report. Scripts must read this, not the human rendering. |
| `--detailed-exit-code` | Exit 0 for no change and 2 for changes planned. Without it a successful plan always exits 0, so `&&` chains keep working. |
| `--apply --plan <digest>` | Apply exactly the plan whose digest was printed. |
| `--allow <class>` | Permit an execution class above `additive`. `destructive` has no allowance. |
| `--ack <change-code>` | Acknowledge a semantic change by its stable code. |

Parse, plan, stale-plan, and blocked-change failures exit 1 and carry stable
codes (`parse-error`, `plan-error`, `plan-mismatch`, `allow-required`,
`ack-required`, `blocked-change`, `apply-unsupported`) when `--json` is set.

> **Not yet implemented:** `--apply` is refused with `apply-unsupported`.
> Applying a plan needs schema to be basis-versioned data — schema
> transactions, schema generations carried through recovery roots, handshakes,
> and tx reports, and an `AlterSchema` authorization action — which this build
> does not have: attribute metadata is still fixed when a database is created.
> Planning is complete and exact; every precondition `--apply` can check
> without writing (the plan digest, blocked changes, allowances,
> acknowledgements) is still checked, in that order, before the operation is
> reported as unavailable. The staged delivery is in
> [schema-migrations.md](design/schema-migrations.md#delivery-plan).

## Index publication pacing and bulk loading

The transactor republishes its covering indexes in the background so cold
peers can bootstrap from a snapshot instead of replaying the whole log.
Each index is published as content-defined leaf chunks under a small
manifest, and only chunks absent from the store are uploaded — so a
publication writes roughly the chunks the changes landed in, not the whole
database. Building the snapshot still costs CPU proportional to the
database, which is what pacing bounds:

| Knob | Default | Effect |
|---|---|---|
| `--index-interval-ms` | 5000 | Base interval between publications. |
| `--index-backoff` | 4 | Minimum wait before the next publication, as a multiple `n` of the previous publication's duration. Bounds indexing to at most `1/(1+n)` of wall time and storage bandwidth as publications get slower; `0` disables. |
| `--index-tail-threshold` | 0 | Defer a due publication while fewer than this many new datoms are pending, so trickle writes coalesce instead of rewriting every index. `0` publishes any pending work. |
| `--index-tail-deadline-ms` | 60000 | Longest a below-threshold tail defers publication. |

Indexing is an optimization, never a durability requirement: the log append
is the commit point, and the transactor serves from its in-memory value
regardless of index lag. Deferring publication only lengthens cold-peer
bootstrap (the log tail past the published basis is replayed) and the
freshness of backups.

All four pacing knobs can also be changed per database at runtime, without
restarting the transactor, and read back the same way (omitted flags are
unchanged; overrides last until the process restarts):

```sh
corium db index-policy people --interval-ms 60000 --tail-threshold 1000000
corium db index-policy people
```

An explicit publication that bypasses pacing entirely:

```sh
corium db request-index people
```

For bulk loads, raise the tail threshold (for example to a million datoms)
so the load coalesces publications, rely on the backoff to keep the
indexing duty cycle bounded as the database grows, and finish with
`request-index` if you want the snapshot current immediately. Watch
`index_lag` in `corium db stats` or the metrics endpoint during the load;
otherwise the final tail publishes within the tail deadline of the last
transaction.

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
:as-of 2026-07-25T09:30:00Z
:since 10
:since 2026-07-25 09:30:00
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

`:as-of` and `:since` take either a transaction number or a UTC timestamp
(`YYYY-MM-DD`, optionally with `HH:MM[:SS[.mmm]]`); a timestamp selects the last
transaction committed at or before it, resolved through the `:db/txInstant`
datom every commit asserts. The SQL shell's `\as-of` and `\since` accept the
same two forms.

`:watch` tails live transaction reports until Ctrl-C. The reproducible M6
smoke script is [m6-console.txt](demo/m6-console.txt).

## Terminal dashboard (TUI)

```sh
corium tui people --transactor http://127.0.0.1:4334
```

A full-screen dashboard in the spirit of Datomic's web console, with four
panels (cycle with `Tab`, or jump with `1`–`4` outside the query editor):

- **Query** — an editor for EDN Datalog queries, `(pull …)` forms, and all
  of the console `:commands` above (`:as-of`, `:history on`, `:schema`, …).
  `Enter` runs when the form's brackets balance; otherwise it inserts a
  newline (`Alt-Enter` always does). Relation results render as a scrollable
  table with `:find` headers; every run reports wall-clock time, datoms
  scanned, and the basis-t it executed against. `↑`/`↓` recall history.
- **Metrics** — data-store statistics sampled from the transactor `Status`
  RPC on `--refresh-ms` (default 2000): basis/index basis and lag, datom,
  entity, and attribute counts, commit queue depth, transaction totals and
  failure rate, indexing and GC counters, and lease ownership, plus
  sparklines of transaction frequency, peer-observed status round-trip
  latency, and index lag, and peer-side query latency (last/avg/max).
- **Transactions** — a live feed from the peer's tx-report subscription
  (`t`, commit time, datom count) with a per-transaction datom detail pane;
  `f` toggles follow-newest.
- **Schema** — the attribute table (ident, value type, cardinality,
  uniqueness, index/component/history flags), filterable with `/`.

Quit with `Ctrl-C` anywhere, `q` outside the query editor, or `:quit`.

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

This section describes the implemented pair topology. The proposed
[transactor fleet](design/transactor-fleet.md) keeps the same lease and
failover guarantees but distributes databases across overlapping candidate
sets and gives clients one load-balanced address. It does not change the
current commands or runbook yet.

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

Backup is online. It contacts the running transactor once to fix the current
transaction basis and obtain connection details for the underlying storage,
then reads the storage log independently only through that basis. Transactions
committed while the backup runs are left for the next incremental run.

```sh
corium backup --transactor http://127.0.0.1:4334 people /backups/people.corium
```

Run the same command with the same file for an incremental refresh. The backup
reads only transaction records after its existing checkpoint and appends one
new checkpoint frame; the report prints `:replayed-transactions`. It retains
the first backup's index snapshot as a replay base and embeds its immutable
snapshot blobs only on that first run.

A backup has exactly one representation: a binary `.corium` archive. Its
header carries an independent backup-file format version and the Corium
version that created it; every incremental checkpoint records the version
that appended it. Unsupported future formats fail before restore and identify
their writer. `--log-format human|json` controls diagnostic logging only and
never changes the backup artifact. Human/JSON/EDN export belongs in a future
`dump` command rather than in backup or restore.

Filesystem and Turso backups must run where the transactor's absolute local
storage path is accessible. PostgreSQL and S3 clients connect to the same
native storage advertised by the transactor (S3 credentials still come from
the standard AWS environment). Process-local memory storage cannot be opened
by a separate backup process and is rejected clearly. The advertised
PostgreSQL connection is read/write in this first version; a future release
can substitute read-only credentials without changing the replay protocol.

Restore remains offline and refuses to overwrite a database. Restoring under
a new name creates a clone:

```sh
corium restore /backups/people.corium --data-dir /srv/corium-restored --as-db people
corium restore /backups/people.corium --data-dir /srv/corium --as-db people-staging
```

After restore, start the target transactor and compare `corium db stats` with
the backup report's basis. Backup-container and database-storage versions are
checked separately before publication.

## Forking a database

A fork creates a new database that duplicates an existing one at a
transaction basis — a sandbox wound back to a point in time, useful for
debugging against real data or trying an alternative approach without
touching the original. Unlike backup/restore, forking is online: it runs
against the live transactor through the catalog service.

```sh
corium db fork people people-debug --as-of 1234
corium db fork people people-scratch          # fork at the current basis
```

The fork copies only the transaction-log prefix through the requested basis
(every `t` up to the source's basis is a transaction, so any value in range
is exact); schema metadata is shared and index segments dedupe by content
address in the blob store. The new database replays that prefix, publishes
its own indexes, and from then on transacts completely independently of its
source. The command prints the fork's basis:

```
{:db "people-debug" :forked-from "people" :basis-t 1234 :created true}
```

Forking refuses a basis ahead of the source and never overwrites: an
existing target reports `:created false` and nothing is changed. Note that
a read-only point-in-time view does not need a fork — peers get one locally
with `as-of` — so fork only when the sandbox must accept writes.

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

## Encryption at rest

Every durable artifact of an encrypted database — index blobs, transaction-log
record payloads, and cached segments — is sealed under a per-database data key
that is itself wrapped by a key-encryption key (KEK) Corium never stores. See
[docs/design/encryption.md](design/encryption.md) for the model and
[ADR-0017](adr/0017-encryption-at-rest.md) for the decision.

Encryption is fixed at creation. A database created without a storage key stays
unencrypted forever; migrating one is a backup and restore into a new database.

A key identity is a URI. `file:/etc/corium/storage.key` and `env:CORIUM_KEK`
resolve locally and hold 32 raw bytes or 64 hexadecimal characters (surrounding
whitespace is ignored). KMS identities (`awskms:`, `gcpkms:`, `vault:`) are
recognized but not yet resolvable.

```sh
# A KEK. Keep it off the machine holding the data if you can.
head -c 32 /dev/urandom > /etc/corium/storage.key && chmod 400 /etc/corium/storage.key

# Every process that reads storage directly needs it. Repeat the flag when a
# node hosts databases under different KEKs; CORIUM_STORAGE_KEY works too.
corium transactor  --data-dir /srv/corium --storage-key file:/etc/corium/storage.key
corium peer-server --db people --peer-bootstrap --storage-key file:/etc/corium/storage.key

# The transactor resolves the key, so no material leaves this host.
corium db create people --schema schema.toml --storage-key file:/etc/corium/storage.key
```

Thin clients and peer-server callers need no key: they receive plaintext over
TLS. A process that *should* hold a key and does not fails at open, naming the
key, rather than at its first read.

Inspect and rotate keys one database at a time:

```sh
corium keys status people                       # epochs, states, nonce budget
corium keys rotate people                       # open a new epoch; rewrites nothing
corium keys rewrap people --kek file:/etc/corium/storage-2026.key
```

`rotate` opens a new storage-key epoch that new writes use immediately. Older
epochs stay readable and drain as ordinary re-indexing rewrites their objects;
an epoch retires only when no live object carries it. Rotate when `corium keys
status` reports `:rotation-due true`, which fires at half the log-record nonce
budget — log records use a random 96-bit nonce, so an epoch must seal well under
2³² records, and that count is simply the span of `t` the epoch covers.

`rewrap` re-encrypts the data keys under a new KEK and touches no stored object.
The transactor must be able to resolve both KEKs at once, so start it with both
`--storage-key` flags, re-wrap, then drop the old one.

### When a node cannot load a key change

A key change made elsewhere — by an operator against another process, or by the
other half of an HA pair — is picked up within a lease-renewal tick. When that
load fails, what happens next depends on *which* change it was, and
`corium keys status` reports both states:

| Field | Meaning | Effect |
|---|---|---|
| `:keys-unavailable true` | The manifest changed but this node could not load it; its existing keys still open the database and still write under the active epoch. Typically a re-wrap to a KEK it cannot resolve, or an unreachable KMS. | Warning only. Reads and writes continue; the `corium_keys_unavailable` gauge rises. |
| `:keys-fenced true` | The manifest opened a storage-key epoch this node cannot load, so it would keep sealing records under one the manifest has closed. | **Writes refuse** with `FAILED_PRECONDITION` naming both epochs. Reads, index publication, and the lease continue. |

The distinction is deliberate. A re-wrap leaves the data keys themselves
unchanged, so refusing writes would turn a KMS outage into a write outage for no
confidentiality gain. A rotation is different in kind: the log-record nonce
budget is measured as the span of `t` between epochs, so records sealed under a
closed epoch are drawn against a budget that has stopped counting them.

Both clear as soon as a load succeeds. The fix is the same either way — give the
process a `--storage-key` that resolves the KEK the manifest now names, and
restart it — but only the fenced state stops writes while you do.

Two operational consequences worth planning for:

- **Offline commands need the key.** `corium log --data-dir` and
  `corium gc --data-dir` read blob and log content, so pass `--storage-key`.
  Offline GC refuses to run without it rather than sweep every index chunk it
  cannot follow.
- **Backup does not support encrypted databases yet.** `corium backup` refuses
  one, because copying its ciphertext without the key manifest (backup format 2)
  would produce an archive no restore could open.

## Attribute protection

Storage encryption protects the medium. **Attribute protection** protects
facts from readers: values on an attribute that names a protection class are
sealed by the writing peer under that class's key, and only a process whose
keyring resolves that key ever sees them in the clear — not the transactor,
not a peer without the key, not an operator with storage credentials. Declare
classes in the create-time schema
([docs/schema-toml.md](schema-toml.md#protection-classes)); see
[docs/design/encryption.md](design/encryption.md) and
[ADR-0018](adr/0018-attribute-protection-classes.md).

Class keys are resolved by identity, exactly like a KEK, and are passed to
whichever processes should read the class. A process with no class keys is a
fully working peer: it commits, indexes, syncs, and answers every query that
does not touch a protected value identically, and protected values come back
redacted, hidden, or refused according to the class's `on-missing-key` policy.

> **Do not expose a key-holding peer server to clients that are not entitled
> to every class it holds.** A peer server hydrates every request with its own
> keyring, whichever principal made it: the per-principal key policy that
> would narrow this — and seal-through mode, where the server forwards sealed
> values and the thin client hydrates for itself — are not implemented. The
> authorization guard gates databases and operations, not classes, so it does
> not close the gap either. Until they land:
>
> - Give a peer server class keys only when **every** client it serves may
>   read **every** class it holds.
> - For mixed or less-trusted clients, run the peer server with no class keys
>   and let entitled applications use an embedded peer (`LocalPeer`) with
>   their own keyring, where the keys stay in the process that owns them.
> - Splitting by class means splitting by peer server for now: one per
>   entitlement group, each holding only its own keys.

Protection is fixed at database creation, like encryption at rest: changing an
attribute's protection needs a schema-alteration mechanism Corium does not yet
have, so plan classes before you create the database.

## Authorization (self-hosted ReBAC)

Servers authorize every request permit-all by default. `--authz-db <name>`
switches them to the relationship policy stored in a Corium database — see
[docs/design/auth.md](design/auth.md) for the model. Bootstrap is two steps,
in this order:

```sh
# 1. Against a transactor started WITHOUT --authz-db:
corium authz init --admin alice --provider oidc     # schema + permissions + first owner
corium authz grant 'group:eng#member' writer database:music
corium authz grant bob member group:eng
corium authz check bob transact --database music    # dry-run the decision

# 2. Restart the surfaces with enforcement on:
corium transactor  --data-dir /srv/corium --authz-db corium_authz
corium peer-server --db music --authz-db corium_authz
```

`authz init` grants its administrator `owner` on `catalog:*` and `database:*`.
It defaults to the identity a `--serve-token` client presents (`operator`,
pinned to `static-token`), so the CLI keeps working after enforcement is on;
pass `--admin`/`--provider` for a real identity, or `--no-admin` to grant
nobody anything.

Operating notes:

- **Fail closed.** A surface that cannot read or compile the policy denies
  every request. It does not refuse to start: it logs the remedy and recovers
  on its own once the database appears, so ordering mistakes are not fatal.
- **Changes propagate without a restart.** Each server watches the policy
  database and recompiles off the request path; a `grant` takes effect in
  milliseconds. `corium authz status` shows the compiled basis (`:authz-t`) and
  entity counts, and every decision logs the basis it used under the
  `corium_authz::audit` tracing target (denials at `info`, grants at `debug`).
- **`--authz-fresh-writes`** makes write and admin actions re-read the policy
  before deciding, at the cost of a snapshot read per such request.
- **Locked yourself out?** Break-glass (`--authz-break-glass-role admin`) only
  applies when the policy is *unreadable*, never to override a deny. The
  recovery path for a policy that denies everyone is to restart the transactor
  without `--authz-db`, fix the tuples with `corium authz grant`, and restart
  with it again.
- The policy database is an ordinary database: back it up, restore it, and
  inspect it (`corium console corium_authz`) like any other. Access to it is
  itself governed by the policy it holds — the administrator's `database:*`
  ownership is what keeps `corium authz grant` working.

## Recovery checklist

1. Stop the affected transactor and preserve its data directory.
2. Restore the newest backup into an empty directory/name.
3. Start a transactor on the restored directory and wait for index lag to
   reach zero.
4. Compare basis, datom/entity/attribute counts, and a known query result.
5. Redirect peers only after those checks pass.
