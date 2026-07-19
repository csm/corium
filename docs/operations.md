# Operations guide

## Processes and logging

`corium transactor` owns writes, logs, indexing, leases, and scheduled GC.
`corium peer-server` hosts peer-local queries for thin clients. Both accept
TLS and bearer-token flags documented by `corium <command> --help`.

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
