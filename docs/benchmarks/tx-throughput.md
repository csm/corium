# Transactor write-throughput benchmark

`crates/corium-transactor/examples/tx_throughput.rs` measures the transactor's
single-writer commit pipeline end to end. It exists to answer the question
behind [transactor pipelining](../design/log-and-transactor.md#transaction-pipeline):
*where does a commit's wall-clock time actually go, and how much of it is
serial?* — so the payoff of each pipelining step (group commit, overlapped
validate/flush, hoisted lease checks) can be measured rather than guessed.

## What it drives

Each timed unit is one `TransactorNode::transact` call, which runs the whole
critical section a real client commit runs:

1. pre-append lease fence (a `RootStore` read),
2. `:db/fn` expansion and EDN → native conversion,
3. `prepare` — tempid/lookup resolution, schema/uniqueness validation,
   cardinality-one retraction (the one strictly serial stage),
4. the durable log append (**the commit point**),
5. the post-append lease fence (a second `RootStore` read),
6. tx-report encode + broadcast.

Every transaction inserts a fresh entity keyed by a globally unique
`:bench/key` (`:db.unique/identity`), so no commit is a no-op re-assert and
concurrent submitters never collide on a unique value — each is a genuine
insert that exercises allocation, the AVET uniqueness check, and the append.

Background indexing, GC, and heartbeats are quieted during the run (large
`index_interval`, no `gc_interval`) so the measurement isolates the write path
instead of competing with index publication for the same store bandwidth.

## Two measurements

- **Serial latency** (`concurrency 1`): one commit at a time, awaited end to
  end — the per-commit critical-path cost. On `mem`/`fs`/`turso` it is CPU plus
  a local `fsync`; on `postgres`/`s3` it is dominated by the round trips (two
  lease reads + one append).
- **Offered-concurrency sweep**: many callers submitting to the *same* database
  at once. One `DbState.commit` mutex serializes the entire pipeline, so today
  throughput is expected to stay roughly flat as concurrency rises — the point
  is to record that ceiling. The gap between the serial commit cost and what
  the hardware/store could sustain if stages overlapped is the head room a
  pipelined transactor should recover; re-run after each change to watch it
  close.

## Running it

Local backends (default features):

```sh
cargo run --release -p corium-transactor --example tx_throughput -- --store mem
cargo run --release -p corium-transactor --example tx_throughput -- \
    --store fs --data-dir /tmp/corium-bench
```

Local SQLite (Turso) vs remote PostgreSQL / S3 — the local-vs-remote write-type
comparison. Build with the matching feature:

```sh
cargo run --release -p corium-transactor --features turso --example tx_throughput -- \
    --store turso --path /tmp/corium-bench.turso

cargo run --release -p corium-transactor --features postgres --example tx_throughput -- \
    --store postgres --postgres-url "$CORIUM_BENCH_POSTGRES_URL"

cargo run --release -p corium-transactor --features s3 --example tx_throughput -- \
    --store s3 --s3-bucket "$CORIUM_BENCH_S3_BUCKET" --s3-prefix bench1
```

Secrets can come from `CORIUM_BENCH_POSTGRES_URL`, `CORIUM_BENCH_S3_BUCKET`,
and `CORIUM_BENCH_S3_PREFIX` instead of flags. `--json` emits one JSON object
per concurrency level for archiving. Other flags: `--transactions`,
`--warmup`, `--concurrency a,b,c`, `--datoms-per-tx`, `--worker-threads`,
`--index-interval-secs` (see `--help`).

For a *truly independent* remote target (e.g. a separate box running
PostgreSQL/MinIO), run the benchmark on a different machine from the store so
the measured round trips are real network latency rather than loopback. Record
the round-trip ping alongside the numbers — for the remote backends the serial
commit latency should track roughly `2 × (lease read) + (append)` network
round trips, which is the quantity the "hoist the lease checks" pipeline step
targets.

## Reading the result

The dominant bound tells you which pipeline step pays off first:

| Serial commit is bound by | Symptom | Highest-payoff step |
|---|---|---|
| durability (`fsync`/append) | `mem` ≫ `fs`/`turso`; latency ≈ one flush | group commit (batch appends behind one flush) |
| round trips (remote store) | `postgres`/`s3` latency ≈ 3× store RTT; flat under concurrency | hoist the two lease checks out of the per-tx path; group commit |
| CPU (`prepare`) | `mem` latency non-trivial and scales with `--datoms-per-tx` | overlap validate(N+1) with flush(N) via optimistic apply |

Numbers are hardware- and network-specific; treat them as a relative baseline
for a given machine + store, re-recorded when either changes — not as absolute
targets.
