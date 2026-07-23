//! Transactor write-throughput benchmark.
//!
//! Drives `TransactorNode::transact` — the full commit critical section
//! (pre-append lease fence, `:db/fn` expansion, EDN conversion, `prepare`
//! validation, the durable log append, and the post-append lease fence) — so
//! the numbers reflect the single-writer pipeline as it exists today, not a
//! synthetic microbenchmark of one stage.
//!
//! It measures two things per backend:
//!
//! * **Serial latency** — one transaction at a time, awaited end to end. This
//!   is the per-commit critical-path cost: on a local store it is dominated by
//!   CPU (`prepare`) plus an `fsync`; on a remote store it is dominated by the
//!   round trips (two lease reads + one log append).
//! * **Offered-concurrency sweep** — many callers submitting to the *same*
//!   database at once. Because one `DbState.commit` mutex serializes the whole
//!   pipeline, added concurrency does not raise throughput today; the gap
//!   between the serial number and the concurrent number is exactly the head
//!   room a pipelined transactor (group commit, overlapped validate/flush,
//!   hoisted lease checks) could recover. Re-run after each pipeline change to
//!   watch that gap close.
//!
//! Backend selection mirrors the transactor's own `--store`. Native backends
//! are behind Cargo features, so build with the ones you want to compare:
//!
//! ```sh
//! # local: in-memory and filesystem
//! cargo run --release -p corium-transactor --example tx_throughput -- \
//!     --store mem
//! cargo run --release -p corium-transactor --example tx_throughput -- \
//!     --store fs --data-dir /tmp/corium-bench
//!
//! # local SQLite (Turso) vs remote PostgreSQL / S3 — the write-type comparison
//! cargo run --release -p corium-transactor --features turso --example tx_throughput -- \
//!     --store turso --path /tmp/corium-bench.turso
//! cargo run --release -p corium-transactor --features postgres --example tx_throughput -- \
//!     --store postgres --postgres-url "$CORIUM_BENCH_POSTGRES_URL"
//! cargo run --release -p corium-transactor --features s3 --example tx_throughput -- \
//!     --store s3 --s3-bucket my-bench-bucket --s3-prefix bench1
//! ```
//!
//! Secrets may come from the environment instead of the command line:
//! `CORIUM_BENCH_POSTGRES_URL`, `CORIUM_BENCH_S3_BUCKET`,
//! `CORIUM_BENCH_S3_PREFIX`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use corium_protocol::codec::encode_edn;
use corium_query::edn::Edn;
use corium_transactor::StoreSpec;
use corium_transactor::node::{NodeConfig, TransactorNode};

/// Parsed benchmark configuration.
struct Args {
    store: String,
    data_dir: Option<String>,
    turso_path: Option<String>,
    postgres_url: Option<String>,
    s3_bucket: Option<String>,
    s3_prefix: Option<String>,
    /// Timed transactions per phase.
    transactions: u64,
    /// Untimed warmup transactions (interns keywords, primes connections).
    warmup: u64,
    /// Offered-concurrency levels to sweep, e.g. `1,2,4,8,32`.
    concurrency: Vec<usize>,
    /// Datoms asserted per transaction (payload size).
    datoms_per_tx: usize,
    /// Tokio worker threads; more lets stalled I/O overlap other callers' CPU.
    worker_threads: usize,
    /// Emit machine-readable JSON lines instead of a table.
    json: bool,
    /// Background index publication interval; large keeps the store bandwidth
    /// for the write path so the measurement isolates commits.
    index_interval_secs: u64,
}

impl Args {
    fn parse() -> Result<Self, String> {
        let mut args = Self {
            store: "mem".into(),
            data_dir: None,
            turso_path: None,
            postgres_url: std::env::var("CORIUM_BENCH_POSTGRES_URL").ok(),
            s3_bucket: std::env::var("CORIUM_BENCH_S3_BUCKET").ok(),
            s3_prefix: std::env::var("CORIUM_BENCH_S3_PREFIX").ok(),
            transactions: 2_000,
            warmup: 200,
            concurrency: vec![1, 2, 4, 8, 32],
            datoms_per_tx: 2,
            worker_threads: num_cpus(),
            json: false,
            index_interval_secs: 3_600,
        };
        let mut it = std::env::args().skip(1);
        while let Some(flag) = it.next() {
            let mut value = || {
                it.next()
                    .ok_or_else(|| format!("flag {flag} needs a value"))
            };
            match flag.as_str() {
                "--store" => args.store = value()?,
                "--data-dir" => args.data_dir = Some(value()?),
                "--path" => args.turso_path = Some(value()?),
                "--postgres-url" => args.postgres_url = Some(value()?),
                "--s3-bucket" => args.s3_bucket = Some(value()?),
                "--s3-prefix" => args.s3_prefix = Some(value()?),
                "--transactions" => {
                    args.transactions = value()?.parse().map_err(|_| "bad --transactions")?;
                }
                "--warmup" => args.warmup = value()?.parse().map_err(|_| "bad --warmup")?,
                "--concurrency" => {
                    args.concurrency = value()?
                        .split(',')
                        .map(|s| s.trim().parse::<usize>().map_err(|_| "bad --concurrency"))
                        .collect::<Result<Vec<_>, _>>()?;
                    if args.concurrency.is_empty() {
                        return Err("--concurrency needs at least one level".into());
                    }
                }
                "--datoms-per-tx" => {
                    args.datoms_per_tx = value()?.parse().map_err(|_| "bad --datoms-per-tx")?;
                    if args.datoms_per_tx == 0 {
                        return Err("--datoms-per-tx must be at least 1".into());
                    }
                }
                "--worker-threads" => {
                    args.worker_threads = value()?.parse().map_err(|_| "bad --worker-threads")?;
                    if args.worker_threads == 0 {
                        return Err("--worker-threads must be at least 1".into());
                    }
                }
                "--index-interval-secs" => {
                    args.index_interval_secs =
                        value()?.parse().map_err(|_| "bad --index-interval-secs")?;
                }
                "--json" => args.json = true,
                "-h" | "--help" => return Err(usage()),
                other => return Err(format!("unknown flag {other}\n\n{}", usage())),
            }
        }
        Ok(args)
    }
}

fn usage() -> String {
    "usage: tx_throughput --store <mem|fs|turso|postgres|s3> [options]\n\
     \n\
     options:\n\
     \x20 --data-dir <dir>            filesystem store + logs (fs backend)\n\
     \x20 --path <file>              Turso database file (turso backend)\n\
     \x20 --postgres-url <url>       or env CORIUM_BENCH_POSTGRES_URL\n\
     \x20 --s3-bucket <name>         or env CORIUM_BENCH_S3_BUCKET\n\
     \x20 --s3-prefix <prefix>       or env CORIUM_BENCH_S3_PREFIX\n\
     \x20 --transactions <n>         timed transactions per phase (default 2000)\n\
     \x20 --warmup <n>               untimed warmup transactions (default 200)\n\
     \x20 --concurrency <a,b,c>      offered-load sweep (default 1,2,4,8,32)\n\
     \x20 --datoms-per-tx <n>        payload datoms per transaction (default 2)\n\
     \x20 --worker-threads <n>       tokio workers (default: logical CPUs)\n\
     \x20 --index-interval-secs <n>  background indexing interval (default 3600)\n\
     \x20 --json                     emit JSON lines instead of a table"
        .into()
}

/// Best-effort logical CPU count without pulling in a dependency.
fn num_cpus() -> usize {
    std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(4)
}

fn build_store_spec(args: &Args) -> Result<StoreSpec, String> {
    match args.store.as_str() {
        "mem" => Ok(StoreSpec::Memory),
        "fs" => Ok(StoreSpec::Fs),
        "turso" => {
            #[cfg(feature = "turso")]
            {
                let path = args
                    .turso_path
                    .clone()
                    .ok_or("turso backend needs --path <file>")?;
                Ok(StoreSpec::Turso { path })
            }
            #[cfg(not(feature = "turso"))]
            Err("rebuild with `--features turso` to use the turso backend".into())
        }
        "postgres" => {
            #[cfg(feature = "postgres")]
            {
                let connection_string = args
                    .postgres_url
                    .clone()
                    .ok_or("postgres backend needs --postgres-url or CORIUM_BENCH_POSTGRES_URL")?;
                Ok(StoreSpec::Postgres { connection_string })
            }
            #[cfg(not(feature = "postgres"))]
            Err("rebuild with `--features postgres` to use the postgres backend".into())
        }
        "s3" => {
            #[cfg(feature = "s3")]
            {
                let bucket = args
                    .s3_bucket
                    .clone()
                    .ok_or("s3 backend needs --s3-bucket or CORIUM_BENCH_S3_BUCKET")?;
                let prefix = args.s3_prefix.clone().unwrap_or_default();
                Ok(StoreSpec::S3 { bucket, prefix })
            }
            #[cfg(not(feature = "s3"))]
            Err("rebuild with `--features s3` to use the s3 backend".into())
        }
        other => Err(format!("unknown --store {other}")),
    }
}

/// A benchmark schema: one unique-identity key (exercises the AVET uniqueness
/// check in `prepare`) plus filler string fields to pad the payload.
fn schema_edn(filler_fields: usize) -> Vec<u8> {
    let mut forms = vec![Edn::Map(vec![
        (Edn::keyword("db/ident"), Edn::keyword("bench/key")),
        (Edn::keyword("db/valueType"), Edn::keyword("db.type/long")),
        (
            Edn::keyword("db/cardinality"),
            Edn::keyword("db.cardinality/one"),
        ),
        (
            Edn::keyword("db/unique"),
            Edn::keyword("db.unique/identity"),
        ),
    ])];
    for i in 0..filler_fields {
        forms.push(Edn::Map(vec![
            (
                Edn::keyword("db/ident"),
                Edn::keyword(&format!("bench/field{i}")),
            ),
            (Edn::keyword("db/valueType"), Edn::keyword("db.type/string")),
            (
                Edn::keyword("db/cardinality"),
                Edn::keyword("db.cardinality/one"),
            ),
        ]));
    }
    encode_edn(&Edn::Vector(forms))
}

/// One transaction inserting a fresh entity keyed by the globally unique `key`,
/// so every transaction is a genuine insert (no dedup, no cross-caller unique
/// conflict) that touches allocation, validation, and the log append.
fn tx_edn(key: u64, filler_fields: usize) -> Vec<u8> {
    let mut entity = vec![
        (Edn::keyword("db/id"), Edn::Str("e".into())),
        (
            Edn::keyword("bench/key"),
            Edn::Long(i64::try_from(key).unwrap_or(i64::MAX)),
        ),
    ];
    for i in 0..filler_fields {
        entity.push((
            Edn::keyword(&format!("bench/field{i}")),
            Edn::Str(format!("value-{key}-{i}")),
        ));
    }
    encode_edn(&Edn::Vector(vec![Edn::Map(entity)]))
}

/// Latency/throughput summary for one phase.
struct Stats {
    concurrency: usize,
    count: u64,
    wall: Duration,
    datoms_per_tx: usize,
    p50: Duration,
    p90: Duration,
    p99: Duration,
    max: Duration,
    mean: Duration,
}

impl Stats {
    fn from_samples(
        concurrency: usize,
        wall: Duration,
        datoms_per_tx: usize,
        mut s: Vec<Duration>,
    ) -> Self {
        s.sort_unstable();
        let count = s.len() as u64;
        let pick = |q: f64| {
            if s.is_empty() {
                Duration::ZERO
            } else {
                let idx = ((s.len() as f64 - 1.0) * q).round() as usize;
                s[idx.min(s.len() - 1)]
            }
        };
        let sum: Duration = s.iter().sum();
        let mean = if count == 0 {
            Duration::ZERO
        } else {
            sum / u32::try_from(count).unwrap_or(u32::MAX)
        };
        Self {
            concurrency,
            count,
            wall,
            datoms_per_tx,
            p50: pick(0.50),
            p90: pick(0.90),
            p99: pick(0.99),
            max: s.last().copied().unwrap_or(Duration::ZERO),
            mean,
        }
    }

    fn tx_per_sec(&self) -> f64 {
        if self.wall.is_zero() {
            0.0
        } else {
            self.count as f64 / self.wall.as_secs_f64()
        }
    }

    fn datoms_per_sec(&self) -> f64 {
        self.tx_per_sec() * self.datoms_per_tx as f64
    }
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1_000.0
}

/// Runs `total` timed transactions across `concurrency` submitters against the
/// same database, pulling unique keys from `next_key`.
async fn run_phase(
    node: &Arc<TransactorNode>,
    db: &str,
    next_key: &Arc<AtomicU64>,
    total: u64,
    concurrency: usize,
    datoms_per_tx: usize,
) -> Stats {
    // A bounded issued-ticket counter (never wraps for realistic `total`);
    // each worker claims tickets until `total` are gone.
    let issued = Arc::new(AtomicU64::new(0));
    let filler = datoms_per_tx.saturating_sub(1);
    let started = Instant::now();
    let mut workers = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let node = Arc::clone(node);
        let db = db.to_owned();
        let issued = Arc::clone(&issued);
        let next_key = Arc::clone(next_key);
        workers.push(tokio::spawn(async move {
            let mut samples = Vec::new();
            while issued.fetch_add(1, Ordering::Relaxed) < total {
                let key = next_key.fetch_add(1, Ordering::Relaxed);
                let tx = tx_edn(key, filler);
                let at = Instant::now();
                node.transact(&db, &tx).await.expect("transact");
                samples.push(at.elapsed());
            }
            samples
        }));
    }
    let mut samples = Vec::with_capacity(total as usize);
    for worker in workers {
        samples.extend(worker.await.expect("worker panicked"));
    }
    Stats::from_samples(concurrency, started.elapsed(), datoms_per_tx, samples)
}

fn report_table(spec: &StoreSpec, stats: &[Stats]) {
    println!("\nbackend: {spec:?}");
    println!(
        "{:>5}  {:>8}  {:>12}  {:>12}  {:>9}  {:>9}  {:>9}  {:>9}  {:>9}",
        "conc", "txns", "tx/s", "datoms/s", "p50 ms", "p90 ms", "p99 ms", "max ms", "mean ms",
    );
    for s in stats {
        println!(
            "{:>5}  {:>8}  {:>12.1}  {:>12.1}  {:>9.3}  {:>9.3}  {:>9.3}  {:>9.3}  {:>9.3}",
            s.concurrency,
            s.count,
            s.tx_per_sec(),
            s.datoms_per_sec(),
            ms(s.p50),
            ms(s.p90),
            ms(s.p99),
            ms(s.max),
            ms(s.mean),
        );
    }
}

fn report_json(store: &str, stats: &[Stats]) {
    for s in stats {
        println!(
            "{{\"store\":\"{}\",\"concurrency\":{},\"transactions\":{},\"tx_per_sec\":{:.3},\
             \"datoms_per_sec\":{:.3},\"p50_ms\":{:.4},\"p90_ms\":{:.4},\"p99_ms\":{:.4},\
             \"max_ms\":{:.4},\"mean_ms\":{:.4}}}",
            store,
            s.concurrency,
            s.count,
            s.tx_per_sec(),
            s.datoms_per_sec(),
            ms(s.p50),
            ms(s.p90),
            ms(s.p99),
            ms(s.max),
            ms(s.mean),
        );
    }
}

async fn run(args: Args) -> Result<(), String> {
    let spec = build_store_spec(&args)?;

    // A fresh data directory for the fs/turso local files unless the caller
    // named one; native backends ignore it.
    let owned_dir;
    let data_dir = match &args.data_dir {
        Some(dir) => std::path::PathBuf::from(dir),
        None => {
            owned_dir = tempfile::tempdir().map_err(|e| e.to_string())?;
            owned_dir.path().to_path_buf()
        }
    };

    let mut config = NodeConfig::new(data_dir);
    config.store = spec.clone();
    // Isolate the write path: keep background indexing/GC/heartbeats from
    // competing for the same store bandwidth during the run.
    config.index_interval = Duration::from_secs(args.index_interval_secs);
    config.gc_interval = None;
    config.heartbeat_interval = Duration::from_secs(args.index_interval_secs);

    let node = TransactorNode::open(config)
        .await
        .map_err(|e| format!("open node: {e}"))?;

    // The S3 backend fences root CAS with conditional writes; an endpoint that
    // accepts but ignores them would silently break lease safety, so confirm
    // enforcement before trusting an unfamiliar (self-hosted) target.
    #[cfg(feature = "s3")]
    if matches!(spec, StoreSpec::S3 { .. }) {
        use corium_transactor::NodeStore;
        if let NodeStore::S3(s3) = node.store().as_ref() {
            eprintln!("verifying S3 conditional-write enforcement…");
            s3.verify_conditional_writes()
                .await
                .map_err(|e| format!("S3 conditional-write self-check failed: {e}"))?;
            eprintln!("  ok: endpoint enforces If-None-Match and If-Match");
        }
    }

    let db = "bench";
    let filler = args.datoms_per_tx.saturating_sub(1);
    node.create_db(db, &schema_edn(filler))
        .await
        .map_err(|e| format!("create db: {e}"))?;

    let next_key = Arc::new(AtomicU64::new(0));

    if !args.json {
        eprintln!(
            "warming up ({} txns) then timing {} txns per concurrency level {:?} \
             at {} datoms/tx on {} worker threads…",
            args.warmup,
            args.transactions,
            args.concurrency,
            args.datoms_per_tx,
            args.worker_threads,
        );
    }
    if args.warmup > 0 {
        run_phase(&node, db, &next_key, args.warmup, 1, args.datoms_per_tx).await;
    }

    let mut stats = Vec::new();
    for &conc in &args.concurrency {
        let s = run_phase(
            &node,
            db,
            &next_key,
            args.transactions,
            conc,
            args.datoms_per_tx,
        )
        .await;
        stats.push(s);
    }

    if args.json {
        report_json(&args.store, &stats);
    } else {
        report_table(&spec, &stats);
    }
    Ok(())
}

fn main() {
    let args = match Args::parse() {
        Ok(args) => args,
        Err(message) => {
            eprintln!("{message}");
            std::process::exit(2);
        }
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(args.worker_threads)
        .enable_all()
        .build()
        .expect("build tokio runtime");
    if let Err(message) = runtime.block_on(run(args)) {
        eprintln!("error: {message}");
        std::process::exit(1);
    }
}
