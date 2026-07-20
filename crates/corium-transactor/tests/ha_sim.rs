//! M7 acceptance: deterministic active/standby takeover simulation.
//!
//! Two writers share one root store and one (lease-versioned) durable log.
//! The active writer A runs the real commit protocol from `node.rs`
//! (pre-check → durable append → post-append fence → ack) plus index
//! publication and lease renewal, all against the production `lease`,
//! `publish_root`, and `VersionedLog` code. A scripted store injects a full
//! standby takeover by B — lease acquisition past expiry, log-tail replay,
//! new commits, index publication — at *every* boundary of A's protocol
//! (each root-store operation plus the append boundary), modeling every
//! crash/partition timing at the granularity of the only shared state.
//!
//! Invariants checked for every timing, with A either crashed (never
//! resumes) or partitioned (resumes and retries):
//! - every transaction A acked before the takeover is in B's recovered
//!   state and in the final durable log, exactly once;
//! - A never acknowledges a transaction after the takeover;
//! - A never installs a root after the takeover (no double-publish), and
//!   installed roots never regress in (lease version, index basis);
//! - the merged durable log stays contiguous and duplicate-free.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use corium_core::{Cardinality, EntityId, Partition, Schema, Value, ValueType};
use corium_db::{Db, attribute};
use corium_log::{TransactionLog, VersionedLog};
use corium_store::{
    BlobId, BlobIdStream, BlobStore, DbRoot, MemoryStore, RootStore, StoreError, db_root_name,
};
use corium_transactor::lease::{self, Lease, LeaseError};
use corium_transactor::{EmbeddedTransactor, TransactError};
use corium_tx::{EntityRef, TxItem, TxOp};

const DB: &str = "sim";
const TTL_MS: i64 = 5_000;
type TakeoverFuture = Pin<Box<dyn Future<Output = ()> + Send>>;
type Takeover = Box<dyn FnOnce() -> TakeoverFuture + Send>;

fn schema() -> (Schema, EntityId) {
    let a = EntityId::new(Partition::Db as u32, 100);
    let mut schema = Schema::default();
    schema.insert(attribute(100, ValueType::Long, Cardinality::One, None));
    (schema, a)
}

fn long_values(db: &Db) -> Vec<i64> {
    let mut values: Vec<i64> = db
        .datoms()
        .into_iter()
        .filter_map(|datom| match datom.v {
            Value::Long(v) => Some(v),
            _ => None,
        })
        .collect();
    values.sort_unstable();
    values
}

/// A successful root install observed by the scripted store.
#[derive(Debug)]
struct Install {
    by_active: bool,
    after_takeover: bool,
    root: DbRoot,
}

/// Shared world state: the schedule, the logical clock, and the audit
/// trail of root installs.
struct Script {
    /// Root-op/append boundaries of A remaining before the takeover fires.
    ops_before_takeover: usize,
    fired: bool,
    takeover: Option<Takeover>,
    installs: Vec<Install>,
}

struct World {
    raw: Arc<MemoryStore>,
    script: Mutex<Script>,
    clock: AtomicI64,
}

impl World {
    fn now(&self) -> i64 {
        self.clock.load(Ordering::SeqCst)
    }

    /// One protocol boundary of the active writer: fires the scheduled
    /// takeover when its op count is reached.
    async fn tick(&self) {
        let takeover = {
            let mut script = self.script.lock().expect("script lock");
            if script.fired || script.ops_before_takeover > 0 {
                script.ops_before_takeover = script.ops_before_takeover.saturating_sub(1);
                None
            } else {
                script.fired = true;
                script.takeover.take()
            }
        };
        if let Some(takeover) = takeover {
            takeover().await;
        }
    }

    fn fired(&self) -> bool {
        self.script.lock().expect("script lock").fired
    }

    /// Runs the takeover immediately if the schedule never reached it.
    async fn force_fire(&self) {
        let takeover = {
            let mut script = self.script.lock().expect("script lock");
            script.fired = true;
            script.takeover.take()
        };
        if let Some(takeover) = takeover {
            takeover().await;
        }
    }
}

/// Store handle: the active writer's handle counts protocol boundaries;
/// every handle records successful db-root installs for the audit.
#[derive(Clone)]
struct ScriptedStore {
    world: Arc<World>,
    counted: bool,
}

#[async_trait::async_trait]
impl BlobStore for ScriptedStore {
    async fn put(&self, bytes: &[u8]) -> Result<BlobId, StoreError> {
        self.world.raw.put(bytes).await
    }
    async fn get(&self, id: &BlobId) -> Result<Option<Vec<u8>>, StoreError> {
        self.world.raw.get(id).await
    }
    async fn delete(&self, id: &BlobId) -> Result<(), StoreError> {
        self.world.raw.delete(id).await
    }
    async fn list(&self) -> Result<BlobIdStream, StoreError> {
        self.world.raw.list().await
    }
}

#[async_trait::async_trait]
impl RootStore for ScriptedStore {
    async fn get_root(&self, name: &str) -> Result<Option<Vec<u8>>, StoreError> {
        if self.counted {
            self.world.tick().await;
        }
        self.world.raw.get_root(name).await
    }
    async fn cas_root(
        &self,
        name: &str,
        expected: Option<&[u8]>,
        new: &[u8],
    ) -> Result<(), StoreError> {
        if self.counted {
            self.world.tick().await;
        }
        let result = self.world.raw.cas_root(name, expected, new).await;
        if result.is_ok()
            && name == db_root_name(DB)
            && let Some(root) = DbRoot::decode(new)
        {
            let mut script = self.world.script.lock().expect("script lock");
            let after_takeover = script.fired;
            script.installs.push(Install {
                by_active: self.counted,
                after_takeover,
                root,
            });
        }
        result
    }
    async fn delete_root(&self, name: &str) -> Result<(), StoreError> {
        self.world.raw.delete_root(name).await
    }
    async fn list_roots(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        self.world.raw.list_roots(prefix).await
    }
}

#[derive(Debug, Eq, PartialEq)]
enum Commit {
    Acked,
    RefusedPre,
    RefusedPost,
}

/// One step of the active writer's workload.
enum Step {
    Commit(i64),
    Publish,
    Renew,
}

/// One transactor process over the shared world, running the node's
/// commit protocol against the production primitives.
struct Writer {
    store: ScriptedStore,
    lease: Lease,
    transactor: EmbeddedTransactor,
    attr: EntityId,
    acked: Vec<i64>,
    /// Values durably appended but refused at the post-append fence.
    refused_appended: Vec<i64>,
}

impl Writer {
    async fn start(store: ScriptedStore, logs: &Path, owner: &str) -> Result<Self, LeaseError> {
        let now = store.world.now();
        let held = lease::acquire(&store, DB, owner, "", TTL_MS, now).await?;
        let log: Arc<dyn TransactionLog> =
            Arc::new(VersionedLog::open(logs, DB, held.version).expect("open log"));
        let (schema, attr) = schema();
        let transactor = EmbeddedTransactor::recover(schema, log).expect("recover");
        Ok(Self {
            store,
            lease: held,
            transactor,
            attr,
            acked: Vec::new(),
            refused_appended: Vec::new(),
        })
    }

    /// The node's commit protocol: pre-check, durable append (with the
    /// takeover boundary between them), post-append fence, ack.
    async fn commit(&mut self, value: i64) -> Commit {
        if lease::verify(&self.store, DB, &self.lease).await.is_err() {
            return Commit::RefusedPre;
        }
        // A takeover can land between the ownership check and the append.
        if self.store.counted {
            self.store.world.tick().await;
        }
        self.transactor
            .transact([TxItem::Op(TxOp::Add(
                EntityRef::Temp("e".into()),
                self.attr,
                Value::Long(value),
            ))])
            .expect("append to own version file");
        if lease::verify(&self.store, DB, &self.lease).await.is_err() {
            self.refused_appended.push(value);
            return Commit::RefusedPost;
        }
        self.acked.push(value);
        Commit::Acked
    }

    async fn publish(&self) -> Result<DbRoot, TransactError> {
        self.transactor
            .publish_indexes(&self.store, &db_root_name(DB), self.lease.version)
            .await
    }

    async fn renew(&mut self) -> Result<(), LeaseError> {
        let now = self.store.world.now();
        self.lease = lease::renew(&self.store, DB, &self.lease, TTL_MS, now).await?;
        Ok(())
    }
}

/// Outcome of B's scripted takeover, captured for the post-run assertions.
struct TakeoverResult {
    /// Values in B's state immediately after the takeover replay.
    recovered: Vec<i64>,
    /// Values B committed after taking over.
    acked: Vec<i64>,
    lease_version: u64,
}

/// A's view shared with the takeover hook (A is mutably borrowed while the
/// hook runs, so the hook reads this mirror instead).
#[derive(Default)]
struct ActiveMirror {
    acked: Vec<i64>,
    /// Value appended (or about to be appended) but not yet acked.
    in_flight: Option<i64>,
}

/// What the scenario exercised, so the sweep can prove its coverage.
#[derive(Default)]
struct Coverage {
    /// A transaction was refused at the post-append fence: the durable
    /// append raced the takeover, was never acked, and the versioned-log
    /// cutoff had to discard it.
    refused_post: bool,
}

#[allow(clippy::too_many_lines)]
async fn run_scenario(pause_at: usize, resume: bool) -> Coverage {
    let logs_dir = tempfile::tempdir().expect("tempdir");
    let logs: PathBuf = logs_dir.path().to_path_buf();
    let world = Arc::new(World {
        raw: Arc::new(MemoryStore::default()),
        script: Mutex::new(Script {
            ops_before_takeover: pause_at,
            fired: false,
            takeover: None,
            installs: Vec::new(),
        }),
        clock: AtomicI64::new(1_000),
    });
    let mirror = Arc::new(Mutex::new(ActiveMirror::default()));
    let takeover_result: Arc<Mutex<Option<TakeoverResult>>> = Arc::new(Mutex::new(None));
    let takeover_failed = Arc::new(AtomicBool::new(false));

    // B's full takeover: jump the clock past A's lease expiry, acquire (the
    // fence), replay the log tail, commit, publish.
    {
        let hook_world = Arc::clone(&world);
        let mirror = Arc::clone(&mirror);
        let takeover_result = Arc::clone(&takeover_result);
        let takeover_failed = Arc::clone(&takeover_failed);
        let logs = logs.clone();
        let hook = move || {
            Box::pin(async move {
                let store = ScriptedStore {
                    world: Arc::clone(&hook_world),
                    counted: false,
                };
                let expiry = store
                    .world
                    .raw
                    .get_root(&db_root_name(DB))
                    .await
                    .expect("read root")
                    .as_deref()
                    .and_then(DbRoot::decode)
                    .map_or(0, |root| root.lease_expires_unix_ms);
                hook_world.clock.fetch_max(expiry + 1, Ordering::SeqCst);
                let mut b = match Writer::start(store, &logs, "owner-b").await {
                    Ok(b) => b,
                    Err(error) => {
                        eprintln!("takeover failed: {error}");
                        takeover_failed.store(true, Ordering::SeqCst);
                        return;
                    }
                };
                let recovered = long_values(&b.transactor.db());
                {
                    // Zero acked-transaction loss at the takeover instant.
                    let mirror = mirror.lock().expect("mirror lock");
                    for value in &mirror.acked {
                        assert!(
                            recovered.contains(value),
                            "takeover lost acked value {value} (recovered {recovered:?})"
                        );
                    }
                    // And nothing beyond acked + the single in-flight append.
                    for value in &recovered {
                        assert!(
                            mirror.acked.contains(value) || mirror.in_flight == Some(*value),
                            "takeover invented value {value}"
                        );
                    }
                }
                assert_eq!(b.commit(100).await, Commit::Acked, "standby serves writes");
                assert_eq!(b.commit(101).await, Commit::Acked);
                b.publish().await.expect("standby publishes");
                *takeover_result.lock().expect("result lock") = Some(TakeoverResult {
                    recovered,
                    acked: b.acked.clone(),
                    lease_version: b.lease.version,
                });
            }) as Pin<Box<dyn Future<Output = ()> + Send>>
        };
        world.script.lock().expect("script lock").takeover = Some(Box::new(hook));
    }

    let store_a = ScriptedStore {
        world: Arc::clone(&world),
        counted: true,
    };
    let mut a = match Writer::start(store_a, &logs, "owner-a").await {
        Ok(a) => a,
        Err(LeaseError::Held { .. }) if world.fired() => {
            // The takeover fired inside A's own acquisition: B owns the
            // database before A ever did, and A correctly stands down.
            // There is nothing of A's to lose or fence.
            return Coverage::default();
        }
        Err(error) => panic!("A failed to start: {error}"),
    };

    // A's workload; every step mirrors into the shared view and, in crash
    // mode, stops at the takeover point.
    let steps = [
        Step::Commit(1),
        Step::Commit(2),
        Step::Publish,
        Step::Renew,
        Step::Commit(3),
        Step::Publish,
        Step::Commit(4),
    ];
    for step in steps {
        if !resume && world.fired() {
            break;
        }
        match step {
            Step::Commit(value) => {
                mirror.lock().expect("mirror lock").in_flight = Some(value);
                let outcome = a.commit(value).await;
                let mut mirror = mirror.lock().expect("mirror lock");
                mirror.in_flight = None;
                if outcome == Commit::Acked {
                    mirror.acked.push(value);
                    assert!(!world.fired(), "A acked value {value} after the takeover");
                }
            }
            Step::Publish => {
                // Pre-takeover publishes succeed; post-takeover must fail.
                let result = a.publish().await;
                if world.fired() {
                    assert!(
                        result.is_err(),
                        "A published an index root after the takeover"
                    );
                }
            }
            Step::Renew => {
                let result = a.renew().await;
                if world.fired() {
                    assert!(result.is_err(), "A renewed a lost lease");
                }
            }
        }
    }

    // If the schedule outlived A's workload, the takeover happens now
    // (models A stalling after its last operation).
    if !world.fired() {
        world.force_fire().await;
    }
    assert!(
        !takeover_failed.load(Ordering::SeqCst),
        "standby failed to take over"
    );

    if resume {
        // The partitioned A wakes and tries everything once more; nothing
        // may ack or install.
        let outcome = a.commit(99).await;
        assert_ne!(outcome, Commit::Acked, "deposed A acked a transaction");
        assert!(
            a.publish().await.is_err(),
            "deposed A published an index root"
        );
        assert!(a.renew().await.is_err(), "deposed A renewed the lease");
    }
    let coverage = Coverage {
        refused_post: !a.refused_appended.is_empty(),
    };
    drop(a);

    // The audit trail: A installed nothing after the takeover, and installed
    // roots never regressed.
    let result = takeover_result
        .lock()
        .expect("result lock")
        .take()
        .expect("takeover ran");
    {
        let script = world.script.lock().expect("script lock");
        for install in &script.installs {
            assert!(
                !(install.by_active && install.after_takeover),
                "A installed a root after the takeover: {:?}",
                install.root
            );
        }
        let mut last = (0_u64, 0_u64);
        for install in &script.installs {
            let next = (install.root.lease_version, install.root.index_basis_t);
            assert!(
                next >= last,
                "published root regressed from {last:?} to {next:?}"
            );
            last = next;
        }
        assert!(
            script
                .installs
                .iter()
                .any(|install| install.root.lease_version == result.lease_version),
            "takeover never rewrote the root under its lease version"
        );
    }

    // A fresh recovery of the merged durable log (a third process): every
    // acked transaction present exactly once, contiguous, and nothing
    // beyond acked + refused-in-flight appends.
    let final_log = VersionedLog::open_read_only(&logs, DB).expect("open merged log");
    let records = final_log.replay().expect("merged log is contiguous");
    for pair in records.windows(2) {
        assert_eq!(pair[1].t, pair[0].t + 1, "hole in the merged log");
    }
    let (schema, _) = schema();
    let mut replayed = Db::new(schema);
    for record in &records {
        replayed = replayed.with_transaction(record.t, &record.datoms);
    }
    let values = long_values(&replayed);
    let acked: Vec<i64> = mirror
        .lock()
        .expect("mirror lock")
        .acked
        .iter()
        .copied()
        .chain(result.acked.iter().copied())
        .collect();
    for value in &acked {
        assert!(
            values.contains(value),
            "acked value {value} missing from the final log (log has {values:?})"
        );
    }
    let mut unique = values.clone();
    unique.dedup();
    assert_eq!(unique, values, "duplicate transaction in the final log");
    for value in &values {
        assert!(
            acked.contains(value) || a_could_have_appended(*value),
            "final log contains unexplained value {value}"
        );
    }
    // B's recovered state was consistent with what it replayed.
    for value in &result.recovered {
        assert!(values.contains(value) || result.acked.contains(value));
    }
    coverage
}

/// Values A appends in any scenario (workload plus the resume attempt);
/// a durable-but-unacked record is allowed to survive as the single
/// in-flight trailer.
fn a_could_have_appended(value: i64) -> bool {
    [1, 2, 3, 4, 99].contains(&value)
}

/// Enough boundaries to cover A's whole workload (lease acquisition, seven
/// steps of commits/publishes/renewals) with margin; the run asserts the
/// margin really was reached so protocol growth cannot silently shrink
/// coverage.
const MAX_BOUNDARIES: usize = 40;

#[tokio::test]
async fn takeover_at_every_boundary_preserves_acked_and_never_double_publishes() {
    let mut fence_exercised = false;
    for pause_at in 0..MAX_BOUNDARIES {
        for resume in [false, true] {
            fence_exercised |= run_scenario(pause_at, resume).await.refused_post;
        }
    }
    assert!(
        fence_exercised,
        "no timing hit the post-append fence; the sweep lost its \
         append-vs-takeover race coverage"
    );
}

#[tokio::test]
async fn workload_fits_within_enumerated_boundaries() {
    // With the takeover scheduled beyond every boundary the workload can
    // generate, it must not fire during the workload — proving
    // MAX_BOUNDARIES covers the whole protocol.
    let logs_dir = tempfile::tempdir().expect("tempdir");
    let world = Arc::new(World {
        raw: Arc::new(MemoryStore::default()),
        script: Mutex::new(Script {
            ops_before_takeover: MAX_BOUNDARIES,
            fired: false,
            takeover: None,
            installs: Vec::new(),
        }),
        clock: AtomicI64::new(1_000),
    });
    let store = ScriptedStore {
        world: Arc::clone(&world),
        counted: true,
    };
    let mut a = Writer::start(store, logs_dir.path(), "owner-a")
        .await
        .expect("start");
    for value in [1, 2] {
        assert_eq!(a.commit(value).await, Commit::Acked);
    }
    a.publish().await.expect("publish");
    a.renew().await.expect("renew");
    for value in [3, 4] {
        assert_eq!(a.commit(value).await, Commit::Acked);
    }
    a.publish().await.expect("publish");
    assert!(
        world
            .script
            .lock()
            .expect("script lock")
            .ops_before_takeover
            > 0,
        "workload used more boundaries than MAX_BOUNDARIES enumerates"
    );
}
