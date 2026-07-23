//! Durable append-only transaction logs with replay and range scans.

use async_trait::async_trait;
use corium_core::{
    Datom, EntityId,
    encoding::{decode_value, encode_value},
};
use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, RwLock},
};
use thiserror::Error;

/// One committed transaction record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TxRecord {
    /// Monotonic transaction number.
    pub t: u64,
    /// Monotonic UTC millisecond timestamp.
    pub tx_instant: i64,
    /// Facts asserted/retracted by the transaction.
    pub datoms: Vec<Datom>,
}

/// Log errors.
#[derive(Debug, Error)]
pub enum LogError {
    /// Filesystem error.
    #[error("log I/O failed: {0}")]
    Io(#[from] io::Error),
    /// Malformed or incomplete log data.
    #[error("corrupt transaction log")]
    Corrupt,
    /// Native store backend failure.
    #[error("native transaction log store failed: {0}")]
    Native(String),
    /// The operation requires the asynchronous log interface.
    #[error("this transaction log requires asynchronous access")]
    AsyncOnly,
}

/// Common transaction log interface.
#[async_trait]
pub trait TransactionLog: Send + Sync {
    /// Durably appends exactly the next transaction.
    ///
    /// # Errors
    /// Returns an error for I/O failure, corruption, or a non-contiguous `t`.
    fn append(&self, record: &TxRecord) -> Result<(), LogError>;
    /// Durably appends exactly the next transaction without blocking an async
    /// runtime worker. Synchronous logs use [`Self::append`] by default;
    /// storage-backed logs override this method and await their backend.
    ///
    /// # Errors
    /// Returns an error for I/O failure, corruption, or a non-contiguous `t`.
    async fn append_async(&self, record: &TxRecord) -> Result<(), LogError> {
        self.append(record)
    }
    /// Durably appends a contiguous run of transactions under a single
    /// durability boundary where the backend supports one (one `fsync`, one
    /// object, one database transaction), so group commit amortizes the
    /// per-append cost across the batch. `records` must be contiguous in `t`
    /// starting at the log's next expected `t`; an empty slice is a no-op. The
    /// default appends them one at a time; batching backends override this.
    ///
    /// # Errors
    /// Returns an error for I/O failure, corruption, or a non-contiguous `t`.
    async fn append_batch_async(&self, records: &[TxRecord]) -> Result<(), LogError> {
        for record in records {
            self.append_async(record).await?;
        }
        Ok(())
    }
    /// Returns records in the half-open transaction range `[start, end)`.
    ///
    /// # Errors
    /// Returns an error when stored records cannot be read or decoded.
    fn tx_range(&self, start: u64, end: Option<u64>) -> Result<Vec<TxRecord>, LogError>;
    /// Asynchronous form of [`Self::tx_range`].
    ///
    /// # Errors
    /// Returns an error when stored records cannot be read or decoded.
    async fn tx_range_async(
        &self,
        start: u64,
        end: Option<u64>,
    ) -> Result<Vec<TxRecord>, LogError> {
        self.tx_range(start, end)
    }
    /// Replays every committed record.
    ///
    /// # Errors
    /// Returns an error when stored records cannot be read or decoded.
    fn replay(&self) -> Result<Vec<TxRecord>, LogError> {
        self.tx_range(0, None)
    }
    /// Asynchronously replays every committed record.
    ///
    /// # Errors
    /// Returns an error when stored records cannot be read or decoded.
    async fn replay_async(&self) -> Result<Vec<TxRecord>, LogError> {
        self.tx_range_async(0, None).await
    }
}

/// In-memory log implementation.
#[derive(Clone, Default)]
pub struct MemoryLog(Arc<RwLock<Vec<TxRecord>>>);
impl TransactionLog for MemoryLog {
    fn append(&self, record: &TxRecord) -> Result<(), LogError> {
        let mut records = self.0.write().expect("poisoned log lock");
        if records.last().map_or(1, |r| r.t + 1) != record.t {
            return Err(LogError::Corrupt);
        }
        records.push(record.clone());
        Ok(())
    }
    fn tx_range(&self, start: u64, end: Option<u64>) -> Result<Vec<TxRecord>, LogError> {
        Ok(self
            .0
            .read()
            .expect("poisoned log lock")
            .iter()
            .filter(|r| r.t >= start && end.is_none_or(|e| r.t < e))
            .cloned()
            .collect())
    }
}

/// Filesystem append log. Each append is flushed and `fsync`ed before returning.
///
/// A crash mid-append leaves a torn, never-acked record at the tail; `open`
/// truncates it away so replay stops at the durability point of the last
/// acked transaction and later appends extend a clean tail.
pub struct FileLog {
    path: PathBuf,
    next_t: RwLock<u64>,
}
impl FileLog {
    /// Opens or creates a log file, dropping any torn tail left by a crash.
    ///
    /// # Errors
    /// Returns an error if the file cannot be created or a fully written
    /// record is corrupt.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, LogError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        OpenOptions::new().create(true).append(true).open(&path)?;
        let (records, durable_len) = read_records(&path)?;
        if fs::metadata(&path)?.len() > durable_len {
            let file = OpenOptions::new().write(true).open(&path)?;
            file.set_len(durable_len)?;
            file.sync_all()?;
        }
        Ok(Self {
            path,
            next_t: RwLock::new(records.last().map_or(1, |r| r.t + 1)),
        })
    }
}
impl TransactionLog for FileLog {
    fn append(&self, record: &TxRecord) -> Result<(), LogError> {
        let mut next_t = self.next_t.write().expect("poisoned log lock");
        if *next_t != record.t {
            return Err(LogError::Corrupt);
        }
        let payload = encode_record(record);
        let mut file = OpenOptions::new().append(true).open(&self.path)?;
        file.write_all(
            &u64::try_from(payload.len())
                .map_err(|_| LogError::Corrupt)?
                .to_be_bytes(),
        )?;
        file.write_all(&payload)?;
        file.sync_all()?;
        *next_t += 1;
        Ok(())
    }
    fn tx_range(&self, start: u64, end: Option<u64>) -> Result<Vec<TxRecord>, LogError> {
        let _guard = self.next_t.read().expect("poisoned log lock");
        Ok(read_records(&self.path)?
            .0
            .into_iter()
            .filter(|r| r.t >= start && end.is_none_or(|e| r.t < e))
            .collect())
    }
}

/// A transaction log split into per-lease-version files for HA append
/// isolation (see `docs/design/log-and-transactor.md`).
///
/// The active writer under lease version `V` appends only to
/// `{name}.v{V}.log` (the pre-HA `{name}.log` reads as version 0). Readers
/// merge the files in version order and drop any record in an older file
/// whose `t` is at or past the first record of a later file: such records
/// were appended by a deposed writer after a takeover and were never
/// acknowledged, because acknowledgement re-verifies lease ownership after
/// the durable append. A deposed writer therefore cannot corrupt or fork
/// the log — its stale appends land in a file nobody considers current.
pub struct VersionedLog {
    dir: PathBuf,
    name: String,
    write_path: PathBuf,
    next_t: RwLock<u64>,
}

impl VersionedLog {
    /// Opens the log for writing under `write_version`, creating the
    /// version file if needed and dropping any torn tail it carries.
    /// Files of other versions are never modified.
    ///
    /// # Errors
    /// Returns an error if files cannot be read/created or a fully written
    /// record is corrupt.
    pub fn open(dir: impl AsRef<Path>, name: &str, write_version: u64) -> Result<Self, LogError> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        let write_path = version_path(&dir, name, write_version);
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(&write_path)?;
        let (_, durable_len) = read_records(&write_path)?;
        if fs::metadata(&write_path)?.len() > durable_len {
            let file = OpenOptions::new().write(true).open(&write_path)?;
            file.set_len(durable_len)?;
            file.sync_all()?;
        }
        let records = read_merged(&dir, name)?;
        Ok(Self {
            dir,
            name: name.to_owned(),
            write_path,
            next_t: RwLock::new(records.last().map_or(1, |r| r.t + 1)),
        })
    }

    /// Opens the log read-only (offline inspection, backup); appends fail.
    ///
    /// # Errors
    /// Returns an error when the directory cannot be read or a fully
    /// written record is corrupt.
    pub fn open_read_only(dir: impl AsRef<Path>, name: &str) -> Result<Self, LogError> {
        let dir = dir.as_ref().to_path_buf();
        Ok(Self {
            write_path: PathBuf::new(),
            name: name.to_owned(),
            next_t: RwLock::new(u64::MAX),
            dir,
        })
    }

    /// Reports whether any log file exists for this database.
    #[must_use]
    pub fn exists(dir: impl AsRef<Path>, name: &str) -> bool {
        !version_files(dir.as_ref(), name).is_empty()
    }

    /// Deletes every version file for this database.
    ///
    /// # Errors
    /// Returns an error when a file cannot be removed.
    pub fn delete_all(dir: impl AsRef<Path>, name: &str) -> Result<(), LogError> {
        for (_, path) in version_files(dir.as_ref(), name) {
            match fs::remove_file(&path) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
        Ok(())
    }
}

impl TransactionLog for VersionedLog {
    fn append(&self, record: &TxRecord) -> Result<(), LogError> {
        let mut next_t = self.next_t.write().expect("poisoned log lock");
        if *next_t != record.t {
            return Err(LogError::Corrupt);
        }
        let payload = encode_record(record);
        let mut file = OpenOptions::new().append(true).open(&self.write_path)?;
        file.write_all(
            &u64::try_from(payload.len())
                .map_err(|_| LogError::Corrupt)?
                .to_be_bytes(),
        )?;
        file.write_all(&payload)?;
        file.sync_all()?;
        *next_t += 1;
        Ok(())
    }

    fn tx_range(&self, start: u64, end: Option<u64>) -> Result<Vec<TxRecord>, LogError> {
        let _guard = self.next_t.read().expect("poisoned log lock");
        Ok(read_merged(&self.dir, &self.name)?
            .into_iter()
            .filter(|r| r.t >= start && end.is_none_or(|e| r.t < e))
            .collect())
    }
}

/// Applies the takeover cutoff rule to per-version record lists, in the same
/// way [`read_merged`] does for on-disk files: a record in an older version
/// dies once any later version begins at or below its `t`, dropping only the
/// never-acked stale appends of a deposed writer.
fn merge_versions(mut per_version: Vec<Vec<TxRecord>>) -> Vec<TxRecord> {
    let mut cutoff = u64::MAX;
    for records in per_version.iter_mut().rev() {
        let first = records.first().map(|r| r.t);
        records.retain(|r| r.t < cutoff);
        if let Some(first) = first {
            cutoff = cutoff.min(first);
        }
    }
    per_version.into_iter().flatten().collect()
}

/// Asynchronous object store for transaction-log records.
///
/// Implementations adapt the same native storage system used for blobs and
/// roots. The live log is written **one object per transaction** — a
/// create-only write keyed `(name, version, t)` whose success is the
/// durability point — so an append is O(1) (a small insert) instead of a
/// read-modify-write of a growing chunk. On a SQL backend that is a
/// row-per-commit insert; on an object store, a create-only `PUT`.
///
/// Earlier releases wrote a different layout: a sequence of *chunk* objects
/// `(name, version, chunk)`, each a run of framed records, appended in place
/// and rolled at a size cap. Those objects are still read, read-only, through
/// [`Self::list_legacy_chunks`] / [`Self::read_legacy_chunk`], so a log
/// written by an older binary keeps replaying after an upgrade; new records
/// are always written in the per-transaction layout.
#[async_trait]
pub trait NativeLogStorage: Send + Sync {
    /// Create-only, atomic write of one contiguous batch of transactions as a
    /// single object, keyed by the batch's last `t`. Each element is
    /// `(t, framed_bytes)` in ascending `t`; the object holds their framed
    /// bytes concatenated (the same encoding a multi-record chunk uses).
    /// Returns `Ok(true)` when written and `Ok(false)` when an object already
    /// exists for that last-`t` — a lost create race or a retry of an
    /// already-durable batch. The create-only condition is the log's fence: a
    /// given `(version, last t)` is written at most once, and the batch is
    /// durable in full or not at all.
    ///
    /// # Errors
    /// Returns an error when the native backend cannot publish the object.
    async fn put_batch(
        &self,
        name: &str,
        version: u64,
        records: &[(u64, Vec<u8>)],
    ) -> Result<bool, LogError>;
    /// Reads the bytes of the batch object keyed by last-`t` `t` for
    /// `(name, version)`.
    ///
    /// # Errors
    /// Returns an error when the native backend cannot read the object.
    async fn read_record(
        &self,
        name: &str,
        version: u64,
        t: u64,
    ) -> Result<Option<Vec<u8>>, LogError>;
    /// Lists every `(version, last-t)` batch object present for `name`.
    ///
    /// # Errors
    /// Returns an error when the native backend cannot enumerate log objects
    /// or returns an invalid identifier.
    async fn list_records(&self, name: &str) -> Result<Vec<(u64, u64)>, LogError>;
    /// Reads one legacy chunk object (pre-per-record layout), for read-only
    /// replay of logs written by older binaries.
    ///
    /// # Errors
    /// Returns an error when the native backend cannot read the chunk object.
    async fn read_legacy_chunk(
        &self,
        name: &str,
        version: u64,
        chunk: u64,
    ) -> Result<Option<Vec<u8>>, LogError>;
    /// Lists every legacy `(version, chunk)` object present for `name`, for
    /// read-only replay of older logs. Returns an empty list on a store that
    /// only ever wrote the per-record layout.
    ///
    /// # Errors
    /// Returns an error when the native backend cannot enumerate log objects
    /// or returns an invalid identifier.
    async fn list_legacy_chunks(&self, name: &str) -> Result<Vec<(u64, u64)>, LogError>;
    /// Deletes every log object for `name`, in both layouts.
    ///
    /// # Errors
    /// Returns an error when the native backend cannot remove an object.
    async fn delete_all(&self, name: &str) -> Result<(), LogError>;
}

/// Versioned transaction log backed by a native key/value-style store.
///
/// Each transaction is written as its own create-only object keyed
/// `(name, write_version, t)`, so an append is a single small insert with no
/// read and no growing buffer. The writer is the sole appender under its lease
/// version (the fence gives each active owner its own version; a deposed
/// writer's stale appends land in a version the takeover cutoff discards), so
/// tracking `next_t` in memory is all the append state required.
pub struct NativeVersionedLog<S: ?Sized> {
    storage: Arc<S>,
    name: String,
    write_version: u64,
    /// Next `t` this writer will accept; also serializes concurrent appends.
    next_t: tokio::sync::Mutex<u64>,
}

impl<S: NativeLogStorage + ?Sized + 'static> NativeVersionedLog<S> {
    /// Opens the log for writing under `write_version`.
    ///
    /// # Errors
    /// Returns an error when stored records cannot be read or decoded.
    pub async fn open(storage: Arc<S>, name: &str, write_version: u64) -> Result<Self, LogError> {
        // The merged view across every version and both layouts establishes the
        // next `t` — the takeover cutoff may place it past this writer's own
        // last record.
        let records = read_native_merged(storage.as_ref(), name).await?;
        let next_t = records.last().map_or(1, |r| r.t + 1);
        Ok(Self {
            storage,
            name: name.to_owned(),
            write_version,
            next_t: tokio::sync::Mutex::new(next_t),
        })
    }
}

#[async_trait]
impl<S: NativeLogStorage + ?Sized + 'static> TransactionLog for NativeVersionedLog<S> {
    fn append(&self, record: &TxRecord) -> Result<(), LogError> {
        let _ = record;
        Err(LogError::AsyncOnly)
    }

    async fn append_async(&self, record: &TxRecord) -> Result<(), LogError> {
        self.append_batch_async(std::slice::from_ref(record)).await
    }

    async fn append_batch_async(&self, records: &[TxRecord]) -> Result<(), LogError> {
        if records.is_empty() {
            return Ok(());
        }
        let mut next_t = self.next_t.lock().await;
        // The batch must be exactly the next contiguous run of transactions.
        for (offset, record) in records.iter().enumerate() {
            if record.t != *next_t + offset as u64 {
                return Err(LogError::Corrupt);
            }
        }
        let framed = records
            .iter()
            .map(|record| {
                let mut bytes = Vec::new();
                append_framed_record(&mut bytes, record)?;
                Ok((record.t, bytes))
            })
            .collect::<Result<Vec<_>, LogError>>()?;
        // Create-only write of the whole batch as one object. As the sole
        // appender under this lease version, an object that already exists for
        // this last-`t` is a duplicate or a racing writer under our version —
        // never a legitimate append — so reject it rather than overwrite.
        if !self
            .storage
            .put_batch(&self.name, self.write_version, &framed)
            .await?
        {
            return Err(LogError::Corrupt);
        }
        *next_t += records.len() as u64;
        Ok(())
    }

    fn tx_range(&self, start: u64, end: Option<u64>) -> Result<Vec<TxRecord>, LogError> {
        let _ = (start, end);
        Err(LogError::AsyncOnly)
    }

    async fn tx_range_async(
        &self,
        start: u64,
        end: Option<u64>,
    ) -> Result<Vec<TxRecord>, LogError> {
        // Range/replay must merge every version (for the takeover cutoff), so
        // they read the store; the lock only serializes them with appends.
        let _guard = self.next_t.lock().await;
        Ok(read_native_merged(self.storage.as_ref(), &self.name)
            .await?
            .into_iter()
            .filter(|r| r.t >= start && end.is_none_or(|e| r.t < e))
            .collect())
    }
}

async fn read_native_merged<S: NativeLogStorage + ?Sized>(
    storage: &S,
    name: &str,
) -> Result<Vec<TxRecord>, LogError> {
    use std::collections::BTreeMap;

    // Gather every version's records from both layouts, keyed by version so the
    // cross-version takeover cutoff below sees them in ascending version order.
    let mut per_version: BTreeMap<u64, Vec<TxRecord>> = BTreeMap::new();

    // Legacy chunk objects (read-only): a version's chunks concatenate in chunk
    // order, which is the order they were filled — i.e. transaction order.
    // Empty on a store that only ever wrote the per-record layout.
    let mut chunks = storage.list_legacy_chunks(name).await?;
    chunks.sort_unstable();
    for (version, chunk) in chunks {
        let bytes = storage
            .read_legacy_chunk(name, version, chunk)
            .await?
            .unwrap_or_default();
        per_version
            .entry(version)
            .or_default()
            .extend(decode_framed_records(&bytes)?);
    }

    // Per-record objects, one framed record each.
    let mut records = storage.list_records(name).await?;
    records.sort_unstable();
    for (version, t) in records {
        let bytes = storage
            .read_record(name, version, t)
            .await?
            .unwrap_or_default();
        per_version
            .entry(version)
            .or_default()
            .extend(decode_framed_records(&bytes)?);
    }

    // Order each version's records by `t` (a version is written in a single
    // layout in practice; sorting keeps even a version that carries both — a
    // legacy tail then per-record appends — correct), then apply the takeover
    // cutoff across versions.
    let per_version: Vec<Vec<TxRecord>> = per_version
        .into_values()
        .map(|mut records| {
            records.sort_by_key(|record| record.t);
            records
        })
        .collect();
    let merged = merge_versions(per_version);
    for pair in merged.windows(2) {
        if pair[1].t != pair[0].t + 1 {
            return Err(LogError::Corrupt);
        }
    }
    Ok(merged)
}

/// Shared store of one log's records, each tagged with the lease version it
/// was appended under.
type VersionedRecords = Arc<Mutex<Vec<(u64, TxRecord)>>>;

/// Process-shared registry of in-memory transaction logs, keyed by database
/// name. It plays the role the log directory plays for [`VersionedLog`]:
/// opening the same name (under any lease version) reaches the same records,
/// so a mem-backed transactor recovers state across `open`/`create` calls
/// within one process. Cloning a registry shares its storage.
#[derive(Clone, Default)]
pub struct MemLogRegistry {
    logs: Arc<Mutex<HashMap<String, VersionedRecords>>>,
}

impl MemLogRegistry {
    /// Creates an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    fn entry(&self, name: &str) -> VersionedRecords {
        Arc::clone(
            self.logs
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .entry(name.to_owned())
                .or_default(),
        )
    }

    /// Opens the named log for writing under `write_version`, mirroring
    /// [`VersionedLog::open`] with in-memory storage.
    #[must_use]
    pub fn open(&self, name: &str, write_version: u64) -> MemVersionedLog {
        let records = self.entry(name);
        let next_t = {
            let guard = records
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            MemVersionedLog::merged(&guard)
                .last()
                .map_or(1, |r| r.t + 1)
        };
        MemVersionedLog {
            records,
            write_version,
            next_t: Mutex::new(next_t),
        }
    }

    /// Reports whether any records exist for the named log.
    #[must_use]
    pub fn exists(&self, name: &str) -> bool {
        self.logs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(name)
            .is_some_and(|entry| {
                !entry
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .is_empty()
            })
    }

    /// Discards every record for the named log.
    pub fn delete_all(&self, name: &str) {
        self.logs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(name);
    }
}

/// An in-memory transaction log with the same per-lease-version merge
/// semantics as [`VersionedLog`], obtained from a [`MemLogRegistry`]. Used by
/// the mem-backed transactor: fully ephemeral, confined to one process.
pub struct MemVersionedLog {
    records: VersionedRecords,
    write_version: u64,
    /// The next `t` this writer will accept, tracked per opened instance
    /// exactly as [`VersionedLog`] does — a deposed writer keeps appending
    /// under its own stale count, and the merge cutoff discards those records.
    next_t: Mutex<u64>,
}

impl MemVersionedLog {
    fn merged(records: &[(u64, TxRecord)]) -> Vec<TxRecord> {
        let mut versions: Vec<u64> = records.iter().map(|(version, _)| *version).collect();
        versions.sort_unstable();
        versions.dedup();
        let per_version = versions
            .into_iter()
            .map(|version| {
                records
                    .iter()
                    .filter(|(record_version, _)| *record_version == version)
                    .map(|(_, record)| record.clone())
                    .collect::<Vec<_>>()
            })
            .collect();
        merge_versions(per_version)
    }
}

impl TransactionLog for MemVersionedLog {
    fn append(&self, record: &TxRecord) -> Result<(), LogError> {
        let mut next_t = self
            .next_t
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *next_t != record.t {
            return Err(LogError::Corrupt);
        }
        self.records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((self.write_version, record.clone()));
        *next_t += 1;
        Ok(())
    }

    fn tx_range(&self, start: u64, end: Option<u64>) -> Result<Vec<TxRecord>, LogError> {
        let records = self
            .records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(Self::merged(&records)
            .into_iter()
            .filter(|r| r.t >= start && end.is_none_or(|e| r.t < e))
            .collect())
    }
}

fn version_path(dir: &Path, name: &str, version: u64) -> PathBuf {
    if version == 0 {
        dir.join(format!("{name}.log"))
    } else {
        dir.join(format!("{name}.v{version}.log"))
    }
}

/// Existing version files for `name`, sorted by version.
fn version_files(dir: &Path, name: &str) -> Vec<(u64, PathBuf)> {
    let mut files = Vec::new();
    let legacy = version_path(dir, name, 0);
    if legacy.is_file() {
        files.push((0, legacy));
    }
    let prefix = format!("{name}.v");
    if let Ok(entries) = fs::read_dir(dir) {
        for entry in entries.flatten() {
            let file_name = entry.file_name();
            let Some(text) = file_name.to_str() else {
                continue;
            };
            if let Some(version) = text
                .strip_prefix(&prefix)
                .and_then(|rest| rest.strip_suffix(".log"))
                .and_then(|v| v.parse::<u64>().ok())
                && version > 0
            {
                files.push((version, entry.path()));
            }
        }
    }
    files.sort_by_key(|(version, _)| *version);
    files
}

/// Merges every version file, applying the takeover cutoff rule, and
/// verifies the surviving sequence is contiguous.
fn read_merged(dir: &Path, name: &str) -> Result<Vec<TxRecord>, LogError> {
    let files = version_files(dir, name);
    let mut per_file: Vec<Vec<TxRecord>> = Vec::with_capacity(files.len());
    for (_, path) in &files {
        per_file.push(read_records(path)?.0);
    }
    // A record in an older file is dead once any later file starts at or
    // below its t: every record acked under version v precedes the first
    // record of every later version (the successor replayed it before
    // choosing its own first t), so only never-acked stale appends die.
    let merged = merge_versions(per_file);
    for pair in merged.windows(2) {
        if pair[1].t != pair[0].t + 1 {
            return Err(LogError::Corrupt);
        }
    }
    Ok(merged)
}

fn encode_record(record: &TxRecord) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&record.t.to_be_bytes());
    out.extend_from_slice(&record.tx_instant.to_be_bytes());
    out.extend_from_slice(&(record.datoms.len() as u64).to_be_bytes());
    for d in &record.datoms {
        out.extend_from_slice(&d.e.raw().to_be_bytes());
        out.extend_from_slice(&d.a.raw().to_be_bytes());
        out.extend_from_slice(&d.tx.raw().to_be_bytes());
        out.push(u8::from(d.added));
        let v = encode_value(&d.v);
        out.extend_from_slice(&(v.len() as u64).to_be_bytes());
        out.extend_from_slice(&v);
    }
    out
}
fn decode_record(mut bytes: &[u8]) -> Result<TxRecord, LogError> {
    fn take<'a>(bytes: &mut &'a [u8], n: usize) -> Result<&'a [u8], LogError> {
        let value = bytes.get(..n).ok_or(LogError::Corrupt)?;
        *bytes = &bytes[n..];
        Ok(value)
    }
    fn u64_be(bytes: &mut &[u8]) -> Result<u64, LogError> {
        Ok(u64::from_be_bytes(
            take(bytes, 8)?.try_into().map_err(|_| LogError::Corrupt)?,
        ))
    }
    let t = u64_be(&mut bytes)?;
    let tx_instant = i64::from_be_bytes(
        take(&mut bytes, 8)?
            .try_into()
            .map_err(|_| LogError::Corrupt)?,
    );
    let count = u64_be(&mut bytes)?;
    let mut datoms = Vec::new();
    for _ in 0..count {
        let e = EntityId::from_raw(u64_be(&mut bytes)?);
        let a = EntityId::from_raw(u64_be(&mut bytes)?);
        let tx = EntityId::from_raw(u64_be(&mut bytes)?);
        let added = take(&mut bytes, 1)?[0] != 0;
        let len = usize::try_from(u64_be(&mut bytes)?).map_err(|_| LogError::Corrupt)?;
        let raw = take(&mut bytes, len)?;
        let (v, used) = decode_value(raw).map_err(|_| LogError::Corrupt)?;
        if used != len {
            return Err(LogError::Corrupt);
        }
        datoms.push(Datom { e, a, v, tx, added });
    }
    if !bytes.is_empty() {
        return Err(LogError::Corrupt);
    }
    Ok(TxRecord {
        t,
        tx_instant,
        datoms,
    })
}
/// Reads fully written records plus the byte length of that durable prefix.
///
/// A record cut short by a crash mid-append (truncated length prefix or
/// payload) ends the scan; a fully present record that fails to decode is
/// genuine corruption and errors.
fn read_records(path: &Path) -> Result<(Vec<TxRecord>, u64), LogError> {
    let mut file = File::open(path)?;
    let mut records = Vec::new();
    let mut durable_len = 0_u64;
    loop {
        let mut len = [0; 8];
        match file.read_exact(&mut len) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }
        let len = usize::try_from(u64::from_be_bytes(len)).map_err(|_| LogError::Corrupt)?;
        let mut payload = vec![0; len];
        match file.read_exact(&mut payload) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }
        records.push(decode_record(&payload)?);
        durable_len += 8 + len as u64;
    }
    Ok((records, durable_len))
}

/// Appends one length-prefixed encoded record to `out`.
///
/// # Errors
/// Returns an error if the record payload length is not representable.
pub fn append_framed_record(out: &mut Vec<u8>, record: &TxRecord) -> Result<(), LogError> {
    let payload = encode_record(record);
    out.extend_from_slice(
        &u64::try_from(payload.len())
            .map_err(|_| LogError::Corrupt)?
            .to_be_bytes(),
    );
    out.extend_from_slice(&payload);
    Ok(())
}

/// Decodes all records from a length-prefixed byte slice.
///
/// Unlike filesystem crash recovery, native stores publish whole values
/// atomically, so any trailing partial frame is treated as corruption.
///
/// # Errors
/// Returns an error when any frame is truncated, has an invalid length, or
/// contains a corrupt encoded transaction record.
pub fn decode_framed_records(mut bytes: &[u8]) -> Result<Vec<TxRecord>, LogError> {
    let mut records = Vec::new();
    while !bytes.is_empty() {
        if bytes.len() < 8 {
            return Err(LogError::Corrupt);
        }
        let len = usize::try_from(u64::from_be_bytes(
            bytes[..8].try_into().map_err(|_| LogError::Corrupt)?,
        ))
        .map_err(|_| LogError::Corrupt)?;
        bytes = &bytes[8..];
        let payload = bytes.get(..len).ok_or(LogError::Corrupt)?;
        records.push(decode_record(payload)?);
        bytes = &bytes[len..];
    }
    Ok(records)
}
