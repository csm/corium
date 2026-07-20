//! Durable append-only transaction logs with replay and range scans.

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
}

/// Common transaction log interface.
pub trait TransactionLog: Send + Sync {
    /// Durably appends exactly the next transaction.
    ///
    /// # Errors
    /// Returns an error for I/O failure, corruption, or a non-contiguous `t`.
    fn append(&self, record: &TxRecord) -> Result<(), LogError>;
    /// Returns records in the half-open transaction range `[start, end)`.
    ///
    /// # Errors
    /// Returns an error when stored records cannot be read or decoded.
    fn tx_range(&self, start: u64, end: Option<u64>) -> Result<Vec<TxRecord>, LogError>;
    /// Replays every committed record.
    ///
    /// # Errors
    /// Returns an error when stored records cannot be read or decoded.
    fn replay(&self) -> Result<Vec<TxRecord>, LogError> {
        self.tx_range(0, None)
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
