//! Durable append-only transaction logs with replay and range scans.

use corium_core::{
    Datom, EntityId,
    encoding::{decode_value, encode_value},
};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
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
pub struct FileLog {
    path: PathBuf,
    lock: RwLock<()>,
}
impl FileLog {
    /// Opens or creates a log file.
    ///
    /// # Errors
    /// Returns an error if the file cannot be created or existing data is corrupt.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, LogError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        OpenOptions::new().create(true).append(true).open(&path)?;
        let log = Self {
            path,
            lock: RwLock::new(()),
        };
        log.replay()?;
        Ok(log)
    }
}
impl TransactionLog for FileLog {
    fn append(&self, record: &TxRecord) -> Result<(), LogError> {
        let _guard = self.lock.write().expect("poisoned log lock");
        let existing = read_records(&self.path)?;
        if existing.last().map_or(1, |r| r.t + 1) != record.t {
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
        Ok(())
    }
    fn tx_range(&self, start: u64, end: Option<u64>) -> Result<Vec<TxRecord>, LogError> {
        let _guard = self.lock.read().expect("poisoned log lock");
        Ok(read_records(&self.path)?
            .into_iter()
            .filter(|r| r.t >= start && end.is_none_or(|e| r.t < e))
            .collect())
    }
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
fn read_records(path: &Path) -> Result<Vec<TxRecord>, LogError> {
    let mut file = File::open(path)?;
    let mut records = Vec::new();
    loop {
        let mut len = [0; 8];
        match file.read_exact(&mut len) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }
        let len = usize::try_from(u64::from_be_bytes(len)).map_err(|_| LogError::Corrupt)?;
        let mut payload = vec![0; len];
        file.read_exact(&mut payload)
            .map_err(|_| LogError::Corrupt)?;
        records.push(decode_record(&payload)?);
    }
    Ok(records)
}
