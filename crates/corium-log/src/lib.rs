//! Durable append-only transaction logs with replay and range scans.

use async_trait::async_trait;
use corium_core::{
    Datom, EntityId,
    encoding::{decode_value, encode_value},
};
use corium_crypt::{
    CryptError, LogHeader, SecretKey, decrypt_log_record, encrypt_log_record,
    is_encrypted_log_record, parse_log_header,
};
use std::{
    collections::{BTreeMap, HashMap},
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, RwLock},
};
use thiserror::Error;

const CHECKSUMMED_FRAME: u64 = 1 << 63;
const FRAME_CHECKSUM_LEN: usize = size_of::<u32>();
const RANGE_READ_CHUNK_BYTES: u64 = 4 * 1024 * 1024;
const MAX_CACHED_READ_VERSION_FILES: usize = 8;

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
    /// The log holds encrypted records and this process has no storage key.
    #[error("transaction log is encrypted; no storage key is configured")]
    Encrypted,
    /// A storage key is configured but the log holds cleartext records.
    /// Encryption is fixed at database creation, so this is a misconfigured
    /// process pointed at somebody else's log, not a database to upgrade.
    #[error("transaction log is not encrypted, but a storage key is configured")]
    Unencrypted,
    /// A record names a key epoch this process cannot resolve.
    #[error("transaction log record uses storage key epoch {0}, which is unavailable")]
    MissingKeyEpoch(u32),
    /// Encryption or authentication of a record payload failed.
    #[error("transaction log record encryption failed: {0}")]
    Crypt(#[from] CryptError),
}

/// Encrypts and decrypts transaction-log record payloads for one database.
///
/// Frame lengths and CRC32C checksums stay cleartext, as do each record's key
/// epoch and transaction number, so frame scanning, range reads, and recovery
/// truncation need no key. The payload — the transaction's datoms — does not.
///
/// The key set is an immutable snapshot of already-unwrapped storage keys, the
/// same shape `EncryptedBlobStore` holds: KMS access belongs on database open
/// and key-manifest reload, not on the append path. A rotation replaces the
/// cipher rather than mutating it.
pub struct LogCipher {
    lineage: Vec<u8>,
    current_epoch: u32,
    keys: BTreeMap<u32, SecretKey>,
}

impl LogCipher {
    /// Creates a cipher over every readable epoch, writing under
    /// `current_epoch`.
    ///
    /// `lineage` identifies the database and is authenticated into every
    /// record, so a record cannot be moved between databases.
    ///
    /// # Errors
    ///
    /// Returns [`LogError::MissingKeyEpoch`] when `current_epoch` has no key.
    pub fn new(
        lineage: impl Into<Vec<u8>>,
        current_epoch: u32,
        keys: impl IntoIterator<Item = (u32, SecretKey)>,
    ) -> Result<Self, LogError> {
        let keys = keys.into_iter().collect::<BTreeMap<_, _>>();
        if !keys.contains_key(&current_epoch) {
            return Err(LogError::MissingKeyEpoch(current_epoch));
        }
        Ok(Self {
            lineage: lineage.into(),
            current_epoch,
            keys,
        })
    }

    /// Creates a single-epoch cipher.
    #[must_use]
    pub fn with_key(lineage: impl Into<Vec<u8>>, epoch: u32, key: SecretKey) -> Self {
        Self {
            lineage: lineage.into(),
            current_epoch: epoch,
            keys: BTreeMap::from([(epoch, key)]),
        }
    }

    /// Returns the epoch new records are written under.
    #[must_use]
    pub fn current_epoch(&self) -> u32 {
        self.current_epoch
    }

    fn key(&self, epoch: u32) -> Result<&SecretKey, LogError> {
        self.keys
            .get(&epoch)
            .ok_or(LogError::MissingKeyEpoch(epoch))
    }

    fn seal(&self, log_version: u64, t: u64, plaintext: &[u8]) -> Result<Vec<u8>, LogError> {
        Ok(encrypt_log_record(
            self.key(self.current_epoch)?,
            self.current_epoch,
            &self.lineage,
            log_version,
            t,
            plaintext,
        )?)
    }

    /// Opens a payload whose header the caller has already parsed, so the
    /// decode path reads it once.
    fn open(
        &self,
        log_version: u64,
        header: LogHeader,
        payload: &[u8],
    ) -> Result<Vec<u8>, LogError> {
        Ok(decrypt_log_record(
            self.key(header.epoch)?,
            &self.lineage,
            log_version,
            payload,
        )?)
    }
}

/// How one log file or object encodes record payloads: cleartext, or sealed
/// under a storage key and bound to the file's lease version.
#[derive(Clone, Default)]
struct RecordCodec {
    cipher: Option<Arc<LogCipher>>,
    log_version: u64,
}

impl RecordCodec {
    fn plaintext() -> Self {
        Self::default()
    }

    fn new(cipher: Option<Arc<LogCipher>>, log_version: u64) -> Self {
        Self {
            cipher,
            log_version,
        }
    }

    fn encode(&self, record: &TxRecord) -> Result<Vec<u8>, LogError> {
        let encoded = encode_record(record);
        match &self.cipher {
            Some(cipher) => cipher.seal(self.log_version, record.t, &encoded),
            None => Ok(encoded),
        }
    }

    fn decode(&self, payload: &[u8]) -> Result<TxRecord, LogError> {
        match (&self.cipher, is_encrypted_log_record(payload)) {
            (Some(cipher), true) => {
                let header = parse_log_header(payload)?;
                let record = decode_record(&cipher.open(self.log_version, header, payload)?)?;
                // The cleartext `t` drives frame indexing and recovery, so a
                // disagreement with the authenticated payload would let the
                // index address a record by a number it does not carry. The
                // AAD puts this out of an attacker's reach — the header is
                // authenticated — so what remains is a writer that sealed one
                // `t` under another, and that must not reach an index.
                if record.t != header.t {
                    return Err(LogError::Corrupt);
                }
                Ok(record)
            }
            (None, true) => Err(LogError::Encrypted),
            (Some(_), false) => Err(LogError::Unencrypted),
            (None, false) => decode_record(payload),
        }
    }
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
/// The file descriptor remains open for the lifetime of the log. An in-memory
/// `(t, byte offset, frame length)` index is built during recovery, extended
/// on append, and used to read only the frames selected by a range scan.
///
/// A crash mid-append leaves a torn, never-acked record at the tail; `open`
/// truncates it away so replay stops at the durability point of the last
/// acked transaction and later appends extend a clean tail.
pub struct FileLog {
    state: RwLock<IndexedFile>,
}

impl FileLog {
    /// Opens or creates a log file, dropping any torn tail left by a crash.
    ///
    /// # Errors
    /// Returns an error if the file cannot be created or a fully written
    /// record is corrupt.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, LogError> {
        Self::open_with(path, None)
    }

    /// Opens or creates a log whose record payloads are sealed under `cipher`.
    ///
    /// A single-file log has no lease versions, so its records bind lease
    /// version 0 — the same number [`VersionedLog`] gives a pre-HA `{name}.log`.
    ///
    /// # Errors
    /// Returns an error if the file cannot be created, a fully written record
    /// is corrupt, or a record cannot be authenticated.
    pub fn open_sealed(path: impl AsRef<Path>, cipher: Arc<LogCipher>) -> Result<Self, LogError> {
        Self::open_with(path, Some(cipher))
    }

    fn open_with(path: impl AsRef<Path>, cipher: Option<Arc<LogCipher>>) -> Result<Self, LogError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let file = IndexedFile::open(&path, true, true, RecordCodec::new(cipher, 0))?;
        file.validate_contiguous_prefix(file.frames.len())?;
        Ok(Self {
            state: RwLock::new(file),
        })
    }
}
impl TransactionLog for FileLog {
    fn append(&self, record: &TxRecord) -> Result<(), LogError> {
        let mut state = self.state.write().expect("poisoned log lock");
        state.refresh()?;
        state.validate_contiguous_prefix(state.frames.len())?;
        if next_t(&state.frames)? != record.t {
            return Err(LogError::Corrupt);
        }
        state.append(record)
    }
    fn tx_range(&self, start: u64, end: Option<u64>) -> Result<Vec<TxRecord>, LogError> {
        if end.is_some_and(|end| end <= start) {
            return Ok(Vec::new());
        }
        let indexed = {
            let state = self.state.read().expect("poisoned log lock");
            range_is_indexed(&state.frames, end)?
        };
        if !indexed {
            let mut state = self.state.write().expect("poisoned log lock");
            state.refresh()?;
            state.validate_contiguous_prefix(state.frames.len())?;
        }
        self.state
            .read()
            .expect("poisoned log lock")
            .tx_range(start, end)
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
    cipher: Option<Arc<LogCipher>>,
    state: RwLock<VersionedLogState>,
}

struct VersionedLogState {
    files: Vec<VersionedFile>,
    write_version: Option<u64>,
    next_t: u64,
}

struct VersionedFile {
    version: u64,
    file: IndexedFile,
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
        Self::open_with(dir, name, write_version, None)
    }

    /// Opens the log for writing with record payloads sealed under `cipher`.
    ///
    /// Every version file is read through the same cipher, and each frame is
    /// authenticated against the version of the file holding it.
    ///
    /// # Errors
    /// Returns an error if files cannot be read/created, a fully written
    /// record is corrupt, or a record cannot be authenticated.
    pub fn open_sealed(
        dir: impl AsRef<Path>,
        name: &str,
        write_version: u64,
        cipher: Arc<LogCipher>,
    ) -> Result<Self, LogError> {
        Self::open_with(dir, name, write_version, Some(cipher))
    }

    fn open_with(
        dir: impl AsRef<Path>,
        name: &str,
        write_version: u64,
        cipher: Option<Arc<LogCipher>>,
    ) -> Result<Self, LogError> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        let write_path = version_path(&dir, name, write_version);
        let mut files = Vec::new();
        for (version, path) in version_files(&dir, name) {
            let writable = version == write_version;
            files.push(VersionedFile {
                version,
                file: IndexedFile::open(
                    &path,
                    writable,
                    writable,
                    RecordCodec::new(cipher.clone(), version),
                )?,
            });
            close_cold_version_files(&mut files);
        }
        if !files.iter().any(|file| file.version == write_version) {
            files.push(VersionedFile {
                version: write_version,
                file: IndexedFile::open(
                    &write_path,
                    true,
                    true,
                    RecordCodec::new(cipher.clone(), write_version),
                )?,
            });
            files.sort_by_key(|file| file.version);
        }
        close_cold_version_files(&mut files);
        let cutoffs = validated_version_cutoffs(&files)?;
        let next_t = merged_next_t(&files, &cutoffs)?;
        Ok(Self {
            dir,
            name: name.to_owned(),
            cipher,
            state: RwLock::new(VersionedLogState {
                files,
                write_version: Some(write_version),
                next_t,
            }),
        })
    }

    /// Opens the log read-only (independent inspection or backup); appends fail.
    ///
    /// # Errors
    /// Returns an error when the directory cannot be read or a fully
    /// written record is corrupt.
    pub fn open_read_only(dir: impl AsRef<Path>, name: &str) -> Result<Self, LogError> {
        Self::open_read_only_with(dir, name, None)
    }

    /// Opens the log read-only, opening sealed record payloads with `cipher`.
    ///
    /// # Errors
    /// Returns an error when the directory cannot be read, a fully written
    /// record is corrupt, or a record cannot be authenticated.
    pub fn open_read_only_sealed(
        dir: impl AsRef<Path>,
        name: &str,
        cipher: Arc<LogCipher>,
    ) -> Result<Self, LogError> {
        Self::open_read_only_with(dir, name, Some(cipher))
    }

    fn open_read_only_with(
        dir: impl AsRef<Path>,
        name: &str,
        cipher: Option<Arc<LogCipher>>,
    ) -> Result<Self, LogError> {
        let dir = dir.as_ref().to_path_buf();
        let mut files = open_version_files(&dir, name, cipher.as_ref())?;
        close_cold_version_files(&mut files);
        let cutoffs = validated_version_cutoffs(&files)?;
        Ok(Self {
            name: name.to_owned(),
            cipher,
            state: RwLock::new(VersionedLogState {
                next_t: merged_next_t(&files, &cutoffs)?,
                write_version: None,
                files,
            }),
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
        let mut state = self.state.write().expect("poisoned log lock");
        if state.next_t != record.t {
            return Err(LogError::Corrupt);
        }
        let write_version = state
            .write_version
            .ok_or_else(|| LogError::Native("transaction log is read-only".into()))?;
        let write_index = state
            .files
            .iter()
            .position(|file| file.version == write_version)
            .ok_or(LogError::Corrupt)?;
        let cutoffs = version_cutoffs(&state.files);
        // A current-version writer must extend its own local tail. A deposed
        // writer may append beyond a later version's cutoff; that harmless gap
        // lives entirely in the dead suffix and is discarded during merge.
        if cutoffs[write_index] == u64::MAX
            && !state.files[write_index].file.frames.is_empty()
            && next_t(&state.files[write_index].file.frames)? != record.t
        {
            return Err(LogError::Corrupt);
        }
        state.files[write_index].file.append(record)?;
        state.next_t = state.next_t.checked_add(1).ok_or(LogError::Corrupt)?;
        Ok(())
    }

    fn tx_range(&self, start: u64, end: Option<u64>) -> Result<Vec<TxRecord>, LogError> {
        if end.is_some_and(|end| end <= start) {
            return Ok(Vec::new());
        }
        {
            let state = self.state.read().expect("poisoned log lock");
            let cutoffs = validated_version_cutoffs(&state.files)?;
            if range_is_merged_indexed(&state.files, &cutoffs, end)? {
                return read_merged_range(&state.files, &cutoffs, start, end);
            }
        }
        {
            let mut state = self.state.write().expect("poisoned log lock");
            refresh_version_files(
                &self.dir,
                &self.name,
                self.cipher.as_ref(),
                &mut state.files,
            )?;
            close_cold_version_files(&mut state.files);
            validated_version_cutoffs(&state.files)?;
        }
        let state = self.state.read().expect("poisoned log lock");
        let cutoffs = validated_version_cutoffs(&state.files)?;
        read_merged_range(&state.files, &cutoffs, start, end)
    }
}

/// Applies the takeover cutoff rule to per-version record lists: a record in
/// an older version dies once any later version begins at or below its `t`,
/// dropping only the never-acked stale appends of a deposed writer.
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
    read_only: bool,
    cipher: Option<Arc<LogCipher>>,
    /// Next `t` this writer will accept; also serializes concurrent appends.
    next_t: tokio::sync::Mutex<u64>,
}

impl<S: NativeLogStorage + ?Sized + 'static> NativeVersionedLog<S> {
    /// Opens the log for writing under `write_version`.
    ///
    /// # Errors
    /// Returns an error when stored records cannot be read or decoded.
    pub async fn open(storage: Arc<S>, name: &str, write_version: u64) -> Result<Self, LogError> {
        Self::open_with(storage, name, write_version, None).await
    }

    /// Opens the log for writing with record payloads sealed under `cipher`.
    ///
    /// # Errors
    /// Returns an error when stored records cannot be read, decoded, or
    /// authenticated.
    pub async fn open_sealed(
        storage: Arc<S>,
        name: &str,
        write_version: u64,
        cipher: Arc<LogCipher>,
    ) -> Result<Self, LogError> {
        Self::open_with(storage, name, write_version, Some(cipher)).await
    }

    async fn open_with(
        storage: Arc<S>,
        name: &str,
        write_version: u64,
        cipher: Option<Arc<LogCipher>>,
    ) -> Result<Self, LogError> {
        // The merged view across every version and both layouts establishes the
        // next `t` — the takeover cutoff may place it past this writer's own
        // last record.
        let records = read_native_merged(storage.as_ref(), name, cipher.as_ref()).await?;
        let next_t = records.last().map_or(1, |r| r.t + 1);
        Ok(Self {
            storage,
            name: name.to_owned(),
            write_version,
            read_only: false,
            cipher,
            next_t: tokio::sync::Mutex::new(next_t),
        })
    }

    /// Opens the log for read-only range replay without first scanning it to
    /// initialize writer state.
    #[must_use]
    pub fn open_read_only(storage: Arc<S>, name: &str) -> Self {
        Self::open_read_only_with(storage, name, None)
    }

    /// Opens the log read-only, opening sealed record payloads with `cipher`.
    #[must_use]
    pub fn open_read_only_sealed(storage: Arc<S>, name: &str, cipher: Arc<LogCipher>) -> Self {
        Self::open_read_only_with(storage, name, Some(cipher))
    }

    fn open_read_only_with(storage: Arc<S>, name: &str, cipher: Option<Arc<LogCipher>>) -> Self {
        Self {
            storage,
            name: name.to_owned(),
            write_version: 0,
            read_only: true,
            cipher,
            next_t: tokio::sync::Mutex::new(0),
        }
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
        if self.read_only {
            return Err(LogError::Native("transaction log is read-only".into()));
        }
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
        let codec = RecordCodec::new(self.cipher.clone(), self.write_version);
        let framed = records
            .iter()
            .map(|record| {
                let mut bytes = Vec::new();
                append_framed_payload(&mut bytes, &codec.encode(record)?)?;
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
        Ok(
            read_native_merged(self.storage.as_ref(), &self.name, self.cipher.as_ref())
                .await?
                .into_iter()
                .filter(|r| r.t >= start && end.is_none_or(|e| r.t < e))
                .collect(),
        )
    }
}

async fn read_native_merged<S: NativeLogStorage + ?Sized>(
    storage: &S,
    name: &str,
    cipher: Option<&Arc<LogCipher>>,
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
            .extend(decode_framed_payloads(
                &bytes,
                &RecordCodec::new(cipher.map(Arc::clone), version),
            )?);
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
            .extend(decode_framed_payloads(
                &bytes,
                &RecordCodec::new(cipher.map(Arc::clone), version),
            )?);
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

#[derive(Clone, Copy)]
struct FrameIndex {
    t: u64,
    offset: u64,
    len: u64,
}

/// One cached file descriptor and its in-memory transaction-to-byte index.
///
/// Opening scans and validates the durable prefix once. Later refreshes scan
/// only bytes appended after that prefix, and concurrent range readers use
/// positional I/O for the selected frames instead of decoding from byte zero.
struct IndexedFile {
    path: PathBuf,
    file: Option<Arc<File>>,
    writable: bool,
    poisoned: bool,
    frames: Vec<FrameIndex>,
    durable_len: u64,
    first_gap: Option<usize>,
    codec: RecordCodec,
}

impl IndexedFile {
    fn open(
        path: &Path,
        writable: bool,
        truncate_torn: bool,
        codec: RecordCodec,
    ) -> Result<Self, LogError> {
        let file = Arc::new(open_index_file(path, writable)?);
        let (frames, durable_len) = scan_frames(file.as_ref(), 0, &codec)?;
        validate_sorted_frames(&frames)?;
        if truncate_torn && file.metadata()?.len() > durable_len {
            file.set_len(durable_len)?;
            file.sync_all()?;
        }
        Ok(Self {
            path: path.to_path_buf(),
            file: Some(file),
            writable,
            poisoned: false,
            first_gap: first_gap_index(&frames),
            frames,
            durable_len,
            codec,
        })
    }

    fn refresh(&mut self) -> Result<(), LogError> {
        self.ensure_healthy()?;
        let file_len = self.physical_len()?;
        if file_len < self.durable_len {
            let file = self.open_for_read()?;
            let (frames, durable_len) = scan_frames(file.as_ref(), 0, &self.codec)?;
            validate_sorted_frames(&frames)?;
            self.first_gap = first_gap_index(&frames);
            self.frames = frames;
            self.durable_len = durable_len;
        } else if file_len > self.durable_len {
            let file = self.open_for_read()?;
            let (new_frames, durable_len) =
                scan_frames(file.as_ref(), self.durable_len, &self.codec)?;
            validate_sorted_extension(&self.frames, &new_frames)?;
            let existing_len = self.frames.len();
            if self.first_gap.is_none() {
                self.first_gap =
                    extension_first_gap(&self.frames, &new_frames).map(|gap| existing_len + gap);
            }
            self.frames.extend(new_frames);
            self.durable_len = durable_len;
        }
        Ok(())
    }

    fn append(&mut self, record: &TxRecord) -> Result<(), LogError> {
        self.ensure_healthy()?;
        if !self.writable {
            return Err(LogError::Native("transaction log is read-only".into()));
        }
        let file = self.open_for_read()?;
        // Never truncate here: durable_len belongs to this handle, and bytes
        // beyond it may be an acknowledged append made by another handle.
        if file.metadata()?.len() != self.durable_len {
            return Err(LogError::Corrupt);
        }

        let mut frame = Vec::new();
        append_framed_payload(&mut frame, &self.codec.encode(record)?)?;
        let frame_len = u64::try_from(frame.len()).map_err(|_| LogError::Corrupt)?;
        let offset = self.durable_len;
        let mut writer = file.as_ref();
        if let Err(error) = writer.write_all(&frame) {
            // A short write makes the tail uncertain. Poison this handle so a
            // retry cannot adopt or append beyond a never-acknowledged frame.
            self.poisoned = true;
            return Err(error.into());
        }
        if let Err(error) = file.sync_all() {
            self.poisoned = true;
            return Err(error.into());
        }
        self.durable_len = self
            .durable_len
            .checked_add(frame_len)
            .ok_or(LogError::Corrupt)?;
        if self.first_gap.is_none()
            && self
                .frames
                .last()
                .is_some_and(|previous| previous.t.checked_add(1) != Some(record.t))
        {
            self.first_gap = Some(self.frames.len());
        }
        self.frames.push(FrameIndex {
            t: record.t,
            offset,
            len: frame_len,
        });
        Ok(())
    }

    fn tx_range(&self, start: u64, end: Option<u64>) -> Result<Vec<TxRecord>, LogError> {
        self.ensure_healthy()?;
        let first = self.frames.partition_point(|frame| frame.t < start);
        let last = end.map_or(self.frames.len(), |end| {
            self.frames.partition_point(|frame| frame.t < end)
        });
        if first >= last {
            return Ok(Vec::new());
        }

        let file = self.open_for_read()?;
        let mut records = Vec::with_capacity(last - first);
        let mut chunk_first = first;
        while chunk_first < last {
            let offset = self.frames[chunk_first].offset;
            let mut chunk_last = chunk_first + 1;
            while chunk_last < last {
                let candidate_end = frame_end(self.frames[chunk_last])?;
                if candidate_end.checked_sub(offset).ok_or(LogError::Corrupt)?
                    > RANGE_READ_CHUNK_BYTES
                {
                    break;
                }
                chunk_last += 1;
            }
            let byte_end = frame_end(self.frames[chunk_last - 1])?;
            let byte_len = usize::try_from(byte_end.checked_sub(offset).ok_or(LogError::Corrupt)?)
                .map_err(|_| LogError::Corrupt)?;
            let mut bytes = vec![0; byte_len];
            read_exact_at(file.as_ref(), &mut bytes, offset)?;
            let chunk_records = decode_framed_payloads(&bytes, &self.codec)?;
            if chunk_records.len() != chunk_last - chunk_first
                || chunk_records
                    .iter()
                    .zip(&self.frames[chunk_first..chunk_last])
                    .any(|(record, frame)| record.t != frame.t)
            {
                return Err(LogError::Corrupt);
            }
            records.extend(chunk_records);
            chunk_first = chunk_last;
        }
        Ok(records)
    }

    fn validate_contiguous_prefix(&self, retained: usize) -> Result<(), LogError> {
        if self.first_gap.is_some_and(|gap| gap < retained) {
            return Err(LogError::Corrupt);
        }
        Ok(())
    }

    fn ensure_healthy(&self) -> Result<(), LogError> {
        if self.poisoned {
            return Err(io::Error::other("transaction log handle is poisoned").into());
        }
        Ok(())
    }

    fn open_for_read(&self) -> Result<Arc<File>, LogError> {
        self.file.as_ref().map_or_else(
            || Ok(Arc::new(open_index_file(&self.path, false)?)),
            |file| Ok(Arc::clone(file)),
        )
    }

    fn physical_len(&self) -> Result<u64, LogError> {
        Ok(self
            .file
            .as_ref()
            .map_or_else(|| fs::metadata(&self.path), |file| file.metadata())?
            .len())
    }

    fn close_cached_reader(&mut self) {
        if !self.writable {
            self.file = None;
        }
    }
}

fn open_index_file(path: &Path, writable: bool) -> Result<File, io::Error> {
    let mut options = OpenOptions::new();
    options.read(true);
    if writable {
        options.create(true).write(true).append(true);
    }
    options.open(path)
}

fn validate_sorted_frames(frames: &[FrameIndex]) -> Result<(), LogError> {
    for pair in frames.windows(2) {
        if pair[0].t >= pair[1].t {
            return Err(LogError::Corrupt);
        }
    }
    Ok(())
}

fn validate_sorted_extension(
    existing: &[FrameIndex],
    appended: &[FrameIndex],
) -> Result<(), LogError> {
    validate_sorted_frames(appended)?;
    if let (Some(previous), Some(next)) = (existing.last(), appended.first())
        && previous.t >= next.t
    {
        return Err(LogError::Corrupt);
    }
    Ok(())
}

fn first_gap_index(frames: &[FrameIndex]) -> Option<usize> {
    frames
        .windows(2)
        .position(|pair| pair[0].t.checked_add(1) != Some(pair[1].t))
        .map(|index| index + 1)
}

fn extension_first_gap(existing: &[FrameIndex], appended: &[FrameIndex]) -> Option<usize> {
    if let (Some(previous), Some(next)) = (existing.last(), appended.first())
        && previous.t.checked_add(1) != Some(next.t)
    {
        return Some(0);
    }
    first_gap_index(appended)
}

fn frame_end(frame: FrameIndex) -> Result<u64, LogError> {
    frame.offset.checked_add(frame.len).ok_or(LogError::Corrupt)
}

fn next_t(frames: &[FrameIndex]) -> Result<u64, LogError> {
    frames.last().map_or(Ok(1), |frame| {
        frame.t.checked_add(1).ok_or(LogError::Corrupt)
    })
}

fn range_is_indexed(frames: &[FrameIndex], end: Option<u64>) -> Result<bool, LogError> {
    end.map_or(Ok(false), |end| Ok(end <= next_t(frames)?))
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

fn open_version_files(
    dir: &Path,
    name: &str,
    cipher: Option<&Arc<LogCipher>>,
) -> Result<Vec<VersionedFile>, LogError> {
    let mut files = Vec::new();
    for (version, path) in version_files(dir, name) {
        files.push(VersionedFile {
            version,
            file: IndexedFile::open(
                &path,
                false,
                false,
                RecordCodec::new(cipher.map(Arc::clone), version),
            )?,
        });
        close_cold_version_files(&mut files);
    }
    Ok(files)
}

fn refresh_version_files(
    dir: &Path,
    name: &str,
    cipher: Option<&Arc<LogCipher>>,
    files: &mut Vec<VersionedFile>,
) -> Result<(), LogError> {
    for file in &mut *files {
        file.file.refresh()?;
    }

    for (version, path) in version_files(dir, name) {
        if files.iter().all(|file| file.version != version) {
            files.push(VersionedFile {
                version,
                file: IndexedFile::open(
                    &path,
                    false,
                    false,
                    RecordCodec::new(cipher.map(Arc::clone), version),
                )?,
            });
            close_cold_version_files(files);
        }
    }
    files.sort_by_key(|file| file.version);
    Ok(())
}

fn close_cold_version_files(files: &mut [VersionedFile]) {
    let mut cached_readers = 0;
    for file in files.iter_mut().rev() {
        if file.file.writable {
            continue;
        }
        if file.file.file.is_some() {
            if cached_readers < MAX_CACHED_READ_VERSION_FILES {
                cached_readers += 1;
            } else {
                file.file.close_cached_reader();
            }
        }
    }
}

/// For each version, the first transaction in any later version. Frames at
/// or beyond this cutoff are stale appends from a deposed writer.
fn version_cutoffs(files: &[VersionedFile]) -> Vec<u64> {
    let mut cutoffs = vec![u64::MAX; files.len()];
    let mut cutoff = u64::MAX;
    for (index, file) in files.iter().enumerate().rev() {
        cutoffs[index] = cutoff;
        if let Some(first) = file.file.frames.first() {
            cutoff = cutoff.min(first.t);
        }
    }
    cutoffs
}

fn validated_version_cutoffs(files: &[VersionedFile]) -> Result<Vec<u64>, LogError> {
    let cutoffs = version_cutoffs(files);
    let mut previous_t: Option<u64> = None;
    for (file, cutoff) in files.iter().zip(&cutoffs) {
        let retained = file.file.frames.partition_point(|frame| frame.t < *cutoff);
        if retained == 0 {
            continue;
        }
        file.file.validate_contiguous_prefix(retained)?;
        let first_t = file.file.frames[0].t;
        if previous_t.is_some_and(|previous| previous.checked_add(1) != Some(first_t)) {
            return Err(LogError::Corrupt);
        }
        previous_t = Some(file.file.frames[retained - 1].t);
    }
    Ok(cutoffs)
}

fn merged_next_t(files: &[VersionedFile], cutoffs: &[u64]) -> Result<u64, LogError> {
    for (file, cutoff) in files.iter().zip(cutoffs).rev() {
        let retained = file.file.frames.partition_point(|frame| frame.t < *cutoff);
        if retained > 0 {
            return file.file.frames[retained - 1]
                .t
                .checked_add(1)
                .ok_or(LogError::Corrupt);
        }
    }
    Ok(1)
}

fn range_is_merged_indexed(
    files: &[VersionedFile],
    cutoffs: &[u64],
    end: Option<u64>,
) -> Result<bool, LogError> {
    end.map_or(Ok(false), |end| Ok(end <= merged_next_t(files, cutoffs)?))
}

fn read_merged_range(
    files: &[VersionedFile],
    cutoffs: &[u64],
    start: u64,
    end: Option<u64>,
) -> Result<Vec<TxRecord>, LogError> {
    let mut records = Vec::new();
    for (file, cutoff) in files.iter().zip(cutoffs) {
        let end = Some(end.map_or(*cutoff, |end| end.min(*cutoff)));
        records.extend(file.file.tx_range(start, end)?);
    }
    Ok(records)
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

fn frame_header(payload_len: usize) -> Result<[u8; 8], LogError> {
    let payload_len = u64::try_from(payload_len).map_err(|_| LogError::Corrupt)?;
    if payload_len & CHECKSUMMED_FRAME != 0 {
        return Err(LogError::Corrupt);
    }
    Ok((payload_len | CHECKSUMMED_FRAME).to_be_bytes())
}

fn frame_payload_len(header: [u8; 8]) -> Result<(usize, bool), LogError> {
    let encoded = u64::from_be_bytes(header);
    let checksummed = encoded & CHECKSUMMED_FRAME != 0;
    let payload_len = encoded & !CHECKSUMMED_FRAME;
    Ok((
        usize::try_from(payload_len).map_err(|_| LogError::Corrupt)?,
        checksummed,
    ))
}

fn frame_checksum(header: [u8; 8], payload: &[u8]) -> u32 {
    crc32c::crc32c_append(crc32c::crc32c(&header), payload)
}

#[cfg(unix)]
fn read_exact_at(file: &File, mut bytes: &mut [u8], mut offset: u64) -> io::Result<()> {
    use std::os::unix::fs::FileExt;
    while !bytes.is_empty() {
        match file.read_at(bytes, offset) {
            Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
            Ok(read) => {
                offset = offset
                    .checked_add(u64::try_from(read).expect("read length fits u64"))
                    .ok_or_else(|| io::Error::other("file offset overflow"))?;
                bytes = &mut bytes[read..];
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

#[cfg(windows)]
fn read_exact_at(file: &File, mut bytes: &mut [u8], mut offset: u64) -> io::Result<()> {
    use std::os::windows::fs::FileExt;
    while !bytes.is_empty() {
        match file.seek_read(bytes, offset) {
            Ok(0) => return Err(io::ErrorKind::UnexpectedEof.into()),
            Ok(read) => {
                offset = offset
                    .checked_add(u64::try_from(read).expect("read length fits u64"))
                    .ok_or_else(|| io::Error::other("file offset overflow"))?;
                bytes = &mut bytes[read..];
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

#[cfg(not(any(unix, windows)))]
fn read_exact_at(file: &File, bytes: &mut [u8], offset: u64) -> io::Result<()> {
    use std::io::{Read, Seek, SeekFrom};
    let mut file = file.try_clone()?;
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(bytes)
}

/// Indexes fully written records starting at `offset`, returning the byte end
/// of that durable prefix.
///
/// A record cut short by a crash mid-append (truncated length prefix or
/// payload/checksum) ends the scan; a fully present record with a checksum
/// mismatch or invalid payload is genuine corruption and errors. Legacy
/// length-only records remain readable, while newly written records set the
/// high bit of the length word and carry a trailing CRC32C.
fn scan_frames(
    file: &File,
    offset: u64,
    codec: &RecordCodec,
) -> Result<(Vec<FrameIndex>, u64), LogError> {
    let file_len = file.metadata()?.len();
    let mut frames = Vec::new();
    let mut durable_len = offset;
    loop {
        if file_len.saturating_sub(durable_len) < 8 {
            break;
        }
        let mut len = [0; 8];
        read_exact_at(file, &mut len, durable_len)?;
        let (payload_len, checksummed) = frame_payload_len(len)?;
        let frame_len = 8_u64
            .checked_add(u64::try_from(payload_len).map_err(|_| LogError::Corrupt)?)
            .and_then(|len| {
                len.checked_add(if checksummed {
                    u64::try_from(FRAME_CHECKSUM_LEN).expect("checksum length fits u64")
                } else {
                    0
                })
            })
            .ok_or(LogError::Corrupt)?;
        // Check the bytes remaining before allocating from an untrusted length
        // word. A short final frame is the recoverable crash-tail case.
        if file_len.saturating_sub(durable_len) < frame_len {
            break;
        }
        let mut payload = vec![0; payload_len];
        let payload_offset = durable_len.checked_add(8).ok_or(LogError::Corrupt)?;
        read_exact_at(file, &mut payload, payload_offset)?;
        if checksummed {
            let mut stored_checksum = [0; FRAME_CHECKSUM_LEN];
            let checksum_offset = payload_offset
                .checked_add(u64::try_from(payload_len).map_err(|_| LogError::Corrupt)?)
                .ok_or(LogError::Corrupt)?;
            read_exact_at(file, &mut stored_checksum, checksum_offset)?;
            if u32::from_be_bytes(stored_checksum) != frame_checksum(len, &payload) {
                return Err(LogError::Corrupt);
            }
        }
        let record = codec.decode(&payload)?;
        frames.push(FrameIndex {
            t: record.t,
            offset: durable_len,
            len: frame_len,
        });
        durable_len = durable_len
            .checked_add(frame_len)
            .ok_or(LogError::Corrupt)?;
    }
    Ok((frames, durable_len))
}

/// Appends one checksummed, length-prefixed record payload to `out`.
///
/// The high bit of the length word identifies the checksummed frame format;
/// the remaining 63 bits are the payload length. A big-endian CRC32C over the
/// encoded length word and payload follows the payload. Framing is identical
/// for cleartext and encrypted payloads, which is what keeps scanning, range
/// reads, and recovery truncation keyless.
fn append_framed_payload(out: &mut Vec<u8>, payload: &[u8]) -> Result<(), LogError> {
    let header = frame_header(payload.len())?;
    out.extend_from_slice(&header);
    out.extend_from_slice(payload);
    out.extend_from_slice(&frame_checksum(header, payload).to_be_bytes());
    Ok(())
}

/// Appends one checksummed, length-prefixed cleartext record to `out`.
///
/// # Errors
/// Returns an error if the record payload length is not representable.
pub fn append_framed_record(out: &mut Vec<u8>, record: &TxRecord) -> Result<(), LogError> {
    append_framed_payload(out, &encode_record(record))
}

/// Appends one checksummed, length-prefixed record to `out`, sealed under
/// `cipher` when one is configured.
///
/// `log_version` is the lease version of the file or object the frame belongs
/// to; it is authenticated, so a frame cannot be moved between version files.
///
/// # Errors
/// Returns an error if encryption fails or the payload length is not
/// representable.
pub fn append_framed_record_sealed(
    out: &mut Vec<u8>,
    record: &TxRecord,
    cipher: Option<&Arc<LogCipher>>,
    log_version: u64,
) -> Result<(), LogError> {
    let codec = RecordCodec::new(cipher.map(Arc::clone), log_version);
    append_framed_payload(out, &codec.encode(record)?)
}

/// Decodes all cleartext records from a framed byte slice.
///
/// # Errors
/// Returns an error when any frame is truncated, has an invalid length, or
/// checksum, or contains a corrupt encoded transaction record.
pub fn decode_framed_records(bytes: &[u8]) -> Result<Vec<TxRecord>, LogError> {
    decode_framed_payloads(bytes, &RecordCodec::plaintext())
}

/// Decodes all records from a framed byte slice, opening sealed payloads with
/// `cipher`.
///
/// # Errors
/// Returns an error when any frame is truncated or corrupt, when a payload
/// cannot be authenticated, or when the frames' encryption state disagrees
/// with the configured `cipher`.
pub fn decode_framed_records_sealed(
    bytes: &[u8],
    cipher: Option<&Arc<LogCipher>>,
    log_version: u64,
) -> Result<Vec<TxRecord>, LogError> {
    decode_framed_payloads(
        bytes,
        &RecordCodec::new(cipher.map(Arc::clone), log_version),
    )
}

/// Decodes all records from a framed byte slice.
///
/// Unlike filesystem crash recovery, native stores publish whole values
/// atomically, so any trailing partial frame is treated as corruption.
/// Both legacy length-only frames and checksummed frames are accepted.
fn decode_framed_payloads(
    mut bytes: &[u8],
    codec: &RecordCodec,
) -> Result<Vec<TxRecord>, LogError> {
    let mut records = Vec::new();
    while !bytes.is_empty() {
        if bytes.len() < 8 {
            return Err(LogError::Corrupt);
        }
        let header: [u8; 8] = bytes[..8].try_into().map_err(|_| LogError::Corrupt)?;
        let (payload_len, checksummed) = frame_payload_len(header)?;
        bytes = &bytes[8..];
        let payload = bytes.get(..payload_len).ok_or(LogError::Corrupt)?;
        bytes = &bytes[payload_len..];
        if checksummed {
            let stored_checksum = u32::from_be_bytes(
                bytes
                    .get(..FRAME_CHECKSUM_LEN)
                    .ok_or(LogError::Corrupt)?
                    .try_into()
                    .map_err(|_| LogError::Corrupt)?,
            );
            if stored_checksum != frame_checksum(header, payload) {
                return Err(LogError::Corrupt);
            }
            bytes = &bytes[FRAME_CHECKSUM_LEN..];
        }
        records.push(codec.decode(payload)?);
    }
    Ok(records)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The cleartext `t` a frame is indexed by must be the `t` the sealed
    /// payload carries. The AAD authenticates the header, so no attacker can
    /// separate them; only a writer that sealed one number under another can,
    /// and this is the check that stops such a record reaching an index.
    #[test]
    fn a_header_t_disagreeing_with_its_payload_is_corrupt() {
        let key = SecretKey::new([7; 32]);
        let codec = RecordCodec::new(Some(Arc::new(LogCipher::with_key("db", 1, key.clone()))), 0);
        let record = TxRecord {
            t: 1,
            tx_instant: 5,
            datoms: Vec::new(),
        };

        let honest = codec.encode(&record).expect("seal");
        assert_eq!(codec.decode(&honest).expect("decode"), record);

        // Same key, same lineage, same lease version, and the header
        // authenticates cleanly — only the two transaction numbers disagree.
        let forged =
            encrypt_log_record(&key, 1, b"db", 0, 2, &encode_record(&record)).expect("seal");
        assert_eq!(parse_log_header(&forged).expect("header").t, 2);
        assert!(matches!(codec.decode(&forged), Err(LogError::Corrupt)));
    }

    #[test]
    fn versioned_log_bounds_cached_read_descriptors() {
        let dir = tempfile::tempdir().expect("tempdir");
        let segment_count = MAX_CACHED_READ_VERSION_FILES + 5;
        for version in 1..=u64::try_from(segment_count).expect("segment count fits u64") {
            File::create(version_path(dir.path(), "db", version)).expect("create segment");
        }

        let log = VersionedLog::open_read_only(dir.path(), "db").expect("open log");
        let state = log.state.read().expect("log lock");
        assert_eq!(state.files.len(), segment_count);
        assert!(
            state
                .files
                .iter()
                .filter(|file| file.file.file.is_some())
                .count()
                <= MAX_CACHED_READ_VERSION_FILES
        );
    }
}
