//! Online, storage-native database backup and offline restore.
//!
//! A backup is one versioned binary archive. The transactor fixes the upper
//! transaction basis and hands the backup client an independent storage
//! connection; [`backup`] replays only through that basis. Re-running it
//! against the same file appends one binary checkpoint containing only the
//! records after the archive's previous checkpoint.
//!
//! An encrypted database is archived without ever decrypting its content:
//! stored objects and log frames are copied byte for byte, and the archive
//! carries the database's key manifest — wrapped data keys, never material — so
//! a restore bootstraps itself from the archive plus access to the KEK. See
//! `docs/design/backup-format.md`.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::future::Future;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use corium_crypt::{CryptError, KeyId, Keyring, SecretKey, decrypt_blob, parse_blob_header};
use corium_log::{
    FileLog, FramedRecord, LogCipher, LogError, TransactionLog, TxRecord,
    decode_framed_records_sealed, split_framed_records,
};
use corium_protocol::pb;
use corium_store::{
    BlobId, BlobStore, DbRoot, DiscoveredStore, DiscoveredStoreSpec, FORMAT_VERSION, FsStore,
    KeyManifest, RootStore, StorageConnectionError, StoreError, db_root_name, keys_root_name,
};
use thiserror::Error;

use crate::LogBackend;
use crate::keys::log_lineage;

/// Binary backup container version written by this release.
pub const BACKUP_FORMAT_VERSION: u32 = 2;

/// The container version that predates storage encryption.
///
/// Archives in this format are still read, and an incremental run extends one
/// in its own format rather than rewriting its header — but only for an
/// unencrypted database, which is the only kind format 1 can carry.
const BACKUP_FORMAT_V1: u32 = 1;

const BACKUP_MAGIC: &[u8; 16] = b"CORIUM_BACKUP\0\0\0";
const BLOB_TAG: [u8; 4] = *b"BLOB";
const CHECKPOINT_TAG: [u8; 4] = *b"CKPT";
const CHECKPOINT_END: &[u8; 4] = b"DONE";
const WRITER_VERSION: &str = env!("CARGO_PKG_VERSION");

const CONTENT_ENCRYPTION_NONE: u32 = 0;
const CONTENT_ENCRYPTION_STORAGE: u32 = 1;

/// How an archive's blobs and transaction records are protected.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ContentEncryption {
    /// Cleartext, copied from an unencrypted database.
    #[default]
    None,
    /// Copied verbatim from a database encrypted under a per-database storage
    /// key. The archive holds ciphertext and the wrapped keys that open it;
    /// reading either needs the key-encryption key the manifest names.
    Storage,
}

impl ContentEncryption {
    const fn code(self) -> u32 {
        match self {
            Self::None => CONTENT_ENCRYPTION_NONE,
            Self::Storage => CONTENT_ENCRYPTION_STORAGE,
        }
    }

    const fn parse(code: u32) -> Option<Self> {
        match code {
            CONTENT_ENCRYPTION_NONE => Some(Self::None),
            CONTENT_ENCRYPTION_STORAGE => Some(Self::Storage),
            _ => None,
        }
    }

    /// Whether the archive's content is encrypted at rest.
    #[must_use]
    pub const fn is_encrypted(self) -> bool {
        matches!(self, Self::Storage)
    }
}

impl std::fmt::Display for ContentEncryption {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::None => "none",
            Self::Storage => "storage",
        })
    }
}

/// Counters and snapshot identity returned by a backup.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackupReport {
    /// Binary backup container version.
    pub backup_format_version: u32,
    /// Corium version that wrote the latest checkpoint.
    pub writer_version: String,
    /// How the archive's content is protected.
    pub content_encryption: ContentEncryption,
    /// Transaction basis preserved by the snapshot.
    pub basis_t: u64,
    /// Index basis named by the copied root.
    pub index_basis_t: u64,
    /// Immutable blobs newly copied during this run.
    pub copied_blobs: usize,
    /// Immutable blobs already present in an incremental destination.
    pub reused_blobs: usize,
    /// Transaction records appended during this run.
    pub replayed_transactions: usize,
}

/// Counters and identity returned by a restore.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RestoreReport {
    /// Binary backup container version.
    pub backup_format_version: u32,
    /// Corium version that wrote the latest checkpoint.
    pub writer_version: String,
    /// How the archive's content was protected.
    pub content_encryption: ContentEncryption,
    /// Source database name recorded in the backup.
    pub source_db: String,
    /// Restored database name (which may differ for clone restores).
    pub target_db: String,
    /// Restored transaction basis.
    pub basis_t: u64,
    /// Immutable blobs newly copied into the target store.
    pub copied_blobs: usize,
    /// Immutable blobs already shared by the target store.
    pub reused_blobs: usize,
}

/// Backup or restore failure.
#[derive(Debug, Error)]
pub enum BackupError {
    /// Filesystem operation failed.
    #[error("backup I/O failed: {0}")]
    Io(#[from] io::Error),
    /// Blob/root store operation failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// Transaction log could not be read.
    #[error(transparent)]
    Log(#[from] LogError),
    /// Stored ciphertext could not be opened.
    #[error(transparent)]
    Crypt(#[from] CryptError),
    /// The requested database does not exist.
    #[error("database {0:?} does not exist")]
    MissingDatabase(String),
    /// A required archive section, root, blob, or log record is malformed.
    #[error("invalid backup: {0}")]
    Invalid(String),
    /// Restore refuses to replace an existing database.
    #[error("database {0:?} already exists in the target")]
    TargetExists(String),
    /// The stored format cannot be read by this binary.
    #[error("storage format {found} is newer than supported format {supported}")]
    UnsupportedFormat {
        /// Version found in the backup.
        found: u32,
        /// Newest version understood by this binary.
        supported: u32,
    },
    /// The binary backup container is newer than this binary understands.
    #[error(
        "backup file format {found} (created by Corium {writer}) is newer than supported format {supported}"
    )]
    UnsupportedBackupFormat {
        /// Version found in the archive header.
        found: u32,
        /// Newest version understood by this binary.
        supported: u32,
        /// Corium version recorded by the archive creator.
        writer: String,
    },
    /// The archive declares a content protection this binary cannot handle.
    #[error("backup uses content encryption {0}, which this binary does not understand")]
    UnsupportedContentEncryption(u32),
    /// The advertised backend cannot be opened independently.
    #[error("storage backend cannot be backed up independently: {0}")]
    UnsupportedSource(String),
    /// The database or archive is encrypted and this process resolves no keys.
    #[error("database {db:?} is encrypted under key {kek}; pass --storage-key naming it")]
    MissingKeyring {
        /// Database whose content is encrypted.
        db: String,
        /// Key-encryption key the manifest names.
        kek: KeyId,
    },
    /// The manifest carries no epoch new records could be sealed under.
    #[error("database {0:?} has a key manifest with no active storage-key epoch")]
    NoActiveEpoch(String),
}

/// An independently accessible storage service plus the transaction basis
/// fixed by the running transactor.
#[derive(Clone, Debug)]
pub struct BackupSource {
    store: DiscoveredStoreSpec,
    basis_t: u64,
}

impl BackupSource {
    /// Decodes the transactor's one-shot backup discovery response.
    ///
    /// # Errors
    /// Returns an error for missing connection details, an in-memory backend,
    /// or a backend omitted from this build.
    pub fn from_info(info: pb::GetStorageInfoResponse) -> Result<Self, BackupError> {
        let storage = info
            .storage
            .ok_or_else(|| BackupError::Invalid("transactor returned no storage backend".into()))?;
        crate::backend::register_static_storage_backends();
        let store =
            DiscoveredStoreSpec::from_connection(storage).map_err(storage_connection_error)?;
        Ok(Self {
            store,
            basis_t: info.basis_t,
        })
    }

    /// Fixed upper transaction basis for this run.
    #[must_use]
    pub const fn basis_t(&self) -> u64 {
        self.basis_t
    }
}

fn storage_connection_error(error: StorageConnectionError) -> BackupError {
    match error {
        StorageConnectionError::Missing => {
            BackupError::Invalid("transactor returned no storage backend".into())
        }
        StorageConnectionError::Unsupported(detail) => BackupError::UnsupportedSource(detail),
        StorageConnectionError::InvalidConfig { kind, detail } => {
            BackupError::UnsupportedSource(format!("invalid {kind:?} storage config: {detail}"))
        }
    }
}

/// One database's storage keys, already unwrapped.
///
/// Backup needs them only to walk index blobs — a blob's children are inside
/// its ciphertext — and never to write the archive, which carries the stored
/// bytes. Restore needs them to move the copied log records onto the target
/// database's lineage.
struct ArchiveCrypto {
    manifest: KeyManifest,
    keys: BTreeMap<u32, SecretKey>,
    active_epoch: u32,
}

impl ArchiveCrypto {
    /// Unwraps `manifest`'s data keys, or reports which KEK this process is
    /// missing.
    async fn resolve(
        db: &str,
        manifest: KeyManifest,
        keyring: Option<&Arc<dyn Keyring>>,
    ) -> Result<Self, BackupError> {
        let Some(keyring) = keyring else {
            return Err(BackupError::MissingKeyring {
                db: db.to_owned(),
                kek: manifest.kek.clone(),
            });
        };
        let active_epoch = manifest
            .active_storage_epoch()
            .ok_or_else(|| BackupError::NoActiveEpoch(db.to_owned()))?;
        let keys = manifest.unwrap_storage_keys(keyring.as_ref()).await?;
        Ok(Self {
            manifest,
            keys,
            active_epoch,
        })
    }

    fn key(&self, epoch: u32) -> Result<&SecretKey, BackupError> {
        self.keys
            .get(&epoch)
            .ok_or(BackupError::Log(LogError::MissingKeyEpoch(epoch)))
    }

    /// The cipher log records of `db` are sealed with.
    fn cipher(&self, db: &str) -> Result<Arc<LogCipher>, BackupError> {
        Ok(Arc::new(LogCipher::new(
            log_lineage(db),
            self.active_epoch,
            self.keys.clone(),
        )?))
    }
}

/// The plaintext of a stored index blob, so its child references can be read.
///
/// The archive keeps the stored bytes; this is only ever a working copy, and
/// for an unencrypted database it is not even a copy.
fn blob_plaintext<'a>(
    crypto: Option<&ArchiveCrypto>,
    stored: &'a [u8],
) -> Result<Cow<'a, [u8]>, BackupError> {
    let Some(crypto) = crypto else {
        return Ok(Cow::Borrowed(stored));
    };
    let header = parse_blob_header(stored)?;
    Ok(Cow::Owned(decrypt_blob(crypto.key(header.epoch)?, stored)?))
}

/// A database's key manifest, or `None` when it is unencrypted — which is
/// fixed at creation and can never change.
async fn stored_manifest(
    store: &dyn RootStore,
    db: &str,
) -> Result<Option<KeyManifest>, BackupError> {
    Ok(corium_store::load_key_manifest(store, db)
        .await?
        .filter(|manifest| !manifest.storage_keys.is_empty()))
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ArchiveHeader {
    format_version: u32,
    creator_version: String,
    source_db: String,
    storage_format_version: u32,
    index_basis_t: u64,
    root: Vec<u8>,
    /// Format 2 and later. A format 1 archive always reads as `None`.
    content_encryption: ContentEncryption,
    /// The key manifest as of the archive's creation, empty when unencrypted.
    /// The authoritative one is the newest checkpoint's, which a storage-key
    /// rotation between incremental runs moves forward.
    key_manifest: Vec<u8>,
}

#[derive(Debug)]
struct Archive {
    header: ArchiveHeader,
    writer_version: String,
    basis_t: u64,
    metadata: Vec<u8>,
    /// Key manifest carried by the newest checkpoint.
    key_manifest: Vec<u8>,
    /// Copied transaction frames, in `t` order, each tagged with the log
    /// version it was sealed under. Empty unless the archive was read with
    /// records included.
    frames: Vec<FramedRecord>,
    blob_offsets: Vec<(u64, u64)>,
    durable_len: u64,
}

fn valid_db_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

fn meta_root_name(db: &str) -> String {
    format!("meta:{db}")
}

fn log_path(data_dir: &Path, db: &str) -> PathBuf {
    data_dir.join("logs").join(format!("{db}.log"))
}

fn invalid(detail: impl Into<String>) -> BackupError {
    BackupError::Invalid(detail.into())
}

fn write_u32(writer: &mut impl Write, value: u32) -> Result<(), BackupError> {
    writer.write_all(&value.to_be_bytes())?;
    Ok(())
}

fn write_u64(writer: &mut impl Write, value: u64) -> Result<(), BackupError> {
    writer.write_all(&value.to_be_bytes())?;
    Ok(())
}

fn read_u32(reader: &mut impl Read) -> Result<u32, BackupError> {
    let mut bytes = [0; 4];
    reader.read_exact(&mut bytes)?;
    Ok(u32::from_be_bytes(bytes))
}

fn read_u64(reader: &mut impl Read) -> Result<u64, BackupError> {
    let mut bytes = [0; 8];
    reader.read_exact(&mut bytes)?;
    Ok(u64::from_be_bytes(bytes))
}

fn write_bytes(writer: &mut impl Write, bytes: &[u8]) -> Result<(), BackupError> {
    write_u64(
        writer,
        u64::try_from(bytes.len()).map_err(|_| invalid("backup field is too large"))?,
    )?;
    writer.write_all(bytes)?;
    Ok(())
}

fn read_bytes(reader: &mut impl Read, field: &str) -> Result<Vec<u8>, BackupError> {
    let len = usize::try_from(read_u64(reader)?)
        .map_err(|_| invalid(format!("{field} is too large for this platform")))?;
    let mut bytes = vec![0; len];
    reader.read_exact(&mut bytes)?;
    Ok(bytes)
}

fn read_string(reader: &mut impl Read, field: &str) -> Result<String, BackupError> {
    String::from_utf8(read_bytes(reader, field)?)
        .map_err(|_| invalid(format!("{field} is not UTF-8")))
}

fn write_frame(writer: &mut impl Write, tag: [u8; 4], payload: &[u8]) -> Result<(), BackupError> {
    writer.write_all(&tag)?;
    write_u64(
        writer,
        u64::try_from(payload.len()).map_err(|_| invalid("backup frame is too large"))?,
    )?;
    writer.write_all(payload)?;
    Ok(())
}

fn write_header(writer: &mut impl Write, header: &ArchiveHeader) -> Result<(), BackupError> {
    writer.write_all(BACKUP_MAGIC)?;
    write_u32(writer, header.format_version)?;
    write_bytes(writer, header.creator_version.as_bytes())?;
    write_bytes(writer, header.source_db.as_bytes())?;
    write_u32(writer, header.storage_format_version)?;
    write_u64(writer, header.index_basis_t)?;
    write_bytes(writer, &header.root)?;
    if header.format_version >= BACKUP_FORMAT_VERSION {
        write_u32(writer, header.content_encryption.code())?;
        write_bytes(writer, &header.key_manifest)?;
    }
    Ok(())
}

/// The `(log version, record count)` runs covering `frames` in order.
///
/// A sealed payload authenticates the log version of the file it was written
/// to, so the version has to survive the copy: restore opens each run against
/// the version it names.
fn encode_log_versions(frames: &[FramedRecord]) -> Result<Vec<u8>, BackupError> {
    let mut runs: Vec<(u64, u64)> = Vec::new();
    for frame in frames {
        match runs.last_mut() {
            Some((version, count)) if *version == frame.log_version => *count += 1,
            _ => runs.push((frame.log_version, 1)),
        }
    }
    let mut bytes = Vec::with_capacity(runs.len() * 16);
    for (version, count) in runs {
        write_u64(&mut bytes, version)?;
        write_u64(&mut bytes, count)?;
    }
    Ok(bytes)
}

/// Pairs an encoded run table with the frames it covers.
fn decode_log_versions(bytes: &[u8], record_count: u64) -> Result<Vec<(u64, u64)>, BackupError> {
    if !bytes.len().is_multiple_of(16) {
        return Err(invalid("checkpoint log-version table is malformed"));
    }
    let mut runs = Vec::with_capacity(bytes.len() / 16);
    let mut total = 0_u64;
    for entry in bytes.chunks_exact(16) {
        let version = u64::from_be_bytes(entry[..8].try_into().expect("eight bytes"));
        let count = u64::from_be_bytes(entry[8..].try_into().expect("eight bytes"));
        if count == 0 {
            return Err(invalid("checkpoint log-version run is empty"));
        }
        total = total
            .checked_add(count)
            .ok_or_else(|| invalid("checkpoint log-version runs overflowed"))?;
        runs.push((version, count));
    }
    // An empty table means "every record under log version 0", which is what a
    // format 1 archive and a cleartext single-file log both carry.
    if runs.is_empty() && record_count > 0 {
        runs.push((0, record_count));
    } else if total != record_count {
        return Err(invalid(
            "checkpoint log-version runs do not cover its records",
        ));
    }
    Ok(runs)
}

fn encode_checkpoint(
    format_version: u32,
    basis_t: u64,
    previous_basis_t: u64,
    metadata: &[u8],
    frames: &[FramedRecord],
    key_manifest: &[u8],
) -> Result<Vec<u8>, BackupError> {
    // Extending a format 1 archive drops the log-version table, which is
    // harmless — a cleartext frame authenticates nothing and opens under any
    // version — but it cannot drop key material's whereabouts. The caller
    // refuses the pairing before reaching here; this states the invariant.
    if format_version < BACKUP_FORMAT_VERSION && !key_manifest.is_empty() {
        return Err(invalid(
            "backup format 1 cannot carry a key manifest; create a new archive",
        ));
    }
    let mut record_bytes = Vec::new();
    for frame in frames {
        record_bytes.extend_from_slice(&frame.frame);
    }
    let mut payload = Vec::new();
    write_bytes(&mut payload, WRITER_VERSION.as_bytes())?;
    write_u64(&mut payload, basis_t)?;
    write_u64(
        &mut payload,
        if frames.is_empty() {
            0
        } else {
            previous_basis_t.saturating_add(1)
        },
    )?;
    write_u64(
        &mut payload,
        u64::try_from(frames.len()).map_err(|_| invalid("too many transaction records"))?,
    )?;
    write_bytes(&mut payload, metadata)?;
    write_bytes(&mut payload, &record_bytes)?;
    if format_version >= BACKUP_FORMAT_VERSION {
        write_bytes(&mut payload, &encode_log_versions(frames)?)?;
        write_bytes(&mut payload, key_manifest)?;
    }
    payload.extend_from_slice(CHECKPOINT_END);
    Ok(payload)
}

fn read_frame_bytes(file: &mut File, frame_end: u64, field: &str) -> Result<Vec<u8>, BackupError> {
    let len = read_u64(file)?;
    let start = file.stream_position()?;
    let end = start
        .checked_add(len)
        .ok_or_else(|| invalid(format!("{field} length overflowed")))?;
    if end > frame_end {
        return Err(invalid(format!("{field} extends past its checkpoint")));
    }
    let len = usize::try_from(len)
        .map_err(|_| invalid(format!("{field} is too large for this platform")))?;
    let mut bytes = vec![0; len];
    file.read_exact(&mut bytes)?;
    Ok(bytes)
}

/// One decoded `CKPT` frame.
struct Checkpoint {
    writer_version: String,
    basis_t: u64,
    metadata: Vec<u8>,
    key_manifest: Vec<u8>,
    frames: Vec<FramedRecord>,
}

fn decode_checkpoint(
    file: &mut File,
    format_version: u32,
    frame_end: u64,
    previous_basis_t: u64,
    include_records: bool,
) -> Result<Checkpoint, BackupError> {
    let writer_version = String::from_utf8(read_frame_bytes(
        file,
        frame_end,
        "checkpoint writer version",
    )?)
    .map_err(|_| invalid("checkpoint writer version is not UTF-8"))?;
    let basis_t = read_u64(file)?;
    let start_t = read_u64(file)?;
    let record_count = read_u64(file)?;
    let metadata = read_frame_bytes(file, frame_end, "checkpoint metadata")?;
    let records_len = read_u64(file)?;
    let records_start = file.stream_position()?;
    let records_end = records_start
        .checked_add(records_len)
        .ok_or_else(|| invalid("checkpoint transaction-record length overflowed"))?;
    if records_end > frame_end {
        return Err(invalid("checkpoint transaction records overrun its frame"));
    }
    let record_bytes = if include_records {
        let len = usize::try_from(records_len)
            .map_err(|_| invalid("transaction records are too large for this platform"))?;
        let mut bytes = vec![0; len];
        file.read_exact(&mut bytes)?;
        bytes
    } else {
        file.seek(SeekFrom::Start(records_end))?;
        Vec::new()
    };
    let (log_versions, key_manifest) = if format_version >= BACKUP_FORMAT_VERSION {
        (
            read_frame_bytes(file, frame_end, "checkpoint log versions")?,
            read_frame_bytes(file, frame_end, "checkpoint key manifest")?,
        )
    } else {
        (Vec::new(), Vec::new())
    };
    let mut end = [0; 4];
    file.read_exact(&mut end)?;
    if &end != CHECKPOINT_END || file.stream_position()? != frame_end {
        return Err(invalid("checkpoint has invalid trailing bytes"));
    }
    let expected_count = basis_t
        .checked_sub(previous_basis_t)
        .ok_or_else(|| invalid("checkpoint basis regressed"))?;
    let expected_start = if expected_count == 0 {
        0
    } else {
        previous_basis_t
            .checked_add(1)
            .ok_or_else(|| invalid("checkpoint basis overflowed"))?
    };
    if record_count != expected_count || start_t != expected_start {
        return Err(invalid("checkpoint transaction range is not contiguous"));
    }
    let frames = if include_records {
        let copied = restore_frames(&record_bytes, &log_versions, record_count)?;
        if record_count > 0 {
            validate_frames(&copied, start_t, basis_t)?;
        }
        copied
    } else {
        Vec::new()
    };
    Ok(Checkpoint {
        writer_version,
        basis_t,
        metadata,
        key_manifest,
        frames,
    })
}

/// Re-associates copied frames with the log versions they were sealed under.
fn restore_frames(
    record_bytes: &[u8],
    log_versions: &[u8],
    record_count: u64,
) -> Result<Vec<FramedRecord>, BackupError> {
    let split = split_framed_records(record_bytes)?;
    if split.len() as u64 != record_count {
        return Err(invalid(
            "checkpoint record count does not match its payload",
        ));
    }
    let runs = decode_log_versions(log_versions, record_count)?;
    let mut copied = Vec::with_capacity(split.len());
    let mut remaining = runs.into_iter();
    let mut current = remaining.next().unwrap_or((0, u64::MAX));
    for (t, frame) in split {
        while current.1 == 0 {
            current = remaining
                .next()
                .ok_or_else(|| invalid("checkpoint log-version runs are short"))?;
        }
        current.1 -= 1;
        copied.push(FramedRecord {
            t,
            log_version: current.0,
            frame,
        });
    }
    Ok(copied)
}

fn read_header(file: &mut File) -> Result<ArchiveHeader, BackupError> {
    let mut magic = [0; 16];
    file.read_exact(&mut magic)?;
    if &magic != BACKUP_MAGIC {
        return Err(invalid("unknown backup file magic"));
    }
    let format_version = read_u32(file)?;
    let creator_version = read_string(file, "archive creator version")?;
    if format_version > BACKUP_FORMAT_VERSION {
        return Err(BackupError::UnsupportedBackupFormat {
            found: format_version,
            supported: BACKUP_FORMAT_VERSION,
            writer: creator_version,
        });
    }
    if format_version < BACKUP_FORMAT_V1 {
        return Err(invalid(format!(
            "unsupported backup file format {format_version}"
        )));
    }
    let source_db = read_string(file, "source database name")?;
    let storage_format_version = read_u32(file)?;
    if storage_format_version > FORMAT_VERSION {
        return Err(BackupError::UnsupportedFormat {
            found: storage_format_version,
            supported: FORMAT_VERSION,
        });
    }
    let index_basis_t = read_u64(file)?;
    let root = read_bytes(file, "database root")?;
    let (content_encryption, key_manifest) = if format_version >= BACKUP_FORMAT_VERSION {
        let code = read_u32(file)?;
        (
            ContentEncryption::parse(code)
                .ok_or(BackupError::UnsupportedContentEncryption(code))?,
            read_bytes(file, "archive key manifest")?,
        )
    } else {
        (ContentEncryption::None, Vec::new())
    };
    Ok(ArchiveHeader {
        format_version,
        creator_version,
        source_db,
        storage_format_version,
        index_basis_t,
        root,
        content_encryption,
        key_manifest,
    })
}

fn read_archive(path: &Path, include_records: bool) -> Result<Archive, BackupError> {
    if !path.is_file() {
        return Err(invalid(format!(
            "{} is not a binary backup file",
            path.display()
        )));
    }
    let mut file = File::open(path)?;
    let file_len = file.metadata()?.len();
    let header = read_header(&mut file)?;
    let mut writer_version = header.creator_version.clone();
    let mut basis_t = 0;
    let mut metadata = Vec::new();
    let mut key_manifest = header.key_manifest.clone();
    let mut frames = Vec::new();
    let mut blob_offsets = Vec::new();
    let mut durable_len = file.stream_position()?;
    let mut saw_checkpoint = false;
    loop {
        let frame_start = file.stream_position()?;
        let mut tag = [0; 4];
        match file.read_exact(&mut tag) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(error) => return Err(error.into()),
        }
        let frame_len = match read_u64(&mut file) {
            Ok(len) => len,
            Err(BackupError::Io(error)) if error.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(error) => return Err(error),
        };
        let payload_offset = file.stream_position()?;
        let frame_end = payload_offset
            .checked_add(frame_len)
            .ok_or_else(|| invalid("backup frame length overflowed"))?;
        if frame_end > file_len {
            break;
        }
        if tag == BLOB_TAG {
            if saw_checkpoint {
                return Err(invalid("blob frame appears after a checkpoint"));
            }
            blob_offsets.push((payload_offset, frame_len));
            file.seek(SeekFrom::Start(frame_end))?;
        } else if tag == CHECKPOINT_TAG {
            saw_checkpoint = true;
            let checkpoint = decode_checkpoint(
                &mut file,
                header.format_version,
                frame_end,
                basis_t,
                include_records,
            )?;
            writer_version = checkpoint.writer_version;
            basis_t = checkpoint.basis_t;
            metadata = checkpoint.metadata;
            if !checkpoint.key_manifest.is_empty() {
                key_manifest = checkpoint.key_manifest;
            }
            frames.extend(checkpoint.frames);
        } else {
            return Err(invalid(format!(
                "unknown backup frame tag at offset {frame_start}"
            )));
        }
        durable_len = frame_end;
    }
    if !saw_checkpoint {
        return Err(invalid("backup contains no complete checkpoint"));
    }
    Ok(Archive {
        header,
        writer_version,
        basis_t,
        metadata,
        key_manifest,
        frames,
        blob_offsets,
        durable_len,
    })
}

/// Writes one blob and everything it references into the binary archive,
/// children first.
///
/// The bytes written are the bytes the store holds: an encrypted database's
/// objects travel as ciphertext, and `crypto` is used only to read the child
/// references inside them.
fn write_blob_tree<'a>(
    source: &'a DiscoveredStore,
    crypto: Option<&'a ArchiveCrypto>,
    id: &'a BlobId,
    seen: &'a mut std::collections::HashSet<BlobId>,
    writer: &'a mut File,
    copied: &'a mut usize,
) -> Pin<Box<dyn Future<Output = Result<(), BackupError>> + Send + 'a>> {
    Box::pin(async move {
        if !seen.insert(id.clone()) {
            return Ok(());
        }
        let bytes = source
            .get(id)
            .await?
            .ok_or_else(|| BackupError::Invalid(format!("root references missing blob {id}")))?;
        for child in corium_store::index_blob_children(&blob_plaintext(crypto, &bytes)?)? {
            write_blob_tree(source, crypto, &child, seen, writer, copied).await?;
        }
        write_frame(writer, BLOB_TAG, &bytes)?;
        *copied += 1;
        Ok(())
    })
}

fn validate_blob_tree<'a>(
    store: &'a dyn BlobStore,
    crypto: Option<&'a ArchiveCrypto>,
    id: &'a BlobId,
    seen: &'a mut std::collections::HashSet<BlobId>,
) -> Pin<Box<dyn Future<Output = Result<(), BackupError>> + Send + 'a>> {
    Box::pin(async move {
        if !seen.insert(id.clone()) {
            return Ok(());
        }
        let bytes = store
            .get(id)
            .await?
            .ok_or_else(|| invalid(format!("archive is missing reachable blob {id}")))?;
        for child in corium_store::index_blob_children(&blob_plaintext(crypto, &bytes)?)? {
            validate_blob_tree(store, crypto, &child, seen).await?;
        }
        Ok(())
    })
}

/// Writes the restored transaction log, sealing its records under the target
/// database's own lineage when the archive is encrypted.
fn write_log_atomically(
    records: &[TxRecord],
    target: &Path,
    cipher: Option<Arc<LogCipher>>,
) -> Result<(), BackupError> {
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)?;
    }
    let temporary = target.with_extension("tmp");
    match fs::remove_file(&temporary) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let log = match cipher {
        Some(cipher) => FileLog::open_sealed(&temporary, cipher)?,
        None => FileLog::open(&temporary)?,
    };
    for record in records {
        log.append(record)?;
    }
    drop(log);
    fs::rename(temporary, target)?;
    Ok(())
}

fn bare_root() -> DbRoot {
    DbRoot {
        format_version: FORMAT_VERSION,
        lease_version: 0,
        owner: String::new(),
        lease_expires_unix_ms: 0,
        owner_endpoint: String::new(),
        index_basis_t: 0,
        roots: None,
        next_entity_id: 0,
        last_tx_instant: i64::MIN,
        key_manifest_version: 0,
    }
}

fn sanitize_root(mut root: DbRoot) -> DbRoot {
    root.lease_version = 0;
    root.owner.clear();
    root.lease_expires_unix_ms = 0;
    root.owner_endpoint.clear();
    root
}

fn validate_frames(
    frames: &[FramedRecord],
    start: u64,
    end_inclusive: u64,
) -> Result<(), BackupError> {
    if start > end_inclusive {
        return Ok(());
    }
    let expected = end_inclusive - start + 1;
    if frames.len() as u64 != expected
        || frames.first().map(|frame| frame.t) != Some(start)
        || frames.last().map(|frame| frame.t) != Some(end_inclusive)
        || frames.windows(2).any(|pair| pair[1].t != pair[0].t + 1)
    {
        return Err(BackupError::Invalid(format!(
            "source log does not contain a contiguous range {start}..={end_inclusive}"
        )));
    }
    Ok(())
}

fn append_archive_checkpoint(
    destination: &Path,
    archive: &Archive,
    checkpoint: &[u8],
    changed: bool,
) -> Result<String, BackupError> {
    if !changed {
        return Ok(archive.writer_version.clone());
    }
    let mut file = OpenOptions::new().write(true).open(destination)?;
    file.set_len(archive.durable_len)?;
    file.seek(SeekFrom::Start(archive.durable_len))?;
    write_frame(&mut file, CHECKPOINT_TAG, checkpoint)?;
    file.sync_all()?;
    Ok(WRITER_VERSION.to_owned())
}

async fn create_archive(
    destination: &Path,
    db: &str,
    root: &DbRoot,
    source_store: &DiscoveredStore,
    crypto: Option<&ArchiveCrypto>,
    checkpoint: &[u8],
) -> Result<usize, BackupError> {
    if let Some(parent) = destination.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)?;
    }
    let mut temporary = destination.as_os_str().to_os_string();
    temporary.push(".tmp");
    let temporary = PathBuf::from(temporary);
    match fs::remove_file(&temporary) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let mut file = File::create(&temporary)?;
    write_header(
        &mut file,
        &ArchiveHeader {
            format_version: BACKUP_FORMAT_VERSION,
            creator_version: WRITER_VERSION.to_owned(),
            source_db: db.to_owned(),
            storage_format_version: root.format_version,
            index_basis_t: root.index_basis_t,
            root: root.encode(),
            content_encryption: content_encryption(crypto),
            key_manifest: encoded_manifest(crypto),
        },
    )?;
    let mut seen = std::collections::HashSet::new();
    let mut copied = 0;
    for id in root.roots.iter().flatten() {
        write_blob_tree(source_store, crypto, id, &mut seen, &mut file, &mut copied).await?;
    }
    write_frame(&mut file, CHECKPOINT_TAG, checkpoint)?;
    file.sync_all()?;
    fs::rename(temporary, destination)?;
    Ok(copied)
}

fn content_encryption(crypto: Option<&ArchiveCrypto>) -> ContentEncryption {
    crypto.map_or(ContentEncryption::None, |_| ContentEncryption::Storage)
}

fn encoded_manifest(crypto: Option<&ArchiveCrypto>) -> Vec<u8> {
    crypto
        .map(|crypto| crypto.manifest.encode())
        .unwrap_or_default()
}

/// Rejects a destination that cannot legitimately be extended by this run.
fn check_incremental_destination(
    archive: &Archive,
    db: &str,
    source_basis_t: u64,
    crypto: Option<&ArchiveCrypto>,
) -> Result<(), BackupError> {
    if archive.header.source_db != db {
        return Err(invalid(format!(
            "backup belongs to database {:?}, not {db:?}",
            archive.header.source_db
        )));
    }
    if archive.basis_t > source_basis_t {
        return Err(invalid(format!(
            "backup basis {} is ahead of source checkpoint {source_basis_t}",
            archive.basis_t
        )));
    }
    if archive.header.content_encryption != content_encryption(crypto) {
        return Err(invalid(format!(
            "backup holds {} content but database {db:?} is {}; encryption is fixed \
             at creation, so this archive belongs to a different database",
            archive.header.content_encryption,
            content_encryption(crypto)
        )));
    }
    Ok(())
}

/// The root the archive replays from.
///
/// An existing archive's snapshot is its stable replay base. On a first backup,
/// opportunistically retain the source snapshot only when it is not ahead of
/// the fixed log checkpoint; otherwise a bare root forces a correct full-log
/// replay on restore.
fn replay_root(
    previous: Option<&Archive>,
    source_root: DbRoot,
    basis_t: u64,
) -> Result<DbRoot, BackupError> {
    let Some(archive) = previous else {
        return Ok(if source_root.index_basis_t <= basis_t {
            sanitize_root(source_root)
        } else {
            bare_root()
        });
    };
    let root = DbRoot::decode(&archive.header.root)
        .ok_or_else(|| invalid("archive database root cannot be decoded"))?;
    if root.index_basis_t != archive.header.index_basis_t
        || root.format_version != archive.header.storage_format_version
    {
        return Err(invalid("archive header does not match its database root"));
    }
    Ok(sanitize_root(root))
}

/// Copies one live database into `destination` through a fixed transaction
/// checkpoint.
///
/// `source` comes from [`BackupSource::from_info`], after the transactor fixes
/// its current `t`. The source log may continue growing; this function reads
/// only `(old_backup_t, source_t]`. Reusing a destination retains its index
/// snapshot and appends only missing transaction records.
///
/// `keyring` is required when the database is encrypted, and only to follow
/// index blobs' child references: nothing this function writes is ever
/// decrypted, so the archive of an encrypted database is itself encrypted.
///
/// # Errors
/// Returns an error if the database is missing, the checkpoint cannot be read
/// contiguously, the destination belongs to another database, storage cannot be
/// opened, or the database is encrypted and `keyring` resolves no key for it.
pub async fn backup(
    source: &BackupSource,
    db: &str,
    destination: impl AsRef<Path>,
    keyring: Option<&Arc<dyn Keyring>>,
) -> Result<BackupReport, BackupError> {
    if !valid_db_name(db) {
        return Err(BackupError::MissingDatabase(db.to_owned()));
    }
    let destination = destination.as_ref();
    let source_store = Arc::new(source.store.clone().open_existing().await?);
    let crypto = match stored_manifest(source_store.as_ref(), db).await? {
        Some(manifest) => Some(ArchiveCrypto::resolve(db, manifest, keyring).await?),
        None => None,
    };
    let source_logs = LogBackend::for_discovered(&source.store, Arc::clone(&source_store))
        .map_err(storage_connection_error)?;
    let db_name = db_root_name(db);
    let meta_name = meta_root_name(db);
    let source_db_bytes = source_store
        .get_root(&db_name)
        .await?
        .ok_or_else(|| BackupError::MissingDatabase(db.to_owned()))?;
    let meta = source_store
        .get_root(&meta_name)
        .await?
        .ok_or_else(|| BackupError::MissingDatabase(db.to_owned()))?;
    let source_root = DbRoot::decode(&source_db_bytes)
        .ok_or_else(|| BackupError::Invalid("database root cannot be decoded".into()))?;
    if source_root.format_version > FORMAT_VERSION {
        return Err(BackupError::UnsupportedFormat {
            found: source_root.format_version,
            supported: FORMAT_VERSION,
        });
    }
    let previous = destination
        .exists()
        .then(|| read_archive(destination, false))
        .transpose()?;
    if let Some(archive) = &previous {
        check_incremental_destination(archive, db, source.basis_t, crypto.as_ref())?;
    }
    // An archive keeps the container version it was created with: an
    // incremental run appends a checkpoint, it does not rewrite the header.
    let format_version = previous.as_ref().map_or(BACKUP_FORMAT_VERSION, |archive| {
        archive.header.format_version
    });
    let existing_basis = previous.as_ref().map_or(0, |archive| archive.basis_t);
    let start = existing_basis.saturating_add(1);
    let frames = if start <= source.basis_t {
        let end = source
            .basis_t
            .checked_add(1)
            .ok_or_else(|| BackupError::Invalid("source basis is too large".into()))?;
        // Frames are copied, not decoded: an encrypted database's records go
        // into the archive exactly as its log holds them.
        let log = source_logs.open_read_only_opaque(db).await?;
        log.tx_range_framed_async(start, Some(end)).await?
    } else {
        Vec::new()
    };
    validate_frames(&frames, start, source.basis_t)?;

    let root = replay_root(previous.as_ref(), source_root, source.basis_t)?;
    let checkpoint = encode_checkpoint(
        format_version,
        source.basis_t,
        existing_basis,
        &meta,
        &frames,
        &encoded_manifest(crypto.as_ref()),
    )?;
    let (copied_blobs, writer_version) = if let Some(archive) = &previous {
        (
            0,
            append_archive_checkpoint(destination, archive, &checkpoint, !frames.is_empty())?,
        )
    } else {
        (
            create_archive(
                destination,
                db,
                &root,
                source_store.as_ref(),
                crypto.as_ref(),
                &checkpoint,
            )
            .await?,
            WRITER_VERSION.to_owned(),
        )
    };
    Ok(BackupReport {
        backup_format_version: format_version,
        writer_version,
        content_encryption: content_encryption(crypto.as_ref()),
        basis_t: source.basis_t,
        index_basis_t: root.index_basis_t,
        copied_blobs,
        reused_blobs: 0,
        replayed_transactions: frames.len(),
    })
}

/// Replaces the archive's stored objects in the target store, byte for byte.
///
/// The bytes go in as they came out — ciphertext included — so the blob id is
/// unchanged and an object the target already shares is not recopied.
async fn copy_blobs(
    source: &Path,
    archive: &Archive,
    target_store: &FsStore,
) -> Result<(usize, usize), BackupError> {
    let mut backup_file = File::open(source)?;
    let mut copied = 0;
    let mut reused = 0;
    for (offset, len) in &archive.blob_offsets {
        backup_file.seek(SeekFrom::Start(*offset))?;
        let len =
            usize::try_from(*len).map_err(|_| invalid("blob is too large for this platform"))?;
        let mut bytes = vec![0; len];
        backup_file.read_exact(&mut bytes)?;
        let id = corium_store::digest(&bytes);
        if target_store.contains(&id).await? {
            reused += 1;
        } else {
            let stored = target_store.put(&bytes).await?;
            if stored != id {
                return Err(invalid(format!("blob digest changed while restoring {id}")));
            }
            copied += 1;
        }
    }
    Ok((copied, reused))
}

/// The manifest the restored database gets: the archive's, with every epoch's
/// nonce budget reopened at `t` 0.
///
/// `opened_at_t` measures how many log records an epoch has sealed, from the
/// span of `t` it covers — and the restored log is sealed entirely under the
/// active epoch, whatever epochs its source used. Carrying the source's
/// openings forward would credit the active epoch with only the tail of the
/// log while it actually holds all of it, and a nonce budget must never
/// under-count. Zeroing every opening gives the active epoch the whole span and
/// the older ones none, which is exactly what the restored log contains; the
/// older epochs stay in the manifest because the copied blobs still carry them.
fn restored_manifest(crypto: &ArchiveCrypto) -> KeyManifest {
    let mut manifest = crypto.manifest.clone();
    for key in &mut manifest.storage_keys {
        key.opened_at_t = 0;
    }
    manifest
}

/// Opens the archive's copied frames and re-seals them onto `target_db`.
///
/// A sealed record authenticates the database lineage it belongs to, so a
/// restore — under the source's own name or a new one — writes the records
/// again rather than copying them. The data keys are the archive's own, taken
/// from the manifest it carries: only the lineage and the log version change,
/// which is why the blobs beside them need no rewriting.
fn restored_records(
    archive: &Archive,
    crypto: Option<&ArchiveCrypto>,
) -> Result<Vec<TxRecord>, BackupError> {
    let cipher = crypto
        .map(|crypto| crypto.cipher(&archive.header.source_db))
        .transpose()?;
    let mut records = Vec::with_capacity(archive.frames.len());
    // Frames arrive in `t` order grouped by log version; each run opens
    // against the version authenticated into it.
    for run in archive
        .frames
        .chunk_by(|left, right| left.log_version == right.log_version)
    {
        let version = run[0].log_version;
        let mut record_bytes = Vec::new();
        for frame in run {
            record_bytes.extend_from_slice(&frame.frame);
        }
        records.extend(decode_framed_records_sealed(
            &record_bytes,
            cipher.as_ref(),
            version,
        )?);
    }
    if records.len() != archive.frames.len()
        || records
            .iter()
            .zip(&archive.frames)
            .any(|(record, frame)| record.t != frame.t)
    {
        return Err(invalid("archive records do not match their frames"));
    }
    Ok(records)
}

/// Restores a backup under `target_db`, allowing restore-as-clone by choosing
/// a name different from the archive's source database.
///
/// Existing target databases are never overwritten. Immutable blobs already
/// present in the target store are shared rather than recopied.
///
/// Restoring an encrypted archive needs `keyring` to resolve the key-encryption
/// key its manifest names: the blobs are replaced verbatim, but the transaction
/// records are re-sealed onto the restored database's own lineage. The restored
/// database keeps the archive's data keys, so it shares key material with its
/// source until `corium keys rotate` or `corium keys rewrap` says otherwise.
///
/// # Errors
/// Returns an error for malformed/incomplete backups, unsupported formats,
/// existing targets, a missing key, or I/O failures.
pub async fn restore(
    source: impl AsRef<Path>,
    target_data_dir: impl AsRef<Path>,
    target_db: &str,
    keyring: Option<&Arc<dyn Keyring>>,
) -> Result<RestoreReport, BackupError> {
    if !valid_db_name(target_db) {
        return Err(BackupError::Invalid(format!(
            "invalid target database name {target_db:?}"
        )));
    }
    let source = source.as_ref();
    let target_data_dir = target_data_dir.as_ref();
    let archive = read_archive(source, true)?;
    let root = DbRoot::decode(&archive.header.root)
        .ok_or_else(|| invalid("archive database root cannot be decoded"))?;
    if root.index_basis_t != archive.header.index_basis_t
        || root.format_version != archive.header.storage_format_version
    {
        return Err(invalid("archive header does not match its database root"));
    }
    let crypto = if archive.header.content_encryption.is_encrypted() {
        let manifest = KeyManifest::decode(&archive.key_manifest)?;
        if manifest.storage_keys.is_empty() {
            return Err(invalid("encrypted archive carries no storage keys"));
        }
        Some(ArchiveCrypto::resolve(&archive.header.source_db, manifest, keyring).await?)
    } else {
        if !archive.key_manifest.is_empty() {
            return Err(invalid("unencrypted archive carries a key manifest"));
        }
        None
    };

    let target_store = FsStore::open(target_data_dir.join("store"))?;
    let target_db_name = db_root_name(target_db);
    let target_meta_name = meta_root_name(target_db);
    let target_keys_name = keys_root_name(target_db);
    if target_store.get_root(&target_db_name).await?.is_some()
        || target_store.get_root(&target_meta_name).await?.is_some()
        || target_store.get_root(&target_keys_name).await?.is_some()
        || log_path(target_data_dir, target_db).exists()
    {
        return Err(BackupError::TargetExists(target_db.to_owned()));
    }
    let (copied_blobs, reused_blobs) = copy_blobs(source, &archive, &target_store).await?;
    let mut seen = std::collections::HashSet::new();
    for id in root.roots.iter().flatten() {
        validate_blob_tree(&target_store, crypto.as_ref(), id, &mut seen).await?;
    }
    let records = restored_records(&archive, crypto.as_ref())?;
    let target_log = log_path(target_data_dir, target_db);
    let target_cipher = crypto
        .as_ref()
        .map(|crypto| crypto.cipher(target_db))
        .transpose()?;
    write_log_atomically(&records, &target_log, target_cipher)?;
    let db_bytes = archive.header.root;
    // Keys first, then the root: a database that cannot be opened without its
    // manifest must never be discoverable without one. Metadata is the catalog
    // entry, so it is published last and a node can never discover a partially
    // restored database.
    let publish = async {
        if let Some(crypto) = &crypto {
            target_store
                .cas_root(&target_keys_name, None, &restored_manifest(crypto).encode())
                .await?;
        }
        target_store
            .cas_root(&target_db_name, None, &db_bytes)
            .await?;
        target_store
            .cas_root(&target_meta_name, None, &archive.metadata)
            .await
    }
    .await;
    if let Err(error) = publish {
        let _ = target_store.delete_root(&target_meta_name).await;
        let _ = target_store.delete_root(&target_db_name).await;
        let _ = target_store.delete_root(&target_keys_name).await;
        let _ = fs::remove_file(&target_log);
        return Err(error.into());
    }
    Ok(RestoreReport {
        backup_format_version: archive.header.format_version,
        writer_version: archive.writer_version,
        content_encryption: archive.header.content_encryption,
        source_db: archive.header.source_db,
        target_db: target_db.to_owned(),
        basis_t: archive.basis_t,
        copied_blobs,
        reused_blobs,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One cleartext frame, as a log holds it.
    fn frame(t: u64, log_version: u64) -> FramedRecord {
        let mut frame = Vec::new();
        corium_log::append_framed_record(
            &mut frame,
            &TxRecord {
                t,
                tx_instant: t.cast_signed(),
                datoms: Vec::new(),
            },
        )
        .expect("frame record");
        FramedRecord {
            t,
            log_version,
            frame,
        }
    }

    /// Writes one archive by hand at `format_version` and reads it back.
    fn round_trip(format_version: u32, frames: &[FramedRecord], key_manifest: &[u8]) -> Archive {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("archive.corium");
        let root = bare_root();
        let mut file = File::create(&path).expect("create");
        write_header(
            &mut file,
            &ArchiveHeader {
                format_version,
                creator_version: WRITER_VERSION.to_owned(),
                source_db: "db".to_owned(),
                storage_format_version: root.format_version,
                index_basis_t: root.index_basis_t,
                root: root.encode(),
                content_encryption: if key_manifest.is_empty() {
                    ContentEncryption::None
                } else {
                    ContentEncryption::Storage
                },
                key_manifest: key_manifest.to_vec(),
            },
        )
        .expect("header");
        let checkpoint = encode_checkpoint(
            format_version,
            frames.len() as u64,
            0,
            b"metadata",
            frames,
            key_manifest,
        )
        .expect("checkpoint");
        write_frame(&mut file, CHECKPOINT_TAG, &checkpoint).expect("frame");
        drop(file);
        read_archive(&path, true).expect("read archive")
    }

    /// Format 1 predates the two header fields and the checkpoint's version
    /// table, and archives in it stay readable: an operator's existing backup
    /// file does not stop working because this release can also carry
    /// ciphertext.
    #[test]
    fn a_format_1_archive_still_reads() {
        let frames = [frame(1, 0), frame(2, 0)];
        let archive = round_trip(BACKUP_FORMAT_V1, &frames, &[]);

        assert_eq!(archive.header.format_version, BACKUP_FORMAT_V1);
        assert_eq!(archive.header.content_encryption, ContentEncryption::None);
        assert!(archive.key_manifest.is_empty());
        assert_eq!(archive.basis_t, 2);
        assert_eq!(archive.metadata, b"metadata");
        // With no version table, every record reads as log version 0 — which
        // is what a cleartext single-file log carries.
        assert_eq!(archive.frames, frames);
    }

    /// Format 2 carries the log version each frame was sealed under, because
    /// the version is authenticated into a sealed payload.
    #[test]
    fn a_format_2_archive_preserves_log_versions_and_the_manifest() {
        let frames = [frame(1, 1), frame(2, 1), frame(3, 4)];
        let manifest = b"corium-keys-v1\nfile:kek\n0\n0\n";
        let archive = round_trip(BACKUP_FORMAT_VERSION, &frames, manifest);

        assert_eq!(archive.header.format_version, BACKUP_FORMAT_VERSION);
        assert_eq!(
            archive.header.content_encryption,
            ContentEncryption::Storage
        );
        assert_eq!(archive.key_manifest, manifest);
        assert_eq!(archive.frames, frames);
    }

    /// The run table is a claim about the frames beside it, so it is checked
    /// rather than trusted: a truncated or overlong table must not silently
    /// hand a frame to the wrong key.
    #[test]
    fn a_log_version_table_that_misses_its_records_is_rejected() {
        let mut short = Vec::new();
        write_u64(&mut short, 1).expect("version");
        write_u64(&mut short, 2).expect("count");
        assert!(decode_log_versions(&short, 2).is_ok());
        assert!(decode_log_versions(&short, 3).is_err());
        assert!(decode_log_versions(&short[..8], 2).is_err());

        let mut empty_run = Vec::new();
        write_u64(&mut empty_run, 1).expect("version");
        write_u64(&mut empty_run, 0).expect("count");
        assert!(decode_log_versions(&empty_run, 0).is_err());

        // No table at all means "all of them under version 0".
        assert_eq!(decode_log_versions(&[], 3).expect("decode"), vec![(0, 3)]);
        assert!(decode_log_versions(&[], 0).expect("decode").is_empty());
    }
}
