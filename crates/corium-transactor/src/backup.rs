//! Online, storage-native database backup and offline restore.
//!
//! A backup is one versioned binary archive. The transactor fixes the upper
//! transaction basis and hands the backup client an independent storage
//! connection; [`backup`] replays only through that basis. Re-running it
//! against the same file appends one binary checkpoint containing only the
//! records after the archive's previous checkpoint.

use std::fs::{self, File, OpenOptions};
use std::future::Future;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;

use corium_log::{
    FileLog, LogError, TransactionLog, TxRecord, append_framed_record, decode_framed_records,
};
use corium_protocol::pb;
use corium_store::{
    BlobId, BlobStore, DbRoot, FORMAT_VERSION, FsStore, RootStore, StoreError, db_root_name,
};
use thiserror::Error;

use crate::{LogBackend, NodeStore, StoreSpec};

/// Binary backup container version written by this release.
pub const BACKUP_FORMAT_VERSION: u32 = 1;

const BACKUP_MAGIC: &[u8; 16] = b"CORIUM_BACKUP\0\0\0";
const BLOB_TAG: [u8; 4] = *b"BLOB";
const CHECKPOINT_TAG: [u8; 4] = *b"CKPT";
const CHECKPOINT_END: &[u8; 4] = b"DONE";
const WRITER_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Counters and snapshot identity returned by a backup.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackupReport {
    /// Binary backup container version.
    pub backup_format_version: u32,
    /// Corium version that wrote the latest checkpoint.
    pub writer_version: String,
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
    /// The advertised backend cannot be opened independently.
    #[error("storage backend cannot be backed up independently: {0}")]
    UnsupportedSource(String),
}

/// An independently accessible storage service plus the transaction basis
/// fixed by the running transactor.
#[derive(Clone, Debug)]
pub struct BackupSource {
    store: StoreSpec,
    data_dir: PathBuf,
    basis_t: u64,
}

impl BackupSource {
    /// Decodes the transactor's one-shot backup discovery response.
    ///
    /// # Errors
    /// Returns an error for missing connection details, an in-memory backend,
    /// or a backend omitted from this build.
    pub fn from_info(info: pb::GetBackupInfoResponse) -> Result<Self, BackupError> {
        use pb::storage_connection::Backend;

        let storage = info
            .storage
            .and_then(|storage| storage.backend)
            .ok_or_else(|| BackupError::Invalid("transactor returned no storage backend".into()))?;
        let (store, data_dir) = match storage {
            Backend::Memory(_) => {
                return Err(BackupError::UnsupportedSource(
                    "memory storage is confined to the transactor process".into(),
                ));
            }
            Backend::Filesystem(storage) => (StoreSpec::Fs, PathBuf::from(storage.data_dir)),
            Backend::Postgres(storage) => {
                #[cfg(feature = "postgres")]
                {
                    (
                        StoreSpec::Postgres {
                            connection_string: storage.connection_string,
                        },
                        PathBuf::new(),
                    )
                }
                #[cfg(not(feature = "postgres"))]
                {
                    let _ = storage;
                    return Err(BackupError::UnsupportedSource(
                        "this build lacks PostgreSQL support".into(),
                    ));
                }
            }
            Backend::Turso(storage) => {
                #[cfg(feature = "turso")]
                {
                    (StoreSpec::Turso { path: storage.path }, PathBuf::new())
                }
                #[cfg(not(feature = "turso"))]
                {
                    let _ = storage;
                    return Err(BackupError::UnsupportedSource(
                        "this build lacks Turso support".into(),
                    ));
                }
            }
            Backend::S3(storage) => {
                #[cfg(feature = "s3")]
                {
                    (
                        StoreSpec::S3 {
                            bucket: storage.bucket,
                            prefix: storage.prefix,
                        },
                        PathBuf::new(),
                    )
                }
                #[cfg(not(feature = "s3"))]
                {
                    let _ = storage;
                    return Err(BackupError::UnsupportedSource(
                        "this build lacks S3 support".into(),
                    ));
                }
            }
        };
        Ok(Self {
            store,
            data_dir,
            basis_t: info.basis_t,
        })
    }

    /// Fixed upper transaction basis for this run.
    #[must_use]
    pub const fn basis_t(&self) -> u64 {
        self.basis_t
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ArchiveHeader {
    creator_version: String,
    source_db: String,
    index_basis_t: u64,
    storage_format_version: u32,
    root: Vec<u8>,
}

#[derive(Debug)]
struct Archive {
    header: ArchiveHeader,
    writer_version: String,
    basis_t: u64,
    metadata: Vec<u8>,
    records: Vec<TxRecord>,
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
    write_u32(writer, BACKUP_FORMAT_VERSION)?;
    write_bytes(writer, header.creator_version.as_bytes())?;
    write_bytes(writer, header.source_db.as_bytes())?;
    write_u32(writer, header.storage_format_version)?;
    write_u64(writer, header.index_basis_t)?;
    write_bytes(writer, &header.root)
}

fn encode_checkpoint(
    basis_t: u64,
    previous_basis_t: u64,
    metadata: &[u8],
    records: &[TxRecord],
) -> Result<Vec<u8>, BackupError> {
    let mut framed = Vec::new();
    for record in records {
        append_framed_record(&mut framed, record)?;
    }
    let mut payload = Vec::new();
    write_bytes(&mut payload, WRITER_VERSION.as_bytes())?;
    write_u64(&mut payload, basis_t)?;
    write_u64(
        &mut payload,
        if records.is_empty() {
            0
        } else {
            previous_basis_t.saturating_add(1)
        },
    )?;
    write_u64(
        &mut payload,
        u64::try_from(records.len()).map_err(|_| invalid("too many transaction records"))?,
    )?;
    write_bytes(&mut payload, metadata)?;
    write_bytes(&mut payload, &framed)?;
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

fn decode_checkpoint(
    file: &mut File,
    frame_end: u64,
    previous_basis_t: u64,
    include_records: bool,
) -> Result<(String, u64, Vec<u8>, Vec<TxRecord>), BackupError> {
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
    let framed_len = read_u64(file)?;
    let framed_start = file.stream_position()?;
    let records_end = framed_start
        .checked_add(framed_len)
        .ok_or_else(|| invalid("checkpoint transaction-record length overflowed"))?;
    if records_end
        .checked_add(4)
        .is_none_or(|checkpoint_end| checkpoint_end != frame_end)
    {
        return Err(invalid(
            "checkpoint transaction records do not fill their frame",
        ));
    }
    let framed = if include_records {
        let len = usize::try_from(framed_len)
            .map_err(|_| invalid("transaction records are too large for this platform"))?;
        let mut bytes = vec![0; len];
        file.read_exact(&mut bytes)?;
        bytes
    } else {
        file.seek(SeekFrom::Start(records_end))?;
        Vec::new()
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
    let records = if include_records {
        let records = decode_framed_records(&framed)?;
        if records.len() as u64 != record_count {
            return Err(invalid(
                "checkpoint record count does not match its payload",
            ));
        }
        if record_count > 0 {
            validate_records(&records, start_t, basis_t)?;
        }
        records
    } else {
        Vec::new()
    };
    Ok((writer_version, basis_t, metadata, records))
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
    if format_version != BACKUP_FORMAT_VERSION {
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
    Ok(ArchiveHeader {
        creator_version,
        source_db,
        storage_format_version,
        index_basis_t: read_u64(file)?,
        root: read_bytes(file, "database root")?,
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
    let mut records = Vec::new();
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
            let (checkpoint_writer, checkpoint_basis, checkpoint_meta, checkpoint_records) =
                decode_checkpoint(&mut file, frame_end, basis_t, include_records)?;
            writer_version = checkpoint_writer;
            basis_t = checkpoint_basis;
            metadata = checkpoint_meta;
            records.extend(checkpoint_records);
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
        records,
        blob_offsets,
        durable_len,
    })
}

/// Writes one blob and everything it references into the binary archive,
/// children first.
fn write_blob_tree<'a>(
    source: &'a dyn BlobStore,
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
        for child in corium_store::index_blob_children(&bytes)? {
            write_blob_tree(source, &child, seen, writer, copied).await?;
        }
        write_frame(writer, BLOB_TAG, &bytes)?;
        *copied += 1;
        Ok(())
    })
}

fn validate_blob_tree<'a>(
    store: &'a dyn BlobStore,
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
        for child in corium_store::index_blob_children(&bytes)? {
            validate_blob_tree(store, &child, seen).await?;
        }
        Ok(())
    })
}

fn write_log_atomically(records: &[TxRecord], target: &Path) -> Result<(), BackupError> {
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)?;
    }
    let temporary = target.with_extension("tmp");
    match fs::remove_file(&temporary) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let log = FileLog::open(&temporary)?;
    for record in records {
        log.append(record)?;
    }
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
    }
}

fn sanitize_root(mut root: DbRoot) -> DbRoot {
    root.lease_version = 0;
    root.owner.clear();
    root.lease_expires_unix_ms = 0;
    root.owner_endpoint.clear();
    root
}

fn validate_records(
    records: &[corium_log::TxRecord],
    start: u64,
    end_inclusive: u64,
) -> Result<(), BackupError> {
    if start > end_inclusive {
        return Ok(());
    }
    let expected = end_inclusive - start + 1;
    if records.len() as u64 != expected
        || records.first().map(|record| record.t) != Some(start)
        || records.last().map(|record| record.t) != Some(end_inclusive)
        || records.windows(2).any(|pair| pair[1].t != pair[0].t + 1)
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
    source_store: &dyn BlobStore,
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
            creator_version: WRITER_VERSION.to_owned(),
            source_db: db.to_owned(),
            index_basis_t: root.index_basis_t,
            storage_format_version: root.format_version,
            root: root.encode(),
        },
    )?;
    let mut seen = std::collections::HashSet::new();
    let mut copied = 0;
    for id in root.roots.iter().flatten() {
        write_blob_tree(source_store, id, &mut seen, &mut file, &mut copied).await?;
    }
    write_frame(&mut file, CHECKPOINT_TAG, checkpoint)?;
    file.sync_all()?;
    fs::rename(temporary, destination)?;
    Ok(copied)
}

/// Copies one live database into `destination` through a fixed transaction
/// checkpoint.
///
/// `source` comes from [`BackupSource::from_info`], after the transactor fixes
/// its current `t`. The source log may continue growing; this function reads
/// only `(old_backup_t, source_t]`. Reusing a destination retains its index
/// snapshot and appends only missing transaction records.
///
/// # Errors
/// Returns an error if the database is missing, the checkpoint cannot be read
/// contiguously, the destination belongs to another database, or storage
/// cannot be opened.
pub async fn backup(
    source: &BackupSource,
    db: &str,
    destination: impl AsRef<Path>,
) -> Result<BackupReport, BackupError> {
    if !valid_db_name(db) {
        return Err(BackupError::MissingDatabase(db.to_owned()));
    }
    let destination = destination.as_ref();
    let source_store = Arc::new(NodeStore::open_existing(&source.store, &source.data_dir).await?);
    let source_logs =
        LogBackend::for_spec(&source.store, &source.data_dir, Arc::clone(&source_store));
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
        if archive.header.source_db != db {
            return Err(invalid(format!(
                "backup belongs to database {:?}, not {db:?}",
                archive.header.source_db
            )));
        }
        if archive.basis_t > source.basis_t {
            return Err(invalid(format!(
                "backup basis {} is ahead of source checkpoint {}",
                archive.basis_t, source.basis_t
            )));
        }
    }
    let existing_basis = previous.as_ref().map_or(0, |archive| archive.basis_t);
    let start = existing_basis.saturating_add(1);
    let records = if start <= source.basis_t {
        let end = source
            .basis_t
            .checked_add(1)
            .ok_or_else(|| BackupError::Invalid("source basis is too large".into()))?;
        let log = source_logs.open_read_only(db).await?;
        log.tx_range_async(start, Some(end)).await?
    } else {
        Vec::new()
    };
    validate_records(&records, start, source.basis_t)?;

    // An existing archive's snapshot is its stable replay base. On a first
    // backup, opportunistically retain the source snapshot only when it is
    // not ahead of the fixed log checkpoint; otherwise a bare root forces a
    // correct full-log replay on restore.
    let root = if let Some(archive) = &previous {
        let root = DbRoot::decode(&archive.header.root)
            .ok_or_else(|| invalid("archive database root cannot be decoded"))?;
        if root.index_basis_t != archive.header.index_basis_t
            || root.format_version != archive.header.storage_format_version
        {
            return Err(invalid("archive header does not match its database root"));
        }
        sanitize_root(root)
    } else if source_root.index_basis_t <= source.basis_t {
        sanitize_root(source_root)
    } else {
        bare_root()
    };
    let checkpoint = encode_checkpoint(source.basis_t, existing_basis, &meta, &records)?;
    let (copied_blobs, writer_version) = if let Some(archive) = &previous {
        (
            0,
            append_archive_checkpoint(destination, archive, &checkpoint, !records.is_empty())?,
        )
    } else {
        (
            create_archive(destination, db, &root, source_store.as_ref(), &checkpoint).await?,
            WRITER_VERSION.to_owned(),
        )
    };
    Ok(BackupReport {
        backup_format_version: BACKUP_FORMAT_VERSION,
        writer_version,
        basis_t: source.basis_t,
        index_basis_t: root.index_basis_t,
        copied_blobs,
        reused_blobs: 0,
        replayed_transactions: records.len(),
    })
}

/// Restores a backup under `target_db`, allowing restore-as-clone by choosing
/// a name different from the archive's source database.
///
/// Existing target databases are never overwritten. Immutable blobs already
/// present in the target store are shared rather than recopied.
///
/// # Errors
/// Returns an error for malformed/incomplete backups, unsupported formats,
/// existing targets, or I/O failures.
pub async fn restore(
    source: impl AsRef<Path>,
    target_data_dir: impl AsRef<Path>,
    target_db: &str,
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

    let target_store = FsStore::open(target_data_dir.join("store"))?;
    let target_db_name = db_root_name(target_db);
    let target_meta_name = meta_root_name(target_db);
    if target_store.get_root(&target_db_name).await?.is_some()
        || target_store.get_root(&target_meta_name).await?.is_some()
        || log_path(target_data_dir, target_db).exists()
    {
        return Err(BackupError::TargetExists(target_db.to_owned()));
    }
    let mut backup_file = File::open(source)?;
    let mut copied_blobs = 0;
    let mut reused_blobs = 0;
    for (offset, len) in &archive.blob_offsets {
        backup_file.seek(SeekFrom::Start(*offset))?;
        let len =
            usize::try_from(*len).map_err(|_| invalid("blob is too large for this platform"))?;
        let mut bytes = vec![0; len];
        backup_file.read_exact(&mut bytes)?;
        let id = corium_store::digest(&bytes);
        if target_store.contains(&id).await? {
            reused_blobs += 1;
        } else {
            let stored = target_store.put(&bytes).await?;
            if stored != id {
                return Err(invalid(format!("blob digest changed while restoring {id}")));
            }
            copied_blobs += 1;
        }
    }
    let mut seen = std::collections::HashSet::new();
    for id in root.roots.iter().flatten() {
        validate_blob_tree(&target_store, id, &mut seen).await?;
    }
    let target_log = log_path(target_data_dir, target_db);
    write_log_atomically(&archive.records, &target_log)?;
    let db_bytes = archive.header.root;
    // Metadata is the catalog entry, so publish it last. A node can never
    // discover a partially restored database.
    let publish = match target_store
        .cas_root(&target_db_name, None, &db_bytes)
        .await
    {
        Ok(()) => {
            target_store
                .cas_root(&target_meta_name, None, &archive.metadata)
                .await
        }
        Err(error) => Err(error),
    };
    if let Err(error) = publish {
        let _ = target_store.delete_root(&target_db_name).await;
        let _ = target_store.delete_root(&target_meta_name).await;
        let _ = fs::remove_file(&target_log);
        return Err(error.into());
    }
    Ok(RestoreReport {
        backup_format_version: BACKUP_FORMAT_VERSION,
        writer_version: archive.writer_version,
        source_db: archive.header.source_db,
        target_db: target_db.to_owned(),
        basis_t: archive.basis_t,
        copied_blobs,
        reused_blobs,
    })
}
