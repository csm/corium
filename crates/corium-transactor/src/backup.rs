//! Offline, content-addressed database backup and restore.
//!
//! A backup directory is itself a Corium blob/root store plus a durable log
//! and a small manifest. Re-running [`backup`] against the same directory is
//! incremental: immutable blobs already present are not copied.

use std::fs;
use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;

use corium_log::{FileLog, LogError, TransactionLog, TxRecord, VersionedLog};
use corium_store::{
    BlobId, BlobStore, DbRoot, FORMAT_VERSION, FsStore, RootStore, StoreError, db_root_name,
};
use thiserror::Error;

const MANIFEST_NAME: &str = "manifest";
const MANIFEST_MAGIC: &str = "corium-backup-v1";

/// Counters and snapshot identity returned by a backup.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackupReport {
    /// Transaction basis preserved by the snapshot.
    pub basis_t: u64,
    /// Index basis named by the copied root.
    pub index_basis_t: u64,
    /// Immutable blobs newly copied during this run.
    pub copied_blobs: usize,
    /// Immutable blobs already present in an incremental destination.
    pub reused_blobs: usize,
}

/// Counters and identity returned by a restore.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RestoreReport {
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
    /// A required root, blob, log, or manifest is malformed/missing.
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
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Manifest {
    source_db: String,
    basis_t: u64,
    index_basis_t: u64,
    format_version: u32,
}

impl Manifest {
    fn encode(&self) -> String {
        format!(
            "{MANIFEST_MAGIC}\nsource-db={}\nbasis-t={}\nindex-basis-t={}\nformat-version={}\n",
            self.source_db, self.basis_t, self.index_basis_t, self.format_version
        )
    }

    fn decode(bytes: &[u8]) -> Result<Self, BackupError> {
        let text = std::str::from_utf8(bytes)
            .map_err(|_| BackupError::Invalid("manifest is not UTF-8".into()))?;
        let mut lines = text.lines();
        if lines.next() != Some(MANIFEST_MAGIC) {
            return Err(BackupError::Invalid("unknown manifest header".into()));
        }
        let field = |line: Option<&str>, name: &str| -> Result<String, BackupError> {
            line.and_then(|line| line.strip_prefix(&format!("{name}=")))
                .map(str::to_owned)
                .ok_or_else(|| BackupError::Invalid(format!("manifest has no {name}")))
        };
        let source_db = field(lines.next(), "source-db")?;
        let basis_t = field(lines.next(), "basis-t")?
            .parse()
            .map_err(|_| BackupError::Invalid("manifest basis-t is invalid".into()))?;
        let index_basis_t = field(lines.next(), "index-basis-t")?
            .parse()
            .map_err(|_| BackupError::Invalid("manifest index-basis-t is invalid".into()))?;
        let format_version = field(lines.next(), "format-version")?
            .parse()
            .map_err(|_| BackupError::Invalid("manifest format-version is invalid".into()))?;
        if format_version > FORMAT_VERSION {
            return Err(BackupError::UnsupportedFormat {
                found: format_version,
                supported: FORMAT_VERSION,
            });
        }
        Ok(Self {
            source_db,
            basis_t,
            index_basis_t,
            format_version,
        })
    }
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

async fn put_root(store: &dyn RootStore, name: &str, bytes: &[u8]) -> Result<(), StoreError> {
    loop {
        let previous = store.get_root(name).await?;
        if previous.as_deref() == Some(bytes) {
            return Ok(());
        }
        match store.cas_root(name, previous.as_deref(), bytes).await {
            Ok(()) => return Ok(()),
            Err(StoreError::CasFailed { .. }) => {}
            Err(error) => return Err(error),
        }
    }
}

fn copy_file_atomically(source: &Path, target: &Path) -> Result<(), io::Error> {
    if let Some(parent) = target.parent() {
        fs::create_dir_all(parent)?;
    }
    let temporary = target.with_extension("tmp");
    fs::copy(source, &temporary)?;
    fs::rename(temporary, target)
}

/// Writes `records` as a single consolidated log file, atomically.
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

async fn copy_root_blobs(
    source: &dyn BlobStore,
    target: &dyn BlobStore,
    root: &DbRoot,
) -> Result<(usize, usize), BackupError> {
    let mut copied = 0;
    let mut reused = 0;
    let mut seen = std::collections::HashSet::new();
    for id in root.roots.iter().flatten() {
        copy_blob_tree(source, target, id, &mut seen, &mut copied, &mut reused).await?;
    }
    Ok((copied, reused))
}

/// Copies one blob and everything it references, children first.
///
/// Child references are decoded with the same helper GC's mark phase uses
/// (`index_blob_children`), so backup and reachability cannot diverge.
/// Post-order preserves the store invariant that a present parent's
/// children are present too — an interrupted run never leaves a manifest
/// whose chunks a later incremental backup would wrongly skip — and lets a
/// blob already in the target count as reused without re-reading its tree.
fn copy_blob_tree<'a>(
    source: &'a dyn BlobStore,
    target: &'a dyn BlobStore,
    id: &'a BlobId,
    seen: &'a mut std::collections::HashSet<BlobId>,
    copied: &'a mut usize,
    reused: &'a mut usize,
) -> Pin<Box<dyn Future<Output = Result<(), BackupError>> + Send + 'a>> {
    Box::pin(async move {
        if !seen.insert(id.clone()) {
            return Ok(());
        }
        if target.contains(id).await? {
            *reused += 1;
            return Ok(());
        }
        let bytes = source
            .get(id)
            .await?
            .ok_or_else(|| BackupError::Invalid(format!("root references missing blob {id}")))?;
        for child in corium_store::index_blob_children(&bytes)? {
            copy_blob_tree(source, target, &child, seen, copied, reused).await?;
        }
        let copied_id = target.put(&bytes).await?;
        if copied_id != *id {
            return Err(BackupError::Invalid(format!(
                "blob digest changed while copying {id}"
            )));
        }
        *copied += 1;
        Ok(())
    })
}

/// Copies one offline database into `destination`.
///
/// `source_data_dir` is a transactor data directory. The transactor must be
/// stopped so the root, metadata, and log form one stable snapshot. Reusing a
/// destination performs an incremental backup and copies only absent blobs.
///
/// # Errors
/// Returns an error if the database is missing, the snapshot is inconsistent,
/// or files cannot be read/written.
pub async fn backup(
    source_data_dir: impl AsRef<Path>,
    db: &str,
    destination: impl AsRef<Path>,
) -> Result<BackupReport, BackupError> {
    if !valid_db_name(db) {
        return Err(BackupError::MissingDatabase(db.to_owned()));
    }
    let source_data_dir = source_data_dir.as_ref();
    let destination = destination.as_ref();
    let source = FsStore::open(source_data_dir.join("store"))?;
    let db_name = db_root_name(db);
    let meta_name = meta_root_name(db);
    let db_bytes = source
        .get_root(&db_name)
        .await?
        .ok_or_else(|| BackupError::MissingDatabase(db.to_owned()))?;
    let meta = source
        .get_root(&meta_name)
        .await?
        .ok_or_else(|| BackupError::MissingDatabase(db.to_owned()))?;
    let root = DbRoot::decode(&db_bytes)
        .ok_or_else(|| BackupError::Invalid("database root cannot be decoded".into()))?;
    if root.format_version > FORMAT_VERSION {
        return Err(BackupError::UnsupportedFormat {
            found: root.format_version,
            supported: FORMAT_VERSION,
        });
    }
    let source_logs = source_data_dir.join("logs");
    if !VersionedLog::exists(&source_logs, db) {
        return Err(BackupError::Invalid("transaction log is missing".into()));
    }
    // Consolidate the (possibly lease-versioned) log into one clean file:
    // the merged read applies the HA takeover cutoffs, so the backup never
    // carries a deposed writer's stale appends.
    let records = VersionedLog::open_read_only(&source_logs, db)?.replay()?;
    let basis_t = records.last().map_or(0, |record| record.t);
    if root.index_basis_t > basis_t {
        return Err(BackupError::Invalid(format!(
            "index basis {} is ahead of log basis {basis_t}",
            root.index_basis_t
        )));
    }

    fs::create_dir_all(destination)?;
    let target = FsStore::open(destination.join("store"))?;
    let (copied_blobs, reused_blobs) = copy_root_blobs(&source, &target, &root).await?;
    put_root(&target, &db_name, &db_bytes).await?;
    put_root(&target, &meta_name, &meta).await?;
    write_log_atomically(&records, &log_path(destination, db))?;
    let manifest = Manifest {
        source_db: db.to_owned(),
        basis_t,
        index_basis_t: root.index_basis_t,
        format_version: root.format_version,
    };
    let manifest_tmp = destination.join("manifest.tmp");
    fs::write(&manifest_tmp, manifest.encode())?;
    fs::rename(manifest_tmp, destination.join(MANIFEST_NAME))?;
    Ok(BackupReport {
        basis_t,
        index_basis_t: root.index_basis_t,
        copied_blobs,
        reused_blobs,
    })
}

/// Restores a backup under `target_db`, allowing restore-as-clone by choosing
/// a name different from the manifest's source database.
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
    let manifest = Manifest::decode(&fs::read(source.join(MANIFEST_NAME))?)?;
    let source_store = FsStore::open(source.join("store"))?;
    let source_db_name = db_root_name(&manifest.source_db);
    let source_meta_name = meta_root_name(&manifest.source_db);
    let db_bytes = source_store
        .get_root(&source_db_name)
        .await?
        .ok_or_else(|| BackupError::Invalid("database root is missing".into()))?;
    let meta = source_store
        .get_root(&source_meta_name)
        .await?
        .ok_or_else(|| BackupError::Invalid("metadata root is missing".into()))?;
    let root = DbRoot::decode(&db_bytes)
        .ok_or_else(|| BackupError::Invalid("database root cannot be decoded".into()))?;
    if root.index_basis_t != manifest.index_basis_t
        || root.format_version != manifest.format_version
    {
        return Err(BackupError::Invalid(
            "manifest does not match the database root".into(),
        ));
    }
    let source_log = log_path(source, &manifest.source_db);
    if !source_log.is_file() {
        return Err(BackupError::Invalid("transaction log is missing".into()));
    }
    let log = FileLog::open(&source_log)?;
    let actual_basis = log.replay()?.last().map_or(0, |record| record.t);
    if actual_basis != manifest.basis_t {
        return Err(BackupError::Invalid(format!(
            "manifest basis {} does not match log basis {actual_basis}",
            manifest.basis_t
        )));
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
    let (copied_blobs, reused_blobs) = copy_root_blobs(&source_store, &target_store, &root).await?;
    let target_log = log_path(target_data_dir, target_db);
    copy_file_atomically(&source_log, &target_log)?;
    // Metadata is the catalog entry, so publish it last. A node can never
    // discover a partially restored database.
    let publish = match target_store
        .cas_root(&target_db_name, None, &db_bytes)
        .await
    {
        Ok(()) => target_store.cas_root(&target_meta_name, None, &meta).await,
        Err(error) => Err(error),
    };
    if let Err(error) = publish {
        let _ = target_store.delete_root(&target_db_name).await;
        let _ = target_store.delete_root(&target_meta_name).await;
        let _ = fs::remove_file(&target_log);
        return Err(error.into());
    }
    Ok(RestoreReport {
        source_db: manifest.source_db,
        target_db: target_db.to_owned(),
        basis_t: manifest.basis_t,
        copied_blobs,
        reused_blobs,
    })
}
