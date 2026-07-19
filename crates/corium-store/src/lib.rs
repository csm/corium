//! Content-addressed blob and fenced root stores for immutable index segments.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fmt,
    fs::{self, File, OpenOptions},
    io,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, RwLock},
};

use async_trait::async_trait;
use fs2::FileExt;
use thiserror::Error;
use tokio_stream::{Stream, StreamExt, wrappers::ReceiverStream};

/// A content identifier for immutable blobs.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct BlobId(String);

impl BlobId {
    /// Returns the hexadecimal digest string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Parses a stored 64-character hexadecimal digest.
    #[must_use]
    pub fn from_hex(text: &str) -> Option<Self> {
        (text.len() == 64 && text.bytes().all(|byte| byte.is_ascii_hexdigit()))
            .then(|| Self(text.to_owned()))
    }
}

impl fmt::Display for BlobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Errors raised by store implementations.
#[derive(Debug, Error)]
pub enum StoreError {
    /// I/O failure.
    #[error("store I/O failed: {0}")]
    Io(#[from] io::Error),
    /// Root compare-and-swap failed because the current fence differed.
    #[error("root CAS failed: expected {expected:?}, actual {actual:?}")]
    CasFailed {
        /// Expected root bytes supplied by the caller.
        expected: Option<Vec<u8>>,
        /// Actual root bytes currently stored.
        actual: Option<Vec<u8>>,
    },
    /// Blob digest did not match its content.
    #[error("blob content did not match digest {0}")]
    CorruptBlob(BlobId),
    /// A live graph references a blob that is not present.
    #[error("reachable blob is missing: {0}")]
    MissingBlob(BlobId),
    /// Root name cannot be safely represented on the filesystem.
    #[error("invalid root name {0:?}")]
    InvalidRootName(String),
    /// A blocking store worker failed before returning its result.
    #[error("store blocking task failed: {0}")]
    BlockingTask(String),
}

/// Asynchronous stream of blob identifiers produced by [`BlobStore::list`].
pub type BlobIdStream = Pin<Box<dyn Stream<Item = Result<BlobId, StoreError>> + Send + 'static>>;

async fn run_blocking<T>(
    operation: impl FnOnce() -> Result<T, StoreError> + Send + 'static,
) -> Result<T, StoreError>
where
    T: Send + 'static,
{
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|error| StoreError::BlockingTask(error.to_string()))?
}

/// Immutable content-addressed blob storage.
#[async_trait]
pub trait BlobStore: Send + Sync {
    /// Stores bytes and returns their content id.
    ///
    /// # Errors
    ///
    /// Returns an error if the backend cannot persist the blob.
    async fn put(&self, bytes: &[u8]) -> Result<BlobId, StoreError>;
    /// Loads bytes by id, returning `None` when missing.
    ///
    /// # Errors
    ///
    /// Returns an error if the backend cannot read or verify the blob.
    async fn get(&self, id: &BlobId) -> Result<Option<Vec<u8>>, StoreError>;
    /// Reports whether a blob is present.
    ///
    /// # Errors
    ///
    /// Returns an error if the backend cannot inspect the blob.
    async fn contains(&self, id: &BlobId) -> Result<bool, StoreError> {
        Ok(self.get(id).await?.is_some())
    }
    /// Deletes a blob during garbage collection. Missing blobs are ignored.
    ///
    /// # Errors
    ///
    /// Returns an error if the backend cannot delete the blob.
    async fn delete(&self, id: &BlobId) -> Result<(), StoreError>;
    /// Lists all blob identifiers known to this backend.
    ///
    /// # Errors
    ///
    /// Returns an error if the backend cannot enumerate blobs.
    async fn list(&self) -> Result<BlobIdStream, StoreError>;
}

/// Named root pointer storage with compare-and-swap fencing.
#[async_trait]
pub trait RootStore: Send + Sync {
    /// Reads a root pointer.
    ///
    /// # Errors
    ///
    /// Returns an error if the backend cannot read the root.
    async fn get_root(&self, name: &str) -> Result<Option<Vec<u8>>, StoreError>;
    /// Publishes a root only if the stored pointer equals `expected`.
    ///
    /// # Errors
    ///
    /// Returns an error if the fence does not match or the backend cannot publish.
    async fn cas_root(
        &self,
        name: &str,
        expected: Option<&[u8]>,
        new: &[u8],
    ) -> Result<(), StoreError>;
    /// Removes a root pointer. Missing roots are ignored.
    ///
    /// # Errors
    ///
    /// Returns an error if the backend cannot delete the root.
    async fn delete_root(&self, name: &str) -> Result<(), StoreError>;
    /// Lists root names beginning with `prefix`, in sorted order.
    ///
    /// # Errors
    ///
    /// Returns an error if the backend cannot enumerate roots.
    async fn list_roots(&self, prefix: &str) -> Result<Vec<String>, StoreError>;
}

/// In-memory blob and root store for tests and embedded use.
#[derive(Clone, Default)]
pub struct MemoryStore {
    inner: Arc<RwLock<MemoryInner>>,
}
#[derive(Default)]
struct MemoryInner {
    blobs: HashMap<BlobId, Vec<u8>>,
    roots: BTreeMap<String, Vec<u8>>,
}

#[async_trait]
impl BlobStore for MemoryStore {
    async fn put(&self, bytes: &[u8]) -> Result<BlobId, StoreError> {
        let inner = Arc::clone(&self.inner);
        let bytes = bytes.to_vec();
        run_blocking(move || {
            let id = digest(&bytes);
            inner
                .write()
                .expect("poisoned store lock")
                .blobs
                .insert(id.clone(), bytes);
            Ok(id)
        })
        .await
    }
    async fn get(&self, id: &BlobId) -> Result<Option<Vec<u8>>, StoreError> {
        let inner = Arc::clone(&self.inner);
        let id = id.clone();
        run_blocking(move || {
            Ok(inner
                .read()
                .expect("poisoned store lock")
                .blobs
                .get(&id)
                .cloned())
        })
        .await
    }
    async fn delete(&self, id: &BlobId) -> Result<(), StoreError> {
        let inner = Arc::clone(&self.inner);
        let id = id.clone();
        run_blocking(move || {
            inner
                .write()
                .expect("poisoned store lock")
                .blobs
                .remove(&id);
            Ok(())
        })
        .await
    }
    async fn list(&self) -> Result<BlobIdStream, StoreError> {
        let inner = Arc::clone(&self.inner);
        let ids = run_blocking(move || {
            Ok(inner
                .read()
                .expect("poisoned store lock")
                .blobs
                .keys()
                .cloned()
                .collect::<Vec<_>>())
        })
        .await?;
        Ok(Box::pin(tokio_stream::iter(ids.into_iter().map(Ok))))
    }
}
#[async_trait]
impl RootStore for MemoryStore {
    async fn get_root(&self, name: &str) -> Result<Option<Vec<u8>>, StoreError> {
        let inner = Arc::clone(&self.inner);
        let name = name.to_owned();
        run_blocking(move || {
            Ok(inner
                .read()
                .expect("poisoned store lock")
                .roots
                .get(&name)
                .cloned())
        })
        .await
    }
    async fn cas_root(
        &self,
        name: &str,
        expected: Option<&[u8]>,
        new: &[u8],
    ) -> Result<(), StoreError> {
        let inner = Arc::clone(&self.inner);
        let name = name.to_owned();
        let expected = expected.map(<[u8]>::to_vec);
        let new = new.to_vec();
        run_blocking(move || {
            let mut inner = inner.write().expect("poisoned store lock");
            let actual = inner.roots.get(&name).cloned();
            if actual != expected {
                return Err(StoreError::CasFailed { expected, actual });
            }
            inner.roots.insert(name, new);
            Ok(())
        })
        .await
    }
    async fn delete_root(&self, name: &str) -> Result<(), StoreError> {
        let inner = Arc::clone(&self.inner);
        let name = name.to_owned();
        run_blocking(move || {
            inner
                .write()
                .expect("poisoned store lock")
                .roots
                .remove(&name);
            Ok(())
        })
        .await
    }
    async fn list_roots(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        let inner = Arc::clone(&self.inner);
        let prefix = prefix.to_owned();
        run_blocking(move || {
            Ok(inner
                .read()
                .expect("poisoned store lock")
                .roots
                .keys()
                .filter(|name| name.starts_with(&prefix))
                .cloned()
                .collect())
        })
        .await
    }
}

/// Filesystem-backed content-addressed blob and fenced root store.
#[derive(Clone)]
pub struct FsStore {
    root: PathBuf,
}
impl FsStore {
    /// Opens or creates a store below `root`.
    ///
    /// # Errors
    ///
    /// Returns an error if the directory layout cannot be created.
    pub fn open(root: impl AsRef<Path>) -> Result<Self, StoreError> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(root.join("blobs"))?;
        fs::create_dir_all(root.join("roots"))?;
        Ok(Self { root })
    }
    fn blob_path(&self, id: &BlobId) -> PathBuf {
        self.root.join("blobs").join(id.as_str())
    }
    fn root_path(&self, name: &str) -> Result<PathBuf, StoreError> {
        if name.is_empty()
            || name == "."
            || name == ".."
            || name.contains('/')
            || name.contains('\\')
        {
            return Err(StoreError::InvalidRootName(name.to_owned()));
        }
        Ok(self.root.join("roots").join(name))
    }

    fn root_lock(&self, name: &str) -> Result<RootLock, StoreError> {
        let root_path = self.root_path(name)?;
        RootLock::acquire(&root_path.with_extension("lock"))
    }
}
#[async_trait]
impl BlobStore for FsStore {
    async fn put(&self, bytes: &[u8]) -> Result<BlobId, StoreError> {
        let store = self.clone();
        let bytes = bytes.to_vec();
        run_blocking(move || {
            let id = digest(&bytes);
            let path = store.blob_path(&id);
            if !path.exists() {
                let tmp = path.with_extension("tmp");
                fs::write(&tmp, &bytes)?;
                fs::rename(tmp, path)?;
            }
            Ok(id)
        })
        .await
    }
    async fn get(&self, id: &BlobId) -> Result<Option<Vec<u8>>, StoreError> {
        let store = self.clone();
        let id = id.clone();
        run_blocking(move || {
            let path = store.blob_path(&id);
            if !path.exists() {
                return Ok(None);
            }
            let bytes = fs::read(path)?;
            if digest(&bytes) != id {
                return Err(StoreError::CorruptBlob(id));
            }
            Ok(Some(bytes))
        })
        .await
    }
    async fn contains(&self, id: &BlobId) -> Result<bool, StoreError> {
        let store = self.clone();
        let id = id.clone();
        run_blocking(move || Ok(store.blob_path(&id).is_file())).await
    }
    async fn delete(&self, id: &BlobId) -> Result<(), StoreError> {
        let store = self.clone();
        let id = id.clone();
        run_blocking(move || match fs::remove_file(store.blob_path(&id)) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        })
        .await
    }
    async fn list(&self) -> Result<BlobIdStream, StoreError> {
        let path = self.root.join("blobs");
        let entries = run_blocking(move || Ok(fs::read_dir(path)?)).await?;
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let failure_tx = tx.clone();
        tokio::spawn(async move {
            let result = tokio::task::spawn_blocking(move || {
                for entry in entries {
                    let id = (|| {
                        let entry = entry?;
                        if !entry.file_type()?.is_file() {
                            return Ok(None);
                        }
                        Ok(entry.file_name().to_str().and_then(BlobId::from_hex))
                    })();
                    match id {
                        Ok(Some(id)) => {
                            if tx.blocking_send(Ok(id)).is_err() {
                                return;
                            }
                        }
                        Ok(None) => {}
                        Err(error) => {
                            let _ = tx.blocking_send(Err(StoreError::Io(error)));
                            return;
                        }
                    }
                }
            })
            .await;
            if let Err(error) = result {
                let _ = failure_tx
                    .send(Err(StoreError::BlockingTask(error.to_string())))
                    .await;
            }
        });
        Ok(Box::pin(ReceiverStream::new(rx)))
    }
}
#[async_trait]
impl RootStore for FsStore {
    async fn get_root(&self, name: &str) -> Result<Option<Vec<u8>>, StoreError> {
        let store = self.clone();
        let name = name.to_owned();
        run_blocking(move || match fs::read(store.root_path(&name)?) {
            Ok(value) => Ok(Some(value)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        })
        .await
    }
    async fn cas_root(
        &self,
        name: &str,
        expected: Option<&[u8]>,
        new: &[u8],
    ) -> Result<(), StoreError> {
        let store = self.clone();
        let name = name.to_owned();
        let expected = expected.map(<[u8]>::to_vec);
        let new = new.to_vec();
        run_blocking(move || {
            let _lock = store.root_lock(&name)?;
            let path = store.root_path(&name)?;
            let actual = match fs::read(&path) {
                Ok(value) => Some(value),
                Err(error) if error.kind() == io::ErrorKind::NotFound => None,
                Err(error) => return Err(error.into()),
            };
            if actual != expected {
                return Err(StoreError::CasFailed { expected, actual });
            }
            let tmp = path.with_extension("tmp");
            fs::write(&tmp, new)?;
            fs::rename(tmp, path)?;
            Ok(())
        })
        .await
    }
    async fn delete_root(&self, name: &str) -> Result<(), StoreError> {
        let store = self.clone();
        let name = name.to_owned();
        run_blocking(move || {
            let _lock = store.root_lock(&name)?;
            match fs::remove_file(store.root_path(&name)?) {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(error.into()),
            }
        })
        .await
    }
    async fn list_roots(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        let root = self.root.clone();
        let prefix = prefix.to_owned();
        run_blocking(move || {
            let mut names = Vec::new();
            for entry in fs::read_dir(root.join("roots"))? {
                let entry = entry?;
                if !entry.file_type()?.is_file() {
                    continue;
                }
                if let Some(name) = entry.file_name().to_str() {
                    let auxiliary = Path::new(name).extension().is_some_and(|ext| {
                        ext.eq_ignore_ascii_case("lock") || ext.eq_ignore_ascii_case("tmp")
                    });
                    if name.starts_with(&prefix) && !auxiliary {
                        names.push(name.to_owned());
                    }
                }
            }
            names.sort();
            Ok(names)
        })
        .await
    }
}

fn digest(bytes: &[u8]) -> BlobId {
    BlobId(blake3::hash(bytes).to_hex().to_string())
}

/// Root-store key for a database's published index root.
#[must_use]
pub fn db_root_name(db: &str) -> String {
    format!("db:{db}")
}

/// Published durable index-root metadata, fenced by lease version
/// (see `docs/design/log-and-transactor.md`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DbRoot {
    /// Lease version of the writer that published this root.
    pub lease_version: u64,
    /// Highest indexed transaction.
    pub index_basis_t: u64,
    /// EAVT, AEVT, AVET, and VAET blob ids; `None` before the first index
    /// publication (a bare fence bump).
    pub roots: Option<[BlobId; 4]>,
}

impl DbRoot {
    /// Encodes the root for the root store.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = format!("{}\n{}\n", self.lease_version, self.index_basis_t);
        match &self.roots {
            Some(roots) => {
                for root in roots {
                    out.push_str(root.as_str());
                    out.push('\n');
                }
            }
            None => out.push_str("-\n-\n-\n-\n"),
        }
        out.into_bytes()
    }

    /// Decodes stored root bytes.
    #[must_use]
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let text = std::str::from_utf8(bytes).ok()?;
        let mut lines = text.lines();
        let lease_version = lines.next()?.parse().ok()?;
        let index_basis_t = lines.next()?.parse().ok()?;
        let ids: Vec<&str> = lines.take(4).collect();
        if ids.len() != 4 {
            return None;
        }
        let roots = if ids.iter().all(|id| *id == "-") {
            None
        } else {
            Some([
                BlobId::from_hex(ids[0])?,
                BlobId::from_hex(ids[1])?,
                BlobId::from_hex(ids[2])?,
                BlobId::from_hex(ids[3])?,
            ])
        };
        Some(Self {
            lease_version,
            index_basis_t,
            roots,
        })
    }
}

/// Result counters from a mark-and-sweep garbage collection pass.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GcReport {
    /// Number of blobs reachable from the supplied roots.
    pub marked: usize,
    /// Number of unreachable blobs deleted.
    pub swept: usize,
}

/// Marks blobs reachable from `live_roots` and deletes every unmarked blob.
///
/// `children` decodes references from each present blob. Callers are responsible for
/// supplying every currently live root and for applying any desired retention window.
///
/// # Errors
///
/// Returns an error if a blob operation or child-reference decode fails.
pub async fn mark_and_sweep(
    store: &dyn BlobStore,
    live_roots: impl IntoIterator<Item = BlobId>,
    mut children: impl FnMut(&BlobId, &[u8]) -> Result<Vec<BlobId>, StoreError>,
) -> Result<GcReport, StoreError> {
    let mut marked = HashSet::new();
    let mut pending = live_roots.into_iter().collect::<Vec<_>>();
    while let Some(id) = pending.pop() {
        if !marked.insert(id.clone()) {
            continue;
        }
        let bytes = store
            .get(&id)
            .await?
            .ok_or_else(|| StoreError::MissingBlob(id.clone()))?;
        pending.extend(children(&id, &bytes)?);
    }

    let mut swept = 0;
    let mut ids = store.list().await?;
    while let Some(id) = ids.next().await {
        let id = id?;
        if !marked.contains(&id) {
            store.delete(&id).await?;
            swept += 1;
        }
    }
    Ok(GcReport {
        marked: marked.len(),
        swept,
    })
}

struct RootLock {
    file: File,
}

impl RootLock {
    fn acquire(path: &Path) -> Result<Self, StoreError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;
        file.lock_exclusive()?;
        Ok(Self { file })
    }
}

impl Drop for RootLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
        // Keep the lock file in place so every contender locks the same inode.
        // Unlinking it here would let a new opener lock a replacement file while
        // a waiter still holds a descriptor for the unlinked original.
    }
}

/// Small read-through segment cache keyed by blob id.
#[derive(Default)]
pub struct SegmentCache {
    entries: RwLock<HashMap<BlobId, Arc<[u8]>>>,
}
impl SegmentCache {
    /// Returns cached bytes, loading from `store` on miss.
    ///
    /// # Errors
    ///
    /// Returns an error if the backing store cannot load the blob.
    ///
    /// # Panics
    ///
    /// Panics if the internal cache lock is poisoned.
    pub async fn get_or_load(
        &self,
        store: &dyn BlobStore,
        id: &BlobId,
    ) -> Result<Option<Arc<[u8]>>, StoreError> {
        if let Some(v) = self.entries.read().expect("poisoned cache lock").get(id) {
            return Ok(Some(v.clone()));
        }
        let Some(bytes) = store.get(id).await? else {
            return Ok(None);
        };
        let bytes: Arc<[u8]> = bytes.into();
        self.entries
            .write()
            .expect("poisoned cache lock")
            .insert(id.clone(), bytes.clone());
        Ok(Some(bytes))
    }
}
