//! Content-addressed blob and fenced root stores for immutable index segments.
//!
//! Backends are selected at runtime through the process-wide storage registry.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fmt,
    fs::{self, File, OpenOptions},
    io,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Arc, RwLock},
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use fs2::FileExt;
use thiserror::Error;
use tokio_stream::{Stream, StreamExt, wrappers::ReceiverStream};

mod segment_cache;
pub use segment_cache::{SegmentCache, SegmentCacheConfig, SegmentCacheMetrics, SegmentReader};

mod discovery;
pub use discovery::{DiscoveredStore, DiscoveredStoreSpec, StorageConnectionError};

mod registry;
pub use registry::{
    BackendCapabilities, FullStore, LogPlacement, ReadStore, StorageBackend, StorageConfig,
    StorageRegistrationError, available_storage_backends, register_storage_backend,
    register_storage_backends, storage_backend,
};

mod encrypted_store;
pub use encrypted_store::EncryptedBlobStore;

mod key_manifest;
pub use key_manifest::{
    KEY_MANIFEST_FORMAT_VERSION, KeyManifest, LOG_RECORDS_PER_EPOCH_LIMIT,
    LOG_RECORDS_PER_EPOCH_WARN, ProtectionClassKey, StorageAlgorithm, StorageKey, StorageKeyState,
    keys_root_name, load_key_manifest, publish_key_manifest,
};

mod snapshot;
pub use snapshot::{
    INDEX_MANIFEST_MAGIC, chunk_segment_keys, decode_index_manifest, decode_segment_keys,
    encode_index_manifest, encode_segment_chunk, index_blob_children, is_index_manifest,
};

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
    /// An encrypted blob references a key epoch that is not configured.
    #[error("encrypted blob requires unavailable storage-key epoch {0}")]
    MissingEncryptionKey(u32),
    /// A wrapped backend returned an id other than the digest of stored bytes.
    #[error("blob store returned id {actual}, expected {expected}")]
    BlobIdMismatch {
        /// Digest of the bytes supplied to the backend.
        expected: BlobId,
        /// Identifier returned by the backend.
        actual: BlobId,
    },
    /// Encryption, authentication, or encrypted-format failure.
    #[error("encrypted blob failed: {0}")]
    Encryption(#[from] corium_crypt::CryptError),
    /// A key could not be resolved, wrapped, or unwrapped.
    #[error("storage key unavailable: {0}")]
    Keyring(corium_crypt::KeyError),
    /// The database is encrypted and this process holds no keyring.
    #[error("database {db:?} is encrypted under key {kek}; no storage key is configured")]
    EncryptedWithoutKey {
        /// Database whose key manifest was found.
        db: String,
        /// Key-encryption key the manifest names.
        kek: String,
    },
    /// The key manifest is malformed.
    #[error("invalid key manifest: {0}")]
    InvalidKeyManifest(String),
    /// The key manifest was written by a newer release.
    #[error("key manifest format {found} is newer than supported format {supported}")]
    UnsupportedKeyManifest {
        /// Format version found in the stored manifest.
        found: u32,
        /// Newest format version this release understands.
        supported: u32,
    },
    /// The key manifest names an AEAD suite this release does not implement.
    #[error("unsupported storage encryption algorithm {0:?}")]
    UnsupportedKeyAlgorithm(String),
    /// Storage-key epochs are exhausted.
    #[error("storage key epochs are exhausted")]
    StorageEpochExhausted,
    /// Root name cannot be safely represented on the filesystem.
    #[error("invalid root name {0:?}")]
    InvalidRootName(String),
    /// A blocking store worker failed before returning its result.
    #[error("store blocking task failed: {0}")]
    BlockingTask(String),
    /// A backend kind is not registered in this process.
    #[error("storage backend {0:?} is not available; install or load its plugin")]
    BackendUnavailable(String),
    /// Backend configuration is malformed or incomplete.
    #[error("invalid storage configuration for {kind:?}: {detail}")]
    InvalidBackendConfig {
        /// Backend kind whose configuration was rejected.
        kind: String,
        /// Backend-provided diagnostic.
        detail: String,
    },
    /// A backend-specific operation failed.
    #[error("storage backend {kind:?} failed: {detail}")]
    Backend {
        /// Backend kind reporting the failure.
        kind: String,
        /// Backend-provided diagnostic.
        detail: String,
    },
    /// A local path advertised by a transactor is not reachable from this
    /// process. Direct-storage peers must be co-located with local backends.
    #[error("the transactor's local storage at {0} is not reachable from this process")]
    UnreachableLocalStorage(PathBuf),
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
    /// Stores bytes only when their content id is absent, skipping the
    /// upload for blobs the store already holds.
    ///
    /// # Errors
    ///
    /// Returns an error if the backend cannot inspect or persist the blob.
    async fn put_if_absent(&self, bytes: &[u8]) -> Result<BlobId, StoreError> {
        let id = digest(bytes);
        if self.contains(&id).await? {
            Ok(id)
        } else {
            self.put(bytes).await
        }
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
    /// Returns the blob's creation/last-modification time when available.
    /// Backends without timestamps return `None`, which conservatively keeps
    /// the blob whenever a non-zero retention window is active.
    ///
    /// # Errors
    /// Returns an error if the backend cannot inspect blob metadata.
    async fn modified_at(&self, _id: &BlobId) -> Result<Option<SystemTime>, StoreError> {
        Ok(None)
    }
}

/// A shared store is a store. Decorators such as [`EncryptedBlobStore`] own
/// what they wrap, so this is what lets one backend be wrapped per database
/// while the node keeps its own handle.
#[async_trait]
impl<S: BlobStore + ?Sized> BlobStore for Arc<S> {
    async fn put(&self, bytes: &[u8]) -> Result<BlobId, StoreError> {
        self.as_ref().put(bytes).await
    }

    async fn get(&self, id: &BlobId) -> Result<Option<Vec<u8>>, StoreError> {
        self.as_ref().get(id).await
    }

    async fn contains(&self, id: &BlobId) -> Result<bool, StoreError> {
        self.as_ref().contains(id).await
    }

    async fn put_if_absent(&self, bytes: &[u8]) -> Result<BlobId, StoreError> {
        self.as_ref().put_if_absent(bytes).await
    }

    async fn delete(&self, id: &BlobId) -> Result<(), StoreError> {
        self.as_ref().delete(id).await
    }

    async fn list(&self) -> Result<BlobIdStream, StoreError> {
        self.as_ref().list().await
    }

    async fn modified_at(&self, id: &BlobId) -> Result<Option<SystemTime>, StoreError> {
        self.as_ref().modified_at(id).await
    }
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

#[async_trait]
impl<S: RootStore + ?Sized> RootStore for Arc<S> {
    async fn get_root(&self, name: &str) -> Result<Option<Vec<u8>>, StoreError> {
        self.as_ref().get_root(name).await
    }

    async fn cas_root(
        &self,
        name: &str,
        expected: Option<&[u8]>,
        new: &[u8],
    ) -> Result<(), StoreError> {
        self.as_ref().cas_root(name, expected, new).await
    }

    async fn delete_root(&self, name: &str) -> Result<(), StoreError> {
        self.as_ref().delete_root(name).await
    }

    async fn list_roots(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        self.as_ref().list_roots(prefix).await
    }
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
    async fn modified_at(&self, id: &BlobId) -> Result<Option<SystemTime>, StoreError> {
        let store = self.clone();
        let id = id.clone();
        run_blocking(move || match fs::metadata(store.blob_path(&id)) {
            Ok(metadata) => Ok(Some(metadata.modified()?)),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        })
        .await
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

/// Computes the content id [`BlobStore::put`] would assign to `bytes`.
#[must_use]
pub fn digest(bytes: &[u8]) -> BlobId {
    BlobId(blake3::hash(bytes).to_hex().to_string())
}

/// Root-store key for a database's published index root.
#[must_use]
pub fn db_root_name(db: &str) -> String {
    format!("db:{db}")
}

/// Root-store key for a database's durable schema and naming metadata.
#[must_use]
pub fn meta_root_name(db: &str) -> String {
    format!("meta:{db}")
}

/// Storage format written by this release.
///
/// Format 2 (M7) folds the write lease into the root record so lease
/// ownership and index publication are fenced by one atomic CAS; format 1
/// roots (separate `lease:` record) decode with an unowned lease.
///
/// Format 3 publishes each covering index as a manifest blob naming
/// content-defined leaf chunks (see [`snapshot`](self::snapshot)-module
/// items such as [`chunk_segment_keys`]), so consecutive publications share
/// unchanged chunks instead of rewriting the whole index; format-2 flat
/// single-blob snapshots remain readable.
///
/// Format 4 adds `key_manifest_version`, so a reader learns a database is
/// encrypted from its root record and can refuse to open it without a storage
/// key, instead of failing later with a decode error on a blob it cannot
/// parse. Roots from formats 1-3 decode with version `0` — no manifest, no
/// encryption.
///
/// Format 5 adds four history covering-index roots. They retain assertions,
/// retractions, and superseded values (except `:db/noHistory` attributes), so
/// a snapshot-bootstrapped reader can answer retained-history views before
/// `index_basis_t` without replaying the log prefix. Older roots decode with
/// no history roots.
pub const FORMAT_VERSION: u32 = 5;

/// Published durable index-root metadata carrying the write lease
/// (see `docs/design/log-and-transactor.md`).
///
/// The lease fields and the index fields live in one record on purpose:
/// every mutation — lease acquisition, renewal, release, index publication —
/// is a CAS on these bytes, so a writer whose ownership has changed hands
/// always fails its next CAS and can never install a root. No cross-record
/// atomicity is required of the store.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DbRoot {
    /// On-disk format version. Readers reject roots from newer formats.
    pub format_version: u32,
    /// Fencing version; increments on every change of lease ownership.
    pub lease_version: u64,
    /// Owning transactor id; empty when the lease has never been acquired
    /// under format 2.
    pub owner: String,
    /// Lease expiry as Unix milliseconds; `0` when released/never held.
    pub lease_expires_unix_ms: i64,
    /// Client endpoint advertised by the owner (for peer lease-holder
    /// rediscovery); empty when the owner does not advertise one.
    pub owner_endpoint: String,
    /// Highest indexed transaction.
    pub index_basis_t: u64,
    /// EAVT, AEVT, AVET, and VAET blob ids; `None` before the first index
    /// publication (a bare fence bump).
    pub roots: Option<[BlobId; 4]>,
    /// Historical EAVT, AEVT, AVET, and VAET blob ids.
    ///
    /// These contain every retained assertion and retraction through
    /// `index_basis_t`. `None` identifies roots published before format 5 (or
    /// a bare fence bump), for which exact pre-snapshot views require log
    /// replay.
    pub history_roots: Option<[BlobId; 4]>,
    /// Next unallocated user-partition entity id as of `index_basis_t`.
    ///
    /// A transactor recovering from the index root replays only the log
    /// tail, so entities created *and* fully retracted before the snapshot
    /// carry no live datom and are invisible to it. Persisting the writer's
    /// allocation high-water lets recovery resume past those ids instead of
    /// reusing them. `0` in roots written before this field existed (and in
    /// a bare fence bump with no published snapshot); such roots force
    /// full-log replay, which reconstructs the allocator exactly.
    pub next_entity_id: u64,
    /// Largest `:db/txInstant` (Unix ms) committed through `index_basis_t`.
    ///
    /// Preserves `:db/txInstant` monotonicity across a recovery whose log
    /// tail is empty (the snapshot alone does not carry the last commit's
    /// instant). `i64::MIN` when absent, which is dominated by any real
    /// instant and so is a safe floor.
    pub last_tx_instant: i64,
    /// Generation of this database's `keys:<db>` manifest, or `0` when the
    /// database is unencrypted.
    ///
    /// Non-zero says "encrypted" before any blob is fetched, so a process
    /// without a storage key fails at open naming the manifest, rather than on
    /// a decode error deep inside a segment. The number increments whenever
    /// the manifest changes — a DEK rotation or a KEK re-wrap — so a running
    /// process notices a manifest it has not loaded and refreshes its key
    /// snapshot, instead of re-reading the manifest on every publication.
    pub key_manifest_version: u64,
}

/// Encodes a possibly empty single-line field.
fn field_line(out: &mut String, value: &str) {
    if value.is_empty() {
        out.push('-');
    } else {
        out.push_str(value);
    }
    out.push('\n');
}

fn parse_field(line: &str) -> String {
    if line == "-" {
        String::new()
    } else {
        line.to_owned()
    }
}

impl DbRoot {
    /// Encodes the root for the root store.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut out = format!(
            "corium-root-v{}\n{}\n{}\n",
            self.format_version, self.lease_version, self.index_basis_t
        );
        match &self.roots {
            Some(roots) => {
                for root in roots {
                    out.push_str(root.as_str());
                    out.push('\n');
                }
            }
            None => out.push_str("-\n-\n-\n-\n"),
        }
        field_line(&mut out, &self.owner);
        out.push_str(&self.lease_expires_unix_ms.to_string());
        out.push('\n');
        field_line(&mut out, &self.owner_endpoint);
        // Recovery hints (appended after the format-2 lease fields, so older
        // binaries that stop reading at `owner_endpoint` ignore them and this
        // stays a plain trailing extension of the same record).
        out.push_str(&self.next_entity_id.to_string());
        out.push('\n');
        out.push_str(&self.last_tx_instant.to_string());
        out.push('\n');
        out.push_str(&self.key_manifest_version.to_string());
        out.push('\n');
        match &self.history_roots {
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

    /// Decodes stored root bytes (any format up to [`FORMAT_VERSION`];
    /// newer formats still yield their fence fields so old binaries fence
    /// correctly, and callers reject them via `format_version`).
    #[must_use]
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let text = std::str::from_utf8(bytes).ok()?;
        let mut lines = text.lines();
        let first = lines.next()?;
        let (format_version, lease_version) =
            if let Some(version) = first.strip_prefix("corium-root-v") {
                let format_version = version.parse().ok()?;
                let lease_version = lines.next()?.parse().ok()?;
                (format_version, lease_version)
            } else {
                // M1-M5 roots had no header. Keep them readable as format v1 so
                // an existing database can be upgraded in place.
                (1, first.parse().ok()?)
            };
        let index_basis_t = lines.next()?.parse().ok()?;
        let ids: Vec<&str> = lines.by_ref().take(4).collect();
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
        // Lease fields; absent in format-1 roots.
        let owner = lines.next().map(parse_field).unwrap_or_default();
        let lease_expires_unix_ms = lines.next().and_then(|l| l.parse().ok()).unwrap_or(0);
        let owner_endpoint = lines.next().map(parse_field).unwrap_or_default();
        // Recovery hints; absent in roots written before index-root recovery.
        // A missing `next_entity_id` (0) signals "no hint", forcing full-log
        // replay, so the default must never look like a valid allocator id.
        let next_entity_id = lines.next().and_then(|l| l.parse().ok()).unwrap_or(0);
        let last_tx_instant = lines
            .next()
            .and_then(|l| l.parse().ok())
            .unwrap_or(i64::MIN);
        // Absent in formats 1-3, which had no key manifest and so no
        // encryption: `0` is exactly what those roots mean.
        let key_manifest_version = lines.next().and_then(|l| l.parse().ok()).unwrap_or(0);
        // Added in format 5. Absence is deliberately distinct from an empty
        // history index, which is represented by four manifest blob ids.
        let history_ids: Vec<&str> = lines.by_ref().take(4).collect();
        let history_roots = if history_ids.is_empty() && format_version < 5 {
            None
        } else if history_ids.len() != 4 {
            return None;
        } else if history_ids.iter().all(|id| *id == "-") {
            None
        } else {
            Some([
                BlobId::from_hex(history_ids[0])?,
                BlobId::from_hex(history_ids[1])?,
                BlobId::from_hex(history_ids[2])?,
                BlobId::from_hex(history_ids[3])?,
            ])
        };
        Some(Self {
            format_version,
            lease_version,
            owner,
            lease_expires_unix_ms,
            owner_endpoint,
            index_basis_t,
            roots,
            history_roots,
            next_entity_id,
            last_tx_instant,
            key_manifest_version,
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
    /// Number of unreachable blobs kept because they are inside retention.
    pub retained: usize,
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
    mark_and_sweep_retained(
        store,
        live_roots,
        &mut children,
        Duration::ZERO,
        SystemTime::now(),
    )
    .await
}

/// Marks reachable blobs and deletes only unreachable blobs older than
/// `retention` relative to `now`.
///
/// # Errors
/// Returns an error if a blob operation or child-reference decode fails.
pub async fn mark_and_sweep_retained(
    store: &dyn BlobStore,
    live_roots: impl IntoIterator<Item = BlobId>,
    mut children: impl FnMut(&BlobId, &[u8]) -> Result<Vec<BlobId>, StoreError>,
    retention: Duration,
    now: SystemTime,
) -> Result<GcReport, StoreError> {
    let mut marked = HashSet::new();
    mark_reachable(store, live_roots, &mut children, &mut marked).await?;
    sweep_unmarked(store, &marked, retention, now).await
}

/// Adds every blob reachable from `live_roots` to `marked`.
///
/// The mark walk reads blob *content* to find references, so it must run
/// through the same reader the blobs were written through — for an encrypted
/// database, that database's decrypting store. Sweeping does not: it lists,
/// stats, and deletes by id, which is why [`sweep_unmarked`] takes the raw
/// store and one marked set can be accumulated across several databases with
/// different keys.
///
/// # Errors
/// Returns an error if a blob read or child-reference decode fails.
pub async fn mark_reachable<S: std::hash::BuildHasher>(
    store: &dyn BlobStore,
    live_roots: impl IntoIterator<Item = BlobId>,
    mut children: impl FnMut(&BlobId, &[u8]) -> Result<Vec<BlobId>, StoreError>,
    marked: &mut HashSet<BlobId, S>,
) -> Result<(), StoreError> {
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
    Ok(())
}

/// Deletes every blob absent from `marked` and older than `retention`.
///
/// # Errors
/// Returns an error if a blob cannot be listed, inspected, or deleted.
pub async fn sweep_unmarked<S: std::hash::BuildHasher>(
    store: &dyn BlobStore,
    marked: &HashSet<BlobId, S>,
    retention: Duration,
    now: SystemTime,
) -> Result<GcReport, StoreError> {
    let mut swept = 0;
    let mut retained = 0;
    let mut ids = store.list().await?;
    while let Some(id) = ids.next().await {
        let id = id?;
        if !marked.contains(&id) {
            // A zero window is the explicit immediate-sweep escape hatch and
            // does not require backend timestamp support. Otherwise, unknown
            // timestamps fail safe by retaining the blob.
            let old_enough = retention.is_zero()
                || store.modified_at(&id).await?.is_some_and(|modified| {
                    now.duration_since(modified).unwrap_or_default() >= retention
                });
            if old_enough {
                store.delete(&id).await?;
                swept += 1;
            } else {
                retained += 1;
            }
        }
    }
    Ok(GcReport {
        marked: marked.len(),
        swept,
        retained,
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
