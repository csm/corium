//! Content-addressed blob and fenced root stores for immutable index segments.

use std::{
    collections::{BTreeMap, HashMap},
    fmt,
    fs::{self, File, OpenOptions},
    io,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};

use fs2::FileExt;
use thiserror::Error;

/// A content identifier for immutable blobs.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct BlobId(String);

impl BlobId {
    /// Returns the hexadecimal digest string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
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
    /// Root name cannot be safely represented on the filesystem.
    #[error("invalid root name {0:?}")]
    InvalidRootName(String),
}

/// Immutable content-addressed blob storage.
pub trait BlobStore: Send + Sync {
    /// Stores bytes and returns their content id.
    ///
    /// # Errors
    ///
    /// Returns an error if the backend cannot persist the blob.
    fn put(&self, bytes: &[u8]) -> Result<BlobId, StoreError>;
    /// Loads bytes by id, returning `None` when missing.
    ///
    /// # Errors
    ///
    /// Returns an error if the backend cannot read or verify the blob.
    fn get(&self, id: &BlobId) -> Result<Option<Vec<u8>>, StoreError>;
}

/// Named root pointer storage with compare-and-swap fencing.
pub trait RootStore: Send + Sync {
    /// Reads a root pointer.
    ///
    /// # Errors
    ///
    /// Returns an error if the backend cannot read the root.
    fn get_root(&self, name: &str) -> Result<Option<Vec<u8>>, StoreError>;
    /// Publishes a root only if the stored pointer equals `expected`.
    ///
    /// # Errors
    ///
    /// Returns an error if the fence does not match or the backend cannot publish.
    fn cas_root(&self, name: &str, expected: Option<&[u8]>, new: &[u8]) -> Result<(), StoreError>;
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

impl BlobStore for MemoryStore {
    fn put(&self, bytes: &[u8]) -> Result<BlobId, StoreError> {
        let id = digest(bytes);
        self.inner
            .write()
            .expect("poisoned store lock")
            .blobs
            .insert(id.clone(), bytes.to_vec());
        Ok(id)
    }
    fn get(&self, id: &BlobId) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(self
            .inner
            .read()
            .expect("poisoned store lock")
            .blobs
            .get(id)
            .cloned())
    }
}
impl RootStore for MemoryStore {
    fn get_root(&self, name: &str) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(self
            .inner
            .read()
            .expect("poisoned store lock")
            .roots
            .get(name)
            .cloned())
    }
    fn cas_root(&self, name: &str, expected: Option<&[u8]>, new: &[u8]) -> Result<(), StoreError> {
        let mut inner = self.inner.write().expect("poisoned store lock");
        let actual = inner.roots.get(name).cloned();
        if actual.as_deref() != expected {
            return Err(StoreError::CasFailed {
                expected: expected.map(<[u8]>::to_vec),
                actual,
            });
        }
        inner.roots.insert(name.to_owned(), new.to_vec());
        Ok(())
    }
}

/// Filesystem-backed content-addressed blob and fenced root store.
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
        RootLock::acquire(root_path.with_extension("lock"))
    }
}
impl BlobStore for FsStore {
    fn put(&self, bytes: &[u8]) -> Result<BlobId, StoreError> {
        let id = digest(bytes);
        let path = self.blob_path(&id);
        if !path.exists() {
            let tmp = path.with_extension("tmp");
            fs::write(&tmp, bytes)?;
            fs::rename(tmp, path)?;
        }
        Ok(id)
    }
    fn get(&self, id: &BlobId) -> Result<Option<Vec<u8>>, StoreError> {
        let path = self.blob_path(id);
        if !path.exists() {
            return Ok(None);
        }
        let bytes = fs::read(path)?;
        if &digest(&bytes) != id {
            return Err(StoreError::CorruptBlob(id.clone()));
        }
        Ok(Some(bytes))
    }
}
impl RootStore for FsStore {
    fn get_root(&self, name: &str) -> Result<Option<Vec<u8>>, StoreError> {
        match fs::read(self.root_path(name)?) {
            Ok(v) => Ok(Some(v)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
    fn cas_root(&self, name: &str, expected: Option<&[u8]>, new: &[u8]) -> Result<(), StoreError> {
        let _lock = self.root_lock(name)?;
        let path = self.root_path(name)?;
        let actual = match fs::read(&path) {
            Ok(v) => Some(v),
            Err(e) if e.kind() == io::ErrorKind::NotFound => None,
            Err(e) => return Err(e.into()),
        };
        if actual.as_deref() != expected {
            return Err(StoreError::CasFailed {
                expected: expected.map(<[u8]>::to_vec),
                actual,
            });
        }
        let tmp = path.with_extension("tmp");
        fs::write(&tmp, new)?;
        fs::rename(tmp, path)?;
        Ok(())
    }
}

fn digest(bytes: &[u8]) -> BlobId {
    BlobId(blake3::hash(bytes).to_hex().to_string())
}

struct RootLock {
    path: PathBuf,
    file: File,
}

impl RootLock {
    fn acquire(path: PathBuf) -> Result<Self, StoreError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        file.lock_exclusive()?;
        Ok(Self { path, file })
    }
}

impl Drop for RootLock {
    fn drop(&mut self) {
        let _ = FileExt::unlock(&self.file);
        let _ = fs::remove_file(&self.path);
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
    pub fn get_or_load(
        &self,
        store: &dyn BlobStore,
        id: &BlobId,
    ) -> Result<Option<Arc<[u8]>>, StoreError> {
        if let Some(v) = self.entries.read().expect("poisoned cache lock").get(id) {
            return Ok(Some(v.clone()));
        }
        let Some(bytes) = store.get(id)? else {
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
