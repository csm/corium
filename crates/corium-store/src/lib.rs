#![allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]

//! Content-addressed blob and fenced root stores for immutable index segments.

use std::{
    collections::{BTreeMap, HashMap},
    fs, io,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
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
}

/// Errors raised by store implementations.
#[derive(Debug)]
pub enum StoreError {
    /// I/O failure.
    Io(io::Error),
    /// Root compare-and-swap failed because the current fence differed.
    CasFailed {
        /// Expected root bytes supplied by the caller.
        expected: Option<Vec<u8>>,
        /// Actual root bytes currently stored.
        actual: Option<Vec<u8>>,
    },
    /// Blob digest did not match its content.
    CorruptBlob(BlobId),
}

impl From<io::Error> for StoreError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

/// Immutable content-addressed blob storage.
pub trait BlobStore: Send + Sync {
    /// Stores bytes and returns their content id.
    fn put(&self, bytes: &[u8]) -> Result<BlobId, StoreError>;
    /// Loads bytes by id, returning `None` when missing.
    fn get(&self, id: &BlobId) -> Result<Option<Vec<u8>>, StoreError>;
}

/// Named root pointer storage with compare-and-swap fencing.
pub trait RootStore: Send + Sync {
    /// Reads a root pointer.
    fn get_root(&self, name: &str) -> Result<Option<Vec<u8>>, StoreError>;
    /// Publishes a root only if the stored pointer equals `expected`.
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
    pub fn open(root: impl AsRef<Path>) -> Result<Self, StoreError> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(root.join("blobs"))?;
        fs::create_dir_all(root.join("roots"))?;
        Ok(Self { root })
    }
    fn blob_path(&self, id: &BlobId) -> PathBuf {
        self.root.join("blobs").join(id.as_str())
    }
    fn root_path(&self, name: &str) -> PathBuf {
        self.root.join("roots").join(name.replace('/', "_"))
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
        match fs::read(self.root_path(name)) {
            Ok(v) => Ok(Some(v)),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
    fn cas_root(&self, name: &str, expected: Option<&[u8]>, new: &[u8]) -> Result<(), StoreError> {
        let actual = self.get_root(name)?;
        if actual.as_deref() != expected {
            return Err(StoreError::CasFailed {
                expected: expected.map(<[u8]>::to_vec),
                actual,
            });
        }
        let path = self.root_path(name);
        let tmp = path.with_extension("tmp");
        fs::write(&tmp, new)?;
        fs::rename(tmp, path)?;
        Ok(())
    }
}

fn digest(bytes: &[u8]) -> BlobId {
    // FNV-1a based 128-bit content id: deterministic and dependency-free for M1 scaffolding.
    let mut a = 0xcbf2_9ce4_8422_2325u64;
    let mut b = 0x8422_2325_cbf2_9ce4u64;
    for &byte in bytes {
        a ^= u64::from(byte);
        a = a.wrapping_mul(0x100_0000_01b3);
        b ^= a.rotate_left(13);
        b = b.wrapping_mul(0x9e37_79b1_85eb_ca87);
    }
    BlobId(format!("{a:016x}{b:016x}"))
}

/// Small read-through segment cache keyed by blob id.
#[derive(Default)]
pub struct SegmentCache {
    entries: RwLock<HashMap<BlobId, Arc<[u8]>>>,
}
impl SegmentCache {
    /// Returns cached bytes, loading from `store` on miss.
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
