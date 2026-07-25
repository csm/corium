//! Bounded memory and optional local-disk read-through segment cache.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use fs2::FileExt;
use tokio::sync::Mutex as AsyncMutex;

use crate::{BlobId, BlobStore, StoreError, digest};
use async_trait::async_trait;

/// Minimal cache-neutral interface for immutable segment reads.
#[async_trait]
pub trait SegmentReader: Send + Sync {
    /// Reads an immutable segment from authoritative storage.
    async fn read_segment(&self, id: &BlobId) -> Result<Option<Vec<u8>>, StoreError>;
}

#[async_trait]
impl<T: BlobStore + ?Sized> SegmentReader for T {
    async fn read_segment(&self, id: &BlobId) -> Result<Option<Vec<u8>>, StoreError> {
        self.get(id).await
    }
}

/// Configuration for the peer's local segment cache.
#[derive(Clone, Debug)]
pub struct SegmentCacheConfig {
    /// Dedicated cache directory, owned by one process.
    pub directory: PathBuf,
    /// Maximum accounted bytes in the SSD tier.
    pub capacity_bytes: u64,
    /// Maximum accounted bytes in the in-process tier.
    pub memory_capacity_bytes: u64,
}

impl SegmentCacheConfig {
    /// Validates the configured capacities.
    ///
    /// # Errors
    /// Returns [`io::ErrorKind::InvalidInput`] for zero disk capacity or a
    /// memory tier larger than the disk tier.
    pub fn validate(&self) -> io::Result<()> {
        if self.capacity_bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cache capacity must be non-zero",
            ));
        }
        if self.memory_capacity_bytes > self.capacity_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "memory cache exceeds total capacity",
            ));
        }
        Ok(())
    }
}

/// Lock-free counters and gauges suitable for Prometheus adapters.
#[derive(Default)]
pub struct SegmentCacheMetrics {
    /// Memory hits.
    pub memory_hits: AtomicU64,
    /// Memory misses.
    pub memory_misses: AtomicU64,
    /// Disk hits.
    pub disk_hits: AtomicU64,
    /// Disk misses.
    pub disk_misses: AtomicU64,
    /// Native found responses.
    pub native_found: AtomicU64,
    /// Native not-found responses.
    pub native_not_found: AtomicU64,
    /// Native errors.
    pub native_errors: AtomicU64,
    /// Coalesced callers.
    pub coalesced_waiters: AtomicU64,
    /// Successful admissions.
    pub admissions: AtomicU64,
    /// Oversize admission bypasses.
    pub too_large: AtomicU64,
    /// Admission I/O failures.
    pub admission_errors: AtomicU64,
    /// Capacity evictions.
    pub evictions: AtomicU64,
    /// Bytes removed by capacity eviction.
    pub evicted_bytes: AtomicU64,
    /// Invalid cache entries discarded.
    pub corruptions: AtomicU64,
    /// Current disk bytes.
    pub disk_bytes: AtomicU64,
    /// Current disk entries.
    pub disk_entries: AtomicU64,
    /// Current memory bytes.
    pub memory_bytes: AtomicU64,
    /// Current memory entries.
    pub memory_entries: AtomicU64,
    /// Bytes returned from memory.
    pub bytes_memory: AtomicU64,
    /// Bytes returned from disk.
    pub bytes_disk: AtomicU64,
    /// Bytes returned from native storage.
    pub bytes_native: AtomicU64,
}

#[derive(Clone)]
struct Entry {
    len: u64,
    generation: u64,
}

#[derive(Default)]
struct MemoryTier {
    entries: HashMap<BlobId, Arc<[u8]>>,
    order: VecDeque<BlobId>,
    bytes: u64,
}

struct DiskTier {
    config: SegmentCacheConfig,
    _lock: File,
    entries: HashMap<BlobId, Entry>,
    generation: u64,
    bytes: u64,
}

/// Bounded read-through cache. Without a disk configuration it remains a
/// bounded in-memory cache; its default capacity is 64 MiB.
pub struct SegmentCache {
    memory_capacity: u64,
    memory: Mutex<MemoryTier>,
    disk: Option<Mutex<DiskTier>>,
    flights: AsyncMutex<HashMap<BlobId, Arc<AsyncMutex<()>>>>,
    metrics: Arc<SegmentCacheMetrics>,
}

impl Default for SegmentCache {
    fn default() -> Self {
        Self::memory_only(64 * 1024 * 1024)
    }
}

impl SegmentCache {
    /// Creates a byte-bounded memory-only cache.
    #[must_use]
    pub fn memory_only(capacity_bytes: u64) -> Self {
        Self {
            memory_capacity: capacity_bytes,
            memory: Mutex::default(),
            disk: None,
            flights: AsyncMutex::default(),
            metrics: Arc::default(),
        }
    }

    /// Opens and reconciles an SSD cache, acquiring its exclusive ownership lock.
    ///
    /// # Errors
    /// Returns an error for invalid configuration, inaccessible storage, or
    /// when another process owns the directory.
    pub fn open(config: &SegmentCacheConfig) -> io::Result<Self> {
        config.validate()?;
        fs::create_dir_all(config.directory.join("objects"))?;
        fs::create_dir_all(config.directory.join("tmp"))?;
        set_owner_only(&config.directory)?;
        set_owner_only(&config.directory.join("objects"))?;
        set_owner_only(&config.directory.join("tmp"))?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(config.directory.join("LOCK"))?;
        lock.try_lock_exclusive().map_err(|error| {
            io::Error::new(
                error.kind(),
                format!("segment cache directory is already owned: {error}"),
            )
        })?;
        for item in fs::read_dir(config.directory.join("tmp"))? {
            let item = item?;
            if item.file_type()?.is_file() {
                fs::remove_file(item.path())?;
            }
        }
        let mut tier = DiskTier {
            config: config.clone(),
            _lock: lock,
            entries: HashMap::new(),
            generation: 0,
            bytes: 0,
        };
        tier.reconcile()?;
        tier.evict_to_capacity(None)?;
        let metrics = Arc::new(SegmentCacheMetrics::default());
        metrics.disk_bytes.store(tier.bytes, Ordering::Relaxed);
        metrics
            .disk_entries
            .store(tier.entries.len() as u64, Ordering::Relaxed);
        Ok(Self {
            memory_capacity: config.memory_capacity_bytes,
            memory: Mutex::default(),
            disk: Some(Mutex::new(tier)),
            flights: AsyncMutex::default(),
            metrics,
        })
    }

    /// Returns the cache's metrics handle.
    #[must_use]
    pub fn metrics(&self) -> Arc<SegmentCacheMetrics> {
        self.metrics.clone()
    }

    /// Returns cached bytes, loading and verifying the authoritative store on miss.
    ///
    /// # Errors
    /// Returns errors from authoritative storage, including corrupt native bytes.
    pub async fn get_or_load(
        &self,
        store: &dyn SegmentReader,
        id: &BlobId,
    ) -> Result<Option<Arc<[u8]>>, StoreError> {
        if let Some(bytes) = self.memory_get(id) {
            return Ok(Some(bytes));
        }
        if let Some(bytes) = self.disk_get(id) {
            self.memory_insert(id.clone(), bytes.clone());
            return Ok(Some(bytes));
        }
        let (flight, joined) = {
            let mut flights = self.flights.lock().await;
            if let Some(f) = flights.get(id) {
                (f.clone(), true)
            } else {
                let f = Arc::new(AsyncMutex::new(()));
                flights.insert(id.clone(), f.clone());
                (f, false)
            }
        };
        if joined {
            self.metrics
                .coalesced_waiters
                .fetch_add(1, Ordering::Relaxed);
        }
        let _guard = flight.lock().await;
        if joined && let Some(bytes) = self.memory_get(id).or_else(|| self.disk_get(id)) {
            return Ok(Some(bytes));
        }
        let loaded = store.read_segment(id).await;
        self.flights.lock().await.remove(id);
        let bytes = match loaded {
            Ok(Some(bytes)) => {
                self.metrics.native_found.fetch_add(1, Ordering::Relaxed);
                bytes
            }
            Ok(None) => {
                self.metrics
                    .native_not_found
                    .fetch_add(1, Ordering::Relaxed);
                return Ok(None);
            }
            Err(error) => {
                self.metrics.native_errors.fetch_add(1, Ordering::Relaxed);
                return Err(error);
            }
        };
        if digest(&bytes) != *id {
            return Err(StoreError::CorruptBlob(id.clone()));
        }
        self.metrics
            .bytes_native
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        let bytes: Arc<[u8]> = bytes.into();
        self.disk_admit(id, &bytes);
        self.memory_insert(id.clone(), bytes.clone());
        Ok(Some(bytes))
    }

    fn memory_get(&self, id: &BlobId) -> Option<Arc<[u8]>> {
        let mut memory = self.memory.lock().expect("cache lock poisoned");
        let value = memory.entries.get(id).cloned();
        if let Some(value) = value {
            memory.order.retain(|key| key != id);
            memory.order.push_back(id.clone());
            self.metrics.memory_hits.fetch_add(1, Ordering::Relaxed);
            self.metrics
                .bytes_memory
                .fetch_add(value.len() as u64, Ordering::Relaxed);
            Some(value)
        } else {
            self.metrics.memory_misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }

    fn memory_insert(&self, id: BlobId, bytes: Arc<[u8]>) {
        if self.memory_capacity == 0 || bytes.len() as u64 > self.memory_capacity {
            return;
        }
        let mut memory = self.memory.lock().expect("cache lock poisoned");
        if let Some(old) = memory.entries.remove(&id) {
            memory.bytes -= old.len() as u64;
            memory.order.retain(|key| key != &id);
        }
        memory.bytes += bytes.len() as u64;
        memory.order.push_back(id.clone());
        memory.entries.insert(id, bytes);
        while memory.bytes > self.memory_capacity {
            if let Some(old) = memory.order.pop_front()
                && let Some(value) = memory.entries.remove(&old)
            {
                memory.bytes -= value.len() as u64;
            }
        }
        self.metrics
            .memory_bytes
            .store(memory.bytes, Ordering::Relaxed);
        self.metrics
            .memory_entries
            .store(memory.entries.len() as u64, Ordering::Relaxed);
    }

    fn disk_get(&self, id: &BlobId) -> Option<Arc<[u8]>> {
        let disk = self.disk.as_ref()?;
        let mut tier = disk.lock().expect("cache lock poisoned");
        let Some(entry) = tier.entries.get(id).cloned() else {
            self.metrics.disk_misses.fetch_add(1, Ordering::Relaxed);
            return None;
        };
        let path = tier.object_path(id);
        match fs::read(path) {
            Ok(bytes) if bytes.len() as u64 == entry.len && digest(&bytes) == *id => {
                tier.generation += 1;
                let generation = tier.generation;
                tier.entries.get_mut(id).expect("entry exists").generation = generation;
                let _ = tier.persist();
                self.metrics.disk_hits.fetch_add(1, Ordering::Relaxed);
                self.metrics
                    .bytes_disk
                    .fetch_add(entry.len, Ordering::Relaxed);
                Some(bytes.into())
            }
            _ => {
                tier.remove_corrupt(id);
                self.metrics.corruptions.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    fn disk_admit(&self, id: &BlobId, bytes: &[u8]) {
        let Some(disk) = &self.disk else {
            return;
        };
        let mut tier = disk.lock().expect("cache lock poisoned");
        if bytes.len() as u64 > tier.config.capacity_bytes {
            self.metrics.too_large.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if tier.admit(id, bytes).is_err() {
            self.metrics
                .admission_errors
                .fetch_add(1, Ordering::Relaxed);
            return;
        }
        self.metrics.admissions.fetch_add(1, Ordering::Relaxed);
        let before_bytes = tier.bytes;
        let before_entries = tier.entries.len();
        let _ = tier.evict_to_capacity(Some(id));
        self.metrics
            .evicted_bytes
            .fetch_add(before_bytes.saturating_sub(tier.bytes), Ordering::Relaxed);
        self.metrics.evictions.fetch_add(
            before_entries.saturating_sub(tier.entries.len()) as u64,
            Ordering::Relaxed,
        );
        self.metrics.disk_bytes.store(tier.bytes, Ordering::Relaxed);
        self.metrics
            .disk_entries
            .store(tier.entries.len() as u64, Ordering::Relaxed);
    }
}

impl DiskTier {
    fn object_path(&self, id: &BlobId) -> PathBuf {
        self.config
            .directory
            .join("objects")
            .join(&id.as_str()[..2])
            .join(&id.as_str()[2..])
    }
    fn reconcile(&mut self) -> io::Result<()> {
        let root = self.config.directory.join("objects");
        for fan in fs::read_dir(root)? {
            let fan = fan?;
            if !fan.file_type()?.is_dir() {
                continue;
            }
            let prefix = fan.file_name().to_string_lossy().into_owned();
            for file in fs::read_dir(fan.path())? {
                let file = file?;
                if !file.file_type()?.is_file() {
                    continue;
                }
                let text = format!("{prefix}{}", file.file_name().to_string_lossy());
                if let Some(id) = BlobId::from_hex(&text) {
                    let len = file.metadata()?.len();
                    self.generation += 1;
                    self.bytes += len;
                    self.entries.insert(
                        id,
                        Entry {
                            len,
                            generation: self.generation,
                        },
                    );
                } else {
                    fs::remove_file(file.path())?;
                }
            }
        }
        self.persist()
    }
    fn admit(&mut self, id: &BlobId, bytes: &[u8]) -> io::Result<()> {
        self.generation += 1;
        if let Some(entry) = self.entries.get_mut(id) {
            entry.generation = self.generation;
            return self.persist();
        }
        let target = self.object_path(id);
        let parent = target.parent().expect("object parent");
        fs::create_dir_all(parent)?;
        set_owner_only(parent)?;
        let temp = self
            .config
            .directory
            .join("tmp")
            .join(format!("{}-{}", id, self.generation));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)?;
        set_file_owner_only(&temp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&temp, &target)?;
        set_file_owner_only(&target)?;
        let len = bytes.len() as u64;
        self.entries.insert(
            id.clone(),
            Entry {
                len,
                generation: self.generation,
            },
        );
        self.bytes += len;
        self.persist()
    }
    fn evict_to_capacity(&mut self, protected: Option<&BlobId>) -> io::Result<()> {
        while self.bytes > self.config.capacity_bytes {
            let victim = self
                .entries
                .iter()
                .filter(|(id, _)| protected != Some(*id))
                .min_by_key(|(_, entry)| entry.generation)
                .map(|(id, _)| id.clone());
            let Some(victim) = victim else {
                break;
            };
            let entry = self.entries.get(&victim).expect("victim exists").clone();
            fs::remove_file(self.object_path(&victim))?;
            self.entries.remove(&victim);
            self.bytes -= entry.len;
        }
        self.persist()
    }
    fn remove_corrupt(&mut self, id: &BlobId) {
        if let Some(entry) = self.entries.remove(id) {
            self.bytes -= entry.len;
        }
        let _ = fs::remove_file(self.object_path(id));
        let _ = self.persist();
    }
    fn persist(&self) -> io::Result<()> {
        let mut sorted = BTreeMap::new();
        for (id, entry) in &self.entries {
            sorted.insert(id.as_str(), entry);
        }
        let temp = self.config.directory.join("index.tmp");
        let mut file = File::create(&temp)?;
        for (id, entry) in sorted {
            writeln!(file, "{id} {} {}", entry.len, entry.generation)?;
        }
        file.sync_all()?;
        fs::rename(temp, self.config.directory.join("index"))
    }
}

#[cfg(unix)]
fn set_owner_only(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
}
#[cfg(not(unix))]
fn set_owner_only(_path: &Path) -> io::Result<()> {
    Ok(())
}
#[cfg(unix)]
fn set_file_owner_only(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
}
#[cfg(not(unix))]
fn set_file_owner_only(_path: &Path) -> io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoryStore;

    fn config(path: &Path, capacity: u64) -> SegmentCacheConfig {
        SegmentCacheConfig {
            directory: path.to_owned(),
            capacity_bytes: capacity,
            memory_capacity_bytes: 0,
        }
    }

    #[tokio::test]
    async fn disk_hit_survives_reopen_without_native_read() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = MemoryStore::default();
        let id = store.put(b"cached on disk").await.expect("put");
        {
            let cache = SegmentCache::open(&config(directory.path(), 1024)).expect("open cache");
            cache.get_or_load(&store, &id).await.expect("cold read");
        }
        let empty = MemoryStore::default();
        let cache = SegmentCache::open(&config(directory.path(), 1024)).expect("reopen cache");
        assert_eq!(
            cache
                .get_or_load(&empty, &id)
                .await
                .expect("warm read")
                .as_deref(),
            Some(b"cached on disk".as_slice())
        );
        assert_eq!(cache.metrics.disk_hits.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn disk_capacity_evicts_least_recently_used_entry() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = MemoryStore::default();
        let first = store.put(b"one").await.expect("first");
        let second = store.put(b"two").await.expect("second");
        let cache = SegmentCache::open(&config(directory.path(), 3)).expect("cache");
        cache.get_or_load(&store, &first).await.expect("load first");
        cache
            .get_or_load(&store, &second)
            .await
            .expect("load second");
        assert_eq!(cache.metrics.disk_bytes.load(Ordering::Relaxed), 3);
        assert_eq!(cache.metrics.disk_entries.load(Ordering::Relaxed), 1);
        assert_eq!(cache.metrics.evictions.load(Ordering::Relaxed), 1);
        assert!(
            !cache
                .disk
                .as_ref()
                .expect("disk")
                .lock()
                .expect("lock")
                .entries
                .contains_key(&first)
        );
    }

    #[test]
    fn rejects_second_process_and_invalid_capacity() {
        let directory = tempfile::tempdir().expect("tempdir");
        let cache = SegmentCache::open(&config(directory.path(), 10)).expect("first owner");
        assert!(SegmentCache::open(&config(directory.path(), 10)).is_err());
        drop(cache);
        let mut invalid = config(directory.path(), 10);
        invalid.memory_capacity_bytes = 11;
        assert!(SegmentCache::open(&invalid).is_err());
    }
}
