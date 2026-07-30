//! Bounded memory and optional local-disk read-through segment cache.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use fs2::FileExt;
use tokio::sync::Mutex as AsyncMutex;

use crate::{BlobId, BlobStore, StoreError, digest};
use async_trait::async_trait;

const ENTRY_METADATA_BYTES: u64 = 96;

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
    /// Default size of the in-process cache tier.
    pub const DEFAULT_MEMORY_CAPACITY_BYTES: u64 = 64 * 1024 * 1024;

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

impl Entry {
    fn accounted_bytes(&self) -> u64 {
        self.len.saturating_add(ENTRY_METADATA_BYTES)
    }
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
    readers: HashMap<BlobId, usize>,
}

#[derive(Clone)]
enum FlightOutcome {
    Found(Arc<[u8]>),
    NotFound,
}

struct Flight {
    lock: AsyncMutex<()>,
    outcome: Mutex<Option<FlightOutcome>>,
}

/// Bounded read-through cache. Without a disk configuration it remains a
/// bounded in-memory cache; its default capacity is 64 MiB.
pub struct SegmentCache {
    memory_capacity: u64,
    memory: Mutex<MemoryTier>,
    disk: Option<Mutex<DiskTier>>,
    flights: Mutex<HashMap<BlobId, Weak<Flight>>>,
    metrics: Arc<SegmentCacheMetrics>,
}

impl Default for SegmentCache {
    fn default() -> Self {
        Self::memory_only(SegmentCacheConfig::DEFAULT_MEMORY_CAPACITY_BYTES)
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
            flights: Mutex::default(),
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
            readers: HashMap::new(),
        };
        tier.reconcile()?;
        tier.evict_to_limit(config.capacity_bytes, None)?;
        let metrics = Arc::new(SegmentCacheMetrics::default());
        metrics.disk_bytes.store(tier.bytes, Ordering::Relaxed);
        metrics
            .disk_entries
            .store(tier.entries.len() as u64, Ordering::Relaxed);
        Ok(Self {
            memory_capacity: config.memory_capacity_bytes,
            memory: Mutex::default(),
            disk: Some(Mutex::new(tier)),
            flights: Mutex::default(),
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
    ///
    /// # Panics
    /// Panics if an internal cache mutex was poisoned by another panicking thread.
    pub async fn get_or_load(
        &self,
        store: &dyn SegmentReader,
        id: &BlobId,
    ) -> Result<Option<Arc<[u8]>>, StoreError> {
        if let Some(bytes) = self.memory_get(id) {
            return Ok(Some(bytes));
        }
        if let Some(bytes) = self.disk_get(id).await {
            self.memory_insert(id.clone(), bytes.clone());
            return Ok(Some(bytes));
        }
        let (flight, joined) = {
            let mut flights = self.flights.lock().expect("cache lock poisoned");
            flights.retain(|_, flight| flight.strong_count() > 0);
            if let Some(flight) = flights.get(id).and_then(Weak::upgrade) {
                (flight, true)
            } else {
                let f = Arc::new(Flight {
                    lock: AsyncMutex::new(()),
                    outcome: Mutex::new(None),
                });
                flights.insert(id.clone(), Arc::downgrade(&f));
                (f, false)
            }
        };
        if joined {
            self.metrics
                .coalesced_waiters
                .fetch_add(1, Ordering::Relaxed);
        }
        let _guard = flight.lock.lock().await;
        if joined {
            match flight.outcome.lock().expect("cache lock poisoned").clone() {
                Some(FlightOutcome::Found(bytes)) => {
                    self.metrics
                        .bytes_native
                        .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                    return Ok(Some(bytes));
                }
                Some(FlightOutcome::NotFound) => return Ok(None),
                None => {}
            }
            if let Some(bytes) = self.memory_get(id) {
                return Ok(Some(bytes));
            }
            if let Some(bytes) = self.disk_get(id).await {
                return Ok(Some(bytes));
            }
        }
        let loaded = store.read_segment(id).await;
        let bytes = match loaded {
            Ok(Some(bytes)) => {
                self.metrics.native_found.fetch_add(1, Ordering::Relaxed);
                bytes
            }
            Ok(None) => {
                self.metrics
                    .native_not_found
                    .fetch_add(1, Ordering::Relaxed);
                *flight.outcome.lock().expect("cache lock poisoned") =
                    Some(FlightOutcome::NotFound);
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
        *flight.outcome.lock().expect("cache lock poisoned") =
            Some(FlightOutcome::Found(bytes.clone()));
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

    async fn disk_get(&self, id: &BlobId) -> Option<Arc<[u8]>> {
        let disk = self.disk.as_ref()?;
        let (entry, path) = {
            let mut tier = disk.lock().expect("cache lock poisoned");
            let Some(entry) = tier.entries.get(id).cloned() else {
                self.metrics.disk_misses.fetch_add(1, Ordering::Relaxed);
                return None;
            };
            let path = tier.object_path(id);
            *tier.readers.entry(id.clone()).or_default() += 1;
            (entry, path)
        };
        let read = tokio::task::spawn_blocking(move || fs::read(path)).await;
        let mut tier = disk.lock().expect("cache lock poisoned");
        if let Some(readers) = tier.readers.get_mut(id) {
            *readers -= 1;
            if *readers == 0 {
                tier.readers.remove(id);
            }
        }
        match read {
            Ok(Ok(bytes)) if bytes.len() as u64 == entry.len && digest(&bytes) == *id => {
                tier.generation += 1;
                let generation = tier.generation;
                tier.entries.get_mut(id).expect("entry exists").generation = generation;
                self.metrics.disk_hits.fetch_add(1, Ordering::Relaxed);
                self.metrics
                    .bytes_disk
                    .fetch_add(entry.len, Ordering::Relaxed);
                Some(bytes.into())
            }
            Ok(Ok(_) | Err(_)) | Err(_) => {
                if let Err(error) = tier.remove_corrupt(id) {
                    tracing::warn!(%error, segment_id = %id, "segment cache could not discard corrupt entry");
                }
                self.metrics.disk_misses.fetch_add(1, Ordering::Relaxed);
                self.metrics.corruptions.fetch_add(1, Ordering::Relaxed);
                self.metrics.disk_bytes.store(tier.bytes, Ordering::Relaxed);
                self.metrics
                    .disk_entries
                    .store(tier.entries.len() as u64, Ordering::Relaxed);
                None
            }
        }
    }

    fn disk_admit(&self, id: &BlobId, bytes: &[u8]) {
        let Some(disk) = &self.disk else {
            return;
        };
        let mut tier = disk.lock().expect("cache lock poisoned");
        let accounted = (bytes.len() as u64).saturating_add(ENTRY_METADATA_BYTES);
        if accounted > tier.config.capacity_bytes {
            self.metrics.too_large.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let already_present = tier.entries.contains_key(id);
        let evicted = if already_present {
            Vec::new()
        } else {
            let limit = tier.config.capacity_bytes - accounted;
            let before = tier
                .entries
                .iter()
                .map(|(id, entry)| (id.clone(), entry.len))
                .collect::<HashMap<_, _>>();
            match tier.evict_to_limit(limit, None) {
                Ok(evicted) => evicted,
                Err(error) => {
                    let evicted = before
                        .into_iter()
                        .filter(|(id, _)| !tier.entries.contains_key(id))
                        .collect::<Vec<_>>();
                    self.record_evictions(&evicted);
                    self.metrics.disk_bytes.store(tier.bytes, Ordering::Relaxed);
                    self.metrics
                        .disk_entries
                        .store(tier.entries.len() as u64, Ordering::Relaxed);
                    self.metrics
                        .admission_errors
                        .fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(%error, segment_id = %id, "segment cache could not make admission space");
                    return;
                }
            }
        };
        self.record_evictions(&evicted);
        if let Err(error) = tier.admit(id, bytes) {
            self.metrics
                .admission_errors
                .fetch_add(1, Ordering::Relaxed);
            tracing::warn!(%error, segment_id = %id, "segment cache admission failed");
        } else {
            self.metrics.admissions.fetch_add(1, Ordering::Relaxed);
        }
        self.metrics.disk_bytes.store(tier.bytes, Ordering::Relaxed);
        self.metrics
            .disk_entries
            .store(tier.entries.len() as u64, Ordering::Relaxed);
    }

    fn memory_remove(&self, id: &BlobId) {
        let mut memory = self.memory.lock().expect("cache lock poisoned");
        if let Some(value) = memory.entries.remove(id) {
            memory.bytes -= value.len() as u64;
            memory.order.retain(|key| key != id);
            self.metrics
                .memory_bytes
                .store(memory.bytes, Ordering::Relaxed);
            self.metrics
                .memory_entries
                .store(memory.entries.len() as u64, Ordering::Relaxed);
        }
    }

    fn record_evictions(&self, evicted: &[(BlobId, u64)]) {
        for (victim, _) in evicted {
            self.memory_remove(victim);
        }
        self.metrics
            .evictions
            .fetch_add(evicted.len() as u64, Ordering::Relaxed);
        self.metrics.evicted_bytes.fetch_add(
            evicted.iter().map(|(_, len)| len).sum::<u64>(),
            Ordering::Relaxed,
        );
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
        let index_path = self.config.directory.join("index");
        let persisted = match self.load_index() {
            Ok(entries) => entries,
            Err(error) => {
                let quarantine = self.config.directory.join(format!(
                    "index.corrupt-{}",
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_nanos()
                ));
                fs::rename(&index_path, quarantine).map_err(|rename_error| {
                    io::Error::new(
                        rename_error.kind(),
                        format!("cannot quarantine corrupt cache index ({error}): {rename_error}"),
                    )
                })?;
                HashMap::new()
            }
        };
        self.generation = persisted
            .values()
            .map(|entry| entry.generation)
            .max()
            .unwrap_or(0);
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
                    let generation = persisted.get(&id).map_or_else(
                        || {
                            self.generation += 1;
                            self.generation
                        },
                        |entry| entry.generation,
                    );
                    self.bytes += len.saturating_add(ENTRY_METADATA_BYTES);
                    self.entries.insert(id, Entry { len, generation });
                } else {
                    fs::remove_file(file.path())?;
                }
            }
        }
        self.persist()
    }

    fn load_index(&self) -> io::Result<HashMap<BlobId, Entry>> {
        let text = match fs::read_to_string(self.config.directory.join("index")) {
            Ok(text) => text,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(HashMap::new()),
            Err(error) => return Err(error),
        };
        let mut entries = HashMap::new();
        for line in text.lines() {
            let mut fields = line.split_whitespace();
            let id = fields.next().and_then(BlobId::from_hex);
            let len = fields.next().and_then(|field| field.parse::<u64>().ok());
            let generation = fields.next().and_then(|field| field.parse::<u64>().ok());
            if fields.next().is_some() || id.is_none() || len.is_none() || generation.is_none() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "malformed segment cache index",
                ));
            }
            entries.insert(
                id.expect("checked"),
                Entry {
                    len: len.expect("checked"),
                    generation: generation.expect("checked"),
                },
            );
        }
        Ok(entries)
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
        sync_directory(parent)?;
        set_file_owner_only(&target)?;
        let len = bytes.len() as u64;
        self.entries.insert(
            id.clone(),
            Entry {
                len,
                generation: self.generation,
            },
        );
        self.bytes += len.saturating_add(ENTRY_METADATA_BYTES);
        self.persist()
    }
    fn evict_to_limit(
        &mut self,
        limit: u64,
        protected: Option<&BlobId>,
    ) -> io::Result<Vec<(BlobId, u64)>> {
        let mut evicted = Vec::new();
        while self.bytes > limit {
            let victim = self
                .entries
                .iter()
                .filter(|(id, _)| protected != Some(*id) && !self.readers.contains_key(*id))
                .min_by_key(|(_, entry)| entry.generation)
                .map(|(id, _)| id.clone());
            let Some(victim) = victim else {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "all cache eviction candidates are being read",
                ));
            };
            let entry = self.entries.get(&victim).expect("victim exists").clone();
            fs::remove_file(self.object_path(&victim))?;
            self.entries.remove(&victim);
            self.bytes -= entry.accounted_bytes();
            evicted.push((victim, entry.len));
        }
        self.persist()?;
        Ok(evicted)
    }
    fn remove_corrupt(&mut self, id: &BlobId) -> io::Result<()> {
        match fs::remove_file(self.object_path(id)) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        if let Some(entry) = self.entries.remove(id) {
            self.bytes -= entry.accounted_bytes();
        }
        self.persist()
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
        fs::rename(temp, self.config.directory.join("index"))?;
        sync_directory(&self.config.directory)
    }
}

impl Drop for DiskTier {
    fn drop(&mut self) {
        if let Err(error) = self.persist() {
            tracing::warn!(%error, "segment cache could not flush access generations during shutdown");
        }
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

#[cfg(unix)]
fn sync_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}
#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> io::Result<()> {
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoryStore;
    use std::sync::atomic::AtomicUsize;
    use tokio::sync::Notify;

    struct BlockingReader {
        reads: AtomicUsize,
        release: Notify,
        bytes: Option<Vec<u8>>,
    }

    #[async_trait]
    impl SegmentReader for BlockingReader {
        async fn read_segment(&self, _id: &BlobId) -> Result<Option<Vec<u8>>, StoreError> {
            self.reads.fetch_add(1, Ordering::Relaxed);
            self.release.notified().await;
            Ok(self.bytes.clone())
        }
    }

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
        let cache = SegmentCache::open(&config(directory.path(), 99)).expect("cache");
        cache.get_or_load(&store, &first).await.expect("load first");
        cache
            .get_or_load(&store, &second)
            .await
            .expect("load second");
        assert_eq!(cache.metrics.disk_bytes.load(Ordering::Relaxed), 99);
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

    #[tokio::test]
    async fn persisted_recency_controls_eviction_after_restart() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = MemoryStore::default();
        let first = store.put(b"one").await.expect("first");
        let second = store.put(b"two").await.expect("second");
        let third = store.put(b"tri").await.expect("third");
        {
            let cache = SegmentCache::open(&config(directory.path(), 198)).expect("cache");
            cache.get_or_load(&store, &first).await.expect("first read");
            cache
                .get_or_load(&store, &second)
                .await
                .expect("second read");
            cache
                .get_or_load(&store, &first)
                .await
                .expect("refresh first");
        }
        let cache = SegmentCache::open(&config(directory.path(), 198)).expect("reopen");
        cache.get_or_load(&store, &third).await.expect("third read");
        let tier = cache.disk.as_ref().expect("disk").lock().expect("lock");
        assert!(tier.entries.contains_key(&first));
        assert!(!tier.entries.contains_key(&second));
        assert!(tier.entries.contains_key(&third));
    }

    #[tokio::test]
    async fn corrupt_disk_entry_self_heals_from_native_storage() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = MemoryStore::default();
        let id = store.put(b"healthy").await.expect("put");
        let cache = SegmentCache::open(&config(directory.path(), 1024)).expect("cache");
        cache.get_or_load(&store, &id).await.expect("cold read");
        let path = cache
            .disk
            .as_ref()
            .expect("disk")
            .lock()
            .expect("lock")
            .object_path(&id);
        fs::write(path, b"broken").expect("corrupt file");
        assert_eq!(
            cache
                .get_or_load(&store, &id)
                .await
                .expect("heal")
                .as_deref(),
            Some(b"healthy".as_slice())
        );
        assert_eq!(cache.metrics.corruptions.load(Ordering::Relaxed), 1);
        assert_eq!(cache.metrics.native_found.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn concurrent_native_misses_are_coalesced_including_not_found() {
        let reader = Arc::new(BlockingReader {
            reads: AtomicUsize::new(0),
            release: Notify::new(),
            bytes: None,
        });
        let cache = Arc::new(SegmentCache::memory_only(0));
        let id = digest(b"missing");
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let cache = Arc::clone(&cache);
            let reader = Arc::clone(&reader);
            let id = id.clone();
            tasks.push(tokio::spawn(async move {
                cache.get_or_load(reader.as_ref(), &id).await
            }));
        }
        while cache.metrics.coalesced_waiters.load(Ordering::Relaxed) < 7 {
            tokio::task::yield_now().await;
        }
        assert_eq!(reader.reads.load(Ordering::Relaxed), 1);
        reader.release.notify_waiters();
        for task in tasks {
            assert!(task.await.expect("join").expect("read").is_none());
        }
        assert_eq!(reader.reads.load(Ordering::Relaxed), 1);
        assert_eq!(cache.metrics.coalesced_waiters.load(Ordering::Relaxed), 7);
    }

    #[tokio::test]
    async fn object_larger_than_accounted_capacity_bypasses_disk() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = MemoryStore::default();
        let id = store.put(b"large").await.expect("put");
        let cache = SegmentCache::open(&config(directory.path(), 100)).expect("cache");
        assert_eq!(
            cache
                .get_or_load(&store, &id)
                .await
                .expect("read")
                .as_deref(),
            Some(b"large".as_slice())
        );
        assert_eq!(cache.metrics.too_large.load(Ordering::Relaxed), 1);
        assert_eq!(cache.metrics.disk_bytes.load(Ordering::Relaxed), 0);
        assert_eq!(cache.metrics.disk_entries.load(Ordering::Relaxed), 0);
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

    #[tokio::test]
    async fn malformed_index_is_quarantined_and_rebuilt() {
        let directory = tempfile::tempdir().expect("tempdir");
        let store = MemoryStore::default();
        let id = store.put(b"survivor").await.expect("put");
        {
            let cache = SegmentCache::open(&config(directory.path(), 1024)).expect("cache");
            cache.get_or_load(&store, &id).await.expect("read");
        }
        fs::write(directory.path().join("index"), "not an index\n").expect("break index");
        let cache = SegmentCache::open(&config(directory.path(), 1024)).expect("rebuild");
        assert!(
            cache
                .disk
                .as_ref()
                .expect("disk")
                .lock()
                .expect("lock")
                .entries
                .contains_key(&id)
        );
        assert!(
            fs::read_dir(directory.path())
                .expect("read directory")
                .any(|item| {
                    item.expect("entry")
                        .file_name()
                        .to_string_lossy()
                        .starts_with("index.corrupt-")
                })
        );
    }
}
