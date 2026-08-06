//! Direct blob-store segment access for peers.
//!
//! Segments never travel over gRPC: peers with storage credentials read
//! published index segments straight from the blob store through a local
//! read-through cache (see `docs/design/protocol.md`). Blobs are immutable
//! and content-addressed, so cache entries never invalidate.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use corium_core::{Datom, IndexOrder, encoding::DecodeError};
use corium_crypt::{Keyring, SecretKey, decrypt_blob, parse_blob_header};
use corium_db::Db;
use corium_protocol::codec::{self, CodecError};
use corium_store::{
    BlobId, BlobStore, DbRoot, DiscoveredStore, FORMAT_VERSION, KeyManifest, RootStore,
    SegmentCache, SegmentCacheConfig, SegmentCacheMetrics, SegmentReader, StoreError, db_root_name,
    decode_index_manifest, decode_segment_keys, digest, is_index_manifest, keys_root_name,
    meta_root_name,
};
use thiserror::Error;

/// Read-only storage operations needed by a storage-aware peer.
///
/// The separate trait makes a backend that implements both [`BlobStore`] and
/// [`RootStore`] usable as one trait object in [`crate::ConnectConfig`].
#[async_trait]
pub trait PeerStorage: Send + Sync {
    /// Loads one immutable blob.
    async fn get_blob(&self, id: &BlobId) -> Result<Option<Vec<u8>>, StoreError>;
    /// Loads one named root record.
    async fn get_peer_root(&self, name: &str) -> Result<Option<Vec<u8>>, StoreError>;
}

/// Storage decorator that caches blob reads while always delegating roots.
pub struct CachedPeerStorage {
    storage: Arc<dyn PeerStorage>,
    cache: SegmentCache,
}

impl CachedPeerStorage {
    /// Opens a cache around peer storage.
    ///
    /// # Errors
    /// Returns an error for invalid configuration, inaccessible storage, or
    /// an already-owned cache directory.
    pub fn open(
        storage: Arc<dyn PeerStorage>,
        config: &SegmentCacheConfig,
    ) -> std::io::Result<Self> {
        Ok(Self {
            storage,
            cache: SegmentCache::open(config)?,
        })
    }

    /// Returns cache counters for a metrics adapter.
    #[must_use]
    pub fn metrics(&self) -> Arc<SegmentCacheMetrics> {
        self.cache.metrics()
    }
}

#[async_trait]
impl SegmentReader for CachedPeerStorage {
    async fn read_segment(&self, id: &BlobId) -> Result<Option<Vec<u8>>, StoreError> {
        self.storage.get_blob(id).await
    }
}

#[async_trait]
impl PeerStorage for CachedPeerStorage {
    async fn get_blob(&self, id: &BlobId) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(self
            .cache
            .get_or_load(self, id)
            .await?
            .map(|bytes| bytes.to_vec()))
    }
    async fn get_peer_root(&self, name: &str) -> Result<Option<Vec<u8>>, StoreError> {
        self.storage.get_peer_root(name).await
    }
}

#[async_trait]
impl<S> PeerStorage for S
where
    S: BlobStore + RootStore + Send + Sync,
{
    async fn get_blob(&self, id: &BlobId) -> Result<Option<Vec<u8>>, StoreError> {
        BlobStore::get(self, id).await
    }

    async fn get_peer_root(&self, name: &str) -> Result<Option<Vec<u8>>, StoreError> {
        RootStore::get_root(self, name).await
    }
}

/// Storage decorator that decrypts blobs for a peer reading an encrypted
/// database directly.
///
/// It sits *above* the segment cache, so the SSD tier holds ciphertext and its
/// existing digest check is unchanged — a cache directory on a shared host is
/// then no more revealing than the object store it mirrors. Root records pass
/// through: they are cleartext everywhere.
pub struct EncryptedPeerStorage {
    storage: Arc<dyn PeerStorage>,
    keys: BTreeMap<u32, SecretKey>,
}

impl EncryptedPeerStorage {
    /// Wraps peer storage with an unwrapped storage-key snapshot.
    ///
    /// The snapshot covers every epoch the manifest carries, because a
    /// published index may still name leaves written under an epoch that has
    /// since been retired.
    #[must_use]
    pub fn new(storage: Arc<dyn PeerStorage>, keys: BTreeMap<u32, SecretKey>) -> Self {
        Self { storage, keys }
    }
}

#[async_trait]
impl PeerStorage for EncryptedPeerStorage {
    async fn get_blob(&self, id: &BlobId) -> Result<Option<Vec<u8>>, StoreError> {
        let Some(object) = self.storage.get_blob(id).await? else {
            return Ok(None);
        };
        if digest(&object) != *id {
            return Err(StoreError::CorruptBlob(id.clone()));
        }
        let header = parse_blob_header(&object)?;
        let key = self
            .keys
            .get(&header.epoch)
            .ok_or(StoreError::MissingEncryptionKey(header.epoch))?;
        Ok(Some(decrypt_blob(key, &object)?))
    }

    async fn get_peer_root(&self, name: &str) -> Result<Option<Vec<u8>>, StoreError> {
        self.storage.get_peer_root(name).await
    }
}

/// Resolves the storage keys `db` needs and wraps `storage` when it is
/// encrypted.
///
/// Returns `storage` unchanged for an unencrypted database, so a peer that
/// holds no keyring keeps working exactly as before against one.
///
/// # Errors
/// Returns [`StoreError`] when the manifest cannot be read, when the database
/// is encrypted and `keyring` is absent, or when an epoch cannot be unwrapped.
pub async fn open_encrypted_storage(
    storage: Arc<dyn PeerStorage>,
    db: &str,
    keyring: Option<&Arc<dyn Keyring>>,
) -> Result<Arc<dyn PeerStorage>, StoreError> {
    let Some(bytes) = storage.get_peer_root(&keys_root_name(db)).await? else {
        return Ok(storage);
    };
    let manifest = KeyManifest::decode(&bytes)?;
    if manifest.storage_keys.is_empty() {
        return Ok(storage);
    }
    let Some(keyring) = keyring else {
        return Err(StoreError::EncryptedWithoutKey {
            db: db.to_owned(),
            kek: manifest.kek.to_string(),
        });
    };
    let keys = manifest.unwrap_storage_keys(keyring.as_ref()).await?;
    Ok(Arc::new(EncryptedPeerStorage::new(storage, keys)))
}

/// Read-only peer adapter for storage discovered through a transactor.
pub struct DiscoveredPeerStorage(DiscoveredStore);

impl DiscoveredPeerStorage {
    /// Wraps a discovered store for peer snapshot reads.
    #[must_use]
    pub const fn new(store: DiscoveredStore) -> Self {
        Self(store)
    }
}

#[async_trait]
impl PeerStorage for DiscoveredPeerStorage {
    async fn get_blob(&self, id: &BlobId) -> Result<Option<Vec<u8>>, StoreError> {
        self.0.get(id).await
    }

    async fn get_peer_root(&self, name: &str) -> Result<Option<Vec<u8>>, StoreError> {
        self.0.get_root(name).await
    }
}

/// Failure while bootstrapping a peer from published storage.
#[derive(Debug, Error)]
pub enum SnapshotError {
    /// Storage read failed.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// Durable schema/naming metadata was malformed.
    #[error(transparent)]
    Codec(#[from] CodecError),
    /// A covering-index key was malformed.
    #[error(transparent)]
    Key(#[from] DecodeError),
    /// A root record was present but malformed.
    #[error("malformed published root for database {0:?}")]
    MalformedRoot(String),
    /// The root uses a newer storage format than this peer understands.
    #[error("storage format {found} is newer than supported format {supported}")]
    UnsupportedFormat {
        /// Version found in storage.
        found: u32,
        /// Newest version understood by this peer.
        supported: u32,
    },
    /// An indexed snapshot had no matching durable metadata.
    #[error("published snapshot for database {0:?} has no metadata root")]
    MissingMetadata(String),
}

/// Loads the newest published current-state snapshot for `db`.
///
/// `None` means the database has not published an index yet, in which case a
/// peer must subscribe from basis zero. Immutable segment reads can race with
/// later publications safely because the root selects a complete snapshot.
///
/// # Errors
/// Returns [`SnapshotError`] for corrupt or unsupported published state.
pub async fn load_current_snapshot(
    store: &dyn PeerStorage,
    db: &str,
) -> Result<Option<Db>, SnapshotError> {
    let Some(root_bytes) = store.get_peer_root(&db_root_name(db)).await? else {
        return Ok(None);
    };
    let root =
        DbRoot::decode(&root_bytes).ok_or_else(|| SnapshotError::MalformedRoot(db.into()))?;
    if root.format_version > FORMAT_VERSION {
        return Err(SnapshotError::UnsupportedFormat {
            found: root.format_version,
            supported: FORMAT_VERSION,
        });
    }
    let Some(roots) = root.roots else {
        return Ok(None);
    };
    let Some(metadata) = store.get_peer_root(&meta_root_name(db)).await? else {
        return Err(SnapshotError::MissingMetadata(db.into()));
    };
    let (schema, idents, interner) = codec::decode_metadata(&metadata)?;
    let current = load_index_keys(store, &roots[0])
        .await?
        .into_iter()
        .map(|key| Datom::from_key(IndexOrder::Eavt, &key))
        .collect::<Result<Vec<_>, _>>()?;
    if let Some([history_eavt, ..]) = root.history_roots {
        let history = load_index_keys(store, &history_eavt)
            .await?
            .into_iter()
            .map(|key| Datom::from_key(IndexOrder::Eavt, &key))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Some(Db::from_history_snapshot_with_next_user(
            root.index_basis_t,
            root.next_entity_id,
            schema,
            idents,
            interner,
            history,
            current,
        )))
    } else {
        Ok(Some(Db::from_current_snapshot_with_next_user(
            root.index_basis_t,
            root.next_entity_id,
            schema,
            idents,
            interner,
            current,
        )))
    }
}

/// Loads one covering index's sorted keys: a format-3 manifest's chunks in
/// order, or a pre-format-3 flat key stream.
async fn load_index_keys(store: &dyn PeerStorage, id: &BlobId) -> Result<Vec<Vec<u8>>, StoreError> {
    let blob = store
        .get_blob(id)
        .await?
        .ok_or_else(|| StoreError::MissingBlob(id.clone()))?;
    if !is_index_manifest(&blob) {
        return decode_segment_keys(&blob);
    }
    let mut keys = Vec::new();
    for child in decode_index_manifest(&blob)? {
        let chunk = store
            .get_blob(&child)
            .await?
            .ok_or_else(|| StoreError::MissingBlob(child.clone()))?;
        keys.extend(decode_segment_keys(&chunk)?);
    }
    Ok(keys)
}

/// Read-through segment source over a blob/root store.
pub struct SegmentSource<S> {
    store: Arc<S>,
    cache: SegmentCache,
}

impl<S: BlobStore + RootStore> SegmentSource<S> {
    /// Wraps a store with an empty cache.
    #[must_use]
    pub fn new(store: Arc<S>) -> Self {
        Self {
            store,
            cache: SegmentCache::default(),
        }
    }

    /// Reads the current published index root for `db`.
    ///
    /// # Errors
    /// Returns an error when the root store cannot be read.
    pub async fn index_root(&self, db: &str) -> Result<Option<DbRoot>, StoreError> {
        Ok(self
            .store
            .get_root(&db_root_name(db))
            .await?
            .as_deref()
            .and_then(DbRoot::decode))
    }

    /// Rediscovers the current lease holder's advertised client endpoint
    /// from the root record — peers with storage credentials can rebuild
    /// their endpoint preference after an HA takeover without any static
    /// configuration.
    ///
    /// # Errors
    /// Returns an error when the root store cannot be read.
    pub async fn lease_holder_endpoint(&self, db: &str) -> Result<Option<String>, StoreError> {
        Ok(self
            .index_root(db)
            .await?
            .and_then(|root| (!root.owner_endpoint.is_empty()).then_some(root.owner_endpoint)))
    }

    /// Loads the full key stream for one index order of a published root,
    /// through the cache: a format-3 manifest's chunks concatenated in
    /// order, or a pre-format-3 flat segment as stored.
    ///
    /// # Errors
    /// Returns an error when a blob cannot be loaded or is missing.
    pub async fn segment(
        &self,
        root: &DbRoot,
        order: IndexOrder,
    ) -> Result<Option<Arc<[u8]>>, StoreError> {
        let Some(roots) = &root.roots else {
            return Ok(None);
        };
        let slot = match order {
            IndexOrder::Eavt => 0,
            IndexOrder::Aevt => 1,
            IndexOrder::Avet => 2,
            IndexOrder::Vaet => 3,
        };
        let Some(blob) = self
            .cache
            .get_or_load(self.store.as_ref(), &roots[slot])
            .await?
        else {
            return Ok(None);
        };
        if !is_index_manifest(&blob) {
            return Ok(Some(blob));
        }
        let mut bytes = Vec::new();
        for child in decode_index_manifest(&blob)? {
            let chunk = self
                .cache
                .get_or_load(self.store.as_ref(), &child)
                .await?
                .ok_or_else(|| StoreError::MissingBlob(child.clone()))?;
            bytes.extend_from_slice(&chunk);
        }
        Ok(Some(bytes.into()))
    }

    /// Decodes a segment's length-prefixed key entries.
    ///
    /// # Errors
    /// Returns [`StoreError::CorruptBlob`]-free decode failures as `None`
    /// entries never occur; malformed framing yields an error.
    pub fn segment_keys(bytes: &[u8]) -> Result<Vec<Vec<u8>>, StoreError> {
        decode_segment_keys(bytes)
    }
}

#[cfg(test)]
mod tests {
    use corium_core::{EntityId, KeywordInterner, Partition, Value};
    use corium_db::Idents;
    use corium_store::{MemoryStore, RootStore};

    use super::*;

    #[tokio::test]
    async fn loads_published_eavt_snapshot() {
        let store = MemoryStore::default();
        let datom = Datom {
            e: EntityId::new(Partition::User as u32, 1_001),
            a: EntityId::from_raw(101),
            v: Value::Str("snapshot".into()),
            tx: EntityId::from_raw(37),
            added: true,
        };
        let key = datom.key(IndexOrder::Eavt);
        let mut segment = Vec::new();
        segment.extend_from_slice(&(key.len() as u64).to_be_bytes());
        segment.extend_from_slice(&key);
        let id = store.put(&segment).await.expect("put segment");
        let root = DbRoot {
            format_version: FORMAT_VERSION,
            lease_version: 1,
            owner: "test".into(),
            lease_expires_unix_ms: 0,
            owner_endpoint: String::new(),
            index_basis_t: 37,
            roots: Some([id.clone(), id.clone(), id.clone(), id]),
            history_roots: None,
            // Legacy roots did not persist an allocator hint.
            next_entity_id: 0,
            last_tx_instant: 0,
            key_manifest_version: 0,
        };
        RootStore::cas_root(&store, &db_root_name("music"), None, &root.encode())
            .await
            .expect("put root");
        let metadata = codec::encode_metadata(
            &corium_core::Schema::default(),
            &Idents::default(),
            &KeywordInterner::default(),
        );
        RootStore::cas_root(&store, &meta_root_name("music"), None, &metadata)
            .await
            .expect("put metadata");

        let db = load_current_snapshot(&store, "music")
            .await
            .expect("load snapshot")
            .expect("published snapshot");
        assert_eq!(db.basis_t(), 37);
        assert_eq!(db.next_user_sequence(), 1_002);
        assert_eq!(db.datoms(), vec![datom]);
    }

    #[tokio::test]
    async fn loads_chunked_manifest_snapshot() {
        let store = MemoryStore::default();
        let datoms: Vec<Datom> = (0..4u64)
            .map(|n| Datom {
                e: EntityId::from_raw(1_001 + n),
                a: EntityId::from_raw(101),
                v: Value::Long(i64::try_from(n).unwrap()),
                tx: EntityId::from_raw(37),
                added: true,
            })
            .collect();
        // Two chunks of two keys each, under one manifest per index.
        let mut chunk_ids = Vec::new();
        for pair in datoms.chunks(2) {
            let mut chunk = Vec::new();
            for datom in pair {
                let key = datom.key(IndexOrder::Eavt);
                chunk.extend_from_slice(&(key.len() as u64).to_be_bytes());
                chunk.extend_from_slice(&key);
            }
            chunk_ids.push(store.put(&chunk).await.expect("put chunk"));
        }
        let manifest = corium_store::encode_index_manifest(&chunk_ids);
        let id = store.put(&manifest).await.expect("put manifest");
        let root = DbRoot {
            format_version: FORMAT_VERSION,
            lease_version: 1,
            owner: "test".into(),
            lease_expires_unix_ms: 0,
            owner_endpoint: String::new(),
            index_basis_t: 37,
            roots: Some([id.clone(), id.clone(), id.clone(), id]),
            history_roots: None,
            next_entity_id: 1_005,
            last_tx_instant: 0,
            key_manifest_version: 0,
        };
        RootStore::cas_root(&store, &db_root_name("music"), None, &root.encode())
            .await
            .expect("put root");
        let metadata = codec::encode_metadata(
            &corium_core::Schema::default(),
            &Idents::default(),
            &KeywordInterner::default(),
        );
        RootStore::cas_root(&store, &meta_root_name("music"), None, &metadata)
            .await
            .expect("put metadata");

        let db = load_current_snapshot(&store, "music")
            .await
            .expect("load snapshot")
            .expect("published snapshot");
        assert_eq!(db.basis_t(), 37);
        assert_eq!(db.next_user_sequence(), 1_005);
        assert_eq!(db.datoms(), datoms);
    }

    #[tokio::test]
    async fn loads_published_history_for_pre_snapshot_views() {
        let store = MemoryStore::default();
        let asserted = Datom {
            e: EntityId::from_raw(1_001),
            a: EntityId::from_raw(101),
            v: Value::Str("past".into()),
            tx: EntityId::from_raw(1),
            added: true,
        };
        let retracted = Datom {
            tx: EntityId::from_raw(2),
            added: false,
            ..asserted.clone()
        };
        let current_manifest = corium_store::encode_index_manifest(&[]);
        let current_id = store
            .put(&current_manifest)
            .await
            .expect("current manifest");
        let history_chunk = corium_store::encode_segment_chunk(
            [&asserted, &retracted]
                .into_iter()
                .map(|datom| datom.key(IndexOrder::Eavt))
                .collect::<Vec<_>>()
                .iter()
                .map(Vec::as_slice),
        );
        let history_chunk_id = store.put(&history_chunk).await.expect("history chunk");
        let history_manifest = corium_store::encode_index_manifest(&[history_chunk_id]);
        let history_id = store
            .put(&history_manifest)
            .await
            .expect("history manifest");
        let root = DbRoot {
            format_version: FORMAT_VERSION,
            lease_version: 1,
            owner: "test".into(),
            lease_expires_unix_ms: 0,
            owner_endpoint: String::new(),
            index_basis_t: 2,
            roots: Some(std::array::from_fn(|_| current_id.clone())),
            history_roots: Some(std::array::from_fn(|_| history_id.clone())),
            next_entity_id: 1_002,
            last_tx_instant: 0,
            key_manifest_version: 0,
        };
        RootStore::cas_root(&store, &db_root_name("history"), None, &root.encode())
            .await
            .expect("put root");
        let metadata = codec::encode_metadata(
            &corium_core::Schema::default(),
            &Idents::default(),
            &KeywordInterner::default(),
        );
        RootStore::cas_root(&store, &meta_root_name("history"), None, &metadata)
            .await
            .expect("put metadata");

        let db = load_current_snapshot(&store, "history")
            .await
            .expect("load snapshot")
            .expect("published snapshot");
        assert!(db.has_complete_history());
        assert!(db.datoms().is_empty());
        assert_eq!(db.as_of(1).datoms(), vec![asserted.clone()]);
        assert_eq!(db.history().datoms(), vec![asserted, retracted]);
    }

    #[tokio::test]
    async fn absent_publication_falls_back_to_log_replay() {
        assert!(
            load_current_snapshot(&MemoryStore::default(), "music")
                .await
                .expect("load snapshot")
                .is_none()
        );
    }
}
