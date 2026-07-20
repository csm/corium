//! Direct blob-store segment access for peers.
//!
//! Segments never travel over gRPC: peers with storage credentials read
//! published index segments straight from the blob store through a local
//! read-through cache (see `docs/design/protocol.md`). Blobs are immutable
//! and content-addressed, so cache entries never invalidate.

use std::sync::Arc;

use async_trait::async_trait;
use corium_core::{Datom, IndexOrder, encoding::DecodeError};
use corium_db::Db;
use corium_protocol::codec::{self, CodecError};
use corium_store::{
    BlobId, BlobStore, DbRoot, FORMAT_VERSION, RootStore, SegmentCache, StoreError, db_root_name,
    decode_index_manifest, is_index_manifest, meta_root_name,
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
    let datoms = load_index_keys(store, &roots[0])
        .await?
        .into_iter()
        .map(|key| Datom::from_key(IndexOrder::Eavt, &key))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Some(Db::from_current_snapshot(
        root.index_basis_t,
        schema,
        idents,
        interner,
        datoms,
    )))
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

fn decode_segment_keys(bytes: &[u8]) -> Result<Vec<Vec<u8>>, StoreError> {
    let mut keys = Vec::new();
    let mut input = bytes;
    while !input.is_empty() {
        let (len_bytes, rest) = input
            .split_at_checked(8)
            .ok_or_else(|| StoreError::Io(std::io::Error::other("truncated segment")))?;
        let len = usize::try_from(u64::from_be_bytes(len_bytes.try_into().unwrap_or_default()))
            .map_err(|_| StoreError::Io(std::io::Error::other("segment key too large")))?;
        let (key, rest) = rest
            .split_at_checked(len)
            .ok_or_else(|| StoreError::Io(std::io::Error::other("truncated segment key")))?;
        keys.push(key.to_vec());
        input = rest;
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
    use corium_core::{EntityId, KeywordInterner, Value};
    use corium_db::Idents;
    use corium_store::{MemoryStore, RootStore};

    use super::*;

    #[tokio::test]
    async fn loads_published_eavt_snapshot() {
        let store = MemoryStore::default();
        let datom = Datom {
            e: EntityId::from_raw(1_001),
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
        assert_eq!(db.datoms(), datoms);
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
