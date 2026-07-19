//! Direct blob-store segment access for peers.
//!
//! Segments never travel over gRPC: peers with storage credentials read
//! published index segments straight from the blob store through a local
//! read-through cache (see `docs/design/protocol.md`). Blobs are immutable
//! and content-addressed, so cache entries never invalidate.

use std::sync::Arc;

use corium_core::IndexOrder;
use corium_store::{BlobStore, DbRoot, RootStore, SegmentCache, StoreError, db_root_name};

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
    pub fn index_root(&self, db: &str) -> Result<Option<DbRoot>, StoreError> {
        Ok(self
            .store
            .get_root(&db_root_name(db))?
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
    pub fn lease_holder_endpoint(&self, db: &str) -> Result<Option<String>, StoreError> {
        Ok(self
            .index_root(db)?
            .and_then(|root| (!root.owner_endpoint.is_empty()).then_some(root.owner_endpoint)))
    }

    /// Loads the segment for one index order of a published root, through
    /// the cache.
    ///
    /// # Errors
    /// Returns an error when the blob cannot be loaded.
    pub fn segment(
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
        self.cache.get_or_load(self.store.as_ref(), &roots[slot])
    }

    /// Decodes a segment's length-prefixed key entries.
    ///
    /// # Errors
    /// Returns [`StoreError::CorruptBlob`]-free decode failures as `None`
    /// entries never occur; malformed framing yields an error.
    pub fn segment_keys(bytes: &[u8]) -> Result<Vec<Vec<u8>>, StoreError> {
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
}
