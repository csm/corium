use std::collections::BTreeMap;
use std::time::SystemTime;

use async_trait::async_trait;
use corium_crypt::{SecretKey, decrypt_blob, encrypt_blob, parse_blob_header};

use crate::{BlobId, BlobIdStream, BlobStore, RootStore, StoreError, digest};

/// Blob-store decorator that exposes plaintext while storing deterministic
/// authenticated ciphertext.
///
/// Blob identifiers are digests of the encrypted object. Listing, deletion,
/// timestamps, and cleartext root records pass through to the wrapped store.
/// The key set is an immutable snapshot; a manifest reload or rotation replaces
/// the decorator rather than resolving a KMS key on each blob read.
#[derive(Clone)]
pub struct EncryptedBlobStore<S> {
    inner: S,
    current_epoch: u32,
    keys: BTreeMap<u32, SecretKey>,
}

impl<S> EncryptedBlobStore<S> {
    /// Creates a decorator with all readable epochs and the epoch used by new
    /// writes.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::MissingEncryptionKey`] when `current_epoch` is not
    /// present in `keys`.
    pub fn new(
        inner: S,
        current_epoch: u32,
        keys: impl IntoIterator<Item = (u32, SecretKey)>,
    ) -> Result<Self, StoreError> {
        let keys = keys.into_iter().collect::<BTreeMap<_, _>>();
        if !keys.contains_key(&current_epoch) {
            return Err(StoreError::MissingEncryptionKey(current_epoch));
        }
        Ok(Self {
            inner,
            current_epoch,
            keys,
        })
    }

    /// Creates a single-epoch decorator.
    #[must_use]
    pub fn with_key(inner: S, epoch: u32, key: SecretKey) -> Self {
        Self {
            inner,
            current_epoch: epoch,
            keys: BTreeMap::from([(epoch, key)]),
        }
    }

    /// Returns the wrapped ciphertext store.
    #[must_use]
    pub fn inner(&self) -> &S {
        &self.inner
    }

    /// Consumes the decorator and returns the wrapped store.
    #[must_use]
    pub fn into_inner(self) -> S {
        self.inner
    }

    /// Returns the epoch used for new writes.
    #[must_use]
    pub fn current_epoch(&self) -> u32 {
        self.current_epoch
    }

    fn current_key(&self) -> &SecretKey {
        // Constructors establish this invariant, and there is no key mutation.
        self.keys
            .get(&self.current_epoch)
            .expect("current encryption epoch must have key material")
    }

    fn key(&self, epoch: u32) -> Result<&SecretKey, StoreError> {
        self.keys
            .get(&epoch)
            .ok_or(StoreError::MissingEncryptionKey(epoch))
    }
}

#[async_trait]
impl<S: BlobStore> BlobStore for EncryptedBlobStore<S> {
    async fn put(&self, bytes: &[u8]) -> Result<BlobId, StoreError> {
        let encrypted = encrypt_blob(self.current_key(), self.current_epoch, bytes)?;
        let expected = digest(&encrypted);
        let actual = self.inner.put(&encrypted).await?;
        if actual != expected {
            return Err(StoreError::BlobIdMismatch { expected, actual });
        }
        Ok(actual)
    }

    async fn get(&self, id: &BlobId) -> Result<Option<Vec<u8>>, StoreError> {
        let Some(encrypted) = self.inner.get(id).await? else {
            return Ok(None);
        };
        if digest(&encrypted) != *id {
            return Err(StoreError::CorruptBlob(id.clone()));
        }
        let header = parse_blob_header(&encrypted)?;
        let plaintext = decrypt_blob(self.key(header.epoch)?, &encrypted)?;
        Ok(Some(plaintext))
    }

    async fn contains(&self, id: &BlobId) -> Result<bool, StoreError> {
        self.inner.contains(id).await
    }

    async fn put_if_absent(&self, bytes: &[u8]) -> Result<BlobId, StoreError> {
        let encrypted = encrypt_blob(self.current_key(), self.current_epoch, bytes)?;
        let expected = digest(&encrypted);
        if self.inner.contains(&expected).await? {
            return Ok(expected);
        }
        // Another writer may store the same object after this check. That race
        // is benign because deterministic encryption gives both writers the
        // same bytes and content id, and BlobStore::put is idempotent.
        let actual = self.inner.put(&encrypted).await?;
        if actual != expected {
            return Err(StoreError::BlobIdMismatch { expected, actual });
        }
        Ok(actual)
    }

    async fn delete(&self, id: &BlobId) -> Result<(), StoreError> {
        self.inner.delete(id).await
    }

    async fn list(&self) -> Result<BlobIdStream, StoreError> {
        self.inner.list().await
    }

    async fn modified_at(&self, id: &BlobId) -> Result<Option<SystemTime>, StoreError> {
        self.inner.modified_at(id).await
    }
}

#[async_trait]
impl<S: RootStore> RootStore for EncryptedBlobStore<S> {
    async fn get_root(&self, name: &str) -> Result<Option<Vec<u8>>, StoreError> {
        self.inner.get_root(name).await
    }

    async fn cas_root(
        &self,
        name: &str,
        expected: Option<&[u8]>,
        new: &[u8],
    ) -> Result<(), StoreError> {
        self.inner.cas_root(name, expected, new).await
    }

    async fn delete_root(&self, name: &str) -> Result<(), StoreError> {
        self.inner.delete_root(name).await
    }

    async fn list_roots(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        self.inner.list_roots(prefix).await
    }
}
