//! Per-database storage encryption.
//!
//! Encryption is a property of one database, and the node's storage service is
//! shared by all of them, so every database that has a key manifest gets its
//! own view of that service: an [`EncryptedBlobStore`] decorator over the
//! shared backend and a [`LogCipher`] over its transaction log. A database
//! with no manifest keeps reading and writing plaintext, forever — that is
//! fixed at creation and is what keeps existing databases working untouched.
//!
//! Keys are unwrapped here, at open and at rotation, and never on a blob read
//! or a log append: everything below holds an already-unwrapped snapshot.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use corium_crypt::{KeyId, Keyring, SecretKey};
use corium_log::{LogCipher, LogError};
use corium_store::{
    BlobId, BlobIdStream, BlobStore, EncryptedBlobStore, KeyManifest, RootStore, StoreError,
};

use crate::backend::NodeStore;

/// One database's view of the node's storage service.
///
/// Root records are cleartext under both arms — they hold no user data, and
/// `RootStore::compare_and_set` compares bytes — so only blob access differs.
pub enum DbStore {
    /// An unencrypted database: the shared backend, unchanged.
    Plain(Arc<NodeStore>),
    /// An encrypted database: plaintext in, deterministic ciphertext out,
    /// blob ids the digests of the stored objects.
    Encrypted(EncryptedBlobStore<Arc<NodeStore>>),
}

impl DbStore {
    /// The undecorated backend, for operations that work on stored bytes:
    /// listing, deletion, timestamps, and root records.
    #[must_use]
    pub fn raw(&self) -> &Arc<NodeStore> {
        match self {
            Self::Plain(store) => store,
            Self::Encrypted(store) => store.inner(),
        }
    }

    /// The storage-key epoch new blobs are written under, when encrypted.
    #[must_use]
    pub fn storage_epoch(&self) -> Option<u32> {
        match self {
            Self::Plain(_) => None,
            Self::Encrypted(store) => Some(store.current_epoch()),
        }
    }
}

#[async_trait]
impl BlobStore for DbStore {
    async fn put(&self, bytes: &[u8]) -> Result<BlobId, StoreError> {
        match self {
            Self::Plain(store) => store.put(bytes).await,
            Self::Encrypted(store) => store.put(bytes).await,
        }
    }

    async fn get(&self, id: &BlobId) -> Result<Option<Vec<u8>>, StoreError> {
        match self {
            Self::Plain(store) => store.get(id).await,
            Self::Encrypted(store) => store.get(id).await,
        }
    }

    async fn contains(&self, id: &BlobId) -> Result<bool, StoreError> {
        match self {
            Self::Plain(store) => store.contains(id).await,
            Self::Encrypted(store) => store.contains(id).await,
        }
    }

    async fn put_if_absent(&self, bytes: &[u8]) -> Result<BlobId, StoreError> {
        match self {
            Self::Plain(store) => store.put_if_absent(bytes).await,
            Self::Encrypted(store) => store.put_if_absent(bytes).await,
        }
    }

    async fn delete(&self, id: &BlobId) -> Result<(), StoreError> {
        self.raw().delete(id).await
    }

    async fn list(&self) -> Result<BlobIdStream, StoreError> {
        self.raw().list().await
    }

    async fn modified_at(&self, id: &BlobId) -> Result<Option<SystemTime>, StoreError> {
        self.raw().modified_at(id).await
    }
}

#[async_trait]
impl RootStore for DbStore {
    async fn get_root(&self, name: &str) -> Result<Option<Vec<u8>>, StoreError> {
        self.raw().get_root(name).await
    }

    async fn cas_root(
        &self,
        name: &str,
        expected: Option<&[u8]>,
        new: &[u8],
    ) -> Result<(), StoreError> {
        self.raw().cas_root(name, expected, new).await
    }

    async fn delete_root(&self, name: &str) -> Result<(), StoreError> {
        self.raw().delete_root(name).await
    }

    async fn list_roots(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        self.raw().list_roots(prefix).await
    }
}

/// A database's resolved storage-encryption state.
pub struct DbCrypto {
    /// The store every read and publication for this database goes through.
    pub store: Arc<DbStore>,
    /// The cipher the database's log seals record payloads with, when
    /// encrypted. The open log holds this same handle, so a rotation installs
    /// a new key snapshot into it rather than reopening anything.
    pub cipher: Option<Arc<LogCipher>>,
}

/// Failure resolving a database's storage keys.
#[derive(Debug, thiserror::Error)]
pub enum KeyWiringError {
    /// The database is encrypted and this process holds no keyring.
    #[error(
        "database {db:?} is encrypted under key {kek}; \
         start this process with --storage-key naming it"
    )]
    NoKeyring {
        /// Database whose manifest was found.
        db: String,
        /// Key-encryption key the manifest names.
        kek: KeyId,
    },
    /// The manifest carries no epoch new writes could use.
    #[error("database {0:?} has a key manifest with no active storage-key epoch")]
    NoActiveEpoch(String),
    /// Store or key-resolution failure.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// The log cipher rejected the resolved keys.
    #[error(transparent)]
    Log(#[from] LogError),
}

/// The lineage bytes authenticated into every log record of `db`.
///
/// A record's AAD binds this, so a record cannot be opened as if it belonged
/// to another database. The database name is that identity today, so a copy of
/// a database under another name re-seals its records rather than moving them:
/// [`crate::backup::restore`] rewrites them onto the target's lineage under the
/// archive's own data keys, and a fork mints a fresh manifest first.
#[must_use]
pub fn log_lineage(db: &str) -> Vec<u8> {
    db.as_bytes().to_vec()
}

/// Resolves `manifest` into the store and log cipher `db` operates through.
///
/// `manifest` is `None` for an unencrypted database, which needs no keyring
/// even when the process has one.
///
/// # Errors
///
/// Returns [`KeyWiringError`] when the database is encrypted and this process
/// has no keyring, when the manifest names no active epoch, or when any epoch
/// cannot be unwrapped — so a misconfigured process fails at open, naming the
/// key, rather than at its first read.
pub async fn resolve_db_crypto(
    db: &str,
    store: &Arc<NodeStore>,
    manifest: Option<&KeyManifest>,
    keyring: Option<&Arc<dyn Keyring>>,
) -> Result<DbCrypto, KeyWiringError> {
    let Some(manifest) = manifest.filter(|manifest| !manifest.storage_keys.is_empty()) else {
        return Ok(DbCrypto {
            store: Arc::new(DbStore::Plain(Arc::clone(store))),
            cipher: None,
        });
    };
    let Some(keyring) = keyring else {
        return Err(KeyWiringError::NoKeyring {
            db: db.to_owned(),
            kek: manifest.kek.clone(),
        });
    };
    let (epoch, keys) = unwrap_active(db, manifest, keyring.as_ref()).await?;
    Ok(DbCrypto {
        store: Arc::new(DbStore::Encrypted(EncryptedBlobStore::new(
            Arc::clone(store),
            epoch,
            keys.clone(),
        )?)),
        cipher: Some(Arc::new(LogCipher::new(log_lineage(db), epoch, keys)?)),
    })
}

/// Rebuilds `crypto` from a manifest that has just changed.
///
/// The blob decorator is reconstructed, and the log cipher — held by the open
/// log — takes the new snapshot in place, so a rotation takes effect on the
/// next append with no restart and no log reopen.
///
/// # Errors
///
/// Returns [`KeyWiringError`] under the same conditions as
/// [`resolve_db_crypto`]. `crypto` is left untouched unless every epoch
/// resolves.
pub async fn reload_db_crypto(
    db: &str,
    crypto: &DbCrypto,
    manifest: &KeyManifest,
    keyring: Option<&Arc<dyn Keyring>>,
) -> Result<Arc<DbStore>, KeyWiringError> {
    let Some(keyring) = keyring else {
        return Err(KeyWiringError::NoKeyring {
            db: db.to_owned(),
            kek: manifest.kek.clone(),
        });
    };
    let (epoch, keys) = unwrap_active(db, manifest, keyring.as_ref()).await?;
    let store = Arc::new(DbStore::Encrypted(EncryptedBlobStore::new(
        Arc::clone(crypto.store.raw()),
        epoch,
        keys.clone(),
    )?));
    if let Some(cipher) = &crypto.cipher {
        cipher.install(epoch, keys)?;
    }
    Ok(store)
}

async fn unwrap_active(
    db: &str,
    manifest: &KeyManifest,
    keyring: &dyn Keyring,
) -> Result<(u32, BTreeMap<u32, SecretKey>), KeyWiringError> {
    let epoch = manifest
        .active_storage_epoch()
        .ok_or_else(|| KeyWiringError::NoActiveEpoch(db.to_owned()))?;
    Ok((epoch, manifest.unwrap_storage_keys(keyring).await?))
}
