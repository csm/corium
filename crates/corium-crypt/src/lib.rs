//! Cryptographic primitives and key resolution for Corium.
//!
//! This crate deliberately has no storage or async-runtime dependency. It owns
//! stored encryption formats and secret-key hygiene; callers own where keys and
//! ciphertext live.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use async_trait::async_trait;
use thiserror::Error;
use zeroize::Zeroizing;

/// Magic prefix for an encrypted content-addressed blob.
pub const BLOB_MAGIC: &[u8; 8] = b"CORIUMB1";

const ALGORITHM_AES_256_GCM: u8 = 1;
const BLOB_HEADER_LEN: usize = BLOB_MAGIC.len() + 1 + size_of::<u32>() + size_of::<u64>();
const AES_GCM_TAG_LEN: usize = 16;
const NONCE_LEN: usize = 12;

/// Opaque, zeroized 256-bit key material.
#[derive(Clone, Eq, PartialEq)]
pub struct SecretKey(Zeroizing<[u8; 32]>);

impl SecretKey {
    /// Copies a 256-bit key into zeroized storage.
    #[must_use]
    pub fn new(bytes: [u8; 32]) -> Self {
        Self(Zeroizing::new(bytes))
    }

    /// Copies a byte slice into zeroized storage.
    ///
    /// # Errors
    ///
    /// Returns [`CryptError::InvalidKeyLength`] unless `bytes` is 32 bytes.
    pub fn from_slice(bytes: &[u8]) -> Result<Self, CryptError> {
        let bytes = <[u8; 32]>::try_from(bytes).map_err(|_| CryptError::InvalidKeyLength)?;
        Ok(Self::new(bytes))
    }

    fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl fmt::Debug for SecretKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretKey([REDACTED])")
    }
}

/// A key identity stored in a manifest or protection-class entity.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct KeyId(String);

impl KeyId {
    /// Creates a key identity.
    ///
    /// # Errors
    ///
    /// Returns [`KeyError::InvalidId`] for an empty identity.
    pub fn new(value: impl Into<String>) -> Result<Self, KeyError> {
        let value = value.into();
        if value.is_empty() {
            return Err(KeyError::InvalidId);
        }
        Ok(Self(value))
    }

    /// Returns the key identity as its URI-like string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for KeyId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Parsed metadata from an encrypted blob header.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BlobHeader {
    /// Key epoch used to encrypt the object.
    pub epoch: u32,
    /// Length of the plaintext payload.
    pub plaintext_len: u64,
}

/// Failures while encrypting or decrypting stored data.
#[derive(Debug, Error)]
pub enum CryptError {
    /// A secret key was not exactly 256 bits.
    #[error("secret key must be exactly 32 bytes")]
    InvalidKeyLength,
    /// The encrypted object does not contain a complete, supported header.
    #[error("invalid encrypted blob header")]
    InvalidBlobHeader,
    /// The encrypted object names an unsupported algorithm.
    #[error("unsupported encrypted blob algorithm {0}")]
    UnsupportedAlgorithm(u8),
    /// The object length disagrees with its authenticated header.
    #[error("encrypted blob length does not match its header")]
    InvalidBlobLength,
    /// Authentication failed, including when the wrong key was supplied.
    #[error("encrypted blob authentication failed")]
    AuthenticationFailed,
    /// The plaintext is too large for the stored length field.
    #[error("plaintext is too large to encrypt")]
    PlaintextTooLarge,
}

/// Failures while resolving or wrapping keys.
#[derive(Debug, Error)]
pub enum KeyError {
    /// A key identity was empty.
    #[error("key identity must not be empty")]
    InvalidId,
    /// No material exists for this identity and epoch.
    #[error("key {id} has no material for epoch {epoch}")]
    MissingKey {
        /// Requested key identity.
        id: KeyId,
        /// Requested key epoch.
        epoch: u32,
    },
    /// No current write epoch is configured for this identity.
    #[error("key {0} has no current epoch")]
    MissingCurrentEpoch(KeyId),
    /// Wrapped key material decrypted to an invalid length.
    #[error("wrapped key did not contain a 256-bit key")]
    InvalidWrappedKey,
    /// Wrapped key metadata named a different key epoch.
    #[error("wrapped key uses epoch {actual}, expected {expected}")]
    WrappedEpochMismatch {
        /// Requested key epoch.
        expected: u32,
        /// Epoch recorded in the wrapped key.
        actual: u32,
    },
    /// A cryptographic operation failed.
    #[error(transparent)]
    Crypt(#[from] CryptError),
}

/// Resolves key material without coupling Corium to a KMS implementation.
#[async_trait]
pub trait Keyring: Send + Sync {
    /// Resolves material for a specific epoch.
    async fn key(&self, id: &KeyId, epoch: u32) -> Result<SecretKey, KeyError>;

    /// Returns the epoch new writes should use.
    async fn current_epoch(&self, id: &KeyId) -> Result<u32, KeyError>;

    /// Wraps a data-encryption key under the requested key and epoch.
    async fn wrap(&self, id: &KeyId, epoch: u32, dek: &SecretKey) -> Result<Vec<u8>, KeyError>;

    /// Unwraps a stored data-encryption key.
    async fn unwrap(&self, id: &KeyId, epoch: u32, wrapped: &[u8]) -> Result<SecretKey, KeyError>;

    /// Lists the key identities this process can resolve.
    fn key_ids(&self) -> &[KeyId];
}

/// In-memory keyring for tests and keys loaded from files or environment
/// variables by a higher-level configuration layer.
#[derive(Clone, Default)]
pub struct StaticKeyring {
    keys: BTreeMap<(KeyId, u32), SecretKey>,
    current_epochs: BTreeMap<KeyId, u32>,
    key_ids: Vec<KeyId>,
}

impl StaticKeyring {
    /// Inserts material and optionally makes its epoch current for writes.
    pub fn insert(&mut self, id: KeyId, epoch: u32, key: SecretKey, current: bool) {
        if current {
            self.current_epochs.insert(id.clone(), epoch);
        }
        self.keys.insert((id, epoch), key);
        self.key_ids = self
            .keys
            .keys()
            .map(|(id, _)| id.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
    }
}

#[async_trait]
impl Keyring for StaticKeyring {
    async fn key(&self, id: &KeyId, epoch: u32) -> Result<SecretKey, KeyError> {
        self.keys
            .get(&(id.clone(), epoch))
            .cloned()
            .ok_or_else(|| KeyError::MissingKey {
                id: id.clone(),
                epoch,
            })
    }

    async fn current_epoch(&self, id: &KeyId) -> Result<u32, KeyError> {
        self.current_epochs
            .get(id)
            .copied()
            .ok_or_else(|| KeyError::MissingCurrentEpoch(id.clone()))
    }

    async fn wrap(&self, id: &KeyId, epoch: u32, dek: &SecretKey) -> Result<Vec<u8>, KeyError> {
        let kek = self.key(id, epoch).await?;
        let wrapping_key = derive_key(&kek, b"corium/key-wrap");
        encrypt_blob(&wrapping_key, epoch, dek.as_bytes()).map_err(Into::into)
    }

    async fn unwrap(&self, id: &KeyId, epoch: u32, wrapped: &[u8]) -> Result<SecretKey, KeyError> {
        let kek = self.key(id, epoch).await?;
        let wrapping_key = derive_key(&kek, b"corium/key-wrap");
        let header = parse_blob_header(wrapped)?;
        if header.epoch != epoch {
            return Err(KeyError::WrappedEpochMismatch {
                expected: epoch,
                actual: header.epoch,
            });
        }
        let plaintext = decrypt_blob(&wrapping_key, wrapped)?;
        SecretKey::from_slice(&plaintext).map_err(|_| KeyError::InvalidWrappedKey)
    }

    fn key_ids(&self) -> &[KeyId] {
        &self.key_ids
    }
}

/// Derives a separate 256-bit key for a domain-specific context.
#[must_use]
pub fn derive_key(parent: &SecretKey, context: &[u8]) -> SecretKey {
    let mut hasher = blake3::Hasher::new_keyed(parent.as_bytes());
    hasher.update(b"corium/derived-key");
    hasher.update(context);
    SecretKey::new(*hasher.finalize().as_bytes())
}

/// Parses and validates an encrypted blob's cleartext header.
///
/// # Errors
///
/// Returns a [`CryptError`] when the header, algorithm, or stored length is
/// invalid.
pub fn parse_blob_header(object: &[u8]) -> Result<BlobHeader, CryptError> {
    if object.len() < BLOB_HEADER_LEN || &object[..BLOB_MAGIC.len()] != BLOB_MAGIC {
        return Err(CryptError::InvalidBlobHeader);
    }
    let algorithm = object[BLOB_MAGIC.len()];
    if algorithm != ALGORITHM_AES_256_GCM {
        return Err(CryptError::UnsupportedAlgorithm(algorithm));
    }

    let epoch_offset = BLOB_MAGIC.len() + 1;
    let length_offset = epoch_offset + size_of::<u32>();
    let epoch = u32::from_be_bytes(
        object[epoch_offset..length_offset]
            .try_into()
            .map_err(|_| CryptError::InvalidBlobHeader)?,
    );
    let plaintext_len = u64::from_be_bytes(
        object[length_offset..BLOB_HEADER_LEN]
            .try_into()
            .map_err(|_| CryptError::InvalidBlobHeader)?,
    );
    let plaintext_len =
        usize::try_from(plaintext_len).map_err(|_| CryptError::InvalidBlobLength)?;
    let expected_len = BLOB_HEADER_LEN
        .checked_add(NONCE_LEN)
        .and_then(|length| length.checked_add(plaintext_len))
        .and_then(|length| length.checked_add(AES_GCM_TAG_LEN))
        .ok_or(CryptError::InvalidBlobLength)?;
    if object.len() != expected_len {
        return Err(CryptError::InvalidBlobLength);
    }
    Ok(BlobHeader {
        epoch,
        plaintext_len: plaintext_len as u64,
    })
}

/// Encrypts a blob deterministically for a given key epoch and plaintext.
///
/// The header remains cleartext, is authenticated as AAD, and the returned
/// object's content digest is suitable as its storage identity.
///
/// # Errors
///
/// Returns a [`CryptError`] if the plaintext is too large or encryption fails.
pub fn encrypt_blob(key: &SecretKey, epoch: u32, plaintext: &[u8]) -> Result<Vec<u8>, CryptError> {
    let plaintext_len =
        u64::try_from(plaintext.len()).map_err(|_| CryptError::PlaintextTooLarge)?;
    let mut header = Vec::with_capacity(BLOB_HEADER_LEN);
    header.extend_from_slice(BLOB_MAGIC);
    header.push(ALGORITHM_AES_256_GCM);
    header.extend_from_slice(&epoch.to_be_bytes());
    header.extend_from_slice(&plaintext_len.to_be_bytes());

    let plaintext_digest = blake3::hash(plaintext);
    let mut nonce_hasher = blake3::Hasher::new_keyed(key.as_bytes());
    nonce_hasher.update(b"corium/blob-nonce");
    nonce_hasher.update(&header);
    nonce_hasher.update(plaintext_digest.as_bytes());
    let nonce_digest = nonce_hasher.finalize();
    let nonce_bytes = &nonce_digest.as_bytes()[..NONCE_LEN];
    let cipher =
        Aes256Gcm::new_from_slice(key.as_bytes()).map_err(|_| CryptError::InvalidKeyLength)?;
    let ciphertext = cipher
        .encrypt(
            Nonce::from_slice(nonce_bytes),
            Payload {
                msg: plaintext,
                aad: &header,
            },
        )
        .map_err(|_| CryptError::AuthenticationFailed)?;
    header.extend_from_slice(nonce_bytes);
    header.extend_from_slice(&ciphertext);
    Ok(header)
}

/// Authenticates and decrypts an encrypted blob.
///
/// # Errors
///
/// Returns a [`CryptError`] for malformed data, the wrong key, or tampering.
pub fn decrypt_blob(key: &SecretKey, object: &[u8]) -> Result<Vec<u8>, CryptError> {
    let _header = parse_blob_header(object)?;
    let header = &object[..BLOB_HEADER_LEN];
    let nonce_end = BLOB_HEADER_LEN + NONCE_LEN;
    let nonce = Nonce::from_slice(&object[BLOB_HEADER_LEN..nonce_end]);
    let ciphertext = &object[nonce_end..];
    let cipher =
        Aes256Gcm::new_from_slice(key.as_bytes()).map_err(|_| CryptError::InvalidKeyLength)?;
    cipher
        .decrypt(
            nonce,
            Payload {
                msg: ciphertext,
                aad: header,
            },
        )
        .map_err(|_| CryptError::AuthenticationFailed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn key(byte: u8) -> SecretKey {
        SecretKey::new([byte; 32])
    }

    proptest! {
        #[test]
        fn blobs_are_deterministic_and_round_trip(
            plaintext in prop::collection::vec(any::<u8>(), 0..4096)
        ) {
            let encrypted = encrypt_blob(&key(7), 3, &plaintext).expect("encrypt");
            let repeated = encrypt_blob(&key(7), 3, &plaintext).expect("repeat");
            prop_assert_eq!(&encrypted, &repeated);
            prop_assert_eq!(decrypt_blob(&key(7), &encrypted).expect("decrypt"), plaintext);
        }
    }

    #[test]
    fn header_and_ciphertext_are_authenticated() {
        let encrypted = encrypt_blob(&key(1), 9, b"sentinel").expect("encrypt");
        assert_eq!(
            parse_blob_header(&encrypted).expect("header"),
            BlobHeader {
                epoch: 9,
                plaintext_len: 8,
            }
        );
        assert!(!encrypted.windows(8).any(|window| window == b"sentinel"));
        assert!(decrypt_blob(&key(2), &encrypted).is_err());

        let mut tampered = encrypted;
        *tampered.last_mut().expect("ciphertext") ^= 1;
        assert!(decrypt_blob(&key(1), &tampered).is_err());
    }

    #[test]
    fn debug_never_reveals_key_material() {
        let rendered = format!("{:?}", key(0xA5));
        assert_eq!(rendered, "SecretKey([REDACTED])");
        assert!(!rendered.contains("165"));
    }

    #[tokio::test]
    async fn static_keyring_resolves_and_wraps_keys() {
        let id = KeyId::new("file:test-kek").expect("key id");
        let mut keyring = StaticKeyring::default();
        keyring.insert(id.clone(), 4, key(4), true);

        assert_eq!(keyring.current_epoch(&id).await.expect("epoch"), 4);
        assert_eq!(keyring.key_ids(), std::slice::from_ref(&id));
        let wrapped = keyring.wrap(&id, 4, &key(8)).await.expect("wrap");
        assert_eq!(
            keyring.unwrap(&id, 4, &wrapped).await.expect("unwrap"),
            key(8)
        );
    }
}
