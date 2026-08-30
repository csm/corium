//! Cryptographic primitives and key resolution for Corium.
//!
//! This crate deliberately has no storage or async-runtime dependency. It owns
//! stored encryption formats and secret-key hygiene; callers own where keys and
//! ciphertext live.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::Path;

use aes_gcm::Aes256Gcm;
use aes_gcm_siv::aead::{Aead, KeyInit, Payload};
use aes_gcm_siv::{Aes256GcmSiv, Nonce};
use async_trait::async_trait;
use thiserror::Error;
use zeroize::Zeroizing;

#[cfg(feature = "aws-kms")]
pub mod aws;
mod composite;
pub mod kms;
#[cfg(any(test, feature = "testing"))]
pub mod testing;

pub use composite::CompositeKeyring;
pub use kms::{KmsClient, KmsError, KmsKeyring};

/// Magic prefix for an encrypted content-addressed blob.
pub const BLOB_MAGIC: &[u8; 8] = b"CORIUMB1";

/// Magic prefix for an encrypted transaction-log record payload.
pub const LOG_MAGIC: &[u8; 8] = b"CORIUML1";

const ALGORITHM_AES_256_GCM_SIV: u8 = 1;
const ALGORITHM_AES_256_GCM: u8 = 2;
const BLOB_HEADER_LEN: usize = BLOB_MAGIC.len() + 1 + size_of::<u32>() + size_of::<u64>();
/// Magic, algorithm, key epoch, transaction number, nonce.
const LOG_HEADER_LEN: usize = LOG_MAGIC.len() + 1 + size_of::<u32>() + size_of::<u64>() + NONCE_LEN;
const AEAD_TAG_LEN: usize = 16;
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

    /// Draws fresh 256-bit key material from the operating system's
    /// cryptographic random source.
    ///
    /// # Errors
    ///
    /// Returns [`CryptError::RandomnessUnavailable`] when the OS entropy
    /// source cannot be read. Callers must fail rather than fall back: a
    /// predictable data key is indistinguishable from no encryption.
    pub fn generate() -> Result<Self, CryptError> {
        let mut bytes = Zeroizing::new([0_u8; 32]);
        getrandom::fill(bytes.as_mut_slice()).map_err(|_| CryptError::RandomnessUnavailable)?;
        Ok(Self(bytes))
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

    /// Splits the identity into its scheme and the rest.
    ///
    /// An identity with no `:` has no scheme, which every resolver rejects —
    /// the URI form is what lets one keyring serve files, environment
    /// variables, and a KMS at once.
    #[must_use]
    pub fn scheme(&self) -> Option<(&str, &str)> {
        self.0.split_once(':')
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

/// Parsed metadata from an encrypted log-record payload header.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LogHeader {
    /// Key epoch used to encrypt the payload.
    pub epoch: u32,
    /// Transaction number the payload was written at.
    pub t: u64,
}

/// Failures while encrypting or decrypting stored data.
#[derive(Debug, Error)]
pub enum CryptError {
    /// A secret key was not exactly 256 bits.
    #[error("secret key must be exactly 32 bytes")]
    InvalidKeyLength,
    /// The operating system's random source was unavailable.
    #[error("cryptographic randomness is unavailable")]
    RandomnessUnavailable,
    /// The encrypted object does not contain a complete, supported header.
    #[error("invalid encrypted blob header")]
    InvalidBlobHeader,
    /// The log payload does not contain a complete, supported header.
    #[error("invalid encrypted log record header")]
    InvalidLogHeader,
    /// The encrypted object names an unsupported algorithm.
    #[error("unsupported encrypted blob algorithm {0}")]
    UnsupportedAlgorithm(u8),
    /// The object length disagrees with its authenticated header.
    #[error("encrypted blob length does not match its header")]
    InvalidBlobLength,
    /// Encryption failed after inputs were validated.
    #[error("encryption failed")]
    EncryptionFailed,
    /// Authentication failed, including when the wrong key was supplied.
    #[error("authentication failed")]
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
    /// The identity names a scheme this build cannot resolve.
    #[error(
        "key {0} names a key source this build cannot resolve; \
         file: and env: are always available"
    )]
    UnsupportedScheme(KeyId),
    /// The key service holding this identity could not serve the request.
    ///
    /// Material this process already resolved keeps working; what fails is
    /// work that needs the service, which is the distinction an operator has
    /// to be able to make from the message alone.
    #[error("key {id} could not be resolved through its key service: {reason}")]
    Kms {
        /// Key identity that failed to resolve.
        id: KeyId,
        /// What the key service reported. Never quotes key material.
        reason: String,
    },
    /// The identity's source could not be read.
    ///
    /// The reason never quotes the material, only where it was looked for.
    #[error("key {id} could not be read: {reason}")]
    Unreadable {
        /// Key identity that failed to resolve.
        id: KeyId,
        /// Why the source was unusable.
        reason: String,
    },
    /// The source resolved but did not hold a 256-bit key.
    #[error("key {0} does not hold 32 bytes of key material (raw or 64 hex characters)")]
    InvalidMaterial(KeyId),
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

/// Epoch a locally held key resolves at.
///
/// A file or environment variable holds one piece of material and has no
/// notion of versions, so it is always epoch 1. Rotating such a key means
/// naming a different identity — which is what `corium keys rewrap --kek`
/// takes — rather than bumping an epoch the source cannot represent.
pub const STATIC_KEY_EPOCH: u32 = 1;

impl StaticKeyring {
    /// Resolves each identity from its own source and returns the keyring.
    ///
    /// Resolution happens once, here, so a misconfigured process fails at
    /// startup naming the key it could not read, rather than at its first
    /// blob read.
    ///
    /// # Errors
    ///
    /// Returns [`KeyError`] when any identity names an unsupported scheme,
    /// cannot be read, or does not hold 256 bits of material.
    pub fn resolve(ids: impl IntoIterator<Item = KeyId>) -> Result<Self, KeyError> {
        let mut keyring = Self::default();
        for id in ids {
            let key = load_key(&id)?;
            keyring.insert(id, STATIC_KEY_EPOCH, key, true);
        }
        Ok(keyring)
    }

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
        let plaintext = Zeroizing::new(decrypt_blob(&wrapping_key, wrapped)?);
        SecretKey::from_slice(plaintext.as_slice()).map_err(|_| KeyError::InvalidWrappedKey)
    }

    fn key_ids(&self) -> &[KeyId] {
        &self.key_ids
    }
}

/// Reads the material one locally resolvable key identity names.
///
/// `file:<path>` reads the file; `env:<NAME>` reads an environment variable.
/// Both accept 32 raw bytes or 64 hexadecimal characters, with surrounding
/// whitespace ignored so a key file written by `printf` or by an editor that
/// appends a newline both work.
///
/// KMS schemes (`awskms:`, `gcpkms:`, `vault:`) are resolved by their own
/// keyring implementations, not here, and are reported as unsupported rather
/// than silently treated as a path.
///
/// # Errors
///
/// Returns [`KeyError`] when the scheme is not locally resolvable, the source
/// cannot be read, or it does not hold 256 bits.
pub fn load_key(id: &KeyId) -> Result<SecretKey, KeyError> {
    let Some((scheme, rest)) = id.scheme() else {
        return Err(KeyError::UnsupportedScheme(id.clone()));
    };
    let material = match scheme {
        "file" => Zeroizing::new(std::fs::read(Path::new(rest)).map_err(|error| {
            KeyError::Unreadable {
                id: id.clone(),
                reason: format!("cannot read {rest}: {error}"),
            }
        })?),
        "env" => Zeroizing::new(
            std::env::var(rest)
                .map_err(|_| KeyError::Unreadable {
                    id: id.clone(),
                    reason: format!("environment variable {rest} is not set"),
                })?
                .into_bytes(),
        ),
        _ => return Err(KeyError::UnsupportedScheme(id.clone())),
    };
    decode_key_material(&material).ok_or_else(|| KeyError::InvalidMaterial(id.clone()))
}

/// Accepts 32 raw bytes, or 64 hexadecimal characters with surrounding
/// whitespace.
fn decode_key_material(material: &[u8]) -> Option<SecretKey> {
    if let Ok(key) = SecretKey::from_slice(material) {
        return Some(key);
    }
    let trimmed = std::str::from_utf8(material).ok()?.trim();
    if trimmed.len() != 64 {
        return None;
    }
    let mut bytes = Zeroizing::new([0_u8; 32]);
    for (slot, pair) in bytes.iter_mut().zip(trimmed.as_bytes().chunks(2)) {
        let pair = std::str::from_utf8(pair).ok()?;
        *slot = u8::from_str_radix(pair, 16).ok()?;
    }
    Some(SecretKey::new(*bytes))
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
    if algorithm != ALGORITHM_AES_256_GCM_SIV {
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
        .and_then(|length| length.checked_add(AEAD_TAG_LEN))
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
    header.push(ALGORITHM_AES_256_GCM_SIV);
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
        Aes256GcmSiv::new_from_slice(key.as_bytes()).map_err(|_| CryptError::InvalidKeyLength)?;
    let ciphertext = cipher
        .encrypt(
            Nonce::from_slice(nonce_bytes),
            Payload {
                msg: plaintext,
                aad: &header,
            },
        )
        .map_err(|_| CryptError::EncryptionFailed)?;
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
        Aes256GcmSiv::new_from_slice(key.as_bytes()).map_err(|_| CryptError::InvalidKeyLength)?;
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

/// Seals a value deterministically: the same key, AAD, and plaintext always
/// produce the same bytes.
///
/// The nonce is all zero by design. Fact identity depends on determinism —
/// the index key *is* the datom, and a retraction cancels an assertion by
/// sharing its `(e, a, v)` byte prefix — so a keyless transactor must be able
/// to pair a retraction with an assertion bytewise, without holding any key.
/// GCM-SIV is what makes the fixed nonce safe: it derives the per-message
/// nonce from the plaintext itself, so encrypting distinct plaintexts under
/// one key never reuses a true nonce, and even a repeated plaintext leaks only
/// equality. (See `docs/design/encryption.md`, "Fact identity, and why
/// sealing is deterministic".)
///
/// The caller must bind the full context in the AAD — the key identity,
/// epoch, attribute, optionally the entity, and the value type (the design's
/// `"corium/seal-v1" ‖ context ‖ vtype`) — so a sealed body cannot be moved
/// to another attribute, epoch, or subject.
///
/// The output is `ciphertext ‖ 16-byte tag`.
///
/// # Errors
///
/// Returns a [`CryptError`] if encryption fails.
pub fn seal_deterministic(
    key: &SecretKey,
    aad: &[u8],
    plaintext: &[u8],
) -> Result<Vec<u8>, CryptError> {
    let cipher =
        Aes256GcmSiv::new_from_slice(key.as_bytes()).map_err(|_| CryptError::InvalidKeyLength)?;
    cipher
        .encrypt(
            Nonce::from_slice(&[0_u8; NONCE_LEN]),
            Payload {
                msg: plaintext,
                aad,
            },
        )
        .map_err(|_| CryptError::EncryptionFailed)
}

/// Authenticates and opens a body produced by [`seal_deterministic`].
///
/// The nonce is the same all-zero nonce the sealer uses, so the caller must
/// present the exact AAD the body was sealed under: any difference in key,
/// context, or body fails authentication rather than returning plaintext.
///
/// # Errors
///
/// Returns [`CryptError::AuthenticationFailed`] when the body is shorter than
/// the 16-byte tag, was sealed under another key or AAD, or was tampered
/// with.
pub fn open_deterministic(key: &SecretKey, aad: &[u8], body: &[u8]) -> Result<Vec<u8>, CryptError> {
    if body.len() < AEAD_TAG_LEN {
        return Err(CryptError::AuthenticationFailed);
    }
    let cipher =
        Aes256GcmSiv::new_from_slice(key.as_bytes()).map_err(|_| CryptError::InvalidKeyLength)?;
    cipher
        .decrypt(
            Nonce::from_slice(&[0_u8; NONCE_LEN]),
            Payload { msg: body, aad },
        )
        .map_err(|_| CryptError::AuthenticationFailed)
}

/// Reports whether a log-record payload carries the encrypted-record header.
///
/// The plaintext record encoding starts with its transaction number as a
/// big-endian `u64`, so [`LOG_MAGIC`] is only ambiguous with a `t` above
/// 4.8 quintillion. A log that reached that many transactions would have
/// exhausted every other counter in the system first.
#[must_use]
pub fn is_encrypted_log_record(payload: &[u8]) -> bool {
    payload.len() >= LOG_MAGIC.len() && &payload[..LOG_MAGIC.len()] == LOG_MAGIC
}

/// Parses and validates an encrypted log record's cleartext header.
///
/// The epoch and transaction number stay cleartext so frame scanning,
/// recovery truncation, and range reads keep working — and so a reader can
/// pick the right key epoch — without holding any key.
///
/// # Errors
///
/// Returns a [`CryptError`] when the header is truncated, is not a log
/// record, or names an unsupported algorithm.
pub fn parse_log_header(payload: &[u8]) -> Result<LogHeader, CryptError> {
    if !is_encrypted_log_record(payload) || payload.len() < LOG_HEADER_LEN + AEAD_TAG_LEN {
        return Err(CryptError::InvalidLogHeader);
    }
    let algorithm = payload[LOG_MAGIC.len()];
    if algorithm != ALGORITHM_AES_256_GCM {
        return Err(CryptError::UnsupportedAlgorithm(algorithm));
    }
    let epoch_offset = LOG_MAGIC.len() + 1;
    let t_offset = epoch_offset + size_of::<u32>();
    let nonce_offset = t_offset + size_of::<u64>();
    let epoch = u32::from_be_bytes(
        payload[epoch_offset..t_offset]
            .try_into()
            .map_err(|_| CryptError::InvalidLogHeader)?,
    );
    let t = u64::from_be_bytes(
        payload[t_offset..nonce_offset]
            .try_into()
            .map_err(|_| CryptError::InvalidLogHeader)?,
    );
    Ok(LogHeader { epoch, t })
}

/// Builds the authenticated header and additional data for one log record.
fn log_header_and_aad(
    epoch: u32,
    lineage: &[u8],
    log_version: u64,
    t: u64,
    nonce: &[u8; NONCE_LEN],
) -> (Vec<u8>, Vec<u8>) {
    let mut header = Vec::with_capacity(LOG_HEADER_LEN);
    header.extend_from_slice(LOG_MAGIC);
    header.push(ALGORITHM_AES_256_GCM);
    header.extend_from_slice(&epoch.to_be_bytes());
    header.extend_from_slice(&t.to_be_bytes());
    header.extend_from_slice(nonce);

    // The lineage is variable length, so its length is bound too: without it
    // a lineage/version pair could be re-split to authenticate under another
    // database.
    let mut aad = Vec::with_capacity(b"corium/log-v1".len() + 16 + lineage.len() + header.len());
    aad.extend_from_slice(b"corium/log-v1");
    aad.extend_from_slice(&(lineage.len() as u64).to_be_bytes());
    aad.extend_from_slice(lineage);
    aad.extend_from_slice(&log_version.to_be_bytes());
    aad.extend_from_slice(&header);
    (header, aad)
}

/// Encrypts one transaction-log record payload.
///
/// The AAD binds the database lineage, the log's lease version, the
/// transaction number, and the key epoch, so a record can neither be replayed
/// at another basis nor moved between the per-lease-version log files that
/// takeover fencing relies on.
///
/// Unlike blobs, log records are not content addressed and nothing requires
/// re-encoding a record to the same bytes, so this uses one-pass AES-256-GCM
/// with a fresh random nonce rather than a nonce derived from
/// `(log_version, t)`. A derived nonce would be reused whenever a transaction
/// number is re-issued with different content — which happens whenever an
/// append is torn by a crash before it is acknowledged, and truncated away on
/// recovery. Key/nonce reuse is fatal under GCM; storing 12 bytes is not.
///
/// # Errors
///
/// Returns a [`CryptError`] when randomness is unavailable or encryption
/// fails.
pub fn encrypt_log_record(
    key: &SecretKey,
    epoch: u32,
    lineage: &[u8],
    log_version: u64,
    t: u64,
    plaintext: &[u8],
) -> Result<Vec<u8>, CryptError> {
    let mut nonce_bytes = [0_u8; NONCE_LEN];
    getrandom::fill(&mut nonce_bytes).map_err(|_| CryptError::RandomnessUnavailable)?;
    let (mut header, aad) = log_header_and_aad(epoch, lineage, log_version, t, &nonce_bytes);
    let cipher =
        Aes256Gcm::new_from_slice(key.as_bytes()).map_err(|_| CryptError::InvalidKeyLength)?;
    let ciphertext = cipher
        .encrypt(
            Nonce::from_slice(&nonce_bytes),
            Payload {
                msg: plaintext,
                aad: &aad,
            },
        )
        .map_err(|_| CryptError::EncryptionFailed)?;
    header.extend_from_slice(&ciphertext);
    Ok(header)
}

/// Authenticates and decrypts one transaction-log record payload.
///
/// `log_version` is the lease version of the file or object the payload was
/// read from; supplying another one fails authentication rather than
/// returning a record from the wrong version file.
///
/// # Errors
///
/// Returns a [`CryptError`] for a malformed payload, the wrong key, a
/// mismatched lineage or log version, or tampering.
pub fn decrypt_log_record(
    key: &SecretKey,
    lineage: &[u8],
    log_version: u64,
    payload: &[u8],
) -> Result<Vec<u8>, CryptError> {
    let LogHeader { epoch, t } = parse_log_header(payload)?;
    let header = &payload[..LOG_HEADER_LEN];
    let nonce_offset = LOG_HEADER_LEN - NONCE_LEN;
    let nonce_bytes = <[u8; NONCE_LEN]>::try_from(&header[nonce_offset..])
        .map_err(|_| CryptError::InvalidLogHeader)?;
    let (_, aad) = log_header_and_aad(epoch, lineage, log_version, t, &nonce_bytes);
    let cipher =
        Aes256Gcm::new_from_slice(key.as_bytes()).map_err(|_| CryptError::InvalidKeyLength)?;
    cipher
        .decrypt(
            Nonce::from_slice(&nonce_bytes),
            Payload {
                msg: &payload[LOG_HEADER_LEN..],
                aad: &aad,
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
        fn sealed_values_are_deterministic(
            aad in prop::collection::vec(any::<u8>(), 0..256),
            plaintext in prop::collection::vec(any::<u8>(), 0..4096)
        ) {
            let sealed = seal_deterministic(&key(7), &aad, &plaintext).expect("seal");
            let repeated = seal_deterministic(&key(7), &aad, &plaintext).expect("repeat");
            prop_assert_eq!(&sealed, &repeated);
            prop_assert_eq!(sealed.len(), plaintext.len() + AEAD_TAG_LEN);
        }

        #[test]
        fn sealed_values_round_trip(
            aad in prop::collection::vec(any::<u8>(), 0..256),
            plaintext in prop::collection::vec(any::<u8>(), 0..4096)
        ) {
            let sealed = seal_deterministic(&key(7), &aad, &plaintext).expect("seal");
            prop_assert_eq!(
                open_deterministic(&key(7), &aad, &sealed).expect("open"),
                plaintext
            );
        }
    }

    #[test]
    fn sealed_values_fail_to_open_under_any_other_context() {
        let aad = b"corium/seal-v1\0storage\0\0\0\x01\0person/email";
        let sealed = seal_deterministic(&key(1), aad, b"ada@example.com").expect("seal");

        // Wrong AAD, wrong key: both are authentication failures, never a
        // panic or a decode error.
        assert!(matches!(
            open_deterministic(&key(1), b"person/name", &sealed),
            Err(CryptError::AuthenticationFailed)
        ));
        assert!(matches!(
            open_deterministic(&key(2), aad, &sealed),
            Err(CryptError::AuthenticationFailed)
        ));

        let mut tampered = sealed;
        *tampered.last_mut().expect("tag") ^= 1;
        assert!(matches!(
            open_deterministic(&key(1), aad, &tampered),
            Err(CryptError::AuthenticationFailed)
        ));
    }

    #[test]
    fn sealed_values_round_trip_empty_plaintext() {
        let sealed = seal_deterministic(&key(3), b"ctx", b"").expect("seal");
        assert_eq!(sealed.len(), AEAD_TAG_LEN);
        assert_eq!(
            open_deterministic(&key(3), b"ctx", &sealed).expect("open"),
            b""
        );
    }

    #[test]
    fn open_deterministic_rejects_bodies_shorter_than_the_tag() {
        for body in [&b""[..], &b"short"[..], &[0_u8; 15][..]] {
            assert!(matches!(
                open_deterministic(&key(3), b"ctx", body),
                Err(CryptError::AuthenticationFailed)
            ));
        }
        // Exactly one tag of zero bytes is well-formed but must not
        // authenticate — only a body from `seal_deterministic` does.
        assert!(matches!(
            open_deterministic(&key(3), b"ctx", &[0_u8; 16]),
            Err(CryptError::AuthenticationFailed)
        ));
    }

    proptest! {
        #[test]
        fn blobs_are_deterministic_and_round_trip(
            plaintext in prop::collection::vec(any::<u8>(), 0..4096)
        ) {
            let encrypted = encrypt_blob(&key(7), 3, &plaintext).expect("encrypt");
            let repeated = encrypt_blob(&key(7), 3, &plaintext).expect("repeat");
            prop_assert_eq!(&encrypted, &repeated);
            prop_assert_ne!(
                encrypt_blob(&key(7), 4, &plaintext).expect("different epoch"),
                encrypted.clone()
            );
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

        let mut tampered_nonce = encrypt_blob(&key(1), 9, b"sentinel").expect("encrypt");
        tampered_nonce[BLOB_HEADER_LEN] ^= 1;
        assert!(decrypt_blob(&key(1), &tampered_nonce).is_err());
    }

    proptest! {
        #[test]
        fn log_records_round_trip(payload in prop::collection::vec(any::<u8>(), 0..4096)) {
            let encrypted = encrypt_log_record(&key(5), 2, b"people", 7, 42, &payload)
                .expect("encrypt");
            prop_assert_eq!(
                parse_log_header(&encrypted).expect("header"),
                LogHeader { epoch: 2, t: 42 }
            );
            prop_assert_eq!(
                decrypt_log_record(&key(5), b"people", 7, &encrypted).expect("decrypt"),
                payload
            );
        }
    }

    #[test]
    fn log_records_are_bound_to_their_position() {
        let plaintext = b"sentinel payload".as_slice();
        let encrypted = encrypt_log_record(&key(5), 2, b"people", 7, 42, plaintext).expect("seal");
        assert!(is_encrypted_log_record(&encrypted));
        assert!(
            !encrypted
                .windows(plaintext.len())
                .any(|window| window == plaintext)
        );

        // Lineage, lease version, transaction number, and epoch are all
        // authenticated: none of them can be restated without detection.
        assert!(decrypt_log_record(&key(5), b"other", 7, &encrypted).is_err());
        assert!(decrypt_log_record(&key(5), b"people", 8, &encrypted).is_err());
        assert!(decrypt_log_record(&key(6), b"people", 7, &encrypted).is_err());

        let mut moved = encrypted.clone();
        let t_offset = LOG_MAGIC.len() + 1 + size_of::<u32>();
        moved[t_offset..t_offset + size_of::<u64>()].copy_from_slice(&43_u64.to_be_bytes());
        assert_eq!(parse_log_header(&moved).expect("header").t, 43);
        assert!(decrypt_log_record(&key(5), b"people", 7, &moved).is_err());

        let mut retagged = encrypted;
        retagged[LOG_MAGIC.len() + 1] ^= 1;
        assert!(decrypt_log_record(&key(5), b"people", 7, &retagged).is_err());
    }

    #[test]
    fn log_records_do_not_reuse_a_nonce() {
        // A torn append is truncated on recovery and the same `t` is re-issued,
        // possibly for different tx-data. Encryption must not repeat itself.
        let first = encrypt_log_record(&key(5), 2, b"people", 7, 42, b"first").expect("first");
        let second = encrypt_log_record(&key(5), 2, b"people", 7, 42, b"first").expect("second");
        assert_ne!(first, second);
        assert_eq!(
            decrypt_log_record(&key(5), b"people", 7, &second).expect("decrypt"),
            b"first"
        );
    }

    #[test]
    fn plaintext_records_are_not_mistaken_for_encrypted_ones() {
        let mut plaintext = 42_u64.to_be_bytes().to_vec();
        plaintext.extend_from_slice(&[0; 32]);
        assert!(!is_encrypted_log_record(&plaintext));
        assert!(matches!(
            parse_log_header(&plaintext),
            Err(CryptError::InvalidLogHeader)
        ));
        assert!(!is_encrypted_log_record(&[]));
        assert!(!is_encrypted_log_record(LOG_MAGIC.as_slice().split_at(4).0));
    }

    #[test]
    fn generated_keys_are_distinct() {
        let first = SecretKey::generate().expect("generate");
        assert_ne!(first, SecretKey::generate().expect("generate"));
        assert_ne!(first, SecretKey::new([0; 32]));
    }

    #[test]
    fn debug_never_reveals_key_material() {
        let rendered = format!("{:?}", key(0xA5));
        assert_eq!(rendered, "SecretKey([REDACTED])");
        assert!(!rendered.contains("165"));
    }

    #[tokio::test]
    async fn local_key_sources_accept_raw_and_hex_material() {
        let dir = std::env::temp_dir().join(format!("corium-crypt-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let raw_path = dir.join("raw.key");
        std::fs::write(&raw_path, [7_u8; 32]).expect("write raw");
        let hex_path = dir.join("hex.key");
        // Trailing newline: what every editor and `echo` produces.
        std::fs::write(&hex_path, format!("{}\n", "07".repeat(32))).expect("write hex");

        let raw = KeyId::new(format!("file:{}", raw_path.display())).expect("id");
        let hex = KeyId::new(format!("file:{}", hex_path.display())).expect("id");
        assert_eq!(load_key(&raw).expect("raw"), key(7));
        assert_eq!(load_key(&hex).expect("hex"), key(7));
        assert!(matches!(
            load_key(&KeyId::new("env:CORIUM_KEY_THAT_IS_NOT_SET").expect("id")),
            Err(KeyError::Unreadable { .. })
        ));

        let keyring = StaticKeyring::resolve([raw.clone(), hex]).expect("resolve");
        assert_eq!(keyring.key_ids().len(), 2);
        assert_eq!(
            keyring.current_epoch(&raw).await.expect("epoch"),
            STATIC_KEY_EPOCH
        );

        std::fs::remove_dir_all(&dir).expect("clean up");
    }

    #[test]
    fn unresolvable_key_identities_name_what_failed() {
        let missing = KeyId::new("file:/nonexistent/corium/storage.key").expect("id");
        assert!(matches!(
            load_key(&missing),
            Err(KeyError::Unreadable { .. })
        ));
        // A KMS identity is a real key source, just not one this function
        // resolves; it must not be mistaken for a relative path.
        let kms = KeyId::new("awskms:arn:aws:kms:us-west-2:1:key/2f1c").expect("id");
        assert!(matches!(
            load_key(&kms),
            Err(KeyError::UnsupportedScheme(_))
        ));
        assert!(matches!(
            load_key(&KeyId::new("storage.key").expect("id")),
            Err(KeyError::UnsupportedScheme(_))
        ));
        assert!(decode_key_material(b"too short").is_none());
        assert!(decode_key_material(&[0; 31]).is_none());
        assert!(decode_key_material("zz".repeat(32).as_bytes()).is_none());
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
