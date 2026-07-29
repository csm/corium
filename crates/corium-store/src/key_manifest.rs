//! The `keys:<db>` root record: which key encrypts a database, and how.
//!
//! The manifest is the bootstrap for storage encryption. It is stored
//! cleartext next to the other root records because it holds no key material —
//! only a KEK identity and data keys already wrapped under it — so a restore
//! can rebuild a working database from the archive plus KMS access and nothing
//! else.

use std::collections::BTreeMap;
use std::fmt;

use corium_crypt::{KeyId, Keyring, SecretKey};

use crate::StoreError;

/// Manifest format written by this release.
pub const KEY_MANIFEST_FORMAT_VERSION: u32 = 1;

const MANIFEST_HEADER: &str = "corium-keys-v";
const ALGORITHM_AES_256: &str = "aes-256";

/// Root-store key for a database's key manifest.
#[must_use]
pub fn keys_root_name(db: &str) -> String {
    format!("keys:{db}")
}

/// AEAD suite a storage-key epoch is used with.
///
/// One name covers both stored formats, because both are keyed by the same
/// data key: blobs use AES-256-GCM-SIV (deterministic encryption makes
/// nonce-misuse resistance mandatory) and log records AES-256-GCM. A future
/// `XChaCha20-Poly1305` or FIPS-mode backend becomes another value here rather
/// than a format rewrite.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum StorageAlgorithm {
    /// AES-256 in GCM-SIV (blobs) and GCM (log records).
    #[default]
    Aes256,
}

impl StorageAlgorithm {
    fn as_str(self) -> &'static str {
        match self {
            Self::Aes256 => ALGORITHM_AES_256,
        }
    }

    fn parse(text: &str) -> Option<Self> {
        match text {
            ALGORITHM_AES_256 => Some(Self::Aes256),
            _ => None,
        }
    }
}

impl fmt::Display for StorageAlgorithm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Lifecycle position of one storage-key epoch.
///
/// Exactly one epoch is `Active` — the one new writes use. Older epochs stay
/// `Retiring` while live objects still carry them, and become `Retired` only
/// once the mark pass counts none, at which point their key material may be
/// destroyed.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum StorageKeyState {
    /// New writes use this epoch.
    #[default]
    Active,
    /// Readable, still referenced by live objects, no longer written.
    Retiring,
    /// Readable while material lasts; no live object carries it.
    Retired,
}

impl StorageKeyState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Retiring => "retiring",
            Self::Retired => "retired",
        }
    }

    fn parse(text: &str) -> Option<Self> {
        match text {
            "active" => Some(Self::Active),
            "retiring" => Some(Self::Retiring),
            "retired" => Some(Self::Retired),
            _ => None,
        }
    }
}

impl fmt::Display for StorageKeyState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One storage data key, wrapped under the manifest's KEK.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageKey {
    /// Epoch this key encrypts under. Stored in every object it produces.
    pub epoch: u32,
    /// KEK epoch the material below is wrapped under. Recorded because a KEK
    /// rotation retires a KEK epoch while the DEK epoch is unchanged.
    pub kek_epoch: u32,
    /// AEAD suite this epoch is used with.
    pub algorithm: StorageAlgorithm,
    /// Wrapped data key. Never usable without the KEK.
    pub wrapped_dek: Vec<u8>,
    /// When the epoch was opened, as Unix milliseconds.
    pub created_at_unix_ms: i64,
    /// Where the epoch sits in its lifecycle.
    pub state: StorageKeyState,
    /// Live objects carrying this epoch as of the last mark pass. An epoch
    /// retires only at zero; `corium keys status` prints it so a drain is a
    /// number rather than a guess.
    pub live_objects: u64,
}

/// The key a protection class currently seals under.
///
/// This is a cache of what the schema already says, so a process can discover
/// which key ids it needs before it can read any datoms. The class entities in
/// `:db.part/db` remain authoritative. Only identities are recorded; class key
/// material is never stored in Corium.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProtectionClassKey {
    /// Entity id of the class in `:db.part/db`.
    pub class: u64,
    /// Identity a process resolves through its own keyring.
    pub key_id: KeyId,
    /// Epoch new seals under this class use.
    pub current_epoch: u32,
}

/// The `keys:<db>` root record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KeyManifest {
    /// Manifest format version. Readers reject manifests from newer formats.
    pub format_version: u32,
    /// Key-encryption key, recorded per database rather than per deployment so
    /// per-tenant key isolation needs no format change. A deployment-wide KEK
    /// is the case where every database names the same one.
    pub kek: KeyId,
    /// Storage data keys, ascending by epoch.
    pub storage_keys: Vec<StorageKey>,
    /// Protection-class key identities, ascending by class entity id.
    pub classes: Vec<ProtectionClassKey>,
}

impl KeyManifest {
    /// Mints a database's first storage key and returns its manifest.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Keyring`] when the KEK cannot be resolved or the
    /// fresh data key cannot be wrapped, and [`StoreError::Encryption`] when
    /// the platform has no usable random source.
    pub async fn create(
        keyring: &dyn Keyring,
        kek: KeyId,
        created_at_unix_ms: i64,
    ) -> Result<Self, StoreError> {
        let mut manifest = Self {
            format_version: KEY_MANIFEST_FORMAT_VERSION,
            kek,
            storage_keys: Vec::new(),
            classes: Vec::new(),
        };
        manifest
            .mint_storage_key(keyring, 1, created_at_unix_ms)
            .await?;
        Ok(manifest)
    }

    /// Returns the epoch new writes use, if any epoch is active.
    #[must_use]
    pub fn active_storage_epoch(&self) -> Option<u32> {
        self.storage_keys
            .iter()
            .filter(|key| key.state == StorageKeyState::Active)
            .map(|key| key.epoch)
            .max()
    }

    /// Returns the entry for one storage epoch.
    #[must_use]
    pub fn storage_key(&self, epoch: u32) -> Option<&StorageKey> {
        self.storage_keys.iter().find(|key| key.epoch == epoch)
    }

    /// Unwraps every storage epoch the manifest carries.
    ///
    /// The result is the immutable key snapshot `EncryptedBlobStore` and the
    /// log cipher hold: KMS access happens here, at open and on manifest
    /// reload, never on a blob read or a log append. Retired epochs are
    /// included while their material still resolves — a live object may still
    /// carry one, and reading it must not depend on when the mark pass last
    /// ran.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Keyring`] when an epoch cannot be unwrapped.
    /// A misconfigured process therefore fails at open, naming the epoch,
    /// rather than at its first read.
    pub async fn unwrap_storage_keys(
        &self,
        keyring: &dyn Keyring,
    ) -> Result<BTreeMap<u32, SecretKey>, StoreError> {
        let mut keys = BTreeMap::new();
        for key in &self.storage_keys {
            let material = keyring
                .unwrap(&self.kek, key.kek_epoch, &key.wrapped_dek)
                .await
                .map_err(StoreError::Keyring)?;
            keys.insert(key.epoch, material);
        }
        Ok(keys)
    }

    /// Opens a new storage epoch that new writes will use.
    ///
    /// Rotation is layered and cheap: no stored object is rewritten. The
    /// previous active epoch becomes `Retiring` and stays readable until
    /// ordinary re-indexing drains it.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Keyring`] when the new key cannot be wrapped and
    /// [`StoreError::Encryption`] when randomness is unavailable.
    pub async fn rotate_storage_key(
        &mut self,
        keyring: &dyn Keyring,
        created_at_unix_ms: i64,
    ) -> Result<u32, StoreError> {
        let epoch = self
            .storage_keys
            .iter()
            .map(|key| key.epoch)
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or(StoreError::StorageEpochExhausted)?;
        for key in &mut self.storage_keys {
            if key.state == StorageKeyState::Active {
                key.state = StorageKeyState::Retiring;
            }
        }
        self.mint_storage_key(keyring, epoch, created_at_unix_ms)
            .await?;
        Ok(epoch)
    }

    /// Re-wraps every storage key under `kek`, touching no stored data.
    ///
    /// `keyring` must resolve both the outgoing and incoming KEK: the
    /// manifest's own KEK to unwrap, and `kek` to wrap again.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Keyring`] when either KEK cannot be resolved.
    /// The manifest is left untouched unless every key re-wraps.
    pub async fn rewrap(&mut self, keyring: &dyn Keyring, kek: KeyId) -> Result<(), StoreError> {
        let kek_epoch = keyring
            .current_epoch(&kek)
            .await
            .map_err(StoreError::Keyring)?;
        let mut rewrapped = Vec::with_capacity(self.storage_keys.len());
        for key in &self.storage_keys {
            let material = keyring
                .unwrap(&self.kek, key.kek_epoch, &key.wrapped_dek)
                .await
                .map_err(StoreError::Keyring)?;
            rewrapped.push(StorageKey {
                kek_epoch,
                wrapped_dek: keyring
                    .wrap(&kek, kek_epoch, &material)
                    .await
                    .map_err(StoreError::Keyring)?,
                ..key.clone()
            });
        }
        self.kek = kek;
        self.storage_keys = rewrapped;
        Ok(())
    }

    async fn mint_storage_key(
        &mut self,
        keyring: &dyn Keyring,
        epoch: u32,
        created_at_unix_ms: i64,
    ) -> Result<(), StoreError> {
        let kek_epoch = keyring
            .current_epoch(&self.kek)
            .await
            .map_err(StoreError::Keyring)?;
        let dek = SecretKey::generate()?;
        let wrapped_dek = keyring
            .wrap(&self.kek, kek_epoch, &dek)
            .await
            .map_err(StoreError::Keyring)?;
        self.storage_keys.push(StorageKey {
            epoch,
            kek_epoch,
            algorithm: StorageAlgorithm::default(),
            wrapped_dek,
            created_at_unix_ms,
            state: StorageKeyState::Active,
            live_objects: 0,
        });
        self.storage_keys.sort_by_key(|key| key.epoch);
        Ok(())
    }

    /// Encodes the manifest for the root store.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        use fmt::Write as _;
        let mut out = format!("{MANIFEST_HEADER}{}\n{}\n", self.format_version, self.kek);
        let _ = writeln!(out, "{}", self.storage_keys.len());
        for key in &self.storage_keys {
            // The wrapped key is hex so the record stays a line-oriented text
            // blob the way `DbRoot` is, and so `RootStore::compare_and_set`
            // keeps comparing bytes over printable content.
            let _ = writeln!(
                out,
                "{} {} {} {} {} {} {}",
                key.epoch,
                key.kek_epoch,
                key.algorithm,
                key.state,
                key.created_at_unix_ms,
                key.live_objects,
                hex(&key.wrapped_dek),
            );
        }
        let _ = writeln!(out, "{}", self.classes.len());
        for class in &self.classes {
            // The key id goes last: it is a URI and may contain spaces.
            let _ = writeln!(
                out,
                "{} {} {}",
                class.class, class.current_epoch, class.key_id,
            );
        }
        out.into_bytes()
    }

    /// Decodes stored manifest bytes.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::InvalidKeyManifest`] for malformed bytes and
    /// [`StoreError::UnsupportedKeyManifest`] for a manifest written by a
    /// newer release, which a reader must refuse rather than half-understand.
    pub fn decode(bytes: &[u8]) -> Result<Self, StoreError> {
        let text = std::str::from_utf8(bytes).map_err(|_| invalid("manifest is not UTF-8"))?;
        let mut lines = text.lines();
        let format_version: u32 = lines
            .next()
            .and_then(|line| line.strip_prefix(MANIFEST_HEADER))
            .and_then(|version| version.parse().ok())
            .ok_or_else(|| invalid("missing manifest header"))?;
        if format_version > KEY_MANIFEST_FORMAT_VERSION {
            return Err(StoreError::UnsupportedKeyManifest {
                found: format_version,
                supported: KEY_MANIFEST_FORMAT_VERSION,
            });
        }
        let kek = lines
            .next()
            .ok_or_else(|| invalid("missing key-encryption key"))
            .and_then(|line| KeyId::new(line).map_err(StoreError::Keyring))?;

        // Neither loop below preallocates from its count; see `parse_count`.
        let storage_count = parse_count(lines.next())?;
        let mut storage_keys = Vec::new();
        for _ in 0..storage_count {
            let line = lines
                .next()
                .ok_or_else(|| invalid("truncated storage key"))?;
            storage_keys.push(decode_storage_key(line)?);
        }
        let class_count = parse_count(lines.next())?;
        let mut classes = Vec::new();
        for _ in 0..class_count {
            let line = lines.next().ok_or_else(|| invalid("truncated class key"))?;
            classes.push(decode_class_key(line)?);
        }
        Ok(Self {
            format_version,
            kek,
            storage_keys,
            classes,
        })
    }
}

fn invalid(reason: &str) -> StoreError {
    StoreError::InvalidKeyManifest(reason.to_owned())
}

/// Reads an entry count.
///
/// The count is a stated length, not an allocation budget, and it must never
/// become one. A manifest is a cleartext root record: it is restorable from any
/// archive and writable by anything that can reach the root store, so a
/// declared count of `usize::MAX` is a byte sequence a reader has to survive.
/// Callers therefore grow their vectors as entries arrive and stop at the first
/// absent line, which bounds both the allocation and the loop by the record's
/// real length. Nothing here may `with_capacity` on the returned value.
fn parse_count(line: Option<&str>) -> Result<usize, StoreError> {
    line.and_then(|line| line.parse().ok())
        .ok_or_else(|| invalid("missing entry count"))
}

fn decode_storage_key(line: &str) -> Result<StorageKey, StoreError> {
    let mut fields = line.split(' ');
    let mut next = || {
        fields
            .next()
            .ok_or_else(|| invalid("truncated storage key"))
    };
    let epoch = next()?
        .parse()
        .map_err(|_| invalid("invalid storage-key epoch"))?;
    let kek_epoch = next()?.parse().map_err(|_| invalid("invalid KEK epoch"))?;
    let algorithm = next().and_then(|text| {
        StorageAlgorithm::parse(text)
            .ok_or_else(|| StoreError::UnsupportedKeyAlgorithm(text.to_owned()))
    })?;
    let state = next().and_then(|text| {
        StorageKeyState::parse(text).ok_or_else(|| invalid("invalid storage-key state"))
    })?;
    let created_at_unix_ms = next()?
        .parse()
        .map_err(|_| invalid("invalid storage-key timestamp"))?;
    let live_objects = next()?
        .parse()
        .map_err(|_| invalid("invalid live-object count"))?;
    let wrapped_dek = next().and_then(unhex)?;
    if fields.next().is_some() {
        return Err(invalid("trailing storage-key field"));
    }
    Ok(StorageKey {
        epoch,
        kek_epoch,
        algorithm,
        wrapped_dek,
        created_at_unix_ms,
        state,
        live_objects,
    })
}

fn decode_class_key(line: &str) -> Result<ProtectionClassKey, StoreError> {
    let mut fields = line.splitn(3, ' ');
    let mut next = || fields.next().ok_or_else(|| invalid("truncated class key"));
    let class = next()?.parse().map_err(|_| invalid("invalid class id"))?;
    let current_epoch = next()?
        .parse()
        .map_err(|_| invalid("invalid class epoch"))?;
    let key_id = next().and_then(|text| KeyId::new(text).map_err(StoreError::Keyring))?;
    Ok(ProtectionClassKey {
        class,
        key_id,
        current_epoch,
    })
}

fn hex(bytes: &[u8]) -> String {
    use fmt::Write as _;
    bytes.iter().fold(String::new(), |mut out, byte| {
        let _ = write!(out, "{byte:02x}");
        out
    })
}

fn unhex(text: &str) -> Result<Vec<u8>, StoreError> {
    if !text.len().is_multiple_of(2) {
        return Err(invalid("wrapped key is not hex"));
    }
    text.as_bytes()
        .chunks(2)
        .map(|pair| {
            std::str::from_utf8(pair)
                .ok()
                .and_then(|pair| u8::from_str_radix(pair, 16).ok())
                .ok_or_else(|| invalid("wrapped key is not hex"))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use corium_crypt::StaticKeyring;

    fn keyring() -> StaticKeyring {
        let mut keyring = StaticKeyring::default();
        keyring.insert(
            KeyId::new("file:/etc/corium/storage.key").expect("kek"),
            1,
            SecretKey::new([9; 32]),
            true,
        );
        keyring
    }

    fn kek() -> KeyId {
        KeyId::new("file:/etc/corium/storage.key").expect("kek")
    }

    #[tokio::test]
    async fn a_created_manifest_round_trips_and_unwraps() {
        let keyring = keyring();
        let mut manifest = KeyManifest::create(&keyring, kek(), 1_700_000_000_000)
            .await
            .expect("create");
        manifest.classes.push(ProtectionClassKey {
            class: 74,
            key_id: KeyId::new("awskms:arn:aws:kms:us-west-2:1:key/2f1c").expect("class key"),
            current_epoch: 3,
        });

        let encoded = manifest.encode();
        assert_eq!(KeyManifest::decode(&encoded).expect("decode"), manifest);
        // The wrapped key is the only key material in the record, and it is
        // wrapped: the manifest never carries anything usable on its own.
        assert!(!encoded.windows(32).any(|window| window == [9_u8; 32]));

        assert_eq!(manifest.active_storage_epoch(), Some(1));
        let keys = manifest
            .unwrap_storage_keys(&keyring)
            .await
            .expect("unwrap");
        assert_eq!(keys.keys().copied().collect::<Vec<_>>(), vec![1]);
    }

    #[tokio::test]
    async fn rotation_opens_an_epoch_and_keeps_the_old_one_readable() {
        let keyring = keyring();
        let mut manifest = KeyManifest::create(&keyring, kek(), 1)
            .await
            .expect("create");
        let first = manifest
            .unwrap_storage_keys(&keyring)
            .await
            .expect("unwrap")[&1]
            .clone();

        assert_eq!(
            manifest
                .rotate_storage_key(&keyring, 2)
                .await
                .expect("rotate"),
            2
        );
        assert_eq!(manifest.active_storage_epoch(), Some(2));
        assert_eq!(
            manifest.storage_key(1).expect("epoch 1").state,
            StorageKeyState::Retiring
        );

        let keys = manifest
            .unwrap_storage_keys(&keyring)
            .await
            .expect("unwrap");
        assert_eq!(keys.keys().copied().collect::<Vec<_>>(), vec![1, 2]);
        assert_eq!(keys[&1], first, "a rotation must not disturb old epochs");
        assert_ne!(keys[&2], first);
    }

    #[tokio::test]
    async fn rewrapping_changes_the_kek_and_no_data_key() {
        let mut keyring = keyring();
        let replacement = KeyId::new("awskms:arn:aws:kms:us-west-2:1:key/9ab3").expect("kek");
        keyring.insert(replacement.clone(), 7, SecretKey::new([4; 32]), true);

        let mut manifest = KeyManifest::create(&keyring, kek(), 1)
            .await
            .expect("create");
        manifest
            .rotate_storage_key(&keyring, 2)
            .await
            .expect("rotate");
        let before = manifest
            .unwrap_storage_keys(&keyring)
            .await
            .expect("unwrap");
        let wrapped_before: Vec<_> = manifest
            .storage_keys
            .iter()
            .map(|key| key.wrapped_dek.clone())
            .collect();

        manifest
            .rewrap(&keyring, replacement.clone())
            .await
            .expect("rewrap");

        assert_eq!(manifest.kek, replacement);
        assert!(manifest.storage_keys.iter().all(|key| key.kek_epoch == 7));
        assert!(
            manifest
                .storage_keys
                .iter()
                .zip(&wrapped_before)
                .all(|(key, before)| key.wrapped_dek != *before)
        );
        assert_eq!(
            manifest
                .unwrap_storage_keys(&keyring)
                .await
                .expect("unwrap"),
            before,
            "re-wrapping must not change any data key"
        );
    }

    #[tokio::test]
    async fn an_unresolvable_epoch_fails_at_open_naming_the_key() {
        let keyring = keyring();
        let manifest = KeyManifest::create(&keyring, kek(), 1)
            .await
            .expect("create");
        let error = manifest
            .unwrap_storage_keys(&StaticKeyring::default())
            .await
            .expect_err("no KEK");
        assert!(
            error.to_string().contains("file:/etc/corium/storage.key"),
            "{error}"
        );
    }

    #[test]
    fn a_newer_manifest_is_refused_rather_than_half_understood() {
        let encoded = b"corium-keys-v2\nfile:kek\n0\n0\n".as_slice();
        assert!(matches!(
            KeyManifest::decode(encoded),
            Err(StoreError::UnsupportedKeyManifest {
                found: 2,
                supported: 1
            })
        ));
    }

    #[test]
    fn an_overstated_entry_count_is_bounded_by_the_record() {
        // The manifest is a cleartext root record, so its declared counts are
        // attacker-reachable. Decoding must be bounded by the bytes actually
        // present — never allocate or iterate to a stated length.
        for bytes in [
            format!("corium-keys-v1\nfile:kek\n{}\n0\n", usize::MAX),
            format!("corium-keys-v1\nfile:kek\n0\n{}\n", usize::MAX),
            format!("corium-keys-v1\nfile:kek\n{}\n", u64::MAX),
        ] {
            assert!(matches!(
                KeyManifest::decode(bytes.as_bytes()),
                Err(StoreError::InvalidKeyManifest(_))
            ));
        }
    }

    #[test]
    fn malformed_manifests_are_rejected() {
        for bytes in [
            b"".as_slice(),
            b"corium-keys-v1\n".as_slice(),
            b"corium-keys-v1\nfile:kek\n1\n".as_slice(),
            b"corium-keys-v1\nfile:kek\n1\n1 1 aes-256 active 0 0 xyz\n0\n".as_slice(),
            b"corium-keys-v1\nfile:kek\n1\n1 1 aes-256 sideways 0 0 00\n0\n".as_slice(),
            b"corium-keys-v1\n\n0\n0\n".as_slice(),
        ] {
            assert!(
                KeyManifest::decode(bytes).is_err(),
                "accepted {:?}",
                String::from_utf8_lossy(bytes)
            );
        }
        assert!(matches!(
            KeyManifest::decode(b"corium-keys-v1\nfile:kek\n1\n1 1 rot13 active 0 0 00\n0\n"),
            Err(StoreError::UnsupportedKeyAlgorithm(_))
        ));
    }
}
