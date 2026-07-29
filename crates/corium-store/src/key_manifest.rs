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

/// Log records to seal under one storage epoch before its key must be rotated.
///
/// Log records use AES-256-GCM with a random 96-bit nonce, so nonce collision
/// is a birthday problem rather than an impossibility: after `q` records the
/// probability is about `q² / 2¹⁹⁷`, which stays negligible up to `2³²` — the
/// standard ceiling for randomized GCM nonces — and stops being negligible
/// beyond it. Rotating the storage key opens a new epoch with fresh key
/// material and resets the count.
///
/// (Blobs are unaffected: their nonce is derived, not random, and they use
/// GCM-SIV precisely so that a derivation collision is not a catastrophe.)
pub const LOG_RECORDS_PER_EPOCH_LIMIT: u64 = 1 << 32;

/// Where [`KeyManifest::storage_rotation_due`] starts saying yes.
///
/// Half the ceiling, so an operator has an epoch's worth of warning rather than
/// an alarm at the moment the bound is reached.
pub const LOG_RECORDS_PER_EPOCH_WARN: u64 = LOG_RECORDS_PER_EPOCH_LIMIT / 2;

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
    /// Transaction number the epoch was opened at.
    ///
    /// This is the log-record nonce budget's meter. Log records use AES-256-GCM
    /// with a random nonce, so the number sealed under one key must stay well
    /// below [`LOG_RECORDS_PER_EPOCH_LIMIT`] — and that number is exactly the
    /// span of `t` the epoch covers, because the log seals one record per
    /// transaction. Recording where an epoch started therefore measures its
    /// budget with no counter to maintain and no write amplification: the next
    /// epoch's `opened_at_t` (or the current basis, for the active epoch) ends
    /// the span. See [`KeyManifest::log_records_sealed`].
    pub opened_at_t: u64,
    /// Where the epoch sits in its lifecycle.
    pub state: StorageKeyState,
    /// Live objects carrying this epoch as of the last mark pass. An epoch
    /// retires only at zero; `corium keys status` prints it so a drain is a
    /// number rather than a guess. This counts stored objects for GC drain, not
    /// records sealed — the nonce budget is `opened_at_t`'s job.
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
    /// A database is created empty, so its first epoch opens at `t` 0 and
    /// covers every transaction until the first rotation.
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
            .mint_storage_key(keyring, 1, created_at_unix_ms, 0)
            .await?;
        Ok(manifest)
    }

    /// Returns the epoch new writes use, if any epoch is active.
    ///
    /// [`Self::validate`] admits at most one active epoch, so the `max` below
    /// is a tie-break that a decoded manifest never needs; it keeps a manifest
    /// assembled in memory from silently writing under a superseded key.
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

    /// Log records sealed under one epoch as of basis `current_t`.
    ///
    /// An epoch covers `t` from where it opened to where its successor did, or
    /// to the current basis if it is the newest. The log seals one record per
    /// transaction, so that span *is* the number of nonces drawn under the
    /// epoch's key — see [`LOG_RECORDS_PER_EPOCH_LIMIT`].
    ///
    /// Returns `None` for an epoch the manifest does not carry.
    #[must_use]
    pub fn log_records_sealed(&self, epoch: u32, current_t: u64) -> Option<u64> {
        let key = self.storage_key(epoch)?;
        let end = self
            .storage_keys
            .iter()
            .filter(|other| other.epoch > epoch)
            .map(|other| other.opened_at_t)
            .min()
            .unwrap_or(current_t);
        // A manifest read at a basis older than its newest epoch (or a
        // successor that opened at the same `t`) yields an empty span, not a
        // negative one.
        Some(end.saturating_sub(key.opened_at_t))
    }

    /// Reports whether the active epoch has drawn enough nonces to want a
    /// rotation, at [`LOG_RECORDS_PER_EPOCH_WARN`].
    #[must_use]
    pub fn storage_rotation_due(&self, current_t: u64) -> bool {
        self.active_storage_epoch()
            .and_then(|epoch| self.log_records_sealed(epoch, current_t))
            .is_some_and(|sealed| sealed >= LOG_RECORDS_PER_EPOCH_WARN)
    }

    /// Opens a new storage epoch that new writes will use, from basis
    /// `current_t`.
    ///
    /// Rotation is layered and cheap: no stored object is rewritten. The
    /// previous active epoch becomes `Retiring` and stays readable until
    /// ordinary re-indexing drains it. `current_t` closes the outgoing epoch's
    /// nonce budget and opens the new one's, so it must be the basis the
    /// rotation commits at.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::Keyring`] when the new key cannot be wrapped,
    /// [`StoreError::Encryption`] when randomness is unavailable, and
    /// [`StoreError::InvalidKeyManifest`] when `current_t` precedes the newest
    /// epoch's opening, which would leave the epochs unordered in `t`.
    pub async fn rotate_storage_key(
        &mut self,
        keyring: &dyn Keyring,
        created_at_unix_ms: i64,
        current_t: u64,
    ) -> Result<u32, StoreError> {
        let epoch = self
            .storage_keys
            .iter()
            .map(|key| key.epoch)
            .max()
            .unwrap_or(0)
            .checked_add(1)
            .ok_or(StoreError::StorageEpochExhausted)?;
        if self
            .storage_keys
            .iter()
            .any(|key| key.opened_at_t > current_t)
        {
            return Err(invalid("rotation basis precedes an existing epoch"));
        }
        for key in &mut self.storage_keys {
            if key.state == StorageKeyState::Active {
                key.state = StorageKeyState::Retiring;
            }
        }
        self.mint_storage_key(keyring, epoch, created_at_unix_ms, current_t)
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
        opened_at_t: u64,
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
            opened_at_t,
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
                "{} {} {} {} {} {} {} {}",
                key.epoch,
                key.kek_epoch,
                key.algorithm,
                key.state,
                key.created_at_unix_ms,
                key.opened_at_t,
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
        if lines.next().is_some() {
            return Err(invalid("trailing content after the class entries"));
        }
        let manifest = Self {
            format_version,
            kek,
            storage_keys,
            classes,
        };
        manifest.validate()?;
        Ok(manifest)
    }

    /// Rejects a manifest that is well-formed but structurally impossible.
    ///
    /// This record bootstraps every storage access a process makes, and it is
    /// cleartext, so "it parsed" is not the bar. A manifest that names an epoch
    /// twice, or none to write under, or two, describes a database that cannot
    /// exist — and each of those would surface much later as a wrong key, a
    /// failed open, or a silent choice between two active epochs.
    ///
    /// # Errors
    ///
    /// Returns [`StoreError::InvalidKeyManifest`] naming the violated
    /// invariant.
    pub fn validate(&self) -> Result<(), StoreError> {
        // Strictly ascending subsumes uniqueness, and it is what `encode`
        // emits, so a decode/encode round trip is byte-stable — which the
        // manifest's compare-and-set update path depends on.
        for pair in self.storage_keys.windows(2) {
            if pair[0].epoch >= pair[1].epoch {
                return Err(invalid("storage-key epochs are not strictly ascending"));
            }
            if pair[0].opened_at_t > pair[1].opened_at_t {
                return Err(invalid("storage-key epochs are not ordered in t"));
            }
        }
        for pair in self.classes.windows(2) {
            if pair[0].class >= pair[1].class {
                return Err(invalid("class entries are not strictly ascending"));
            }
        }
        let active = self
            .storage_keys
            .iter()
            .filter(|key| key.state == StorageKeyState::Active)
            .count();
        // An empty manifest is legal — it is what a database with classes but
        // no storage encryption would hold — but a populated one has exactly
        // one epoch that new writes go to.
        if !self.storage_keys.is_empty() && active != 1 {
            return Err(invalid(if active == 0 {
                "no active storage-key epoch"
            } else {
                "more than one active storage-key epoch"
            }));
        }
        Ok(())
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
    let opened_at_t = next()?
        .parse()
        .map_err(|_| invalid("invalid storage-key basis"))?;
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
        opened_at_t,
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
                .rotate_storage_key(&keyring, 2, 5_000)
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
    async fn an_epochs_nonce_budget_is_the_span_of_t_it_covers() {
        let keyring = keyring();
        let mut manifest = KeyManifest::create(&keyring, kek(), 1)
            .await
            .expect("create");
        // The first epoch opens on an empty database and covers everything so
        // far; there is no counter to maintain, only where it started.
        assert_eq!(manifest.log_records_sealed(1, 900), Some(900));
        assert!(!manifest.storage_rotation_due(900));
        assert!(manifest.storage_rotation_due(LOG_RECORDS_PER_EPOCH_WARN));

        manifest
            .rotate_storage_key(&keyring, 2, 1_000)
            .await
            .expect("rotate");
        // Rotation closes the old span and opens a new one, so the budget the
        // warning watches resets while the retired epoch's stays fixed.
        assert_eq!(manifest.log_records_sealed(1, 4_000), Some(1_000));
        assert_eq!(manifest.log_records_sealed(2, 4_000), Some(3_000));
        assert_eq!(manifest.log_records_sealed(3, 4_000), None);
        assert!(!manifest.storage_rotation_due(4_000));
        assert!(manifest.storage_rotation_due(1_000 + LOG_RECORDS_PER_EPOCH_WARN));

        // A basis behind the newest epoch reports an empty span rather than
        // underflowing.
        assert_eq!(manifest.log_records_sealed(2, 10), Some(0));
        // ...and cannot be used to open one, which would leave the epochs
        // unordered in `t` and make every span meaningless.
        assert!(matches!(
            manifest.rotate_storage_key(&keyring, 3, 10).await,
            Err(StoreError::InvalidKeyManifest(_))
        ));
    }

    #[tokio::test]
    async fn structurally_impossible_manifests_are_rejected() {
        let keyring = keyring();
        let mut manifest = KeyManifest::create(&keyring, kek(), 1)
            .await
            .expect("create");
        manifest
            .rotate_storage_key(&keyring, 2, 1_000)
            .await
            .expect("rotate");
        assert!(manifest.validate().is_ok());
        assert!(KeyManifest::decode(&manifest.encode()).is_ok());

        // Two epochs claiming new writes: a reader would have to guess.
        let mut two_active = manifest.clone();
        two_active.storage_keys[0].state = StorageKeyState::Active;
        assert!(two_active.validate().is_err());
        assert!(KeyManifest::decode(&two_active.encode()).is_err());

        // None: nothing can be written, and nothing says so.
        let mut none_active = manifest.clone();
        for key in &mut none_active.storage_keys {
            key.state = StorageKeyState::Retired;
        }
        assert!(none_active.validate().is_err());

        // A duplicated epoch resolves to whichever entry is found first.
        let mut duplicate = manifest.clone();
        duplicate.storage_keys[1].epoch = 1;
        assert!(duplicate.validate().is_err());

        // Epochs running backwards in `t` make every nonce budget nonsense.
        let mut unordered = manifest.clone();
        unordered.storage_keys[0].opened_at_t = 9_999;
        assert!(unordered.validate().is_err());

        // Classes are keyed by entity id; a repeat is two answers to one
        // question.
        let mut classes = manifest;
        classes.classes = vec![
            ProtectionClassKey {
                class: 74,
                key_id: KeyId::new("file:a").expect("key"),
                current_epoch: 1,
            },
            ProtectionClassKey {
                class: 74,
                key_id: KeyId::new("file:b").expect("key"),
                current_epoch: 1,
            },
        ];
        assert!(classes.validate().is_err());
        assert!(KeyManifest::decode(&classes.encode()).is_err());
    }

    #[test]
    fn trailing_content_after_the_entries_is_rejected() {
        let mut encoded = b"corium-keys-v1\nfile:kek\n0\n0\n".to_vec();
        assert!(KeyManifest::decode(&encoded).is_ok());
        encoded.extend_from_slice(b"1 1 aes-256 active 0 0 0 00\n");
        assert!(matches!(
            KeyManifest::decode(&encoded),
            Err(StoreError::InvalidKeyManifest(_))
        ));
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
            .rotate_storage_key(&keyring, 2, 5_000)
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
            // Wrapped key is not hex.
            b"corium-keys-v1\nfile:kek\n1\n1 1 aes-256 active 0 0 0 xyz\n0\n".as_slice(),
            // Unknown lifecycle state.
            b"corium-keys-v1\nfile:kek\n1\n1 1 aes-256 sideways 0 0 0 00\n0\n".as_slice(),
            // One field short of a storage key.
            b"corium-keys-v1\nfile:kek\n1\n1 1 aes-256 active 0 0 00\n0\n".as_slice(),
            // Empty key-encryption key identity.
            b"corium-keys-v1\n\n0\n0\n".as_slice(),
        ] {
            assert!(
                KeyManifest::decode(bytes).is_err(),
                "accepted {:?}",
                String::from_utf8_lossy(bytes)
            );
        }
        assert!(matches!(
            KeyManifest::decode(b"corium-keys-v1\nfile:kek\n1\n1 1 rot13 active 0 0 0 00\n0\n"),
            Err(StoreError::UnsupportedKeyAlgorithm(_))
        ));
    }
}
