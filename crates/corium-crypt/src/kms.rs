//! KMS-backed key resolution.
//!
//! A [`StaticKeyring`](crate::StaticKeyring) holds material this process can
//! read: a file, an environment variable, a test key. A KMS holds material
//! this process must never read. The split in [`KmsClient`] follows that
//! difference exactly — wrapping and unwrapping a data key happen *inside* the
//! key service, and the only material that ever crosses the boundary is either
//! an already-wrapped data key or the output of a keyed MAC, never the key
//! that produced it.
//!
//! Two remote operations, two uses:
//!
//! - **Wrap/unwrap** carries the storage data keys recorded in `keys:<db>`.
//!   The key-encryption key stays remote; Corium stores only ciphertext.
//! - **Derive** produces the *local* material a protection class needs, since
//!   sealing a value is a computation on the peer holding it. It is a keyed MAC
//!   over a context naming the key identity and its epoch, which makes it a
//!   pseudorandom function of that context: deterministic, so two peers holding
//!   the same grant seal identically; per-epoch, so `corium keys rotate
//!   --class` changes the context rather than requiring new KMS key material;
//!   and one-way, so a derived class key never discloses the KMS key behind it.
//!
//! [`KmsKeyring`] caches what it resolves. The design puts KMS access at
//! database open and manifest reload rather than on a blob read
//! ([`docs/design/encryption.md`]), and that cache is also what makes a KMS
//! outage degrade the way the design's failure table requires: material
//! already resolved keeps serving, and only work needing material this process
//! has never held fails.
//!
//! [`docs/design/encryption.md`]: https://github.com/csm/corium/blob/main/docs/design/encryption.md

use std::collections::BTreeMap;
use std::fmt;
use std::sync::RwLock;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use thiserror::Error;

use crate::{KeyError, KeyId, Keyring, SecretKey};

/// Failures reported by a [`KmsClient`].
///
/// The distinction that matters to a caller is [`KmsError::Unavailable`] — the
/// key service could not be reached, so the request may succeed later — versus
/// the rest, which fail identically until a human changes something.
#[derive(Debug, Error)]
pub enum KmsError {
    /// The key service could not be reached, or failed retryably.
    #[error("key service is unavailable: {0}")]
    Unavailable(String),
    /// The key service answered, and refused.
    #[error("key service refused the request: {0}")]
    Rejected(String),
    /// The key service answered with material Corium cannot use.
    #[error("key service returned unusable key material")]
    InvalidMaterial,
    /// This client cannot perform the operation at all.
    #[error("key service does not support {0}")]
    Unsupported(&'static str),
}

impl KmsError {
    /// Whether the request may succeed on a later attempt.
    #[must_use]
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::Unavailable(_))
    }
}

/// Remote key operations, as narrow as Corium's use of a key service.
///
/// Implementations are held behind an `Arc` for the life of a process, so one
/// client can back several keyrings.
#[async_trait]
pub trait KmsClient: Send + Sync + fmt::Debug {
    /// Returns the epoch new wraps should use for this key.
    ///
    /// A service that versions keys internally — and resolves the version from
    /// the ciphertext when unwrapping — has one epoch as far as Corium is
    /// concerned. Rotating such a key is `corium keys rewrap --kek`, naming the
    /// new identity, rather than an epoch Corium counts.
    async fn current_epoch(&self, key: &KeyId) -> Result<u32, KmsError>;

    /// Wraps 32 bytes of data-key material under the remote key, binding
    /// `epoch` to the result so it cannot be replayed under another one.
    ///
    /// `dek` borrows zeroized storage; an implementation must not copy it into
    /// a buffer that outlives the call.
    async fn wrap(&self, key: &KeyId, epoch: u32, dek: &[u8]) -> Result<Vec<u8>, KmsError>;

    /// Unwraps a stored data key, verifying the binding [`KmsClient::wrap`]
    /// created.
    async fn unwrap(&self, key: &KeyId, epoch: u32, wrapped: &[u8]) -> Result<SecretKey, KmsError>;

    /// Derives locally usable material from the remote key and `context`.
    ///
    /// Must be a deterministic pseudorandom function of `(key, context)`: a
    /// value sealed under a protection class has to open in every other
    /// process holding the same grant, at any later time, with no coordination.
    async fn derive(&self, key: &KeyId, context: &[u8]) -> Result<SecretKey, KmsError>;
}

/// A keyring whose material lives in a key-management service.
///
/// It serves exactly the identities it was constructed with. An identity it
/// does not serve is reported as missing rather than guessed at, which is what
/// lets [`CompositeKeyring`](crate::CompositeKeyring) put a file-backed storage
/// key and a KMS-backed class key in one process.
pub struct KmsKeyring {
    client: std::sync::Arc<dyn KmsClient>,
    key_ids: Vec<KeyId>,
    derived: RwLock<BTreeMap<(KeyId, u32), SecretKey>>,
    epochs: RwLock<BTreeMap<KeyId, u32>>,
    unavailable: AtomicBool,
}

impl fmt::Debug for KmsKeyring {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KmsKeyring")
            .field("client", &self.client)
            .field("key_ids", &self.key_ids)
            .field("unavailable", &self.unavailable())
            .finish_non_exhaustive()
    }
}

impl KmsKeyring {
    /// Serves `key_ids` through `client`.
    #[must_use]
    pub fn new(
        client: std::sync::Arc<dyn KmsClient>,
        key_ids: impl IntoIterator<Item = KeyId>,
    ) -> Self {
        let mut key_ids: Vec<KeyId> = key_ids.into_iter().collect();
        key_ids.sort();
        key_ids.dedup();
        Self {
            client,
            key_ids,
            derived: RwLock::new(BTreeMap::new()),
            epochs: RwLock::new(BTreeMap::new()),
            unavailable: AtomicBool::new(false),
        }
    }

    /// Whether the last remote call failed because the service was unreachable.
    ///
    /// Cached material keeps serving while this is set, which is the degraded
    /// mode the encryption design requires: reads continue, and only work
    /// needing material this process has never resolved fails.
    #[must_use]
    pub fn unavailable(&self) -> bool {
        self.unavailable.load(Ordering::Relaxed)
    }

    /// The context a class key is derived over.
    ///
    /// Naming the identity keeps two classes pointed at the same KMS key from
    /// sharing material; naming the epoch is what makes a class rotation fresh
    /// material without a fresh KMS key.
    fn derivation_context(id: &KeyId, epoch: u32) -> Vec<u8> {
        let mut context = Vec::with_capacity(id.as_str().len() + 32);
        context.extend_from_slice(b"corium/kms-derived-key\0");
        context.extend_from_slice(id.as_str().as_bytes());
        context.push(0);
        context.extend_from_slice(&epoch.to_be_bytes());
        context
    }

    fn serves(&self, id: &KeyId) -> bool {
        self.key_ids.binary_search(id).is_ok()
    }

    /// Records reachability and converts a remote failure into a key failure.
    fn observe(&self, id: &KeyId, error: KmsError) -> KeyError {
        self.unavailable
            .store(error.is_transient(), Ordering::Relaxed);
        match error {
            KmsError::InvalidMaterial => KeyError::InvalidMaterial(id.clone()),
            error => KeyError::Kms {
                id: id.clone(),
                reason: error.to_string(),
            },
        }
    }

    fn reached(&self) {
        self.unavailable.store(false, Ordering::Relaxed);
    }
}

#[async_trait]
impl Keyring for KmsKeyring {
    async fn key(&self, id: &KeyId, epoch: u32) -> Result<SecretKey, KeyError> {
        if !self.serves(id) {
            return Err(KeyError::MissingKey {
                id: id.clone(),
                epoch,
            });
        }
        if let Some(key) = self
            .derived
            .read()
            .expect("derived key cache is not poisoned")
            .get(&(id.clone(), epoch))
        {
            return Ok(key.clone());
        }
        let key = self
            .client
            .derive(id, &Self::derivation_context(id, epoch))
            .await
            .map_err(|error| self.observe(id, error))?;
        self.reached();
        self.derived
            .write()
            .expect("derived key cache is not poisoned")
            .insert((id.clone(), epoch), key.clone());
        Ok(key)
    }

    async fn current_epoch(&self, id: &KeyId) -> Result<u32, KeyError> {
        if !self.serves(id) {
            return Err(KeyError::MissingCurrentEpoch(id.clone()));
        }
        if let Some(epoch) = self
            .epochs
            .read()
            .expect("key epoch cache is not poisoned")
            .get(id)
        {
            return Ok(*epoch);
        }
        let epoch = self
            .client
            .current_epoch(id)
            .await
            .map_err(|error| self.observe(id, error))?;
        self.reached();
        self.epochs
            .write()
            .expect("key epoch cache is not poisoned")
            .insert(id.clone(), epoch);
        Ok(epoch)
    }

    async fn wrap(&self, id: &KeyId, epoch: u32, dek: &SecretKey) -> Result<Vec<u8>, KeyError> {
        if !self.serves(id) {
            return Err(KeyError::MissingKey {
                id: id.clone(),
                epoch,
            });
        }
        let wrapped = self
            .client
            .wrap(id, epoch, dek.as_bytes())
            .await
            .map_err(|error| self.observe(id, error))?;
        self.reached();
        Ok(wrapped)
    }

    async fn unwrap(&self, id: &KeyId, epoch: u32, wrapped: &[u8]) -> Result<SecretKey, KeyError> {
        if !self.serves(id) {
            return Err(KeyError::MissingKey {
                id: id.clone(),
                epoch,
            });
        }
        let key = self
            .client
            .unwrap(id, epoch, wrapped)
            .await
            .map_err(|error| self.observe(id, error))?;
        self.reached();
        Ok(key)
    }

    fn key_ids(&self) -> &[KeyId] {
        &self.key_ids
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::testing::InMemoryKms;
    use crate::{CompositeKeyring, STATIC_KEY_EPOCH, StaticKeyring};

    fn key_id(text: &str) -> KeyId {
        KeyId::new(text).expect("key id")
    }

    fn keyring(service: &Arc<InMemoryKms>, ids: &[&str]) -> KmsKeyring {
        KmsKeyring::new(
            Arc::clone(service) as Arc<dyn KmsClient>,
            ids.iter().map(|id| key_id(id)).collect::<Vec<_>>(),
        )
    }

    fn service(byte: u8) -> Arc<InMemoryKms> {
        Arc::new(InMemoryKms::new(byte))
    }

    #[tokio::test]
    async fn wrapped_data_keys_round_trip_through_the_service() {
        let arn = "awskms:arn:aws:kms:us-west-2:111122223333:key/2f1c";
        let ring = keyring(&service(1), &[arn]);
        let id = key_id(arn);
        let dek = SecretKey::new([9; 32]);

        assert_eq!(ring.current_epoch(&id).await.expect("epoch"), 1);
        let wrapped = ring.wrap(&id, 1, &dek).await.expect("wrap");
        assert!(!wrapped.windows(32).any(|window| window == dek.as_bytes()));
        assert_eq!(ring.unwrap(&id, 1, &wrapped).await.expect("unwrap"), dek);

        // The epoch is bound remotely: a stored data key cannot be re-read as
        // though it had been wrapped under a different one.
        assert!(matches!(
            ring.unwrap(&id, 2, &wrapped).await,
            Err(KeyError::Kms { .. })
        ));
    }

    #[tokio::test]
    async fn class_keys_are_deterministic_per_identity_and_epoch() {
        let service = service(2);
        let first = keyring(&service, &["awskms:pii", "awskms:audit"]);
        let second = keyring(&service, &["awskms:pii"]);
        let pii = key_id("awskms:pii");

        // Two peers holding the same grant derive the same material, which is
        // what lets one seal a value the other opens.
        let key = first.key(&pii, 1).await.expect("derive");
        assert_eq!(second.key(&pii, 1).await.expect("derive"), key);
        // A rotation and a different class each mean different material.
        assert_ne!(first.key(&pii, 2).await.expect("rotate"), key);
        assert_ne!(
            first.key(&key_id("awskms:audit"), 1).await.expect("other"),
            key
        );
        // And nothing derived is the service's own key.
        assert_ne!(key, SecretKey::new([2; 32]));
    }

    #[tokio::test]
    async fn resolved_material_survives_a_service_outage() {
        let service = service(3);
        let ring = keyring(&service, &["awskms:pii"]);
        let id = key_id("awskms:pii");
        let key = ring.key(&id, 1).await.expect("derive");
        assert!(!ring.unavailable());

        // Take the service away. Material already resolved keeps serving from
        // cache with no call at all; an epoch never resolved fails, and says
        // the service is why.
        service.set_offline(true);
        let calls = service.calls();
        assert_eq!(ring.key(&id, 1).await.expect("cached"), key);
        assert_eq!(service.calls(), calls);
        assert!(!ring.unavailable());

        assert!(matches!(ring.key(&id, 2).await, Err(KeyError::Kms { .. })));
        assert!(ring.unavailable());

        // Recovery clears the flag, and the epoch resolves.
        service.set_offline(false);
        assert!(ring.key(&id, 2).await.is_ok());
        assert!(!ring.unavailable());
    }

    #[tokio::test]
    async fn identities_this_ring_does_not_serve_are_missing_not_guessed() {
        let ring = keyring(&service(4), &["awskms:pii"]);
        let other = key_id("awskms:payroll");
        assert!(matches!(
            ring.key(&other, 1).await,
            Err(KeyError::MissingKey { .. })
        ));
        assert!(matches!(
            ring.current_epoch(&other).await,
            Err(KeyError::MissingCurrentEpoch(_))
        ));
        assert!(matches!(
            ring.wrap(&other, 1, &SecretKey::new([0; 32])).await,
            Err(KeyError::MissingKey { .. })
        ));
        assert!(matches!(
            ring.unwrap(&other, 1, &[]).await,
            Err(KeyError::MissingKey { .. })
        ));
        assert_eq!(ring.key_ids(), &[key_id("awskms:pii")]);
    }

    #[tokio::test]
    async fn a_kms_key_and_a_local_key_serve_one_process() {
        let mut local = StaticKeyring::default();
        local.insert(
            key_id("file:storage"),
            STATIC_KEY_EPOCH,
            SecretKey::new([5; 32]),
            true,
        );
        let remote = keyring(&service(6), &["awskms:pii"]);
        let composite = CompositeKeyring::new([
            Arc::new(local) as Arc<dyn Keyring>,
            Arc::new(remote) as Arc<dyn Keyring>,
        ]);

        assert_eq!(
            composite.key_ids(),
            &[key_id("awskms:pii"), key_id("file:storage")]
        );
        assert!(composite.key(&key_id("file:storage"), 1).await.is_ok());
        assert!(composite.key(&key_id("awskms:pii"), 1).await.is_ok());
        assert!(matches!(
            composite.key(&key_id("file:absent"), 1).await,
            Err(KeyError::MissingKey { .. })
        ));
    }

    #[test]
    fn derivation_contexts_separate_identities_and_epochs() {
        let contexts = [
            KmsKeyring::derivation_context(&key_id("awskms:pii"), 1),
            KmsKeyring::derivation_context(&key_id("awskms:pii"), 2),
            KmsKeyring::derivation_context(&key_id("awskms:pii\0"), 1),
            KmsKeyring::derivation_context(&key_id("awskms:audit"), 1),
        ];
        for (index, context) in contexts.iter().enumerate() {
            for other in &contexts[index + 1..] {
                assert_ne!(context, other);
            }
        }
    }
}
