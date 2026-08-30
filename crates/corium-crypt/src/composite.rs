//! A keyring assembled from several others.

use std::sync::Arc;

use async_trait::async_trait;

use crate::{KeyError, KeyId, Keyring, SecretKey};

/// Tries each keyring in order for the identity it is asked about.
///
/// One process routinely holds keys from more than one source — a storage KEK
/// in an operator file and a protection-class key in a KMS is the deployment
/// the encryption design describes — and the manifest and schema name those
/// keys by identity, not by where they live. This is the seam that lets the
/// rest of the system keep asking a single `dyn Keyring`.
///
/// A ring that reports an identity in [`Keyring::key_ids`] is asked first;
/// nothing else is consulted for it, so a misconfigured key fails naming its
/// own source rather than falling through to an unrelated one. An identity no
/// ring claims falls through the list in order, which is what lets a keyring
/// that resolves identities dynamically sit at the end.
pub struct CompositeKeyring {
    rings: Vec<Arc<dyn Keyring>>,
    key_ids: Vec<KeyId>,
}

impl std::fmt::Debug for CompositeKeyring {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompositeKeyring")
            .field("rings", &self.rings.len())
            .field("key_ids", &self.key_ids)
            .finish()
    }
}

impl CompositeKeyring {
    /// Composes `rings`, preferring earlier ones.
    #[must_use]
    pub fn new(rings: impl IntoIterator<Item = Arc<dyn Keyring>>) -> Self {
        let rings: Vec<Arc<dyn Keyring>> = rings.into_iter().collect();
        let mut key_ids: Vec<KeyId> = rings
            .iter()
            .flat_map(|ring| ring.key_ids().iter().cloned())
            .collect();
        key_ids.sort();
        key_ids.dedup();
        Self { rings, key_ids }
    }

    /// The rings to consult for `id`: the ones claiming it, or all of them
    /// when none does.
    fn candidates(&self, id: &KeyId) -> Vec<&Arc<dyn Keyring>> {
        let claiming: Vec<&Arc<dyn Keyring>> = self
            .rings
            .iter()
            .filter(|ring| ring.key_ids().contains(id))
            .collect();
        if claiming.is_empty() {
            self.rings.iter().collect()
        } else {
            claiming
        }
    }
}

/// Runs `operation` over each candidate ring, keeping the first success and
/// otherwise the first failure — which is the one that named the key source a
/// reader configured, rather than the last ring's report that it never had it.
macro_rules! first_success {
    ($self:expr, $id:expr, $fallback:expr, |$ring:ident| $operation:expr) => {{
        let mut first_error = None;
        for $ring in $self.candidates($id) {
            match $operation.await {
                Ok(value) => return Ok(value),
                Err(error) => first_error.get_or_insert(error),
            };
        }
        Err(first_error.unwrap_or_else(|| $fallback))
    }};
}

#[async_trait]
impl Keyring for CompositeKeyring {
    async fn key(&self, id: &KeyId, epoch: u32) -> Result<SecretKey, KeyError> {
        first_success!(
            self,
            id,
            KeyError::MissingKey {
                id: id.clone(),
                epoch,
            },
            |ring| ring.key(id, epoch)
        )
    }

    async fn current_epoch(&self, id: &KeyId) -> Result<u32, KeyError> {
        first_success!(
            self,
            id,
            KeyError::MissingCurrentEpoch(id.clone()),
            |ring| ring.current_epoch(id)
        )
    }

    async fn wrap(&self, id: &KeyId, epoch: u32, dek: &SecretKey) -> Result<Vec<u8>, KeyError> {
        first_success!(
            self,
            id,
            KeyError::MissingKey {
                id: id.clone(),
                epoch,
            },
            |ring| ring.wrap(id, epoch, dek)
        )
    }

    async fn unwrap(&self, id: &KeyId, epoch: u32, wrapped: &[u8]) -> Result<SecretKey, KeyError> {
        first_success!(
            self,
            id,
            KeyError::MissingKey {
                id: id.clone(),
                epoch,
            },
            |ring| ring.unwrap(id, epoch, wrapped)
        )
    }

    fn key_ids(&self) -> &[KeyId] {
        &self.key_ids
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{STATIC_KEY_EPOCH, StaticKeyring};

    fn key_id(text: &str) -> KeyId {
        KeyId::new(text).expect("key id")
    }

    fn ring(id: &str, byte: u8) -> Arc<dyn Keyring> {
        let mut keyring = StaticKeyring::default();
        keyring.insert(
            key_id(id),
            STATIC_KEY_EPOCH,
            SecretKey::new([byte; 32]),
            true,
        );
        Arc::new(keyring)
    }

    #[tokio::test]
    async fn each_identity_resolves_through_the_ring_that_holds_it() {
        let composite = CompositeKeyring::new([ring("file:storage", 1), ring("env:PII", 2)]);
        assert_eq!(
            composite.key_ids(),
            &[key_id("env:PII"), key_id("file:storage")]
        );
        assert_eq!(
            composite
                .key(&key_id("file:storage"), STATIC_KEY_EPOCH)
                .await
                .expect("storage key"),
            SecretKey::new([1; 32])
        );
        assert_eq!(
            composite
                .key(&key_id("env:PII"), STATIC_KEY_EPOCH)
                .await
                .expect("class key"),
            SecretKey::new([2; 32])
        );
        assert_eq!(
            composite
                .current_epoch(&key_id("env:PII"))
                .await
                .expect("epoch"),
            STATIC_KEY_EPOCH
        );
    }

    #[tokio::test]
    async fn an_earlier_ring_wins_a_shared_identity() {
        let composite = CompositeKeyring::new([ring("file:kek", 1), ring("file:kek", 2)]);
        let id = key_id("file:kek");
        assert_eq!(composite.key_ids(), std::slice::from_ref(&id));
        assert_eq!(
            composite.key(&id, STATIC_KEY_EPOCH).await.expect("key"),
            SecretKey::new([1; 32])
        );
        // Wrapping and unwrapping stay on the same ring, so a data key wrapped
        // through the composite unwraps through it too.
        let wrapped = composite
            .wrap(&id, STATIC_KEY_EPOCH, &SecretKey::new([7; 32]))
            .await
            .expect("wrap");
        assert_eq!(
            composite
                .unwrap(&id, STATIC_KEY_EPOCH, &wrapped)
                .await
                .expect("unwrap"),
            SecretKey::new([7; 32])
        );
    }

    #[tokio::test]
    async fn an_identity_no_ring_holds_is_missing() {
        let composite = CompositeKeyring::new([ring("file:storage", 1)]);
        assert!(matches!(
            composite.key(&key_id("file:absent"), 1).await,
            Err(KeyError::MissingKey { .. })
        ));
        assert!(matches!(
            composite.current_epoch(&key_id("file:absent")).await,
            Err(KeyError::MissingCurrentEpoch(_))
        ));
        assert!(matches!(
            composite.unwrap(&key_id("file:absent"), 1, &[]).await,
            Err(KeyError::MissingKey { .. })
        ));
    }

    #[tokio::test]
    async fn an_empty_composite_resolves_nothing() {
        let composite = CompositeKeyring::new([]);
        assert!(composite.key_ids().is_empty());
        assert!(matches!(
            composite.key(&key_id("file:storage"), 1).await,
            Err(KeyError::MissingKey { .. })
        ));
    }
}
