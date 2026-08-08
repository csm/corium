//! A key service that runs in the process under test.
//!
//! Behind the `testing` feature, so it is available to Corium's own
//! integration tests and to anyone wiring a [`KmsClient`](crate::KmsClient) of
//! their own, without shipping in a release build.
//!
//! [`InMemoryKms`] holds its key the way a real service does — nothing hands it
//! out — so a test that opens a database through it proves the remote path
//! rather than a local file wearing a different scheme.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use async_trait::async_trait;

use crate::{KeyId, KmsClient, KmsError, SecretKey, decrypt_blob, derive_key, encrypt_blob};

/// An in-process stand-in for a key-management service.
#[derive(Debug)]
pub struct InMemoryKms {
    key: SecretKey,
    offline: AtomicBool,
    calls: AtomicUsize,
}

impl InMemoryKms {
    /// A service whose key is `byte` repeated — distinctive enough that a test
    /// can assert it never reached storage.
    #[must_use]
    pub fn new(byte: u8) -> Self {
        Self::with_key(SecretKey::new([byte; 32]))
    }

    /// A service holding specific key material.
    #[must_use]
    pub fn with_key(key: SecretKey) -> Self {
        Self {
            key,
            offline: AtomicBool::new(false),
            calls: AtomicUsize::new(0),
        }
    }

    /// Makes every later call fail as unreachable, or stops doing so.
    ///
    /// This is the "KMS unreachable" row of the encryption design's failure
    /// table: what a keyring already resolved must keep serving.
    pub fn set_offline(&self, offline: bool) {
        self.offline.store(offline, Ordering::Relaxed);
    }

    /// How many calls this service has answered or refused.
    #[must_use]
    pub fn calls(&self) -> usize {
        self.calls.load(Ordering::Relaxed)
    }

    fn reached(&self) -> Result<(), KmsError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if self.offline.load(Ordering::Relaxed) {
            return Err(KmsError::Unavailable("connection refused".into()));
        }
        Ok(())
    }

    /// The per-(key, epoch) wrapping key, so a wrapped data key is bound to
    /// the epoch it was wrapped under exactly as an encryption context binds
    /// it at a real service.
    fn wrapping_key(&self, key: &KeyId, epoch: u32) -> SecretKey {
        let mut context = b"corium/test-kms-wrap\0".to_vec();
        context.extend_from_slice(key.as_str().as_bytes());
        context.extend_from_slice(&epoch.to_be_bytes());
        derive_key(&self.key, &context)
    }
}

#[async_trait]
impl KmsClient for InMemoryKms {
    async fn current_epoch(&self, _key: &KeyId) -> Result<u32, KmsError> {
        self.reached()?;
        Ok(crate::STATIC_KEY_EPOCH)
    }

    async fn wrap(&self, key: &KeyId, epoch: u32, dek: &[u8]) -> Result<Vec<u8>, KmsError> {
        self.reached()?;
        encrypt_blob(&self.wrapping_key(key, epoch), epoch, dek)
            .map_err(|error| KmsError::Rejected(error.to_string()))
    }

    async fn unwrap(&self, key: &KeyId, epoch: u32, wrapped: &[u8]) -> Result<SecretKey, KmsError> {
        self.reached()?;
        let plaintext = decrypt_blob(&self.wrapping_key(key, epoch), wrapped)
            .map_err(|error| KmsError::Rejected(error.to_string()))?;
        SecretKey::from_slice(&plaintext).map_err(|_| KmsError::InvalidMaterial)
    }

    async fn derive(&self, _key: &KeyId, context: &[u8]) -> Result<SecretKey, KmsError> {
        self.reached()?;
        Ok(derive_key(&self.key, context))
    }
}
