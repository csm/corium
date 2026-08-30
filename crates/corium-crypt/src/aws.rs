//! AWS KMS backing for [`KmsKeyring`](crate::KmsKeyring).
//!
//! Behind the `aws-kms` feature. An identity is written `awskms:<key>`, where
//! `<key>` is anything AWS KMS accepts as a key identifier — a key ARN, an
//! alias ARN, `alias/<name>`, or a bare key id. A key ARN names its own
//! region, so one process reaches keys in several regions without configuring
//! any of them; everything else uses the ambient AWS region.
//!
//! Which AWS operation backs which use is decided by what the key can do:
//!
//! - A **symmetric encryption key** wraps storage data keys, through `Encrypt`
//!   and `Decrypt`. The epoch travels in the encryption context, so KMS itself
//!   refuses a wrapped key replayed under another epoch.
//! - An **HMAC key** (`HMAC_256`) backs a protection class, through
//!   `GenerateMac`. KMS computes HMAC-SHA-256 over the derivation context and
//!   returns the tag, which is the class key: deterministic, so peers agree;
//!   one-way, so the class key never discloses the KMS key; and per-epoch,
//!   because the epoch is in the context that gets MAC'd.
//!
//! Credentials, region, and endpoint come from the ambient AWS configuration
//! (`aws_config::defaults`), like the S3 storage backend.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use aws_sdk_kms::Client;
use aws_sdk_kms::config::Region;
use aws_sdk_kms::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_kms::primitives::Blob;
use aws_sdk_kms::types::MacAlgorithmSpec;

use crate::{KeyId, KmsClient, KmsError, STATIC_KEY_EPOCH, SecretKey};

/// The scheme an AWS KMS identity is written under.
pub const AWS_KMS_SCHEME: &str = "awskms";

/// Encryption-context keys. AWS records these on every `Encrypt`, so they are
/// also what a `CloudTrail` reader sees describing why a key was used.
const CONTEXT_PURPOSE: &str = "corium:purpose";
const CONTEXT_EPOCH: &str = "corium:kek-epoch";
const CONTEXT_KEY_ID: &str = "corium:key-id";
const PURPOSE_STORAGE_DEK: &str = "storage-data-key";

/// Resolves Corium key identities through AWS KMS.
///
/// Clients are built per region and cached: a key ARN carries its region, and
/// a deployment whose class keys live beside their data is the reason to
/// support more than one.
#[derive(Debug)]
pub struct AwsKmsClient {
    config: aws_config::SdkConfig,
    clients: Mutex<HashMap<Option<String>, Client>>,
}

impl AwsKmsClient {
    /// Loads the ambient AWS configuration — region, credentials, endpoint.
    ///
    /// No call is made here, so a misconfigured deployment surfaces at its
    /// first key operation, which for an encrypted database is opening it.
    pub async fn from_env() -> Self {
        Self::new(
            aws_config::defaults(aws_config::BehaviorVersion::latest())
                .load()
                .await,
        )
    }

    /// Uses an already-loaded AWS configuration.
    #[must_use]
    pub fn new(config: aws_config::SdkConfig) -> Self {
        Self {
            config,
            clients: Mutex::new(HashMap::new()),
        }
    }

    /// The key identifier AWS sees, with Corium's scheme removed.
    ///
    /// # Errors
    ///
    /// Returns [`KmsError::Rejected`] for an identity that is not an
    /// `awskms:` one, which only happens if a keyring was built with the wrong
    /// client.
    fn key_ref(key: &KeyId) -> Result<&str, KmsError> {
        match key.scheme() {
            Some((AWS_KMS_SCHEME, rest)) if !rest.is_empty() => Ok(rest),
            _ => Err(KmsError::Rejected(format!(
                "{key} is not an {AWS_KMS_SCHEME}: key identity"
            ))),
        }
    }

    /// The region a key ARN names, if it is an ARN.
    ///
    /// `arn:aws:kms:<region>:<account>:key/<id>`; anything else (an alias, a
    /// bare key id) resolves in the ambient region.
    fn region_of(key_ref: &str) -> Option<String> {
        let mut fields = key_ref.split(':');
        (fields.next()? == "arn" && fields.next().is_some() && fields.next()? == "kms")
            .then(|| fields.next().filter(|region| !region.is_empty()))
            .flatten()
            .map(str::to_owned)
    }

    fn client(&self, key_ref: &str) -> Client {
        let region = Self::region_of(key_ref);
        let mut clients = self.clients.lock().expect("KMS client cache");
        clients
            .entry(region.clone())
            .or_insert_with(|| {
                let mut builder = aws_sdk_kms::config::Builder::from(&self.config);
                if let Some(region) = region {
                    builder = builder.region(Region::new(region));
                }
                Client::from_conf(builder.build())
            })
            .clone()
    }

    /// The encryption context binding a wrapped storage data key.
    ///
    /// Naming the identity and the epoch means KMS enforces both: a data key
    /// wrapped for one database's KEK epoch cannot be unwrapped as another's,
    /// even by a caller that can decrypt under the key.
    fn wrap_context(key: &KeyId, epoch: u32) -> [(String, String); 3] {
        [
            (CONTEXT_PURPOSE.to_owned(), PURPOSE_STORAGE_DEK.to_owned()),
            (CONTEXT_KEY_ID.to_owned(), key.as_str().to_owned()),
            (CONTEXT_EPOCH.to_owned(), epoch.to_string()),
        ]
    }
}

/// Classifies an AWS failure as retryable or not.
///
/// Transport failures and the KMS errors AWS documents as retryable are
/// [`KmsError::Unavailable`], so a keyring keeps serving cached material and
/// an operator reads "the service is down", not "the key is wrong".
fn classify<E, R>(operation: &'static str, error: &SdkError<E, R>) -> KmsError
where
    E: ProvideErrorMetadata,
{
    let transient = matches!(
        error,
        SdkError::DispatchFailure(_) | SdkError::TimeoutError(_)
    ) || matches!(
        error.code(),
        Some(
            "KMSInternalException"
                | "KeyUnavailableException"
                | "DependencyTimeoutException"
                | "ThrottlingException"
                | "LimitExceededException"
        )
    );
    let reason = format!(
        "{operation}: {}",
        error.message().map_or_else(
            || error.code().unwrap_or("no detail").to_owned(),
            str::to_owned
        )
    );
    if transient {
        KmsError::Unavailable(reason)
    } else {
        KmsError::Rejected(reason)
    }
}

#[async_trait]
impl KmsClient for AwsKmsClient {
    /// KMS versions a key internally and picks the version from the ciphertext
    /// when decrypting, so a Corium epoch is not a KMS key version. Rotating
    /// the KEK means naming another key with `corium keys rewrap --kek`.
    async fn current_epoch(&self, key: &KeyId) -> Result<u32, KmsError> {
        AwsKmsClient::key_ref(key)?;
        Ok(STATIC_KEY_EPOCH)
    }

    async fn wrap(&self, key: &KeyId, epoch: u32, dek: &[u8]) -> Result<Vec<u8>, KmsError> {
        let key_ref = Self::key_ref(key)?;
        let mut request = self
            .client(key_ref)
            .encrypt()
            .key_id(key_ref)
            .plaintext(Blob::new(dek));
        for (name, value) in Self::wrap_context(key, epoch) {
            request = request.encryption_context(name, value);
        }
        let response = request
            .send()
            .await
            .map_err(|error| classify("Encrypt", &error))?;
        response
            .ciphertext_blob
            .map(Blob::into_inner)
            .ok_or(KmsError::InvalidMaterial)
    }

    async fn unwrap(&self, key: &KeyId, epoch: u32, wrapped: &[u8]) -> Result<SecretKey, KmsError> {
        let key_ref = Self::key_ref(key)?;
        let mut request = self
            .client(key_ref)
            .decrypt()
            .key_id(key_ref)
            .ciphertext_blob(Blob::new(wrapped));
        for (name, value) in Self::wrap_context(key, epoch) {
            request = request.encryption_context(name, value);
        }
        let response = request
            .send()
            .await
            .map_err(|error| classify("Decrypt", &error))?;
        let plaintext = response.plaintext.ok_or(KmsError::InvalidMaterial)?;
        SecretKey::from_slice(plaintext.as_ref()).map_err(|_| KmsError::InvalidMaterial)
    }

    /// HMAC-SHA-256 over the derivation context, computed inside KMS.
    ///
    /// The key must be an HMAC key; a symmetric encryption key is rejected by
    /// KMS, which is the right answer — its material cannot leave, so it can
    /// wrap data keys but can never back a protection class.
    async fn derive(&self, key: &KeyId, context: &[u8]) -> Result<SecretKey, KmsError> {
        let key_ref = Self::key_ref(key)?;
        let response = self
            .client(key_ref)
            .generate_mac()
            .key_id(key_ref)
            .mac_algorithm(MacAlgorithmSpec::HmacSha256)
            .message(Blob::new(context))
            .send()
            .await
            .map_err(|error| classify("GenerateMac", &error))?;
        let mac = response.mac.ok_or(KmsError::InvalidMaterial)?;
        SecretKey::from_slice(mac.as_ref()).map_err(|_| KmsError::InvalidMaterial)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key_id(text: &str) -> KeyId {
        KeyId::new(text).expect("key id")
    }

    #[test]
    fn key_identities_lose_their_scheme_and_keep_everything_else() {
        let arn = "arn:aws:kms:us-west-2:111122223333:key/2f1c";
        assert_eq!(
            AwsKmsClient::key_ref(&key_id(&format!("awskms:{arn}"))).expect("arn"),
            arn
        );
        assert_eq!(
            AwsKmsClient::key_ref(&key_id("awskms:alias/corium-pii")).expect("alias"),
            "alias/corium-pii"
        );
        for rejected in ["file:/etc/corium/storage.key", "awskms:", "awskms"] {
            assert!(AwsKmsClient::key_ref(&key_id(rejected)).is_err());
        }
    }

    #[test]
    fn a_key_arn_names_the_region_it_lives_in() {
        assert_eq!(
            AwsKmsClient::region_of("arn:aws:kms:us-west-2:111122223333:key/2f1c"),
            Some("us-west-2".to_owned())
        );
        assert_eq!(
            AwsKmsClient::region_of("arn:aws-us-gov:kms:us-gov-west-1:1:alias/pii"),
            Some("us-gov-west-1".to_owned())
        );
        // Anything that is not a KMS ARN resolves in the ambient region.
        for ambient in [
            "alias/corium-pii",
            "2f1c8a1e-0000-4000-8000-000000000000",
            "arn:aws:s3:::bucket",
            "arn:aws:kms::1:key/2f1c",
            "arn:aws",
        ] {
            assert_eq!(AwsKmsClient::region_of(ambient), None);
        }
    }

    #[test]
    fn the_encryption_context_binds_the_identity_and_epoch() {
        let context = AwsKmsClient::wrap_context(&key_id("awskms:alias/kek"), 3);
        assert_eq!(
            context,
            [
                ("corium:purpose".to_owned(), "storage-data-key".to_owned()),
                ("corium:key-id".to_owned(), "awskms:alias/kek".to_owned()),
                ("corium:kek-epoch".to_owned(), "3".to_owned()),
            ]
        );
        assert_ne!(
            AwsKmsClient::wrap_context(&key_id("awskms:alias/kek"), 4),
            context
        );
    }
}
