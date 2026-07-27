use std::time::SystemTime;

use async_trait::async_trait;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_sdk_s3::Client;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::primitives::ByteStream;
use tokio_stream::wrappers::ReceiverStream;

use crate::{BlobId, BlobIdStream, BlobStore, RootStore, StoreError, digest};

/// Explicit client settings used for an independently opened S3 store.
///
/// An empty value uses the standard AWS configuration chain. Storage-info
/// clients receive a populated value containing the transactor's separately
/// configured read-only credentials.
#[derive(Clone, Default)]
pub struct S3ClientConfig {
    /// AWS region override.
    pub region: Option<String>,
    /// Custom S3-compatible endpoint.
    pub endpoint_url: Option<String>,
    /// Access-key id. Must be paired with [`Self::secret_access_key`].
    pub access_key_id: Option<String>,
    /// Secret access key.
    pub secret_access_key: Option<String>,
    /// Optional temporary-credential session token.
    pub session_token: Option<String>,
    /// Expiration time for explicit temporary credentials.
    pub expires_after: Option<SystemTime>,
    /// Refreshing credential provider for long-lived clients. When supplied,
    /// this takes precedence over the explicit access-key fields.
    pub credentials_provider: Option<SharedCredentialsProvider>,
}

impl std::fmt::Debug for S3ClientConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("S3ClientConfig")
            .field("region", &self.region)
            .field("endpoint_url", &self.endpoint_url)
            .field(
                "credentials",
                &(self.access_key_id.is_some() || self.credentials_provider.is_some())
                    .then_some("[REDACTED]"),
            )
            .finish_non_exhaustive()
    }
}

/// Content-addressed blob and fenced-root storage backed by an S3 (or
/// S3-compatible) bucket.
///
/// Blobs live under `{prefix}blobs/{id}` and roots under `{prefix}roots/{name}`.
/// Root fencing relies on S3 conditional writes (`If-None-Match: *` for a
/// first publish, `If-Match: <etag>` for a fenced update), so the target
/// bucket and any S3-compatible substitute must support them.
#[derive(Clone)]
pub struct S3BlobStore {
    client: Client,
    bucket: String,
    prefix: String,
}

impl S3BlobStore {
    /// Connects using the standard AWS configuration chain (environment,
    /// profile, IMDS, `AWS_ENDPOINT_URL` for S3-compatible services) and
    /// verifies the bucket is reachable.
    ///
    /// `prefix` namespaces every key this store touches (for example
    /// `"corium/"`); pass an empty string to use the bucket root.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket cannot be reached.
    pub async fn connect(
        bucket: impl Into<String>,
        prefix: impl Into<String>,
    ) -> Result<Self, StoreError> {
        Self::connect_with_config(bucket, prefix, &S3ClientConfig::default()).await
    }

    /// Connects with explicit region, endpoint, and credential overrides.
    ///
    /// Unspecified settings still come from the standard AWS configuration
    /// chain. Supplying an endpoint enables path-style addressing.
    ///
    /// # Errors
    /// Returns an error when only half of the access-key pair is supplied, or
    /// when the bucket cannot be reached.
    pub async fn connect_with_config(
        bucket: impl Into<String>,
        prefix: impl Into<String>,
        options: &S3ClientConfig,
    ) -> Result<Self, StoreError> {
        let client = Self::client(options).await?;
        Self::from_client(client, bucket, prefix).await
    }

    /// Opens an existing store without a bucket-wide `HeadBucket` probe.
    ///
    /// This is the read-only entry point: prefix-scoped IAM credentials can
    /// list and fetch Corium objects without permission to inspect the bucket
    /// itself. The first actual read surfaces connectivity or permission
    /// errors.
    ///
    /// # Errors
    /// Returns an error when explicit client credentials are incomplete.
    pub async fn connect_existing_with_config(
        bucket: impl Into<String>,
        prefix: impl Into<String>,
        options: &S3ClientConfig,
    ) -> Result<Self, StoreError> {
        Ok(Self {
            client: Self::client(options).await?,
            bucket: bucket.into(),
            prefix: normalize_s3_prefix(prefix.into()),
        })
    }

    async fn client(options: &S3ClientConfig) -> Result<Client, StoreError> {
        if options.access_key_id.is_some() != options.secret_access_key.is_some() {
            return Err(StoreError::S3(
                "S3 access key id and secret access key must be supplied together".into(),
            ));
        }
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
        if let Some(region) = &options.region {
            loader = loader.region(aws_sdk_s3::config::Region::new(region.clone()));
        }
        if let Some(provider) = &options.credentials_provider {
            loader = loader.credentials_provider(provider.clone());
        } else if let (Some(access_key_id), Some(secret_access_key)) =
            (&options.access_key_id, &options.secret_access_key)
        {
            loader = loader.credentials_provider(aws_sdk_s3::config::Credentials::new(
                access_key_id,
                secret_access_key,
                options.session_token.clone(),
                options.expires_after,
                "corium-storage-info",
            ));
        }
        let config = loader.load().await;
        // Local S3-compatible servers (MinIO, Garage, LocalStack) are reached
        // by host/IP, where virtual-hosted addressing would fold the bucket
        // into a subdomain that does not resolve; they need path-style keys.
        // A configured `AWS_ENDPOINT_URL` is the signal for such a target, the
        // same heuristic the crate's S3 test harness uses. AWS itself ignores
        // the setting (its endpoints are DNS-addressable).
        let mut s3_config = aws_sdk_s3::config::Builder::from(&config);
        if let Some(endpoint_url) = &options.endpoint_url {
            s3_config = s3_config.endpoint_url(endpoint_url).force_path_style(true);
        } else if std::env::var_os("AWS_ENDPOINT_URL").is_some() {
            s3_config = s3_config.force_path_style(true);
        }
        Ok(Client::from_conf(s3_config.build()))
    }

    /// Creates a store from an existing S3 client, verifying the bucket is
    /// reachable.
    ///
    /// This is the entry point for custom client configuration, such as
    /// path-style addressing against `MinIO` or `LocalStack`.
    ///
    /// # Errors
    ///
    /// Returns an error if the bucket cannot be reached.
    pub async fn from_client(
        client: Client,
        bucket: impl Into<String>,
        prefix: impl Into<String>,
    ) -> Result<Self, StoreError> {
        let store = Self {
            client,
            bucket: bucket.into(),
            prefix: normalize_s3_prefix(prefix.into()),
        };
        store
            .client
            .head_bucket()
            .bucket(&store.bucket)
            .send()
            .await?;
        Ok(store)
    }

    fn blob_prefix(&self) -> String {
        format!("{}blobs/", self.prefix)
    }

    fn root_prefix(&self) -> String {
        format!("{}roots/", self.prefix)
    }

    fn blob_key(&self, id: &BlobId) -> String {
        format!("{}{}", self.blob_prefix(), id.as_str())
    }

    fn root_key(&self, name: &str) -> String {
        format!("{}{}", self.root_prefix(), name)
    }

    /// Reads a root's current bytes together with the `ETag` fencing them,
    /// for use as the `If-Match` precondition on the next publish.
    async fn get_root_with_etag(
        &self,
        name: &str,
    ) -> Result<Option<(Vec<u8>, String)>, StoreError> {
        let key = self.root_key(name);
        match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
        {
            Ok(output) => {
                let etag = output
                    .e_tag()
                    .ok_or_else(|| StoreError::InvalidS3Data(format!("root {name:?} has no ETag")))?
                    .to_owned();
                let bytes = collect_body(output.body).await?;
                Ok(Some((bytes, etag)))
            }
            Err(error)
                if matches!(error.as_service_error(), Some(GetObjectError::NoSuchKey(_))) =>
            {
                Ok(None)
            }
            Err(error) => Err(error.into()),
        }
    }

    /// Uploads `bytes` to `key`, applying `if_match` or an `If-None-Match: *`
    /// precondition. Returns `false` (instead of an error) when the
    /// precondition was not met, so callers can turn that into a
    /// [`StoreError::CasFailed`] with a freshly read `actual` value.
    async fn put_conditional(
        &self,
        key: &str,
        bytes: Vec<u8>,
        if_match: Option<&str>,
        if_none_match: bool,
    ) -> Result<bool, StoreError> {
        let mut request = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(bytes));
        if if_none_match {
            request = request.if_none_match("*");
        }
        if let Some(etag) = if_match {
            request = request.if_match(etag);
        }
        match request.send().await {
            Ok(_) => Ok(true),
            Err(error) if is_precondition_conflict(&error, if_match.is_some()) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    /// Verifies the target actually *enforces* the two conditional writes
    /// corium's root fencing depends on: `If-None-Match: *` (create-only) and
    /// `If-Match: <etag>` (compare-and-swap). Many S3-compatible servers
    /// accept these headers but ignore them; on such a server every root CAS
    /// silently succeeds, so a deposed writer's publish is no longer fenced
    /// and lease safety is lost. This runs a throwaway probe against a
    /// dedicated key and reports which precondition, if any, is not enforced,
    /// so an operator pointing at an unfamiliar endpoint can find out before
    /// trusting it rather than after a split-brain. The probe key is removed
    /// on every exit path.
    ///
    /// # Errors
    /// Returns [`StoreError::S3`] naming the unenforced precondition, or a
    /// store error if the probe itself cannot run.
    pub async fn verify_conditional_writes(&self) -> Result<(), StoreError> {
        let name = format!(
            "__corium-condwrite-probe-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos()),
        );
        let key = self.root_key(&name);
        // Start from a clean slate even if a previous probe leaked its key.
        let _ = self.delete_root(&name).await;

        // `If-None-Match: *`: the first create must succeed, and a second
        // create against the now-present key must be refused.
        if !self
            .put_conditional(&key, b"1".to_vec(), None, true)
            .await?
        {
            return Err(StoreError::S3(
                "conditional-write probe: initial If-None-Match:* create was rejected".into(),
            ));
        }
        let unenforced = |which: &str| {
            StoreError::S3(format!(
                "S3 endpoint does not enforce {which} writes; corium root fencing would be \
                 unsafe on it (a deposed writer's publish would not be fenced)"
            ))
        };
        if self
            .put_conditional(&key, b"2".to_vec(), None, true)
            .await?
        {
            let _ = self.delete_root(&name).await;
            return Err(unenforced("If-None-Match:* (create-only)"));
        }

        // `If-Match: <etag>`: a stale etag must be refused, and the current
        // etag must be accepted.
        let stale = "\"corium-condwrite-probe-stale-etag\"";
        if self
            .put_conditional(&key, b"3".to_vec(), Some(stale), false)
            .await?
        {
            let _ = self.delete_root(&name).await;
            return Err(unenforced("If-Match:<etag> (compare-and-swap)"));
        }
        let Some((_, etag)) = self.get_root_with_etag(&name).await? else {
            return Err(StoreError::S3(
                "conditional-write probe: probe key vanished mid-check".into(),
            ));
        };
        if !self
            .put_conditional(&key, b"4".to_vec(), Some(&etag), false)
            .await?
        {
            let _ = self.delete_root(&name).await;
            return Err(StoreError::S3(
                "conditional-write probe: If-Match with the current etag was rejected".into(),
            ));
        }

        self.delete_root(&name).await
    }
}

/// Normalizes an S3 namespace prefix to empty or trailing-slash form.
#[must_use]
pub fn normalize_s3_prefix(prefix: String) -> String {
    if prefix.is_empty() || prefix.ends_with('/') {
        prefix
    } else {
        format!("{prefix}/")
    }
}

async fn collect_body(body: ByteStream) -> Result<Vec<u8>, StoreError> {
    Ok(body
        .collect()
        .await
        // `ByteStreamError`'s own `Display` is terse ("streaming error"); its
        // real detail lives in `source()`, which `DisplayErrorContext` prints.
        .map_err(|error| StoreError::S3(aws_sdk_s3::error::DisplayErrorContext(error).to_string()))?
        .into_bytes()
        .to_vec())
}

/// True when `error` is an S3 response to a conditional `PutObject` that
/// means the precondition was not met: `412 Precondition Failed`,
/// `409 ConditionalRequestConflict`, or — for an `If-Match` request only —
/// `404 Not Found`, which S3 returns when the target key was deleted (racing
/// a `delete_root`) rather than the RFC 7232 `412`. Any resulting `false` is
/// resolved by a follow-up `get_root`, so a coincidental 404 from another
/// cause (e.g. the bucket itself vanishing) still surfaces as a real error
/// there instead of being silently swallowed here.
fn is_precondition_conflict<E>(error: &SdkError<E>, if_match: bool) -> bool {
    error.raw_response().is_some_and(|response| {
        matches!(response.status().as_u16(), 409 | 412)
            || (if_match && response.status().as_u16() == 404)
    })
}

#[async_trait]
impl BlobStore for S3BlobStore {
    async fn put(&self, bytes: &[u8]) -> Result<BlobId, StoreError> {
        let id = digest(bytes);
        // Blobs are immutable and content-addressed; skip the upload when
        // this content is already present.
        if self.contains(&id).await? {
            return Ok(id);
        }
        let key = self.blob_key(&id);
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&key)
            .body(ByteStream::from(bytes.to_vec()))
            .send()
            .await?;
        Ok(id)
    }

    async fn get(&self, id: &BlobId) -> Result<Option<Vec<u8>>, StoreError> {
        let key = self.blob_key(id);
        match self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
        {
            Ok(output) => {
                let bytes = collect_body(output.body).await?;
                if digest(&bytes) != *id {
                    return Err(StoreError::CorruptBlob(id.clone()));
                }
                Ok(Some(bytes))
            }
            Err(error)
                if matches!(error.as_service_error(), Some(GetObjectError::NoSuchKey(_))) =>
            {
                Ok(None)
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn contains(&self, id: &BlobId) -> Result<bool, StoreError> {
        let key = self.blob_key(id);
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(error)
                if matches!(error.as_service_error(), Some(HeadObjectError::NotFound(_))) =>
            {
                Ok(false)
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn delete(&self, id: &BlobId) -> Result<(), StoreError> {
        let key = self.blob_key(id);
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await?;
        Ok(())
    }

    async fn list(&self) -> Result<BlobIdStream, StoreError> {
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let prefix = self.blob_prefix();
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        tokio::spawn(async move {
            let mut continuation = None;
            loop {
                let mut request = client.list_objects_v2().bucket(&bucket).prefix(&prefix);
                if let Some(token) = continuation.take() {
                    request = request.continuation_token(token);
                }
                let output = match request.send().await {
                    Ok(output) => output,
                    Err(error) => {
                        let _ = tx.send(Err(StoreError::from(error))).await;
                        return;
                    }
                };
                for object in output.contents() {
                    let Some(key) = object.key() else { continue };
                    let Some(name) = key.strip_prefix(&prefix) else {
                        continue;
                    };
                    let Some(id) = BlobId::from_hex(name) else {
                        let _ = tx
                            .send(Err(StoreError::InvalidS3Data(format!(
                                "invalid blob key {key:?}"
                            ))))
                            .await;
                        return;
                    };
                    if tx.send(Ok(id)).await.is_err() {
                        return;
                    }
                }
                continuation = output.next_continuation_token().map(str::to_owned);
                if continuation.is_none() {
                    return;
                }
            }
        });
        Ok(Box::pin(ReceiverStream::new(rx)))
    }

    async fn modified_at(&self, id: &BlobId) -> Result<Option<SystemTime>, StoreError> {
        let key = self.blob_key(id);
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await
        {
            Ok(output) => Ok(match output.last_modified() {
                Some(timestamp) => Some(SystemTime::try_from(*timestamp).map_err(|error| {
                    StoreError::InvalidS3Data(format!("invalid last-modified timestamp: {error}"))
                })?),
                None => None,
            }),
            Err(error)
                if matches!(error.as_service_error(), Some(HeadObjectError::NotFound(_))) =>
            {
                Ok(None)
            }
            Err(error) => Err(error.into()),
        }
    }
}

#[async_trait]
impl RootStore for S3BlobStore {
    async fn get_root(&self, name: &str) -> Result<Option<Vec<u8>>, StoreError> {
        Ok(self
            .get_root_with_etag(name)
            .await?
            .map(|(bytes, _etag)| bytes))
    }

    async fn cas_root(
        &self,
        name: &str,
        expected: Option<&[u8]>,
        new: &[u8],
    ) -> Result<(), StoreError> {
        let key = self.root_key(name);
        match expected {
            None => {
                if self.put_conditional(&key, new.to_vec(), None, true).await? {
                    Ok(())
                } else {
                    let actual = self.get_root(name).await?;
                    Err(StoreError::CasFailed {
                        expected: None,
                        actual,
                    })
                }
            }
            Some(expected_bytes) => {
                let Some((actual_bytes, etag)) = self.get_root_with_etag(name).await? else {
                    return Err(StoreError::CasFailed {
                        expected: Some(expected_bytes.to_vec()),
                        actual: None,
                    });
                };
                if actual_bytes != expected_bytes {
                    return Err(StoreError::CasFailed {
                        expected: Some(expected_bytes.to_vec()),
                        actual: Some(actual_bytes),
                    });
                }
                if self
                    .put_conditional(&key, new.to_vec(), Some(&etag), false)
                    .await?
                {
                    Ok(())
                } else {
                    let actual = self.get_root(name).await?;
                    Err(StoreError::CasFailed {
                        expected: Some(expected_bytes.to_vec()),
                        actual,
                    })
                }
            }
        }
    }

    async fn delete_root(&self, name: &str) -> Result<(), StoreError> {
        let key = self.root_key(name);
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(&key)
            .send()
            .await?;
        Ok(())
    }

    async fn list_roots(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        let root_prefix = self.root_prefix();
        let full_prefix = format!("{root_prefix}{prefix}");
        let mut names = Vec::new();
        let mut continuation = None;
        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&full_prefix);
            if let Some(token) = continuation.take() {
                request = request.continuation_token(token);
            }
            let output = request.send().await?;
            for object in output.contents() {
                if let Some(name) = object.key().and_then(|key| key.strip_prefix(&root_prefix)) {
                    names.push(name.to_owned());
                }
            }
            continuation = output.next_continuation_token().map(str::to_owned);
            if continuation.is_none() {
                break;
            }
        }
        names.sort();
        Ok(names)
    }
}

#[cfg(test)]
mod tests {
    use super::normalize_s3_prefix;

    #[test]
    fn normalizes_prefix_once() {
        assert_eq!(normalize_s3_prefix(String::new()), "");
        assert_eq!(normalize_s3_prefix("tenant".into()), "tenant/");
        assert_eq!(normalize_s3_prefix("tenant/".into()), "tenant/");
    }
}
