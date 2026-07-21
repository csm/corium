use std::time::SystemTime;

use async_trait::async_trait;
use aws_sdk_s3::Client;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::get_object::GetObjectError;
use aws_sdk_s3::operation::head_object::HeadObjectError;
use aws_sdk_s3::primitives::ByteStream;
use tokio_stream::wrappers::ReceiverStream;

use crate::{BlobId, BlobIdStream, BlobStore, RootStore, StoreError, digest};

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
        let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .load()
            .await;
        Self::from_client(Client::new(&config), bucket, prefix).await
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
            prefix: normalize_prefix(prefix.into()),
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
            Err(error) if is_precondition_conflict(&error) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }
}

fn normalize_prefix(prefix: String) -> String {
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
        .map_err(|error| StoreError::S3(error.to_string()))?
        .into_bytes()
        .to_vec())
}

/// True when `error` is an S3 `412 Precondition Failed` or
/// `409 ConditionalRequestConflict` response to a conditional `PutObject`.
fn is_precondition_conflict<E>(error: &SdkError<E>) -> bool {
    error
        .raw_response()
        .is_some_and(|response| matches!(response.status().as_u16(), 409 | 412))
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
