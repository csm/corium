use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use deadpool_postgres::{Config, Pool, Runtime};
use tokio_stream::{StreamExt, wrappers::ReceiverStream};

use crate::{BlobId, BlobIdStream, BlobStore, RootStore, StoreError, digest};

const CREATE_BLOBS_TABLE: &str = "
    CREATE TABLE IF NOT EXISTS corium_blobs (
        id TEXT PRIMARY KEY NOT NULL,
        data BYTEA NOT NULL,
        created_at_unix_seconds BIGINT NOT NULL DEFAULT
            (EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)::BIGINT)
    )
";

const CREATE_ROOTS_TABLE: &str = "
    CREATE TABLE IF NOT EXISTS corium_roots (
        name TEXT PRIMARY KEY NOT NULL,
        data BYTEA NOT NULL
    )
";

/// Content-addressed blob and fenced-root storage backed by `PostgreSQL`.
///
/// Constructing a store creates the `corium_blobs` and `corium_roots` tables
/// when they are absent. Blob writes are immutable and idempotent. Root
/// compare-and-swap uses conditional `PostgreSQL` `INSERT` and `UPDATE`
/// statements, so competing publishers cannot both cross the same fence.
#[derive(Clone)]
pub struct PostgresBlobStore {
    pool: Pool,
}

impl PostgresBlobStore {
    /// Connects with a `PostgreSQL` connection string and the platform's native
    /// TLS certificate roots.
    ///
    /// The connection string must name a database. It can use `PostgreSQL` URL
    /// syntax or the keyword/value syntax accepted by `tokio-postgres`.
    ///
    /// # Errors
    ///
    /// Returns an error when TLS roots cannot be loaded, the pool cannot be
    /// configured, `PostgreSQL` cannot be reached, or the tables cannot be
    /// initialized.
    pub async fn connect(connection_string: impl Into<String>) -> Result<Self, StoreError> {
        let pool = Self::create_pool(connection_string)?;
        Self::from_pool(pool).await
    }

    /// Connects to an already initialized `PostgreSQL` store without running
    /// schema DDL.
    ///
    /// This is the preferred entry point for storage-aware peers. It checks
    /// out one connection eagerly so connection and TLS failures surface at
    /// startup; table existence is verified by the first peer read.
    ///
    /// # Errors
    /// Returns an error when TLS roots cannot be loaded, the pool cannot be
    /// configured, or `PostgreSQL` cannot be reached.
    pub async fn connect_existing(
        connection_string: impl Into<String>,
    ) -> Result<Self, StoreError> {
        let pool = Self::create_pool(connection_string)?;
        let _client = pool.get().await?;
        Ok(Self { pool })
    }

    fn create_pool(connection_string: impl Into<String>) -> Result<Pool, StoreError> {
        // A host application may already have selected another rustls
        // provider. In that case its process-wide choice remains in force.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let (tls, _certificate_errors) =
            tokio_postgres_rustls::MakeRustlsConnect::with_native_certs()
                .map_err(|errors| StoreError::PostgresTlsRoots(format!("{errors:?}")))?;
        let config = Config {
            url: Some(connection_string.into()),
            ..Config::default()
        };
        Ok(config.create_pool(Some(Runtime::Tokio1), tls)?)
    }

    /// Creates a store from an existing `deadpool-postgres` pool.
    ///
    /// This entry point supports custom pool limits, TLS policies, client
    /// certificates, and other connection settings. The pool is cheaply
    /// cloneable and can be shared with other application components.
    ///
    /// # Errors
    ///
    /// Returns an error when a connection cannot be checked out or the store
    /// tables cannot be initialized.
    pub async fn from_pool(pool: Pool) -> Result<Self, StoreError> {
        let client = pool.get().await?;
        client.batch_execute(CREATE_BLOBS_TABLE).await?;
        client.batch_execute(CREATE_ROOTS_TABLE).await?;
        Ok(Self { pool })
    }

    async fn client(&self) -> Result<deadpool_postgres::Client, StoreError> {
        Ok(self.pool.get().await?)
    }

    async fn read_root(&self, name: &str) -> Result<Option<Vec<u8>>, StoreError> {
        let row = self
            .client()
            .await?
            .query_opt("SELECT data FROM corium_roots WHERE name = $1", &[&name])
            .await?;
        match row {
            Some(row) => Ok(Some(row.try_get(0)?)),
            None => Ok(None),
        }
    }

    async fn cas_failed(&self, name: &str, expected: Option<&[u8]>) -> StoreError {
        match self.read_root(name).await {
            Ok(actual) => StoreError::CasFailed {
                expected: expected.map(<[u8]>::to_vec),
                actual,
            },
            Err(error) => error,
        }
    }
}

#[async_trait]
impl BlobStore for PostgresBlobStore {
    async fn put(&self, bytes: &[u8]) -> Result<BlobId, StoreError> {
        let id = digest(bytes);
        self.client()
            .await?
            .execute(
                "INSERT INTO corium_blobs (id, data) VALUES ($1, $2)
                 ON CONFLICT (id) DO NOTHING",
                &[&id.as_str(), &bytes],
            )
            .await?;
        Ok(id)
    }

    async fn get(&self, id: &BlobId) -> Result<Option<Vec<u8>>, StoreError> {
        let row = self
            .client()
            .await?
            .query_opt(
                "SELECT data FROM corium_blobs WHERE id = $1",
                &[&id.as_str()],
            )
            .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let bytes: Vec<u8> = row.try_get(0)?;
        if digest(&bytes) != *id {
            return Err(StoreError::CorruptBlob(id.clone()));
        }
        Ok(Some(bytes))
    }

    async fn contains(&self, id: &BlobId) -> Result<bool, StoreError> {
        Ok(self
            .client()
            .await?
            .query_opt("SELECT 1 FROM corium_blobs WHERE id = $1", &[&id.as_str()])
            .await?
            .is_some())
    }

    async fn delete(&self, id: &BlobId) -> Result<(), StoreError> {
        self.client()
            .await?
            .execute("DELETE FROM corium_blobs WHERE id = $1", &[&id.as_str()])
            .await?;
        Ok(())
    }

    async fn list(&self) -> Result<BlobIdStream, StoreError> {
        let client = self.client().await?;
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        tokio::spawn(async move {
            let rows = match client
                .query_raw(
                    "SELECT id FROM corium_blobs ORDER BY id",
                    std::iter::empty::<&str>(),
                )
                .await
            {
                Ok(rows) => rows,
                Err(error) => {
                    let _ = tx.send(Err(StoreError::Postgres(error))).await;
                    return;
                }
            };
            let mut rows = std::pin::pin!(rows);
            while let Some(row) = rows.next().await {
                let row = match row {
                    Ok(row) => row,
                    Err(error) => {
                        let _ = tx.send(Err(StoreError::Postgres(error))).await;
                        return;
                    }
                };
                let text: String = match row.try_get(0) {
                    Ok(text) => text,
                    Err(error) => {
                        let _ = tx.send(Err(StoreError::Postgres(error))).await;
                        return;
                    }
                };
                let Some(id) = BlobId::from_hex(&text) else {
                    let _ = tx
                        .send(Err(StoreError::InvalidPostgresData(format!(
                            "invalid blob id {text:?}"
                        ))))
                        .await;
                    return;
                };
                if tx.send(Ok(id)).await.is_err() {
                    return;
                }
            }
        });
        Ok(Box::pin(ReceiverStream::new(rx)))
    }

    async fn modified_at(&self, id: &BlobId) -> Result<Option<SystemTime>, StoreError> {
        let row = self
            .client()
            .await?
            .query_opt(
                "SELECT created_at_unix_seconds FROM corium_blobs WHERE id = $1",
                &[&id.as_str()],
            )
            .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let seconds: i64 = row.try_get(0)?;
        let seconds = u64::try_from(seconds).map_err(|_| {
            StoreError::InvalidPostgresData(format!("negative blob timestamp {seconds}"))
        })?;
        let timestamp = UNIX_EPOCH
            .checked_add(Duration::from_secs(seconds))
            .ok_or_else(|| {
                StoreError::InvalidPostgresData(format!(
                    "blob timestamp is out of range: {seconds}"
                ))
            })?;
        Ok(Some(timestamp))
    }
}

#[async_trait]
impl RootStore for PostgresBlobStore {
    async fn get_root(&self, name: &str) -> Result<Option<Vec<u8>>, StoreError> {
        self.read_root(name).await
    }

    async fn cas_root(
        &self,
        name: &str,
        expected: Option<&[u8]>,
        new: &[u8],
    ) -> Result<(), StoreError> {
        let changed = if let Some(expected) = expected {
            self.client()
                .await?
                .execute(
                    "UPDATE corium_roots SET data = $3 WHERE name = $1 AND data = $2",
                    &[&name, &expected, &new],
                )
                .await?
        } else {
            self.client()
                .await?
                .execute(
                    "INSERT INTO corium_roots (name, data) VALUES ($1, $2)
                     ON CONFLICT (name) DO NOTHING",
                    &[&name, &new],
                )
                .await?
        };
        if changed == 1 {
            Ok(())
        } else {
            Err(self.cas_failed(name, expected).await)
        }
    }

    async fn delete_root(&self, name: &str) -> Result<(), StoreError> {
        self.client()
            .await?
            .execute("DELETE FROM corium_roots WHERE name = $1", &[&name])
            .await?;
        Ok(())
    }

    async fn list_roots(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        let pattern = format!("{}%", escape_like(prefix));
        let rows = self
            .client()
            .await?
            .query(
                "SELECT name FROM corium_roots
                 WHERE name LIKE $1 ESCAPE '\\' ORDER BY name",
                &[&pattern],
            )
            .await?;
        rows.into_iter()
            .map(|row| row.try_get(0))
            .collect::<Result<_, _>>()
            .map_err(StoreError::Postgres)
    }
}

fn escape_like(text: &str) -> String {
    text.replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

#[cfg(test)]
mod tests {
    use super::escape_like;

    #[test]
    fn root_prefix_escapes_like_metacharacters() {
        assert_eq!(escape_like(r"db:%_\main"), r"db:\%\_\\main");
    }
}
