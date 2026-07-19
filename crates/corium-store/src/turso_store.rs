use std::{
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use tokio_stream::wrappers::ReceiverStream;
use turso::{Builder, Database};

use crate::{BlobId, BlobIdStream, BlobStore, StoreError, digest};

const CREATE_BLOBS_TABLE: &str = "
    CREATE TABLE IF NOT EXISTS corium_blobs (
        id TEXT PRIMARY KEY NOT NULL,
        data BLOB NOT NULL,
        created_at_unix_seconds INTEGER NOT NULL DEFAULT (unixepoch())
    )
";

/// Content-addressed blob storage backed by a local Turso database.
///
/// Constructing a store creates the `corium_blobs` table when it is absent.
/// Blobs are immutable and duplicate writes of the same content are ignored.
#[derive(Clone)]
pub struct TursoBlobStore {
    database: Database,
}

impl TursoBlobStore {
    /// Opens or creates a local Turso database at `path`.
    ///
    /// # Errors
    ///
    /// Returns an error when the path is not UTF-8, the database cannot be
    /// opened, or the blob table cannot be initialized.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let path = path.as_ref();
        let text = path
            .to_str()
            .ok_or_else(|| StoreError::InvalidTursoPath(path.to_path_buf()))?;
        let database = Builder::new_local(text).build().await?;
        Self::from_database(database).await
    }

    /// Creates a blob store from an existing Turso database handle.
    ///
    /// This is useful for in-memory databases and databases configured with a
    /// custom [`Builder`].
    ///
    /// # Errors
    ///
    /// Returns an error when the blob table cannot be initialized.
    pub async fn from_database(database: Database) -> Result<Self, StoreError> {
        database.connect()?.execute(CREATE_BLOBS_TABLE, ()).await?;
        Ok(Self { database })
    }

    fn connect(&self) -> Result<turso::Connection, StoreError> {
        Ok(self.database.connect()?)
    }
}

#[async_trait]
impl BlobStore for TursoBlobStore {
    async fn put(&self, bytes: &[u8]) -> Result<BlobId, StoreError> {
        let id = digest(bytes);
        self.connect()?
            .execute(
                "INSERT OR IGNORE INTO corium_blobs (id, data) VALUES (?1, ?2)",
                (id.as_str(), bytes),
            )
            .await?;
        Ok(id)
    }

    async fn get(&self, id: &BlobId) -> Result<Option<Vec<u8>>, StoreError> {
        let mut rows = self
            .connect()?
            .query("SELECT data FROM corium_blobs WHERE id = ?1", [id.as_str()])
            .await?;
        let Some(row) = rows.next().await? else {
            return Ok(None);
        };
        let bytes: Vec<u8> = row.get(0)?;
        if digest(&bytes) != *id {
            return Err(StoreError::CorruptBlob(id.clone()));
        }
        Ok(Some(bytes))
    }

    async fn contains(&self, id: &BlobId) -> Result<bool, StoreError> {
        let mut rows = self
            .connect()?
            .query("SELECT 1 FROM corium_blobs WHERE id = ?1", [id.as_str()])
            .await?;
        Ok(rows.next().await?.is_some())
    }

    async fn delete(&self, id: &BlobId) -> Result<(), StoreError> {
        self.connect()?
            .execute("DELETE FROM corium_blobs WHERE id = ?1", [id.as_str()])
            .await?;
        Ok(())
    }

    async fn list(&self) -> Result<BlobIdStream, StoreError> {
        let mut rows = self
            .connect()?
            .query("SELECT id FROM corium_blobs ORDER BY id", ())
            .await?;
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        tokio::spawn(async move {
            loop {
                let row = match rows.next().await {
                    Ok(Some(row)) => row,
                    Ok(None) => return,
                    Err(error) => {
                        let _ = tx.send(Err(StoreError::Turso(error))).await;
                        return;
                    }
                };
                let text = match row.get::<String>(0) {
                    Ok(text) => text,
                    Err(error) => {
                        let _ = tx.send(Err(StoreError::Turso(error))).await;
                        return;
                    }
                };
                let Some(id) = BlobId::from_hex(&text) else {
                    let _ = tx
                        .send(Err(StoreError::InvalidTursoData(format!(
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
        let mut rows = self
            .connect()?
            .query(
                "SELECT created_at_unix_seconds FROM corium_blobs WHERE id = ?1",
                [id.as_str()],
            )
            .await?;
        let Some(row) = rows.next().await? else {
            return Ok(None);
        };
        let seconds: i64 = row.get(0)?;
        let seconds = u64::try_from(seconds).map_err(|_| {
            StoreError::InvalidTursoData(format!("negative blob timestamp {seconds}"))
        })?;
        let timestamp = UNIX_EPOCH
            .checked_add(Duration::from_secs(seconds))
            .ok_or_else(|| {
                StoreError::InvalidTursoData(format!("blob timestamp is out of range: {seconds}"))
            })?;
        Ok(Some(timestamp))
    }
}
