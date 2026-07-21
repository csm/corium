use std::{
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use tokio_stream::wrappers::ReceiverStream;
use turso::{Builder, Database};

use crate::{BlobId, BlobIdStream, BlobStore, RootStore, StoreError, TransactionLogStore, digest};

const CREATE_BLOBS_TABLE: &str = "
    CREATE TABLE IF NOT EXISTS corium_blobs (
        id TEXT PRIMARY KEY NOT NULL,
        data BLOB NOT NULL,
        created_at_unix_seconds INTEGER NOT NULL DEFAULT (unixepoch())
    )
";

const CREATE_ROOTS_TABLE: &str = "
    CREATE TABLE IF NOT EXISTS corium_roots (
        name TEXT PRIMARY KEY NOT NULL,
        data BLOB NOT NULL
    )
";

const CREATE_LOGS_TABLE: &str = "
    CREATE TABLE IF NOT EXISTS corium_tx_logs (
        name TEXT NOT NULL,
        version INTEGER NOT NULL,
        data BLOB NOT NULL,
        PRIMARY KEY (name, version)
    )
";

/// Content-addressed blob and fenced-root storage backed by a Turso
/// (embeddable `SQLite`) database.
///
/// Constructing a store creates the `corium_blobs` and `corium_roots`
/// tables when they are absent. Blobs are immutable and duplicate writes of
/// the same content are ignored; roots are mutable pointers published with
/// compare-and-swap fencing (an atomic `BEGIN IMMEDIATE` transaction), so a
/// single Turso database can back the whole transactor storage service.
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
        let database = Self::open_database(path.as_ref()).await?;
        Self::from_database(database).await
    }

    /// Opens an existing local Turso database without running schema DDL.
    ///
    /// This is the preferred entry point for storage-aware peers, which need
    /// only read the tables already initialized by a transactor.
    ///
    /// # Errors
    /// Returns an error when the path is not UTF-8 or the database cannot be
    /// opened.
    pub async fn open_existing(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        Ok(Self {
            database: Self::open_database(path.as_ref()).await?,
        })
    }

    async fn open_database(path: &Path) -> Result<Database, StoreError> {
        let text = path
            .to_str()
            .ok_or_else(|| StoreError::InvalidTursoPath(path.to_path_buf()))?;
        // Independent transactors and storage-aware peers open the same local
        // database file. Turso requires its multi-process WAL coordinator for
        // that topology; without it a second process is rejected at open.
        let database = Builder::new_local(text)
            .experimental_multiprocess_wal(true)
            .build()
            .await?;
        Ok(database)
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
        let connection = database.connect()?;
        connection.execute(CREATE_BLOBS_TABLE, ()).await?;
        connection.execute(CREATE_ROOTS_TABLE, ()).await?;
        connection.execute(CREATE_LOGS_TABLE, ()).await?;
        Ok(Self { database })
    }

    fn connect(&self) -> Result<turso::Connection, StoreError> {
        Ok(self.database.connect()?)
    }
}

#[async_trait]
impl TransactionLogStore for TursoBlobStore {
    async fn get_log_version(
        &self,
        name: &str,
        version: u64,
    ) -> Result<Option<Vec<u8>>, StoreError> {
        let version = i64::try_from(version).map_err(|_| {
            StoreError::InvalidTursoData(format!("log version too large: {version}"))
        })?;
        let mut rows = self
            .connect()?
            .query(
                "SELECT data FROM corium_tx_logs WHERE name = ?1 AND version = ?2",
                (name, version),
            )
            .await?;
        match rows.next().await? {
            Some(row) => Ok(Some(row.get::<Vec<u8>>(0)?)),
            None => Ok(None),
        }
    }

    async fn cas_log_version(
        &self,
        name: &str,
        version: u64,
        expected: Option<&[u8]>,
        new: &[u8],
    ) -> Result<(), StoreError> {
        let version = i64::try_from(version).map_err(|_| {
            StoreError::InvalidTursoData(format!("log version too large: {version}"))
        })?;
        let connection = self.connect()?;
        connection.execute("BEGIN IMMEDIATE", ()).await?;
        let result = async {
            let mut rows = connection
                .query(
                    "SELECT data FROM corium_tx_logs WHERE name = ?1 AND version = ?2",
                    (name, version),
                )
                .await?;
            let actual = match rows.next().await? {
                Some(row) => Some(row.get::<Vec<u8>>(0)?),
                None => None,
            };
            if actual.as_deref() != expected {
                return Err(StoreError::CasFailed {
                    expected: expected.map(<[u8]>::to_vec),
                    actual,
                });
            }
            connection
                .execute(
                    "INSERT INTO corium_tx_logs (name, version, data) VALUES (?1, ?2, ?3)
                     ON CONFLICT(name, version) DO UPDATE SET data = excluded.data",
                    (name, version, new),
                )
                .await?;
            Ok(())
        }
        .await;
        if result.is_ok() {
            connection.execute("COMMIT", ()).await?;
        } else {
            let _ = connection.execute("ROLLBACK", ()).await;
        }
        result
    }

    async fn list_log_versions(&self, name: &str) -> Result<Vec<u64>, StoreError> {
        let connection = self.connect()?;
        let mut rows = connection
            .query(
                "SELECT version FROM corium_tx_logs WHERE name = ?1 ORDER BY version",
                [name],
            )
            .await?;
        let mut versions = Vec::new();
        while let Some(row) = rows.next().await? {
            let version: i64 = row.get(0)?;
            versions.push(u64::try_from(version).map_err(|_| {
                StoreError::InvalidTursoData(format!("negative log version {version}"))
            })?);
        }
        Ok(versions)
    }

    async fn delete_log_versions(&self, name: &str) -> Result<(), StoreError> {
        self.connect()?
            .execute("DELETE FROM corium_tx_logs WHERE name = ?1", [name])
            .await?;
        Ok(())
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

impl TursoBlobStore {
    /// Reads the current bytes stored for a root name (transaction-local).
    async fn read_root(
        connection: &turso::Connection,
        name: &str,
    ) -> Result<Option<Vec<u8>>, StoreError> {
        let mut rows = connection
            .query("SELECT data FROM corium_roots WHERE name = ?1", [name])
            .await?;
        match rows.next().await? {
            Some(row) => Ok(Some(row.get::<Vec<u8>>(0)?)),
            None => Ok(None),
        }
    }
}

#[async_trait]
impl RootStore for TursoBlobStore {
    async fn get_root(&self, name: &str) -> Result<Option<Vec<u8>>, StoreError> {
        Self::read_root(&self.connect()?, name).await
    }

    async fn cas_root(
        &self,
        name: &str,
        expected: Option<&[u8]>,
        new: &[u8],
    ) -> Result<(), StoreError> {
        let connection = self.connect()?;
        // An immediate transaction takes the write lock up front so the
        // read-compare-write fence is atomic against a concurrent writer.
        connection.execute("BEGIN IMMEDIATE", ()).await?;
        let result = async {
            let actual = Self::read_root(&connection, name).await?;
            if actual.as_deref() != expected {
                return Err(StoreError::CasFailed {
                    expected: expected.map(<[u8]>::to_vec),
                    actual,
                });
            }
            connection
                .execute(
                    "INSERT INTO corium_roots (name, data) VALUES (?1, ?2)
                     ON CONFLICT(name) DO UPDATE SET data = excluded.data",
                    (name, new),
                )
                .await?;
            Ok(())
        }
        .await;
        if result.is_ok() {
            connection.execute("COMMIT", ()).await?;
        } else {
            // Roll back the (empty) transaction; the CAS/read error is what
            // the caller cares about, so ignore a rollback failure.
            let _ = connection.execute("ROLLBACK", ()).await;
        }
        result
    }

    async fn delete_root(&self, name: &str) -> Result<(), StoreError> {
        self.connect()?
            .execute("DELETE FROM corium_roots WHERE name = ?1", [name])
            .await?;
        Ok(())
    }

    async fn list_roots(&self, prefix: &str) -> Result<Vec<String>, StoreError> {
        let connection = self.connect()?;
        let mut rows = connection
            .query(
                "SELECT name FROM corium_roots WHERE name >= ?1 ORDER BY name",
                [prefix],
            )
            .await?;
        let mut names = Vec::new();
        while let Some(row) = rows.next().await? {
            let name: String = row.get(0)?;
            if name.starts_with(prefix) {
                names.push(name);
            } else {
                break;
            }
        }
        Ok(names)
    }
}
