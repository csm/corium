//! A [`DbCatalog`] backed by peer connections to the transactor.
//!
//! Databases are opened lazily on first use and cached, so one peer
//! `Connection` (and its segment cache) is shared by every client connection
//! that queries the same database.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use corium_db::Db;
use corium_peer::Connection;
use corium_pgwire::{CatalogError, DbCatalog};
use tokio::sync::Mutex;

use crate::ClientFlags;

/// Resolves databases by name through the transactor, caching one shared
/// [`Connection`] per database.
pub struct PeerCatalog {
    client: ClientFlags,
    /// When present, only these database names are exposed.
    whitelist: Option<HashSet<String>>,
    connections: Mutex<HashMap<String, Arc<Connection>>>,
}

impl PeerCatalog {
    /// Builds a catalog. An empty `databases` list exposes the whole catalog;
    /// otherwise only the listed databases are reachable.
    pub fn new(client: ClientFlags, databases: Vec<String>) -> Self {
        let whitelist = if databases.is_empty() {
            None
        } else {
            Some(databases.into_iter().collect())
        };
        Self {
            client,
            whitelist,
            connections: Mutex::new(HashMap::new()),
        }
    }

    fn allowed(&self, name: &str) -> bool {
        self.whitelist
            .as_ref()
            .is_none_or(|allowed| allowed.contains(name))
    }
}

#[async_trait::async_trait]
impl DbCatalog for PeerCatalog {
    async fn list(&self) -> Result<Vec<String>, CatalogError> {
        let mut admin = crate::admin_client(&self.client)
            .await
            .map_err(CatalogError::Unavailable)?;
        let mut names = admin
            .list_databases()
            .await
            .map_err(|error| CatalogError::Unavailable(error.to_string()))?;
        if let Some(allowed) = &self.whitelist {
            names.retain(|name| allowed.contains(name));
        }
        names.sort();
        Ok(names)
    }

    async fn db(&self, name: &str) -> Result<Db, CatalogError> {
        if !self.allowed(name) {
            return Err(CatalogError::NotFound(name.to_owned()));
        }
        {
            let cache = self.connections.lock().await;
            if let Some(connection) = cache.get(name) {
                return Ok(connection.db());
            }
        }
        // Not cached: connect outside the lock. A concurrent first use may
        // build a second connection; `or_insert` keeps whichever lands first
        // and drops the loser.
        let config = self
            .client
            .connect_config(name.to_owned())
            .await
            .map_err(CatalogError::Unavailable)?;
        let connection = Arc::new(
            Connection::connect(config)
                .await
                .map_err(|error| CatalogError::Unavailable(error.to_string()))?,
        );
        let mut cache = self.connections.lock().await;
        let entry = cache.entry(name.to_owned()).or_insert(connection);
        Ok(entry.db())
    }
}
