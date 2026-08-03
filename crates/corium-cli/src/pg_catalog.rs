//! A [`DbCatalog`] backed by peer connections to the transactor.
//!
//! Databases are opened lazily on first use and cached, so one peer
//! `Connection` (and its segment cache) is shared by every client connection
//! that queries the same database.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use corium_crypt::Keyring;
use corium_db::Db;
use corium_db::protect::Hydrator;
use corium_peer::Connection;
use corium_peer::PeerError;
use corium_pgwire::{CatalogError, CatalogTxResult, DbCatalog};
use corium_query::edn::Edn;
use tokio::sync::Mutex;
use tonic::Code;

use crate::ClientFlags;

/// Resolves databases by name through the transactor, caching one shared
/// [`Connection`] per database.
pub struct PeerCatalog {
    client: ClientFlags,
    /// When present, only these database names are exposed.
    whitelist: Option<HashSet<String>>,
    allow_writes: bool,
    /// Class keys this process can resolve. Which of them a given SQL
    /// principal may use is decided per statement by the pgwire front end.
    keyring: Option<Arc<dyn Keyring>>,
    connections: Mutex<HashMap<String, Arc<Connection>>>,
}

impl PeerCatalog {
    /// Builds a catalog. An empty `databases` list exposes the whole catalog;
    /// otherwise only the listed databases are reachable. Writes are rejected
    /// unless `allow_writes` was explicitly enabled by the operator.
    pub fn new(
        client: ClientFlags,
        databases: Vec<String>,
        allow_writes: bool,
        keyring: Option<Arc<dyn Keyring>>,
    ) -> Self {
        let whitelist = if databases.is_empty() {
            None
        } else {
            Some(databases.into_iter().collect())
        };
        Self {
            client,
            whitelist,
            allow_writes,
            keyring,
            connections: Mutex::new(HashMap::new()),
        }
    }

    fn allowed(&self, name: &str) -> bool {
        self.whitelist
            .as_ref()
            .is_none_or(|allowed| allowed.contains(name))
    }

    async fn connection(&self, name: &str) -> Result<Arc<Connection>, CatalogError> {
        if !self.allowed(name) {
            return Err(CatalogError::NotFound(name.to_owned()));
        }
        {
            let cache = self.connections.lock().await;
            if let Some(connection) = cache.get(name) {
                return Ok(Arc::clone(connection));
            }
        }
        let mut config = self
            .client
            .connect_config(name.to_owned())
            .await
            .map_err(CatalogError::Unavailable)?;
        if let Some(keyring) = &self.keyring {
            config = config.with_keyring(Arc::clone(keyring));
        }
        let connection = Arc::new(
            Connection::connect(config)
                .await
                .map_err(|error| CatalogError::Unavailable(error.to_string()))?,
        );
        let mut cache = self.connections.lock().await;
        Ok(Arc::clone(
            cache.entry(name.to_owned()).or_insert(connection),
        ))
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
        Ok(self.connection(name).await?.db())
    }

    async fn hydrator(&self, name: &str) -> Result<Arc<Hydrator>, CatalogError> {
        // The connection resolves its keyring once and caches the snapshot;
        // which of those keys a given SQL principal may use is decided per
        // statement by the pgwire front end.
        self.connection(name)
            .await?
            .hydrator()
            .await
            .map_err(|error| CatalogError::Unavailable(error.to_string()))
    }

    async fn transact(
        &self,
        name: &str,
        expected_basis_t: u64,
        forms: Vec<Edn>,
    ) -> Result<CatalogTxResult, CatalogError> {
        if !self.allow_writes {
            return Err(CatalogError::ReadOnly(name.to_owned()));
        }
        let connection = self.connection(name).await?;
        let result = connection
            .transact_at(forms, Some(expected_basis_t))
            .await
            .map_err(|error| map_write_error(&error))?;
        Ok(CatalogTxResult {
            db_after: result.db_after,
            tempids: result.tempids,
        })
    }
}

fn map_write_error(error: &PeerError) -> CatalogError {
    match error {
        PeerError::Rpc(status) if status.code() == Code::Aborted => {
            CatalogError::Conflict(status.message().to_owned())
        }
        PeerError::Rpc(status)
            if matches!(
                status.code(),
                Code::PermissionDenied | Code::Unauthenticated
            ) =>
        {
            CatalogError::Denied(status.message().to_owned())
        }
        // The transactor refuses a principal whose policy view hides
        // attributes, because it cannot apply one. That is a policy decision
        // wearing an UNIMPLEMENTED status, and a SQL client should hear it as
        // insufficient privilege rather than as a missing feature.
        PeerError::Rpc(status)
            if status.code() == Code::Unimplemented
                && status.message().contains("view filtering") =>
        {
            CatalogError::Denied(status.message().to_owned())
        }
        PeerError::Rpc(status) if status.code() == Code::Unimplemented => {
            CatalogError::Unsupported(format!(
                "the connected peer/transactor does not support SQL writes: {}",
                status.message()
            ))
        }
        PeerError::Rpc(status)
            if status.code() == Code::FailedPrecondition
                && status.message().contains("protocol version") =>
        {
            CatalogError::Unsupported(status.message().to_owned())
        }
        PeerError::Rpc(status) if status.code() == Code::InvalidArgument => {
            CatalogError::Rejected(status.message().to_owned())
        }
        _ => CatalogError::Unavailable(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn catalog_rejects_writes_unless_the_operator_opts_in() {
        let catalog = PeerCatalog::new(
            ClientFlags {
                transactor: "http://127.0.0.1:1".to_owned(),
                token: None,
                ca: None,
                tls_domain: None,
                peer_bootstrap: false,
            },
            Vec::new(),
            false,
            None,
        );
        let Err(error) = catalog.transact("corium", 0, Vec::new()).await else {
            panic!("writes default to disabled");
        };
        assert!(matches!(error, CatalogError::ReadOnly(name) if name == "corium"));
    }

    #[test]
    fn write_error_mapping_separates_authz_from_missing_capabilities() {
        let filtered = PeerError::Rpc(tonic::Status::unimplemented(
            "view filtering is not enforced on this surface yet",
        ));
        assert!(matches!(
            map_write_error(&filtered),
            CatalogError::Denied(_)
        ));

        let old_peer = PeerError::Rpc(tonic::Status::unimplemented("unknown RPC method Transact"));
        assert!(matches!(
            map_write_error(&old_peer),
            CatalogError::Unsupported(_)
        ));

        let old_server = PeerError::Rpc(tonic::Status::failed_precondition(
            "protocol version 2 is not supported",
        ));
        assert!(matches!(
            map_write_error(&old_server),
            CatalogError::Unsupported(_)
        ));
    }
}
