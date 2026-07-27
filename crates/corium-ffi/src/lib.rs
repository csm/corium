//! Owned, runtime-neutral facade for native language adapters.
//!
//! This crate keeps `PyO3` and other language runtimes out of Corium's core
//! crates. It exposes opaque peer/database handles, owned composite-protocol
//! payloads, and a stable error taxonomy. Dropping a future cancels the
//! caller's wait; [`PeerHandle::close`] deterministically prevents further
//! operations and releases the live peer held by the facade.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, Weak};

use corium_client::{
    ClientError, Db, LocalPeer, Peer, RemotePeer, ResultShape as ClientResultShape, TxData,
};
use corium_peer::{ConnectConfig, PeerError};
use corium_protocol::codec;
use corium_query::QueryError;
use corium_query::edn::Edn;
use thiserror::Error;
use tonic::transport::ClientTlsConfig;
use tonic::{Code, Status};

/// Stable categories that language adapters map to native exception classes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorKind {
    /// A peer or server could not be reached.
    Connection,
    /// Credentials were missing or rejected.
    Authentication,
    /// The caller is authenticated but not authorized.
    PermissionDenied,
    /// The peer violated or rejected the protocol contract.
    Protocol,
    /// Query parsing or execution failed.
    Query,
    /// Transaction validation or commit failed.
    Transaction,
    /// A boundary value could not be decoded.
    Decode,
    /// Direct storage access failed.
    Storage,
    /// Query execution consumed its fuel budget.
    FuelExhausted,
    /// The facade handle was explicitly closed.
    Closed,
}

/// A language-neutral facade error.
#[derive(Clone, Debug, Error)]
#[error("{message}")]
pub struct FfiError {
    kind: ErrorKind,
    message: String,
    grpc_code: Option<i32>,
}

impl FfiError {
    /// The stable error category.
    #[must_use]
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// An owned diagnostic message.
    #[must_use]
    pub fn message(&self) -> String {
        self.message.clone()
    }

    /// The numeric gRPC status code, when the failure came from an RPC.
    #[must_use]
    pub fn grpc_code(&self) -> Option<i32> {
        self.grpc_code
    }

    fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            grpc_code: None,
        }
    }

    fn from_status(status: &Status) -> Self {
        let kind = match status.code() {
            Code::Unauthenticated => ErrorKind::Authentication,
            Code::PermissionDenied => ErrorKind::PermissionDenied,
            Code::Unavailable | Code::DeadlineExceeded => ErrorKind::Connection,
            Code::ResourceExhausted => ErrorKind::FuelExhausted,
            _ => ErrorKind::Protocol,
        };
        Self {
            kind,
            message: status.to_string(),
            grpc_code: Some(status.code() as i32),
        }
    }

    fn from_client(error: ClientError) -> Self {
        match error {
            ClientError::Peer(PeerError::Transport(error)) => {
                Self::new(ErrorKind::Connection, error.to_string())
            }
            ClientError::Peer(PeerError::Rpc(status)) | ClientError::Rpc(status) => {
                Self::from_status(&status)
            }
            ClientError::Peer(PeerError::Codec(error)) | ClientError::Codec(error) => {
                Self::new(ErrorKind::Decode, error.to_string())
            }
            ClientError::Peer(PeerError::Snapshot(error)) => {
                Self::new(ErrorKind::Storage, error.to_string())
            }
            ClientError::Peer(PeerError::Protocol(message)) | ClientError::Protocol(message) => {
                Self::new(ErrorKind::Protocol, message)
            }
            ClientError::Peer(PeerError::Closed) => {
                Self::new(ErrorKind::Closed, "connection closed")
            }
            ClientError::Query(QueryError::FuelExhausted) => {
                Self::new(ErrorKind::FuelExhausted, "query fuel exhausted")
            }
            ClientError::Query(error) => Self::new(ErrorKind::Query, error.to_string()),
            ClientError::Transport(error) => Self::new(ErrorKind::Connection, error.to_string()),
            ClientError::Decode(message) => Self::new(ErrorKind::Decode, message),
        }
    }

    fn for_operation(mut self, fallback: ErrorKind) -> Self {
        if matches!(self.kind, ErrorKind::Protocol) && self.grpc_code.is_none() {
            self.kind = fallback;
        }
        self
    }
}

/// One validated, owned Corium composite-protocol value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompositeValue(Vec<u8>);

impl CompositeValue {
    /// Validates and owns one standalone composite value.
    ///
    /// # Errors
    /// Returns [`ErrorKind::Decode`] for malformed or trailing bytes.
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, FfiError> {
        codec::decode_edn(&bytes)
            .map_err(|error| FfiError::new(ErrorKind::Decode, error.to_string()))?;
        Ok(Self(bytes))
    }

    /// Copies the encoded bytes for a language adapter.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        self.0.clone()
    }

    fn from_edn(value: &Edn) -> Self {
        Self(codec::encode_edn(value))
    }

    fn decode(&self) -> Result<Edn, FfiError> {
        codec::decode_edn(&self.0)
            .map_err(|error| FfiError::new(ErrorKind::Decode, error.to_string()))
    }
}

/// Options for an in-process full peer.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalConnectOptions {
    /// Ordered transactor endpoints.
    pub endpoints: Vec<String>,
    /// Database name.
    pub database_name: String,
    /// Optional bearer token.
    pub token: Option<String>,
    /// Use platform TLS roots for the endpoints.
    pub tls: bool,
}

/// Options for a lightweight peer-server client.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemoteConnectOptions {
    /// Peer-server endpoint.
    pub endpoint: String,
    /// Database name.
    pub database_name: String,
    /// Optional bearer token.
    pub token: Option<String>,
    /// Use platform TLS roots for the endpoint.
    pub tls: bool,
}

struct PeerState {
    database_name: String,
    peer: Mutex<Option<Arc<dyn Peer>>>,
    dbs: Mutex<Vec<Weak<DbState>>>,
}

impl PeerState {
    fn peer(&self) -> Result<Arc<dyn Peer>, FfiError> {
        let guard = self
            .peer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard
            .clone()
            .ok_or_else(|| FfiError::new(ErrorKind::Closed, "peer is closed"))
    }

    fn is_closed(&self) -> bool {
        self.peer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_none()
    }

    fn close(&self) {
        self.peer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let dbs = {
            let mut guard = self
                .dbs
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            std::mem::take(&mut *guard)
        };
        for db in dbs {
            if let Some(db) = db.upgrade() {
                db.close();
            }
        }
    }

    fn register_db(&self, db: &Arc<DbState>) {
        let mut guard = self
            .dbs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.is_closed() {
            drop(guard);
            db.close();
            return;
        }
        guard.retain(|handle| handle.strong_count() > 0);
        guard.push(Arc::downgrade(db));
    }
}

/// Opaque handle to either a local or remote peer.
#[derive(Clone)]
pub struct PeerHandle {
    state: Arc<PeerState>,
}

impl PeerHandle {
    /// Connects an in-process full peer.
    ///
    /// # Errors
    /// Returns a categorized connection or protocol failure.
    pub async fn connect_local(options: LocalConnectOptions) -> Result<Self, FfiError> {
        if options.endpoints.is_empty() {
            return Err(FfiError::new(
                ErrorKind::Connection,
                "at least one transactor endpoint is required",
            ));
        }
        let tls = options.tls.then(tls_config);
        let mut config =
            ConnectConfig::with_failover(options.endpoints, options.database_name.clone());
        config.token = options.token;
        config.tls = tls;
        let peer = LocalPeer::connect(config)
            .await
            .map_err(FfiError::from_client)?;
        Ok(Self::new(Arc::new(peer)))
    }

    /// Connects a lightweight peer-server client.
    ///
    /// # Errors
    /// Returns a categorized connection or protocol failure.
    pub async fn connect_remote(options: RemoteConnectOptions) -> Result<Self, FfiError> {
        let peer = RemotePeer::connect(
            options.endpoint,
            options.database_name,
            options.token,
            options.tls.then(tls_config),
        )
        .await
        .map_err(FfiError::from_client)?;
        Ok(Self::new(Arc::new(peer)))
    }

    /// The connected database name.
    #[must_use]
    pub fn database_name(&self) -> String {
        self.state.database_name.clone()
    }

    /// Whether [`Self::close`] has been called.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.state.is_closed()
    }

    /// Returns the current immutable database value.
    ///
    /// # Errors
    /// Returns [`ErrorKind::Closed`] after explicit close.
    pub async fn db(&self) -> Result<DbHandle, FfiError> {
        let db = self
            .state
            .peer()?
            .db()
            .await
            .map_err(FfiError::from_client)?;
        Ok(DbHandle::new(db, Arc::clone(&self.state)))
    }

    /// Synchronizes with the transactor and returns the resulting database.
    ///
    /// # Errors
    /// Returns a categorized client failure.
    pub async fn sync(&self) -> Result<DbHandle, FfiError> {
        let db = self
            .state
            .peer()?
            .sync()
            .await
            .map_err(FfiError::from_client)?;
        Ok(DbHandle::new(db, Arc::clone(&self.state)))
    }

    /// Submits raw transaction forms encoded as a composite vector.
    ///
    /// # Errors
    /// Returns [`ErrorKind::Decode`] unless `forms` is a vector, or a
    /// categorized transaction/client failure.
    pub async fn transact(&self, forms: CompositeValue) -> Result<TxReport, FfiError> {
        let Edn::Vector(forms) = forms.decode()? else {
            return Err(FfiError::new(
                ErrorKind::Decode,
                "transaction data must be a vector of forms",
            ));
        };
        let report = self
            .state
            .peer()?
            .transact(TxData::from(forms))
            .await
            .map_err(FfiError::from_client)
            .map_err(|error| error.for_operation(ErrorKind::Transaction))?;
        Ok(TxReport {
            basis_before: report.basis_before,
            basis_t: report.basis_t,
            tx_instant: report.tx_instant,
            tempids: report
                .tempids
                .into_iter()
                .map(|(name, id)| (name, id.raw()))
                .collect(),
            db_after: DbHandle::new(report.db_after, Arc::clone(&self.state)),
        })
    }

    /// Idempotently prevents new work and releases the facade's live peer.
    pub fn close(&self) {
        self.state.close();
    }

    fn new(peer: Arc<dyn Peer>) -> Self {
        Self {
            state: Arc::new(PeerState {
                database_name: peer.db_name().to_owned(),
                peer: Mutex::new(Some(peer)),
                dbs: Mutex::new(Vec::new()),
            }),
        }
    }
}

fn tls_config() -> ClientTlsConfig {
    ClientTlsConfig::new().with_enabled_roots()
}

/// Opaque immutable database handle.
#[derive(Clone)]
pub struct DbHandle {
    inner: Arc<DbState>,
    state: Arc<PeerState>,
}

struct DbState {
    db: Mutex<Option<Db>>,
}

impl DbState {
    fn db(&self) -> Result<Db, FfiError> {
        self.db
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .ok_or_else(|| FfiError::new(ErrorKind::Closed, "peer is closed"))
    }

    fn close(&self) {
        self.db
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
    }
}

impl DbHandle {
    fn new(db: Db, state: Arc<PeerState>) -> Self {
        let inner = Arc::new(DbState {
            db: Mutex::new(Some(db)),
        });
        state.register_db(&inner);
        Self { inner, state }
    }

    fn db(&self) -> Result<Db, FfiError> {
        if self.state.is_closed() {
            Err(FfiError::new(ErrorKind::Closed, "peer is closed"))
        } else {
            self.inner.db()
        }
    }

    /// The database name.
    #[must_use]
    pub fn database_name(&self) -> String {
        self.state.database_name.clone()
    }

    /// Derives an as-of transaction view.
    ///
    /// # Errors
    /// Returns [`ErrorKind::Closed`] after peer close.
    pub fn as_of(&self, t: u64) -> Result<Self, FfiError> {
        Ok(Self::new(self.db()?.as_of(t), Arc::clone(&self.state)))
    }

    /// Derives a since-transaction view.
    ///
    /// # Errors
    /// Returns [`ErrorKind::Closed`] after peer close.
    pub fn since(&self, t: u64) -> Result<Self, FfiError> {
        Ok(Self::new(self.db()?.since(t), Arc::clone(&self.state)))
    }

    /// Derives a full-history view.
    ///
    /// # Errors
    /// Returns [`ErrorKind::Closed`] after peer close.
    pub fn history(&self) -> Result<Self, FfiError> {
        Ok(Self::new(self.db()?.history(), Arc::clone(&self.state)))
    }

    /// Derives an as-of wall-clock view using Unix milliseconds.
    ///
    /// # Errors
    /// Returns [`ErrorKind::Closed`] after peer close.
    pub fn as_of_instant(&self, unix_millis: i64) -> Result<Self, FfiError> {
        Ok(Self::new(
            self.db()?.as_of_instant(unix_millis),
            Arc::clone(&self.state),
        ))
    }

    /// Derives a since wall-clock view using Unix milliseconds.
    ///
    /// # Errors
    /// Returns [`ErrorKind::Closed`] after peer close.
    pub fn since_instant(&self, unix_millis: i64) -> Result<Self, FfiError> {
        Ok(Self::new(
            self.db()?.since_instant(unix_millis),
            Arc::clone(&self.state),
        ))
    }

    /// Executes a raw query and returns its explicit result shape.
    ///
    /// # Errors
    /// Returns a categorized decode, query, connection, or close failure.
    pub async fn query(
        &self,
        query: CompositeValue,
        args: Vec<CompositeValue>,
        fuel: Option<u64>,
    ) -> Result<QueryOutput, FfiError> {
        let db = self.db()?;
        let query = query.decode()?;
        let args = args
            .into_iter()
            .map(|arg| arg.decode())
            .collect::<Result<Vec<_>, _>>()?;
        let result = db
            .query_edn_with_fuel(query, args, fuel)
            .await
            .map_err(FfiError::from_client)
            .map_err(|error| error.for_operation(ErrorKind::Query))?;
        Ok(QueryOutput {
            shape: result.shape().into(),
            value: CompositeValue::from_edn(&result.into_edn()),
        })
    }

    /// Executes a raw Pull pattern.
    ///
    /// # Errors
    /// Returns a categorized decode, query, connection, or close failure.
    pub async fn pull(
        &self,
        pattern: CompositeValue,
        entity: CompositeValue,
    ) -> Result<CompositeValue, FfiError> {
        let db = self.db()?;
        let value = db
            .pull_edn(pattern.decode()?, entity.decode()?)
            .await
            .map_err(FfiError::from_client)
            .map_err(|error| error.for_operation(ErrorKind::Query))?;
        Ok(CompositeValue::from_edn(&value))
    }

    /// Scans a covering index from a component prefix.
    ///
    /// # Errors
    /// Returns a categorized decode, query, connection, or close failure.
    pub async fn datoms(
        &self,
        index: Index,
        components: Vec<CompositeValue>,
        limit: u64,
    ) -> Result<Vec<Datom>, FfiError> {
        let db = self.db()?;
        let components = components
            .into_iter()
            .map(|component| component.decode())
            .collect::<Result<Vec<_>, _>>()?;
        let limit = usize::try_from(limit)
            .map_err(|_| FfiError::new(ErrorKind::Protocol, "datom limit is out of range"))?;
        db.datoms(index.into(), components, limit)
            .await
            .map(|rows| {
                rows.into_iter()
                    .map(|row| Datom {
                        e: row.e,
                        a: row.a,
                        value: CompositeValue::from_edn(&row.v),
                        tx: row.tx,
                        added: row.added,
                    })
                    .collect()
            })
            .map_err(FfiError::from_client)
            .map_err(|error| error.for_operation(ErrorKind::Query))
    }

    /// Returns coarse statistics for this database view.
    ///
    /// # Errors
    /// Returns a categorized connection or close failure.
    pub async fn stats(&self) -> Result<DbStats, FfiError> {
        self.db()?
            .stats()
            .await
            .map(DbStats::from)
            .map_err(FfiError::from_client)
    }

    /// Returns the basis transaction for this database view.
    ///
    /// # Errors
    /// Returns a categorized connection or close failure.
    pub async fn basis_t(&self) -> Result<u64, FfiError> {
        Ok(self.stats().await?.basis_t)
    }
}

/// Query result shape independent of the Rust client type.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ResultShape {
    /// A set of tuples.
    Relation,
    /// A flat collection.
    Collection,
    /// A single tuple.
    Tuple,
    /// A single scalar.
    Scalar,
}

impl From<ClientResultShape> for ResultShape {
    fn from(value: ClientResultShape) -> Self {
        match value {
            ClientResultShape::Relation => Self::Relation,
            ClientResultShape::Collection => Self::Collection,
            ClientResultShape::Tuple => Self::Tuple,
            ClientResultShape::Scalar => Self::Scalar,
        }
    }
}

/// Owned raw query output.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryOutput {
    /// Shape declared by the query's find specification.
    pub shape: ResultShape,
    /// Composite-encoded result value.
    pub value: CompositeValue,
}

/// A covering datom index.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Index {
    /// Entity, attribute, value, transaction.
    Eavt,
    /// Attribute, entity, value, transaction.
    Aevt,
    /// Attribute, value, entity, transaction.
    Avet,
    /// Value, attribute, entity, transaction.
    Vaet,
}

impl From<Index> for corium_client::Index {
    fn from(value: Index) -> Self {
        match value {
            Index::Eavt => Self::Eavt,
            Index::Aevt => Self::Aevt,
            Index::Avet => Self::Avet,
            Index::Vaet => Self::Vaet,
        }
    }
}

/// One owned datom returned by an index scan.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Datom {
    /// Entity id.
    pub e: u64,
    /// Attribute id.
    pub a: u64,
    /// Composite-encoded value.
    pub value: CompositeValue,
    /// Transaction id.
    pub tx: u64,
    /// Whether this is an assertion rather than a retraction.
    pub added: bool,
}

/// Coarse database statistics.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DbStats {
    /// Basis transaction.
    pub basis_t: u64,
    /// Datom count.
    pub datoms: u64,
    /// Distinct entity count.
    pub entities: u64,
    /// Attribute count.
    pub attributes: u64,
}

impl From<corium_client::DbStats> for DbStats {
    fn from(value: corium_client::DbStats) -> Self {
        Self {
            basis_t: value.basis_t,
            datoms: value.datoms,
            entities: value.entities,
            attributes: value.attributes,
        }
    }
}

/// Result of a committed transaction.
#[derive(Clone)]
pub struct TxReport {
    /// Basis before the transaction.
    pub basis_before: u64,
    /// The transaction's basis.
    pub basis_t: u64,
    /// Commit timestamp in Unix milliseconds.
    pub tx_instant: i64,
    /// Tempid string to allocated entity id.
    pub tempids: BTreeMap<String, u64>,
    /// Immutable database value immediately after the commit.
    pub db_after: DbHandle,
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;

    use super::*;

    struct FailingPeer;

    #[async_trait]
    impl Peer for FailingPeer {
        #[allow(clippy::unnecessary_literal_bound)]
        fn db_name(&self) -> &str {
            "test"
        }

        async fn db(&self) -> Result<Db, ClientError> {
            Err(ClientError::Protocol("called".into()))
        }

        async fn transact(&self, _tx: TxData) -> Result<corium_client::TxReport, ClientError> {
            Err(ClientError::Protocol("called".into()))
        }

        async fn sync(&self) -> Result<Db, ClientError> {
            Err(ClientError::Protocol("called".into()))
        }
    }

    #[test]
    fn composite_values_validate_and_round_trip() {
        let form = Edn::Vector(vec![
            Edn::keyword("person/name"),
            Edn::Str("Ada".into()),
            Edn::Tagged("eid".into(), Box::new(Edn::Long(42))),
        ]);
        let value = CompositeValue::from_edn(&form);
        assert_eq!(value.decode().expect("valid composite value"), form);
        assert_eq!(
            CompositeValue::from_bytes(vec![0xff])
                .expect_err("unknown tag")
                .kind(),
            ErrorKind::Decode
        );
    }

    #[tokio::test]
    async fn close_is_shared_idempotent_and_prevents_new_work() {
        let peer = PeerHandle::new(Arc::new(FailingPeer));
        let clone = peer.clone();
        assert_eq!(peer.database_name(), "test");
        let Err(error) = peer.db().await else {
            panic!("fake peer unexpectedly returned a database");
        };
        assert_eq!(error.kind(), ErrorKind::Protocol);

        peer.close();
        peer.close();
        assert!(clone.is_closed());
        let Err(error) = clone.db().await else {
            panic!("closed peer unexpectedly returned a database");
        };
        assert_eq!(error.kind(), ErrorKind::Closed);
    }

    #[test]
    fn query_fuel_has_a_stable_category() {
        let error = FfiError::from_client(ClientError::Query(QueryError::FuelExhausted));
        assert_eq!(error.kind(), ErrorKind::FuelExhausted);
    }
}
