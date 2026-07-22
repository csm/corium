//! A fluent, async, Datomic-style client for corium.
//!
//! This crate is the ergonomic front door to the peer. It offers one API
//! surface over two backends:
//!
//! - [`LocalPeer`] wraps the [`corium_peer::Connection`] library, so queries
//!   run in-process against immutable database values read directly from
//!   storage — no round trip to the transactor.
//! - [`RemotePeer`] speaks the peer-server gRPC protocol, presenting the same
//!   surface to processes that reach a hosted peer over the network.
//!
//! Both implement the [`Peer`] trait and hand back [`Db`] values that share
//! the [`Db::query`], [`Db::pull`], [`Db::datoms`], and time-view
//! ([`Db::as_of`], [`Db::since`], [`Db::history`]) methods.
//!
//! Datalog queries and pull specifications are built as typesafe, immutable
//! values (see the [`query`] and [`pull`] modules) that lower to the boundary
//! EDN the engine parses, so a malformed query is a compile error rather than
//! a runtime parse failure.
//!
//! ```no_run
//! use corium_client::{LocalPeer, Peer};
//! use corium_client::query::{Query, data, var, attr};
//! use corium_peer::ConnectConfig;
//!
//! # async fn demo() -> Result<(), corium_client::ClientError> {
//! let peer = LocalPeer::connect(ConnectConfig::new("http://127.0.0.1:4334", "people")).await?;
//! let db = peer.db().await?;
//! let q = Query::find([var("name")])
//!     .where_(data(var("e"), attr("person/name"), var("name")));
//! let result = db.query(&q, Default::default()).await?;
//! for row in result.rows() {
//!     let name: String = row.get(0)?;
//!     println!("{name}");
//! }
//! # Ok(())
//! # }
//! ```

pub mod pull;
pub mod query;
pub mod result;
pub mod tx;
pub mod value;

mod local;
mod remote;

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use corium_core::EntityId;
use corium_query::edn::Edn;
use thiserror::Error;

pub use crate::local::LocalPeer;
pub use crate::pull::Pull;
pub use crate::query::Query;
pub use crate::remote::RemotePeer;
pub use crate::result::{QueryResult, ResultShape, Row};
pub use crate::tx::{TxBuilder, TxData};
pub use crate::value::{FromEdn, IntoEdn};

/// A failure from the fluent client layer.
#[derive(Debug, Error)]
pub enum ClientError {
    /// A peer-library failure (local backend).
    #[error(transparent)]
    Peer(#[from] corium_peer::PeerError),
    /// A query-engine failure (local execution).
    #[error(transparent)]
    Query(#[from] corium_query::QueryError),
    /// A gRPC status (remote backend).
    #[error(transparent)]
    Rpc(#[from] tonic::Status),
    /// A transport failure establishing a remote connection.
    #[error(transparent)]
    Transport(#[from] tonic::transport::Error),
    /// A wire codec failure.
    #[error(transparent)]
    Codec(#[from] corium_protocol::codec::CodecError),
    /// A result or argument could not be decoded to the requested type.
    #[error("decode error: {0}")]
    Decode(String),
    /// A protocol contract was violated.
    #[error("protocol error: {0}")]
    Protocol(String),
}

/// A database time view.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum View {
    /// The latest value.
    Current,
    /// The value as of transaction `t` (inclusive).
    AsOf(u64),
    /// Only assertions since transaction `t`.
    Since(u64),
    /// The full history view, including retractions.
    History,
}

/// A covering index, naming a datom-scan order.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Index {
    /// Entity, attribute, value, tx.
    Eavt,
    /// Attribute, entity, value, tx.
    Aevt,
    /// Attribute, value, entity, tx (the value index).
    Avet,
    /// Value, attribute, entity, tx (the reverse-ref index).
    Vaet,
}

impl Index {
    /// The wire name (`"eavt"`, `"aevt"`, `"avet"`, `"vaet"`).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Eavt => "eavt",
            Self::Aevt => "aevt",
            Self::Avet => "avet",
            Self::Vaet => "vaet",
        }
    }
}

/// One raw datom returned by [`Db::datoms`]. Entity, attribute, and
/// transaction positions are raw ids; the value is boundary [`Edn`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatomRow {
    /// Entity id.
    pub e: u64,
    /// Attribute id.
    pub a: u64,
    /// Value.
    pub v: Edn,
    /// Transaction id.
    pub tx: u64,
    /// Assertion flag (`false` on history-view retractions).
    pub added: bool,
}

/// Coarse database statistics.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct DbStats {
    /// Basis transaction of the view.
    pub basis_t: u64,
    /// Datom count.
    pub datoms: u64,
    /// Distinct entity count.
    pub entities: u64,
    /// Attribute count.
    pub attributes: u64,
}

/// The result of a committed transaction.
#[derive(Clone, Debug)]
pub struct TxReport {
    /// Basis before the transaction.
    pub basis_before: u64,
    /// The transaction's `t`.
    pub basis_t: u64,
    /// Commit timestamp (Unix milliseconds).
    pub tx_instant: i64,
    /// Tempid string to allocated entity id.
    pub tempids: BTreeMap<String, EntityId>,
    /// A database value pinned to the state right after the transaction.
    pub db_after: Db,
}

/// The backend that resolves [`Db`] operations, implemented in-process by the
/// local peer and over gRPC by the remote peer-server client.
#[async_trait]
pub(crate) trait DbBackend: Send + Sync {
    fn db_name(&self) -> &str;

    async fn query(
        &self,
        view: View,
        query: Edn,
        args: Vec<Edn>,
        fuel: Option<u64>,
    ) -> Result<QueryResult, ClientError>;

    async fn pull(&self, view: View, pattern: Edn, eid: Edn) -> Result<Edn, ClientError>;

    async fn datoms(
        &self,
        view: View,
        index: Index,
        components: Vec<Edn>,
        limit: usize,
    ) -> Result<Vec<DatomRow>, ClientError>;

    async fn stats(&self, view: View) -> Result<DbStats, ClientError>;
}

/// An immutable database value.
///
/// A `Db` names a snapshot to read from; [`Db::as_of`], [`Db::since`], and
/// [`Db::history`] derive new views cheaply. Every read method is async
/// because the remote backend may need a round trip; the local backend
/// resolves in-process.
#[derive(Clone)]
pub struct Db {
    backend: Arc<dyn DbBackend>,
    view: View,
}

impl Db {
    pub(crate) fn new(backend: Arc<dyn DbBackend>, view: View) -> Self {
        Self { backend, view }
    }

    /// The database name.
    #[must_use]
    pub fn db_name(&self) -> &str {
        self.backend.db_name()
    }

    /// This value's time view.
    #[must_use]
    pub fn view(&self) -> View {
        self.view
    }

    /// The value as of transaction `t` (inclusive).
    #[must_use]
    pub fn as_of(&self, t: u64) -> Self {
        Self {
            backend: Arc::clone(&self.backend),
            view: View::AsOf(t),
        }
    }

    /// The value including only assertions since transaction `t`.
    #[must_use]
    pub fn since(&self, t: u64) -> Self {
        Self {
            backend: Arc::clone(&self.backend),
            view: View::Since(t),
        }
    }

    /// The full history view, including retractions.
    #[must_use]
    pub fn history(&self) -> Self {
        Self {
            backend: Arc::clone(&self.backend),
            view: View::History,
        }
    }

    /// Runs a typed [`Query`] with input arguments.
    ///
    /// The receiver binds the query's default `$` source; additional
    /// non-database `:in` inputs are supplied positionally by `args`.
    ///
    /// # Errors
    /// Returns [`ClientError`] for malformed queries or execution failures.
    pub async fn query(&self, query: &Query, args: Args) -> Result<QueryResult, ClientError> {
        self.backend
            .query(self.view, query.to_edn(), args.arg_forms(), args.fuel)
            .await
    }

    /// Runs a raw boundary-[`Edn`] query with positional argument forms — an
    /// escape hatch for queries not built through [`Query`].
    ///
    /// # Errors
    /// Returns [`ClientError`] for malformed queries or execution failures.
    pub async fn query_edn(&self, query: Edn, args: Vec<Edn>) -> Result<QueryResult, ClientError> {
        self.backend.query(self.view, query, args, None).await
    }

    /// Pulls a typed [`Pull`] specification for one entity.
    ///
    /// The entity is named by any [`IntoEdn`] value the boundary accepts: an
    /// entity id (long), an ident keyword, or a lookup ref
    /// (`tx::lookup(...)`).
    ///
    /// # Errors
    /// Returns [`ClientError`] for malformed patterns or unknown idents.
    pub async fn pull(&self, pattern: &Pull, entity: impl IntoEdn) -> Result<Edn, ClientError> {
        self.backend
            .pull(self.view, pattern.to_edn(), entity.into_edn())
            .await
    }

    /// Scans datoms from a covering index, binding `components` as a leading
    /// prefix in the index's order.
    ///
    /// # Errors
    /// Returns [`ClientError`] on bad components or transport failure.
    pub async fn datoms(
        &self,
        index: Index,
        components: Vec<Edn>,
        limit: usize,
    ) -> Result<Vec<DatomRow>, ClientError> {
        self.backend
            .datoms(self.view, index, components, limit)
            .await
    }

    /// Coarse statistics for this view.
    ///
    /// # Errors
    /// Returns [`ClientError`] on transport failure.
    pub async fn stats(&self) -> Result<DbStats, ClientError> {
        self.backend.stats(self.view).await
    }

    /// The basis transaction of this view.
    ///
    /// # Errors
    /// Returns [`ClientError`] on transport failure.
    pub async fn basis_t(&self) -> Result<u64, ClientError> {
        Ok(self.stats().await?.basis_t)
    }
}

impl std::fmt::Debug for Db {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Db")
            .field("db_name", &self.backend.db_name())
            .field("view", &self.view)
            .finish()
    }
}

/// Positional non-database arguments for a [`Query`]'s `:in` inputs, in
/// declaration order.
#[derive(Clone, Debug, Default)]
pub struct Args {
    values: Vec<Edn>,
    fuel: Option<u64>,
}

impl Args {
    /// An empty argument list.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends a scalar argument for an `in_scalar` input.
    #[must_use]
    pub fn scalar(mut self, value: impl IntoEdn) -> Self {
        self.values.push(value.into_edn());
        self
    }

    /// Appends a tuple argument for an `in_tuple` input.
    #[must_use]
    pub fn tuple<V: IntoEdn>(mut self, values: impl IntoIterator<Item = V>) -> Self {
        self.values.push(Edn::Vector(
            values.into_iter().map(IntoEdn::into_edn).collect(),
        ));
        self
    }

    /// Appends a collection argument for an `in_coll` input.
    #[must_use]
    pub fn coll<V: IntoEdn>(mut self, values: impl IntoIterator<Item = V>) -> Self {
        self.values.push(Edn::Vector(
            values.into_iter().map(IntoEdn::into_edn).collect(),
        ));
        self
    }

    /// Appends a relation argument (vector of tuples) for an `in_rel` input.
    #[must_use]
    pub fn relation<V: IntoEdn>(mut self, tuples: impl IntoIterator<Item = Vec<V>>) -> Self {
        let rows = tuples
            .into_iter()
            .map(|tuple| Edn::Vector(tuple.into_iter().map(IntoEdn::into_edn).collect()))
            .collect();
        self.values.push(Edn::Vector(rows));
        self
    }

    /// Appends a raw boundary form as the next argument.
    #[must_use]
    pub fn arg(mut self, value: Edn) -> Self {
        self.values.push(value);
        self
    }

    /// Bounds the query's execution fuel (datoms touched).
    #[must_use]
    pub fn fuel(mut self, fuel: u64) -> Self {
        self.fuel = Some(fuel);
        self
    }

    fn arg_forms(&self) -> Vec<Edn> {
        self.values.clone()
    }
}

/// A live connection to a database, backed either by the local peer library
/// or a remote peer server.
///
/// Both [`LocalPeer`] and [`RemotePeer`] implement this trait, so code can be
/// written once against `impl Peer` and run against either backend.
#[async_trait]
pub trait Peer: Send + Sync {
    /// The connected database name.
    fn db_name(&self) -> &str;

    /// The current database value.
    ///
    /// # Errors
    /// Returns [`ClientError`] on transport failure.
    async fn db(&self) -> Result<Db, ClientError>;

    /// Submits a transaction and waits until it is applied, returning a
    /// report whose `db_after` observes it.
    ///
    /// # Errors
    /// Returns [`ClientError`] for rejected transactions or transport
    /// failure.
    async fn transact(&self, tx: TxData) -> Result<TxReport, ClientError>;

    /// Waits until the local view reaches the transactor's current basis and
    /// returns the resulting database value.
    ///
    /// # Errors
    /// Returns [`ClientError`] on transport failure.
    async fn sync(&self) -> Result<Db, ClientError>;
}
