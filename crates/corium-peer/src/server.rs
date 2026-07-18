//! Peer server: hosts a peer for thin clients over the public
//! `PeerServerService` gRPC surface (see `docs/design/protocol.md`).
//!
//! Queries execute server-side against the hosted peer's local database
//! values with per-request fuel and chunked result streams.

use std::pin::Pin;
use std::sync::Arc;

use corium_core::{EntityId, IndexOrder};
use corium_db::{Db, key_prefix};
use corium_protocol::auth::{AuthInterceptor, Authenticator};
use corium_protocol::codec;
use corium_protocol::pb;
use corium_protocol::pb::peer_server_server::{PeerServer, PeerServerServer};
use corium_query::edn::Edn;
use corium_query::{ExecOptions, QInput, QueryCache, ast, boundary, exec};
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use crate::Connection;

/// Peer-server execution limits.
#[derive(Clone, Copy, Debug)]
pub struct PeerServerConfig {
    /// Fuel ceiling per query (datoms touched).
    pub max_fuel: u64,
    /// Rows or datoms per streamed chunk.
    pub chunk_size: usize,
    /// Maximum datoms returned by one `Datoms` request.
    pub max_datoms: usize,
}

impl Default for PeerServerConfig {
    fn default() -> Self {
        Self {
            max_fuel: 10_000_000,
            chunk_size: 1024,
            max_datoms: 1_000_000,
        }
    }
}

/// The hosted-peer gRPC service.
pub struct PeerServerSvc {
    connection: Arc<Connection>,
    cache: QueryCache,
    config: PeerServerConfig,
}

impl PeerServerSvc {
    /// Hosts `connection` with the given limits.
    #[must_use]
    pub fn new(connection: Arc<Connection>, config: PeerServerConfig) -> Self {
        Self {
            connection,
            cache: QueryCache::new(),
            config,
        }
    }

    fn view_db(&self, spec: Option<&pb::DbViewSpec>) -> Result<Db, Status> {
        let spec = spec.ok_or_else(|| Status::invalid_argument("request names no database"))?;
        if spec.db != self.connection.db_name() {
            return Err(Status::not_found(format!(
                "this peer server hosts {:?}, not {:?}",
                self.connection.db_name(),
                spec.db
            )));
        }
        let base = self.connection.db();
        Ok(match &spec.view {
            Some(pb::db_view_spec::View::AsOf(t)) => base.as_of(*t),
            Some(pb::db_view_spec::View::Since(t)) => base.since(*t),
            Some(pb::db_view_spec::View::History(true)) => base.history(),
            None | Some(pb::db_view_spec::View::History(false)) => base,
        })
    }
}

fn decode_edn(bytes: &[u8], what: &str) -> Result<Edn, Status> {
    codec::decode_edn(bytes)
        .map_err(|error| Status::invalid_argument(format!("bad {what}: {error}")))
}

fn resolve_eid(db: &Db, form: &Edn) -> Result<EntityId, Status> {
    match form {
        Edn::Long(n) => u64::try_from(*n)
            .map(EntityId::from_raw)
            .map_err(|_| Status::invalid_argument("negative entity id")),
        Edn::Tagged(tag, value) if tag == "eid" => match value.as_ref() {
            Edn::Long(n) => u64::try_from(*n)
                .map(EntityId::from_raw)
                .map_err(|_| Status::invalid_argument("negative entity id")),
            _ => Err(Status::invalid_argument("#eid requires a long")),
        },
        Edn::Keyword(keyword) => db
            .idents()
            .entid(keyword)
            .ok_or_else(|| Status::invalid_argument(format!("unknown ident {keyword}"))),
        Edn::Vector(items) => {
            let [attr_form, value_form] = items.as_slice() else {
                return Err(Status::invalid_argument("lookup ref requires [attr value]"));
            };
            let attr = attr_form
                .as_keyword()
                .and_then(|keyword| db.idents().entid(keyword))
                .ok_or_else(|| Status::invalid_argument("unknown lookup attribute"))?;
            let value = boundary::edn_to_value(Some(db), value_form)
                .ok_or_else(|| Status::invalid_argument("bad lookup value"))?;
            let value = db.schema().get(attr).map_or(value.clone(), |meta| {
                exec::coerce_for_type(value, meta.value_type)
            });
            db.lookup(attr, &value)
                .ok_or_else(|| Status::not_found(format!("lookup ref {form} did not resolve")))
        }
        other => Err(Status::invalid_argument(format!(
            "bad entity position {other}"
        ))),
    }
}

type ChunkStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send>>;

enum Slot {
    E,
    A,
    V,
}

fn chunk_stream<T: Send + 'static>(chunks: Vec<T>) -> ChunkStream<T> {
    Box::pin(tokio_stream::iter(chunks.into_iter().map(Ok)))
}

#[tonic::async_trait]
impl PeerServer for PeerServerSvc {
    type QueryStream = ChunkStream<pb::QueryResultChunk>;

    async fn query(
        &self,
        request: Request<pb::QueryRequest>,
    ) -> Result<Response<Self::QueryStream>, Status> {
        let request = request.into_inner();
        let query_form = decode_edn(&request.query, "query")?;
        let parsed = self
            .cache
            .parse(&query_form)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let args = match decode_edn(&request.args, "args")? {
            Edn::Nil => Vec::new(),
            Edn::Vector(items) | Edn::List(items) => items,
            other => {
                return Err(Status::invalid_argument(format!(
                    "args must be a vector, got {other}"
                )));
            }
        };
        // Bind database views and arguments positionally per the :in spec.
        let mut dbs = Vec::new();
        for spec in &request.dbs {
            dbs.push(self.view_db(Some(spec))?);
        }
        let mut next_db = 0;
        let mut next_arg = 0;
        let mut inputs: Vec<QInput<'_>> = Vec::with_capacity(parsed.inputs.len());
        for spec in &parsed.inputs {
            if matches!(spec, ast::InSpec::Db(_)) {
                let db = dbs
                    .get(next_db)
                    .ok_or_else(|| Status::invalid_argument("query needs more database views"))?;
                inputs.push(QInput::Db(db));
                next_db += 1;
            } else {
                let arg = args
                    .get(next_arg)
                    .cloned()
                    .ok_or_else(|| Status::invalid_argument("query needs more arguments"))?;
                inputs.push(QInput::Edn(arg));
                next_arg += 1;
            }
        }
        let fuel = if request.fuel == 0 {
            self.config.max_fuel
        } else {
            request.fuel.min(self.config.max_fuel)
        };
        let (result, _) = corium_query::run(
            &parsed,
            &inputs,
            ExecOptions {
                fuel: Some(fuel),
                ..ExecOptions::default()
            },
        )
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let (shape, rows) = match (&parsed.find, result) {
            (ast::FindSpec::Rel(_), Edn::Vector(rows)) => (pb::ResultShape::Relation, rows),
            (ast::FindSpec::Coll(_), Edn::Vector(rows)) => (pb::ResultShape::Collection, rows),
            (ast::FindSpec::Tuple(_), value) => (pb::ResultShape::Tuple, vec![value]),
            (ast::FindSpec::Scalar(_), value) => (pb::ResultShape::Scalar, vec![value]),
            (_, value) => (pb::ResultShape::Relation, vec![value]),
        };
        let mut chunks = Vec::new();
        match shape {
            pb::ResultShape::Tuple | pb::ResultShape::Scalar => {
                chunks.push(pb::QueryResultChunk {
                    shape: shape.into(),
                    rows: codec::encode_edn(&rows.into_iter().next().unwrap_or(Edn::Nil)),
                    last: true,
                });
            }
            _ => {
                let groups: Vec<&[Edn]> = if rows.is_empty() {
                    vec![&[]]
                } else {
                    rows.chunks(self.config.chunk_size.max(1)).collect()
                };
                let total = groups.len();
                for (index, group) in groups.into_iter().enumerate() {
                    chunks.push(pb::QueryResultChunk {
                        shape: shape.into(),
                        rows: codec::encode_edn(&Edn::Vector(group.to_vec())),
                        last: index + 1 == total,
                    });
                }
            }
        }
        Ok(Response::new(chunk_stream(chunks)))
    }

    async fn pull(
        &self,
        request: Request<pb::PullRequest>,
    ) -> Result<Response<pb::PullResponse>, Status> {
        let request = request.into_inner();
        let db = self.view_db(request.db.as_ref())?;
        let pattern = decode_edn(&request.pattern, "pull pattern")?;
        let eid = resolve_eid(&db, &decode_edn(&request.eid, "entity")?)?;
        let result = corium_query::pull(&db, &pattern, eid)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        Ok(Response::new(pb::PullResponse {
            result: codec::encode_edn(&result),
        }))
    }

    async fn transact(
        &self,
        request: Request<pb::TransactRequest>,
    ) -> Result<Response<pb::TransactResponse>, Status> {
        let request = request.into_inner();
        if request.db != self.connection.db_name() {
            return Err(Status::not_found(format!(
                "this peer server hosts {:?}, not {:?}",
                self.connection.db_name(),
                request.db
            )));
        }
        let response = self
            .connection
            .transact_raw(request.tx_data)
            .await
            .map_err(|error| match error {
                crate::PeerError::Rpc(status) => status,
                other => Status::internal(other.to_string()),
            })?;
        // Read-your-writes for thin clients: later queries on this server
        // observe the transaction.
        self.connection
            .sync_to(response.basis_t)
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
        Ok(Response::new(response))
    }

    type DatomsStream = ChunkStream<pb::DatomChunk>;

    #[allow(clippy::too_many_lines)]
    async fn datoms(
        &self,
        request: Request<pb::DatomsRequest>,
    ) -> Result<Response<Self::DatomsStream>, Status> {
        let request = request.into_inner();
        let db = self.view_db(request.db.as_ref())?;
        let order = match request.index.as_str() {
            "eavt" => IndexOrder::Eavt,
            "aevt" => IndexOrder::Aevt,
            "avet" => IndexOrder::Avet,
            "vaet" => IndexOrder::Vaet,
            other => {
                return Err(Status::invalid_argument(format!("unknown index {other:?}")));
            }
        };
        let components = match decode_edn(&request.components, "components")? {
            Edn::Nil => Vec::new(),
            Edn::Vector(items) | Edn::List(items) => items,
            other => {
                return Err(Status::invalid_argument(format!(
                    "components must be a vector, got {other}"
                )));
            }
        };
        // Components bind positionally in index order and must be a prefix.
        let positions = match order {
            IndexOrder::Eavt => [Slot::E, Slot::A, Slot::V],
            IndexOrder::Aevt => [Slot::A, Slot::E, Slot::V],
            IndexOrder::Avet => [Slot::A, Slot::V, Slot::E],
            IndexOrder::Vaet => [Slot::V, Slot::A, Slot::E],
        };
        if components.len() > 3 {
            return Err(Status::invalid_argument("at most three components"));
        }
        let mut e = None;
        let mut a = None;
        let mut v = None;
        for (slot, form) in positions.iter().zip(&components) {
            match slot {
                Slot::E => e = Some(resolve_eid(&db, form)?),
                Slot::A => {
                    a = Some(match form {
                        Edn::Keyword(keyword) => db.idents().entid(keyword).ok_or_else(|| {
                            Status::invalid_argument(format!("unknown attribute {keyword}"))
                        })?,
                        other => resolve_eid(&db, other)?,
                    });
                }
                Slot::V => {
                    let value = boundary::edn_to_value(Some(&db), form)
                        .ok_or_else(|| Status::invalid_argument(format!("bad value {form}")))?;
                    let value = a
                        .and_then(|a| db.schema().get(a))
                        .map_or(value.clone(), |meta| {
                            exec::coerce_for_type(value, meta.value_type)
                        });
                    v = Some(value);
                }
            }
        }
        let prefix = key_prefix(order, e, a, v.as_ref());
        let limit = if request.limit == 0 {
            self.config.max_datoms
        } else {
            usize::try_from(request.limit)
                .unwrap_or(usize::MAX)
                .min(self.config.max_datoms)
        };
        let datoms: Vec<corium_core::Datom> = db
            .datoms_prefix(order, &prefix)
            .take(limit)
            .cloned()
            .collect();
        let interner = db.interner();
        let mut chunks = Vec::new();
        let groups: Vec<&[corium_core::Datom]> = if datoms.is_empty() {
            vec![&[]]
        } else {
            datoms.chunks(self.config.chunk_size.max(1)).collect()
        };
        let total = groups.len();
        for (index, group) in groups.into_iter().enumerate() {
            let bytes = codec::encode_datoms(group, interner)
                .map_err(|error| Status::internal(error.to_string()))?;
            chunks.push(pb::DatomChunk {
                datoms: bytes,
                last: index + 1 == total,
            });
        }
        Ok(Response::new(chunk_stream(chunks)))
    }

    type TxRangeStream = ChunkStream<pb::TxChunk>;

    async fn tx_range(
        &self,
        request: Request<pb::TxRangeRequest>,
    ) -> Result<Response<Self::TxRangeStream>, Status> {
        let request = request.into_inner();
        if request.db != self.connection.db_name() {
            return Err(Status::not_found(format!(
                "this peer server hosts {:?}, not {:?}",
                self.connection.db_name(),
                request.db
            )));
        }
        let end = if request.end == 0 {
            None
        } else {
            Some(request.end)
        };
        let records = self.connection.tx_range(request.start, end);
        let db = self.connection.db();
        let interner = db.interner();
        let mut txes = Vec::with_capacity(records.len());
        for record in records {
            txes.push(pb::TxReport {
                t: record.t,
                tx_instant: record.tx_instant,
                datoms: codec::encode_datoms(&record.datoms, interner)
                    .map_err(|error| Status::internal(error.to_string()))?,
            });
        }
        let groups: Vec<Vec<pb::TxReport>> = if txes.is_empty() {
            vec![Vec::new()]
        } else {
            txes.chunks(self.config.chunk_size.max(1))
                .map(<[pb::TxReport]>::to_vec)
                .collect()
        };
        let total = groups.len();
        let chunks: Vec<pb::TxChunk> = groups
            .into_iter()
            .enumerate()
            .map(|(index, group)| pb::TxChunk {
                txes: group,
                last: index + 1 == total,
            })
            .collect();
        Ok(Response::new(chunk_stream(chunks)))
    }

    async fn db_stats(
        &self,
        request: Request<pb::DbStatsRequest>,
    ) -> Result<Response<pb::DbStatsResponse>, Status> {
        let request = request.into_inner();
        let db = self.view_db(request.db.as_ref())?;
        let stats = db.stats();
        Ok(Response::new(pb::DbStatsResponse {
            basis_t: db.basis_t(),
            datom_count: stats.datoms as u64,
            entity_count: stats.entities as u64,
            attribute_count: stats.attributes as u64,
        }))
    }

    type SubscribeStream = tonic::Streaming<pb::SubscribeItem>;

    async fn subscribe(
        &self,
        request: Request<pb::SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let request = request.into_inner();
        if request.db != self.connection.db_name() {
            return Err(Status::not_found(format!(
                "this peer server hosts {:?}, not {:?}",
                self.connection.db_name(),
                request.db
            )));
        }
        let upstream = self
            .connection
            .subscribe_raw(request.from_basis_t)
            .await
            .map_err(|error| match error {
                crate::PeerError::Rpc(status) => status,
                other => Status::unavailable(other.to_string()),
            })?;
        Ok(Response::new(upstream))
    }
}

/// Serves the peer-server service until `shutdown` resolves.
///
/// # Errors
/// Returns an error when the listener cannot be bound or TLS is invalid.
pub async fn serve(
    connection: Arc<Connection>,
    addr: std::net::SocketAddr,
    authenticator: Arc<dyn Authenticator>,
    tls: Option<tonic::transport::ServerTlsConfig>,
    config: PeerServerConfig,
    shutdown: impl std::future::Future<Output = ()> + Send,
) -> Result<(), tonic::transport::Error> {
    let mut builder = tonic::transport::Server::builder();
    if let Some(tls) = tls {
        builder = builder.tls_config(tls)?;
    }
    builder
        .add_service(PeerServerServer::with_interceptor(
            PeerServerSvc::new(connection, config),
            AuthInterceptor::new(authenticator),
        ))
        .serve_with_shutdown(addr, shutdown)
        .await
}

/// Reassembles a chunked query result on the client side.
///
/// # Errors
/// Returns [`Status`]-style codec failures as strings.
pub fn assemble_query_result(chunks: &[pb::QueryResultChunk]) -> Result<Edn, String> {
    let shape = chunks
        .first()
        .map_or(pb::ResultShape::Relation, pb::QueryResultChunk::shape);
    match shape {
        pb::ResultShape::Tuple | pb::ResultShape::Scalar => {
            let bytes = &chunks
                .first()
                .ok_or_else(|| "empty result stream".to_owned())?
                .rows;
            codec::decode_edn(bytes).map_err(|error| error.to_string())
        }
        _ => {
            let mut rows = Vec::new();
            for chunk in chunks {
                match codec::decode_edn(&chunk.rows).map_err(|error| error.to_string())? {
                    Edn::Vector(items) => rows.extend(items),
                    other => return Err(format!("bad result chunk {other}")),
                }
            }
            Ok(Edn::Vector(rows))
        }
    }
}
