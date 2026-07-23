//! The remote peer: the fluent API over a peer server, reached over gRPC.
//!
//! Presents the same surface as [`crate::LocalPeer`], but every read and
//! write is an RPC to a hosted peer. Views are named in the request and
//! resolved server-side; results stream back in chunks and are reassembled
//! here.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use corium_core::{EntityId, KeywordInterner, TotalF64, Value};
use corium_peer::server::assemble_query_result;
use corium_protocol::auth::TokenInterceptor;
use corium_protocol::codec;
use corium_protocol::pb;
use corium_protocol::pb::peer_server_client::PeerServerClient;
use corium_query::edn::Edn;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

use crate::result::{QueryResult, ResultShape};
use crate::{ClientError, DatomRow, Db, DbBackend, DbStats, Index, Peer, TxData, TxReport, View};

type ServerClient = PeerServerClient<InterceptedService<Channel, TokenInterceptor>>;

/// A fluent client backed by a remote peer server over gRPC.
pub struct RemotePeer {
    backend: Arc<RemoteDbBackend>,
}

impl RemotePeer {
    /// Connects to a peer server hosting `db`.
    ///
    /// # Errors
    /// Returns [`ClientError`] when the endpoint is unreachable.
    pub async fn connect(
        endpoint: impl Into<String>,
        db: impl Into<String>,
        token: Option<String>,
        tls: Option<ClientTlsConfig>,
    ) -> Result<Self, ClientError> {
        let endpoint = endpoint.into();
        let mut builder = Endpoint::from_shared(endpoint)
            .map_err(|error| ClientError::Protocol(format!("bad endpoint: {error}")))?
            .connect_timeout(Duration::from_secs(10));
        if let Some(tls) = tls {
            builder = builder.tls_config(tls)?;
        }
        let channel = builder.connect().await?;
        let client = PeerServerClient::with_interceptor(channel, TokenInterceptor::new(token));
        Ok(Self {
            backend: Arc::new(RemoteDbBackend {
                client,
                db_name: db.into(),
            }),
        })
    }

    fn db_at(&self, view: View) -> Db {
        Db::new(self.backend.clone(), view)
    }
}

#[async_trait]
impl Peer for RemotePeer {
    fn db_name(&self) -> &str {
        &self.backend.db_name
    }

    async fn db(&self) -> Result<Db, ClientError> {
        Ok(self.db_at(View::Current))
    }

    async fn transact(&self, tx: TxData) -> Result<TxReport, ClientError> {
        let tx_data = codec::encode_edn(&Edn::Vector(tx.into_forms()));
        let mut client = self.backend.client.clone();
        let response = client
            .transact(pb::TransactRequest {
                db: self.backend.db_name.clone(),
                protocol_version: corium_protocol::PROTOCOL_VERSION,
                tx_data,
            })
            .await?
            .into_inner();
        Ok(TxReport {
            basis_before: response.basis_before,
            basis_t: response.basis_t,
            tx_instant: response.tx_instant,
            tempids: decode_tempids(&response.tempids)?,
            // The server syncs its hosted peer to this basis before replying,
            // so an as-of view at `basis_t` is a stable post-commit snapshot.
            db_after: self.db_at(View::AsOf(response.basis_t)),
        })
    }

    async fn sync(&self) -> Result<Db, ClientError> {
        // A peer server keeps its hosted peer synced; the current view already
        // reflects the latest applied basis.
        Ok(self.db_at(View::Current))
    }
}

/// A database backend that issues peer-server RPCs.
struct RemoteDbBackend {
    client: ServerClient,
    db_name: String,
}

impl RemoteDbBackend {
    fn view_spec(&self, view: View) -> pb::DbViewSpec {
        let view = match view {
            View::Current => None,
            View::AsOf(t) => Some(pb::db_view_spec::View::AsOf(t)),
            View::Since(t) => Some(pb::db_view_spec::View::Since(t)),
            View::History => Some(pb::db_view_spec::View::History(true)),
        };
        pb::DbViewSpec {
            db: self.db_name.clone(),
            view,
        }
    }
}

#[async_trait]
impl DbBackend for RemoteDbBackend {
    fn db_name(&self) -> &str {
        &self.db_name
    }

    async fn query(
        &self,
        view: View,
        query: Edn,
        args: Vec<Edn>,
        fuel: Option<u64>,
    ) -> Result<QueryResult, ClientError> {
        let mut client = self.client.clone();
        let mut stream = client
            .query(pb::QueryRequest {
                dbs: vec![self.view_spec(view)],
                query: codec::encode_edn(&query),
                args: codec::encode_edn(&Edn::Vector(args)),
                fuel: fuel.unwrap_or(0),
            })
            .await?
            .into_inner();
        let mut chunks = Vec::new();
        while let Some(chunk) = stream.message().await? {
            chunks.push(chunk);
        }
        let shape = chunks
            .first()
            .map_or(ResultShape::Relation, |chunk| shape_of(chunk.shape()));
        let value = assemble_query_result(&chunks).map_err(ClientError::Decode)?;
        Ok(QueryResult::new(shape, value))
    }

    async fn pull(&self, view: View, pattern: Edn, eid: Edn) -> Result<Edn, ClientError> {
        let mut client = self.client.clone();
        let response = client
            .pull(pb::PullRequest {
                db: Some(self.view_spec(view)),
                pattern: codec::encode_edn(&pattern),
                eid: codec::encode_edn(&eid),
            })
            .await?
            .into_inner();
        Ok(codec::decode_edn(&response.result)?)
    }

    async fn datoms(
        &self,
        view: View,
        index: Index,
        components: Vec<Edn>,
        limit: usize,
    ) -> Result<Vec<DatomRow>, ClientError> {
        let mut client = self.client.clone();
        let mut stream = client
            .datoms(pb::DatomsRequest {
                db: Some(self.view_spec(view)),
                index: index.as_str().to_owned(),
                components: codec::encode_edn(&Edn::Vector(components)),
                limit: u64::try_from(limit).unwrap_or(0),
            })
            .await?
            .into_inner();
        let mut interner = KeywordInterner::default();
        let mut rows = Vec::new();
        while let Some(chunk) = stream.message().await? {
            for datom in codec::decode_datoms(&chunk.datoms, &mut interner)? {
                rows.push(DatomRow {
                    e: datom.e.raw(),
                    a: datom.a.raw(),
                    v: value_to_edn(&interner, &datom.v),
                    tx: datom.tx.raw(),
                    added: datom.added,
                });
            }
        }
        Ok(rows)
    }

    async fn stats(&self, view: View) -> Result<DbStats, ClientError> {
        let mut client = self.client.clone();
        let response = client
            .db_stats(pb::DbStatsRequest {
                db: Some(self.view_spec(view)),
            })
            .await?
            .into_inner();
        Ok(DbStats {
            basis_t: response.basis_t,
            datoms: response.datom_count,
            entities: response.entity_count,
            attributes: response.attribute_count,
        })
    }
}

fn shape_of(shape: pb::ResultShape) -> ResultShape {
    match shape {
        pb::ResultShape::Collection => ResultShape::Collection,
        pb::ResultShape::Tuple => ResultShape::Tuple,
        pb::ResultShape::Scalar => ResultShape::Scalar,
        pb::ResultShape::Relation | pb::ResultShape::Unspecified => ResultShape::Relation,
    }
}

/// Renders a decoded value to boundary EDN using a client-side interner for
/// keyword names. Refs surface as longs, matching the query boundary.
fn value_to_edn(interner: &KeywordInterner, value: &Value) -> Edn {
    match value {
        Value::Bool(v) => Edn::Bool(*v),
        Value::Long(v) => Edn::Long(*v),
        Value::Double(TotalF64(v)) => Edn::Double(TotalF64(*v)),
        Value::Str(v) => Edn::Str(v.to_string()),
        Value::Instant(ms) => Edn::Tagged("inst".into(), Box::new(Edn::Long(*ms))),
        Value::Uuid(v) => Edn::Tagged("uuid".into(), Box::new(Edn::Str(format!("{v:032x}")))),
        Value::Bytes(bytes) => Edn::Tagged(
            "bytes".into(),
            Box::new(Edn::Str(bytes.iter().fold(String::new(), |mut acc, b| {
                use std::fmt::Write as _;
                let _ = write!(acc, "{b:02x}");
                acc
            }))),
        ),
        Value::Keyword(id) => interner
            .resolve(*id)
            .map_or(Edn::Nil, |keyword| Edn::Keyword(keyword.clone())),
        Value::Ref(e) => Edn::Long(i64::try_from(e.raw()).unwrap_or(i64::MAX)),
    }
}

/// Decodes a tempid map (string name -> allocated entity id long).
fn decode_tempids(bytes: &[u8]) -> Result<BTreeMap<String, EntityId>, ClientError> {
    let Edn::Map(pairs) = codec::decode_edn(bytes)? else {
        return Err(ClientError::Protocol("tempids must be a map".into()));
    };
    let mut tempids = BTreeMap::new();
    for (key, value) in pairs {
        let (Edn::Str(name), Edn::Long(raw)) = (key, value) else {
            return Err(ClientError::Protocol("bad tempid entry".into()));
        };
        let raw = u64::try_from(raw).map_err(|_| ClientError::Protocol("bad entity id".into()))?;
        tempids.insert(name, EntityId::from_raw(raw));
    }
    Ok(tempids)
}
