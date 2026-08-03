//! Peer server: hosts a peer for thin clients over the public
//! `PeerServerService` gRPC surface (see `docs/design/protocol.md`).
//!
//! Queries execute server-side against the hosted peer's local database
//! values with per-request fuel and chunked result streams.
//!
//! # One database value, one key set, many views
//!
//! Every read here is answered under the caller's own
//! [`ReadContext`]: the attributes the authorization policy lets that
//! principal see, and the protection classes it lets that principal hydrate.
//! Two principals querying the same hosted `Db` concurrently get different
//! answers from the same immutable value, which is the whole point of
//! deciding identity per request rather than per connection.
//!
//! The key half is the [`KeyPolicy`](docs) of `docs/design/encryption.md`:
//! policy names key ids, and this server hands each principal the subset of
//! its own keyring those ids resolve to. **Holding a class key is no longer
//! the same as disclosing it.** Which principals get keys by default is
//! [`KeyPolicyMode`]:
//!
//! * `Strict` — a principal hydrates only the classes its decision names.
//!   The default whenever a real [`Guard`] is configured. Note that
//!   `:authz.binding/unfiltered` grants full *attribute* visibility and no
//!   keys: keys are named by key id, and that binding names none.
//! * `ServerWide` — every request hydrates with the server's whole keyring.
//!   The default when authorization is disabled, which is the single-tenant
//!   and embedded case.
//!
//! What is still missing is seal-through mode (`--seal-through`), where the
//! server forwards sealed values and the thin client hydrates with its own
//! keyring. Until that lands, a peer server under `Strict` is enforcing key
//! policy in the process that holds the plaintext — it is authorization, not
//! cryptography. The cryptographic floor is still a peer server that was
//! never given the key: it answers every query that does not touch a
//! protected value identically, and protected values come back redacted,
//! hidden, or refused per class policy.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use corium_core::{EntityId, IndexOrder};
use corium_db::protect::Hydrator;
use corium_db::read::{AttrVisibility, ReadContext};
use corium_db::{Db, key_prefix};
use corium_protocol::authz::{
    self, Access, Action, Guard, IdentityInterceptor, KeyGrant, KeyPolicyMode, Principal, ReadGrant,
};
use corium_protocol::codec;
use corium_protocol::pb;
use corium_protocol::pb::peer_server_server::{PeerServer, PeerServerServer};
use corium_query::edn::Edn;
use corium_query::{ExecOptions, QInput, QueryCache, ast, boundary, exec};
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use crate::Connection;
use crate::metrics::Metrics;

/// Peer-server execution limits and key policy.
#[derive(Clone, Copy, Debug)]
pub struct PeerServerConfig {
    /// Fuel ceiling per query (datoms touched).
    pub max_fuel: u64,
    /// Rows or datoms per streamed chunk.
    pub chunk_size: usize,
    /// Maximum datoms returned by one `Datoms` request.
    pub max_datoms: usize,
    /// How a principal's key set is chosen, or `None` to derive it from the
    /// guard: [`KeyPolicyMode::Strict`] when authorization is configured, and
    /// [`KeyPolicyMode::ServerWide`] when it is not.
    ///
    /// Deriving it is what keeps a single-tenant deployment working exactly
    /// as before while making the multi-tenant one safe by default: a server
    /// that was told who its callers are should not hand all of them every
    /// key it holds.
    pub key_policy: Option<KeyPolicyMode>,
}

impl Default for PeerServerConfig {
    fn default() -> Self {
        Self {
            max_fuel: 10_000_000,
            chunk_size: 1024,
            max_datoms: 1_000_000,
            key_policy: None,
        }
    }
}

/// The hosted-peer gRPC service.
pub struct PeerServerSvc {
    connection: Arc<Connection>,
    cache: QueryCache,
    config: PeerServerConfig,
    metrics: Arc<Metrics>,
    guard: Guard,
    /// Granted key ids → the hydrator that opens exactly those classes.
    ///
    /// A hydrator carries a bounded plaintext cache, so building one per
    /// request would throw that cache away on every call. Principals share a
    /// hydrator whenever policy grants them the same key ids, which is the
    /// common case: key sets are per role, not per user.
    hydrators: std::sync::Mutex<std::collections::HashMap<KeySetKey, Arc<Hydrator>>>,
}

/// Cache key for a per-principal hydrator: `None` is the server's whole
/// keyring, `Some(ids)` the classes those key ids resolve to.
type KeySetKey = Option<std::collections::BTreeSet<String>>;

impl PeerServerSvc {
    /// Hosts `connection` with the given limits and no auth
    /// ([`Guard::disabled`]); add a policy with [`PeerServerSvc::with_guard`].
    #[must_use]
    pub fn new(connection: Arc<Connection>, config: PeerServerConfig) -> Self {
        let metrics = Arc::new(Metrics::new(connection.segment_cache_metrics()));
        Self {
            connection,
            cache: QueryCache::new(),
            config,
            metrics,
            guard: Guard::disabled(),
            hydrators: std::sync::Mutex::new(std::collections::HashMap::new()),
        }
    }

    /// Enforces `guard` on every request (builder style).
    #[must_use]
    pub fn with_guard(mut self, guard: Guard) -> Self {
        self.guard = guard;
        self
    }

    /// Process query metrics suitable for a Prometheus endpoint.
    #[must_use]
    pub fn metrics(&self) -> Arc<Metrics> {
        Arc::clone(&self.metrics)
    }

    /// Authorizes `access` for the request's [`Principal`], mapping the
    /// guard's decision onto a gRPC status.
    ///
    /// The returned [`ReadGrant`] is what the caller may see; every read path
    /// turns it into a [`ReadContext`] before touching the database. A
    /// surface that cannot apply a restriction must refuse it rather than
    /// answer in full.
    async fn authorize<T>(
        &self,
        request: &Request<T>,
        access: Access,
    ) -> Result<ReadGrant, Status> {
        let principal: Principal = authz::principal(request);
        match self.guard.authorize(&principal, &access).await {
            Ok(None) => Ok(ReadGrant::unrestricted()),
            Ok(Some(grant)) => Ok(grant),
            Err(error) => Err(error.to_status()),
        }
    }

    /// The database this peer server hosts, for building an [`Access`].
    fn db_target(&self) -> String {
        self.connection.db_name().to_owned()
    }

    /// How this server chooses a principal's key set.
    fn key_mode(&self) -> KeyPolicyMode {
        self.config.key_policy.unwrap_or({
            if self.guard.is_disabled() {
                KeyPolicyMode::ServerWide
            } else {
                KeyPolicyMode::Strict
            }
        })
    }

    /// The key ids `grant` entitles this request to, or `None` for the
    /// server's whole keyring.
    fn granted_keys(&self, grant: &ReadGrant) -> KeySetKey {
        match (&grant.keys, self.key_mode()) {
            // The policy named key ids: honour exactly those, under either
            // mode. Naming keys is always a restriction.
            (KeyGrant::Only(ids), _) => Some(ids.clone()),
            (KeyGrant::Unrestricted, KeyPolicyMode::ServerWide) => None,
            // Strict: a decision that names no key grants none.
            (KeyGrant::Unrestricted, KeyPolicyMode::Strict) => {
                Some(std::collections::BTreeSet::new())
            }
        }
    }

    /// The hydrator for one principal's key set, built once and shared.
    async fn hydrator_for(&self, grant: &ReadGrant) -> Result<Arc<Hydrator>, Status> {
        let wanted = self.granted_keys(grant);
        if let Some(hit) = self
            .hydrators
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&wanted)
        {
            return Ok(Arc::clone(hit));
        }
        let full = self
            .connection
            .hydrator()
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
        let hydrator = match &wanted {
            None => full,
            Some(allowed) => Arc::new(Hydrator::new(
                full.keys()
                    .restrict_to(self.connection.db().schema(), allowed),
            )),
        };
        let mut cache = self
            .hydrators
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(Arc::clone(
            cache.entry(wanted).or_insert_with(|| Arc::clone(&hydrator)),
        ))
    }

    /// Turns an authorization decision into the read `db` is answered under.
    ///
    /// The attribute filter is resolved against this database value's own
    /// schema once per request, so the scan compares attribute ids rather
    /// than re-deriving idents per datom.
    async fn read_context(&self, db: &Db, grant: &ReadGrant) -> Result<ReadContext, Status> {
        let mut context = ReadContext::open().with_hydrator(self.hydrator_for(grant).await?);
        if let Some(view) = &grant.view {
            context = context.with_visibility(Arc::new(AttrVisibility::resolve(
                db.schema(),
                db.idents(),
                |ident| view.attribute_visible(&ident.to_string()),
            )));
        }
        Ok(context)
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
            Some(pb::db_view_spec::View::AsOfInstant(instant)) => base.as_of_instant(*instant),
            Some(pb::db_view_spec::View::SinceInstant(instant)) => base.since_instant(*instant),
            None | Some(pb::db_view_spec::View::History(false)) => base,
        })
    }
}

fn decode_edn(bytes: &[u8], what: &str) -> Result<Edn, Status> {
    codec::decode_edn(bytes)
        .map_err(|error| Status::invalid_argument(format!("bad {what}: {error}")))
}

/// Resolves an entity-position form for one reader.
///
/// A lookup ref over an attribute the reader cannot see does not resolve:
/// whether it does is an existence oracle over exactly the attribute the view
/// withholds.
fn resolve_eid(db: &Db, read: &ReadContext, form: &Edn) -> Result<EntityId, Status> {
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
            if !read.visible(attr) {
                return Err(Status::permission_denied(format!(
                    "lookup ref {form} names an attribute this principal cannot see"
                )));
            }
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

/// Splits a query result into wire chunks. Scalar and tuple shapes are one
/// value, so they are always a single chunk.
fn result_chunks(
    shape: pb::ResultShape,
    rows: Vec<Edn>,
    chunk_size: usize,
) -> Vec<pb::QueryResultChunk> {
    if matches!(shape, pb::ResultShape::Tuple | pb::ResultShape::Scalar) {
        return vec![pb::QueryResultChunk {
            shape: shape.into(),
            rows: codec::encode_edn(&rows.into_iter().next().unwrap_or(Edn::Nil)),
            last: true,
        }];
    }
    let groups: Vec<&[Edn]> = if rows.is_empty() {
        vec![&[]]
    } else {
        rows.chunks(chunk_size.max(1)).collect()
    };
    let total = groups.len();
    groups
        .into_iter()
        .enumerate()
        .map(|(index, group)| pb::QueryResultChunk {
            shape: shape.into(),
            rows: codec::encode_edn(&Edn::Vector(group.to_vec())),
            last: index + 1 == total,
        })
        .collect()
}

#[tonic::async_trait]
impl PeerServer for PeerServerSvc {
    type QueryStream = ChunkStream<pb::QueryResultChunk>;

    async fn query(
        &self,
        request: Request<pb::QueryRequest>,
    ) -> Result<Response<Self::QueryStream>, Status> {
        let grant = self
            .authorize(&request, Access::on(Action::Query, self.db_target()))
            .await?;
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
        // Every database view the query binds is the one hosted database, so
        // one read context covers them all.
        let read = self
            .read_context(dbs.first().unwrap_or(&self.connection.db()), &grant)
            .await?;
        let started = Instant::now();
        let (result, report) = corium_query::run(
            &parsed,
            &inputs,
            ExecOptions {
                fuel: Some(fuel),
                read,
                ..ExecOptions::default()
            },
        )
        .map_err(|error| Status::invalid_argument(error.to_string()))?;
        self.metrics
            .record_query(started.elapsed(), report.datoms_scanned);
        tracing::debug!(
            db = %self.connection.db_name(),
            datoms_scanned = report.datoms_scanned,
            elapsed_micros = started.elapsed().as_micros(),
            "query completed"
        );
        let (shape, rows) = match (&parsed.find, result) {
            (ast::FindSpec::Rel(_), Edn::Vector(rows)) => (pb::ResultShape::Relation, rows),
            (ast::FindSpec::Coll(_), Edn::Vector(rows)) => (pb::ResultShape::Collection, rows),
            (ast::FindSpec::Tuple(_), value) => (pb::ResultShape::Tuple, vec![value]),
            (ast::FindSpec::Scalar(_), value) => (pb::ResultShape::Scalar, vec![value]),
            (_, value) => (pb::ResultShape::Relation, vec![value]),
        };
        Ok(Response::new(chunk_stream(result_chunks(
            shape,
            rows,
            self.config.chunk_size,
        ))))
    }

    async fn pull(
        &self,
        request: Request<pb::PullRequest>,
    ) -> Result<Response<pb::PullResponse>, Status> {
        let grant = self
            .authorize(&request, Access::on(Action::Pull, self.db_target()))
            .await?;
        let request = request.into_inner();
        let db = self.view_db(request.db.as_ref())?;
        let pattern = decode_edn(&request.pattern, "pull pattern")?;
        let read = self.read_context(&db, &grant).await?;
        let eid = resolve_eid(&db, &read, &decode_edn(&request.eid, "entity")?)?;
        let result = corium_query::pull_with(&db, &pattern, eid, &read)
            .map_err(|error| Status::invalid_argument(error.to_string()))?;
        Ok(Response::new(pb::PullResponse {
            result: codec::encode_edn(&result),
        }))
    }

    async fn transact(
        &self,
        request: Request<pb::TransactRequest>,
    ) -> Result<Response<pb::TransactResponse>, Status> {
        let grant = self
            .authorize(&request, Access::on(Action::Transact, self.db_target()))
            .await?;
        // Transaction data arrives already encoded and is forwarded to the
        // transactor as bytes, so this surface cannot tell whether a write
        // touches an attribute the principal may not read — and a write it
        // cannot read back is exactly how a view gets probed (`:db/cas` on a
        // hidden attribute answers a question the read path refuses). A
        // principal whose view hides attributes is refused rather than
        // trusted.
        //
        // A key restriction is not a reason to refuse: it hides no attribute,
        // so a writer that may not hydrate a class can still be told honestly
        // what it did and did not change.
        if grant.view.is_some() {
            return Err(Status::permission_denied(
                "this principal reads through a restricted view and may not transact",
            ));
        }
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
            .transact_raw_at(request.tx_data, request.expected_basis_t)
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
        let grant = self
            .authorize(&request, Access::on(Action::Datoms, self.db_target()))
            .await?;
        let request = request.into_inner();
        let db = self.view_db(request.db.as_ref())?;
        let read = self.read_context(&db, &grant).await?;
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
                Slot::E => e = Some(resolve_eid(&db, &read, form)?),
                Slot::A => {
                    let attr = match form {
                        Edn::Keyword(keyword) => db.idents().entid(keyword).ok_or_else(|| {
                            Status::invalid_argument(format!("unknown attribute {keyword}"))
                        })?,
                        other => resolve_eid(&db, &read, other)?,
                    };
                    // Binding a hidden attribute as a prefix component would
                    // turn an empty result into a statement about the data.
                    if !read.visible(attr) {
                        return Err(Status::permission_denied(
                            "this principal cannot see that attribute",
                        ));
                    }
                    a = Some(attr);
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
        // The raw datom stream discloses exactly as much as `query` and
        // `pull` do, and no more: same view, same keys.
        let mut datoms: Vec<corium_core::Datom> = Vec::new();
        for datom in db.datoms_prefix(order, &prefix) {
            if datoms.len() >= limit {
                break;
            }
            match read.read(db.schema(), db.interner(), datom.a, datom.e, &datom.v) {
                Ok(corium_db::protect::Hydration::Bind(v)) => {
                    datoms.push(corium_core::Datom { v, ..datom.clone() });
                }
                Ok(corium_db::protect::Hydration::Hide) => {}
                Ok(corium_db::protect::Hydration::Refuse(class)) => {
                    return Err(Status::permission_denied(format!(
                        "protection class {class} is not readable without its key"
                    )));
                }
                Err(error) => return Err(Status::internal(error.to_string())),
            }
        }
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
        let grant = self
            .authorize(&request, Access::on(Action::TxRange, self.db_target()))
            .await?;
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
        let read = self.read_context(&db, &grant).await?;
        let interner = db.interner();
        let mut txes = Vec::with_capacity(records.len());
        for record in records {
            // History is read under the same view and keys as the present.
            // A transaction log that reported facts the current-value read
            // withholds would be the simplest possible way around the view.
            let mut datoms = Vec::with_capacity(record.datoms.len());
            for datom in &record.datoms {
                match read.read(db.schema(), db.interner(), datom.a, datom.e, &datom.v) {
                    Ok(corium_db::protect::Hydration::Bind(v)) => {
                        datoms.push(corium_core::Datom { v, ..datom.clone() });
                    }
                    Ok(corium_db::protect::Hydration::Hide) => {}
                    Ok(corium_db::protect::Hydration::Refuse(class)) => {
                        return Err(Status::permission_denied(format!(
                            "protection class {class} is not readable without its key"
                        )));
                    }
                    Err(error) => return Err(Status::internal(error.to_string())),
                }
            }
            txes.push(pb::TxReport {
                t: record.t,
                tx_instant: record.tx_instant,
                datoms: codec::encode_datoms(&datoms, interner)
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
        // Stats are counts over the whole database value, not facts, and the
        // guard has already decided whether this principal may inspect it.
        let _grant = self
            .authorize(&request, Access::on(Action::Inspect, self.db_target()))
            .await?;
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
        let grant = self
            .authorize(&request, Access::on(Action::Subscribe, self.db_target()))
            .await?;
        // This stream is the transactor's own, proxied through untouched, so
        // there is nowhere to apply a view to it. A principal whose view
        // hides attributes is refused rather than handed the unfiltered log;
        // the filtered re-streaming that would lift this is not implemented.
        //
        // A key restriction needs no refusal here: the transactor holds no
        // class key, so every protected value on this stream is already
        // sealed and stays that way.
        if grant.view.is_some() {
            return Err(Status::permission_denied(
                "this principal reads through a restricted view; \
                 Subscribe cannot filter its stream and is refused",
            ));
        }
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
    guard: Guard,
    tls: Option<tonic::transport::ServerTlsConfig>,
    config: PeerServerConfig,
    shutdown: impl std::future::Future<Output = ()> + Send,
) -> Result<(), tonic::transport::Error> {
    serve_service(
        PeerServerSvc::new(connection, config).with_guard(guard),
        addr,
        tls,
        shutdown,
    )
    .await
}

/// Serves a pre-built peer service, allowing process wiring to expose its
/// metrics handle before the server future is awaited.
///
/// # Errors
/// Returns an error when the listener cannot be bound or TLS is invalid.
pub async fn serve_service(
    service: PeerServerSvc,
    addr: std::net::SocketAddr,
    tls: Option<tonic::transport::ServerTlsConfig>,
    shutdown: impl std::future::Future<Output = ()> + Send,
) -> Result<(), tonic::transport::Error> {
    // The interceptor authenticates each request with the same guard the
    // service uses to authorize it.
    let interceptor = IdentityInterceptor::new(service.guard.clone());
    let mut builder = tonic::transport::Server::builder();
    if let Some(tls) = tls {
        builder = builder.tls_config(tls)?;
    }
    builder
        .add_service(PeerServerServer::with_interceptor(service, interceptor))
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
