//! The local peer: the fluent API over the in-process [`corium_peer`]
//! library. Queries execute against immutable database values read directly
//! from storage, with no round trip to the transactor.

use std::sync::Arc;

use async_trait::async_trait;
use corium_core::{EntityId, IndexOrder};
use corium_db::{Db as DbValue, key_prefix};
use corium_peer::{ConnectConfig, Connection};
use corium_query::edn::Edn;
use corium_query::{ExecOptions, QInput, ast, boundary, exec};

use crate::result::{QueryResult, ResultShape};
use crate::{ClientError, DatomRow, Db, DbBackend, DbStats, Index, Peer, TxData, TxReport, View};

/// A fluent client backed by the in-process peer library.
pub struct LocalPeer {
    connection: Arc<Connection>,
}

impl LocalPeer {
    /// Connects a peer to a transactor and wraps it in the fluent API.
    ///
    /// # Errors
    /// Returns [`ClientError`] when no endpoint accepts the subscription.
    pub async fn connect(config: ConnectConfig) -> Result<Self, ClientError> {
        Ok(Self {
            connection: Arc::new(Connection::connect(config).await?),
        })
    }

    /// Wraps an already-established peer connection.
    #[must_use]
    pub fn from_connection(connection: Arc<Connection>) -> Self {
        Self { connection }
    }

    /// The underlying peer connection, for peer-library operations the fluent
    /// API does not surface (tx-report streaming, index policy, and so on).
    #[must_use]
    pub fn connection(&self) -> &Arc<Connection> {
        &self.connection
    }

    fn snapshot_db(&self, snapshot: DbValue) -> Db {
        Db::new(
            Arc::new(LocalDbBackend {
                snapshot,
                db_name: self.connection.db_name().to_owned(),
            }),
            View::Current,
        )
    }
}

#[async_trait]
impl Peer for LocalPeer {
    fn db_name(&self) -> &str {
        self.connection.db_name()
    }

    async fn db(&self) -> Result<Db, ClientError> {
        Ok(self.snapshot_db(self.connection.db()))
    }

    async fn transact(&self, tx: TxData) -> Result<TxReport, ClientError> {
        let result = self.connection.transact(tx.into_forms()).await?;
        Ok(TxReport {
            basis_before: result.basis_before,
            basis_t: result.basis_t,
            tx_instant: result.tx_instant,
            tempids: result.tempids,
            db_after: self.snapshot_db(result.db_after),
        })
    }

    async fn sync(&self) -> Result<Db, ClientError> {
        Ok(self.snapshot_db(self.connection.sync().await?))
    }
}

/// A database backend over a captured immutable snapshot.
struct LocalDbBackend {
    snapshot: DbValue,
    db_name: String,
}

impl LocalDbBackend {
    fn resolve(&self, view: View) -> DbValue {
        match view {
            View::Current => self.snapshot.clone(),
            View::AsOf(t) => self.snapshot.as_of(t),
            View::Since(t) => self.snapshot.since(t),
            View::History => self.snapshot.history(),
        }
    }
}

#[async_trait]
impl DbBackend for LocalDbBackend {
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
        let db = self.resolve(view);
        let parsed = ast::parse_query(&query)?;
        // Bind the receiver as the single default `$`, and each remaining
        // non-database `:in` spec to the next positional argument.
        let mut inputs: Vec<QInput<'_>> = Vec::with_capacity(parsed.inputs.len());
        let mut bound_db = false;
        let mut next_arg = args.iter();
        for spec in &parsed.inputs {
            match spec {
                ast::InSpec::Db(_) if !bound_db => {
                    inputs.push(QInput::Db(&db));
                    bound_db = true;
                }
                ast::InSpec::Db(_) => {
                    return Err(ClientError::Protocol(
                        "the local fluent API binds a single database source".into(),
                    ));
                }
                _ => {
                    let arg = next_arg.next().ok_or_else(|| {
                        ClientError::Protocol("query needs more arguments".into())
                    })?;
                    inputs.push(QInput::Edn(arg.clone()));
                }
            }
        }
        if parsed.inputs.is_empty() {
            // Default `:in [$]`.
            inputs.push(QInput::Db(&db));
        }
        let (value, _report) = corium_query::run(
            &parsed,
            &inputs,
            ExecOptions {
                fuel,
                ..ExecOptions::default()
            },
        )?;
        Ok(QueryResult::new(shape_of(&parsed.find), value))
    }

    async fn pull(&self, view: View, pattern: Edn, eid: Edn) -> Result<Edn, ClientError> {
        let db = self.resolve(view);
        let entity = resolve_eid(&db, &eid)?;
        Ok(corium_query::pull(&db, &pattern, entity)?)
    }

    async fn datoms(
        &self,
        view: View,
        index: Index,
        components: Vec<Edn>,
        limit: usize,
    ) -> Result<Vec<DatomRow>, ClientError> {
        let db = self.resolve(view);
        let order = index_order(index);
        let (e, a, v) = resolve_components(&db, order, &components)?;
        let prefix = key_prefix(order, e, a, v.as_ref());
        let limit = if limit == 0 { usize::MAX } else { limit };
        Ok(db
            .datoms_prefix(order, &prefix)
            .take(limit)
            .map(|datom| DatomRow {
                e: datom.e.raw(),
                a: datom.a.raw(),
                v: boundary::value_to_edn(&db, &datom.v),
                tx: datom.tx.raw(),
                added: datom.added,
            })
            .collect())
    }

    async fn stats(&self, view: View) -> Result<DbStats, ClientError> {
        let db = self.resolve(view);
        let stats = db.stats();
        Ok(DbStats {
            basis_t: db.basis_t(),
            datoms: stats.datoms as u64,
            entities: stats.entities as u64,
            attributes: stats.attributes as u64,
        })
    }
}

fn shape_of(find: &ast::FindSpec) -> ResultShape {
    match find {
        ast::FindSpec::Rel(_) => ResultShape::Relation,
        ast::FindSpec::Coll(_) => ResultShape::Collection,
        ast::FindSpec::Tuple(_) => ResultShape::Tuple,
        ast::FindSpec::Scalar(_) => ResultShape::Scalar,
    }
}

fn index_order(index: Index) -> IndexOrder {
    match index {
        Index::Eavt => IndexOrder::Eavt,
        Index::Aevt => IndexOrder::Aevt,
        Index::Avet => IndexOrder::Avet,
        Index::Vaet => IndexOrder::Vaet,
    }
}

/// Resolves an entity-position boundary form to an entity id, accepting raw
/// longs, `#eid` tags, idents, and lookup refs (matching the peer server).
fn resolve_eid(db: &DbValue, form: &Edn) -> Result<EntityId, ClientError> {
    match form {
        Edn::Long(n) => u64::try_from(*n)
            .map(EntityId::from_raw)
            .map_err(|_| ClientError::Decode("negative entity id".into())),
        Edn::Tagged(tag, value) if tag == "eid" => match value.as_ref() {
            Edn::Long(n) => u64::try_from(*n)
                .map(EntityId::from_raw)
                .map_err(|_| ClientError::Decode("negative entity id".into())),
            _ => Err(ClientError::Decode("#eid requires a long".into())),
        },
        Edn::Keyword(keyword) => db
            .idents()
            .entid(keyword)
            .ok_or_else(|| ClientError::Decode(format!("unknown ident {keyword}"))),
        Edn::Vector(items) => {
            let [attr_form, value_form] = items.as_slice() else {
                return Err(ClientError::Decode(
                    "lookup ref requires [attr value]".into(),
                ));
            };
            let attr = attr_form
                .as_keyword()
                .and_then(|keyword| db.idents().entid(keyword))
                .ok_or_else(|| ClientError::Decode("unknown lookup attribute".into()))?;
            let value = boundary::edn_to_value(Some(db), value_form)
                .ok_or_else(|| ClientError::Decode("bad lookup value".into()))?;
            let value = db.schema().get(attr).map_or(value.clone(), |meta| {
                exec::coerce_for_type(value, meta.value_type)
            });
            db.lookup(attr, &value)
                .ok_or_else(|| ClientError::Decode(format!("lookup ref {form} did not resolve")))
        }
        other => Err(ClientError::Decode(format!("bad entity position {other}"))),
    }
}

enum Slot {
    E,
    A,
    V,
}

/// The entity/attribute/value prefix positions of an index scan.
type PrefixParts = (
    Option<EntityId>,
    Option<EntityId>,
    Option<corium_core::Value>,
);

/// Resolves the leading index components into the entity/attribute/value
/// prefix positions for `order`.
fn resolve_components(
    db: &DbValue,
    order: IndexOrder,
    components: &[Edn],
) -> Result<PrefixParts, ClientError> {
    if components.len() > 3 {
        return Err(ClientError::Decode("at most three components".into()));
    }
    let positions = match order {
        IndexOrder::Eavt => [Slot::E, Slot::A, Slot::V],
        IndexOrder::Aevt => [Slot::A, Slot::E, Slot::V],
        IndexOrder::Avet => [Slot::A, Slot::V, Slot::E],
        IndexOrder::Vaet => [Slot::V, Slot::A, Slot::E],
    };
    let mut e = None;
    let mut a = None;
    let mut v = None;
    for (slot, form) in positions.iter().zip(components) {
        match slot {
            Slot::E => e = Some(resolve_eid(db, form)?),
            Slot::A => {
                a = Some(match form {
                    Edn::Keyword(keyword) => db.idents().entid(keyword).ok_or_else(|| {
                        ClientError::Decode(format!("unknown attribute {keyword}"))
                    })?,
                    other => resolve_eid(db, other)?,
                });
            }
            Slot::V => {
                let value = boundary::edn_to_value(Some(db), form)
                    .ok_or_else(|| ClientError::Decode(format!("bad value {form}")))?;
                let value = a
                    .and_then(|a| db.schema().get(a))
                    .map_or(value.clone(), |meta| {
                        exec::coerce_for_type(value, meta.value_type)
                    });
                v = Some(value);
            }
        }
    }
    Ok((e, a, v))
}
