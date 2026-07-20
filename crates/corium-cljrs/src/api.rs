//! The `corium.api` namespace: Datomic-shaped peer API surface for
//! Clojurust programs, bound to `corium-peer` so queries execute locally
//! (see `docs/design/clojurust-integration.md`).
//!
//! Connections, database values, and tx-report queues surface as opaque
//! native objects; all data crossing the boundary converts once through
//! [`crate::convert`].

use std::any::Any;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use cljrs_env::env::GlobalEnv;
use cljrs_gc::{GcPtr, MarkVisitor, Trace};
use cljrs_value::native_object::{NativeObject, NativeObjectBox};
use cljrs_value::{Arity, NativeFn, Value, ValueError, ValueResult};
use corium_core::{EntityId, IndexOrder};
use corium_db::Db;
use corium_peer::{ConnectConfig, Connection, PeerReport};
use corium_query::boundary::{edn_to_value, value_to_edn};
use corium_query::edn::Edn;
use corium_query::{QInput, pull};
use tokio::runtime::Handle;
use tokio::sync::broadcast;

use crate::convert::{from_edn, to_edn};

fn verr(text: impl Into<String>) -> ValueError {
    ValueError::Other(text.into())
}

// ── Native object handles ────────────────────────────────────────────────────

/// An immutable database value held by the cljrs runtime.
pub struct DbHandle {
    /// The wrapped database value.
    pub db: Db,
}

impl std::fmt::Debug for DbHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "#corium/db[basis {}]", self.db.basis_t())
    }
}

impl Trace for DbHandle {
    fn trace(&self, _: &mut MarkVisitor) {}
}

impl NativeObject for DbHandle {
    #[allow(clippy::unnecessary_literal_bound)]
    fn type_tag(&self) -> &str {
        "CoriumDb"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// A live peer connection held by the cljrs runtime.
pub struct ConnHandle {
    conn: Arc<Connection>,
    handle: Handle,
}

impl std::fmt::Debug for ConnHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "#corium/conn[{}]", self.conn.db_name())
    }
}

impl Trace for ConnHandle {
    fn trace(&self, _: &mut MarkVisitor) {}
}

impl NativeObject for ConnHandle {
    #[allow(clippy::unnecessary_literal_bound)]
    fn type_tag(&self) -> &str {
        "CoriumConnection"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// A tx-report queue held by the cljrs runtime.
pub struct QueueHandle {
    rx: Mutex<broadcast::Receiver<PeerReport>>,
    handle: Handle,
}

impl std::fmt::Debug for QueueHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "#corium/tx-report-queue")
    }
}

impl Trace for QueueHandle {
    fn trace(&self, _: &mut MarkVisitor) {}
}

impl NativeObject for QueueHandle {
    #[allow(clippy::unnecessary_literal_bound)]
    fn type_tag(&self) -> &str {
        "CoriumTxReportQueue"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Wraps a database value for the current cljrs isolate.
#[must_use]
pub fn db_value(db: Db) -> Value {
    Value::NativeObject(GcPtr::new(NativeObjectBox::new(DbHandle { db })))
}

fn db_of(value: &Value) -> ValueResult<Db> {
    if let Value::NativeObject(obj) = value
        && let Some(handle) = obj.get().downcast_ref::<DbHandle>()
    {
        return Ok(handle.db.clone());
    }
    Err(verr("expected a database value"))
}

fn conn_of(value: &Value) -> ValueResult<(Arc<Connection>, Handle)> {
    if let Value::NativeObject(obj) = value
        && let Some(handle) = obj.get().downcast_ref::<ConnHandle>()
    {
        return Ok((Arc::clone(&handle.conn), handle.handle.clone()));
    }
    Err(verr("expected a connection"))
}

fn long_of(value: &Value, what: &str) -> ValueResult<i64> {
    match value {
        Value::Long(n) => Ok(*n),
        _ => Err(verr(format!("{what} must be a long"))),
    }
}

fn t_of(value: &Value) -> ValueResult<u64> {
    u64::try_from(long_of(value, "t")?).map_err(|_| verr("t must be non-negative"))
}

/// Resolves an entity position: long, ident keyword, `#eid`, or lookup ref.
fn eid_of(db: &Db, form: &Edn) -> ValueResult<EntityId> {
    match form {
        Edn::Long(n) => u64::try_from(*n)
            .map(EntityId::from_raw)
            .map_err(|_| verr("entity id must be non-negative")),
        Edn::Keyword(keyword) => db
            .idents()
            .entid(keyword)
            .ok_or_else(|| verr(format!("unknown ident {keyword}"))),
        Edn::Tagged(tag, inner) if tag == "eid" => match inner.as_ref() {
            Edn::Long(n) => u64::try_from(*n)
                .map(EntityId::from_raw)
                .map_err(|_| verr("entity id must be non-negative")),
            _ => Err(verr("#eid requires a long")),
        },
        Edn::Vector(items) => {
            let [attr_form, value_form] = items.as_slice() else {
                return Err(verr("lookup ref must be [attr value]"));
            };
            let attr = attr_form
                .as_keyword()
                .and_then(|keyword| db.idents().entid(keyword))
                .ok_or_else(|| verr(format!("unknown lookup attribute {attr_form}")))?;
            let value_type = db
                .schema()
                .get(attr)
                .map(|meta| meta.value_type)
                .ok_or_else(|| verr("lookup attribute is not installed"))?;
            let value = edn_to_value(Some(db), value_form)
                .map(|value| corium_query::exec::coerce_for_type(value, value_type))
                .ok_or_else(|| verr(format!("bad lookup value {value_form}")))?;
            db.lookup(attr, &value)
                .ok_or_else(|| verr(format!("lookup ref [{attr_form} {value_form}] not found")))
        }
        other => Err(verr(format!("bad entity position {other}"))),
    }
}

fn datom_edn(db: &Db, datom: &corium_core::Datom) -> Edn {
    let attr = db.idents().ident(datom.a).map_or_else(
        || Edn::Long(i64::try_from(datom.a.raw()).unwrap_or(i64::MAX)),
        |keyword| Edn::Keyword(keyword.clone()),
    );
    Edn::Vector(vec![
        Edn::Long(i64::try_from(datom.e.raw()).unwrap_or(i64::MAX)),
        attr,
        value_to_edn(db, &datom.v),
        Edn::Long(i64::try_from(datom.tx.raw()).unwrap_or(i64::MAX)),
        Edn::Bool(datom.added),
    ])
}

fn define(globals: &Arc<GlobalEnv>, name: &str, function: NativeFn) {
    globals.intern(
        "corium.api",
        name.into(),
        Value::NativeFunction(GcPtr::new(function)),
    );
}

// ── Read-only API (also available inside the db-function sandbox) ────────────

/// A query input owned across the boundary conversion.
enum Input {
    Db(Db),
    Arg(Edn),
}

/// Registers the read-only `corium.api` operations (`q`, `pull`, `entity`,
/// `datoms`, time views) into `globals`. This subset is what the sandboxed
/// database-function environment sees.
#[allow(clippy::too_many_lines)]
pub fn register_read_api(globals: &Arc<GlobalEnv>) {
    define(
        globals,
        "q",
        NativeFn::with_closure("q", Arity::Variadic { min: 2 }, |args| {
            let query = to_edn(&args[0]).map_err(|error| verr(error.to_string()))?;
            let inputs = args[1..]
                .iter()
                .map(|arg| match db_of(arg) {
                    Ok(db) => Ok(Input::Db(db)),
                    Err(_) => to_edn(arg)
                        .map(Input::Arg)
                        .map_err(|error| verr(error.to_string())),
                })
                .collect::<ValueResult<Vec<_>>>()?;
            let qinputs: Vec<QInput<'_>> = inputs
                .iter()
                .map(|input| match input {
                    Input::Db(db) => QInput::Db(db),
                    Input::Arg(edn) => QInput::Edn(edn.clone()),
                })
                .collect();
            let result =
                corium_query::q(&query, &qinputs).map_err(|error| verr(error.to_string()))?;
            Ok(from_edn(&result))
        }),
    );
    define(
        globals,
        "pull",
        NativeFn::with_closure("pull", Arity::Fixed(3), |args| {
            let db = db_of(&args[0])?;
            let pattern = to_edn(&args[1]).map_err(|error| verr(error.to_string()))?;
            let eid_form = to_edn(&args[2]).map_err(|error| verr(error.to_string()))?;
            let eid = eid_of(&db, &eid_form)?;
            let result = pull(&db, &pattern, eid).map_err(|error| verr(error.to_string()))?;
            Ok(from_edn(&result))
        }),
    );
    define(
        globals,
        "entity",
        NativeFn::with_closure("entity", Arity::Fixed(2), |args| {
            let db = db_of(&args[0])?;
            let eid_form = to_edn(&args[1]).map_err(|error| verr(error.to_string()))?;
            let eid = eid_of(&db, &eid_form)?;
            let entity = corium_query::Entity::new(&db, eid);
            let mut pairs = vec![(
                Edn::keyword("db/id"),
                Edn::Long(i64::try_from(eid.raw()).unwrap_or(i64::MAX)),
            )];
            for attr in entity.keys() {
                let Some(keyword) = db.idents().ident(attr) else {
                    continue;
                };
                let values = entity.get(attr);
                let many = db
                    .schema()
                    .get(attr)
                    .is_some_and(|meta| meta.cardinality == corium_core::Cardinality::Many);
                let value = if many {
                    let mut items: Vec<Edn> = values
                        .iter()
                        .map(|value| value_to_edn(&db, value))
                        .collect();
                    items.sort();
                    items.dedup();
                    Edn::Set(items)
                } else if let Some(value) = values.first() {
                    value_to_edn(&db, value)
                } else {
                    continue;
                };
                pairs.push((Edn::Keyword(keyword.clone()), value));
            }
            pairs.sort_by(|a, b| a.0.cmp(&b.0));
            Ok(from_edn(&Edn::Map(pairs)))
        }),
    );
    define(
        globals,
        "datoms",
        NativeFn::with_closure("datoms", Arity::Fixed(2), |args| {
            let db = db_of(&args[0])?;
            let index = to_edn(&args[1]).map_err(|error| verr(error.to_string()))?;
            let order = match index.as_keyword().map(|keyword| keyword.name.as_str()) {
                Some("eavt") => IndexOrder::Eavt,
                Some("aevt") => IndexOrder::Aevt,
                Some("avet") => IndexOrder::Avet,
                Some("vaet") => IndexOrder::Vaet,
                _ => return Err(verr("index must be :eavt, :aevt, :avet, or :vaet")),
            };
            let items: Vec<Edn> = db
                .datoms_at(order)
                .map(|datom| datom_edn(&db, datom))
                .collect();
            Ok(from_edn(&Edn::Vector(items)))
        }),
    );
    define(
        globals,
        "as-of",
        NativeFn::with_closure("as-of", Arity::Fixed(2), |args| {
            Ok(db_value(db_of(&args[0])?.as_of(t_of(&args[1])?)))
        }),
    );
    define(
        globals,
        "since",
        NativeFn::with_closure("since", Arity::Fixed(2), |args| {
            Ok(db_value(db_of(&args[0])?.since(t_of(&args[1])?)))
        }),
    );
    define(
        globals,
        "history",
        NativeFn::with_closure("history", Arity::Fixed(1), |args| {
            Ok(db_value(db_of(&args[0])?.history()))
        }),
    );
    define(
        globals,
        "basis-t",
        NativeFn::with_closure("basis-t", Arity::Fixed(1), |args| {
            let db = db_of(&args[0])?;
            Ok(Value::Long(
                i64::try_from(db.basis_t()).map_err(|_| verr("basis out of range"))?,
            ))
        }),
    );
}

// ── Full client API ──────────────────────────────────────────────────────────

fn parse_url(url: &str) -> ValueResult<ConnectConfig> {
    let rest = url
        .strip_prefix("corium://")
        .ok_or_else(|| verr("connection url must look like corium://host:port/db"))?;
    let (authority, db) = rest
        .split_once('/')
        .filter(|(authority, db)| !authority.is_empty() && !db.is_empty())
        .ok_or_else(|| verr("connection url must look like corium://host:port/db"))?;
    Ok(ConnectConfig::new(format!("http://{authority}"), db))
}

fn report_edn(db: &Db, report: &PeerReport) -> Edn {
    Edn::Map(vec![
        (
            Edn::keyword("t"),
            Edn::Long(i64::try_from(report.t).unwrap_or(i64::MAX)),
        ),
        (Edn::keyword("tx-instant"), Edn::Long(report.tx_instant)),
        (
            Edn::keyword("tx-data"),
            Edn::Vector(
                report
                    .datoms
                    .iter()
                    .map(|datom| datom_edn(db, datom))
                    .collect(),
            ),
        ),
    ])
}

/// Registers the complete `corium.api` namespace (read operations plus
/// connection management) into `globals`, driving async peer calls on
/// `handle`'s runtime.
#[allow(clippy::too_many_lines)]
pub fn register_api(globals: &Arc<GlobalEnv>, handle: &Handle) {
    register_read_api(globals);
    let h = handle.clone();
    define(
        globals,
        "connect",
        NativeFn::with_closure("connect", Arity::Fixed(1), move |args| {
            let Value::Str(url) = &args[0] else {
                return Err(verr("connect takes a corium://host:port/db url string"));
            };
            let config = parse_url(url.get())?;
            let conn = h
                .block_on(Connection::connect(config))
                .map_err(|error| verr(error.to_string()))?;
            Ok(Value::NativeObject(GcPtr::new(NativeObjectBox::new(
                ConnHandle {
                    conn: Arc::new(conn),
                    handle: h.clone(),
                },
            ))))
        }),
    );
    define(
        globals,
        "transact",
        NativeFn::with_closure("transact", Arity::Fixed(2), |args| {
            let (conn, handle) = conn_of(&args[0])?;
            let tx = to_edn(&args[1]).map_err(|error| verr(error.to_string()))?;
            let forms = tx
                .as_seq()
                .ok_or_else(|| verr("tx-data must be a vector"))?
                .to_vec();
            let result = handle
                .block_on(conn.transact(forms))
                .map_err(|error| verr(error.to_string()))?;
            let tempids = Edn::Map(
                result
                    .tempids
                    .iter()
                    .map(|(temp, eid)| {
                        (
                            Edn::Str(temp.clone()),
                            Edn::Long(i64::try_from(eid.raw()).unwrap_or(i64::MAX)),
                        )
                    })
                    .collect(),
            );
            let summary = Edn::Map(vec![
                (
                    Edn::keyword("basis-t"),
                    Edn::Long(i64::try_from(result.basis_t).unwrap_or(i64::MAX)),
                ),
                (Edn::keyword("tempids"), tempids),
                (Edn::keyword("tx-instant"), Edn::Long(result.tx_instant)),
            ]);
            // Splice the db-after native object into the converted map.
            let Value::Map(map) = from_edn(&summary) else {
                return Err(verr("internal: summary must convert to a map"));
            };
            Ok(Value::Map(map.assoc(
                Value::keyword(cljrs_value::Keyword::simple("db-after")),
                db_value(result.db_after),
            )))
        }),
    );
    define(
        globals,
        "db",
        NativeFn::with_closure("db", Arity::Fixed(1), |args| {
            let (conn, _) = conn_of(&args[0])?;
            Ok(db_value(conn.db()))
        }),
    );
    define(
        globals,
        "sync",
        NativeFn::with_closure("sync", Arity::Fixed(1), |args| {
            let (conn, handle) = conn_of(&args[0])?;
            let db = handle
                .block_on(conn.sync())
                .map_err(|error| verr(error.to_string()))?;
            Ok(db_value(db))
        }),
    );
    define(
        globals,
        "tx-range",
        NativeFn::with_closure("tx-range", Arity::Fixed(3), |args| {
            let (conn, _) = conn_of(&args[0])?;
            let start = t_of(&args[1])?;
            let end = match &args[2] {
                Value::Nil => None,
                other => Some(t_of(other)?),
            };
            let db = conn.db();
            let items: Vec<Edn> = conn
                .tx_range(start, end)
                .into_iter()
                .map(|record| {
                    Edn::Map(vec![
                        (
                            Edn::keyword("t"),
                            Edn::Long(i64::try_from(record.t).unwrap_or(i64::MAX)),
                        ),
                        (Edn::keyword("tx-instant"), Edn::Long(record.tx_instant)),
                        (
                            Edn::keyword("tx-data"),
                            Edn::Vector(
                                record
                                    .datoms
                                    .iter()
                                    .map(|datom| datom_edn(&db, datom))
                                    .collect(),
                            ),
                        ),
                    ])
                })
                .collect();
            Ok(from_edn(&Edn::Vector(items)))
        }),
    );
    define(
        globals,
        "tx-report-queue",
        NativeFn::with_closure("tx-report-queue", Arity::Fixed(1), |args| {
            let (conn, handle) = conn_of(&args[0])?;
            Ok(Value::NativeObject(GcPtr::new(NativeObjectBox::new(
                QueueHandle {
                    rx: Mutex::new(conn.tx_reports()),
                    handle,
                },
            ))))
        }),
    );
    define(
        globals,
        "take-report",
        NativeFn::with_closure("take-report", Arity::Fixed(2), |args| {
            let Value::NativeObject(obj) = &args[0] else {
                return Err(verr("expected a tx-report queue"));
            };
            let Some(queue) = obj.get().downcast_ref::<QueueHandle>() else {
                return Err(verr("expected a tx-report queue"));
            };
            let timeout =
                u64::try_from(long_of(&args[1], "timeout-ms")?).map_err(|_| verr("timeout-ms"))?;
            let mut rx = queue
                .rx
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let report = queue.handle.block_on(async {
                tokio::time::timeout(Duration::from_millis(timeout), rx.recv()).await
            });
            match report {
                Err(_) => Ok(Value::Nil),
                Ok(Err(_)) => Err(verr("tx-report queue closed")),
                Ok(Ok(report)) => {
                    let edn = report_edn(&report.db_after, &report);
                    let Value::Map(map) = from_edn(&edn) else {
                        return Err(verr("internal: report must convert to a map"));
                    };
                    Ok(Value::Map(map.assoc(
                        Value::keyword(cljrs_value::Keyword::simple("db-after")),
                        db_value(report.db_after.clone()),
                    )))
                }
            }
        }),
    );
}

/// Builds a full cljrs client environment: `clojure.core`, the complete
/// `corium.api` namespace, and a `d` alias in `user`.
///
/// Must be called on the thread that will run the cljrs program.
#[must_use]
pub fn client_env(handle: &Handle) -> Arc<GlobalEnv> {
    let globals = cljrs_interp::standard_env_minimal(None, None, None);
    register_api(&globals, handle);
    globals.add_alias("user", "d", "corium.api");
    globals
}
