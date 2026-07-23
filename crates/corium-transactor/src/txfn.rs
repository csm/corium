//! Built-in Clojure transaction-function runtime (`:db/fn`, ADR-0008) on the
//! bounded, GC-less `cljrs-tx` interpreter (feature `cljrs`).
//!
//! A database function is an entity whose `:db/fn` attribute holds the
//! function source. An invocation form `[:my/fn arg…]` inside transaction
//! data calls the function with the db-in-transaction plus the arguments;
//! the returned tx-data is expanded recursively up to a depth limit. The log
//! records only expanded datoms, so replay never re-runs functions.
//!
//! Every invocation runs in a fresh `cljrs-tx` arena under a fuel (gas) and
//! managed-memory budget; the arena — environment, inputs, and every
//! intermediate value — is destroyed when the call returns, so no watchdog
//! thread or wall-clock deadline is needed. The database never enters the
//! arena: functions receive an opaque pure-data *token* as their `db`
//! argument, and the read-only `corium.api` host functions (`q`, `pull`,
//! `entity`, `datoms`, `as-of`, `since`, `history`, `basis-t`) interpret the
//! token against the real database value they close over.

use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

use cljrs_tx::{HostApi, TxError, TxLimits, TxProgram};
use cljrs_value::clone::SerializedValue;
use corium_core::{EntityId, IndexOrder, Keyword, TotalF64, Value as CoreValue};
use corium_db::Db;
use corium_query::QInput;
use corium_query::boundary::{edn_to_value, value_to_edn};
use corium_query::edn::Edn;
use thiserror::Error;

use crate::node::TxFnExpander;

/// Per-invocation resource budget for database functions.
#[derive(Clone, Copy, Debug)]
pub struct DbFnBudget {
    /// Execution credits (cljrs gas) one invocation may spend.
    pub fuel: u64,
    /// Managed bytes one invocation's arena may allocate.
    pub memory_bytes: usize,
    /// Nested function applications one invocation may stack (interpreted
    /// frames consume real Rust stack on the invocation thread, so this
    /// must stay well inside [`INVOCATION_STACK_BYTES`]).
    pub call_depth: u64,
}

impl Default for DbFnBudget {
    fn default() -> Self {
        let defaults = TxLimits::default();
        Self {
            fuel: defaults.gas,
            memory_bytes: defaults.memory_bytes,
            call_depth: defaults.call_depth,
        }
    }
}

/// Invocation thread stack size: sized so a `call_depth` chain of
/// interpreter frames cannot overflow the thread.
pub const INVOCATION_STACK_BYTES: usize = 64 * 1024 * 1024;

/// Database-function expansion failure; aborts the transaction.
#[derive(Debug, Error)]
pub enum DbFnError {
    /// The function source failed to parse or evaluate, or blew its budget.
    #[error("db function failed: {0}")]
    Execution(#[from] TxError),
    /// Functions kept returning invocations past the depth limit.
    #[error("db function recursion limit ({0}) exceeded")]
    Recursion(usize),
    /// A function returned something other than tx-data.
    #[error("db function must return a sequence of tx forms, got {0}")]
    BadResult(String),
    /// A value could not cross the isolation boundary.
    #[error("db function boundary conversion failed: {0}")]
    Convert(String),
}

/// Transaction operations handled natively by `corium-tx`.
const NATIVE_OPS: &[&str] = &["db/add", "db/retract", "db/cas", "db/retractEntity"];

/// Expands `[:fn-ident arg…]` invocations through the `cljrs-tx` runtime.
pub struct DbFnExpander {
    budget: DbFnBudget,
    max_depth: usize,
    /// Parsed programs cached by source hash; parsing is outside the
    /// invocation arena, so installed sources compile once per process.
    programs: Mutex<HashMap<u64, Arc<TxProgram>>>,
}

impl Default for DbFnExpander {
    fn default() -> Self {
        Self::new(DbFnBudget::default())
    }
}

impl DbFnExpander {
    /// Creates an expander with the given per-invocation budget and the
    /// default recursion depth (16).
    #[must_use]
    pub fn new(budget: DbFnBudget) -> Self {
        Self {
            budget,
            max_depth: 16,
            programs: Mutex::new(HashMap::new()),
        }
    }

    /// Overrides the recursion depth limit.
    #[must_use]
    pub fn with_max_depth(mut self, max_depth: usize) -> Self {
        self.max_depth = max_depth;
        self
    }

    /// Expands every database-function invocation in `forms` against `db`,
    /// returning tx forms containing only native operations and map forms.
    ///
    /// # Errors
    /// Returns [`DbFnError`] when a function fails, exceeds its budget, or
    /// recursion exceeds the depth limit.
    pub fn expand(&self, db: &Db, forms: Vec<Edn>) -> Result<Vec<Edn>, DbFnError> {
        let host = read_api(db);
        self.expand_at(db, &host, forms, self.max_depth)
    }

    fn expand_at(
        &self,
        db: &Db,
        host: &HostApi,
        forms: Vec<Edn>,
        depth: usize,
    ) -> Result<Vec<Edn>, DbFnError> {
        let mut out = Vec::with_capacity(forms.len());
        for form in forms {
            match invocation(db, &form) {
                None => out.push(form),
                Some((source, args)) => {
                    if depth == 0 {
                        return Err(DbFnError::Recursion(self.max_depth));
                    }
                    let produced = self.invoke(db, host, &source, &args)?;
                    out.extend(self.expand_at(db, host, produced, depth - 1)?);
                }
            }
        }
        Ok(out)
    }

    /// Runs one invocation and returns the produced tx forms.
    fn invoke(
        &self,
        db: &Db,
        host: &HostApi,
        source: &str,
        args: &[Edn],
    ) -> Result<Vec<Edn>, DbFnError> {
        let program = self.program(source)?;
        let mut call_args = Vec::with_capacity(args.len() + 1);
        call_args.push(db_token(db));
        for arg in args {
            call_args.push(edn_to_sv(arg));
        }
        let limits = TxLimits {
            memory_bytes: self.budget.memory_bytes,
            gas: self.budget.fuel,
            call_depth: self.budget.call_depth,
        };
        // Interpreted frames consume real Rust stack, so each invocation
        // runs on a scoped thread sized for the full call-depth budget
        // rather than on the caller's (possibly small) blocking thread.
        let result = std::thread::scope(|scope| {
            std::thread::Builder::new()
                .name("corium-txfn".into())
                .stack_size(INVOCATION_STACK_BYTES)
                .spawn_scoped(scope, || {
                    cljrs_tx::execute_with_host(&program, call_args, limits, host)
                })
                .map_err(|error| {
                    DbFnError::Convert(format!("cannot spawn invocation thread: {error}"))
                })?
                .join()
                .map_err(|_| DbFnError::Convert("invocation thread panicked".into()))?
                .map_err(DbFnError::from)
        })?;
        let items = match &result {
            SerializedValue::List(items) | SerializedValue::Vector(items) => items.clone(),
            SerializedValue::Nil => Vec::new(),
            other => return Err(DbFnError::BadResult(format!("{other:?}"))),
        };
        items
            .iter()
            .map(|item| sv_to_edn(item).map_err(DbFnError::Convert))
            .collect()
    }

    /// Parses (or reuses) the program for `source`.
    fn program(&self, source: &str) -> Result<Arc<TxProgram>, DbFnError> {
        let mut hasher = DefaultHasher::new();
        source.hash(&mut hasher);
        let key = hasher.finish();
        let mut programs = self
            .programs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(program) = programs.get(&key) {
            return Ok(Arc::clone(program));
        }
        let program = Arc::new(TxProgram::parse(source)?);
        programs.insert(key, Arc::clone(&program));
        Ok(program)
    }
}

impl TxFnExpander for DbFnExpander {
    fn expand(&self, db: &Db, forms: Vec<Edn>) -> Result<Vec<Edn>, String> {
        Self::expand(self, db, forms).map_err(|error| error.to_string())
    }
}

/// Recognizes a database-function invocation form, returning its source and
/// argument forms.
fn invocation(db: &Db, form: &Edn) -> Option<(String, Vec<Edn>)> {
    let (Edn::Vector(items) | Edn::List(items)) = form else {
        return None;
    };
    let head = items.first()?.as_keyword()?;
    let full = head.namespace.as_deref().map_or_else(
        || head.name.clone(),
        |namespace| format!("{namespace}/{}", head.name),
    );
    if NATIVE_OPS.contains(&full.as_str()) {
        return None;
    }
    let source = fn_source(db, head)?;
    Some((source, items[1..].to_vec()))
}

/// Looks up the `:db/fn` source for a function ident: first through the
/// schema-installed ident registry, then through a `:db/ident`
/// unique-keyword attribute when the database declares one.
fn fn_source(db: &Db, ident: &Keyword) -> Option<String> {
    let fn_attr = db.idents().entid(&Keyword::parse("db/fn"))?;
    let entity = db.idents().entid(ident).or_else(|| {
        let ident_attr = db.idents().entid(&Keyword::parse("db/ident"))?;
        let interned = db.interner().get(ident)?;
        db.lookup(ident_attr, &CoreValue::Keyword(interned))
    })?;
    match db.values(entity, fn_attr).into_iter().next() {
        Some(CoreValue::Str(source)) => Some(source.to_string()),
        _ => None,
    }
}

// ── Database token ───────────────────────────────────────────────────────────
//
// The db argument a function receives is a plain map carrying the basis and
// the requested time view under `:corium.db/*` keys. Host functions rebuild
// the corresponding database view from the value they close over; `as-of`,
// `since`, and `history` just return an adjusted token.

const TOKEN_BASIS: &str = "basis-t";
const TOKEN_AS_OF: &str = "as-of";
const TOKEN_SINCE: &str = "since";
const TOKEN_HISTORY: &str = "history";

fn token_key(name: &str) -> SerializedValue {
    SerializedValue::Keyword {
        namespace: Some("corium.db".into()),
        name: name.into(),
    }
}

/// Structural keyword match (`SerializedValue` has no `PartialEq`).
fn is_keyword(value: &SerializedValue, namespace: Option<&str>, name: &str) -> bool {
    matches!(
        value,
        SerializedValue::Keyword {
            namespace: kw_namespace,
            name: kw_name,
        } if kw_namespace.as_deref() == namespace && &**kw_name == name
    )
}

/// The time view a token requests, applied to the closed-over database.
#[derive(Clone, Copy, Default)]
struct TokenView {
    as_of: Option<u64>,
    since: Option<u64>,
    history: bool,
}

impl TokenView {
    fn apply(self, db: &Db) -> Db {
        let mut view = db.clone();
        if let Some(t) = self.as_of {
            view = view.as_of(t);
        }
        if let Some(t) = self.since {
            view = view.since(t);
        }
        if self.history {
            view = view.history();
        }
        view
    }

    fn token(self, db: &Db) -> SerializedValue {
        let optional_t = |t: Option<u64>| {
            t.map_or(SerializedValue::Nil, |t| {
                SerializedValue::Long(i64::try_from(t).unwrap_or(i64::MAX))
            })
        };
        SerializedValue::ArrayMap(vec![
            (
                token_key(TOKEN_BASIS),
                SerializedValue::Long(i64::try_from(db.basis_t()).unwrap_or(i64::MAX)),
            ),
            (token_key(TOKEN_AS_OF), optional_t(self.as_of)),
            (token_key(TOKEN_SINCE), optional_t(self.since)),
            (
                token_key(TOKEN_HISTORY),
                SerializedValue::Bool(self.history),
            ),
        ])
    }
}

/// The token handed to a function as its `db` argument.
fn db_token(db: &Db) -> SerializedValue {
    TokenView::default().token(db)
}

/// Parses a token map back into its view; `None` when `value` is not a
/// database token.
fn token_view(value: &SerializedValue) -> Option<TokenView> {
    let (SerializedValue::ArrayMap(pairs)
    | SerializedValue::HashMap(pairs)
    | SerializedValue::SortedMap(pairs)) = value
    else {
        return None;
    };
    let field = |name: &str| {
        pairs
            .iter()
            .find(|(key, _)| is_keyword(key, Some("corium.db"), name))
            .map(|(_, value)| value)
    };
    // The basis field marks a token; the remaining fields are optional so
    // hand-built tokens degrade gracefully.
    field(TOKEN_BASIS)?;
    let t_of = |name: &str| match field(name) {
        Some(SerializedValue::Long(t)) => u64::try_from(*t).ok(),
        _ => None,
    };
    Some(TokenView {
        as_of: t_of(TOKEN_AS_OF),
        since: t_of(TOKEN_SINCE),
        history: matches!(field(TOKEN_HISTORY), Some(SerializedValue::Bool(true))),
    })
}

// ── Read-only `corium.api` host functions ────────────────────────────────────

/// Builds the read-only `corium.api` host surface over `db`.
#[allow(clippy::too_many_lines)]
fn read_api(db: &Db) -> HostApi {
    let mut host = HostApi::new();
    let arity = |args: &[SerializedValue], want: usize, what: &str| {
        if args.len() == want {
            Ok(())
        } else {
            Err(format!("{what} takes {want} arguments, got {}", args.len()))
        }
    };
    let view_arg = |db: &Db, value: &SerializedValue, what: &str| {
        token_view(value)
            .map(|view| view.apply(db))
            .ok_or_else(|| format!("{what} takes a database as its first argument"))
    };

    let base = db.clone();
    host.define("corium.api", "q", move |args: &[SerializedValue]| {
        if args.len() < 2 {
            return Err("q takes a query and at least one input".into());
        }
        let query = sv_to_edn(&args[0])?;
        // Own every input across the boundary conversion, then borrow for
        // the query call.
        let inputs: Vec<Result<Db, Edn>> = args[1..]
            .iter()
            .map(|arg| match token_view(arg) {
                Some(view) => Ok(Ok(view.apply(&base))),
                None => sv_to_edn(arg).map(Err),
            })
            .collect::<Result<_, _>>()?;
        let qinputs: Vec<QInput<'_>> = inputs
            .iter()
            .map(|input| match input {
                Ok(db) => QInput::Db(db),
                Err(edn) => QInput::Edn(edn.clone()),
            })
            .collect();
        let result = corium_query::q(&query, &qinputs).map_err(|error| error.to_string())?;
        Ok(edn_to_sv(&result))
    });

    let base = db.clone();
    host.define("corium.api", "pull", move |args: &[SerializedValue]| {
        arity(args, 3, "pull")?;
        let db = view_arg(&base, &args[0], "pull")?;
        let pattern = sv_to_edn(&args[1])?;
        let eid = eid_of(&db, &sv_to_edn(&args[2])?)?;
        let result = corium_query::pull(&db, &pattern, eid).map_err(|error| error.to_string())?;
        Ok(edn_to_sv(&result))
    });

    let base = db.clone();
    host.define("corium.api", "entity", move |args: &[SerializedValue]| {
        arity(args, 2, "entity")?;
        let db = view_arg(&base, &args[0], "entity")?;
        let eid = eid_of(&db, &sv_to_edn(&args[1])?)?;
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
        Ok(edn_to_sv(&Edn::Map(pairs)))
    });

    let base = db.clone();
    host.define("corium.api", "datoms", move |args: &[SerializedValue]| {
        arity(args, 2, "datoms")?;
        let db = view_arg(&base, &args[0], "datoms")?;
        let index = sv_to_edn(&args[1])?;
        let order = match index.as_keyword().map(|keyword| keyword.name.as_str()) {
            Some("eavt") => IndexOrder::Eavt,
            Some("aevt") => IndexOrder::Aevt,
            Some("avet") => IndexOrder::Avet,
            Some("vaet") => IndexOrder::Vaet,
            _ => return Err("index must be :eavt, :aevt, :avet, or :vaet".into()),
        };
        let items: Vec<Edn> = db
            .datoms_at(order)
            .map(|datom| datom_edn(&db, datom))
            .collect();
        Ok(edn_to_sv(&Edn::Vector(items)))
    });

    let base = db.clone();
    host.define("corium.api", "as-of", move |args: &[SerializedValue]| {
        arity(args, 2, "as-of")?;
        let mut view =
            token_view(&args[0]).ok_or("as-of takes a database as its first argument")?;
        view.as_of = Some(t_arg(&args[1])?);
        Ok(view.token(&base))
    });

    let base = db.clone();
    host.define("corium.api", "since", move |args: &[SerializedValue]| {
        arity(args, 2, "since")?;
        let mut view =
            token_view(&args[0]).ok_or("since takes a database as its first argument")?;
        view.since = Some(t_arg(&args[1])?);
        Ok(view.token(&base))
    });

    let base = db.clone();
    host.define(
        "corium.api",
        "history",
        move |args: &[SerializedValue]| {
            arity(args, 1, "history")?;
            let mut view =
                token_view(&args[0]).ok_or("history takes a database as its argument")?;
            view.history = true;
            Ok(view.token(&base))
        },
    );

    let base = db.clone();
    host.define(
        "corium.api",
        "basis-t",
        move |args: &[SerializedValue]| {
            arity(args, 1, "basis-t")?;
            token_view(&args[0]).ok_or("basis-t takes a database as its argument")?;
            Ok(SerializedValue::Long(
                i64::try_from(base.basis_t()).unwrap_or(i64::MAX),
            ))
        },
    );

    host
}

fn t_arg(value: &SerializedValue) -> Result<u64, String> {
    match value {
        SerializedValue::Long(t) => u64::try_from(*t).map_err(|_| "t must be non-negative".into()),
        _ => Err("t must be a long".into()),
    }
}

/// Resolves an entity position: long, ident keyword, `#eid`, or lookup ref.
fn eid_of(db: &Db, form: &Edn) -> Result<EntityId, String> {
    match form {
        Edn::Long(n) => u64::try_from(*n)
            .map(EntityId::from_raw)
            .map_err(|_| "entity id must be non-negative".into()),
        Edn::Keyword(keyword) => db
            .idents()
            .entid(keyword)
            .ok_or_else(|| format!("unknown ident {keyword}")),
        Edn::Tagged(tag, inner) if tag == "eid" => match inner.as_ref() {
            Edn::Long(n) => u64::try_from(*n)
                .map(EntityId::from_raw)
                .map_err(|_| "entity id must be non-negative".into()),
            _ => Err("#eid requires a long".into()),
        },
        Edn::Vector(items) => {
            let [attr_form, value_form] = items.as_slice() else {
                return Err("lookup ref must be [attr value]".into());
            };
            let attr = attr_form
                .as_keyword()
                .and_then(|keyword| db.idents().entid(keyword))
                .ok_or_else(|| format!("unknown lookup attribute {attr_form}"))?;
            let value_type = db
                .schema()
                .get(attr)
                .map(|meta| meta.value_type)
                .ok_or("lookup attribute is not installed")?;
            let value = edn_to_value(Some(db), value_form)
                .map(|value| corium_query::exec::coerce_for_type(value, value_type))
                .ok_or_else(|| format!("bad lookup value {value_form}"))?;
            db.lookup(attr, &value)
                .ok_or_else(|| format!("lookup ref [{attr_form} {value_form}] not found"))
        }
        other => Err(format!("bad entity position {other}")),
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

// ── Boundary EDN ↔ pure boundary data ────────────────────────────────────────
//
// Mirrors `corium-cljrs`'s conversion policy at the `SerializedValue` level:
// `#uuid` maps to the native UUID shape; tags with no native shape (`#inst`,
// `#eid`, `#tx`, `#bytes`) ride as `{:corium/tag <tag>}` metadata on the
// wrapped value, which cljrs treats as equality-transparent.

/// Metadata key marking a tagged engine value carried as wrapped data.
const TAG_KEY: &str = "tag";

fn tag_meta_key() -> SerializedValue {
    SerializedValue::Keyword {
        namespace: Some("corium".into()),
        name: TAG_KEY.into(),
    }
}

/// Converts a boundary EDN form to pure boundary data.
fn edn_to_sv(form: &Edn) -> SerializedValue {
    match form {
        Edn::Nil => SerializedValue::Nil,
        Edn::Bool(v) => SerializedValue::Bool(*v),
        Edn::Long(v) => SerializedValue::Long(*v),
        Edn::Double(v) => SerializedValue::Double(v.0),
        Edn::Str(v) => SerializedValue::Str(v.clone()),
        Edn::Keyword(k) => SerializedValue::Keyword {
            namespace: k.namespace.as_deref().map(Arc::from),
            name: Arc::from(k.name.as_str()),
        },
        Edn::Symbol(s) => {
            let (namespace, name) = match s.split_once('/') {
                Some((namespace, name)) if !namespace.is_empty() && !name.is_empty() => {
                    (Some(Arc::from(namespace)), Arc::from(name))
                }
                _ => (None, Arc::from(s.as_str())),
            };
            SerializedValue::Symbol {
                namespace,
                name,
                version: None,
            }
        }
        Edn::List(items) => SerializedValue::List(items.iter().map(edn_to_sv).collect()),
        Edn::Vector(items) => SerializedValue::Vector(items.iter().map(edn_to_sv).collect()),
        Edn::Map(pairs) => SerializedValue::ArrayMap(
            pairs
                .iter()
                .map(|(k, v)| (edn_to_sv(k), edn_to_sv(v)))
                .collect(),
        ),
        Edn::Set(items) => SerializedValue::HashSet(items.iter().map(edn_to_sv).collect()),
        Edn::Tagged(tag, inner) => match (tag.as_str(), inner.as_ref()) {
            ("uuid", Edn::Str(hex)) => u128::from_str_radix(hex, 16)
                .map_or_else(|_| tagged_fallback(tag, inner), SerializedValue::Uuid),
            _ => tagged_fallback(tag, inner),
        },
    }
}

/// Tags with no native pure-data shape ride as `{:corium/tag <tag>}`
/// metadata on the converted inner value.
fn tagged_fallback(tag: &str, inner: &Edn) -> SerializedValue {
    SerializedValue::WithMeta {
        value: Box::new(edn_to_sv(inner)),
        meta: Box::new(SerializedValue::ArrayMap(vec![(
            tag_meta_key(),
            SerializedValue::Keyword {
                namespace: None,
                name: Arc::from(tag),
            },
        )])),
    }
}

/// Converts pure boundary data back to boundary EDN.
///
/// # Errors
/// Returns a message for values with no engine representation (shared
/// state, big numbers, records, …).
fn sv_to_edn(value: &SerializedValue) -> Result<Edn, String> {
    match value {
        SerializedValue::Nil => Ok(Edn::Nil),
        SerializedValue::Bool(v) => Ok(Edn::Bool(*v)),
        SerializedValue::Long(v) => Ok(Edn::Long(*v)),
        SerializedValue::Double(v) => Ok(Edn::Double(TotalF64(*v))),
        SerializedValue::Char(c) => Ok(Edn::Str(c.to_string())),
        SerializedValue::Str(v) => Ok(Edn::Str(v.clone())),
        SerializedValue::Uuid(v) => Ok(Edn::Tagged(
            "uuid".into(),
            Box::new(Edn::Str(format!("{v:032x}"))),
        )),
        SerializedValue::Keyword { namespace, name } => Ok(Edn::Keyword(Keyword {
            namespace: namespace.as_deref().map(str::to_owned),
            name: name.to_string(),
        })),
        SerializedValue::Symbol {
            namespace, name, ..
        } => Ok(Edn::Symbol(namespace.as_deref().map_or_else(
            || name.to_string(),
            |namespace| format!("{namespace}/{name}"),
        ))),
        SerializedValue::List(items) | SerializedValue::Queue(items) => Ok(Edn::List(
            items.iter().map(sv_to_edn).collect::<Result<_, _>>()?,
        )),
        SerializedValue::Vector(items) => Ok(Edn::Vector(
            items.iter().map(sv_to_edn).collect::<Result<_, _>>()?,
        )),
        SerializedValue::Cons { .. } => {
            let mut items = Vec::new();
            let mut current = value;
            loop {
                match current {
                    SerializedValue::Cons { head, tail } => {
                        items.push(sv_to_edn(head)?);
                        current = tail;
                    }
                    SerializedValue::Nil => break,
                    SerializedValue::List(rest) | SerializedValue::Vector(rest) => {
                        for item in rest {
                            items.push(sv_to_edn(item)?);
                        }
                        break;
                    }
                    other => {
                        items.push(sv_to_edn(other)?);
                        break;
                    }
                }
            }
            Ok(Edn::List(items))
        }
        SerializedValue::ArrayMap(pairs)
        | SerializedValue::HashMap(pairs)
        | SerializedValue::SortedMap(pairs) => {
            let mut converted = pairs
                .iter()
                .map(|(k, v)| Ok((sv_to_edn(k)?, sv_to_edn(v)?)))
                .collect::<Result<Vec<_>, String>>()?;
            converted.sort_by(|a, b| a.0.cmp(&b.0));
            Ok(Edn::Map(converted))
        }
        SerializedValue::HashSet(items) | SerializedValue::SortedSet(items) => {
            let mut converted = items.iter().map(sv_to_edn).collect::<Result<Vec<_>, _>>()?;
            converted.sort();
            converted.dedup();
            Ok(Edn::Set(converted))
        }
        SerializedValue::WithMeta { value, meta } => {
            let converted = sv_to_edn(value)?;
            Ok(match meta_tag(meta) {
                Some(tag) => Edn::Tagged(tag, Box::new(converted)),
                None => converted,
            })
        }
        other => Err(format!("value has no Corium representation: {other:?}")),
    }
}

/// Extracts the `{:corium/tag <tag>}` marker from converted metadata.
fn meta_tag(meta: &SerializedValue) -> Option<String> {
    let (SerializedValue::ArrayMap(pairs)
    | SerializedValue::HashMap(pairs)
    | SerializedValue::SortedMap(pairs)) = meta
    else {
        return None;
    };
    pairs.iter().find_map(|(key, value)| {
        if !is_keyword(key, Some("corium"), TAG_KEY) {
            return None;
        }
        match value {
            SerializedValue::Keyword { namespace, name } => Some(namespace.as_deref().map_or_else(
                || name.to_string(),
                |namespace| format!("{namespace}/{name}"),
            )),
            _ => None,
        }
    })
}
