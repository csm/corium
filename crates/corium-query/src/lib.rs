//! The Corium query engine: EDN Datalog, Pull, the entity API, and direct
//! index access, executing on the peer against immutable [`corium_db::Db`]
//! values (see `docs/design/query-engine.md`).

pub mod aggregate;
pub mod ast;
pub mod boundary;
pub mod builtins;
pub mod cache;
pub mod edn;
pub mod entity;
pub mod exec;
pub mod plan;
pub mod pull;

use std::collections::{BTreeMap, BTreeSet};

use corium_core::{Keyword, Value};
use corium_db::Db;
use thiserror::Error;

use crate::aggregate::AggOut;
use crate::ast::{FindElem, FindSpec, InSpec, Query, Var, parse_query, parse_rules};
use crate::boundary::{edn_to_value, value_to_edn};
use crate::edn::{Edn, EdnError};
use crate::exec::{ExecCtx, Frame, to_entity};

pub use crate::cache::QueryCache;
pub use crate::entity::Entity;
pub use crate::pull::{pull, pull_many};

/// Query failure.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum QueryError {
    /// Malformed query, rules, or pull pattern.
    #[error("query parse error: {0}")]
    Parse(String),
    /// Boundary EDN failed to read.
    #[error(transparent)]
    Edn(#[from] EdnError),
    /// An ident has no entity in the database.
    #[error("unknown ident {0}")]
    UnknownIdent(Keyword),
    /// A `$…` source has no bound database.
    #[error("unknown database source {0}")]
    UnknownSource(String),
    /// A variable was consumed before anything bound it.
    #[error("unbound variable {0}")]
    Unbound(String),
    /// Wrong number of arguments.
    #[error("arity error: {0}")]
    Arity(String),
    /// Operand types not supported by an operation.
    #[error("type error: {0}")]
    Type(String),
    /// Feature or name outside the v1 native set.
    #[error("unsupported: {0}")]
    Unsupported(String),
    /// The execution fuel budget was exhausted.
    #[error("query fuel exhausted")]
    FuelExhausted,
}

/// One query input, positionally matching the query's `:in` specification.
#[derive(Clone, Debug)]
pub enum QInput<'a> {
    /// A database value for a `$…` source.
    Db(&'a Db),
    /// An EDN argument (scalar, tuple, collection, relation, or rule set).
    Edn(Edn),
}

/// Execution telemetry.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ExecReport {
    /// Datoms touched by index scans.
    pub datoms_scanned: usize,
}

/// Resolution hook for predicate/function clause names outside the native
/// set: given the name and evaluated arguments, returns `Some(result)` when
/// the extern resolves the call, `None` to fall through to the canonical
/// unsupported-name error. This is the seam the sandboxed cljrs host wires
/// into (`docs/design/query-engine.md`, resolution step 2).
pub type ExternCall = std::sync::Arc<
    dyn Fn(&str, &[Value]) -> Option<Result<builtins::CallResult, QueryError>> + Send + Sync,
>;

/// Options bounding a query execution.
#[derive(Clone, Default)]
pub struct ExecOptions {
    /// Maximum datoms the execution may touch (fuel).
    pub fuel: Option<u64>,
    /// Optional resolver for non-native call clause names.
    pub extern_call: Option<ExternCall>,
}

impl std::fmt::Debug for ExecOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecOptions")
            .field("fuel", &self.fuel)
            .field("extern_call", &self.extern_call.is_some())
            .finish()
    }
}

/// Runs a query given as an EDN form.
///
/// # Errors
/// Returns [`QueryError`] for malformed queries and execution failures.
pub fn q(form: &Edn, inputs: &[QInput<'_>]) -> Result<Edn, QueryError> {
    let query = parse_query(form)?;
    run(&query, inputs, ExecOptions::default()).map(|(result, _)| result)
}

/// Runs a query given as EDN text.
///
/// # Errors
/// Returns [`QueryError`] for unreadable text, malformed queries, and
/// execution failures.
pub fn q_str(text: &str, inputs: &[QInput<'_>]) -> Result<Edn, QueryError> {
    q(&edn::read_one(text)?, inputs)
}

/// Runs a parsed query with options, returning the result and telemetry.
///
/// # Errors
/// Returns [`QueryError`] for execution failures.
#[allow(clippy::too_many_lines, clippy::needless_pass_by_value)]
pub fn run(
    query: &Query,
    inputs: &[QInput<'_>],
    options: ExecOptions,
) -> Result<(Edn, ExecReport), QueryError> {
    if inputs.len() != query.inputs.len() {
        return Err(QueryError::Arity(format!(
            "query takes {} inputs, got {}",
            query.inputs.len(),
            inputs.len()
        )));
    }

    // Bind inputs: databases, rules, and initial frames.
    let mut dbs: BTreeMap<String, &Db> = BTreeMap::new();
    let mut rules = Vec::new();
    for (spec, input) in query.inputs.iter().zip(inputs) {
        if let (InSpec::Db(name), QInput::Db(db)) = (spec, input) {
            dbs.insert(name.clone(), db);
        }
    }
    let default_db = dbs
        .get(ast::DEFAULT_SRC)
        .copied()
        .or_else(|| dbs.values().next().copied());
    let mut frames: Vec<Frame> = vec![Frame::new()];
    for (spec, input) in query.inputs.iter().zip(inputs) {
        match (spec, input) {
            (InSpec::Db(_), QInput::Db(_)) => {}
            (InSpec::Rules, QInput::Edn(form)) => rules = parse_rules(form)?,
            (InSpec::Scalar(var), QInput::Edn(form)) => {
                let value = input_value(default_db, form)?;
                for frame in &mut frames {
                    frame.insert(var.clone(), value.clone());
                }
            }
            (InSpec::Tuple(vars), QInput::Edn(form)) => {
                let items = form.as_seq().ok_or_else(|| {
                    QueryError::Type(format!("tuple input requires a vector, got {form}"))
                })?;
                if items.len() != vars.len() {
                    return Err(QueryError::Arity("tuple input arity mismatch".into()));
                }
                for (var, item) in vars.iter().zip(items) {
                    let value = input_value(default_db, item)?;
                    for frame in &mut frames {
                        frame.insert(var.clone(), value.clone());
                    }
                }
            }
            (InSpec::Coll(var), QInput::Edn(form)) => {
                let items = form.as_seq().ok_or_else(|| {
                    QueryError::Type(format!("collection input requires a vector, got {form}"))
                })?;
                let values = items
                    .iter()
                    .map(|item| input_value(default_db, item))
                    .collect::<Result<Vec<_>, _>>()?;
                frames = cross_bind(&frames, |frame, out| {
                    for value in &values {
                        let mut next = frame.clone();
                        next.insert(var.clone(), value.clone());
                        out.push(next);
                    }
                });
            }
            (InSpec::Rel(vars), QInput::Edn(form)) => {
                let tuples = form.as_seq().ok_or_else(|| {
                    QueryError::Type(format!("relation input requires a vector, got {form}"))
                })?;
                let mut converted: Vec<Vec<Value>> = Vec::new();
                for tuple in tuples {
                    let items = tuple
                        .as_seq()
                        .ok_or_else(|| QueryError::Type("relation input requires tuples".into()))?;
                    if items.len() != vars.len() {
                        return Err(QueryError::Arity("relation input arity mismatch".into()));
                    }
                    converted.push(
                        items
                            .iter()
                            .map(|item| input_value(default_db, item))
                            .collect::<Result<_, _>>()?,
                    );
                }
                frames = cross_bind(&frames, |frame, out| {
                    for tuple in &converted {
                        let mut next = frame.clone();
                        for (var, value) in vars.iter().zip(tuple) {
                            next.insert(var.clone(), value.clone());
                        }
                        out.push(next);
                    }
                });
            }
            (spec, input) => {
                return Err(QueryError::Type(format!(
                    "input does not fit :in specification {spec:?}: {input:?}"
                )));
            }
        }
    }

    let mut ctx = ExecCtx::new(dbs, rules);
    if let Some(fuel) = options.fuel {
        ctx.set_fuel(fuel);
    }
    if let Some(extern_call) = options.extern_call.clone() {
        ctx.set_extern_call(extern_call);
    }
    let frames = exec::eval_clauses(&ctx, &query.wheres, frames, &mut Vec::new())?;

    // Project to find + with variables with set semantics.
    let elems = query.find.elems();
    let mut proj_vars: Vec<Var> = elems.iter().map(|elem| elem.var().clone()).collect();
    proj_vars.extend(query.with.iter().cloned());
    let mut rows: BTreeSet<Vec<Value>> = BTreeSet::new();
    for frame in frames {
        let row = proj_vars
            .iter()
            .map(|var| {
                frame
                    .get(var)
                    .cloned()
                    .ok_or_else(|| QueryError::Unbound(var.clone()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        rows.insert(row);
    }

    // Aggregate or project.
    let has_aggregate = elems
        .iter()
        .any(|elem| matches!(elem, FindElem::Aggregate(_)));
    let out_rows: Vec<Vec<Cell>> = if has_aggregate {
        let group_positions: Vec<usize> = elems
            .iter()
            .enumerate()
            .filter(|(_, elem)| !matches!(elem, FindElem::Aggregate(_)))
            .map(|(index, _)| index)
            .collect();
        let mut groups: BTreeMap<Vec<Value>, Vec<Vec<Value>>> = BTreeMap::new();
        for row in rows {
            let key = group_positions
                .iter()
                .map(|&index| row[index].clone())
                .collect();
            groups.entry(key).or_default().push(row);
        }
        let mut out = Vec::new();
        for group in groups.values() {
            let mut cells = Vec::with_capacity(elems.len());
            for (index, elem) in elems.iter().enumerate() {
                match elem {
                    FindElem::Aggregate(agg) => {
                        let values: Vec<Value> =
                            group.iter().map(|row| row[index].clone()).collect();
                        cells.push(Cell::Agg(aggregate::apply(&agg.op, agg.n, &values)?));
                    }
                    FindElem::Var(_) | FindElem::Pull(_, _) => {
                        cells.push(Cell::Val(group[0][index].clone(), (*elem).clone()));
                    }
                }
            }
            out.push(cells);
        }
        out
    } else {
        rows.into_iter()
            .map(|row| {
                elems
                    .iter()
                    .enumerate()
                    .map(|(index, elem)| Cell::Val(row[index].clone(), (*elem).clone()))
                    .collect()
            })
            .collect()
    };

    // Convert to EDN, applying pull expressions.
    let mut edn_rows: Vec<Vec<Edn>> = Vec::with_capacity(out_rows.len());
    for row in out_rows {
        let mut out = Vec::with_capacity(row.len());
        for cell in row {
            out.push(cell_to_edn(default_db, cell)?);
        }
        edn_rows.push(out);
    }
    edn_rows.sort();

    let result = match &query.find {
        FindSpec::Rel(_) => Edn::Vector(edn_rows.into_iter().map(Edn::Vector).collect()),
        FindSpec::Coll(_) => Edn::Vector(
            edn_rows
                .into_iter()
                .filter_map(|row| row.into_iter().next())
                .collect(),
        ),
        FindSpec::Tuple(_) => edn_rows.into_iter().next().map_or(Edn::Nil, Edn::Vector),
        FindSpec::Scalar(_) => edn_rows
            .into_iter()
            .next()
            .and_then(|row| row.into_iter().next())
            .unwrap_or(Edn::Nil),
    };
    Ok((
        result,
        ExecReport {
            datoms_scanned: ctx.scanned(),
        },
    ))
}

enum Cell {
    Val(Value, FindElem),
    Agg(AggOut),
}

fn cell_to_edn(default_db: Option<&Db>, cell: Cell) -> Result<Edn, QueryError> {
    let db = default_db.ok_or_else(|| QueryError::UnknownSource(ast::DEFAULT_SRC.into()))?;
    match cell {
        Cell::Val(value, FindElem::Pull(_, pattern)) => {
            let eid = to_entity(&value).ok_or_else(|| {
                QueryError::Type("pull requires an entity-valued variable".into())
            })?;
            pull::pull(db, &pattern, eid)
        }
        Cell::Val(value, _) | Cell::Agg(AggOut::Value(value)) => Ok(value_to_edn(db, &value)),
        Cell::Agg(AggOut::Coll(values)) => Ok(Edn::Vector(
            values.iter().map(|value| value_to_edn(db, value)).collect(),
        )),
        Cell::Agg(AggOut::Set(values)) => {
            let mut items: Vec<Edn> = values.iter().map(|value| value_to_edn(db, value)).collect();
            items.sort();
            items.dedup();
            Ok(Edn::Set(items))
        }
    }
}

fn cross_bind(frames: &[Frame], mut expand: impl FnMut(&Frame, &mut Vec<Frame>)) -> Vec<Frame> {
    let mut out = Vec::new();
    for frame in frames {
        expand(frame, &mut out);
    }
    out
}

/// Converts an input argument to a value, supporting scalars, tagged
/// entities/instants/uuids, and lookup refs.
fn input_value(db: Option<&Db>, form: &Edn) -> Result<Value, QueryError> {
    if let Some(value) = edn_to_value(db, form) {
        return Ok(value);
    }
    // Lookup ref `[attr value]` resolved against the default database.
    if let (Some(db), Edn::Vector(items)) = (db, form) {
        if let [attr_form, value_form] = items.as_slice() {
            if let Some(attr_kw) = attr_form.as_keyword() {
                let attr = db
                    .idents()
                    .entid(attr_kw)
                    .ok_or_else(|| QueryError::UnknownIdent(attr_kw.clone()))?;
                let value = edn_to_value(Some(db), value_form).ok_or_else(|| {
                    QueryError::Type(format!("bad lookup ref value {value_form}"))
                })?;
                let value = db.schema().get(attr).map_or(value.clone(), |meta| {
                    exec::coerce_for_type(value, meta.value_type)
                });
                return db
                    .lookup(attr, &value)
                    .map(Value::Ref)
                    .ok_or_else(|| QueryError::Type(format!("lookup ref {form} did not resolve")));
            }
        }
    }
    Err(QueryError::Type(format!("cannot convert input {form}")))
}
