//! Frame-based query executor.
//!
//! Execution is a fold of clause evaluations over a set of frames (partial
//! variable bindings). Pattern clauses pick a covering index per frame from
//! what is bound at that moment (see [`crate::plan::choose_index`]), so a
//! bound attribute can never degenerate into a full index scan. Rules are
//! evaluated bottom-up to a fixpoint with per-invocation memo tables.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};

use corium_core::{AttrId, EntityId, IndexOrder, Value, ValueType};
use corium_db::{Db, avet_covered, key_prefix};

use crate::QueryError;
use crate::ast::{BindTarget, Binding, Clause, Pattern, RuleDef, Term, Var};
use crate::builtins::{self, CallResult};
use crate::edn::Edn;
use crate::plan::{ScanChoice, choose_index, order_clauses};

/// A partial assignment of variables to values.
pub type Frame = BTreeMap<Var, Value>;

/// Keyword id used for keyword literals absent from the database's
/// interner: such literals can never equal a stored keyword value.
pub const UNKNOWN_KEYWORD: u64 = u64::MAX;

type RuleKey = (String, Vec<Option<Value>>);

#[derive(Default)]
struct RuleState {
    partial: BTreeMap<RuleKey, BTreeSet<Vec<Value>>>,
    changed: bool,
}

/// Execution context: database sources, rules, and counters.
pub struct ExecCtx<'a> {
    dbs: BTreeMap<String, &'a Db>,
    rules: BTreeMap<String, Vec<RuleDef>>,
    scanned: Cell<usize>,
    fuel: Cell<u64>,
    rule_state: RefCell<RuleState>,
}

impl<'a> ExecCtx<'a> {
    /// Creates a context over named database sources and rule definitions.
    #[must_use]
    pub fn new(dbs: BTreeMap<String, &'a Db>, rule_defs: Vec<RuleDef>) -> Self {
        let mut rules: BTreeMap<String, Vec<RuleDef>> = BTreeMap::new();
        for def in rule_defs {
            rules.entry(def.name.clone()).or_default().push(def);
        }
        Self {
            dbs,
            rules,
            scanned: Cell::new(0),
            fuel: Cell::new(u64::MAX),
            rule_state: RefCell::new(RuleState::default()),
        }
    }

    /// Limits the number of datoms the execution may touch.
    pub fn set_fuel(&self, fuel: u64) {
        self.fuel.set(fuel);
    }

    /// Datoms touched by pattern scans so far.
    #[must_use]
    pub fn scanned(&self) -> usize {
        self.scanned.get()
    }

    /// Resolves a database source by name.
    #[must_use]
    pub fn db(&self, src: &str) -> Option<&'a Db> {
        self.dbs.get(src).copied()
    }

    /// The default database source, if bound.
    #[must_use]
    pub fn default_db(&self) -> Option<&'a Db> {
        self.db(crate::ast::DEFAULT_SRC)
            .or_else(|| self.dbs.values().next().copied())
    }

    /// Resolves an attribute-position constant against a database.
    #[must_use]
    pub fn attr_of(&self, db: &Db, form: &Edn) -> Option<AttrId> {
        match form {
            Edn::Keyword(k) => db.idents().entid(k),
            Edn::Long(n) => u64::try_from(*n).ok().map(EntityId::from_raw),
            Edn::Tagged(tag, value) if tag == "eid" => match value.as_ref() {
                Edn::Long(n) => u64::try_from(*n).ok().map(EntityId::from_raw),
                _ => None,
            },
            _ => None,
        }
    }

    fn spend(&self, amount: u64) -> Result<(), QueryError> {
        let fuel = self.fuel.get();
        if fuel < amount {
            return Err(QueryError::FuelExhausted);
        }
        self.fuel.set(fuel - amount);
        Ok(())
    }
}

/// Converts a constant EDN form to an engine value, without attribute context.
///
/// Returns `None` for forms that cannot denote a storable value.
#[must_use]
pub fn const_value(db: &Db, form: &Edn) -> Option<Value> {
    crate::boundary::edn_to_value(Some(db), form)
}

/// Coerces a value toward an attribute's schema type where representations
/// legitimately overlap (longs standing for entity ids or instants).
#[must_use]
pub fn coerce_for_type(value: Value, value_type: ValueType) -> Value {
    match (&value, value_type) {
        (Value::Long(n), ValueType::Ref) => u64::try_from(*n)
            .map(|n| Value::Ref(EntityId::from_raw(n)))
            .unwrap_or(value),
        (Value::Long(n), ValueType::Instant) => Value::Instant(*n),
        #[allow(clippy::cast_precision_loss)]
        (Value::Long(n), ValueType::Double) => Value::Double(corium_core::TotalF64(*n as f64)),
        _ => value,
    }
}

/// Interprets a bound value as an entity id.
#[must_use]
pub fn to_entity(value: &Value) -> Option<EntityId> {
    match value {
        Value::Ref(e) => Some(*e),
        Value::Long(n) => u64::try_from(*n).ok().map(EntityId::from_raw),
        _ => None,
    }
}

/// Pattern-position match with entity/instant coercion between overlapping
/// representations. Distinct value types otherwise never match.
#[must_use]
pub fn value_match(actual: &Value, expected: &Value) -> bool {
    if actual == expected {
        return true;
    }
    match (actual, expected) {
        (Value::Ref(e), Value::Long(n)) | (Value::Long(n), Value::Ref(e)) => {
            u64::try_from(*n).is_ok_and(|n| e.raw() == n)
        }
        (Value::Instant(i), Value::Long(n)) | (Value::Long(n), Value::Instant(i)) => i == n,
        _ => false,
    }
}

/// Evaluates clauses over the given frames, in planner order.
///
/// # Errors
/// Propagates evaluation failures (unknown idents, unbound call arguments,
/// arity and type errors, fuel exhaustion).
pub fn eval_clauses(
    ctx: &ExecCtx<'_>,
    clauses: &[Clause],
    frames: Vec<Frame>,
    stack: &mut Vec<RuleKey>,
) -> Result<Vec<Frame>, QueryError> {
    let initially_bound: BTreeSet<Var> = frames
        .first()
        .map(|frame| frame.keys().cloned().collect())
        .unwrap_or_default();
    let order = order_clauses(clauses, &initially_bound, ctx);
    let mut frames = frames;
    for index in order {
        frames = eval_clause(ctx, &clauses[index], frames, stack)?;
        // Set semantics: identical frames carry no extra information.
        let set: BTreeSet<Frame> = frames.into_iter().collect();
        frames = set.into_iter().collect();
        if frames.is_empty() {
            return Ok(frames);
        }
    }
    Ok(frames)
}

fn eval_clause(
    ctx: &ExecCtx<'_>,
    clause: &Clause,
    frames: Vec<Frame>,
    stack: &mut Vec<RuleKey>,
) -> Result<Vec<Frame>, QueryError> {
    match clause {
        Clause::Pattern(pattern) => eval_pattern(ctx, pattern, frames),
        Clause::Pred { name, args } => eval_pred(ctx, name, args, frames),
        Clause::Fn {
            name,
            args,
            binding,
        } => eval_fn(ctx, name, args, binding, frames),
        Clause::Not {
            src: _,
            vars,
            clauses,
        } => {
            let mut kept = Vec::new();
            for frame in frames {
                let seed = match vars {
                    Some(vars) => restrict(&frame, vars),
                    None => frame.clone(),
                };
                let sub = eval_clauses(ctx, clauses, vec![seed], stack)?;
                if sub.is_empty() {
                    kept.push(frame);
                }
            }
            Ok(kept)
        }
        Clause::Or {
            src: _,
            vars,
            branches,
        } => {
            let mut out = Vec::new();
            for frame in frames {
                for branch in branches {
                    let seed = match vars {
                        Some(vars) => restrict(&frame, vars),
                        None => frame.clone(),
                    };
                    for result in eval_clauses(ctx, branch, vec![seed], stack)? {
                        let mut merged = frame.clone();
                        let mut consistent = true;
                        let exported: Vec<&Var> = match vars {
                            Some(vars) => vars.iter().collect(),
                            None => result.keys().collect(),
                        };
                        for var in exported {
                            if let Some(value) = result.get(var) {
                                match merged.get(var) {
                                    Some(existing) if !value_match(existing, value) => {
                                        consistent = false;
                                        break;
                                    }
                                    Some(_) => {}
                                    None => {
                                        merged.insert(var.clone(), value.clone());
                                    }
                                }
                            }
                        }
                        if consistent {
                            out.push(merged);
                        }
                    }
                }
            }
            Ok(out)
        }
        Clause::RuleCall { name, args } => eval_rule_call(ctx, name, args, frames, stack),
    }
}

fn restrict(frame: &Frame, vars: &[Var]) -> Frame {
    vars.iter()
        .filter_map(|var| frame.get(var).map(|value| (var.clone(), value.clone())))
        .collect()
}

/// A resolved pattern position: unbound, a concrete value, or provably
/// unmatchable (e.g. a lookup ref that resolves to nothing).
enum Spec {
    Free(Option<Var>),
    Bound(Value),
    NoMatch,
}

fn resolve_term(
    ctx: &ExecCtx<'_>,
    db: &Db,
    frame: &Frame,
    term: &Term,
    position: Position,
    attr: Option<AttrId>,
) -> Result<Spec, QueryError> {
    let coerce = |value: Value| -> Value {
        match (position, attr) {
            (Position::V, Some(a)) => db.schema().get(a).map_or_else(
                || value.clone(),
                |meta| coerce_for_type(value.clone(), meta.value_type),
            ),
            _ => value,
        }
    };
    match term {
        Term::Blank => Ok(Spec::Free(None)),
        Term::Var(var) => Ok(frame
            .get(var)
            .map_or(Spec::Free(Some(var.clone())), |value| {
                Spec::Bound(coerce(value.clone()))
            })),
        Term::Const(form) => match position {
            Position::E | Position::Tx => match entity_const(db, form)? {
                Some(e) => Ok(Spec::Bound(Value::Ref(e))),
                None => Ok(Spec::NoMatch),
            },
            Position::A => match ctx.attr_of(db, form) {
                Some(a) => Ok(Spec::Bound(Value::Ref(a))),
                None => match form {
                    Edn::Keyword(k) => Err(QueryError::UnknownIdent(k.clone())),
                    _ => Ok(Spec::NoMatch),
                },
            },
            Position::Added => match form {
                Edn::Bool(b) => Ok(Spec::Bound(Value::Bool(*b))),
                _ => Ok(Spec::NoMatch),
            },
            Position::V => match const_value(db, form) {
                Some(value) => Ok(Spec::Bound(coerce(value))),
                None => match entity_const(db, form)? {
                    Some(e) => Ok(Spec::Bound(Value::Ref(e))),
                    None => Ok(Spec::NoMatch),
                },
            },
        },
    }
}

/// Resolves an entity-position constant: id, tagged id, ident keyword, or
/// lookup ref `[attr value]`.
fn entity_const(db: &Db, form: &Edn) -> Result<Option<EntityId>, QueryError> {
    match form {
        Edn::Long(_) | Edn::Tagged(_, _) => match const_value(db, form) {
            Some(Value::Ref(e)) => Ok(Some(e)),
            Some(Value::Long(n)) => Ok(u64::try_from(n).ok().map(EntityId::from_raw)),
            _ => Ok(None),
        },
        Edn::Keyword(k) => Ok(db.idents().entid(k)),
        Edn::Vector(items) => {
            let [attr_form, value_form] = items.as_slice() else {
                return Ok(None);
            };
            let Some(attr_kw) = attr_form.as_keyword() else {
                return Ok(None);
            };
            let attr = db
                .idents()
                .entid(attr_kw)
                .ok_or_else(|| QueryError::UnknownIdent(attr_kw.clone()))?;
            let Some(value) = const_value(db, value_form) else {
                return Ok(None);
            };
            let value = db.schema().get(attr).map_or(value.clone(), |meta| {
                coerce_for_type(value, meta.value_type)
            });
            Ok(db.lookup(attr, &value))
        }
        _ => Ok(None),
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Position {
    E,
    A,
    V,
    Tx,
    Added,
}

#[allow(clippy::too_many_lines)]
fn eval_pattern(
    ctx: &ExecCtx<'_>,
    pattern: &Pattern,
    frames: Vec<Frame>,
) -> Result<Vec<Frame>, QueryError> {
    let db = ctx
        .db(&pattern.src)
        .ok_or_else(|| QueryError::UnknownSource(pattern.src.clone()))?;
    let mut out = Vec::new();
    for frame in frames {
        // Attribute first: value coercion and index choice depend on it.
        let a_spec = resolve_term(ctx, db, &frame, &pattern.a, Position::A, None)?;
        let attr = match &a_spec {
            Spec::Bound(Value::Ref(a)) => Some(*a),
            Spec::Bound(other) => to_entity(other),
            _ => None,
        };
        let e_spec = resolve_term(ctx, db, &frame, &pattern.e, Position::E, None)?;
        let v_spec = resolve_term(ctx, db, &frame, &pattern.v, Position::V, attr)?;
        let tx_spec = resolve_term(ctx, db, &frame, &pattern.tx, Position::Tx, None)?;
        let added_spec = resolve_term(ctx, db, &frame, &pattern.added, Position::Added, None)?;
        if [&a_spec, &e_spec, &v_spec, &tx_spec, &added_spec]
            .iter()
            .any(|spec| matches!(spec, Spec::NoMatch))
        {
            continue;
        }
        let e = match &e_spec {
            Spec::Bound(value) => to_entity(value),
            _ => None,
        };
        let v = match &v_spec {
            Spec::Bound(value) => Some(value.clone()),
            _ => None,
        };
        let choice: ScanChoice = choose_index(
            e.is_some(),
            attr.is_some(),
            v.is_some(),
            attr.is_some_and(|a| avet_covered(db.schema(), a)),
            matches!(v, Some(Value::Ref(_))),
        );
        let prefix = match choice.order {
            IndexOrder::Eavt => key_prefix(
                IndexOrder::Eavt,
                e,
                if choice.prefix_len >= 2 { attr } else { None },
                if choice.prefix_len >= 3 {
                    v.as_ref()
                } else {
                    None
                },
            ),
            IndexOrder::Aevt => key_prefix(IndexOrder::Aevt, None, attr, None),
            IndexOrder::Avet => key_prefix(IndexOrder::Avet, None, attr, v.as_ref()),
            IndexOrder::Vaet => key_prefix(IndexOrder::Vaet, None, None, v.as_ref()),
        };
        for datom in db.datoms_prefix(choice.order, &prefix) {
            ctx.spend(1)?;
            ctx.scanned.set(ctx.scanned.get() + 1);
            let fields = [
                (Position::E, Value::Ref(datom.e)),
                (Position::A, Value::Ref(datom.a)),
                (Position::V, datom.v.clone()),
                (Position::Tx, Value::Ref(datom.tx)),
                (Position::Added, Value::Bool(datom.added)),
            ];
            let specs = [&e_spec, &a_spec, &v_spec, &tx_spec, &added_spec];
            let mut extended = frame.clone();
            let mut matched = true;
            for ((_, actual), spec) in fields.into_iter().zip(specs) {
                match spec {
                    Spec::Bound(expected) => {
                        if !value_match(&actual, expected) {
                            matched = false;
                            break;
                        }
                    }
                    Spec::Free(Some(var)) => match extended.get(var) {
                        Some(existing) => {
                            if !value_match(&actual, existing) {
                                matched = false;
                                break;
                            }
                        }
                        None => {
                            extended.insert(var.clone(), actual);
                        }
                    },
                    Spec::Free(None) | Spec::NoMatch => {}
                }
            }
            if matched {
                out.push(extended);
            }
        }
    }
    Ok(out)
}

/// Resolves call arguments to values; `Err` inside the option marks an
/// argument that is a variable still unbound.
fn call_args(ctx: &ExecCtx<'_>, frame: &Frame, args: &[Term]) -> Result<Vec<Value>, QueryError> {
    let db = ctx.default_db();
    args.iter()
        .map(|term| match term {
            Term::Var(var) => frame
                .get(var)
                .cloned()
                .ok_or_else(|| QueryError::Unbound(var.clone())),
            Term::Blank => Err(QueryError::Parse("call arguments cannot be _".into())),
            Term::Const(form) => db
                .and_then(|db| const_value(db, form))
                .ok_or_else(|| QueryError::Type(format!("bad call argument {form}"))),
        })
        .collect()
}

fn eval_pred(
    ctx: &ExecCtx<'_>,
    name: &str,
    args: &[Term],
    frames: Vec<Frame>,
) -> Result<Vec<Frame>, QueryError> {
    if name == "missing?" {
        return eval_missing(ctx, args, frames);
    }
    let mut out = Vec::new();
    for frame in frames {
        let values = call_args(ctx, &frame, args)?;
        match builtins::call(name, &values)? {
            CallResult::Test(true) => out.push(frame),
            CallResult::Test(false) => {}
            _ => {
                return Err(QueryError::Type(format!("{name} is not a predicate")));
            }
        }
    }
    Ok(out)
}

fn eval_fn(
    ctx: &ExecCtx<'_>,
    name: &str,
    args: &[Term],
    binding: &Binding,
    frames: Vec<Frame>,
) -> Result<Vec<Frame>, QueryError> {
    if name == "ground" {
        return eval_ground(ctx, args, binding, frames);
    }
    if name == "get-else" {
        return eval_get_else(ctx, args, binding, frames);
    }
    let mut out = Vec::new();
    for frame in frames {
        let values = call_args(ctx, &frame, args)?;
        let result = builtins::call(name, &values)?;
        bind_result(&frame, binding, result, &mut out)?;
    }
    Ok(out)
}

fn bind_result(
    frame: &Frame,
    binding: &Binding,
    result: CallResult,
    out: &mut Vec<Frame>,
) -> Result<(), QueryError> {
    let scalar = |value: &Value, var: &Var, frame: &Frame| -> Option<Frame> {
        let mut next = frame.clone();
        match next.get(var) {
            Some(existing) if !value_match(existing, value) => None,
            Some(_) => Some(next),
            None => {
                next.insert(var.clone(), value.clone());
                Some(next)
            }
        }
    };
    let bind_tuple = |values: &[Value], targets: &[BindTarget], frame: &Frame| -> Option<Frame> {
        if values.len() != targets.len() {
            return None;
        }
        let mut next = frame.clone();
        for (value, target) in values.iter().zip(targets) {
            match target {
                BindTarget::Blank => {}
                BindTarget::Var(var) => match next.get(var) {
                    Some(existing) if !value_match(existing, value) => return None,
                    Some(_) => {}
                    None => {
                        next.insert(var.clone(), value.clone());
                    }
                },
            }
        }
        Some(next)
    };
    match (binding, result) {
        (Binding::Scalar(var), CallResult::Scalar(value)) => {
            out.extend(scalar(&value, var, frame));
        }
        (Binding::Scalar(var), CallResult::Test(flag)) => {
            out.extend(scalar(&Value::Bool(flag), var, frame));
        }
        (Binding::Coll(target), CallResult::Coll(values)) => match target {
            BindTarget::Blank => {
                if !values.is_empty() {
                    out.push(frame.clone());
                }
            }
            BindTarget::Var(var) => {
                for value in values {
                    out.extend(scalar(&value, var, frame));
                }
            }
        },
        (Binding::Tuple(targets), CallResult::Coll(values)) => {
            out.extend(bind_tuple(&values, targets, frame));
        }
        (Binding::Rel(targets), CallResult::Rel(rows)) => {
            for row in rows {
                out.extend(bind_tuple(&row, targets, frame));
            }
        }
        (_, result) => {
            return Err(QueryError::Type(format!(
                "binding form does not fit result {result:?}"
            )));
        }
    }
    Ok(())
}

/// `[(ground <const>) binding]`: converts a constant EDN form per binding shape.
fn eval_ground(
    ctx: &ExecCtx<'_>,
    args: &[Term],
    binding: &Binding,
    frames: Vec<Frame>,
) -> Result<Vec<Frame>, QueryError> {
    let db = ctx
        .default_db()
        .ok_or_else(|| QueryError::UnknownSource("$".into()))?;
    let [Term::Const(form)] = args else {
        return Err(QueryError::Arity("ground takes one constant".into()));
    };
    let value_of = |form: &Edn| -> Result<Value, QueryError> {
        const_value(db, form).ok_or_else(|| QueryError::Type(format!("cannot ground {form}")))
    };
    let result = match (binding, form) {
        (Binding::Coll(_) | Binding::Tuple(_), Edn::Vector(items) | Edn::List(items)) => {
            CallResult::Coll(items.iter().map(value_of).collect::<Result<_, _>>()?)
        }
        (Binding::Rel(_), Edn::Vector(rows) | Edn::List(rows)) => CallResult::Rel(
            rows.iter()
                .map(|row| match row {
                    Edn::Vector(items) | Edn::List(items) => {
                        items.iter().map(value_of).collect::<Result<Vec<_>, _>>()
                    }
                    _ => Err(QueryError::Type("ground relation requires tuples".into())),
                })
                .collect::<Result<_, _>>()?,
        ),
        (Binding::Scalar(_), form) => CallResult::Scalar(value_of(form)?),
        _ => return Err(QueryError::Type(format!("cannot ground {form}"))),
    };
    let mut out = Vec::new();
    for frame in frames {
        bind_result(&frame, binding, result.clone(), &mut out)?;
    }
    Ok(out)
}

/// `[(get-else $src ?e attr default) ?v]`.
fn eval_get_else(
    ctx: &ExecCtx<'_>,
    args: &[Term],
    binding: &Binding,
    frames: Vec<Frame>,
) -> Result<Vec<Frame>, QueryError> {
    let (db, rest) = db_context_args(ctx, args)?;
    let [entity_term, attr_term, default_term] = rest else {
        return Err(QueryError::Arity(
            "get-else takes a source, entity, attribute, and default".into(),
        ));
    };
    let attr = attr_const(ctx, db, attr_term)?;
    let value_type = db.schema().get(attr).map(|meta| meta.value_type);
    let mut out = Vec::new();
    for frame in frames {
        let entity = entity_arg(db, &frame, entity_term)?;
        let value = entity
            .and_then(|e| db.values(e, attr).into_iter().next())
            .map_or_else(
                || match default_term {
                    Term::Const(form) => {
                        let value = const_value(db, form).ok_or_else(|| {
                            QueryError::Type(format!("bad get-else default {form}"))
                        })?;
                        Ok(match value_type {
                            Some(t) => coerce_for_type(value, t),
                            None => value,
                        })
                    }
                    Term::Var(var) => frame
                        .get(var)
                        .cloned()
                        .ok_or_else(|| QueryError::Unbound(var.clone())),
                    Term::Blank => Err(QueryError::Parse("get-else default cannot be _".into())),
                },
                Ok,
            )?;
        bind_result(&frame, binding, CallResult::Scalar(value), &mut out)?;
    }
    Ok(out)
}

/// `[(missing? $src ?e attr)]`.
fn eval_missing(
    ctx: &ExecCtx<'_>,
    args: &[Term],
    frames: Vec<Frame>,
) -> Result<Vec<Frame>, QueryError> {
    let (db, rest) = db_context_args(ctx, args)?;
    let [entity_term, attr_term] = rest else {
        return Err(QueryError::Arity(
            "missing? takes a source, entity, and attribute".into(),
        ));
    };
    let attr = attr_const(ctx, db, attr_term)?;
    let mut out = Vec::new();
    for frame in frames {
        let entity = entity_arg(db, &frame, entity_term)?;
        let missing = entity.is_none_or(|e| db.values(e, attr).is_empty());
        if missing {
            out.push(frame);
        }
    }
    Ok(out)
}

fn db_context_args<'a, 'b>(
    ctx: &'b ExecCtx<'a>,
    args: &'b [Term],
) -> Result<(&'a Db, &'b [Term]), QueryError> {
    match args.split_first() {
        Some((Term::Const(Edn::Symbol(src)), rest)) if src.starts_with('$') => {
            let db = ctx
                .db(src)
                .ok_or_else(|| QueryError::UnknownSource(src.clone()))?;
            Ok((db, rest))
        }
        _ => {
            let db = ctx
                .default_db()
                .ok_or_else(|| QueryError::UnknownSource("$".into()))?;
            Ok((db, args))
        }
    }
}

fn attr_const(ctx: &ExecCtx<'_>, db: &Db, term: &Term) -> Result<AttrId, QueryError> {
    match term {
        Term::Const(form) => ctx.attr_of(db, form).ok_or_else(|| match form {
            Edn::Keyword(k) => QueryError::UnknownIdent(k.clone()),
            _ => QueryError::Type(format!("bad attribute argument {form}")),
        }),
        _ => Err(QueryError::Type(
            "attribute argument must be a constant".into(),
        )),
    }
}

fn entity_arg(db: &Db, frame: &Frame, term: &Term) -> Result<Option<EntityId>, QueryError> {
    match term {
        Term::Var(var) => {
            let value = frame
                .get(var)
                .ok_or_else(|| QueryError::Unbound(var.clone()))?;
            Ok(to_entity(value))
        }
        Term::Const(form) => entity_const(db, form),
        Term::Blank => Err(QueryError::Parse("entity argument cannot be _".into())),
    }
}

fn eval_rule_call(
    ctx: &ExecCtx<'_>,
    name: &str,
    args: &[Term],
    frames: Vec<Frame>,
    stack: &mut Vec<RuleKey>,
) -> Result<Vec<Frame>, QueryError> {
    let db = ctx.default_db();
    let mut out = Vec::new();
    for frame in frames {
        let mut key_args: Vec<Option<Value>> = Vec::with_capacity(args.len());
        for term in args {
            key_args.push(match term {
                Term::Var(var) => frame.get(var).cloned(),
                Term::Blank => None,
                Term::Const(form) => Some(
                    db.and_then(|db| const_value(db, form))
                        .ok_or_else(|| QueryError::Type(format!("bad rule argument {form}")))?,
                ),
            });
        }
        let tuples = rule_tuples(ctx, name, key_args, stack)?;
        for tuple in tuples {
            let mut extended = frame.clone();
            let mut consistent = true;
            for (term, value) in args.iter().zip(&tuple) {
                match term {
                    // Blanks bind nothing; constants were seeded into the key.
                    Term::Blank | Term::Const(_) => {}
                    Term::Var(var) => match extended.get(var) {
                        Some(existing) if !value_match(existing, value) => {
                            consistent = false;
                            break;
                        }
                        Some(_) => {}
                        None => {
                            extended.insert(var.clone(), value.clone());
                        }
                    },
                }
            }
            if consistent {
                out.push(extended);
            }
        }
    }
    Ok(out)
}

/// Solves a rule invocation to its tuple set.
///
/// Bottom-up evaluation with per-invocation memo tables: tables grow
/// monotonically; a top-level invocation iterates to a global fixpoint,
/// while invocations nested inside rule bodies read the tables as they
/// stand (the enclosing loop drives them to closure).
fn rule_tuples(
    ctx: &ExecCtx<'_>,
    name: &str,
    key_args: Vec<Option<Value>>,
    stack: &mut Vec<RuleKey>,
) -> Result<BTreeSet<Vec<Value>>, QueryError> {
    let key: RuleKey = (name.to_owned(), key_args);
    if stack.is_empty() {
        loop {
            ctx.rule_state.borrow_mut().changed = false;
            eval_rule_once(ctx, &key, stack)?;
            if !ctx.rule_state.borrow().changed {
                break;
            }
            ctx.spend(1)?;
        }
    } else {
        eval_rule_once(ctx, &key, stack)?;
    }
    Ok(ctx
        .rule_state
        .borrow()
        .partial
        .get(&key)
        .cloned()
        .unwrap_or_default())
}

fn eval_rule_once(
    ctx: &ExecCtx<'_>,
    key: &RuleKey,
    stack: &mut Vec<RuleKey>,
) -> Result<(), QueryError> {
    if stack.contains(key) {
        return Ok(());
    }
    let defs = ctx
        .rules
        .get(&key.0)
        .ok_or_else(|| QueryError::Unsupported(format!("unknown rule {}", key.0)))?
        .clone();
    stack.push(key.clone());
    let result = (|| -> Result<(), QueryError> {
        for def in &defs {
            let head: Vec<&Var> = def.head_vars();
            if head.len() != key.1.len() {
                return Err(QueryError::Arity(format!(
                    "rule {} expects {} arguments",
                    key.0,
                    head.len()
                )));
            }
            for (index, _) in def.required.iter().enumerate() {
                if key.1[index].is_none() {
                    return Err(QueryError::Unbound(format!(
                        "rule {} requires its first {} arguments bound",
                        key.0,
                        def.required.len()
                    )));
                }
            }
            let mut seed = Frame::new();
            for (var, value) in head.iter().zip(&key.1) {
                if let Some(value) = value {
                    seed.insert((*var).clone(), value.clone());
                }
            }
            let frames = eval_clauses(ctx, &def.clauses, vec![seed], stack)?;
            for frame in frames {
                let tuple = head
                    .iter()
                    .map(|var| {
                        frame
                            .get(*var)
                            .cloned()
                            .ok_or_else(|| QueryError::Unbound((*var).clone()))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let mut state = ctx.rule_state.borrow_mut();
                if state.partial.entry(key.clone()).or_default().insert(tuple) {
                    state.changed = true;
                }
            }
        }
        Ok(())
    })();
    stack.pop();
    result
}
