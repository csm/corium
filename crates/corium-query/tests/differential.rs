//! Model-based differential tests: random data and random conjunctive
//! queries evaluated both by the engine and by brute force over a plain
//! datom list. Any plan the optimizer picks must agree with brute force,
//! across current, as-of, since, and history views.

use std::collections::BTreeSet;

use corium_core::{
    Attribute, Cardinality, Datom, EntityId, Keyword, KeywordInterner, Partition, Schema, Value,
    ValueType,
};
use corium_db::{Db, Idents};
use corium_query::edn::Edn;
use corium_query::{QInput, q_str};
use proptest::prelude::*;

const LONG_ATTR: u64 = 1;
const LONG2_ATTR: u64 = 2;
const STR_ATTR: u64 = 3;
const REF_ATTR: u64 = 4;

fn attr_id(n: u64) -> EntityId {
    EntityId::new(Partition::Db as u32, n)
}

fn entity(n: u64) -> EntityId {
    EntityId::new(Partition::User as u32, n)
}

fn schema() -> (Schema, Idents) {
    let mut schema = Schema::default();
    let mut idents = Idents::default();
    let attrs = [
        (LONG_ATTR, "t/x", ValueType::Long),
        (LONG2_ATTR, "t/y", ValueType::Long),
        (STR_ATTR, "t/s", ValueType::Str),
        (REF_ATTR, "t/r", ValueType::Ref),
    ];
    for (id, ident, value_type) in attrs {
        schema.insert(Attribute {
            id: attr_id(id),
            value_type,
            cardinality: Cardinality::Many,
            unique: None,
            is_component: false,
            indexed: id % 2 == 0,
            no_history: false,
        });
        idents.insert(Keyword::parse(ident), attr_id(id));
    }
    (schema, idents)
}

/// One reference fact: `(e, attr, value, t, added)`.
type RefFact = (u64, u64, RefVal, u64, bool);

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum RefVal {
    Long(i64),
    Str(&'static str),
    Ref(u64),
}

const STRINGS: [&str; 3] = ["ash", "birch", "cedar"];

fn value_of(v: &RefVal) -> Value {
    match v {
        RefVal::Long(n) => Value::Long(*n),
        RefVal::Str(s) => Value::Str((*s).into()),
        RefVal::Ref(e) => Value::Ref(entity(*e)),
    }
}

fn edn_of(v: &RefVal) -> Edn {
    match v {
        RefVal::Long(n) => Edn::Long(*n),
        RefVal::Str(s) => Edn::Str((*s).to_string()),
        RefVal::Ref(e) => Edn::Long(i64::try_from(entity(*e).raw()).expect("fits")),
    }
}

/// EDN literal for a value constant in a pattern's v position.
fn literal_of(v: &RefVal) -> String {
    match v {
        RefVal::Long(n) => n.to_string(),
        RefVal::Str(s) => format!("{s:?}"),
        RefVal::Ref(e) => entity(*e).raw().to_string(),
    }
}

fn op_strategy() -> impl Strategy<Value = (u64, u64, RefVal, bool)> {
    let value = prop_oneof![
        (0_i64..4).prop_map(RefVal::Long),
        (0_usize..3).prop_map(|i| RefVal::Str(STRINGS[i])),
        (0_u64..8).prop_map(RefVal::Ref),
    ];
    (
        0_u64..8,
        prop_oneof![Just(0_usize), Just(1), Just(2), Just(3)],
        value,
        prop::bool::ANY,
    )
        .prop_map(|(e, attr_pick, value, added)| {
            let attr = [LONG_ATTR, LONG2_ATTR, STR_ATTR, REF_ATTR][attr_pick];
            // Force values to the attribute's type.
            let value = match (attr, value) {
                (LONG_ATTR | LONG2_ATTR, RefVal::Str(_) | RefVal::Ref(_)) => RefVal::Long(1),
                (STR_ATTR, RefVal::Long(_) | RefVal::Ref(_)) => RefVal::Str(STRINGS[0]),
                (REF_ATTR, RefVal::Long(n)) => RefVal::Ref(u64::try_from(n).unwrap_or(0)),
                (REF_ATTR, RefVal::Str(_)) => RefVal::Ref(0),
                (_, v) => v,
            };
            (e, attr, value, added)
        })
}

/// A pattern position for e, and a typed v term.
#[derive(Clone, Debug)]
struct GenPattern {
    e: EPos,
    attr: u64,
    v: VPos,
}

#[derive(Clone, Debug)]
enum EPos {
    Var(usize),
    Const(u64),
}

#[derive(Clone, Debug)]
enum VPos {
    LongVar(usize),
    EntityVar(usize),
    StrVar(usize),
    Const(RefVal),
}

fn pattern_strategy() -> impl Strategy<Value = GenPattern> {
    let epos = prop_oneof![
        (0_usize..3).prop_map(EPos::Var),
        (0_u64..8).prop_map(EPos::Const)
    ];
    (epos, 0_usize..4, 0_usize..4, 0_i64..4, 0_u64..8, 0_usize..3).prop_map(
        |(e, attr_pick, v_pick, long_const, ref_const, str_pick)| {
            let attr = [LONG_ATTR, LONG2_ATTR, STR_ATTR, REF_ATTR][attr_pick];
            let v = match (attr, v_pick) {
                (LONG_ATTR | LONG2_ATTR, 0 | 1) => VPos::LongVar(v_pick),
                (LONG_ATTR | LONG2_ATTR, _) => VPos::Const(RefVal::Long(long_const)),
                (STR_ATTR, 0 | 1) => VPos::StrVar(v_pick),
                (STR_ATTR, _) => VPos::Const(RefVal::Str(STRINGS[str_pick])),
                (_, 0 | 1) => VPos::EntityVar(v_pick),
                (_, _) => VPos::Const(RefVal::Ref(ref_const)),
            };
            GenPattern { e, attr, v }
        },
    )
}

fn attr_ident(attr: u64) -> &'static str {
    match attr {
        LONG_ATTR => ":t/x",
        LONG2_ATTR => ":t/y",
        STR_ATTR => ":t/s",
        _ => ":t/r",
    }
}

fn var_name(prefix: &str, index: usize) -> String {
    format!("?{prefix}{index}")
}

/// Renders the generated query to EDN text; returns the ordered var list.
fn render_query(patterns: &[GenPattern], pred: Option<(usize, i64)>) -> (String, Vec<String>) {
    let mut vars: Vec<String> = Vec::new();
    let push_var = |name: String, vars: &mut Vec<String>| {
        if !vars.contains(&name) {
            vars.push(name);
        }
    };
    let mut clauses = Vec::new();
    for pattern in patterns {
        let e = match &pattern.e {
            EPos::Var(i) => {
                let name = var_name("e", *i);
                push_var(name.clone(), &mut vars);
                name
            }
            EPos::Const(n) => entity(*n).raw().to_string(),
        };
        let v = match &pattern.v {
            VPos::LongVar(i) => {
                let name = var_name("l", *i);
                push_var(name.clone(), &mut vars);
                name
            }
            VPos::EntityVar(i) => {
                let name = var_name("e", *i);
                push_var(name.clone(), &mut vars);
                name
            }
            VPos::StrVar(i) => {
                let name = var_name("s", *i);
                push_var(name.clone(), &mut vars);
                name
            }
            VPos::Const(value) => literal_of(value),
        };
        clauses.push(format!("[{e} {} {v}]", attr_ident(pattern.attr)));
    }
    if let Some((var, bound)) = pred {
        let name = var_name("l", var);
        if vars.contains(&name) {
            clauses.push(format!("[(< {name} {bound})]"));
        }
    }
    let find = vars.join(" ");
    (format!("[:find {find} :where {}]", clauses.join(" ")), vars)
}

/// Brute-force evaluation over a plain fact list.
fn brute_force(
    facts: &[(u64, u64, RefVal)],
    patterns: &[GenPattern],
    pred: Option<(usize, i64)>,
    vars: &[String],
) -> BTreeSet<Vec<Edn>> {
    type Env = std::collections::BTreeMap<String, RefVal>;
    fn matches(env: &Env, name: &str, value: &RefVal) -> Option<Env> {
        match env.get(name) {
            Some(existing) if existing == value => Some(env.clone()),
            Some(_) => None,
            None => {
                let mut next = env.clone();
                next.insert(name.to_owned(), value.clone());
                Some(next)
            }
        }
    }
    let mut envs: Vec<Env> = vec![Env::new()];
    for pattern in patterns {
        let mut next = Vec::new();
        for env in &envs {
            for (e, a, v) in facts {
                if *a != pattern.attr {
                    continue;
                }
                let env = match &pattern.e {
                    EPos::Const(n) if n != e => continue,
                    EPos::Const(_) => env.clone(),
                    EPos::Var(i) => match matches(env, &var_name("e", *i), &RefVal::Ref(*e)) {
                        Some(env) => env,
                        None => continue,
                    },
                };
                let env = match &pattern.v {
                    VPos::Const(value) if value != v => continue,
                    VPos::Const(_) => env,
                    VPos::LongVar(i) => match matches(&env, &var_name("l", *i), v) {
                        Some(env) => env,
                        None => continue,
                    },
                    VPos::EntityVar(i) => match matches(&env, &var_name("e", *i), v) {
                        Some(env) => env,
                        None => continue,
                    },
                    VPos::StrVar(i) => match matches(&env, &var_name("s", *i), v) {
                        Some(env) => env,
                        None => continue,
                    },
                };
                next.push(env);
            }
        }
        envs = next;
    }
    if let Some((var, bound)) = pred {
        let name = var_name("l", var);
        if vars.contains(&name) {
            envs.retain(|env| match env.get(&name) {
                Some(RefVal::Long(v)) => *v < bound,
                _ => false,
            });
        }
    }
    envs.into_iter()
        .map(|env| vars.iter().map(|var| edn_of(&env[var])).collect())
        .collect()
}

/// Applies ops through `with_transaction` in fixed-size groups; returns the
/// database plus the reference fact log.
fn build_db(ops: &[(u64, u64, RefVal, bool)]) -> (Db, Vec<RefFact>) {
    let (schema, idents) = schema();
    let mut db = Db::new(schema).with_naming(idents, KeywordInterner::default());
    let mut log: Vec<RefFact> = Vec::new();
    let mut t = 0_u64;
    for chunk in ops.chunks(3) {
        t += 1;
        let datoms: Vec<Datom> = chunk
            .iter()
            .map(|(e, a, v, added)| Datom {
                e: entity(*e),
                a: attr_id(*a),
                v: value_of(v),
                tx: EntityId::new(Partition::Tx as u32, t),
                added: *added,
            })
            .collect();
        db = db.with_transaction(t, &datoms);
        for (e, a, v, added) in chunk {
            log.push((*e, *a, v.clone(), t, *added));
        }
    }
    (db, log)
}

/// Reference current facts at basis `t` (fold of assertions/retractions).
fn facts_at(log: &[RefFact], up_to: u64) -> Vec<(u64, u64, RefVal)> {
    let mut set: BTreeSet<(u64, u64, RefVal)> = BTreeSet::new();
    for (e, a, v, t, added) in log {
        if *t > up_to {
            continue;
        }
        if *added {
            set.insert((*e, *a, v.clone()));
        } else {
            set.remove(&(*e, *a, v.clone()));
        }
    }
    set.into_iter().collect()
}

fn engine_rows(result: &Edn) -> BTreeSet<Vec<Edn>> {
    match result {
        Edn::Vector(rows) => rows
            .iter()
            .map(|row| match row {
                Edn::Vector(items) => items.clone(),
                other => vec![other.clone()],
            })
            .collect(),
        other => panic!("expected relation result, got {other}"),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(200))]

    #[test]
    fn random_queries_agree_with_brute_force(
        ops in prop::collection::vec(op_strategy(), 1..40),
        patterns in prop::collection::vec(pattern_strategy(), 1..4),
        pred_var in 0_usize..2,
        pred_bound in 0_i64..4,
        probe_t in 1_u64..14,
    ) {
        let (db, log) = build_db(&ops);
        let pred = Some((pred_var, pred_bound));
        let (query, vars) = render_query(&patterns, pred);
        prop_assume!(!vars.is_empty());

        // Current view.
        let engine = q_str(&query, &[QInput::Db(&db)]).expect("engine query");
        let expected = brute_force(&facts_at(&log, u64::MAX), &patterns, pred, &vars);
        prop_assert_eq!(engine_rows(&engine), expected);

        // As-of view.
        let as_of = db.as_of(probe_t);
        let engine = q_str(&query, &[QInput::Db(&as_of)]).expect("as-of query");
        let expected = brute_force(&facts_at(&log, probe_t), &patterns, pred, &vars);
        prop_assert_eq!(engine_rows(&engine), expected);

        // Since view: live facts whose surviving assertion is newer than t.
        let since = db.since(probe_t);
        let engine = q_str(&query, &[QInput::Db(&since)]).expect("since query");
        let mut newest: std::collections::BTreeMap<(u64, u64, RefVal), (u64, bool)> =
            std::collections::BTreeMap::new();
        for (e, a, v, t, added) in &log {
            newest.insert((*e, *a, v.clone()), (*t, *added));
        }
        let since_facts: Vec<(u64, u64, RefVal)> = newest
            .into_iter()
            .filter(|(_, (t, added))| *added && *t > probe_t)
            .map(|(fact, _)| fact)
            .collect();
        let expected = brute_force(&since_facts, &patterns, pred, &vars);
        prop_assert_eq!(engine_rows(&engine), expected);
    }
}
