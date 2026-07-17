//! Datomic-semantics conformance suite.
//!
//! Runs every vector in `tests/conformance/*.edn` (repo root). Each vector
//! is a map: `:name`, `:schema` (Datomic-style attribute maps), `:tx`
//! (a vector of transactions, each a vector of list/map forms), optional
//! `:view` (`:history`, `{:as-of t}`, `{:since t}`), then either `:query`
//! (+ optional `:args`) or `:pull` (`{:eid … :pattern …}`), and
//! `:expected` (or `:expect-error true`).
//!
//! `#tempid "name"` in queries/args/expected resolves to the entity
//! allocated for that tempid; `#tx t` to the transaction entity id.

use std::collections::BTreeMap;
use std::sync::Arc;

use corium_core::{
    Attribute, Cardinality, EntityId, KeywordInterner, Partition, Schema, Unique, Value, ValueType,
};
use corium_db::{Db, Idents};
use corium_log::MemoryLog;
use corium_query::edn::{Edn, read_all};
use corium_query::{QInput, QueryCache, exec, pull, run};
use corium_transactor::EmbeddedTransactor;
use corium_tx::{EntityRef, TxItem, TxOp};

fn kw(text: &str) -> Edn {
    Edn::keyword(text)
}

/// Walks a form, interning every keyword so keyword values are resolvable.
fn intern_all(form: &Edn, interner: &mut KeywordInterner) {
    match form {
        Edn::Keyword(k) => {
            interner.intern(k.clone());
        }
        Edn::List(items) | Edn::Vector(items) | Edn::Set(items) => {
            for item in items {
                intern_all(item, interner);
            }
        }
        Edn::Map(pairs) => {
            for (key, value) in pairs {
                intern_all(key, interner);
                intern_all(value, interner);
            }
        }
        Edn::Tagged(_, value) => intern_all(value, interner),
        _ => {}
    }
}

fn parse_schema(forms: &[Edn]) -> (Schema, Idents) {
    let mut schema = Schema::default();
    let mut idents = Idents::default();
    for (index, form) in forms.iter().enumerate() {
        let id = EntityId::new(
            Partition::Db as u32,
            100 + u64::try_from(index).expect("fits"),
        );
        let ident = form
            .get(&kw("db/ident"))
            .and_then(Edn::as_keyword)
            .expect(":db/ident required")
            .clone();
        let value_type = match form
            .get(&kw("db/valueType"))
            .and_then(Edn::as_keyword)
            .map(|k| k.name.clone())
            .expect(":db/valueType required")
            .as_str()
        {
            "string" => ValueType::Str,
            "long" => ValueType::Long,
            "double" => ValueType::Double,
            "boolean" => ValueType::Bool,
            "instant" => ValueType::Instant,
            "uuid" => ValueType::Uuid,
            "keyword" => ValueType::Keyword,
            "bytes" => ValueType::Bytes,
            "ref" => ValueType::Ref,
            other => panic!("unknown value type {other}"),
        };
        let cardinality = match form
            .get(&kw("db/cardinality"))
            .and_then(Edn::as_keyword)
            .map(|k| k.name.clone())
            .as_deref()
        {
            Some("many") => Cardinality::Many,
            _ => Cardinality::One,
        };
        let unique = form
            .get(&kw("db/unique"))
            .and_then(Edn::as_keyword)
            .map(|k| match k.name.as_str() {
                "identity" => Unique::Identity,
                "value" => Unique::Value,
                other => panic!("unknown uniqueness {other}"),
            });
        let flag = |name: &str| form.get(&kw(name)) == Some(&Edn::Bool(true));
        schema.insert(Attribute {
            id,
            value_type,
            cardinality,
            unique,
            is_component: flag("db/isComponent"),
            indexed: flag("db/index") || unique.is_some(),
            no_history: flag("db/noHistory"),
        });
        idents.insert(ident, id);
    }
    (schema, idents)
}

struct Vectors {
    db: Db,
    tempids: BTreeMap<String, EntityId>,
}

fn entity_ref(form: &Edn, ctx: &Vectors) -> EntityRef {
    match form {
        Edn::Str(name) => EntityRef::Temp(name.clone()),
        Edn::Long(n) => EntityRef::Id(EntityId::from_raw(u64::try_from(*n).expect("eid"))),
        Edn::Keyword(k) => EntityRef::Id(ctx.db.idents().entid(k).expect("ident entity")),
        Edn::Tagged(tag, value) if tag == "tempid" => match value.as_ref() {
            Edn::Str(name) => EntityRef::Id(ctx.tempids[name]),
            _ => panic!("#tempid requires a string"),
        },
        Edn::Vector(items) => {
            let [attr, value] = items.as_slice() else {
                panic!("lookup ref requires [attr value]");
            };
            let attr_id = ctx
                .db
                .idents()
                .entid(attr.as_keyword().expect("lookup attr"))
                .expect("known attr");
            EntityRef::Lookup(attr_id, tx_value(value, attr_id, ctx))
        }
        other => panic!("bad entity position {other}"),
    }
}

/// Converts a transaction value form for an attribute, resolving tempids
/// and lookup refs in reference-valued positions against the current basis.
fn tx_value(form: &Edn, attr: EntityId, ctx: &Vectors) -> Value {
    let value_type = ctx.db.schema().get(attr).expect("known attr").value_type;
    if value_type == ValueType::Ref {
        return match entity_ref(form, ctx) {
            EntityRef::Id(e) => Value::Ref(e),
            EntityRef::Temp(name) => Value::Ref(
                *ctx.tempids
                    .get(&name)
                    .unwrap_or_else(|| panic!("value tempid {name} must come from a prior tx")),
            ),
            EntityRef::Lookup(a, v) => Value::Ref(ctx.db.lookup(a, &v).expect("lookup resolves")),
        };
    }
    let value = exec::const_value(&ctx.db, form).unwrap_or_else(|| panic!("bad value {form}"));
    exec::coerce_for_type(value, value_type)
}

fn tx_items(forms: &[Edn], ctx: &Vectors) -> Vec<TxItem> {
    forms
        .iter()
        .map(|form| match form {
            Edn::Vector(items) => {
                let op = items
                    .first()
                    .and_then(Edn::as_keyword)
                    .expect("op keyword")
                    .clone();
                match (format!(":{}/{}", op.namespace.as_deref().unwrap_or(""), op.name)).as_str() {
                    ":db/add" => {
                        let attr = attr_of(&items[2], ctx);
                        TxItem::Op(TxOp::Add(
                            entity_ref(&items[1], ctx),
                            attr,
                            tx_value(&items[3], attr, ctx),
                        ))
                    }
                    ":db/retract" => {
                        let attr = attr_of(&items[2], ctx);
                        TxItem::Op(TxOp::Retract(
                            entity_ref(&items[1], ctx),
                            attr,
                            tx_value(&items[3], attr, ctx),
                        ))
                    }
                    ":db/cas" => {
                        let attr = attr_of(&items[2], ctx);
                        let old = match &items[3] {
                            Edn::Nil => None,
                            form => Some(tx_value(form, attr, ctx)),
                        };
                        TxItem::Op(TxOp::Cas(
                            entity_ref(&items[1], ctx),
                            attr,
                            old,
                            tx_value(&items[4], attr, ctx),
                        ))
                    }
                    ":db/retractEntity" => {
                        TxItem::Op(TxOp::RetractEntity(entity_ref(&items[1], ctx)))
                    }
                    other => panic!("unknown tx op {other}"),
                }
            }
            Edn::Map(pairs) => {
                let entity = pairs
                    .iter()
                    .find(|(key, _)| key == &kw("db/id"))
                    .map_or_else(
                        || panic!("map form requires :db/id"),
                        |(_, value)| entity_ref(value, ctx),
                    );
                let attributes = pairs
                    .iter()
                    .filter(|(key, _)| key != &kw("db/id"))
                    .map(|(key, value)| {
                        let attr = attr_of(key, ctx);
                        let many = matches!(value, Edn::Vector(_))
                            && !matches!(
                                value,
                                Edn::Vector(items)
                                    if items.len() == 2 && items[0].as_keyword().is_some()
                            );
                        let values = if many {
                            value
                                .as_seq()
                                .expect("vector")
                                .iter()
                                .map(|v| tx_value(v, attr, ctx))
                                .collect()
                        } else {
                            vec![tx_value(value, attr, ctx)]
                        };
                        (attr, values)
                    })
                    .collect();
                TxItem::Map(corium_tx::EntityMap { entity, attributes })
            }
            other => panic!("bad tx form {other}"),
        })
        .collect()
}

fn attr_of(form: &Edn, ctx: &Vectors) -> EntityId {
    ctx.db
        .idents()
        .entid(form.as_keyword().expect("attribute keyword"))
        .unwrap_or_else(|| panic!("unknown attribute {form}"))
}

/// Rewrites `#tempid`/`#tx` tags. Queries and args get `#eid` tags (engine
/// input syntax); expected results get plain longs (engine output syntax).
fn substitute(form: &Edn, ctx: &Vectors, output: bool) -> Edn {
    match form {
        Edn::Tagged(tag, value) if tag == "tempid" => {
            let Edn::Str(name) = value.as_ref() else {
                panic!("#tempid requires a string");
            };
            let raw = i64::try_from(ctx.tempids[name].raw()).expect("fits");
            if output {
                Edn::Long(raw)
            } else {
                Edn::Tagged("eid".into(), Box::new(Edn::Long(raw)))
            }
        }
        Edn::Tagged(tag, value) if output && tag == "tx" => {
            let Edn::Long(t) = value.as_ref() else {
                panic!("#tx requires a long");
            };
            let raw = EntityId::new(Partition::Tx as u32, u64::try_from(*t).expect("t")).raw();
            Edn::Long(i64::try_from(raw).expect("fits"))
        }
        Edn::List(items) => Edn::List(items.iter().map(|i| substitute(i, ctx, output)).collect()),
        Edn::Vector(items) => {
            Edn::Vector(items.iter().map(|i| substitute(i, ctx, output)).collect())
        }
        Edn::Set(items) => {
            let mut out: Vec<Edn> = items.iter().map(|i| substitute(i, ctx, output)).collect();
            out.sort();
            out.dedup();
            Edn::Set(out)
        }
        Edn::Map(pairs) => {
            let mut out: Vec<(Edn, Edn)> = pairs
                .iter()
                .map(|(k, v)| (substitute(k, ctx, output), substitute(v, ctx, output)))
                .collect();
            out.sort_by(|left, right| left.0.cmp(&right.0));
            Edn::Map(out)
        }
        Edn::Tagged(tag, value) => {
            Edn::Tagged(tag.clone(), Box::new(substitute(value, ctx, output)))
        }
        other => other.clone(),
    }
}

/// Normalizes a relation result for order-insensitive comparison.
fn normalize(result: &Edn) -> Edn {
    match result {
        Edn::Set(items) => {
            let mut rows = items.clone();
            rows.sort();
            rows.dedup();
            Edn::Vector(rows)
        }
        Edn::Vector(rows) => {
            let mut rows = rows.clone();
            rows.sort();
            Edn::Vector(rows)
        }
        other => other.clone(),
    }
}

#[allow(clippy::too_many_lines)]
fn run_vector(vector: &Edn, cache: &QueryCache) {
    let name = vector
        .get(&kw("name"))
        .map_or_else(|| "<unnamed>".to_owned(), ToString::to_string);
    let context = || format!("conformance vector {name}");

    // Schema and naming.
    let schema_forms = vector
        .get(&kw("schema"))
        .and_then(Edn::as_seq)
        .unwrap_or(&[]);
    let (schema, idents) = parse_schema(schema_forms);
    let mut interner = KeywordInterner::default();
    intern_all(vector, &mut interner);
    let base = Db::new(schema).with_naming(idents, interner);

    // Transact through the embedded pipeline.
    let log = Arc::new(MemoryLog::default());
    let transactor = EmbeddedTransactor::recover_from(base, log).expect("recover");
    let mut ctx = Vectors {
        db: transactor.db(),
        tempids: BTreeMap::new(),
    };
    if let Some(txes) = vector.get(&kw("tx")).and_then(Edn::as_seq) {
        for tx_forms in txes {
            let forms = tx_forms.as_seq().expect("tx must be a vector");
            let items = tx_items(forms, &ctx);
            let report = transactor
                .transact(items)
                .unwrap_or_else(|error| panic!("{}: transact failed: {error}", context()));
            for (tempid, eid) in &report.tx.tempids {
                ctx.tempids.insert(tempid.clone(), *eid);
            }
            ctx.db = transactor.db();
        }
    }

    // Time view.
    let db = match vector.get(&kw("view")) {
        None => ctx.db.clone(),
        Some(view) if view == &kw("history") => ctx.db.history(),
        Some(Edn::Map(pairs)) => match pairs.as_slice() {
            [(key, Edn::Long(t))] if key == &kw("as-of") => {
                ctx.db.as_of(u64::try_from(*t).expect("t"))
            }
            [(key, Edn::Long(t))] if key == &kw("since") => {
                ctx.db.since(u64::try_from(*t).expect("t"))
            }
            other => panic!("{}: bad view {other:?}", context()),
        },
        Some(other) => panic!("{}: bad view {other}", context()),
    };

    let expect_error = vector.get(&kw("expect-error")) == Some(&Edn::Bool(true));
    let expected = vector.get(&kw("expected"));

    // Pull vectors.
    if let Some(pull_spec) = vector.get(&kw("pull")) {
        let eid_form = substitute(pull_spec.get(&kw("eid")).expect(":eid"), &ctx, false);
        let eid = match &eid_form {
            Edn::Tagged(tag, boxed) if tag == "eid" => match boxed.as_ref() {
                Edn::Long(n) => EntityId::from_raw(u64::try_from(*n).expect("eid")),
                other => panic!("{}: bad eid {other}", context()),
            },
            Edn::Long(n) => EntityId::from_raw(u64::try_from(*n).expect("eid")),
            Edn::Vector(_) => {
                let Value::Ref(e) = corium_query::boundary::edn_to_value(Some(&db), &eid_form)
                    .unwrap_or_else(|| {
                        // Lookup ref.
                        let items = eid_form.as_seq().expect("lookup");
                        let attr = db
                            .idents()
                            .entid(items[0].as_keyword().expect("attr"))
                            .expect("known attr");
                        let value = exec::const_value(&db, &items[1]).expect("value");
                        Value::Ref(db.lookup(attr, &value).expect("lookup resolves"))
                    })
                else {
                    panic!("{}: bad :eid", context())
                };
                e
            }
            other => panic!("{}: bad eid {other}", context()),
        };
        let pattern = pull_spec.get(&kw("pattern")).expect(":pattern");
        let result = pull(&db, pattern, eid);
        match (expect_error, result) {
            (true, Err(_)) => return,
            (true, Ok(result)) => panic!("{}: expected error, got {result}", context()),
            (false, Err(error)) => panic!("{}: pull failed: {error}", context()),
            (false, Ok(result)) => {
                let expected = substitute(expected.expect(":expected"), &ctx, true);
                assert_eq!(result, expected, "{}", context());
            }
        }
        return;
    }

    // Query vectors.
    let query_form = substitute(vector.get(&kw("query")).expect(":query"), &ctx, false);
    let parsed = match cache.parse(&query_form) {
        Ok(parsed) => parsed,
        Err(error) => {
            assert!(expect_error, "{}: parse failed: {error}", context());
            return;
        }
    };
    // Extra database inputs (views of the same underlying value), bound
    // positionally after `$`: `:extra-dbs [:history {:as-of 1} :current]`.
    let extra_dbs: Vec<Db> = vector
        .get(&kw("extra-dbs"))
        .and_then(Edn::as_seq)
        .unwrap_or(&[])
        .iter()
        .map(|view| {
            if view == &kw("history") {
                ctx.db.history()
            } else if view == &kw("current") {
                ctx.db.clone()
            } else if let Edn::Map(pairs) = view {
                match pairs.as_slice() {
                    [(key, Edn::Long(t))] if key == &kw("as-of") => {
                        ctx.db.as_of(u64::try_from(*t).expect("t"))
                    }
                    [(key, Edn::Long(t))] if key == &kw("since") => {
                        ctx.db.since(u64::try_from(*t).expect("t"))
                    }
                    other => panic!("{}: bad extra db {other:?}", context()),
                }
            } else {
                panic!("{}: bad extra db {view}", context())
            }
        })
        .collect();
    let mut inputs: Vec<QInput<'_>> = vec![QInput::Db(&db)];
    inputs.extend(extra_dbs.iter().map(QInput::Db));
    if let Some(args) = vector.get(&kw("args")).and_then(Edn::as_seq) {
        for arg in args {
            inputs.push(QInput::Edn(substitute(arg, &ctx, false)));
        }
    }
    let result = run(&parsed, &inputs, corium_query::ExecOptions::default());
    match (expect_error, result) {
        (true, Err(_)) => {}
        (true, Ok((result, _))) => panic!("{}: expected error, got {result}", context()),
        (false, Err(error)) => panic!("{}: query failed: {error}", context()),
        (false, Ok((result, _))) => {
            let expected = substitute(expected.expect(":expected"), &ctx, true);
            // Only relation/collection results are order-insensitive; tuple
            // and scalar results compare positionally.
            let unordered = matches!(
                parsed.find,
                corium_query::ast::FindSpec::Rel(_) | corium_query::ast::FindSpec::Coll(_)
            );
            if unordered {
                assert_eq!(
                    normalize(&result),
                    normalize(&expected),
                    "{}: got {result}",
                    context()
                );
            } else {
                assert_eq!(result, expected, "{}", context());
            }
        }
    }
}

#[test]
fn conformance_corpus() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("tests/conformance");
    let mut files: Vec<_> = std::fs::read_dir(&dir)
        .expect("tests/conformance directory")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "edn"))
        .collect();
    files.sort();
    assert!(!files.is_empty(), "no conformance files found");
    let cache = QueryCache::new();
    let mut total = 0;
    for file in files {
        let text = std::fs::read_to_string(&file).expect("readable corpus file");
        let vectors = read_all(&text)
            .unwrap_or_else(|error| panic!("{}: EDN error: {error}", file.display()));
        for vector in vectors {
            run_vector(&vector, &cache);
            total += 1;
        }
    }
    assert!(
        total >= 150,
        "conformance corpus has {total} vectors (< 150)"
    );
    println!("conformance corpus: {total} vectors green");
}
