//! Boundary conversion from wire EDN transaction forms to engine `TxItem`s.
//!
//! Accepts the Datomic-dialect forms used by the conformance corpus: map
//! forms with `:db/id`, and list forms `[:db/add e a v]`, `[:db/retract e a
//! v]`, `[:db/cas e a old new]`, `[:db/retractEntity e]`. Entity positions
//! accept tempid strings, raw entity-id longs, `#eid` tags, idents, and
//! lookup refs. Value positions for `ref` attributes accept the same except
//! tempid strings (same-transaction value tempids are not supported by the
//! transaction layer; clients resolve them against prior tempid maps).

use corium_core::{EntityId, KeywordInterner, TotalF64, Value, ValueType};
use corium_db::Db;
use corium_query::edn::Edn;
use corium_tx::{EntityMap, EntityRef, TxItem, TxOp};
use thiserror::Error;

/// Transaction form conversion failure.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum TxFormError {
    /// Form is not a map or list form.
    #[error("bad transaction form: {0}")]
    BadForm(String),
    /// Unknown transaction operation keyword.
    #[error("unknown transaction op {0}")]
    UnknownOp(String),
    /// Attribute keyword has no ident.
    #[error("unknown attribute {0}")]
    UnknownAttribute(String),
    /// Entity position not understood.
    #[error("bad entity position: {0}")]
    BadEntity(String),
    /// Value form not convertible for the attribute.
    #[error("bad value {0}")]
    BadValue(String),
}

fn kw(text: &str) -> Edn {
    Edn::keyword(text)
}

fn attr_of(db: &Db, form: &Edn) -> Result<EntityId, TxFormError> {
    let keyword = form
        .as_keyword()
        .ok_or_else(|| TxFormError::BadForm(format!("attribute position {form}")))?;
    db.idents()
        .entid(keyword)
        .ok_or_else(|| TxFormError::UnknownAttribute(keyword.to_string()))
}

fn entity_ref(db: &Db, form: &Edn) -> Result<EntityRef, TxFormError> {
    match form {
        Edn::Str(name) => Ok(EntityRef::Temp(name.clone())),
        Edn::Long(n) => u64::try_from(*n)
            .map(|raw| EntityRef::Id(EntityId::from_raw(raw)))
            .map_err(|_| TxFormError::BadEntity(form.to_string())),
        Edn::Keyword(keyword) => db
            .idents()
            .entid(keyword)
            .map(EntityRef::Id)
            .ok_or_else(|| TxFormError::UnknownAttribute(keyword.to_string())),
        Edn::Tagged(tag, value) if tag == "eid" => match value.as_ref() {
            Edn::Long(n) => u64::try_from(*n)
                .map(|raw| EntityRef::Id(EntityId::from_raw(raw)))
                .map_err(|_| TxFormError::BadEntity(form.to_string())),
            _ => Err(TxFormError::BadEntity(form.to_string())),
        },
        Edn::Vector(items) => {
            let [attr_form, value_form] = items.as_slice() else {
                return Err(TxFormError::BadEntity(form.to_string()));
            };
            let attr = attr_of(db, attr_form)?;
            let value_type = db
                .schema()
                .get(attr)
                .map(|meta| meta.value_type)
                .ok_or_else(|| TxFormError::UnknownAttribute(format!("{attr:?}")))?;
            // Lookup values never intern new keywords: an uninterned keyword
            // cannot equal any stored value, so resolution would fail anyway.
            let value = match value_form {
                Edn::Keyword(keyword) => Value::Keyword(
                    db.interner()
                        .get(keyword)
                        .unwrap_or(corium_query::exec::UNKNOWN_KEYWORD),
                ),
                other => {
                    let mut scratch = KeywordInterner::default();
                    scalar_value(other, &mut scratch)
                        .ok_or_else(|| TxFormError::BadValue(form.to_string()))?
                }
            };
            Ok(EntityRef::Lookup(attr, coerce(value, value_type)))
        }
        other => Err(TxFormError::BadEntity(other.to_string())),
    }
}

/// Converts a scalar EDN form to a value, interning keywords into `interner`.
fn scalar_value(form: &Edn, interner: &mut KeywordInterner) -> Option<Value> {
    match form {
        Edn::Bool(v) => Some(Value::Bool(*v)),
        Edn::Long(v) => Some(Value::Long(*v)),
        Edn::Double(v) => Some(Value::Double(*v)),
        Edn::Str(v) => Some(Value::Str(v.as_str().into())),
        Edn::Keyword(k) => Some(Value::Keyword(interner.intern(k.clone()))),
        Edn::Tagged(tag, value) => match (tag.as_str(), value.as_ref()) {
            ("eid", Edn::Long(n)) => u64::try_from(*n)
                .ok()
                .map(|raw| Value::Ref(EntityId::from_raw(raw))),
            ("inst", Edn::Long(ms)) => Some(Value::Instant(*ms)),
            ("uuid", Edn::Str(hex)) => u128::from_str_radix(hex, 16).ok().map(Value::Uuid),
            ("bytes", Edn::Str(hex)) => decode_hex(hex).map(|b| Value::Bytes(b.into())),
            _ => None,
        },
        _ => None,
    }
}

fn decode_hex(hex: &str) -> Option<Vec<u8>> {
    if !hex.len().is_multiple_of(2) {
        return None;
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(hex.get(i..i + 2)?, 16).ok())
        .collect()
}

#[allow(clippy::cast_precision_loss)]
fn coerce(value: Value, value_type: ValueType) -> Value {
    match (&value, value_type) {
        (Value::Long(n), ValueType::Ref) => u64::try_from(*n)
            .map(|raw| Value::Ref(EntityId::from_raw(raw)))
            .unwrap_or(value),
        (Value::Long(n), ValueType::Instant) => Value::Instant(*n),
        (Value::Long(n), ValueType::Double) => Value::Double(TotalF64(*n as f64)),
        _ => value,
    }
}

/// Converts a value-position form for `attr`, resolving reference values.
fn tx_value(
    db: &Db,
    interner: &mut KeywordInterner,
    attr: EntityId,
    form: &Edn,
) -> Result<Value, TxFormError> {
    let value_type = db
        .schema()
        .get(attr)
        .map(|meta| meta.value_type)
        .ok_or_else(|| TxFormError::UnknownAttribute(format!("{attr:?}")))?;
    if value_type == ValueType::Ref {
        return match entity_ref(db, form)? {
            EntityRef::Id(e) => Ok(Value::Ref(e)),
            EntityRef::Temp(name) => Err(TxFormError::BadValue(format!(
                "value-position tempid \"{name}\" must be resolved by the client"
            ))),
            EntityRef::Lookup(a, v) => db
                .lookup(a, &v)
                .map(Value::Ref)
                .ok_or_else(|| TxFormError::BadValue(format!("lookup ref {form} did not resolve"))),
        };
    }
    scalar_value(form, interner)
        .map(|value| coerce(value, value_type))
        .ok_or_else(|| TxFormError::BadValue(form.to_string()))
}

/// Converts wire EDN transaction forms into engine transaction items.
///
/// New keyword values are interned into `interner` (the caller persists the
/// naming change before committing the transaction).
///
/// # Errors
/// Returns [`TxFormError`] when a form is malformed or references unknown
/// attributes/idents.
pub fn tx_items_from_edn(
    db: &Db,
    interner: &mut KeywordInterner,
    forms: &[Edn],
) -> Result<Vec<TxItem>, TxFormError> {
    forms
        .iter()
        .map(|form| match form {
            Edn::Vector(items) => list_form(db, interner, items, form),
            Edn::Map(pairs) => map_form(db, interner, pairs),
            other => Err(TxFormError::BadForm(other.to_string())),
        })
        .collect()
}

fn list_form(
    db: &Db,
    interner: &mut KeywordInterner,
    items: &[Edn],
    form: &Edn,
) -> Result<TxItem, TxFormError> {
    let op = items
        .first()
        .and_then(Edn::as_keyword)
        .ok_or_else(|| TxFormError::BadForm(form.to_string()))?;
    let name = format!(
        "{}/{}",
        op.namespace.as_deref().unwrap_or_default(),
        op.name
    );
    let arg = |index: usize| {
        items
            .get(index)
            .ok_or_else(|| TxFormError::BadForm(form.to_string()))
    };
    Ok(TxItem::Op(match name.as_str() {
        "db/add" => {
            let attr = attr_of(db, arg(2)?)?;
            TxOp::Add(
                entity_ref(db, arg(1)?)?,
                attr,
                tx_value(db, interner, attr, arg(3)?)?,
            )
        }
        "db/retract" => {
            let attr = attr_of(db, arg(2)?)?;
            TxOp::Retract(
                entity_ref(db, arg(1)?)?,
                attr,
                tx_value(db, interner, attr, arg(3)?)?,
            )
        }
        "db/cas" => {
            let attr = attr_of(db, arg(2)?)?;
            let old = match arg(3)? {
                Edn::Nil => None,
                other => Some(tx_value(db, interner, attr, other)?),
            };
            TxOp::Cas(
                entity_ref(db, arg(1)?)?,
                attr,
                old,
                tx_value(db, interner, attr, arg(4)?)?,
            )
        }
        "db/retractEntity" => TxOp::RetractEntity(entity_ref(db, arg(1)?)?),
        _ => return Err(TxFormError::UnknownOp(op.to_string())),
    }))
}

fn map_form(
    db: &Db,
    interner: &mut KeywordInterner,
    pairs: &[(Edn, Edn)],
) -> Result<TxItem, TxFormError> {
    let id_key = kw("db/id");
    let entity = pairs
        .iter()
        .find(|(key, _)| *key == id_key)
        .map(|(_, value)| entity_ref(db, value))
        .ok_or_else(|| TxFormError::BadForm("map form requires :db/id".into()))??;
    let mut attributes = Vec::new();
    for (key, value) in pairs.iter().filter(|(key, _)| *key != id_key) {
        let attr = attr_of(db, key)?;
        // A vector value is a cardinality-many set of values unless it reads
        // as a lookup ref (`[:attr value]`), matching the corpus convention.
        let many = matches!(value, Edn::Vector(items)
            if !(items.len() == 2 && items[0].as_keyword().is_some()));
        let values = if many {
            value
                .as_seq()
                .unwrap_or_default()
                .iter()
                .map(|item| tx_value(db, interner, attr, item))
                .collect::<Result<Vec<_>, _>>()?
        } else {
            vec![tx_value(db, interner, attr, value)?]
        };
        attributes.push((attr, values));
    }
    Ok(TxItem::Map(EntityMap { entity, attributes }))
}
