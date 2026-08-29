//! Boundary conversion from wire EDN transaction forms to engine `TxItem`s.
//!
//! Accepts the Datomic-dialect forms used by the conformance corpus: map
//! forms with `:db/id`, and list forms `[:db/add e a v]`, `[:db/retract e a
//! v]`, `[:db/cas e a old new]`, `[:db/retractEntity e]`. Entity positions
//! accept tempid strings, raw entity-id longs, `#eid` tags, idents, and
//! lookup refs; the transaction's own entity is named by the reserved tempid
//! `"datomic.tx"` or by `:db/current-tx`, which is how transaction metadata is
//! asserted. Value positions for `ref` attributes accept the same except
//! tempid strings (same-transaction value tempids are not supported by the
//! transaction layer; clients resolve them against prior tempid maps).

use corium_core::{EntityId, Keyword, KeywordInterner, TotalF64, Value, ValueType};
use corium_db::Db;
use corium_query::edn::Edn;
use corium_tx::{EntityMap, EntityRef, TX_TEMPID, TxItem, TxOp};
use thiserror::Error;

/// Whether a keyword in entity position names the transaction being built.
fn is_current_tx(keyword: &Keyword) -> bool {
    keyword.namespace.as_deref() == Some("db") && keyword.name == "current-tx"
}

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
        // `:db/current-tx` names the transaction being built, whose entity id
        // only the transactor knows; the reserved tempid carries that
        // intention through to `prepare`.
        Edn::Keyword(keyword) if is_current_tx(keyword) => {
            Ok(EntityRef::Temp(TX_TEMPID.to_owned()))
        }
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
            (SEALED_TAG, map) => sealed_value(map),
            _ => None,
        },
        _ => None,
    }
}

/// EDN tag naming a value that is already sealed.
///
/// A sealed value travels as ordinary EDN so the transactor decodes tx-data
/// with the same pure expansion it uses for everything else, and validates
/// the cleartext header without holding any key.
pub const SEALED_TAG: &str = "corium/sealed";

/// Reads `#corium/sealed {:class … :epoch … :vtype … :body "hex"}`.
fn sealed_value(form: &Edn) -> Option<Value> {
    let class = match form.get(&kw("class"))? {
        Edn::Long(raw) => EntityId::from_raw(u64::try_from(*raw).ok()?),
        _ => return None,
    };
    let epoch = match form.get(&kw("epoch"))? {
        Edn::Long(epoch) => u32::try_from(*epoch).ok()?,
        _ => return None,
    };
    let vtype = value_type_named(form.get(&kw("vtype"))?.as_keyword()?)?;
    let body = match form.get(&kw("body"))? {
        Edn::Str(hex) => decode_hex(hex)?,
        Edn::Tagged(tag, value) if tag == "bytes" => match value.as_ref() {
            Edn::Str(hex) => decode_hex(hex)?,
            _ => return None,
        },
        _ => return None,
    };
    Some(Value::Sealed(corium_core::Sealed {
        class,
        epoch,
        vtype,
        body: body.into(),
    }))
}

/// Writes the wire form [`sealed_value`] reads.
#[must_use]
pub fn sealed_to_edn(sealed: &corium_core::Sealed) -> Edn {
    use std::fmt::Write as _;
    let body = sealed.body.iter().fold(String::new(), |mut acc, byte| {
        let _ = write!(acc, "{byte:02x}");
        acc
    });
    let mut pairs = vec![
        (
            kw("class"),
            Edn::Long(i64::try_from(sealed.class.raw()).unwrap_or(i64::MAX)),
        ),
        (kw("epoch"), Edn::Long(i64::from(sealed.epoch))),
        (
            kw("vtype"),
            kw(&format!("db.type/{}", value_type_name(sealed.vtype))),
        ),
        (kw("body"), Edn::Str(body)),
    ];
    pairs.sort_unstable();
    Edn::Tagged(SEALED_TAG.to_owned(), Box::new(Edn::Map(pairs)))
}

const fn value_type_name(value_type: ValueType) -> &'static str {
    match value_type {
        ValueType::Bool => "boolean",
        ValueType::Long => "long",
        ValueType::Double => "double",
        ValueType::Instant => "instant",
        ValueType::Uuid => "uuid",
        ValueType::Keyword => "keyword",
        ValueType::Str => "string",
        ValueType::Bytes => "bytes",
        ValueType::Ref => "ref",
    }
}

fn value_type_named(keyword: &Keyword) -> Option<ValueType> {
    Some(match keyword.name.as_str() {
        "boolean" => ValueType::Bool,
        "long" => ValueType::Long,
        "double" => ValueType::Double,
        "instant" => ValueType::Instant,
        "uuid" => ValueType::Uuid,
        "keyword" => ValueType::Keyword,
        "string" => ValueType::Str,
        "bytes" => ValueType::Bytes,
        "ref" => ValueType::Ref,
        _ => return None,
    })
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

/// Resolves an entity-position form: an id, an `#eid` tag, an ident, a lookup
/// ref, or a tempid string.
///
/// Public because entity positions turn up outside transaction data proper —
/// a saga's merge guards and conflict resolutions name entities the same way
/// transaction forms do, and spelling them a second way would be one more
/// dialect for a caller to get wrong.
///
/// # Errors
/// Returns [`TxFormError::BadEntity`] when the form is not an entity position,
/// and [`TxFormError::UnknownAttribute`] when a lookup ref names an attribute
/// this database does not have.
pub fn tx_entity(db: &Db, form: &Edn) -> Result<EntityRef, TxFormError> {
    entity_ref(db, form)
}

/// Resolves an attribute-position keyword to its entity.
///
/// # Errors
/// Returns [`TxFormError`] when the form is not a keyword or names an
/// attribute this database does not have.
pub fn tx_attribute(db: &Db, form: &Edn) -> Result<EntityId, TxFormError> {
    attr_of(db, form)
}

/// Converts a value-position form for `attr`, resolving reference values.
///
/// # Errors
/// Returns [`TxFormError`] when `attr` is not in the schema or the form is
/// not a value that attribute can hold.
pub fn tx_value(
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
                "tempid \"{name}\" names an entity this transaction is creating, \
                 which only an assertion may reference"
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
    let mut items = Vec::with_capacity(forms.len());
    for form in forms {
        match form {
            Edn::Vector(list) => items.push(list_form(db, interner, list, form)?),
            Edn::Map(pairs) => items.extend(map_form(db, interner, pairs)?),
            other => return Err(TxFormError::BadForm(other.to_string())),
        }
    }
    Ok(items)
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
            let entity = entity_ref(db, arg(1)?)?;
            // A ref whose value is a tempid points at an entity this
            // transaction is creating, and only the transactor can resolve it.
            match value_tempid(db, attr, arg(3)?) {
                Some(target) => TxOp::AddRef(entity, attr, EntityRef::Temp(target)),
                None => TxOp::Add(entity, attr, tx_value(db, interner, attr, arg(3)?)?),
            }
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

/// The tempid a value-position form names, when the attribute is a reference.
///
/// A bare string in value position is only ever a tempid: every other value a
/// ref attribute can hold has a form of its own (`#eid`, a lookup ref, an
/// ident keyword), and a ref cannot hold a string.
fn value_tempid(db: &Db, attr: EntityId, form: &Edn) -> Option<String> {
    match form {
        Edn::Str(name)
            if db
                .schema()
                .get(attr)
                .is_some_and(|meta| meta.value_type == ValueType::Ref) =>
        {
            Some(name.clone())
        }
        _ => None,
    }
}

/// Expands a map form into the assertions it stands for.
///
/// Literal values become one [`TxItem::Map`] — the shape the engine expands
/// attribute by attribute — while values naming a transaction-local entity
/// become reference assertions beside it, since a map's values are resolved
/// values and a tempid is not one yet.
fn map_form(
    db: &Db,
    interner: &mut KeywordInterner,
    pairs: &[(Edn, Edn)],
) -> Result<Vec<TxItem>, TxFormError> {
    let id_key = kw("db/id");
    let entity = pairs
        .iter()
        .find(|(key, _)| *key == id_key)
        .map(|(_, value)| entity_ref(db, value))
        .ok_or_else(|| TxFormError::BadForm("map form requires :db/id".into()))??;
    let mut attributes = Vec::new();
    let mut references = Vec::new();
    for (key, value) in pairs.iter().filter(|(key, _)| *key != id_key) {
        let attr = attr_of(db, key)?;
        // A vector value is a cardinality-many set of values unless it reads
        // as a lookup ref (`[:attr value]`), matching the corpus convention.
        let many = matches!(value, Edn::Vector(items)
            if !(items.len() == 2 && items[0].as_keyword().is_some()));
        let forms = if many {
            value.as_seq().unwrap_or_default().to_vec()
        } else {
            vec![value.clone()]
        };
        let mut values = Vec::new();
        for form in &forms {
            match value_tempid(db, attr, form) {
                Some(target) => references.push(TxItem::Op(TxOp::AddRef(
                    entity.clone(),
                    attr,
                    EntityRef::Temp(target),
                ))),
                None => values.push(tx_value(db, interner, attr, form)?),
            }
        }
        if !values.is_empty() {
            attributes.push((attr, values));
        }
    }
    let mut items = vec![TxItem::Map(EntityMap { entity, attributes })];
    items.extend(references);
    Ok(items)
}

#[cfg(test)]
mod tests {
    use super::*;
    use corium_core::{Attribute, Cardinality, Sealed, ValueType};
    use corium_query::edn::{read_all, read_one};

    /// A database with one ref attribute, `:parent/child`.
    fn db_with_ref_attribute() -> (Db, EntityId) {
        let attr = EntityId::new(corium_core::Partition::Db as u32, 100);
        let mut schema = corium_core::Schema::default();
        schema.insert(Attribute {
            id: attr,
            value_type: ValueType::Ref,
            cardinality: Cardinality::One,
            unique: None,
            is_component: true,
            indexed: false,
            no_history: false,
        });
        let mut idents = corium_db::Idents::default();
        idents.insert(corium_core::Keyword::new(Some("parent"), "child"), attr);
        (
            Db::new(schema).with_naming(idents, KeywordInterner::default()),
            attr,
        )
    }

    /// A component child has to be created and attached in one transaction:
    /// its id is allocated by the transactor, so the parent can only name it
    /// by the tempid the same transaction gives it.
    #[test]
    fn a_reference_may_name_an_entity_this_transaction_creates() {
        let (db, attr) = db_with_ref_attribute();
        let mut interner = KeywordInterner::default();
        let forms = read_all(r#"[:db/add "parent" :parent/child "child"]"#).expect("parses");
        let items = tx_items_from_edn(&db, &mut interner, &forms).expect("converts");
        assert_eq!(
            items,
            vec![TxItem::Op(TxOp::AddRef(
                EntityRef::Temp("parent".into()),
                attr,
                EntityRef::Temp("child".into()),
            ))]
        );
    }

    /// The map form is sugar for assertions, so it expands the same way — the
    /// literal attributes as a map, the tempid reference beside it.
    #[test]
    fn a_map_form_expands_a_tempid_reference_beside_its_literals() {
        let (db, attr) = db_with_ref_attribute();
        let mut interner = KeywordInterner::default();
        let forms = read_all(r#"{:db/id "parent" :parent/child "child"}"#).expect("parses");
        let items = tx_items_from_edn(&db, &mut interner, &forms).expect("converts");
        assert_eq!(
            items,
            vec![
                TxItem::Map(EntityMap {
                    entity: EntityRef::Temp("parent".into()),
                    attributes: Vec::new(),
                }),
                TxItem::Op(TxOp::AddRef(
                    EntityRef::Temp("parent".into()),
                    attr,
                    EntityRef::Temp("child".into()),
                )),
            ]
        );
    }

    /// Retraction and compare-and-swap have no such form: there is nothing to
    /// retract about an entity that does not exist yet.
    #[test]
    fn only_an_assertion_may_name_an_entity_being_created() {
        let (db, _) = db_with_ref_attribute();
        let mut interner = KeywordInterner::default();
        let forms = read_all(r#"[:db/retract "parent" :parent/child "child"]"#).expect("parses");
        let error = tx_items_from_edn(&db, &mut interner, &forms).expect_err("is refused");
        assert!(matches!(error, TxFormError::BadValue(_)), "{error:?}");
    }

    #[test]
    fn a_sealed_value_round_trips_through_its_edn_form() {
        let sealed = Sealed {
            class: EntityId::new(corium_core::Partition::Db as u32, 100),
            epoch: 3,
            vtype: ValueType::Keyword,
            body: vec![0x00, 0x7f, 0xff, 0x10].into(),
        };
        let form = sealed_to_edn(&sealed);
        // The form is ordinary EDN, so it survives a trip through text — which
        // is what lets the transactor expand tx-data with no special case.
        let printed = form.to_string();
        assert_eq!(read_one(&printed).expect("prints as readable EDN"), form);
        assert_eq!(
            scalar_value(&form, &mut KeywordInterner::default()),
            Some(Value::Sealed(sealed))
        );
    }

    #[test]
    fn a_malformed_sealed_form_is_not_a_value() {
        for text in [
            r#"#corium/sealed {:epoch 1 :vtype :db.type/string :body "00"}"#,
            r#"#corium/sealed {:class 100 :vtype :db.type/string :body "00"}"#,
            r#"#corium/sealed {:class 100 :epoch 1 :vtype :db.type/thing :body "00"}"#,
            r#"#corium/sealed {:class 100 :epoch 1 :vtype :db.type/string :body "0"}"#,
            r"#corium/sealed {:class 100 :epoch 1 :vtype :db.type/string}",
        ] {
            let form = read_one(text).expect("parses as EDN");
            assert_eq!(
                scalar_value(&form, &mut KeywordInterner::default()),
                None,
                "{text} must not decode"
            );
        }
    }
}
