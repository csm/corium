//! Value conversion between engine values and boundary EDN.

use corium_core::{EntityId, Value};
use corium_db::Db;

use crate::edn::Edn;
use crate::exec::UNKNOWN_KEYWORD;

/// Converts an engine value to its boundary EDN representation.
///
/// Entity ids surface as longs (as in Datomic); instants, UUIDs, and byte
/// arrays surface as tagged elements.
#[must_use]
pub fn value_to_edn(db: &Db, value: &Value) -> Edn {
    match value {
        Value::Bool(v) => Edn::Bool(*v),
        Value::Long(v) => Edn::Long(*v),
        Value::Double(v) => Edn::Double(*v),
        Value::Str(v) => Edn::Str(v.to_string()),
        Value::Instant(ms) => Edn::Tagged("inst".into(), Box::new(Edn::Long(*ms))),
        Value::Uuid(v) => Edn::Tagged("uuid".into(), Box::new(Edn::Str(format!("{v:032x}")))),
        Value::Bytes(bytes) => Edn::Tagged(
            "bytes".into(),
            Box::new(Edn::Str(bytes.iter().fold(String::new(), |mut acc, b| {
                use std::fmt::Write as _;
                let _ = write!(acc, "{b:02x}");
                acc
            }))),
        ),
        Value::Keyword(id) => db.interner().resolve(*id).map_or_else(
            || {
                Edn::Tagged(
                    "kw".into(),
                    Box::new(Edn::Long(i64::try_from(*id).unwrap_or(-1))),
                )
            },
            |keyword| Edn::Keyword(keyword.clone()),
        ),
        Value::Ref(e) => Edn::Long(i64::try_from(e.raw()).unwrap_or(i64::MAX)),
    }
}

/// Converts a boundary EDN scalar to an engine value.
///
/// Keywords are resolved against the database's interner when one is
/// supplied; unknown keywords convert to a sentinel id that never matches a
/// stored value. Returns `None` for non-scalar forms.
#[must_use]
pub fn edn_to_value(db: Option<&Db>, form: &Edn) -> Option<Value> {
    match form {
        Edn::Bool(v) => Some(Value::Bool(*v)),
        Edn::Long(v) => Some(Value::Long(*v)),
        Edn::Double(v) => Some(Value::Double(*v)),
        Edn::Str(v) => Some(Value::Str(v.as_str().into())),
        Edn::Keyword(k) => Some(Value::Keyword(
            db.and_then(|db| db.interner().get(k))
                .unwrap_or(UNKNOWN_KEYWORD),
        )),
        Edn::Tagged(tag, value) => match (tag.as_str(), value.as_ref()) {
            ("eid", Edn::Long(n)) => u64::try_from(*n)
                .ok()
                .map(|n| Value::Ref(EntityId::from_raw(n))),
            ("tx", Edn::Long(t)) => u64::try_from(*t)
                .ok()
                .map(|t| Value::Ref(EntityId::new(corium_core::Partition::Tx as u32, t))),
            ("inst", Edn::Long(ms)) => Some(Value::Instant(*ms)),
            ("uuid", Edn::Str(hex)) => u128::from_str_radix(hex, 16).ok().map(Value::Uuid),
            _ => None,
        },
        _ => None,
    }
}
