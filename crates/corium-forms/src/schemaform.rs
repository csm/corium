//! Boundary conversion from Datomic-style EDN attribute maps to the engine
//! schema model, used by `CreateDatabase`.

use corium_core::{Attribute, Cardinality, EntityId, Partition, Schema, Unique, ValueType};
use corium_db::Idents;
use corium_query::edn::Edn;
use thiserror::Error;

/// Sequence of the first installable attribute entity in the db partition.
pub const FIRST_ATTR_ID: u64 = 100;

/// Schema form conversion failure.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum SchemaFormError {
    /// Attribute definition is not a map.
    #[error("attribute definition must be a map: {0}")]
    NotAMap(String),
    /// `:db/ident` missing or not a keyword.
    #[error("attribute requires a :db/ident keyword")]
    MissingIdent,
    /// `:db/valueType` missing or unknown.
    #[error("unknown or missing :db/valueType {0}")]
    BadValueType(String),
    /// `:db/cardinality` unknown.
    #[error("unknown :db/cardinality {0}")]
    BadCardinality(String),
    /// `:db/unique` unknown.
    #[error("unknown :db/unique {0}")]
    BadUnique(String),
    /// Two attributes share one ident.
    #[error("duplicate :db/ident {0}")]
    DuplicateIdent(String),
}

fn kw(text: &str) -> Edn {
    Edn::keyword(text)
}

/// Parses Datomic-style attribute maps into a schema and ident registry.
///
/// Attribute entity ids are assigned sequentially in the `Db` partition
/// starting at [`FIRST_ATTR_ID`].
///
/// # Errors
/// Returns [`SchemaFormError`] for malformed attribute definitions.
pub fn schema_from_edn(forms: &[Edn]) -> Result<(Schema, Idents), SchemaFormError> {
    let mut schema = Schema::default();
    let mut idents = Idents::default();
    for (index, form) in forms.iter().enumerate() {
        if !matches!(form, Edn::Map(_)) {
            return Err(SchemaFormError::NotAMap(form.to_string()));
        }
        let id = EntityId::new(Partition::Db as u32, FIRST_ATTR_ID + index as u64);
        let ident = form
            .get(&kw("db/ident"))
            .and_then(Edn::as_keyword)
            .ok_or(SchemaFormError::MissingIdent)?
            .clone();
        if idents.entid(&ident).is_some() {
            return Err(SchemaFormError::DuplicateIdent(ident.to_string()));
        }
        let value_type_name = form
            .get(&kw("db/valueType"))
            .and_then(Edn::as_keyword)
            .map(|keyword| keyword.name.clone())
            .ok_or_else(|| SchemaFormError::BadValueType("<missing>".into()))?;
        let value_type = match value_type_name.as_str() {
            "string" => ValueType::Str,
            "long" => ValueType::Long,
            "double" => ValueType::Double,
            "boolean" => ValueType::Bool,
            "instant" => ValueType::Instant,
            "uuid" => ValueType::Uuid,
            "keyword" => ValueType::Keyword,
            "bytes" => ValueType::Bytes,
            "ref" => ValueType::Ref,
            other => return Err(SchemaFormError::BadValueType(other.to_owned())),
        };
        let cardinality = match form
            .get(&kw("db/cardinality"))
            .and_then(Edn::as_keyword)
            .map(|keyword| keyword.name.clone())
        {
            None => Cardinality::One,
            Some(name) => match name.as_str() {
                "one" => Cardinality::One,
                "many" => Cardinality::Many,
                other => return Err(SchemaFormError::BadCardinality(other.to_owned())),
            },
        };
        let unique = match form
            .get(&kw("db/unique"))
            .and_then(Edn::as_keyword)
            .map(|keyword| keyword.name.clone())
        {
            None => None,
            Some(name) => match name.as_str() {
                "identity" => Some(Unique::Identity),
                "value" => Some(Unique::Value),
                other => return Err(SchemaFormError::BadUnique(other.to_owned())),
            },
        };
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
    Ok((schema, idents))
}
