//! Boundary conversion from Datomic-style EDN attribute maps to the engine
//! schema model, used by `CreateDatabase`.

use corium_core::{
    Attribute, Cardinality, EntityId, FIRST_KEY_EPOCH, LegacyPlaintextPolicy, MissingKeyPolicy,
    Partition, ProtectionClass, ProtectionScope, ProtectionTimeline, Schema, SealAlgorithm, Unique,
    ValueType,
};
use corium_db::{Idents, bootstrap};
use corium_query::edn::Edn;
use thiserror::Error;

/// Smallest padding a protection class may set.
///
/// Padding below one AEAD block buys nothing: the 16-byte tag already makes
/// every body at least that long, so a smaller multiple leaves the length
/// side channel exactly where it was.
pub const MIN_PADDING: i64 = 16;

/// Sequence of the first installable attribute entity in the db partition.
/// Lower sequences are reserved for the engine's own attributes
/// (`corium_db::bootstrap`).
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
    /// A schema file declared an attribute the engine owns.
    ///
    /// The schema vocabulary (`:db/ident`, `:db/valueType`, …) and
    /// `:db/txInstant` are installed in every database by
    /// [`corium_db::bootstrap`]. Redeclaring one cannot change it and would
    /// only disagree with it, so it is refused rather than ignored — the same
    /// rule the schema-update planner applies.
    #[error("{0} is an engine attribute and cannot be declared by a schema file")]
    EngineAttribute(String),
    /// A protection class definition is malformed.
    #[error("protection class {ident}: {reason}")]
    BadProtectionClass {
        /// The class being defined.
        ident: String,
        /// What is wrong with it.
        reason: String,
    },
    /// `:db/protection` names a class no form in the schema defines.
    #[error("attribute {attr} names unknown protection class {class}")]
    UnknownProtectionClass {
        /// Attribute carrying the `:db/protection`.
        attr: String,
        /// Class it names.
        class: String,
    },
    /// `:db/protection` was combined with something protected datoms cannot
    /// have.
    ///
    /// Ciphertext order is not value order, so a protected attribute can
    /// never appear in AVET or VAET. Rejecting the combination at install
    /// time makes that a schema rule rather than a runtime surprise
    /// (ADR-0018).
    #[error("attribute {attr} cannot be protected and also {conflict}")]
    ProtectionConflict {
        /// Attribute carrying the `:db/protection`.
        attr: String,
        /// The conflicting declaration.
        conflict: String,
    },
    /// `:db/protection` is not a keyword.
    #[error("attribute {0} requires a keyword :db/protection")]
    BadProtection(String),
}

fn kw(text: &str) -> Edn {
    Edn::keyword(text)
}

/// Parses Datomic-style attribute maps into a schema and ident registry.
///
/// Attribute entity ids are assigned sequentially in the `Db` partition
/// starting at [`FIRST_ATTR_ID`]. The engine's own attributes are installed
/// first, so every database created this way — and every peer that receives
/// the resulting handshake — knows `:db/txInstant`.
///
/// # Errors
/// Returns [`SchemaFormError`] for malformed attribute definitions.
pub fn schema_from_edn(forms: &[Edn]) -> Result<(Schema, Idents), SchemaFormError> {
    let mut schema = Schema::default();
    let mut idents = Idents::default();
    bootstrap::install(&mut schema, &mut idents);

    let ids = register_idents(forms, &mut idents)?;
    for (form, id) in forms.iter().zip(&ids) {
        if form.get(&kw("db.protect/ident")).is_some() {
            schema.insert_class(protection_class(form, *id)?);
        }
    }

    for (index, form) in forms.iter().enumerate() {
        if form.get(&kw("db.protect/ident")).is_some() {
            continue;
        }
        let id = ids[index];
        let ident = form
            .get(&kw("db/ident"))
            .and_then(Edn::as_keyword)
            .ok_or(SchemaFormError::MissingIdent)?
            .clone();
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
        let indexed = flag("db/index") || unique.is_some();
        if let Some(protection) = form.get(&kw("db/protection")) {
            let class_id = protected_class_id(
                protection,
                &ident.to_string(),
                value_type,
                unique.is_some(),
                indexed,
                &schema,
                &idents,
            )?;
            schema.set_protection(id, ProtectionTimeline::protected_from(0, class_id));
        }
        // Documentation is part of the pre-basis seed too. Without it, a
        // schema file that has always carried a `:db/doc` would plan a
        // documentation change against the database it created.
        if let Some(Edn::Str(doc)) = form.get(&kw("db/doc")) {
            schema.set_doc(id, Some(doc.clone()));
        }
        schema.insert(Attribute {
            id,
            value_type,
            cardinality,
            unique,
            is_component: flag("db/isComponent"),
            indexed,
            no_history: flag("db/noHistory"),
        });
    }
    Ok((schema, idents))
}

/// Assigns an entity id to every form and registers its ident.
///
/// This happens before anything is parsed so that an attribute may name a
/// protection class defined after it — the order of maps in a schema vector
/// is not meaningful anywhere else either.
fn register_idents(forms: &[Edn], idents: &mut Idents) -> Result<Vec<EntityId>, SchemaFormError> {
    let mut ids = Vec::with_capacity(forms.len());
    for (index, form) in forms.iter().enumerate() {
        if !matches!(form, Edn::Map(_)) {
            return Err(SchemaFormError::NotAMap(form.to_string()));
        }
        let id = EntityId::new(Partition::Db as u32, FIRST_ATTR_ID + index as u64);
        let ident = form
            .get(&kw("db.protect/ident"))
            .or_else(|| form.get(&kw("db/ident")))
            .and_then(Edn::as_keyword)
            .ok_or(SchemaFormError::MissingIdent)?
            .clone();
        // An ident the engine already installed is its own attribute, not a
        // duplicate of another declaration in this file. Naming the two apart
        // is what tells an operator whether to rename their attribute or to
        // delete a line the engine now provides.
        if let Some(existing) = idents.entid(&ident) {
            return Err(
                if existing.partition() == Partition::Db as u32
                    && existing.sequence() < FIRST_ATTR_ID
                {
                    SchemaFormError::EngineAttribute(ident.to_string())
                } else {
                    SchemaFormError::DuplicateIdent(ident.to_string())
                },
            );
        }
        idents.insert(ident, id);
        ids.push(id);
    }
    Ok(ids)
}

/// Resolves an attribute's `:db/protection` to a class entity id, enforcing
/// the install-time rules that keep protected datoms out of AVET and VAET.
fn protected_class_id(
    protection: &Edn,
    attr: &str,
    value_type: ValueType,
    unique: bool,
    indexed: bool,
    schema: &Schema,
    idents: &Idents,
) -> Result<EntityId, SchemaFormError> {
    let class = protection
        .as_keyword()
        .ok_or_else(|| SchemaFormError::BadProtection(attr.to_owned()))?;
    let conflict = if unique {
        Some(":db/unique")
    } else if indexed {
        Some(":db/index")
    } else if value_type == ValueType::Ref {
        Some(":db.type/ref")
    } else {
        None
    };
    if let Some(conflict) = conflict {
        return Err(SchemaFormError::ProtectionConflict {
            attr: attr.to_owned(),
            conflict: conflict.to_owned(),
        });
    }
    idents
        .entid(class)
        .filter(|class_id| schema.class(*class_id).is_some())
        .ok_or_else(|| SchemaFormError::UnknownProtectionClass {
            attr: attr.to_owned(),
            class: class.to_string(),
        })
}

/// Parses one `:db.protect/ident` map into a class.
fn protection_class(form: &Edn, id: EntityId) -> Result<ProtectionClass, SchemaFormError> {
    let ident = form
        .get(&kw("db.protect/ident"))
        .and_then(Edn::as_keyword)
        .ok_or(SchemaFormError::MissingIdent)?
        .to_string();
    let bad = |reason: String| SchemaFormError::BadProtectionClass {
        ident: ident.clone(),
        reason,
    };

    let Some(Edn::Str(key_id)) = form.get(&kw("db.protect/key")) else {
        return Err(bad(":db.protect/key must be a key identity string".into()));
    };
    if key_id.is_empty() {
        return Err(bad(":db.protect/key must not be empty".into()));
    }

    let name = |field: &str| -> Result<Option<String>, SchemaFormError> {
        match form.get(&kw(field)) {
            None => Ok(None),
            Some(Edn::Keyword(keyword)) => Ok(Some(keyword.name.clone())),
            Some(other) => Err(bad(format!(":{field} must be a keyword, found {other}"))),
        }
    };

    let algorithm = match name("db.protect/algorithm")?.as_deref() {
        None | Some("aes-256-gcm-siv") => SealAlgorithm::Aes256GcmSiv,
        Some(other) => return Err(bad(format!("unknown algorithm {other}"))),
    };
    let scope = match name("db.protect/scope")?.as_deref() {
        None | Some("attribute") => ProtectionScope::Attribute,
        Some("entity") => ProtectionScope::Entity,
        Some(other) => return Err(bad(format!("unknown scope {other}"))),
    };
    let on_missing_key = match name("db.protect/on-missing-key")?.as_deref() {
        None | Some("redact") => MissingKeyPolicy::Redact,
        Some("hide") => MissingKeyPolicy::Hide,
        Some("error") => MissingKeyPolicy::Error,
        Some(other) => return Err(bad(format!("unknown on-missing-key policy {other}"))),
    };
    let legacy_plaintext = match name("db.protect/legacy-plaintext")?.as_deref() {
        None | Some("redact") => LegacyPlaintextPolicy::Redact,
        Some("pass-through") => LegacyPlaintextPolicy::PassThrough,
        Some(other) => return Err(bad(format!("unknown legacy-plaintext policy {other}"))),
    };
    let padding = match form.get(&kw("db.protect/padding")) {
        None => None,
        Some(Edn::Long(bytes)) if *bytes >= MIN_PADDING => Some(
            u32::try_from(*bytes)
                .map_err(|_| bad(format!("padding {bytes} does not fit in 32 bits")))?,
        ),
        Some(Edn::Long(bytes)) => {
            return Err(bad(format!(
                "padding {bytes} is below the {MIN_PADDING}-byte minimum"
            )));
        }
        Some(other) => {
            return Err(bad(format!(
                ":db.protect/padding must be a long, found {other}"
            )));
        }
    };
    let current_epoch = match form.get(&kw("db.protect/epoch")) {
        None => FIRST_KEY_EPOCH,
        Some(Edn::Long(epoch)) if *epoch >= 1 => u32::try_from(*epoch)
            .map_err(|_| bad(format!("epoch {epoch} does not fit in 32 bits")))?,
        Some(other) => {
            return Err(bad(format!(
                ":db.protect/epoch must be a positive long, found {other}"
            )));
        }
    };

    Ok(ProtectionClass {
        id,
        key_id: key_id.clone(),
        algorithm,
        scope,
        padding,
        on_missing_key,
        legacy_plaintext,
        current_epoch,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use corium_query::edn::read_one;

    fn forms(text: &str) -> Vec<Edn> {
        match read_one(text).expect("schema EDN parses") {
            Edn::Vector(items) => items,
            other => panic!("expected a vector, got {other}"),
        }
    }

    const PROTECTED: &str = r#"
[{:db.protect/ident :protect/pii
  :db.protect/key "file:/etc/corium/pii.key"
  :db.protect/padding 64
  :db.protect/on-missing-key :db.protect.missing/hide}
 {:db/ident :person/ssn
  :db/valueType :db.type/string
  :db/cardinality :db.cardinality/one
  :db/protection :protect/pii}
 {:db/ident :person/name
  :db/valueType :db.type/string}]
"#;

    #[test]
    fn installs_a_class_and_binds_the_attribute_to_it() {
        let (schema, idents) = schema_from_edn(&forms(PROTECTED)).expect("schema installs");
        let class_id = idents
            .entid(&corium_core::Keyword::new(Some("protect"), "pii"))
            .expect("class ident is registered");
        let class = schema.class(class_id).expect("class is installed");
        assert_eq!(class.key_id, "file:/etc/corium/pii.key");
        assert_eq!(class.padding, Some(64));
        assert_eq!(class.on_missing_key, MissingKeyPolicy::Hide);
        // Defaults come from the design's defaults, not from zero values.
        assert_eq!(class.scope, ProtectionScope::Attribute);
        assert_eq!(class.legacy_plaintext, LegacyPlaintextPolicy::Redact);
        assert_eq!(class.current_epoch, FIRST_KEY_EPOCH);

        let ssn = idents
            .entid(&corium_core::Keyword::new(Some("person"), "ssn"))
            .expect("ssn ident");
        assert_eq!(schema.protection(ssn).current(), Some(class_id));
        assert!(schema.is_protected(ssn));
        assert_eq!(schema.protection_class(ssn).map(|c| c.id), Some(class_id));

        let name = idents
            .entid(&corium_core::Keyword::new(Some("person"), "name"))
            .expect("name ident");
        assert!(!schema.is_protected(name));
        assert!(schema.protection(name).entries().is_empty());
    }

    #[test]
    fn a_class_may_be_declared_after_the_attribute_that_names_it() {
        let text = r#"
[{:db/ident :person/ssn
  :db/valueType :db.type/string
  :db/protection :protect/pii}
 {:db.protect/ident :protect/pii
  :db.protect/key "file:/etc/corium/pii.key"}]
"#;
        let (schema, idents) = schema_from_edn(&forms(text)).expect("schema installs");
        let ssn = idents
            .entid(&corium_core::Keyword::new(Some("person"), "ssn"))
            .expect("ssn ident");
        assert!(schema.is_protected(ssn));
    }

    #[test]
    fn protection_is_rejected_with_anything_that_needs_value_order() {
        for (conflict, attr) in [
            (
                ":db/index",
                r"{:db/ident :person/ssn :db/valueType :db.type/string
                    :db/index true :db/protection :protect/pii}",
            ),
            (
                ":db/unique",
                r"{:db/ident :person/ssn :db/valueType :db.type/string
                    :db/unique :db.unique/identity :db/protection :protect/pii}",
            ),
            (
                ":db.type/ref",
                r"{:db/ident :person/ssn :db/valueType :db.type/ref
                    :db/protection :protect/pii}",
            ),
        ] {
            let text =
                format!(r#"[{{:db.protect/ident :protect/pii :db.protect/key "file:/k"}} {attr}]"#);
            let error = schema_from_edn(&forms(&text)).expect_err("conflict must be rejected");
            assert_eq!(
                error,
                SchemaFormError::ProtectionConflict {
                    attr: ":person/ssn".into(),
                    conflict: conflict.into(),
                }
            );
        }
    }

    #[test]
    fn protection_naming_an_undeclared_class_is_rejected() {
        let text = r"[{:db/ident :person/ssn :db/valueType :db.type/string
                        :db/protection :protect/nope}]";
        assert_eq!(
            schema_from_edn(&forms(text)).expect_err("unknown class must be rejected"),
            SchemaFormError::UnknownProtectionClass {
                attr: ":person/ssn".into(),
                class: ":protect/nope".into(),
            }
        );
    }

    #[test]
    fn malformed_class_declarations_are_rejected() {
        for (text, expected) in [
            (
                r"[{:db.protect/ident :protect/pii}]",
                ":db.protect/key must be a key identity string",
            ),
            (
                r#"[{:db.protect/ident :protect/pii :db.protect/key "file:/k"
                     :db.protect/padding 8}]"#,
                "padding 8 is below the 16-byte minimum",
            ),
            (
                r#"[{:db.protect/ident :protect/pii :db.protect/key "file:/k"
                     :db.protect/scope :db.protect.scope/database}]"#,
                "unknown scope database",
            ),
            (
                r#"[{:db.protect/ident :protect/pii :db.protect/key "file:/k"
                     :db.protect/on-missing-key :db.protect.missing/shrug}]"#,
                "unknown on-missing-key policy shrug",
            ),
            (
                r#"[{:db.protect/ident :protect/pii :db.protect/key "file:/k"
                     :db.protect/algorithm :db.protect.alg/rot13}]"#,
                "unknown algorithm rot13",
            ),
        ] {
            let error = schema_from_edn(&forms(text)).expect_err("class must be rejected");
            assert_eq!(
                error,
                SchemaFormError::BadProtectionClass {
                    ident: ":protect/pii".into(),
                    reason: expected.into(),
                }
            );
        }
    }
}
