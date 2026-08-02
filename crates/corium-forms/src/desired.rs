//! The normalized desired schema a `corium schema update` compares against a
//! database (`docs/design/schema-migrations.md`).
//!
//! Both TOML and EDN inputs normalize to the same ordered map keyed by
//! canonical ident. Nothing here allocates an attribute entity id: ids belong
//! to the installed schema and are never inferred from file order after the
//! database exists. Database *creation* keeps its positional allocation
//! ([`crate::schemaform::schema_from_edn`]) so embedded, WASM, and fixture
//! databases stay reproducible.
//!
//! Matching is exact by ident. A removed ident and an added ident are two
//! changes, never a rename, however similar their definitions: an incorrect
//! rename aliases two meanings permanently.

use std::collections::BTreeMap;

use corium_core::migration::{cardinality_name, digest_hex, flag, unique_name, value_type_name};
use corium_core::{Cardinality, Keyword, Unique, ValueType};
use corium_query::edn::Edn;
use thiserror::Error;

/// One attribute as a schema file declares it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DesiredAttribute {
    /// Canonical ident.
    pub ident: Keyword,
    /// Stored value type.
    pub value_type: ValueType,
    /// Cardinality.
    pub cardinality: Cardinality,
    /// Uniqueness mode.
    pub unique: Option<Unique>,
    /// Whether AVET coverage is requested. Uniqueness implies it.
    pub indexed: bool,
    /// Component reference flag.
    pub is_component: bool,
    /// Whether history storage is disabled.
    pub no_history: bool,
    /// Documentation.
    pub doc: Option<String>,
}

impl DesiredAttribute {
    /// The rendered ident (`:person/email`).
    #[must_use]
    pub fn rendered_ident(&self) -> String {
        self.ident.to_string()
    }

    /// A one-line rendering of the whole declaration, as an `add` step prints
    /// it.
    #[must_use]
    pub fn rendering(&self) -> String {
        use std::fmt::Write as _;

        let mut out = format!(
            "{} cardinality-{}",
            value_type_name(self.value_type),
            cardinality_name(self.cardinality)
        );
        if let Some(unique) = self.unique {
            let _ = write!(out, " unique-{}", unique_name(Some(unique)));
        } else if self.indexed {
            out.push_str(" indexed");
        }
        if self.is_component {
            out.push_str(" component");
        }
        if self.no_history {
            out.push_str(" no-history");
        }
        out
    }
}

/// A desired schema file, normalized and ordered by canonical ident.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DesiredSchema {
    attrs: BTreeMap<String, DesiredAttribute>,
}

/// Failure to read a desired schema file.
#[derive(Debug, Error)]
pub enum DesiredSchemaError {
    /// A TOML document did not parse or normalize.
    #[cfg(feature = "toml")]
    #[error("{0}")]
    Toml(#[from] crate::toml_schema::TomlSchemaError),
    /// An EDN attribute form is not a map.
    #[error("attribute definition must be a map: {0}")]
    NotAMap(String),
    /// `:db/ident` is missing or is not a keyword.
    #[error("attribute requires a :db/ident keyword")]
    MissingIdent,
    /// A property value is missing or not part of Corium's schema model.
    #[error("unknown or missing {property} {value:?} for {ident}")]
    BadProperty {
        /// Property being read, spelled as its `:db/…` ident.
        property: &'static str,
        /// Attribute being normalized.
        ident: String,
        /// Offending source value.
        value: String,
    },
    /// The same canonical ident is declared twice.
    #[error("duplicate attribute {0}")]
    DuplicateAttribute(String),
}

impl DesiredSchema {
    /// Builds a desired schema from already normalized attributes.
    ///
    /// # Errors
    /// Returns [`DesiredSchemaError::DuplicateAttribute`] when two
    /// declarations share a canonical ident.
    pub fn new(attributes: Vec<DesiredAttribute>) -> Result<Self, DesiredSchemaError> {
        let mut attrs = BTreeMap::new();
        for attribute in attributes {
            let ident = attribute.rendered_ident();
            if attrs.insert(ident.clone(), attribute).is_some() {
                return Err(DesiredSchemaError::DuplicateAttribute(ident));
            }
        }
        Ok(Self { attrs })
    }

    /// Parses a hierarchical TOML schema document.
    ///
    /// # Errors
    /// Returns [`DesiredSchemaError::Toml`] for invalid TOML, unsupported
    /// values, invalid names, or duplicate idents.
    #[cfg(feature = "toml")]
    pub fn from_toml(input: &str) -> Result<Self, DesiredSchemaError> {
        let definitions = crate::toml_schema::parse(input)?;
        Self::new(
            definitions
                .into_iter()
                .map(|definition| DesiredAttribute {
                    ident: definition.ident(),
                    value_type: definition.value_type,
                    cardinality: definition.cardinality,
                    unique: definition.unique,
                    indexed: definition.indexed,
                    is_component: definition.component,
                    no_history: definition.no_history,
                    doc: definition.doc,
                })
                .collect(),
        )
    }

    /// Parses Datomic-style EDN attribute maps.
    ///
    /// # Errors
    /// Returns [`DesiredSchemaError`] for malformed attribute definitions.
    pub fn from_edn(forms: &[Edn]) -> Result<Self, DesiredSchemaError> {
        let mut attributes = Vec::with_capacity(forms.len());
        for form in forms {
            attributes.push(attribute_from_edn(form)?);
        }
        Self::new(attributes)
    }

    /// Looks up a declaration by rendered ident.
    #[must_use]
    pub fn get(&self, ident: &str) -> Option<&DesiredAttribute> {
        self.attrs.get(ident)
    }

    /// Whether the file declares nothing at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.attrs.is_empty()
    }

    /// Number of declarations.
    #[must_use]
    pub fn len(&self) -> usize {
        self.attrs.len()
    }

    /// Iterates declarations in canonical ident order.
    pub fn iter(&self) -> impl Iterator<Item = (&String, &DesiredAttribute)> {
        self.attrs.iter()
    }

    /// The digest of the normalized desired schema.
    ///
    /// Two files that declare the same attributes — in either syntax, in any
    /// order — have the same digest. `--apply` submits it so the transactor
    /// can recompute the logical plan rather than trust an opaque token.
    #[must_use]
    pub fn digest(&self) -> String {
        let mut canonical = Vec::new();
        let mut field = |value: &str| {
            canonical.extend_from_slice(value.len().to_string().as_bytes());
            canonical.push(b':');
            canonical.extend_from_slice(value.as_bytes());
            canonical.push(b'\n');
        };
        field("corium.desired-schema");
        field("1");
        field(&self.attrs.len().to_string());
        for (ident, attribute) in &self.attrs {
            field(ident);
            field(value_type_name(attribute.value_type));
            field(cardinality_name(attribute.cardinality));
            field(unique_name(attribute.unique));
            field(flag(attribute.indexed));
            field(flag(attribute.is_component));
            field(flag(attribute.no_history));
            match &attribute.doc {
                Some(doc) => {
                    field("some");
                    field(doc);
                }
                None => field("none"),
            }
        }
        digest_hex(&canonical)
    }
}

fn attribute_from_edn(form: &Edn) -> Result<DesiredAttribute, DesiredSchemaError> {
    if !matches!(form, Edn::Map(_)) {
        return Err(DesiredSchemaError::NotAMap(form.to_string()));
    }
    let ident = form
        .get(&Edn::keyword("db/ident"))
        .and_then(Edn::as_keyword)
        .ok_or(DesiredSchemaError::MissingIdent)?
        .clone();
    let rendered = ident.to_string();
    let bad = |property: &'static str, value: String| DesiredSchemaError::BadProperty {
        property,
        ident: rendered.clone(),
        value,
    };

    let value_type_name = form
        .get(&Edn::keyword("db/valueType"))
        .and_then(Edn::as_keyword)
        .map(|keyword| keyword.name.clone())
        .ok_or_else(|| bad(":db/valueType", "<missing>".to_owned()))?;
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
        other => return Err(bad(":db/valueType", other.to_owned())),
    };
    let cardinality = match form
        .get(&Edn::keyword("db/cardinality"))
        .and_then(Edn::as_keyword)
        .map(|keyword| keyword.name.clone())
    {
        None => Cardinality::One,
        Some(name) => match name.as_str() {
            "one" => Cardinality::One,
            "many" => Cardinality::Many,
            other => return Err(bad(":db/cardinality", other.to_owned())),
        },
    };
    let unique = match form
        .get(&Edn::keyword("db/unique"))
        .and_then(Edn::as_keyword)
        .map(|keyword| keyword.name.clone())
    {
        None => None,
        Some(name) => match name.as_str() {
            "identity" => Some(Unique::Identity),
            "value" => Some(Unique::Value),
            other => return Err(bad(":db/unique", other.to_owned())),
        },
    };
    let boolean = |name: &str| form.get(&Edn::keyword(name)) == Some(&Edn::Bool(true));
    let doc = match form.get(&Edn::keyword("db/doc")) {
        None => None,
        Some(Edn::Str(text)) => Some(text.clone()),
        Some(other) => return Err(bad(":db/doc", other.to_string())),
    };

    Ok(DesiredAttribute {
        ident,
        value_type,
        cardinality,
        unique,
        // Uniqueness implies AVET coverage, matching how the installed schema
        // records it; without this an `add unique` plan would also claim a
        // redundant index change.
        indexed: boolean("db/index") || unique.is_some(),
        is_component: boolean("db/isComponent"),
        no_history: boolean("db/noHistory"),
        doc,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use corium_query::edn::read_all;

    const TOML_SOURCE: &str = r#"
schema-version = 1

[[entity]]
name = "person"

[entity.attributes]
email = { type = "string", unique = "identity", doc = "primary contact" }
tags = { type = "keyword", many = true }
"#;

    const EDN_SOURCE: &str = r#"
    {:db/ident :person/tags :db/valueType :db.type/keyword :db/cardinality :db.cardinality/many}
    {:db/ident :person/email :db/valueType :db.type/string :db/unique :db.unique/identity
     :db/doc "primary contact"}
    "#;

    fn edn_schema(source: &str) -> DesiredSchema {
        DesiredSchema::from_edn(&read_all(source).expect("EDN parses")).expect("schema normalizes")
    }

    #[test]
    fn toml_and_edn_normalize_to_the_same_schema() {
        let from_toml = DesiredSchema::from_toml(TOML_SOURCE).expect("TOML parses");
        let from_edn = edn_schema(EDN_SOURCE);
        assert_eq!(from_toml, from_edn);
        assert_eq!(from_toml.digest(), from_edn.digest());
    }

    #[test]
    fn declaration_order_does_not_change_the_digest() {
        let forward = edn_schema(EDN_SOURCE);
        let reverse = edn_schema(
            r#"
        {:db/ident :person/email :db/valueType :db.type/string :db/unique :db.unique/identity
         :db/doc "primary contact"}
        {:db/ident :person/tags :db/valueType :db.type/keyword :db/cardinality :db.cardinality/many}
        "#,
        );
        assert_eq!(forward.digest(), reverse.digest());
    }

    #[test]
    fn every_property_reaches_the_digest() {
        let base = edn_schema("{:db/ident :a/b :db/valueType :db.type/string}");
        for source in [
            "{:db/ident :a/c :db/valueType :db.type/string}",
            "{:db/ident :a/b :db/valueType :db.type/long}",
            "{:db/ident :a/b :db/valueType :db.type/string :db/cardinality :db.cardinality/many}",
            "{:db/ident :a/b :db/valueType :db.type/string :db/unique :db.unique/value}",
            "{:db/ident :a/b :db/valueType :db.type/string :db/index true}",
            "{:db/ident :a/b :db/valueType :db.type/string :db/isComponent true}",
            "{:db/ident :a/b :db/valueType :db.type/string :db/noHistory true}",
            "{:db/ident :a/b :db/valueType :db.type/string :db/doc \"why\"}",
        ] {
            assert_ne!(base.digest(), edn_schema(source).digest(), "{source}");
        }
    }

    #[test]
    fn uniqueness_implies_index_coverage_in_both_syntaxes() {
        let toml = DesiredSchema::from_toml(
            r#"
[[attribute]]
name = "id"
type = "uuid"
unique = "value"
"#,
        )
        .expect("TOML parses");
        assert!(toml.get(":id").expect("declared").indexed);
        let edn =
            edn_schema("{:db/ident :id :db/valueType :db.type/uuid :db/unique :db.unique/value}");
        assert!(edn.get(":id").expect("declared").indexed);
    }

    #[test]
    fn malformed_declarations_are_rejected() {
        let duplicate = DesiredSchema::from_edn(
            &read_all(
                "{:db/ident :a/b :db/valueType :db.type/string}
                 {:db/ident :a/b :db/valueType :db.type/long}",
            )
            .expect("EDN parses"),
        )
        .expect_err("duplicate idents must fail");
        assert_eq!(duplicate.to_string(), "duplicate attribute :a/b");

        let missing =
            DesiredSchema::from_edn(&read_all("{:db/valueType :db.type/string}").unwrap())
                .expect_err("missing ident must fail");
        assert_eq!(
            missing.to_string(),
            "attribute requires a :db/ident keyword"
        );

        let bad_type = DesiredSchema::from_edn(
            &read_all("{:db/ident :a/b :db/valueType :db.type/tuple}").unwrap(),
        )
        .expect_err("unknown type must fail");
        assert!(bad_type.to_string().contains(":db/valueType"), "{bad_type}");

        let bad_doc = DesiredSchema::from_edn(
            &read_all("{:db/ident :a/b :db/valueType :db.type/string :db/doc 7}").unwrap(),
        )
        .expect_err("non-string doc must fail");
        assert!(bad_doc.to_string().contains(":db/doc"), "{bad_doc}");

        let not_a_map =
            DesiredSchema::from_edn(&read_all("[1 2]").unwrap()).expect_err("non-map must fail");
        assert!(
            not_a_map.to_string().contains("must be a map"),
            "{not_a_map}"
        );
    }

    #[test]
    fn declarations_render_for_human_plans() {
        let schema = edn_schema(
            "{:db/ident :a/b :db/valueType :db.type/ref :db/cardinality :db.cardinality/many
              :db/isComponent true :db/noHistory true}",
        );
        assert_eq!(
            schema.get(":a/b").expect("declared").rendering(),
            "ref cardinality-many component no-history"
        );
        assert_eq!(
            edn_schema("{:db/ident :a/b :db/valueType :db.type/string :db/index true}")
                .get(":a/b")
                .expect("declared")
                .rendering(),
            "string cardinality-one indexed"
        );
    }
}
