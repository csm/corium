//! Hierarchical TOML schema declarations.
//!
//! `[[entity]]` declarations are authoring sugar: their names become keyword
//! namespaces, but they do not install or enforce entity types. `[[attribute]]`
//! declarations expose the same model without an entity block.

use std::collections::{BTreeMap, BTreeSet};

use corium_core::{Cardinality, Keyword, Unique, ValueType};
use corium_query::edn::Edn;
use serde::Deserialize;
use thiserror::Error;

/// One normalized attribute declaration, independent of its source syntax.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttributeDefinition {
    /// Optional authoring group, mapped to the keyword namespace.
    pub group: Option<String>,
    /// Attribute name within its group, or its complete unnamespaced name.
    pub name: String,
    /// Stored value type.
    pub value_type: ValueType,
    /// Whether entities may hold one or many values for the attribute.
    pub cardinality: Cardinality,
    /// Optional uniqueness behavior.
    pub unique: Option<Unique>,
    /// Whether the attribute requests AVET coverage.
    pub indexed: bool,
    /// Whether a reference is a component of its parent.
    pub component: bool,
    /// Whether history storage is disabled for the attribute.
    pub no_history: bool,
}

impl AttributeDefinition {
    /// Returns the canonical Corium keyword ident.
    #[must_use]
    pub fn ident(&self) -> Keyword {
        Keyword::new(self.group.as_deref(), &self.name)
    }

    /// Converts the declaration to the existing Datomic-style EDN schema form.
    #[must_use]
    pub fn to_edn(&self) -> Edn {
        let mut pairs = vec![
            (kw("db/ident"), Edn::Keyword(self.ident())),
            (
                kw("db/valueType"),
                kw(&format!("db.type/{}", value_type_name(self.value_type))),
            ),
            (
                kw("db/cardinality"),
                kw(match self.cardinality {
                    Cardinality::One => "db.cardinality/one",
                    Cardinality::Many => "db.cardinality/many",
                }),
            ),
        ];
        if let Some(unique) = self.unique {
            pairs.push((
                kw("db/unique"),
                kw(match unique {
                    Unique::Identity => "db.unique/identity",
                    Unique::Value => "db.unique/value",
                }),
            ));
        }
        if self.indexed {
            pairs.push((kw("db/index"), Edn::Bool(true)));
        }
        if self.component {
            pairs.push((kw("db/isComponent"), Edn::Bool(true)));
        }
        if self.no_history {
            pairs.push((kw("db/noHistory"), Edn::Bool(true)));
        }
        pairs.sort_unstable();
        Edn::Map(pairs)
    }
}

/// TOML schema parsing or normalization failure.
#[derive(Debug, Error)]
pub enum TomlSchemaError {
    /// The input is not valid TOML or does not match the schema document shape.
    #[error("invalid TOML schema: {0}")]
    Parse(#[from] toml::de::Error),
    /// The document declares an unsupported format version.
    #[error("unsupported schema-version {0}; expected 1")]
    UnsupportedVersion(u32),
    /// A group or attribute name is empty or uses the reserved `/` separator.
    #[error("invalid {kind} name {name:?}: names must be non-empty and may not contain '/'")]
    InvalidName {
        /// Kind of declaration containing the name.
        kind: &'static str,
        /// Invalid source name.
        name: String,
    },
    /// A value type name is not part of Corium's schema model.
    #[error("unknown value type {value:?} for {ident}")]
    UnknownValueType {
        /// Attribute being normalized.
        ident: String,
        /// Unknown source value.
        value: String,
    },
    /// A cardinality name is neither `one` nor `many`.
    #[error("unknown cardinality {value:?} for {ident}")]
    UnknownCardinality {
        /// Attribute being normalized.
        ident: String,
        /// Unknown source value.
        value: String,
    },
    /// A uniqueness mode is neither `identity` nor `value`.
    #[error("unknown unique mode {value:?} for {ident}")]
    UnknownUnique {
        /// Attribute being normalized.
        ident: String,
        /// Unknown source value.
        value: String,
    },
    /// Both cardinality spellings were supplied for one declaration.
    #[error("{ident} specifies both `cardinality` and `many`; use only one")]
    ConflictingCardinality {
        /// Attribute being normalized.
        ident: String,
    },
    /// Grouped and/or flat syntax declared the same canonical attribute twice.
    #[error("duplicate attribute {0}")]
    DuplicateAttribute(String),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
struct SchemaDocument {
    #[serde(default = "default_schema_version")]
    schema_version: u32,
    #[serde(default, rename = "entity")]
    entities: Vec<EntityDeclaration>,
    #[serde(default, rename = "attribute")]
    attributes: Vec<FlatAttributeDeclaration>,
}

const fn default_schema_version() -> u32 {
    1
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EntityDeclaration {
    name: String,
    attributes: BTreeMap<String, RawAttribute>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RawAttribute {
    Shorthand(String),
    Detailed(AttributeOptions),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
struct FlatAttributeDeclaration {
    #[serde(default)]
    group: Option<String>,
    name: String,
    #[serde(flatten)]
    options: AttributeOptions,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
struct AttributeOptions {
    #[serde(rename = "type")]
    value_type: String,
    #[serde(default)]
    cardinality: Option<String>,
    #[serde(default)]
    many: Option<bool>,
    #[serde(default)]
    unique: Option<String>,
    #[serde(default)]
    index: bool,
    #[serde(default)]
    component: bool,
    #[serde(default)]
    no_history: bool,
}

impl RawAttribute {
    fn into_options(self) -> AttributeOptions {
        match self {
            Self::Shorthand(value_type) => AttributeOptions {
                value_type,
                cardinality: None,
                many: None,
                unique: None,
                index: false,
                component: false,
                no_history: false,
            },
            Self::Detailed(options) => options,
        }
    }
}

/// Parses hierarchical TOML into normalized attribute declarations.
///
/// Grouped syntax uses `[[entity]]` followed by `[entity.attributes]`. The
/// entity name supplies the attribute group. Flat syntax uses `[[attribute]]`
/// with an optional `group`. Entity groups are not persisted as types.
///
/// # Errors
/// Returns [`TomlSchemaError`] for invalid TOML, unsupported values, invalid
/// names, conflicting cardinality spellings, or duplicate canonical idents.
pub fn parse(input: &str) -> Result<Vec<AttributeDefinition>, TomlSchemaError> {
    let document: SchemaDocument = toml::from_str(input)?;
    if document.schema_version != 1 {
        return Err(TomlSchemaError::UnsupportedVersion(document.schema_version));
    }

    let mut definitions = Vec::new();
    for entity in document.entities {
        validate_name("entity", &entity.name)?;
        for (name, raw) in entity.attributes {
            definitions.push(normalize(
                Some(entity.name.clone()),
                name,
                raw.into_options(),
            )?);
        }
    }
    for attribute in document.attributes {
        definitions.push(normalize(
            attribute.group,
            attribute.name,
            attribute.options,
        )?);
    }

    let mut seen = BTreeSet::new();
    for definition in &definitions {
        let ident = definition.ident().to_string();
        if !seen.insert(ident.clone()) {
            return Err(TomlSchemaError::DuplicateAttribute(ident));
        }
    }
    Ok(definitions)
}

/// Parses TOML and emits equivalent EDN attribute maps for existing APIs.
///
/// # Errors
/// Returns the same failures as [`parse`].
pub fn parse_edn(input: &str) -> Result<Vec<Edn>, TomlSchemaError> {
    parse(input).map(|definitions| {
        definitions
            .iter()
            .map(AttributeDefinition::to_edn)
            .collect()
    })
}

fn normalize(
    group: Option<String>,
    name: String,
    options: AttributeOptions,
) -> Result<AttributeDefinition, TomlSchemaError> {
    if let Some(group) = &group {
        validate_name("group", group)?;
    }
    validate_name("attribute", &name)?;
    let ident = display_ident(group.as_deref(), &name);
    let value_type =
        parse_value_type(&options.value_type).ok_or_else(|| TomlSchemaError::UnknownValueType {
            ident: ident.clone(),
            value: options.value_type,
        })?;
    let cardinality = match (options.cardinality, options.many) {
        (Some(_), Some(_)) => {
            return Err(TomlSchemaError::ConflictingCardinality { ident });
        }
        (Some(value), None) => {
            parse_cardinality(&value).ok_or_else(|| TomlSchemaError::UnknownCardinality {
                ident: ident.clone(),
                value,
            })?
        }
        (None, Some(true)) => Cardinality::Many,
        (None, Some(false) | None) => Cardinality::One,
    };
    let unique = options
        .unique
        .map(|value| {
            parse_unique(&value).ok_or_else(|| TomlSchemaError::UnknownUnique {
                ident: ident.clone(),
                value,
            })
        })
        .transpose()?;

    Ok(AttributeDefinition {
        group,
        name,
        value_type,
        cardinality,
        unique,
        indexed: options.index,
        component: options.component,
        no_history: options.no_history,
    })
}

fn validate_name(kind: &'static str, name: &str) -> Result<(), TomlSchemaError> {
    if name.is_empty() || name.contains('/') {
        Err(TomlSchemaError::InvalidName {
            kind,
            name: name.to_owned(),
        })
    } else {
        Ok(())
    }
}

fn display_ident(group: Option<&str>, name: &str) -> String {
    group.map_or_else(|| format!(":{name}"), |group| format!(":{group}/{name}"))
}

fn parse_value_type(value: &str) -> Option<ValueType> {
    match value {
        "boolean" => Some(ValueType::Bool),
        "long" => Some(ValueType::Long),
        "double" => Some(ValueType::Double),
        "instant" => Some(ValueType::Instant),
        "uuid" => Some(ValueType::Uuid),
        "keyword" => Some(ValueType::Keyword),
        "string" => Some(ValueType::Str),
        "bytes" => Some(ValueType::Bytes),
        "ref" => Some(ValueType::Ref),
        _ => None,
    }
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

fn parse_cardinality(value: &str) -> Option<Cardinality> {
    match value {
        "one" => Some(Cardinality::One),
        "many" => Some(Cardinality::Many),
        _ => None,
    }
}

fn parse_unique(value: &str) -> Option<Unique> {
    match value {
        "identity" => Some(Unique::Identity),
        "value" => Some(Unique::Value),
        _ => None,
    }
}

fn kw(text: &str) -> Edn {
    Edn::keyword(text)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schemaform::schema_from_edn;

    const SCHEMA: &str = r#"
schema-version = 1

[[entity]]
name = "person"

[entity.attributes]
age = "long"
id = { type = "uuid", unique = "identity" }
name = { type = "string", index = true }
tags = { type = "keyword", many = true }
address = { type = "ref", component = true }

[[entity]]
name = "organization"

[entity.attributes]
name = "string"
employees = { type = "ref", cardinality = "many", no-history = true }

[[attribute]]
name = "created-at"
type = "instant"
index = true

[[attribute]]
group = "audit"
name = "created-by"
type = "ref"
"#;

    #[test]
    fn parses_grouped_and_flat_attributes() {
        let definitions = parse(SCHEMA).expect("schema parses");
        assert_eq!(definitions.len(), 9);
        assert_eq!(definitions[0].ident().to_string(), ":person/address");
        assert_eq!(definitions[1].ident().to_string(), ":person/age");
        assert_eq!(definitions[3].ident().to_string(), ":person/name");
        assert_eq!(
            definitions[5].ident().to_string(),
            ":organization/employees"
        );
        assert_eq!(definitions[7].ident().to_string(), ":created-at");
        assert_eq!(definitions[8].ident().to_string(), ":audit/created-by");
        assert_eq!(definitions[2].unique, Some(Unique::Identity));
        assert_eq!(definitions[4].cardinality, Cardinality::Many);
        assert!(definitions[5].no_history);
    }

    #[test]
    fn generated_edn_installs_through_existing_schema_path() {
        let forms = parse_edn(SCHEMA).expect("schema parses");
        let (schema, idents) = schema_from_edn(&forms).expect("schema installs");
        assert_eq!(schema.iter().count(), 9);
        let tags = idents
            .entid(&Keyword::new(Some("person"), "tags"))
            .expect("tags ident");
        assert_eq!(
            schema.get(tags).expect("tags attribute").cardinality,
            Cardinality::Many
        );
        let id = idents
            .entid(&Keyword::new(Some("person"), "id"))
            .expect("id ident");
        let id_meta = schema.get(id).expect("id attribute");
        assert_eq!(id_meta.unique, Some(Unique::Identity));
        assert!(id_meta.indexed);
    }

    #[test]
    fn rejects_duplicate_canonical_attributes() {
        let error = parse(
            r#"
[[entity]]
name = "person"
[entity.attributes]
name = "string"

[[attribute]]
group = "person"
name = "name"
type = "string"
"#,
        )
        .expect_err("duplicate must fail");
        assert_eq!(error.to_string(), "duplicate attribute :person/name");
    }

    #[test]
    fn rejects_unknown_fields_and_values() {
        let unknown_field = parse(
            r#"
[[entity]]
name = "person"
[entity.attributes]
name = { type = "string", required = true }
"#,
        )
        .expect_err("unknown option must fail");
        assert!(
            unknown_field
                .to_string()
                .contains("did not match any variant"),
            "{unknown_field}"
        );

        let unknown_type = parse(
            r#"
[[attribute]]
name = "score"
type = "integer"
"#,
        )
        .expect_err("unknown type must fail");
        assert_eq!(
            unknown_type.to_string(),
            "unknown value type \"integer\" for :score"
        );
    }

    #[test]
    fn rejects_names_that_embed_the_edn_separator() {
        let error = parse(
            r#"
[[entity]]
name = "person/legacy"
[entity.attributes]
name = "string"
"#,
        )
        .expect_err("slash must fail");
        assert!(
            error
                .to_string()
                .contains("names must be non-empty and may not contain '/'")
        );
    }

    #[test]
    fn rejects_conflicting_cardinality_spellings() {
        let error = parse(
            r#"
[[attribute]]
name = "tags"
type = "string"
cardinality = "many"
many = true
"#,
        )
        .expect_err("conflict must fail");
        assert_eq!(
            error.to_string(),
            ":tags specifies both `cardinality` and `many`; use only one"
        );
    }
}
