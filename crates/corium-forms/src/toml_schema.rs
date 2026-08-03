//! Hierarchical TOML schema declarations.
//!
//! `[[entity]]` declarations are authoring sugar: their names become keyword
//! namespaces, but they do not install or enforce entity types. `[[attribute]]`
//! declarations expose the same model without an entity block.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use corium_core::{
    Cardinality, FIRST_KEY_EPOCH, Keyword, LegacyPlaintextPolicy, MissingKeyPolicy,
    ProtectionScope, SealAlgorithm, Unique, ValueType,
};
use corium_query::edn::Edn;
use serde::de::value::MapAccessDeserializer;
use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer};
use thiserror::Error;

use crate::schemaform::MIN_PADDING;

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
    /// Optional documentation (`:db/doc`).
    pub doc: Option<String>,
    /// Protection class ident, when the attribute is protected.
    pub protection: Option<Keyword>,
}

/// One normalized protection-class declaration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProtectionClassDefinition {
    /// Class name within the `protect` namespace, giving ident `:protect/…`.
    pub name: String,
    /// Key identity the class names; it never holds material.
    pub key_id: String,
    /// Sealing algorithm.
    pub algorithm: SealAlgorithm,
    /// What the sealing context binds.
    pub scope: ProtectionScope,
    /// Plaintext padding multiple, in bytes.
    pub padding: Option<u32>,
    /// What a reader without the key sees.
    pub on_missing_key: MissingKeyPolicy,
    /// How pre-protection plaintext is treated.
    pub legacy_plaintext: LegacyPlaintextPolicy,
    /// Epoch new values seal under.
    pub epoch: u32,
}

impl ProtectionClassDefinition {
    /// Returns the canonical class ident, `:protect/<name>`.
    #[must_use]
    pub fn ident(&self) -> Keyword {
        Keyword::new(Some("protect"), &self.name)
    }

    /// Converts the declaration to the EDN class map `schema_from_edn` reads.
    #[must_use]
    pub fn to_edn(&self) -> Edn {
        let mut pairs = vec![
            (kw("db.protect/ident"), Edn::Keyword(self.ident())),
            (kw("db.protect/key"), Edn::Str(self.key_id.clone())),
            (
                kw("db.protect/algorithm"),
                kw(match self.algorithm {
                    SealAlgorithm::Aes256GcmSiv => "db.protect.alg/aes-256-gcm-siv",
                }),
            ),
            (
                kw("db.protect/scope"),
                kw(match self.scope {
                    ProtectionScope::Attribute => "db.protect.scope/attribute",
                    ProtectionScope::Entity => "db.protect.scope/entity",
                }),
            ),
            (
                kw("db.protect/on-missing-key"),
                kw(match self.on_missing_key {
                    MissingKeyPolicy::Redact => "db.protect.missing/redact",
                    MissingKeyPolicy::Hide => "db.protect.missing/hide",
                    MissingKeyPolicy::Error => "db.protect.missing/error",
                }),
            ),
            (
                kw("db.protect/legacy-plaintext"),
                kw(match self.legacy_plaintext {
                    LegacyPlaintextPolicy::Redact => "db.protect.legacy/redact",
                    LegacyPlaintextPolicy::PassThrough => "db.protect.legacy/pass-through",
                }),
            ),
        ];
        if let Some(padding) = self.padding {
            pairs.push((kw("db.protect/padding"), Edn::Long(i64::from(padding))));
        }
        if self.epoch != FIRST_KEY_EPOCH {
            pairs.push((kw("db.protect/epoch"), Edn::Long(i64::from(self.epoch))));
        }
        pairs.sort_unstable();
        Edn::Map(pairs)
    }
}

/// Everything one schema document declares.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SchemaDefinitions {
    /// Protection classes, in class-name order.
    pub classes: Vec<ProtectionClassDefinition>,
    /// Attribute declarations.
    pub attributes: Vec<AttributeDefinition>,
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
        if let Some(doc) = &self.doc {
            pairs.push((kw("db/doc"), Edn::Str(doc.clone())));
        }
        if let Some(protection) = &self.protection {
            pairs.push((kw("db/protection"), Edn::Keyword(protection.clone())));
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
    /// A group or attribute name is not a valid EDN keyword component.
    #[error(
        "invalid {kind} name {name:?}: expected a non-empty EDN keyword component \
         without reserved punctuation, whitespace, or a leading digit"
    )]
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
    /// Two entity authoring blocks use the same group name.
    #[error("duplicate entity group {0:?}")]
    DuplicateEntity(String),
    /// A protection-class option names a value the model does not have.
    #[error("unknown {field} {value:?} for protection class {class}")]
    UnknownProtectionOption {
        /// Class being normalized.
        class: String,
        /// Option name.
        field: &'static str,
        /// Unknown source value.
        value: String,
    },
    /// A protection class sets a padding below the useful minimum.
    #[error(
        "padding {padding} for protection class {class} is below the \
         {minimum}-byte minimum"
    )]
    PaddingTooSmall {
        /// Class being normalized.
        class: String,
        /// Declared padding.
        padding: i64,
        /// Smallest padding the model accepts.
        minimum: i64,
    },
    /// An attribute names a protection class the document does not declare.
    #[error("attribute {ident} names unknown protection class {class:?}")]
    UnknownProtectionClass {
        /// Attribute being normalized.
        ident: String,
        /// Class it names.
        class: String,
    },
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
    // TOML tables are unordered; a BTreeMap makes class-id allocation
    // deterministic rather than dependent on parser insertion order.
    #[serde(default, rename = "protect")]
    classes: BTreeMap<String, ProtectionClassDeclaration>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "kebab-case")]
struct ProtectionClassDeclaration {
    key: String,
    #[serde(default)]
    algorithm: Option<String>,
    #[serde(default)]
    scope: Option<String>,
    #[serde(default)]
    padding: Option<i64>,
    #[serde(default)]
    on_missing_key: Option<String>,
    #[serde(default)]
    legacy_plaintext: Option<String>,
    #[serde(default)]
    epoch: Option<u32>,
}

const fn default_schema_version() -> u32 {
    1
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EntityDeclaration {
    name: String,
    // TOML tables are unordered. Sorting local names makes initial attribute-id
    // allocation deterministic rather than dependent on parser insertion order.
    #[serde(default)]
    attributes: BTreeMap<String, RawAttribute>,
}

#[derive(Debug)]
enum RawAttribute {
    Shorthand(String),
    Detailed(AttributeOptions),
}

impl<'de> Deserialize<'de> for RawAttribute {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct RawAttributeVisitor;

        impl<'de> Visitor<'de> for RawAttributeVisitor {
            type Value = RawAttribute;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a value type string or an attribute options table")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(RawAttribute::Shorthand(value.to_owned()))
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(RawAttribute::Shorthand(value))
            }

            fn visit_map<M>(self, map: M) -> Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                AttributeOptions::deserialize(MapAccessDeserializer::new(map))
                    .map(RawAttribute::Detailed)
            }
        }

        deserializer.deserialize_any(RawAttributeVisitor)
    }
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
    #[serde(default)]
    doc: Option<String>,
    #[serde(default)]
    protection: Option<String>,
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
                doc: None,
                protection: None,
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
pub fn parse(input: &str) -> Result<SchemaDefinitions, TomlSchemaError> {
    let document: SchemaDocument = toml::from_str(input)?;
    if document.schema_version != 1 {
        return Err(TomlSchemaError::UnsupportedVersion(document.schema_version));
    }

    let mut classes = Vec::with_capacity(document.classes.len());
    for (name, declaration) in document.classes {
        validate_name("protection class", &name)?;
        classes.push(normalize_class(name, declaration)?);
    }

    let mut definitions = Vec::new();
    let mut seen_entities = BTreeSet::new();
    for entity in document.entities {
        validate_name("entity", &entity.name)?;
        if !seen_entities.insert(entity.name.clone()) {
            return Err(TomlSchemaError::DuplicateEntity(entity.name));
        }
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
        if let Some(protection) = &definition.protection
            && !classes.iter().any(|class| &class.ident() == protection)
        {
            return Err(TomlSchemaError::UnknownProtectionClass {
                ident,
                class: protection.to_string(),
            });
        }
    }
    Ok(SchemaDefinitions {
        classes,
        attributes: definitions,
    })
}

/// Parses TOML and emits equivalent EDN maps for existing APIs.
///
/// Classes come first, so their entity ids are stable under later attribute
/// edits and an attribute may name a class regardless of source order.
///
/// # Errors
/// Returns the same failures as [`parse`].
pub fn parse_edn(input: &str) -> Result<Vec<Edn>, TomlSchemaError> {
    parse(input).map(|definitions| {
        definitions
            .classes
            .iter()
            .map(ProtectionClassDefinition::to_edn)
            .chain(
                definitions
                    .attributes
                    .iter()
                    .map(AttributeDefinition::to_edn),
            )
            .collect()
    })
}

fn normalize_class(
    name: String,
    declaration: ProtectionClassDeclaration,
) -> Result<ProtectionClassDefinition, TomlSchemaError> {
    let class = format!(":protect/{name}");
    let unknown = |field: &'static str, value: String| TomlSchemaError::UnknownProtectionOption {
        class: class.clone(),
        field,
        value,
    };
    let algorithm = match declaration.algorithm.as_deref() {
        None | Some("aes-256-gcm-siv") => SealAlgorithm::Aes256GcmSiv,
        Some(_) => {
            return Err(unknown(
                "algorithm",
                declaration.algorithm.unwrap_or_default(),
            ));
        }
    };
    let scope = match declaration.scope.as_deref() {
        None | Some("attribute") => ProtectionScope::Attribute,
        Some("entity") => ProtectionScope::Entity,
        Some(_) => return Err(unknown("scope", declaration.scope.unwrap_or_default())),
    };
    let on_missing_key = match declaration.on_missing_key.as_deref() {
        None | Some("redact") => MissingKeyPolicy::Redact,
        Some("hide") => MissingKeyPolicy::Hide,
        Some("error") => MissingKeyPolicy::Error,
        Some(_) => {
            return Err(unknown(
                "on-missing-key",
                declaration.on_missing_key.unwrap_or_default(),
            ));
        }
    };
    let legacy_plaintext = match declaration.legacy_plaintext.as_deref() {
        None | Some("redact") => LegacyPlaintextPolicy::Redact,
        Some("pass-through") => LegacyPlaintextPolicy::PassThrough,
        Some(_) => {
            return Err(unknown(
                "legacy-plaintext",
                declaration.legacy_plaintext.unwrap_or_default(),
            ));
        }
    };
    let padding = match declaration.padding {
        None => None,
        Some(padding) => Some(
            u32::try_from(padding)
                .ok()
                .filter(|_| padding >= MIN_PADDING)
                .ok_or(TomlSchemaError::PaddingTooSmall {
                    class: class.clone(),
                    padding,
                    minimum: MIN_PADDING,
                })?,
        ),
    };
    Ok(ProtectionClassDefinition {
        name,
        key_id: declaration.key,
        algorithm,
        scope,
        padding,
        on_missing_key,
        legacy_plaintext,
        epoch: declaration.epoch.unwrap_or(FIRST_KEY_EPOCH),
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
    let indexed = options.index || unique.is_some();

    let protection = options.protection.map(|class| Keyword::parse(&class));
    Ok(AttributeDefinition {
        group,
        name,
        value_type,
        cardinality,
        unique,
        indexed,
        component: options.component,
        no_history: options.no_history,
        doc: options.doc,
        protection,
    })
}

fn validate_name(kind: &'static str, name: &str) -> Result<(), TomlSchemaError> {
    if is_valid_edn_keyword_component(name) {
        Ok(())
    } else {
        Err(TomlSchemaError::InvalidName {
            kind,
            name: name.to_owned(),
        })
    }
}

fn is_valid_edn_keyword_component(name: &str) -> bool {
    let Some(first) = name.chars().next() else {
        return false;
    };
    if first.is_ascii_digit() {
        return false;
    }
    !name.chars().any(|character| {
        character.is_whitespace()
            || character.is_control()
            || matches!(
                character,
                '/' | ':'
                    | ';'
                    | '"'
                    | '('
                    | ')'
                    | '['
                    | ']'
                    | '{'
                    | '}'
                    | ','
                    | '`'
                    | '~'
                    | '@'
                    | '^'
                    | '\\'
            )
    })
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
    use corium_query::edn::read_one;

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
        let definitions = parse(SCHEMA).expect("schema parses").attributes;
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
        assert!(definitions[2].indexed);
        assert_eq!(definitions[4].cardinality, Cardinality::Many);
        assert!(definitions[5].no_history);
    }

    #[test]
    fn generated_edn_installs_through_existing_schema_path() {
        let forms = parse_edn(SCHEMA).expect("schema parses");
        let (schema, idents) = schema_from_edn(&forms).expect("schema installs");
        assert_eq!(schema.iter().count(), forms.len() + 1);
        assert_eq!(
            idents.entid(&corium_db::bootstrap::tx_instant_ident()),
            Some(corium_db::bootstrap::TX_INSTANT)
        );
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
        let address = idents
            .entid(&Keyword::new(Some("person"), "address"))
            .expect("address ident");
        assert!(schema.get(address).expect("address attribute").is_component);
        let employees = idents
            .entid(&Keyword::new(Some("organization"), "employees"))
            .expect("employees ident");
        assert!(
            schema
                .get(employees)
                .expect("employees attribute")
                .no_history
        );
    }

    #[test]
    fn documentation_reaches_the_edn_form() {
        let definitions = parse(
            r#"
[[attribute]]
name = "score"
type = "long"
doc = "points earned"
"#,
        )
        .expect("schema parses");
        assert_eq!(definitions[0].doc.as_deref(), Some("points earned"));
        assert_eq!(
            definitions[0].to_edn().get(&kw("db/doc")),
            Some(&Edn::Str("points earned".into()))
        );
        // Documentation is optional and is not invented for declarations
        // that omit it.
        assert_eq!(
            parse("[[attribute]]\nname = \"n\"\ntype = \"long\"").expect("schema parses")[0].doc,
            None
        );
    }
    fn protection_classes_install_through_the_edn_path() {
        let toml = r#"
[protect.pii]
key = "file:/etc/corium/pii.key"
padding = 64
on-missing-key = "hide"
scope = "entity"

[[attribute]]
group = "person"
name = "ssn"
type = "string"
protection = "protect/pii"
"#;
        let definitions = parse(toml).expect("schema parses");
        assert_eq!(definitions.classes.len(), 1);
        assert_eq!(definitions.classes[0].ident().to_string(), ":protect/pii");
        assert_eq!(
            definitions.attributes[0].protection,
            Some(Keyword::new(Some("protect"), "pii"))
        );

        // Classes come first, so their entity ids do not move when attributes
        // are added or removed later.
        let forms = parse_edn(toml).expect("schema parses");
        let (schema, idents) = schema_from_edn(&forms).expect("schema installs");
        let ssn = idents
            .entid(&Keyword::new(Some("person"), "ssn"))
            .expect("ssn ident");
        let class = schema.protection_class(ssn).expect("ssn is protected");
        assert_eq!(class.key_id, "file:/etc/corium/pii.key");
        assert_eq!(class.padding, Some(64));
        assert_eq!(class.on_missing_key, MissingKeyPolicy::Hide);
        assert_eq!(class.scope, ProtectionScope::Entity);
    }

    #[test]
    fn rejects_bad_protection_declarations() {
        let unknown_class = parse(
            r#"
[[attribute]]
name = "ssn"
type = "string"
protection = "protect/pii"
"#,
        )
        .expect_err("unknown class must fail");
        assert_eq!(
            unknown_class.to_string(),
            "attribute :ssn names unknown protection class \":protect/pii\""
        );

        let small = parse(
            r#"
[protect.pii]
key = "file:/k"
padding = 8
"#,
        )
        .expect_err("small padding must fail");
        assert_eq!(
            small.to_string(),
            "padding 8 for protection class :protect/pii is below the 16-byte minimum"
        );

        let policy = parse(
            r#"
[protect.pii]
key = "file:/k"
on-missing-key = "shrug"
"#,
        )
        .expect_err("unknown policy must fail");
        assert_eq!(
            policy.to_string(),
            "unknown on-missing-key \"shrug\" for protection class :protect/pii"
        );
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
                .contains("unknown field `required`"),
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
    fn rejects_names_that_cannot_round_trip_through_edn() {
        for name in [
            "",
            "123name",
            "my attr",
            "a:b[c]",
            "semi;colon",
            "quoted\"name",
            "person/legacy",
        ] {
            let input = format!(
                r#"
[[attribute]]
name = {name:?}
type = "string"
"#
            );
            let error = parse(&input).expect_err("invalid name must fail");
            assert!(
                error
                    .to_string()
                    .contains("expected a non-empty EDN keyword component"),
                "{error}"
            );
        }

        let group_error = parse(
            r#"
[[attribute]]
group = "group name"
name = "value"
type = "string"
"#,
        )
        .expect_err("invalid group must fail");
        assert!(
            group_error
                .to_string()
                .contains("invalid group name \"group name\"")
        );
    }

    #[test]
    fn quoted_toml_keys_still_round_trip_as_edn_keywords() {
        let forms = parse_edn(
            r#"
[[entity]]
name = "person"
[entity.attributes]
"active?" = "boolean"
"#,
        )
        .expect("valid quoted key");
        let printed = forms[0].to_string();
        assert_eq!(read_one(&printed).expect("printed EDN parses"), forms[0]);
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

    #[test]
    fn rejects_unsupported_version_and_unknown_unique_mode() {
        let version = parse("schema-version = 2").expect_err("version must fail");
        assert_eq!(
            version.to_string(),
            "unsupported schema-version 2; expected 1"
        );

        let unique = parse(
            r#"
[[attribute]]
name = "id"
type = "uuid"
unique = "primary"
"#,
        )
        .expect_err("unique mode must fail");
        assert_eq!(
            unique.to_string(),
            "unknown unique mode \"primary\" for :id"
        );
    }

    #[test]
    fn allows_empty_entity_groups_but_rejects_duplicate_groups() {
        let definitions = parse(
            r#"
[[entity]]
name = "person"

[[attribute]]
group = "person"
name = "name"
type = "string"
"#,
        )
        .expect("empty group is authoring-only")
        .attributes;
        assert_eq!(definitions.len(), 1);

        let duplicate = parse(
            r#"
[[entity]]
name = "person"

[[entity]]
name = "person"
"#,
        )
        .expect_err("duplicate entity group must fail");
        assert_eq!(duplicate.to_string(), "duplicate entity group \"person\"");
    }
}
