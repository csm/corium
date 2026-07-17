//! Schema model shared by transaction validation and peers.

use std::collections::BTreeMap;

use crate::AttrId;

/// Supported value types in v1.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ValueType {
    /// Boolean values.
    Bool,
    /// Signed 64-bit integers.
    Long,
    /// Double precision floating point values.
    Double,
    /// Milliseconds since Unix epoch.
    Instant,
    /// UUID values.
    Uuid,
    /// Interned keywords.
    Keyword,
    /// UTF-8 strings.
    Str,
    /// Byte arrays.
    Bytes,
    /// Entity references.
    Ref,
}

/// Attribute cardinality.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Cardinality {
    /// One value per entity.
    One,
    /// Many values per entity.
    Many,
}

/// Attribute uniqueness mode.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Unique {
    /// Upsert identity.
    Identity,
    /// Unique value with conflict errors.
    Value,
}

/// Schema attribute metadata.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Attribute {
    /// Attribute entity id.
    pub id: AttrId,
    /// Value type.
    pub value_type: ValueType,
    /// Cardinality.
    pub cardinality: Cardinality,
    /// Optional uniqueness.
    pub unique: Option<Unique>,
    /// Component reference flag.
    pub is_component: bool,
    /// AVET coverage flag.
    pub indexed: bool,
    /// Skip history storage.
    pub no_history: bool,
}

/// Immutable schema cache.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Schema {
    attrs: BTreeMap<AttrId, Attribute>,
}

impl Schema {
    /// Adds or replaces an attribute.
    pub fn insert(&mut self, attr: Attribute) {
        self.attrs.insert(attr.id, attr);
    }

    /// Looks up an attribute.
    #[must_use]
    pub fn get(&self, id: AttrId) -> Option<&Attribute> {
        self.attrs.get(&id)
    }

    /// Iterates over every installed attribute in id order.
    pub fn iter(&self) -> impl Iterator<Item = (&AttrId, &Attribute)> {
        self.attrs.iter()
    }
}
