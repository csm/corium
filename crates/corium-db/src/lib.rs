//! Immutable database values and bootstrap metadata.

use std::collections::{BTreeMap, BTreeSet};

use corium_core::{
    AttrId, Cardinality, Datom, EntityId, IndexOrder, Partition, Schema, Unique, Value,
};

/// The first user-assignable sequence number. Lower ids are reserved for bootstrap data.
pub const FIRST_USER_ID: u64 = 1_000;

/// An immutable value of a database at one basis transaction.
#[derive(Clone, Debug, Default)]
pub struct Db {
    basis_t: u64,
    schema: Schema,
    history: Vec<Datom>,
}

impl Db {
    /// Creates an empty database with the supplied schema.
    #[must_use]
    pub const fn new(schema: Schema) -> Self {
        Self {
            basis_t: 0,
            schema,
            history: Vec::new(),
        }
    }
    /// Current transaction basis.
    #[must_use]
    pub const fn basis_t(&self) -> u64 {
        self.basis_t
    }
    /// Schema at this basis.
    #[must_use]
    pub const fn schema(&self) -> &Schema {
        &self.schema
    }
    /// Complete assertion/retraction history.
    #[must_use]
    pub fn history(&self) -> &[Datom] {
        &self.history
    }
    /// Returns current facts, deterministically ordered by EAVT.
    #[must_use]
    pub fn datoms(&self) -> Vec<Datom> {
        let mut current: BTreeMap<(EntityId, AttrId, Value), Datom> = BTreeMap::new();
        for datom in &self.history {
            let key = (datom.e, datom.a, datom.v.clone());
            if datom.added {
                current.insert(key, datom.clone());
            } else {
                current.remove(&key);
            }
        }
        let mut result: Vec<_> = current.into_values().collect();
        result.sort_by_key(|d| d.key(IndexOrder::Eavt));
        result
    }
    /// Current values for an entity/attribute pair.
    #[must_use]
    pub fn values(&self, e: EntityId, a: AttrId) -> Vec<Value> {
        self.datoms()
            .into_iter()
            .filter(|d| d.e == e && d.a == a)
            .map(|d| d.v)
            .collect()
    }
    /// Resolves a unique attribute/value pair.
    #[must_use]
    pub fn lookup(&self, a: AttrId, v: &Value) -> Option<EntityId> {
        self.datoms()
            .into_iter()
            .find(|d| d.a == a && &d.v == v)
            .map(|d| d.e)
    }
    /// Applies a committed record, returning a new database value.
    #[must_use]
    pub fn with_transaction(&self, t: u64, datoms: &[Datom]) -> Self {
        let mut next = self.clone();
        next.basis_t = t;
        next.history.extend_from_slice(datoms);
        next
    }
    /// Computes basic current database statistics.
    #[must_use]
    pub fn stats(&self) -> DbStats {
        let datoms = self.datoms();
        DbStats {
            datoms: datoms.len(),
            entities: datoms.iter().map(|d| d.e).collect::<BTreeSet<_>>().len(),
            attributes: datoms.iter().map(|d| d.a).collect::<BTreeSet<_>>().len(),
        }
    }
}

/// Counts over the current database value.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DbStats {
    /// Current facts.
    pub datoms: usize,
    /// Entities having at least one current fact.
    pub entities: usize,
    /// Attributes used by current facts.
    pub attributes: usize,
}

/// Convenience constructor for schema attributes used during bootstrap/tests.
#[must_use]
pub const fn attribute(
    id: u64,
    value_type: corium_core::ValueType,
    cardinality: Cardinality,
    unique: Option<Unique>,
) -> corium_core::Attribute {
    corium_core::Attribute {
        id: EntityId::new(Partition::Db as u32, id),
        value_type,
        cardinality,
        unique,
        is_component: false,
        indexed: unique.is_some(),
        no_history: false,
    }
}
