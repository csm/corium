//! The lazy entity API: map-like navigation over EAVT.

use corium_core::{AttrId, EntityId, IndexOrder, Keyword, Value};
use corium_db::{Db, key_prefix};

/// A lazy, map-like view of one entity. Nothing is read until asked for;
/// each access is an index prefix scan against the underlying [`Db`] value.
#[derive(Clone, Copy, Debug)]
pub struct Entity<'a> {
    db: &'a Db,
    id: EntityId,
}

impl<'a> Entity<'a> {
    /// Wraps an entity id over a database value.
    #[must_use]
    pub const fn new(db: &'a Db, id: EntityId) -> Self {
        Self { db, id }
    }

    /// The entity id.
    #[must_use]
    pub const fn id(&self) -> EntityId {
        self.id
    }

    /// The database value this entity reads from.
    #[must_use]
    pub const fn db(&self) -> &'a Db {
        self.db
    }

    /// Values of an attribute (empty when absent).
    #[must_use]
    pub fn get(&self, attr: AttrId) -> Vec<Value> {
        self.db.values(self.id, attr)
    }

    /// Values of an attribute by ident keyword.
    #[must_use]
    pub fn get_kw(&self, keyword: &Keyword) -> Vec<Value> {
        self.db
            .idents()
            .entid(keyword)
            .map(|attr| self.get(attr))
            .unwrap_or_default()
    }

    /// Attributes present on this entity, in id order.
    #[must_use]
    pub fn keys(&self) -> Vec<AttrId> {
        let prefix = key_prefix(IndexOrder::Eavt, Some(self.id), None, None);
        let mut attrs: Vec<AttrId> = self
            .db
            .datoms_prefix(IndexOrder::Eavt, &prefix)
            .map(|datom| datom.a)
            .collect();
        attrs.dedup();
        attrs
    }

    /// Navigates a reference attribute to child entities.
    #[must_use]
    pub fn refs(&self, attr: AttrId) -> Vec<Entity<'a>> {
        self.get(attr)
            .into_iter()
            .filter_map(|value| match value {
                Value::Ref(child) => Some(Entity::new(self.db, child)),
                _ => None,
            })
            .collect()
    }

    /// Reverse navigation: entities whose `attr` references this entity.
    #[must_use]
    pub fn reverse(&self, attr: AttrId) -> Vec<Entity<'a>> {
        let value = Value::Ref(self.id);
        let prefix = key_prefix(IndexOrder::Vaet, None, Some(attr), Some(&value));
        self.db
            .datoms_prefix(IndexOrder::Vaet, &prefix)
            .map(|datom| Entity::new(self.db, datom.e))
            .collect()
    }
}
