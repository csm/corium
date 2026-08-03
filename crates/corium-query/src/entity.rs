//! The lazy entity API: map-like navigation over EAVT.

use corium_core::{AttrId, EntityId, IndexOrder, Keyword, Value};
use corium_db::{Db, key_prefix, protect::Hydrator};

use crate::QueryError;
use crate::pull::hydrate;

/// A lazy, map-like view of one entity. Nothing is read until asked for;
/// each access is an index prefix scan against the underlying [`Db`] value.
#[derive(Clone, Copy, Debug)]
pub struct Entity<'a> {
    db: &'a Db,
    id: EntityId,
    hydrator: Option<&'a Hydrator>,
}

impl<'a> Entity<'a> {
    /// Wraps an entity id over a database value.
    ///
    /// Values on protected attributes come back sealed; use
    /// [`Entity::with_keys`] to read them as a key holder.
    #[must_use]
    pub const fn new(db: &'a Db, id: EntityId) -> Self {
        Self {
            db,
            id,
            hydrator: None,
        }
    }

    /// Returns this entity reading through `hydrator`'s key set.
    #[must_use]
    pub const fn with_keys(mut self, hydrator: &'a Hydrator) -> Self {
        self.hydrator = Some(hydrator);
        self
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
    ///
    /// A class whose missing-key policy is `error` reads as absent here;
    /// [`Entity::try_get`] surfaces it instead.
    #[must_use]
    pub fn get(&self, attr: AttrId) -> Vec<Value> {
        self.try_get(attr).unwrap_or_default()
    }

    /// Values of an attribute, failing when a class asks reads to fail.
    ///
    /// # Errors
    /// Returns [`QueryError::Protected`] when this reader holds no key for a
    /// value whose class sets `:db.protect.missing/error`.
    pub fn try_get(&self, attr: AttrId) -> Result<Vec<Value>, QueryError> {
        self.db
            .values(self.id, attr)
            .into_iter()
            .filter_map(|value| hydrate(self.db, self.hydrator, attr, self.id, &value).transpose())
            .collect()
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
                Value::Ref(child) => Some(Self {
                    db: self.db,
                    id: child,
                    hydrator: self.hydrator,
                }),
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
            .map(|datom| Entity {
                db: self.db,
                id: datom.e,
                hydrator: self.hydrator,
            })
            .collect()
    }
}
