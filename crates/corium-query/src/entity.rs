//! The lazy entity API: map-like navigation over EAVT.

use corium_core::{AttrId, EntityId, IndexOrder, Keyword, Value};
use corium_db::{Db, key_prefix, read::ReadContext};

use crate::QueryError;
use crate::pull::hydrate;

/// A lazy, map-like view of one entity. Nothing is read until asked for;
/// each access is an index prefix scan against the underlying [`Db`] value.
#[derive(Clone, Copy, Debug)]
pub struct Entity<'a> {
    db: &'a Db,
    id: EntityId,
    read: &'a ReadContext,
}

impl<'a> Entity<'a> {
    /// Wraps an entity id over a database value, read without restriction.
    ///
    /// Values on protected attributes come back sealed; use
    /// [`Entity::with_read`] to read them as a key holder, or under a policy
    /// view.
    #[must_use]
    pub fn new(db: &'a Db, id: EntityId) -> Self {
        Self {
            db,
            id,
            read: ReadContext::unrestricted(),
        }
    }

    /// Returns this entity read under `read`'s view and key set.
    #[must_use]
    pub const fn with_read(mut self, read: &'a ReadContext) -> Self {
        self.read = read;
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
            .filter_map(|value| hydrate(self.db, self.read, attr, self.id, &value).transpose())
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
    ///
    /// Attributes this reader's view hides are absent: the key list is itself
    /// information, so an entity looks to a restricted reader exactly as if
    /// those datoms had never been asserted.
    #[must_use]
    pub fn keys(&self) -> Vec<AttrId> {
        let prefix = key_prefix(IndexOrder::Eavt, Some(self.id), None, None);
        let mut attrs: Vec<AttrId> = self
            .db
            .datoms_prefix(IndexOrder::Eavt, &prefix)
            .map(|datom| datom.a)
            .filter(|attr| self.read.visible(*attr))
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
                    read: self.read,
                }),
                _ => None,
            })
            .collect()
    }

    /// Reverse navigation: entities whose `attr` references this entity.
    ///
    /// A reference this reader cannot see forward does not point back either.
    #[must_use]
    pub fn reverse(&self, attr: AttrId) -> Vec<Entity<'a>> {
        if !self.read.visible(attr) {
            return Vec::new();
        }
        let value = Value::Ref(self.id);
        let prefix = key_prefix(IndexOrder::Vaet, None, Some(attr), Some(&value));
        self.db
            .datoms_prefix(IndexOrder::Vaet, &prefix)
            .map(|datom| Entity {
                db: self.db,
                id: datom.e,
                read: self.read,
            })
            .collect()
    }
}
