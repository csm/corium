//! Who is reading: the attribute view and the key set one read is answered
//! under.
//!
//! Hydration was already a property of the read rather than of the `Db`
//! ([`protect::Hydrator`](crate::protect::Hydrator)), because one peer server
//! answers principals with different key sets from the same immutable value.
//! Policy visibility is the same kind of thing — one server, two tenants, two
//! views of one database — so the two travel together in a [`ReadContext`],
//! and every read path takes one instead of a bare hydrator.
//!
//! The two controls are independent and answer different questions
//! (`docs/design/auth.md`, "Relationship to encryption"): a [`ViewFilter`]
//! says who *should* see an attribute and is enforced by this process, while a
//! protection class says who *can* and is enforced by the absence of a key.
//! A reader must pass both.
//!
//! `ViewFilter` itself lives in `corium-protocol`, which depends on
//! `corium-query` and therefore on this crate; so the filter is resolved to
//! [`AttrVisibility`] — a set of [`AttrId`]s — at the boundary, by the surface
//! that holds the authorization decision. That keeps the per-datom check a set
//! lookup rather than an id-to-keyword resolution, and keeps the dependency
//! edge pointing one way.
//!
//! [`ViewFilter`]: https://docs.rs/corium-protocol

use std::collections::BTreeSet;
use std::sync::{Arc, OnceLock};

use corium_core::{AttrId, EntityId, Keyword, KeywordInterner, Schema, Value};

use crate::Idents;
use crate::protect::{Hydration, Hydrator, ProtectError};

/// The attributes one reader may not see, resolved against one database value.
///
/// Resolution happens once per read, not once per datom: a schema holds tens
/// or hundreds of attributes, while a scan touches millions of datoms.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AttrVisibility {
    hidden: BTreeSet<AttrId>,
}

impl AttrVisibility {
    /// A view hiding exactly `hidden`.
    #[must_use]
    pub fn hiding(hidden: impl IntoIterator<Item = AttrId>) -> Self {
        Self {
            hidden: hidden.into_iter().collect(),
        }
    }

    /// Resolves `visible` over every attribute the schema declares.
    ///
    /// `visible` is asked about the attribute's ident keyword, which is the
    /// spelling policy data uses. An attribute with no ident cannot be named
    /// by a policy, so it is hidden: a filter is a restriction, and something
    /// a restriction cannot describe is withheld rather than disclosed.
    #[must_use]
    pub fn resolve(schema: &Schema, idents: &Idents, visible: impl Fn(&Keyword) -> bool) -> Self {
        let hidden = schema
            .iter()
            .map(|(attr, _)| *attr)
            .filter(|attr| idents.ident(*attr).is_none_or(|ident| !visible(ident)))
            .collect();
        Self { hidden }
    }

    /// Whether datoms of `attr` reach this reader.
    #[must_use]
    pub fn is_visible(&self, attr: AttrId) -> bool {
        !self.hidden.contains(&attr)
    }

    /// The hidden attributes, in id order.
    pub fn hidden(&self) -> impl Iterator<Item = AttrId> + '_ {
        self.hidden.iter().copied()
    }

    /// Whether this view hides nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.hidden.is_empty()
    }
}

/// The keys and the view one read is answered under.
///
/// [`ReadContext::open`] is the unrestricted default — every attribute
/// visible, no keys — which is what an embedded peer with no keyring and no
/// policy has always done.
#[derive(Clone, Debug, Default)]
pub struct ReadContext {
    hydrator: Option<Arc<Hydrator>>,
    visibility: Option<Arc<AttrVisibility>>,
}

impl ReadContext {
    /// An unrestricted read: no key set, no view.
    #[must_use]
    pub fn open() -> Self {
        Self::default()
    }

    /// The shared unrestricted read.
    ///
    /// Borrowing seams — the entity API holds a `&ReadContext` so it stays
    /// `Copy` — need somewhere for the default to live.
    #[must_use]
    pub fn unrestricted() -> &'static Self {
        static OPEN: OnceLock<ReadContext> = OnceLock::new();
        OPEN.get_or_init(Self::default)
    }

    /// Reads through `hydrator`'s key set (builder style).
    #[must_use]
    pub fn with_hydrator(mut self, hydrator: Arc<Hydrator>) -> Self {
        self.hydrator = Some(hydrator);
        self
    }

    /// Restricts the read to `visibility` (builder style).
    #[must_use]
    pub fn with_visibility(mut self, visibility: Arc<AttrVisibility>) -> Self {
        self.visibility = Some(visibility);
        self
    }

    /// The key set this read opens sealed values with, if any.
    #[must_use]
    pub fn hydrator(&self) -> Option<&Arc<Hydrator>> {
        self.hydrator.as_ref()
    }

    /// The attribute view this read is answered under, if any.
    #[must_use]
    pub fn visibility(&self) -> Option<&Arc<AttrVisibility>> {
        self.visibility.as_ref()
    }

    /// Whether policy hides anything from this reader.
    ///
    /// Surfaces that cannot filter their output — the peer server's proxied
    /// `Subscribe` stream — refuse a restricted read rather than answering it
    /// unfiltered.
    #[must_use]
    pub fn is_restricted(&self) -> bool {
        self.visibility
            .as_ref()
            .is_some_and(|view| !view.is_empty())
    }

    /// Whether datoms of `attr` reach this reader at all.
    #[must_use]
    pub fn visible(&self, attr: AttrId) -> bool {
        self.visibility
            .as_ref()
            .is_none_or(|view| view.is_visible(attr))
    }

    /// What this read does with one scanned datom.
    ///
    /// Policy is checked before keys, and hides the datom outright: a reader
    /// who may not see an attribute must not learn that a value exists, so a
    /// hidden attribute never binds, never joins, and never satisfies a
    /// predicate. Only then does the class's own missing-key policy apply.
    ///
    /// # Errors
    ///
    /// Returns [`ProtectError`] when a value this reader holds a key for
    /// cannot be opened; see [`Hydrator::hydrate`].
    pub fn read(
        &self,
        schema: &Schema,
        interner: &KeywordInterner,
        attr: AttrId,
        e: EntityId,
        value: &Value,
    ) -> Result<Hydration, ProtectError> {
        if !self.visible(attr) {
            return Ok(Hydration::Hide);
        }
        match &self.hydrator {
            Some(hydrator) => hydrator.hydrate(schema, interner, attr, e, value),
            None => Ok(Hydration::Bind(value.clone())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use corium_core::{Attribute, Cardinality, ValueType};

    fn schema_with(idents: &mut Idents, names: &[(&str, u64)]) -> Schema {
        let mut schema = Schema::default();
        for (name, id) in names {
            let id = EntityId::from_raw(*id);
            schema.insert(Attribute {
                id,
                value_type: ValueType::Str,
                cardinality: Cardinality::One,
                unique: None,
                is_component: false,
                indexed: false,
                no_history: false,
            });
            idents.insert(Keyword::parse(name), id);
        }
        schema
    }

    #[test]
    fn resolve_hides_everything_outside_an_allowlist() {
        let mut idents = Idents::default();
        let schema = schema_with(&mut idents, &[("person/name", 100), ("person/ssn", 101)]);
        let allowed = [":person/name"];
        let view = AttrVisibility::resolve(&schema, &idents, |ident| {
            allowed.contains(&ident.to_string().as_str())
        });
        assert!(view.is_visible(EntityId::from_raw(100)));
        assert!(!view.is_visible(EntityId::from_raw(101)));
    }

    #[test]
    fn an_open_context_shows_everything_and_binds_as_is() {
        let context = ReadContext::open();
        assert!(context.visible(EntityId::from_raw(999)));
        assert!(!context.is_restricted());
    }

    #[test]
    fn a_hidden_attribute_is_hidden_before_keys_are_consulted() {
        let context = ReadContext::open()
            .with_visibility(Arc::new(AttrVisibility::hiding([EntityId::from_raw(101)])));
        assert!(context.is_restricted());
        assert!(!context.visible(EntityId::from_raw(101)));
        let hidden = context
            .read(
                &Schema::default(),
                &KeywordInterner::default(),
                EntityId::from_raw(101),
                EntityId::from_raw(1),
                &Value::Str("secret".into()),
            )
            .expect("hiding never fails");
        assert_eq!(hidden, Hydration::Hide);
    }
}
