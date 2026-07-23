//! Transaction data as builder-patterned values.
//!
//! A [`TxBuilder`] assembles the Datomic-dialect transaction forms corium
//! accepts — map forms with `:db/id`, and the list ops `[:db/add …]`,
//! `[:db/retract …]`, `[:db/cas …]`, `[:db/retractEntity …]` — into a
//! [`TxData`] value ready to submit through a [`crate::Peer`].
//!
//! ```
//! use corium_client::tx::{TxBuilder, EntityMap, tempid, lookup};
//!
//! let tx = TxBuilder::new()
//!     .entity(
//!         EntityMap::with_id(tempid("alice"))
//!             .set("person/name", "Alice")
//!             .set("person/age", 39_i64),
//!     )
//!     .add(lookup("person/email", "bob@example.com"), "person/age", 40_i64)
//!     .build();
//! ```

use corium_query::edn::Edn;

use crate::value::IntoEdn;

/// A string tempid such as `"alice"`, unified with an allocated entity id
/// after the transaction commits.
#[must_use]
pub fn tempid(name: impl Into<String>) -> Edn {
    Edn::Str(name.into())
}

/// A lookup ref `[:attr value]` naming an existing entity by a unique
/// attribute value.
#[must_use]
pub fn lookup(attr: &str, value: impl IntoEdn) -> Edn {
    Edn::Vector(vec![Edn::keyword(attr), value.into_edn()])
}

/// An `#eid` reference to a raw entity id, for entity positions where a bare
/// long would be ambiguous.
#[must_use]
pub fn eid(id: impl IntoEdn) -> Edn {
    Edn::Tagged("eid".into(), Box::new(id.into_edn()))
}

/// A map-form entity: `{:db/id … :attr value …}`. Without an id the
/// transactor allocates a fresh entity.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EntityMap {
    id: Option<Edn>,
    pairs: Vec<(Edn, Edn)>,
}

impl EntityMap {
    /// A map-form entity with no `:db/id` (the transactor allocates one).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// A map-form entity with an explicit `:db/id` (a tempid, entity id,
    /// ident, or lookup ref).
    #[must_use]
    pub fn with_id(id: impl IntoEdn) -> Self {
        Self {
            id: Some(id.into_edn()),
            pairs: Vec::new(),
        }
    }

    /// Sets a single-valued attribute.
    #[must_use]
    pub fn set(mut self, attr: &str, value: impl IntoEdn) -> Self {
        self.pairs.push((Edn::keyword(attr), value.into_edn()));
        self
    }

    /// Sets a cardinality-many attribute to a vector of values.
    #[must_use]
    pub fn set_many<V: IntoEdn>(mut self, attr: &str, values: impl IntoIterator<Item = V>) -> Self {
        let values = values.into_iter().map(IntoEdn::into_edn).collect();
        self.pairs.push((Edn::keyword(attr), Edn::Vector(values)));
        self
    }

    fn into_edn(self) -> Edn {
        let mut pairs = Vec::with_capacity(self.pairs.len() + 1);
        if let Some(id) = self.id {
            pairs.push((Edn::keyword("db/id"), id));
        }
        pairs.extend(self.pairs);
        Edn::Map(pairs)
    }
}

/// A completed set of transaction forms, ready to submit through a
/// [`crate::Peer`].
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct TxData(Vec<Edn>);

impl TxData {
    /// The transaction forms.
    #[must_use]
    pub fn forms(&self) -> &[Edn] {
        &self.0
    }

    /// Consumes the value, yielding the transaction forms.
    #[must_use]
    pub fn into_forms(self) -> Vec<Edn> {
        self.0
    }
}

impl From<Vec<Edn>> for TxData {
    fn from(forms: Vec<Edn>) -> Self {
        Self(forms)
    }
}

/// Builds a [`TxData`] value from transaction forms.
#[derive(Clone, Debug, Default)]
pub struct TxBuilder {
    forms: Vec<Edn>,
}

impl TxBuilder {
    /// An empty transaction.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a map-form entity.
    #[must_use]
    pub fn entity(mut self, entity: EntityMap) -> Self {
        self.forms.push(entity.into_edn());
        self
    }

    /// Asserts a fact: `[:db/add e a v]`.
    #[must_use]
    pub fn add(mut self, e: impl IntoEdn, a: &str, v: impl IntoEdn) -> Self {
        self.forms.push(Edn::Vector(vec![
            Edn::keyword("db/add"),
            e.into_edn(),
            Edn::keyword(a),
            v.into_edn(),
        ]));
        self
    }

    /// Retracts a fact: `[:db/retract e a v]`.
    #[must_use]
    pub fn retract(mut self, e: impl IntoEdn, a: &str, v: impl IntoEdn) -> Self {
        self.forms.push(Edn::Vector(vec![
            Edn::keyword("db/retract"),
            e.into_edn(),
            Edn::keyword(a),
            v.into_edn(),
        ]));
        self
    }

    /// Compare-and-swap: `[:db/cas e a old new]`.
    #[must_use]
    pub fn cas(mut self, e: impl IntoEdn, a: &str, old: impl IntoEdn, new: impl IntoEdn) -> Self {
        self.forms.push(Edn::Vector(vec![
            Edn::keyword("db/cas"),
            e.into_edn(),
            Edn::keyword(a),
            old.into_edn(),
            new.into_edn(),
        ]));
        self
    }

    /// Retracts a whole entity: `[:db/retractEntity e]`.
    #[must_use]
    pub fn retract_entity(mut self, e: impl IntoEdn) -> Self {
        self.forms.push(Edn::Vector(vec![
            Edn::keyword("db/retractEntity"),
            e.into_edn(),
        ]));
        self
    }

    /// Appends a raw transaction form (an escape hatch for forms the typed
    /// builder does not model, e.g. `:db/fn` invocations).
    #[must_use]
    pub fn form(mut self, form: Edn) -> Self {
        self.forms.push(form);
        self
    }

    /// Finalizes the transaction.
    #[must_use]
    pub fn build(self) -> TxData {
        TxData(self.forms)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn map_and_list_forms_render() {
        let tx = TxBuilder::new()
            .entity(
                EntityMap::with_id(tempid("alice"))
                    .set("person/name", "Alice")
                    .set_many("person/aliases", ["Al", "Ali"]),
            )
            .add(1_i64, "person/age", 40_i64)
            .retract(
                lookup("person/email", "bob@example.com"),
                "person/age",
                39_i64,
            )
            .retract_entity(eid(2_i64))
            .build();
        let forms = tx.into_forms();
        assert_eq!(forms.len(), 4);
        assert_eq!(
            forms[0].to_string(),
            "{:db/id \"alice\", :person/name \"Alice\", :person/aliases [\"Al\" \"Ali\"]}"
        );
        assert_eq!(forms[1].to_string(), "[:db/add 1 :person/age 40]");
        assert_eq!(
            forms[2].to_string(),
            "[:db/retract [:person/email \"bob@example.com\"] :person/age 39]"
        );
        assert_eq!(forms[3].to_string(), "[:db/retractEntity #eid 2]");
    }
}
