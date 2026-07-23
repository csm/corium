//! A typesafe, builder-patterned Pull specification.
//!
//! A [`Pull`] is an immutable value that lowers to the boundary [`Edn`] pull
//! pattern the engine understands: attribute selections, `*`, `:db/id`,
//! reverse refs, nested sub-patterns, bounded/unbounded recursion, and the
//! `:as`/`:limit`/`:default` options.
//!
//! ```
//! use corium_client::pull::{Pull, Attr};
//!
//! // [:person/name {:person/friends [:person/name]} :person/_manager]
//! let spec = Pull::new()
//!     .attr("person/name")
//!     .nested("person/friends", Pull::new().attr("person/name"))
//!     .reverse("person/manager");
//! ```

use corium_query::edn::Edn;

use crate::value::IntoEdn;

/// The cardinality-many result limit for a pulled attribute.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Limit {
    /// `:limit n`.
    At(usize),
    /// `:limit nil` — no bound.
    Unlimited,
}

/// One attribute selection with optional `:as`, `:limit`, and `:default`
/// options. Forward by default; [`Attr::reverse`] flips it to a reverse ref.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Attr {
    name: String,
    reverse: bool,
    as_key: Option<Edn>,
    limit: Option<Limit>,
    default: Option<Edn>,
}

impl Attr {
    /// A forward attribute selection for `"ns/name"`.
    #[must_use]
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            reverse: false,
            as_key: None,
            limit: None,
            default: None,
        }
    }

    /// Marks this as a reverse ref (`:ns/_name`).
    #[must_use]
    pub fn reverse(mut self) -> Self {
        self.reverse = true;
        self
    }

    /// Renames the result key (`:as`). Accepts a keyword or any scalar.
    #[must_use]
    pub fn as_key(mut self, key: impl IntoEdn) -> Self {
        self.as_key = Some(key.into_edn());
        self
    }

    /// Bounds a cardinality-many result to `n` values (`:limit n`).
    #[must_use]
    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(Limit::At(n));
        self
    }

    /// Removes the result bound (`:limit nil`).
    #[must_use]
    pub fn unlimited(mut self) -> Self {
        self.limit = Some(Limit::Unlimited);
        self
    }

    /// Supplies a value when the attribute is absent (`:default`).
    #[must_use]
    pub fn default(mut self, value: impl IntoEdn) -> Self {
        self.default = Some(value.into_edn());
        self
    }

    /// The attribute keyword, applying the reverse `_` prefix if set.
    fn keyword(&self) -> Edn {
        Edn::keyword(&reverse_name(&self.name, self.reverse))
    }

    fn has_options(&self) -> bool {
        self.as_key.is_some() || self.limit.is_some() || self.default.is_some()
    }

    fn to_edn(&self) -> Edn {
        if !self.has_options() {
            return self.keyword();
        }
        let mut items = vec![self.keyword()];
        if let Some(key) = &self.as_key {
            items.push(Edn::keyword("as"));
            items.push(key.clone());
        }
        if let Some(limit) = self.limit {
            items.push(Edn::keyword("limit"));
            items.push(match limit {
                Limit::At(n) => Edn::Long(i64::try_from(n).unwrap_or(i64::MAX)),
                Limit::Unlimited => Edn::Nil,
            });
        }
        if let Some(default) = &self.default {
            items.push(Edn::keyword("default"));
            items.push(default.clone());
        }
        Edn::Vector(items)
    }
}

impl From<&str> for Attr {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for Attr {
    fn from(value: String) -> Self {
        Self::new(value)
    }
}

/// Inserts the reverse-ref `_` prefix on the attribute name component.
fn reverse_name(name: &str, reverse: bool) -> String {
    if !reverse {
        return name.to_owned();
    }
    match name.rsplit_once('/') {
        Some((namespace, local)) => format!("{namespace}/_{local}"),
        None => format!("_{name}"),
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Item {
    Wildcard,
    DbId,
    Attr(Attr),
    Nested(Attr, Pull),
    Recurse(Attr, Option<usize>),
}

impl Item {
    fn to_edn(&self) -> Edn {
        match self {
            Self::Wildcard => Edn::symbol("*"),
            Self::DbId => Edn::keyword("db/id"),
            Self::Attr(attr) => attr.to_edn(),
            Self::Nested(attr, sub) => Edn::Map(vec![(attr.to_edn(), sub.to_edn())]),
            Self::Recurse(attr, depth) => {
                let value = depth.map_or_else(
                    || Edn::symbol("..."),
                    |depth| Edn::Long(i64::try_from(depth).unwrap_or(i64::MAX)),
                );
                Edn::Map(vec![(attr.to_edn(), value)])
            }
        }
    }
}

/// An immutable Pull specification. Build it up with the fluent methods and
/// render with [`Pull::to_edn`].
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Pull {
    items: Vec<Item>,
}

impl Pull {
    /// An empty pull pattern.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Selects everything (`*`).
    #[must_use]
    pub fn wildcard(mut self) -> Self {
        self.items.push(Item::Wildcard);
        self
    }

    /// Selects the entity id (`:db/id`).
    #[must_use]
    pub fn db_id(mut self) -> Self {
        self.items.push(Item::DbId);
        self
    }

    /// Selects a forward attribute by `"ns/name"`.
    #[must_use]
    pub fn attr(mut self, attr: impl Into<Attr>) -> Self {
        self.items.push(Item::Attr(attr.into()));
        self
    }

    /// Selects a reverse ref for `"ns/name"` (`:ns/_name`).
    #[must_use]
    pub fn reverse(mut self, name: impl Into<String>) -> Self {
        self.items.push(Item::Attr(Attr::new(name).reverse()));
        self
    }

    /// Selects a ref attribute with a nested sub-pattern
    /// (`{:ns/name sub}`).
    #[must_use]
    pub fn nested(mut self, attr: impl Into<Attr>, sub: Pull) -> Self {
        self.items.push(Item::Nested(attr.into(), sub));
        self
    }

    /// Recurses into a ref attribute to a bounded depth (`{:ns/name n}`).
    #[must_use]
    pub fn recurse(mut self, attr: impl Into<Attr>, depth: usize) -> Self {
        self.items.push(Item::Recurse(attr.into(), Some(depth)));
        self
    }

    /// Recurses into a ref attribute without bound (`{:ns/name ...}`).
    #[must_use]
    pub fn recurse_unbounded(mut self, attr: impl Into<Attr>) -> Self {
        self.items.push(Item::Recurse(attr.into(), None));
        self
    }

    /// Appends an explicit [`Attr`] selection carrying options.
    #[must_use]
    pub fn spec(mut self, attr: Attr) -> Self {
        self.items.push(Item::Attr(attr));
        self
    }

    /// Renders the pattern to its boundary [`Edn`] vector form.
    #[must_use]
    pub fn to_edn(&self) -> Edn {
        Edn::Vector(self.items.iter().map(Item::to_edn).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_expected_forms() {
        let spec = Pull::new()
            .attr("person/name")
            .db_id()
            .wildcard()
            .reverse("person/manager")
            .nested("person/friends", Pull::new().attr("person/name"))
            .recurse("person/reports", 3)
            .recurse_unbounded("person/mentor")
            .spec(
                Attr::new("person/email")
                    .as_key(Edn::keyword("email"))
                    .default("n/a"),
            );
        assert_eq!(
            spec.to_edn().to_string(),
            "[:person/name :db/id * :person/_manager {:person/friends [:person/name]} \
             {:person/reports 3} {:person/mentor ...} \
             [:person/email :as :email :default \"n/a\"]]"
        );
    }

    #[test]
    fn reverse_name_handles_bare_and_namespaced() {
        assert_eq!(reverse_name("person/manager", true), "person/_manager");
        assert_eq!(reverse_name("manager", true), "_manager");
        assert_eq!(reverse_name("person/manager", false), "person/manager");
    }
}
