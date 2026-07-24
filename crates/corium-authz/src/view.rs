//! [`ViewFilter`] implementations built from policy data, and the conservative
//! rule for combining the filters of several successful authorization paths.
//!
//! Policy data names attributes as idents (`:person/email`); callers ask about
//! whatever spelling their surface uses, so every comparison here normalizes to
//! the leading-colon form.

use std::collections::BTreeSet;
use std::sync::Arc;

use corium_protocol::authz::ViewFilter;

use crate::model::{FilterKind, ViewDef};

/// Normalizes an attribute ident to its leading-colon spelling.
#[must_use]
pub fn normalize_attribute(attribute: &str) -> String {
    if attribute.starts_with(':') {
        attribute.to_owned()
    } else {
        format!(":{attribute}")
    }
}

/// Hides every attribute outside the allowlist.
#[derive(Clone, Debug)]
pub struct AttributeAllowlist {
    name: String,
    allowed: BTreeSet<String>,
}

impl AttributeAllowlist {
    /// Builds a named allowlist over attribute idents.
    pub fn new(
        name: impl Into<String>,
        attributes: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Self {
        Self {
            name: name.into(),
            allowed: attributes
                .into_iter()
                .map(|attribute| normalize_attribute(attribute.as_ref()))
                .collect(),
        }
    }

    /// The policy name this filter was compiled from.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl ViewFilter for AttributeAllowlist {
    fn attribute_visible(&self, attribute: &str) -> bool {
        self.allowed.contains(&normalize_attribute(attribute))
    }
}

/// Hides the named attributes and shows everything else.
#[derive(Clone, Debug)]
pub struct AttributeDenylist {
    name: String,
    denied: BTreeSet<String>,
}

impl AttributeDenylist {
    /// Builds a named denylist over attribute idents.
    pub fn new(
        name: impl Into<String>,
        attributes: impl IntoIterator<Item = impl AsRef<str>>,
    ) -> Self {
        Self {
            name: name.into(),
            denied: attributes
                .into_iter()
                .map(|attribute| normalize_attribute(attribute.as_ref()))
                .collect(),
        }
    }

    /// The policy name this filter was compiled from.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl ViewFilter for AttributeDenylist {
    fn attribute_visible(&self, attribute: &str) -> bool {
        !self.denied.contains(&normalize_attribute(attribute))
    }
}

/// The conjunction of several filters: an attribute is visible only when every
/// part admits it.
///
/// This is how the evaluator combines the views of several successful paths —
/// intersecting visibility constraints rather than taking the widest, so
/// holding one more relation can never *reveal* more than holding it alone.
#[derive(Clone, Debug)]
pub struct IntersectionView {
    parts: Vec<Arc<dyn ViewFilter>>,
}

impl IntersectionView {
    /// Intersects `parts`.
    #[must_use]
    pub fn new(parts: Vec<Arc<dyn ViewFilter>>) -> Self {
        Self { parts }
    }
}

impl ViewFilter for IntersectionView {
    fn attribute_visible(&self, attribute: &str) -> bool {
        self.parts
            .iter()
            .all(|part| part.attribute_visible(attribute))
    }
}

/// Builds the filter a [`ViewDef`] describes.
#[must_use]
pub fn build(definition: &ViewDef) -> Arc<dyn ViewFilter> {
    match definition.kind {
        FilterKind::AttributeAllowlist => Arc::new(AttributeAllowlist::new(
            definition.name.clone(),
            &definition.attributes,
        )),
        FilterKind::AttributeDenylist => Arc::new(AttributeDenylist::new(
            definition.name.clone(),
            &definition.attributes,
        )),
    }
}

/// Combines the views of every successful path.
///
/// * no path declared a view — full visibility;
/// * one path — that view;
/// * several — their intersection.
///
/// A path whose relation is explicitly marked unfiltered is handled by the
/// caller, which drops every filter when it sees one.
#[must_use]
pub fn combine(views: Vec<Arc<dyn ViewFilter>>) -> Option<Arc<dyn ViewFilter>> {
    match views.len() {
        0 => None,
        1 => views.into_iter().next(),
        _ => Some(Arc::new(IntersectionView::new(views))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn allowlist(attributes: &[&str]) -> Arc<dyn ViewFilter> {
        Arc::new(AttributeAllowlist::new("test", attributes))
    }

    #[test]
    fn allowlist_normalizes_idents() {
        let filter = AttributeAllowlist::new("v", ["person/name", ":person/email"]);
        assert!(filter.attribute_visible(":person/name"));
        assert!(filter.attribute_visible("person/email"));
        assert!(!filter.attribute_visible(":person/ssn"));
    }

    #[test]
    fn denylist_hides_only_named_attributes() {
        let filter = AttributeDenylist::new("v", [":person/ssn"]);
        assert!(filter.attribute_visible(":person/name"));
        assert!(!filter.attribute_visible(":person/ssn"));
    }

    #[test]
    fn intersection_is_conservative() {
        let combined = combine(vec![
            allowlist(&[":person/name", ":person/email"]),
            allowlist(&[":person/name", ":person/ssn"]),
        ])
        .expect("two views intersect");
        assert!(combined.attribute_visible(":person/name"));
        assert!(!combined.attribute_visible(":person/email"));
        assert!(!combined.attribute_visible(":person/ssn"));
        assert!(combine(Vec::new()).is_none());
    }
}
