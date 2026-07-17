//! Keyword representation and simple in-memory interning.

use std::collections::BTreeMap;

use crate::KwId;

/// Namespaced keyword.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Keyword {
    /// Optional namespace.
    pub namespace: Option<String>,
    /// Keyword name.
    pub name: String,
}

impl Keyword {
    /// Constructs a keyword from namespace and name parts.
    #[must_use]
    pub fn new(namespace: Option<&str>, name: &str) -> Self {
        Self {
            namespace: namespace.map(str::to_owned),
            name: name.to_owned(),
        }
    }

    /// Parses `"ns/name"` or `"name"` (without a leading colon).
    ///
    /// Only the first `/` separates the namespace, matching EDN symbol rules.
    #[must_use]
    pub fn parse(text: &str) -> Self {
        match text.split_once('/') {
            Some((namespace, name)) if !namespace.is_empty() && !name.is_empty() => {
                Self::new(Some(namespace), name)
            }
            _ => Self::new(None, text),
        }
    }
}

impl std::fmt::Display for Keyword {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.namespace {
            Some(namespace) => write!(f, ":{namespace}/{}", self.name),
            None => write!(f, ":{}", self.name),
        }
    }
}

/// Deterministic keyword interner for tests and bootstrap metadata.
#[derive(Clone, Debug, Default)]
pub struct KeywordInterner {
    by_keyword: BTreeMap<Keyword, KwId>,
    by_id: BTreeMap<KwId, Keyword>,
    next: KwId,
}

impl KeywordInterner {
    /// Interns a keyword, returning the stable id assigned by this interner.
    pub fn intern(&mut self, keyword: Keyword) -> KwId {
        if let Some(id) = self.by_keyword.get(&keyword) {
            return *id;
        }
        let id = self.next;
        self.next += 1;
        self.by_id.insert(id, keyword.clone());
        self.by_keyword.insert(keyword, id);
        id
    }

    /// Looks up an already interned keyword without interning it.
    #[must_use]
    pub fn get(&self, keyword: &Keyword) -> Option<KwId> {
        self.by_keyword.get(keyword).copied()
    }

    /// Resolves an id back to a keyword.
    #[must_use]
    pub fn resolve(&self, id: KwId) -> Option<&Keyword> {
        self.by_id.get(&id)
    }
}
