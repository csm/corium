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

    /// Resolves an id back to a keyword.
    #[must_use]
    pub fn resolve(&self, id: KwId) -> Option<&Keyword> {
        self.by_id.get(&id)
    }
}
