//! Keyword representation and simple in-memory interning.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, PoisonError},
};

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

/// First keyword id in the reader-local range.
///
/// A sealed keyword value carries its text inside the ciphertext, so the
/// vocabulary of a protected attribute never reaches the naming table the
/// transactor publishes (see `docs/design/encryption.md`). Hydrating such a
/// value still has to produce a [`crate::Value::Keyword`], so the reader
/// mints an id for the text locally. Local ids sit far above the dense
/// durable range and never reach storage or the wire — keywords travel by
/// name in both.
pub const FIRST_LOCAL_KW_ID: KwId = 1 << 56;

#[derive(Debug, Default)]
struct LocalKeywords {
    by_keyword: BTreeMap<Keyword, KwId>,
    by_id: BTreeMap<KwId, Keyword>,
}

/// Deterministic keyword interner for tests and bootstrap metadata.
///
/// Cloning shares the reader-local overlay ([`FIRST_LOCAL_KW_ID`]) so that
/// hydrated keyword values keep one id across every database value derived
/// from the same connection.
#[derive(Clone, Debug, Default)]
pub struct KeywordInterner {
    by_keyword: BTreeMap<Keyword, KwId>,
    by_id: BTreeMap<KwId, Keyword>,
    next: KwId,
    local: Arc<Mutex<LocalKeywords>>,
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

    /// Interns a keyword read out of a sealed value, without adding it to the
    /// durable table.
    ///
    /// A keyword already known durably keeps its durable id, so a hydrated
    /// value and a query literal naming the same keyword compare equal.
    /// Anything else gets an id from the local range.
    #[must_use]
    pub fn intern_local(&self, keyword: Keyword) -> KwId {
        if let Some(id) = self.by_keyword.get(&keyword) {
            return *id;
        }
        let mut local = self.local.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(id) = local.by_keyword.get(&keyword) {
            return *id;
        }
        let id = FIRST_LOCAL_KW_ID + local.by_id.len() as KwId;
        local.by_id.insert(id, keyword.clone());
        local.by_keyword.insert(keyword, id);
        id
    }

    /// Looks up an already interned keyword without interning it.
    #[must_use]
    pub fn get(&self, keyword: &Keyword) -> Option<KwId> {
        self.by_keyword.get(keyword).copied().or_else(|| {
            self.local
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .by_keyword
                .get(keyword)
                .copied()
        })
    }

    /// Resolves an id back to a keyword.
    #[must_use]
    pub fn resolve(&self, id: KwId) -> Option<Keyword> {
        self.by_id.get(&id).cloned().or_else(|| {
            self.local
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .by_id
                .get(&id)
                .cloned()
        })
    }

    /// Iterates every durably interned keyword in id order. Ids are dense
    /// from zero, so re-interning the keywords in this order reproduces the
    /// same ids. Reader-local ids ([`FIRST_LOCAL_KW_ID`]) are not included:
    /// they exist only in the reader that hydrated them.
    pub fn iter(&self) -> impl Iterator<Item = (KwId, &Keyword)> {
        self.by_id.iter().map(|(id, keyword)| (*id, keyword))
    }

    /// Number of interned keywords.
    #[must_use]
    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    /// Whether the interner is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }
}
