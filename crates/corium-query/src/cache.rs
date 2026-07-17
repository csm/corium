//! Parsed-query cache keyed by the query's EDN value, as in Datomic.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::QueryError;
use crate::ast::{Query, parse_query};
use crate::edn::Edn;

/// Caches parsed queries by their EDN form. Cloning shares the cache.
#[derive(Clone, Debug, Default)]
pub struct QueryCache {
    entries: Arc<Mutex<BTreeMap<Edn, Arc<Query>>>>,
}

impl QueryCache {
    /// Creates an empty cache.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Parses a query, reusing the cached parse for a previously seen form.
    ///
    /// # Errors
    /// Returns [`QueryError`] when the form fails to parse; failures are not
    /// cached.
    pub fn parse(&self, form: &Edn) -> Result<Arc<Query>, QueryError> {
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(hit) = entries.get(form) {
            return Ok(Arc::clone(hit));
        }
        let parsed = Arc::new(parse_query(form)?);
        entries.insert(form.clone(), Arc::clone(&parsed));
        Ok(parsed)
    }

    /// Number of cached parses.
    ///
    /// # Panics
    /// Never; the lock is recovered if poisoned.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    /// Whether the cache is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
