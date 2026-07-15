//! Immutable ordered segment trees and live indexes for datoms.

use std::{
    collections::BTreeMap,
    ops::Bound::{Included, Unbounded},
    sync::Arc,
};

use corium_core::{Datom, IndexOrder};

/// Key bytes paired with a datom in one covering index.
pub type IndexEntry = (Vec<u8>, Datom);

/// An immutable sorted index segment.
#[derive(Clone, Debug, Default)]
pub struct Segment {
    entries: Arc<[IndexEntry]>,
}

impl Segment {
    /// Builds a deduplicated immutable segment for `order`.
    #[must_use]
    pub fn build(order: IndexOrder, datoms: impl IntoIterator<Item = Datom>) -> Self {
        let mut entries: Vec<_> = datoms.into_iter().map(|d| (d.key(order), d)).collect();
        entries.sort_by(|left, right| left.0.cmp(&right.0));
        entries.dedup_by(|left, right| left.0 == right.0);
        Self {
            entries: entries.into(),
        }
    }

    /// Returns all entries in key order.
    #[must_use]
    pub fn entries(&self) -> &[IndexEntry] {
        &self.entries
    }

    /// Seeks to the first entry whose key is greater than or equal to `key`.
    pub fn seek<'a>(&'a self, key: &'a [u8]) -> impl Iterator<Item = &'a IndexEntry> + 'a {
        let start = self
            .entries
            .partition_point(|entry| entry.0.as_slice() < key);
        self.entries[start..].iter()
    }

    /// Merges sorted segments plus new datoms into a new immutable segment.
    #[must_use]
    pub fn merge(
        order: IndexOrder,
        segments: &[Self],
        live: impl IntoIterator<Item = Datom>,
    ) -> Self {
        let mut by_key: BTreeMap<Vec<u8>, Datom> = BTreeMap::new();
        for segment in segments {
            for (key, datom) in segment.entries() {
                by_key.insert(key.clone(), datom.clone());
            }
        }
        for datom in live {
            by_key.insert(datom.key(order), datom);
        }
        Self {
            entries: by_key.into_iter().collect::<Vec<_>>().into(),
        }
    }

    /// Counts entries that are shared exactly with `older`.
    #[must_use]
    pub fn shared_entry_count(&self, older: &Self) -> usize {
        let old: BTreeMap<_, _> = older.entries.iter().map(|(k, d)| (k, d)).collect();
        self.entries
            .iter()
            .filter(|(k, d)| old.get(k).is_some_and(|old_d| *old_d == d))
            .count()
    }
}

/// Mutable in-memory index for freshly transacted datoms.
#[derive(Clone, Debug)]
pub struct LiveIndex {
    order: IndexOrder,
    entries: BTreeMap<Vec<u8>, Datom>,
}
impl LiveIndex {
    /// Creates an empty live index for `order`.
    #[must_use]
    pub fn new(order: IndexOrder) -> Self {
        Self {
            order,
            entries: BTreeMap::new(),
        }
    }
    /// Inserts one datom, replacing only an identical covering index key.
    ///
    /// Datom keys include transaction and assertion state, so this is append-oriented
    /// history storage rather than current-value replacement.
    pub fn insert(&mut self, datom: Datom) {
        self.entries.insert(datom.key(self.order), datom);
    }
    /// Returns entries in key order.
    pub fn iter(&self) -> impl Iterator<Item = (&Vec<u8>, &Datom)> {
        self.entries.iter()
    }
    /// Returns entries at or after `key`.
    pub fn seek<'a>(
        &'a self,
        key: &'a [u8],
    ) -> impl Iterator<Item = (&'a Vec<u8>, &'a Datom)> + 'a {
        self.entries.range::<[u8], _>((Included(key), Unbounded))
    }
    /// Freezes this live index into an immutable segment.
    #[must_use]
    pub fn freeze(self) -> Segment {
        Segment {
            entries: self.entries.into_iter().collect::<Vec<_>>().into(),
        }
    }
}

/// Unions one durable segment and one live index at read time.
///
/// If an identical covering index key appears in both inputs, the live datom is returned.
pub fn merged_iter<'a>(
    durable: &'a Segment,
    live: &'a LiveIndex,
) -> impl Iterator<Item = IndexEntry> + 'a {
    let mut by_key: BTreeMap<Vec<u8>, Datom> = BTreeMap::new();
    for (key, datom) in durable.entries() {
        by_key.insert(key.clone(), datom.clone());
    }
    for (key, datom) in live.iter() {
        by_key.insert(key.clone(), datom.clone());
    }
    by_key.into_iter()
}

#[cfg(test)]
mod tests {
    use super::*;
    use corium_core::{EntityId, Value};

    fn datom(e: u64, a: u64, v: i64) -> Datom {
        Datom {
            e: EntityId::from_raw(e),
            a: EntityId::from_raw(a),
            v: Value::Long(v),
            tx: EntityId::from_raw(100 + e),
            added: true,
        }
    }

    #[test]
    fn build_seek_and_merge_match_btree_model() {
        let left = Segment::build(IndexOrder::Eavt, [datom(2, 1, 20), datom(1, 1, 10)]);
        let right = Segment::merge(
            IndexOrder::Eavt,
            std::slice::from_ref(&left),
            [datom(3, 1, 30)],
        );
        assert_eq!(right.entries().len(), 3);
        assert_eq!(right.shared_entry_count(&left), 2);
        let first_key = datom(2, 1, 20).key(IndexOrder::Eavt);
        assert_eq!(right.seek(&first_key).next().expect("entry").1.e.raw(), 2);
    }

    #[test]
    fn merged_iterator_unions_disjoint_keys() {
        let durable = Segment::build(IndexOrder::Eavt, [datom(1, 1, 10)]);
        let mut live = LiveIndex::new(IndexOrder::Eavt);
        live.insert(datom(2, 1, 20));
        let merged: Vec<_> = merged_iter(&durable, &live).collect();
        assert_eq!(merged.len(), 2);
    }

    #[test]
    fn merged_iterator_deduplicates_identical_key() {
        let original = datom(1, 1, 10);
        let durable = Segment::build(IndexOrder::Eavt, [original.clone()]);
        let mut replacement = original;
        replacement.added = true;
        let mut live = LiveIndex::new(IndexOrder::Eavt);
        live.insert(replacement);
        let merged: Vec<_> = merged_iter(&durable, &live).collect();
        assert_eq!(merged.len(), 1);
    }
}
