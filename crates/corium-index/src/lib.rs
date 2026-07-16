//! Immutable ordered segment trees and live indexes for datoms.

use std::{
    collections::BTreeMap,
    ops::Bound::{Included, Unbounded},
    sync::Arc,
};

use corium_core::{Datom, IndexOrder};

/// Key bytes paired with a datom in one covering index.
pub type IndexEntry = (Vec<u8>, Datom);

const LEAF_CAPACITY: usize = 64;

/// An immutable sorted index segment.
#[derive(Clone, Debug, Default)]
pub struct Segment {
    leaves: Arc<[Arc<[IndexEntry]>]>,
}

impl Segment {
    /// Builds a deduplicated immutable segment for `order`.
    #[must_use]
    pub fn build(order: IndexOrder, datoms: impl IntoIterator<Item = Datom>) -> Self {
        let mut entries: Vec<_> = datoms.into_iter().map(|d| (d.key(order), d)).collect();
        entries.sort_by(|left, right| left.0.cmp(&right.0));
        entries.dedup_by(|left, right| left.0 == right.0);
        Self::from_entries(&entries, &[])
    }

    /// Returns all entries in key order.
    pub fn entries(&self) -> impl Iterator<Item = &IndexEntry> {
        self.leaves.iter().flat_map(|leaf| leaf.iter())
    }

    /// Seeks to the first entry whose key is greater than or equal to `key`.
    pub fn seek<'a>(&'a self, key: &'a [u8]) -> impl Iterator<Item = &'a IndexEntry> + 'a {
        self.entries()
            .skip_while(move |entry| entry.0.as_slice() < key)
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
        Self::from_entries(&by_key.into_iter().collect::<Vec<_>>(), segments)
    }

    /// Counts entries that are shared exactly with `older`.
    #[must_use]
    pub fn shared_entry_count(&self, older: &Self) -> usize {
        let old: BTreeMap<_, _> = older.entries().map(|(k, d)| (k, d)).collect();
        self.entries()
            .filter(|(k, d)| old.get(k).is_some_and(|old_d| *old_d == d))
            .count()
    }

    /// Counts leaf allocations physically shared with `older`.
    #[must_use]
    pub fn shared_leaf_count(&self, older: &Self) -> usize {
        self.leaves
            .iter()
            .filter(|leaf| older.leaves.iter().any(|old| Arc::ptr_eq(leaf, old)))
            .count()
    }

    fn from_entries(entries: &[IndexEntry], reusable: &[Self]) -> Self {
        let leaves = entries
            .chunks(LEAF_CAPACITY)
            .map(|chunk| {
                reusable
                    .iter()
                    .flat_map(|segment| segment.leaves.iter())
                    .find(|leaf| leaf.as_ref() == chunk)
                    .cloned()
                    .unwrap_or_else(|| Arc::from(chunk))
            })
            .collect::<Vec<_>>();
        Self {
            leaves: leaves.into(),
        }
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
        Segment::from_entries(&self.entries.into_iter().collect::<Vec<_>>(), &[])
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
    use proptest::prelude::*;

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
        assert_eq!(right.entries().count(), 3);
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

    #[test]
    fn append_merge_reuses_all_complete_unchanged_leaves() {
        let original = Segment::build(
            IndexOrder::Eavt,
            (0..LEAF_CAPACITY * 3).map(|e| {
                let entity = u64::try_from(e).expect("test entity fits u64");
                let value = i64::try_from(e).expect("test value fits i64");
                datom(entity, 1, value)
            }),
        );
        let merged = Segment::merge(
            IndexOrder::Eavt,
            std::slice::from_ref(&original),
            [datom(1_000, 1, 1_000)],
        );

        assert_eq!(merged.shared_leaf_count(&original), 3);
    }

    proptest! {
        #[test]
        fn segment_operations_match_btree_model(
            original in prop::collection::vec((0_u16..500, any::<i16>()), 0..300),
            additions in prop::collection::vec((0_u16..500, any::<i16>()), 0..100),
            seek_entity in 0_u16..500,
        ) {
            let to_datom = |(e, value)| datom(u64::from(e), 1, i64::from(value));
            let original_datoms = original.into_iter().map(to_datom).collect::<Vec<_>>();
            let additions = additions.into_iter().map(to_datom).collect::<Vec<_>>();
            let segment = Segment::build(IndexOrder::Eavt, original_datoms.clone());
            let merged = Segment::merge(
                IndexOrder::Eavt,
                std::slice::from_ref(&segment),
                additions.clone(),
            );

            let mut model = BTreeMap::new();
            for datom in original_datoms.into_iter().chain(additions) {
                model.insert(datom.key(IndexOrder::Eavt), datom);
            }
            let actual = merged.entries().cloned().collect::<Vec<_>>();
            let expected = model.into_iter().collect::<Vec<_>>();
            prop_assert_eq!(&actual, &expected);

            let seek_key = datom(u64::from(seek_entity), 0, i64::MIN).key(IndexOrder::Eavt);
            let actual_tail = merged.seek(&seek_key).cloned().collect::<Vec<_>>();
            let expected_tail = expected
                .into_iter()
                .skip_while(|entry| entry.0 < seek_key)
                .collect::<Vec<_>>();
            prop_assert_eq!(actual_tail, expected_tail);
        }
    }
}
