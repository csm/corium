//! Immutable ordered index segments for datoms.
//!
//! A [`Segment`] is one covering index's sorted key stream — a covering-index
//! key encodes its whole datom, so the keys *are* the index — split into
//! **leaves** at the content-defined boundaries in [`corium_core::chunk`],
//! the same boundaries the published snapshot format cuts its chunks at. A
//! leaf therefore is exactly one published chunk, and leaves are
//! `Arc`-shared: [`Segment::apply`] folds a transaction tail into a segment
//! and rebuilds only the leaves the tail touched, leaving every other leaf
//! pointer-identical to the one the previous indexing pass published. The
//! publisher turns that identity straight into "re-encode and re-upload only
//! what changed" ([`Leaf::id`]), so a pass costs the tail plus one pointer
//! per leaf instead of a full rebuild.
//!
//! Segments hold **current** facts: a key is placed by its
//! [`corium_core::key_components`] prefix, so at most one datom per
//! `(e, a, v)` is present and a retraction cancels the assertion whatever
//! transaction made it.

use std::{
    borrow::Borrow,
    cmp::Ordering,
    collections::{BTreeMap, HashSet},
    sync::Arc,
};

use corium_core::{Datom, IndexOrder, chunk, key_components};

/// How many untouched leaves a rebuilt run may re-cut through before it is
/// closed at a leaf boundary regardless.
///
/// Rebuilding a leaf can move the boundary after it, so the run continues
/// into the following leaves until it cuts where the previous generation also
/// cut and the two chunkings agree again. Content-defined boundaries make
/// that happen almost immediately, but "almost" is not a bound: this limit
/// makes the worst case (a run that keeps missing the old boundaries) cost a
/// fixed number of leaves rather than the rest of the segment.
const RESYNC_LEAF_LIMIT: usize = 4;

/// One content-defined run of covering-index keys, shared between the
/// segments that contain it unchanged.
#[derive(Clone, Debug)]
pub struct Leaf(Arc<[Vec<u8>]>);

impl Leaf {
    /// The leaf's keys in order.
    #[must_use]
    pub fn keys(&self) -> &[Vec<u8>] {
        &self.0
    }

    /// Identity of this leaf's allocation.
    ///
    /// Two leaves with the same id hold the same keys, because a leaf is
    /// immutable and is only ever shared by cloning its handle. Ids are
    /// addresses, so they mean anything only while a segment holding the leaf
    /// is alive — a publisher comparing against the previous pass has to keep
    /// that segment, not just its ids.
    #[must_use]
    pub fn id(&self) -> LeafId {
        LeafId(Arc::as_ptr(&self.0).cast::<()>() as usize)
    }

    /// Whether this leaf ends where the chunking rule says to cut.
    ///
    /// The last leaf of a segment usually does not: it holds the run left
    /// over after the final boundary. Anything appended behind an open leaf
    /// belongs *in* it, so a pass that carries one over unchanged would leave
    /// a short leaf behind every append and drift away from the chunking a
    /// rebuild from scratch produces.
    fn is_closed(&self) -> bool {
        self.0
            .last()
            .is_some_and(|key| chunk::cut_after(key, self.0.len()))
    }
}

/// Address identity of a [`Leaf`] allocation; see [`Leaf::id`].
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct LeafId(usize);

/// An immutable sorted index segment.
#[derive(Clone, Debug, Default)]
pub struct Segment {
    leaves: Arc<[Leaf]>,
}

impl Segment {
    /// Builds a segment from datoms already sorted by their key in `order`.
    ///
    /// A current-value index map (`corium_db`'s covering indexes) iterates in
    /// exactly this order, so publication never re-sorts what the transaction
    /// pipeline already sorted.
    ///
    /// Only the key each datom encodes to is kept, so a caller that already
    /// holds the datoms — publication reading the database's own covering
    /// index — should use [`Segment::from_sorted_ref`] and hand over
    /// references rather than copies.
    ///
    /// # Panics
    /// Debug builds assert the input really is strictly increasing by key.
    #[must_use]
    pub fn from_sorted(order: IndexOrder, datoms: impl IntoIterator<Item = Datom>) -> Self {
        Self::from_sorted_borrowed(order, datoms)
    }

    /// [`Segment::from_sorted`] over datoms the caller keeps.
    ///
    /// # Panics
    /// Debug builds assert the input really is strictly increasing by key.
    #[must_use]
    pub fn from_sorted_ref<'a>(
        order: IndexOrder,
        datoms: impl IntoIterator<Item = &'a Datom>,
    ) -> Self {
        Self::from_sorted_borrowed(order, datoms)
    }

    fn from_sorted_borrowed(
        order: IndexOrder,
        datoms: impl IntoIterator<Item = impl Borrow<Datom>>,
    ) -> Self {
        let mut builder = LeafBuilder::default();
        for datom in datoms {
            builder.push(datom.borrow().key(order));
        }
        Self {
            leaves: builder.finish().into(),
        }
    }

    /// Builds a segment from datoms in any order, keeping the last datom
    /// stated for each fact.
    ///
    /// Unsorted input has to be ordered somewhere; [`Segment::from_sorted`]
    /// is the path publication takes.
    #[must_use]
    pub fn build(order: IndexOrder, datoms: impl IntoIterator<Item = Datom>) -> Self {
        let mut by_fact: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
        for datom in datoms {
            let key = datom.key(order);
            by_fact.insert(key_components(&key).to_vec(), key);
        }
        Self::from_keys(by_fact.into_values())
    }

    /// Builds a segment from covering-index keys already in sorted order —
    /// how a reader reconstitutes a published snapshot's chunk stream.
    ///
    /// # Panics
    /// Debug builds assert the input really is strictly increasing.
    #[must_use]
    pub fn from_keys(keys: impl IntoIterator<Item = Vec<u8>>) -> Self {
        let mut builder = LeafBuilder::default();
        for key in keys {
            builder.push(key);
        }
        Self {
            leaves: builder.finish().into(),
        }
    }

    /// Returns every key in order.
    pub fn keys(&self) -> impl Iterator<Item = &Vec<u8>> {
        self.leaves.iter().flat_map(|leaf| leaf.0.iter())
    }

    /// Returns the segment's leaves in key order.
    #[must_use]
    pub fn leaves(&self) -> impl ExactSizeIterator<Item = &Leaf> {
        self.leaves.iter()
    }

    /// Number of keys across every leaf.
    #[must_use]
    pub fn len(&self) -> usize {
        self.leaves.iter().map(|leaf| leaf.0.len()).sum()
    }

    /// Whether the segment holds no keys.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.leaves.is_empty()
    }

    /// Seeks to the first key greater than or equal to `key`.
    ///
    /// Binary-searches leaf boundaries and then within the target leaf, so
    /// positioning costs O(log n) rather than a scan of preceding keys.
    pub fn seek<'a>(&'a self, key: &'a [u8]) -> impl Iterator<Item = &'a Vec<u8>> + 'a {
        let leaf_idx = self
            .leaves
            .partition_point(|leaf| leaf.0.last().is_none_or(|last| last.as_slice() < key));
        let entry_idx = self.leaves.get(leaf_idx).map_or(0, |leaf| {
            leaf.0.partition_point(|entry| entry.as_slice() < key)
        });
        self.leaves[leaf_idx..]
            .iter()
            .enumerate()
            .flat_map(move |(offset, leaf)| {
                let start = if offset == 0 { entry_idx } else { 0 };
                leaf.0[start..].iter()
            })
    }

    /// Folds a transaction tail into this segment, returning the segment at
    /// the tail's basis.
    ///
    /// `changes` are the tail's datoms in transaction order: an assertion
    /// replaces whatever the index held for that fact, a retraction removes
    /// it — the same current-value fold `corium_db` applies to its in-memory
    /// indexes. Only leaves whose key span the tail touches are rebuilt;
    /// every other leaf is carried over by handle, so the result shares its
    /// allocations — and therefore its published chunks — with `self`.
    ///
    /// The tail is collapsed to its net key per fact, so only those keys are
    /// kept; a caller that already holds the tail — publication reading the
    /// log tail since the last basis — should use [`Segment::apply_ref`] and
    /// hand over references rather than copies.
    #[must_use]
    pub fn apply(&self, order: IndexOrder, changes: impl IntoIterator<Item = Datom>) -> Self {
        self.apply_borrowed(order, changes)
    }

    /// [`Segment::apply`] over a tail the caller keeps.
    #[must_use]
    pub fn apply_ref<'a>(
        &self,
        order: IndexOrder,
        changes: impl IntoIterator<Item = &'a Datom>,
    ) -> Self {
        self.apply_borrowed(order, changes)
    }

    fn apply_borrowed(
        &self,
        order: IndexOrder,
        changes: impl IntoIterator<Item = impl Borrow<Datom>>,
    ) -> Self {
        // Collapse the tail to one net change per fact first, so a fact
        // churned repeatedly across the tail rewrites its leaf once.
        let mut folded: BTreeMap<Vec<u8>, Option<Vec<u8>>> = BTreeMap::new();
        for datom in changes {
            let datom = datom.borrow();
            let key = datom.key(order);
            let fact = key_components(&key).to_vec();
            folded.insert(fact, datom.added.then_some(key));
        }
        if folded.is_empty() {
            return self.clone();
        }
        // Each entry is consumed exactly once, in order, by the walk below,
        // so its replacement key is moved into the leaf it lands in.
        let mut pending: Vec<(Vec<u8>, Option<Vec<u8>>)> = folded.into_iter().collect();
        // Retractions add nothing behind the leaf they land in, so only a
        // pending assertion can force an open trailing leaf to be re-cut.
        let last_assertion = pending.iter().rposition(|(_, key)| key.is_some());

        let mut builder = LeafBuilder::with_capacity(self.leaves.len() + 1);
        let mut cursor = 0;
        for (index, leaf) in self.leaves.iter().enumerate() {
            let keys = leaf.keys();
            let (Some(first), Some(last)) = (keys.first(), keys.last()) else {
                continue;
            };
            // Changes sorting before this leaf are insertions into the gap
            // ahead of it; a retraction of an absent fact is a no-op.
            while pending
                .get(cursor)
                .is_some_and(|(fact, _)| fact.as_slice() < key_components(first))
            {
                builder.push_change(pending[cursor].1.take());
                cursor += 1;
            }
            let touched = pending
                .get(cursor)
                .is_some_and(|(fact, _)| fact.as_slice() <= key_components(last));
            let followed = index + 1 < self.leaves.len()
                || last_assertion.is_some_and(|assertion| assertion >= cursor);
            if !touched && (leaf.is_closed() || !followed) {
                builder.carry(leaf);
                continue;
            }
            builder.begin_dirty();
            for key in keys {
                let fact = key_components(key);
                while pending
                    .get(cursor)
                    .is_some_and(|(pending_fact, _)| pending_fact.as_slice() < fact)
                {
                    builder.push_change(pending[cursor].1.take());
                    cursor += 1;
                }
                if pending
                    .get(cursor)
                    .is_some_and(|(pending_fact, _)| pending_fact.as_slice() == fact)
                {
                    builder.push_change(pending[cursor].1.take());
                    cursor += 1;
                } else {
                    builder.push(key.clone());
                }
            }
        }
        for (_, replacement) in &mut pending[cursor..] {
            builder.push_change(replacement.take());
        }
        Self {
            leaves: builder.finish().into(),
        }
    }

    /// Inserts retained history datoms into this segment.
    ///
    /// Unlike [`Self::apply`], this indexes the complete covering key,
    /// including transaction and assertion/retraction bit: a later statement
    /// of the same fact is another history entry rather than a replacement.
    /// Untouched leaves remain shared, so publishing a history tail rewrites
    /// only the chunks its new keys land in.
    #[must_use]
    pub fn insert_history_ref<'a>(
        &self,
        order: IndexOrder,
        datoms: impl IntoIterator<Item = &'a Datom>,
    ) -> Self {
        let pending: Vec<Vec<u8>> = datoms
            .into_iter()
            .map(|datom| datom.key(order))
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        if pending.is_empty() {
            return self.clone();
        }

        let mut builder = LeafBuilder::with_capacity(self.leaves.len() + 1);
        let mut cursor = 0;
        for (index, leaf) in self.leaves.iter().enumerate() {
            let keys = leaf.keys();
            let (Some(first), Some(last)) = (keys.first(), keys.last()) else {
                continue;
            };
            while pending
                .get(cursor)
                .is_some_and(|key| key.as_slice() < first.as_slice())
            {
                builder.push(pending[cursor].clone());
                cursor += 1;
            }
            let touched = pending
                .get(cursor)
                .is_some_and(|key| key.as_slice() <= last.as_slice());
            let followed = index + 1 < self.leaves.len() || cursor < pending.len();
            if !touched && (leaf.is_closed() || !followed) {
                builder.carry(leaf);
                continue;
            }
            if touched {
                builder.begin_dirty();
            }
            for key in keys {
                while pending
                    .get(cursor)
                    .is_some_and(|inserted| inserted.as_slice() < key.as_slice())
                {
                    builder.push(pending[cursor].clone());
                    cursor += 1;
                }
                if pending.get(cursor).is_some_and(|inserted| inserted == key) {
                    // Re-indexing the same transaction is idempotent.
                    cursor += 1;
                }
                builder.push(key.clone());
            }
        }
        for key in &pending[cursor..] {
            builder.push(key.clone());
        }
        Self {
            leaves: builder.finish().into(),
        }
    }

    /// Counts keys shared exactly with `older`.
    ///
    /// Both segments are sorted, so this is one linear pass over the two key
    /// streams rather than an index built over either of them.
    #[must_use]
    pub fn shared_key_count(&self, older: &Self) -> usize {
        let mut mine = self.keys().peekable();
        let mut theirs = older.keys().peekable();
        let mut shared = 0;
        while let (Some(left), Some(right)) = (mine.peek(), theirs.peek()) {
            match left.cmp(right) {
                Ordering::Less => {
                    mine.next();
                }
                Ordering::Greater => {
                    theirs.next();
                }
                Ordering::Equal => {
                    shared += 1;
                    mine.next();
                    theirs.next();
                }
            }
        }
        shared
    }

    /// Counts leaves carried over from `older` by handle — the chunks a
    /// publisher can reuse instead of re-encoding and re-uploading.
    #[must_use]
    pub fn shared_leaf_count(&self, older: &Self) -> usize {
        let old: HashSet<LeafId> = older.leaves.iter().map(Leaf::id).collect();
        self.leaves
            .iter()
            .filter(|leaf| old.contains(&leaf.id()))
            .count()
    }
}

/// Assembles a segment's leaves, cutting rebuilt runs at the same
/// content-defined boundaries a fresh build would and carrying untouched
/// leaves across by handle.
#[derive(Default)]
struct LeafBuilder {
    leaves: Vec<Leaf>,
    /// Keys of the rebuilt run in progress, since the last cut.
    pending: Vec<Vec<u8>>,
    /// Untouched leaves the open run has already re-cut through.
    spilled: usize,
}

impl LeafBuilder {
    fn with_capacity(leaves: usize) -> Self {
        Self {
            leaves: Vec::with_capacity(leaves),
            ..Self::default()
        }
    }

    fn push(&mut self, key: Vec<u8>) {
        debug_assert!(
            self.pending.last().is_none_or(|previous| *previous < key),
            "segment keys must be strictly increasing"
        );
        let cut = chunk::cut_after(&key, self.pending.len() + 1);
        self.pending.push(key);
        if cut {
            self.seal();
        }
    }

    fn push_change(&mut self, key: Option<Vec<u8>>) {
        if let Some(key) = key {
            self.push(key);
        }
    }

    /// Notes that the leaf about to be pushed through is one the tail
    /// touched, so re-cutting it is required work rather than spill.
    fn begin_dirty(&mut self) {
        self.spilled = 0;
    }

    /// Carries an untouched leaf over by handle when the chunking still
    /// agrees with the previous generation's, and re-cuts through it when it
    /// does not.
    ///
    /// A rebuilt run that is still open reaches this leaf mid-chunk, so the
    /// boundary at its end may have moved; the run continues through it until
    /// it cuts where the old chunking cut, after which leaves carry over
    /// untouched again. [`RESYNC_LEAF_LIMIT`] caps how far that search runs.
    fn carry(&mut self, leaf: &Leaf) {
        if self.pending.is_empty() {
            self.spilled = 0;
            self.leaves.push(leaf.clone());
            return;
        }
        if self.spilled >= RESYNC_LEAF_LIMIT {
            self.seal();
            self.spilled = 0;
            self.leaves.push(leaf.clone());
            return;
        }
        self.spilled += 1;
        for key in leaf.keys() {
            self.push(key.clone());
        }
    }

    fn seal(&mut self) {
        if !self.pending.is_empty() {
            self.leaves
                .push(Leaf(Arc::from(std::mem::take(&mut self.pending))));
        }
    }

    fn finish(mut self) -> Vec<Leaf> {
        self.seal();
        self.leaves
    }
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

    fn retraction(e: u64, a: u64, v: i64) -> Datom {
        Datom {
            added: false,
            tx: EntityId::from_raw(9_999),
            ..datom(e, a, v)
        }
    }

    /// Datoms for entities `0..count`, enough of them to span several leaves.
    fn many(count: u64) -> Vec<Datom> {
        (0..count)
            .map(|e| datom(e, 1, i64::try_from(e).expect("test value fits i64")))
            .collect()
    }

    fn leaf_sizes(segment: &Segment) -> Vec<usize> {
        segment.leaves().map(|leaf| leaf.keys().len()).collect()
    }

    /// Leaves whose boundaries a rebuild from scratch would not reproduce.
    fn boundary_drift(incremental: &Segment, fresh: &Segment) -> usize {
        let left = leaf_sizes(incremental);
        let right = leaf_sizes(fresh);
        let matching = left.iter().zip(&right).take_while(|(a, b)| a == b).count();
        left.len().max(right.len()) - matching
    }

    #[test]
    fn build_seek_and_apply_match_btree_model() {
        let left = Segment::build(IndexOrder::Eavt, [datom(2, 1, 20), datom(1, 1, 10)]);
        let right = left.apply(IndexOrder::Eavt, [datom(3, 1, 30)]);
        assert_eq!(right.len(), 3);
        assert_eq!(right.shared_key_count(&left), 2);
        let second = datom(2, 1, 20).key(IndexOrder::Eavt);
        assert_eq!(right.seek(&second).next(), Some(&second));
    }

    #[test]
    fn apply_removes_the_asserted_fact_a_retraction_cancels() {
        let segment = Segment::build(IndexOrder::Eavt, [datom(1, 1, 10), datom(2, 1, 20)]);
        // The retraction carries a different transaction than the assertion,
        // so removal has to key on the fact rather than the whole key.
        let after = segment.apply(IndexOrder::Eavt, [retraction(1, 1, 10)]);
        assert_eq!(
            after.keys().collect::<Vec<_>>(),
            vec![&datom(2, 1, 20).key(IndexOrder::Eavt)]
        );
    }

    #[test]
    fn apply_keeps_the_tails_final_answer_for_a_fact() {
        let segment = Segment::build(IndexOrder::Eavt, [datom(1, 1, 10)]);
        // Asserted then retracted within one tail: nothing survives.
        let churned = segment.apply(IndexOrder::Eavt, [datom(2, 1, 20), retraction(2, 1, 20)]);
        assert_eq!(churned.len(), 1);
        // Retracted then re-asserted: the assertion survives.
        let restored = segment.apply(IndexOrder::Eavt, [retraction(1, 1, 10), datom(1, 1, 10)]);
        assert_eq!(restored.len(), 1);
    }

    #[test]
    fn retracting_an_absent_fact_changes_nothing() {
        let segment = Segment::build(IndexOrder::Eavt, [datom(1, 1, 10)]);
        let after = segment.apply(IndexOrder::Eavt, [retraction(7, 1, 70)]);
        assert_eq!(after.len(), 1);
        assert_eq!(after.shared_leaf_count(&segment), 1);
    }

    #[test]
    fn an_empty_tail_reuses_every_leaf() {
        let segment = Segment::from_sorted(IndexOrder::Eavt, many(5_000));
        let after = segment.apply(IndexOrder::Eavt, []);
        assert_eq!(after.shared_leaf_count(&segment), segment.leaves().len());
    }

    #[test]
    fn append_reuses_every_settled_leaf() {
        let original = Segment::from_sorted(IndexOrder::Eavt, many(20_000));
        let leaves = original.leaves().len();
        assert!(leaves > 4, "20k datoms should span several leaves");
        let appended = original.apply(IndexOrder::Eavt, [datom(1_000_000, 1, 1_000_000)]);
        assert_eq!(
            appended.shared_leaf_count(&original),
            leaves - 1,
            "an append must only rebuild the leaf it lands in"
        );
    }

    #[test]
    fn mid_keyspace_change_reuses_untouched_leaves() {
        // Even entities only, so an odd entity lands strictly inside a leaf.
        let original = Segment::from_sorted(
            IndexOrder::Eavt,
            (0..20_000u64).map(|e| datom(e * 2, 1, i64::try_from(e).expect("fits i64"))),
        );
        let leaves = original.leaves().len();
        let merged = original.apply(IndexOrder::Eavt, [datom(20_001, 1, -1)]);

        assert_eq!(merged.len(), 20_001);
        let rebuilt = leaves - merged.shared_leaf_count(&original);
        assert!(
            rebuilt <= 1 + RESYNC_LEAF_LIMIT,
            "one insertion rebuilt {rebuilt} of {leaves} leaves"
        );
    }

    #[test]
    fn history_insertion_keeps_prior_statements_and_reuses_untouched_leaves() {
        let original = Segment::from_sorted(IndexOrder::Eavt, many(20_000));
        let retained = [retraction(1_000, 1, 1_000), retraction(15_000, 1, 15_000)];
        let inserted = original.insert_history_ref(IndexOrder::Eavt, retained.iter());

        let expected: std::collections::BTreeSet<Vec<u8>> = original
            .keys()
            .cloned()
            .chain(retained.iter().map(|datom| datom.key(IndexOrder::Eavt)))
            .collect();
        assert_eq!(
            inserted.keys().cloned().collect::<Vec<_>>(),
            expected.into_iter().collect::<Vec<_>>()
        );
        assert_eq!(inserted.len(), original.len() + retained.len());
        assert!(inserted.shared_leaf_count(&original) > 0);

        let repeated = inserted.insert_history_ref(IndexOrder::Eavt, retained.iter());
        assert_eq!(
            repeated.len(),
            inserted.len(),
            "history insertion is idempotent"
        );
    }

    #[test]
    fn incremental_chunking_tracks_a_rebuild_from_scratch() {
        // Structural sharing is only worth anything if a transactor that
        // rebuilds from scratch after a restart lands on the chunks the
        // previous process published.
        let mut all = many(20_000);
        let segment = Segment::from_sorted(IndexOrder::Eavt, all.clone());
        let tail: Vec<Datom> = (20_000..20_050)
            .map(|e| datom(e, 1, i64::try_from(e).expect("fits i64")))
            .collect();
        all.extend(tail.clone());

        let incremental = segment.apply(IndexOrder::Eavt, tail);
        let fresh = Segment::from_sorted(IndexOrder::Eavt, all);
        assert_eq!(
            incremental.keys().collect::<Vec<_>>(),
            fresh.keys().collect::<Vec<_>>()
        );
        assert!(
            boundary_drift(&incremental, &fresh) <= 1,
            "incremental leaves {:?} diverge from a fresh build {:?}",
            leaf_sizes(&incremental),
            leaf_sizes(&fresh)
        );
    }

    #[test]
    fn leaf_ids_distinguish_carried_leaves_from_rebuilt_ones() {
        let original = Segment::from_sorted(IndexOrder::Eavt, many(20_000));
        let appended = original.apply(IndexOrder::Eavt, [datom(1_000_000, 1, 1_000_000)]);
        let carried: HashSet<LeafId> = original.leaves().map(Leaf::id).collect();
        let rebuilt = appended
            .leaves()
            .filter(|leaf| !carried.contains(&leaf.id()))
            .count();
        assert_eq!(rebuilt, 1, "only the appended-to leaf should be rebuilt");
    }

    /// Publication indexes the database's own covering index and the log tail
    /// in place, so the borrowed variants have to agree with the owned ones
    /// key for key — they are the same fold, differing only in what the
    /// caller gives up.
    #[test]
    fn borrowed_and_owned_folds_agree() {
        let datoms = many(2_000);
        let tail = [datom(2_000, 1, 2_000), retraction(0, 1, 0)];

        let borrowed = Segment::from_sorted_ref(IndexOrder::Eavt, datoms.iter());
        let owned = Segment::from_sorted(IndexOrder::Eavt, datoms.clone());
        assert_eq!(
            borrowed.keys().collect::<Vec<_>>(),
            owned.keys().collect::<Vec<_>>()
        );

        let from_refs = borrowed.apply_ref(IndexOrder::Eavt, tail.iter());
        let from_values = owned.apply(IndexOrder::Eavt, tail);
        assert_eq!(
            from_refs.keys().collect::<Vec<_>>(),
            from_values.keys().collect::<Vec<_>>()
        );
        assert_eq!(from_refs.len(), 2_000);

        // The owned entry points still infer an untyped empty tail.
        assert_eq!(borrowed.apply(IndexOrder::Eavt, []).len(), 2_000);
        assert_eq!(borrowed.apply_ref(IndexOrder::Eavt, []).len(), 2_000);
    }

    #[test]
    fn shared_key_count_compares_two_sorted_streams() {
        let original = Segment::from_sorted(IndexOrder::Eavt, many(1_000));
        assert_eq!(original.shared_key_count(&original), 1_000);
        assert_eq!(original.shared_key_count(&Segment::default()), 0);
        assert_eq!(Segment::default().shared_key_count(&original), 0);

        // Disjoint runs share nothing even though both are non-empty; an
        // overlapping one shares exactly its overlap.
        let disjoint = Segment::from_sorted(
            IndexOrder::Eavt,
            (2_000..3_000u64).map(|e| datom(e, 1, i64::try_from(e).expect("fits i64"))),
        );
        assert_eq!(original.shared_key_count(&disjoint), 0);
        let overlapping = original.apply(IndexOrder::Eavt, [retraction(0, 1, 0)]);
        assert_eq!(overlapping.shared_key_count(&original), 999);
    }

    #[test]
    fn seek_handles_segment_bounds() {
        assert!(Segment::default().seek(b"anything").next().is_none());

        let total = 5_000u64;
        let segment = Segment::from_sorted(IndexOrder::Eavt, many(total));
        assert_eq!(
            segment.seek(&[]).count(),
            usize::try_from(total).expect("fits usize")
        );
        let last = total - 1;
        let last_key = datom(last, 1, i64::try_from(last).expect("fits i64")).key(IndexOrder::Eavt);
        assert_eq!(segment.seek(&last_key).count(), 1);
        assert!(segment.seek(&[0xFF; 64]).next().is_none());
    }

    proptest! {
        #[test]
        fn apply_matches_the_current_value_fold(
            original in prop::collection::vec((0_u16..500, any::<i16>()), 0..300),
            tail in prop::collection::vec((0_u16..500, any::<i16>(), any::<bool>()), 0..100),
            seek_entity in 0_u16..500,
        ) {
            let original_datoms: Vec<Datom> = original
                .into_iter()
                .map(|(e, value)| datom(u64::from(e), 1, i64::from(value)))
                .collect();
            let tail: Vec<Datom> = tail
                .into_iter()
                .map(|(e, value, added)| Datom {
                    added,
                    tx: EntityId::from_raw(50_000),
                    ..datom(u64::from(e), 1, i64::from(value))
                })
                .collect();
            let segment = Segment::build(IndexOrder::Eavt, original_datoms.clone());
            let applied = segment.apply(IndexOrder::Eavt, tail.clone());

            // The model is the fold the database itself applies to its
            // in-memory indexes: assert replaces the fact, retract removes it.
            let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
            for datom in original_datoms.into_iter().chain(tail) {
                let key = datom.key(IndexOrder::Eavt);
                let fact = key_components(&key).to_vec();
                if datom.added {
                    model.insert(fact, key);
                } else {
                    model.remove(&fact);
                }
            }
            let expected: Vec<Vec<u8>> = model.into_values().collect();
            prop_assert_eq!(applied.keys().cloned().collect::<Vec<_>>(), expected.clone());

            let seek_key = datom(u64::from(seek_entity), 0, i64::MIN).key(IndexOrder::Eavt);
            let actual_tail = applied.seek(&seek_key).cloned().collect::<Vec<_>>();
            let expected_tail = expected
                .into_iter()
                .skip_while(|key| *key < seek_key)
                .collect::<Vec<_>>();
            prop_assert_eq!(actual_tail, expected_tail);
        }

        /// However a segment is grown, its leaves stay the chunks the
        /// published format cuts — the invariant that lets a publisher
        /// re-upload only the leaves it rebuilt.
        #[test]
        fn leaves_obey_the_published_chunk_boundaries(
            original in prop::collection::vec(0_u16..5_000, 0..2_000),
            tail in prop::collection::vec((0_u16..5_000, any::<bool>()), 0..500),
        ) {
            let segment = Segment::build(
                IndexOrder::Eavt,
                original.into_iter().map(|e| datom(u64::from(e), 1, 0)),
            );
            let applied = segment.apply(
                IndexOrder::Eavt,
                tail.into_iter().map(|(e, added)| Datom {
                    added,
                    tx: EntityId::from_raw(50_000),
                    ..datom(u64::from(e), 1, 0)
                }),
            );
            for leaf in applied.leaves() {
                let keys = leaf.keys();
                prop_assert!(!keys.is_empty());
                prop_assert!(keys.len() <= chunk::CHUNK_MAX_ENTRIES);
                // No cut may fall where the rule says the chunk runs on.
                for (position, key) in keys.iter().enumerate().take(keys.len() - 1) {
                    prop_assert!(!chunk::cut_after(key, position + 1));
                }
            }
        }
    }
}
