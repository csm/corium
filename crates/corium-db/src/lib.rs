//! Immutable database values: time views, covering-index access, naming,
//! per-attribute statistics, and bootstrap metadata.
//!
//! A [`Db`] is a value: cheap to clone, never mutated in place. Time views
//! ([`Db::as_of`], [`Db::since`], [`Db::history`]) wrap the same recorded
//! datoms with a different fold policy — no copying of facts. The four
//! covering indexes for a view are materialized lazily on first read and
//! shared by every clone of that value.
//!
//! Cheap to clone is not the same as cheap to hold or cheap to open: see
//! [`Db`'s memory and cost notes](Db#memory), which say plainly what a
//! holder of this value pays today and where that departs from the segment
//! design in `docs/design/indexes-and-storage.md`.
//!
//! Transaction time is data: every committed transaction asserts
//! `:db/txInstant` on its transaction entity (see [`bootstrap`]), so views can
//! also be named by wall clock ([`Db::as_of_instant`], [`Db::since_instant`]).
//!
//! Schema is data too: attribute metadata is stored under the vocabulary in
//! [`bootstrap`] and folded back into a [`Schema`] by [`schemadatoms`], so
//! applying a transaction derives the schema it leaves behind. Every holder of
//! a `Db` — transactor, peer, replay — reaches the same schema from the same
//! log without a side channel.

pub mod bootstrap;
pub mod impact;
pub mod protect;
pub mod schemadatoms;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, OnceLock};

use corium_core::{
    AttrId, Cardinality, Datom, EntityId, IndexOrder, Keyword, KeywordInterner, Partition, Schema,
    Unique, Value, encoding::Encodable,
};
use rpds::{RedBlackTreeMapSync, VectorSync};

/// Sequence of the first installable attribute entity in the db partition.
///
/// Everything below this in `:db.part/db` belongs to the engine: the schema
/// vocabulary, `:db/txInstant`, and the schema-update audit trail
/// ([`bootstrap`]). A schema that claims one of those ids is silently replaced
/// when the engine installs its own, so the reservation is asserted in debug
/// builds rather than left as a rule to remember.
pub const FIRST_ATTR_ID: u64 = 100;

/// The first user-assignable sequence number. Lower ids are reserved for bootstrap data.
pub const FIRST_USER_ID: u64 = 1_000;

/// Time-view selector for a database value (see `docs/design/time-model.md`).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum DbView {
    /// Live facts as of the basis transaction.
    #[default]
    Current,
    /// Facts as they stood at basis `t` (inclusive).
    AsOf(u64),
    /// Only live facts added after `t` (exclusive).
    Since(u64),
    /// Every assertion and retraction ever recorded, except `:db/noHistory` attributes.
    History,
}

/// Registry of `:db/ident` names for entities (attributes chiefly).
#[derive(Clone, Debug, Default)]
pub struct Idents {
    by_keyword: BTreeMap<Keyword, EntityId>,
    by_id: BTreeMap<EntityId, Keyword>,
}

impl Idents {
    /// Registers an ident for an entity.
    pub fn insert(&mut self, keyword: Keyword, id: EntityId) {
        self.by_id.insert(id, keyword.clone());
        self.by_keyword.insert(keyword, id);
    }

    /// Resolves a keyword to its entity id.
    #[must_use]
    pub fn entid(&self, keyword: &Keyword) -> Option<EntityId> {
        self.by_keyword.get(keyword).copied()
    }

    /// Resolves an entity id back to its ident.
    #[must_use]
    pub fn ident(&self, id: EntityId) -> Option<&Keyword> {
        self.by_id.get(&id)
    }

    /// Iterates all registered idents in keyword order.
    pub fn iter(&self) -> impl Iterator<Item = (&Keyword, &EntityId)> {
        self.by_keyword.iter()
    }
}

/// Per-attribute statistics driving planner selectivity estimates.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AttrStats {
    /// Datoms carrying this attribute in the view.
    pub count: usize,
    /// Distinct values for this attribute.
    pub distinct_values: usize,
    /// Distinct entities carrying this attribute.
    pub distinct_entities: usize,
    /// Whether the attribute is protected, in which case the planner must
    /// not use `distinct_values`.
    pub protected: bool,
}

/// Whole-view statistics for the query planner.
#[derive(Clone, Debug, Default)]
pub struct PlannerStats {
    /// Statistics per attribute.
    pub per_attr: BTreeMap<AttrId, AttrStats>,
    /// Total datoms in the view.
    pub total_datoms: usize,
    /// Distinct entities in the view.
    pub entity_count: usize,
}

impl PlannerStats {
    /// Estimated datoms matched by a scan with the given bound components.
    #[must_use]
    pub fn estimate(&self, e_bound: bool, a: Option<AttrId>, v_bound: bool) -> usize {
        let attr = a.and_then(|a| self.per_attr.get(&a));
        match (e_bound, attr) {
            // Bound entity: at most the entity's datoms; refine by attribute.
            (true, Some(stats)) => (stats.count / stats.distinct_entities.max(1)).max(1),
            (true, None) => (self.total_datoms / self.entity_count.max(1)).max(1),
            // A bound value on a protected attribute is treated as unbound
            // for selectivity. A distinct-*ciphertext* count would be a real
            // estimate and a real leak, so it is dropped rather than used
            // (`docs/design/encryption.md`).
            (false, Some(stats)) if v_bound && !stats.protected => {
                (stats.count / stats.distinct_values.max(1)).max(1)
            }
            (false, Some(stats)) => stats.count.max(1),
            // Unknown attribute constant: nothing will match.
            (false, None) if a.is_some() => 1,
            (false, None) => self.total_datoms.max(1),
        }
    }
}

/// Covering index for one order.
///
/// A persistent (structurally shared) ordered map: cloning is O(1) and shares
/// every unchanged node with the parent, so applying a transaction to a
/// materialized index derives a new map that copies only the O(log n) nodes on
/// the touched paths instead of the whole tree. This is what keeps
/// `with_transaction` — and the per-operation folding inside `corium-tx` —
/// from re-cloning the entire index on every write.
///
/// Entries hold the datom [by handle](Db#memory): a datom is allocated once,
/// when it is recorded, and the log and every covering index of every
/// materialized view point at that one allocation. Only the encoded key is
/// per-index, so a second order costs a key and a pointer rather than a
/// duplicate of the fact — which matters most for the values that are
/// themselves heap-allocated (strings, byte arrays), since those would
/// otherwise be copied once per order per view.
type Index = RedBlackTreeMapSync<Vec<u8>, Arc<Datom>>;

const ORDERS: [IndexOrder; 4] = [
    IndexOrder::Eavt,
    IndexOrder::Aevt,
    IndexOrder::Avet,
    IndexOrder::Vaet,
];

const fn slot(order: IndexOrder) -> usize {
    match order {
        IndexOrder::Eavt => 0,
        IndexOrder::Aevt => 1,
        IndexOrder::Avet => 2,
        IndexOrder::Vaet => 3,
    }
}

/// Builds the encoded key prefix for a partial datom in one index order.
///
/// Components are consumed in the index's component order and encoding stops
/// at the first missing component, so the result is a proper range prefix.
#[must_use]
pub fn key_prefix(
    order: IndexOrder,
    e: Option<EntityId>,
    a: Option<AttrId>,
    v: Option<&Value>,
) -> Vec<u8> {
    enum C {
        E,
        A,
        V,
    }
    let components = match order {
        IndexOrder::Eavt => [C::E, C::A, C::V],
        IndexOrder::Aevt => [C::A, C::E, C::V],
        IndexOrder::Avet => [C::A, C::V, C::E],
        IndexOrder::Vaet => [C::V, C::A, C::E],
    };
    let mut out = Vec::new();
    for component in components {
        match component {
            C::E => match e {
                Some(e) => e.encode_into(&mut out),
                None => break,
            },
            C::A => match a {
                Some(a) => a.encode_into(&mut out),
                None => break,
            },
            C::V => match v {
                Some(v) => v.encode_into(&mut out),
                None => break,
            },
        }
    }
    out
}

/// The `t` ↔ `:db/txInstant` correspondence for the transactions a value has
/// seen, in both directions.
///
/// Both maps are persistent, so carrying them on every database value costs
/// one O(log n) insert per transaction and nothing per clone. They are a
/// property of the recorded transactions rather than of a time view, so a
/// derived view (`as-of`, `since`, `history`) keeps the whole correspondence:
/// naming a view by instant means the same thing whatever view you start from.
///
/// `:db/txInstant` is monotone by construction (the transactor stamps
/// `max(now, last + 1)`), so ordering by instant and ordering by `t` agree.
#[derive(Clone, Debug, Default)]
pub struct TxInstants {
    by_t: RedBlackTreeMapSync<u64, i64>,
    by_instant: RedBlackTreeMapSync<i64, u64>,
}

impl TxInstants {
    fn record(&mut self, t: u64, instant: i64) {
        self.by_t.insert_mut(t, instant);
        // Equal instants can only come from a log written without the
        // monotonicity rule; keeping the highest `t` makes resolution pick the
        // last transaction that shares an instant, matching `as-of`'s
        // inclusive semantics.
        if self.by_instant.get(&instant).is_none_or(|held| *held < t) {
            self.by_instant.insert_mut(instant, t);
        }
    }

    /// The commit instant of transaction `t`.
    #[must_use]
    pub fn instant(&self, t: u64) -> Option<i64> {
        self.by_t.get(&t).copied()
    }

    /// The latest `t` committed at or before `instant`, or zero when no known
    /// transaction is that old (the basis of an empty database).
    #[must_use]
    pub fn t_at(&self, instant: i64) -> u64 {
        self.by_instant
            .range(..=instant)
            .next_back()
            .map_or(0, |(_, t)| *t)
    }

    /// Number of transactions with a known instant.
    #[must_use]
    pub fn len(&self) -> usize {
        self.by_t.size()
    }

    /// Whether no transaction instant is known.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_t.is_empty()
    }
}

/// An immutable value of a database at one basis transaction and time view.
///
/// # Memory
///
/// This value is fully resident. `recorded` holds **every datom the value has
/// seen** — assertions and retractions alike, the whole history, not just the
/// live facts — and a materialized view adds four covering indexes over it.
/// A holder therefore pays, per datom:
///
/// - one heap allocation for the datom itself, shared by handle with every
///   index entry that refers to it, in every materialized view; plus
/// - one encoded key per covering order the datom is in (always EAVT and
///   AEVT, plus AVET for an indexed or unique attribute and VAET for a
///   reference value), plus a pointer per entry.
///
/// So a fact is stored once however many orders and views index it. The keys
/// are not free — a key embeds the encoded value, so a long string still
/// appears once per covering order — but they are the ordering itself rather
/// than a duplicate of the datom.
///
/// What is *not* bounded is history: nothing here is ever evicted, and a
/// peer's resident set grows with everything ever transacted rather than
/// with the live database. A database whose history greatly exceeds its live
/// set will be dominated by datoms no current query can reach.
///
/// # Cost of a time view
///
/// Materializing a view is O(recorded history), not O(the view's size).
/// [`Db::as_of`] folds the log up to its basis, [`Db::history`] folds all of
/// it, and [`Db::since`] folds all of it and then filters. The result is
/// cached in this value and shared by its clones — and views that select
/// exactly the same datoms as an already-folded one share that fold rather
/// than repeating it — but a *distinct* time view is a fresh fold over the
/// whole history the first time it is read. Deriving many different `as-of`
/// values, or re-deriving one from a fresh [`Db`] each time, pays that fold
/// each time.
///
/// # How this differs from the design
///
/// `docs/design/indexes-and-storage.md` describes readers descending
/// persistent segment trees and pulling only the segments a query touches,
/// through a bounded cache. That is what the published format is being built
/// toward; the inner tree levels that would let a reader seek without
/// downloading an index are still future work. Until they land, a peer holds
/// the whole database — and its whole history — in memory, and durable
/// storage serves to reconstruct that state rather than to bound it.
#[derive(Clone, Debug)]
pub struct Db {
    basis_t: u64,
    schema: Schema,
    recorded: VectorSync<Arc<Datom>>,
    idents: Arc<Idents>,
    interner: Arc<KeywordInterner>,
    /// Monotone, database-local counter that advances once per committed
    /// transaction containing a schema change. The basis says *when* a schema
    /// changed; the generation says whether two database values — a peer's and
    /// a transactor's, a published index root and the log — were built under
    /// the same schema.
    schema_generation: u64,
    view: DbView,
    instants: TxInstants,
    indexes: Arc<OnceLock<[Index; 4]>>,
    stats: Arc<OnceLock<PlannerStats>>,
    /// Whether `recorded` is the whole log rather than a snapshot's live
    /// prefix. A value opened from a published current-state snapshot has
    /// already lost the retractions before that snapshot's basis, so
    /// historical questions about it can be answered only as a floor.
    complete_history: bool,
}

impl Default for Db {
    fn default() -> Self {
        Self {
            basis_t: 0,
            schema: Schema::default(),
            recorded: VectorSync::new_sync(),
            idents: Arc::default(),
            interner: Arc::default(),
            schema_generation: 0,
            view: DbView::default(),
            instants: TxInstants::default(),
            indexes: Arc::new(OnceLock::new()),
            stats: Arc::new(OnceLock::new()),
            // An empty database has nothing missing from its history.
            complete_history: true,
        }
    }
}

impl Db {
    /// Creates an empty database with the supplied schema.
    ///
    /// The engine's own attributes ([`bootstrap`]) are installed over
    /// `schema`, so every database understands `:db/txInstant` whether or not
    /// the caller's schema mentions it.
    #[must_use]
    pub fn new(mut schema: Schema) -> Self {
        bootstrap::install_schema(&mut schema);
        Self {
            schema,
            ..Self::default()
        }
    }

    /// Creates a current database value from a published EAVT snapshot.
    ///
    /// `datoms` must be the live facts at `basis_t`. Their original
    /// transaction ids are retained, but transactions before `basis_t` that
    /// no longer contribute a live fact are not reconstructed. This makes
    /// the value suitable for current queries and for applying the log tail;
    /// complete historical views still require replaying the full log.
    ///
    /// `:db/txInstant` datoms are live facts (nothing retracts them), so a
    /// snapshot carries the transaction-time correspondence for every
    /// transaction it covers — unless it was published before the engine
    /// recorded instants as datoms, in which case instant-named views resolve
    /// only within the replayed tail.
    #[must_use]
    pub fn from_current_snapshot(
        basis_t: u64,
        mut schema: Schema,
        mut idents: Idents,
        interner: KeywordInterner,
        datoms: Vec<Datom>,
    ) -> Self {
        bootstrap::install(&mut schema, &mut idents);
        let mut instants = TxInstants::default();
        for datom in &datoms {
            if let Datom {
                e,
                a,
                v: Value::Instant(instant),
                tx,
                added: true,
                ..
            } = datom
                && *a == bootstrap::TX_INSTANT
                && *e == *tx
                && tx.partition() == Partition::Tx as u32
            {
                instants.record(tx.sequence(), *instant);
            }
        }
        Self {
            basis_t,
            schema,
            recorded: datoms.into_iter().map(Arc::new).collect(),
            idents: Arc::new(idents),
            interner: Arc::new(interner),
            schema_generation: 0,
            view: DbView::Current,
            instants,
            indexes: Arc::new(OnceLock::new()),
            stats: Arc::new(OnceLock::new()),
            complete_history: false,
        }
    }

    /// Attaches ident and keyword naming registries, returning the named value.
    #[must_use]
    pub fn with_naming(mut self, mut idents: Idents, interner: KeywordInterner) -> Self {
        bootstrap::install(&mut self.schema, &mut idents);
        self.idents = Arc::new(idents);
        self.interner = Arc::new(interner);
        self
    }

    /// Current transaction basis.
    #[must_use]
    pub const fn basis_t(&self) -> u64 {
        self.basis_t
    }

    /// Schema at this basis.
    #[must_use]
    pub const fn schema(&self) -> &Schema {
        &self.schema
    }

    /// Ident registry.
    #[must_use]
    pub fn idents(&self) -> &Idents {
        &self.idents
    }

    /// Schema generation of this value.
    ///
    /// A monotone database-local counter, separate from the transaction basis:
    /// it advances once for a committed transaction that changes the schema.
    /// Two values with the same generation were built under the same schema.
    #[must_use]
    pub const fn schema_generation(&self) -> u64 {
        self.schema_generation
    }

    /// Keyword interner used by keyword values in this database.
    #[must_use]
    pub fn interner(&self) -> &KeywordInterner {
        &self.interner
    }

    /// The time view this value presents.
    #[must_use]
    pub const fn view(&self) -> DbView {
        self.view
    }

    /// Whether this value carries every fact ever recorded, rather than a
    /// published snapshot's live prefix plus the log tail replayed onto it.
    ///
    /// History views and historical counts are exact only when this holds.
    /// It is a property of how the value was opened, so replaying more
    /// transactions onto a snapshot never restores it.
    #[must_use]
    pub const fn has_complete_history(&self) -> bool {
        self.complete_history
    }

    /// Every recorded assertion and retraction, in transaction order.
    pub fn recorded_datoms(&self) -> impl Iterator<Item = &Datom> {
        self.recorded.iter().map(AsRef::as_ref)
    }

    /// Number of recorded assertions and retractions.
    #[must_use]
    pub fn recorded_len(&self) -> usize {
        self.recorded.len()
    }

    /// The datoms transactions after `t` recorded, in the order they were
    /// recorded.
    ///
    /// `recorded` is append-ordered, so the answer is a suffix: the scan
    /// walks back from the end and stops at the first datom at or before `t`,
    /// costing time proportional to the tail rather than to the whole
    /// history. This is what an indexing pass folds into the segments it last
    /// published ([`corium_index::Segment::apply`]) instead of rebuilding
    /// them.
    ///
    /// A value opened from a published snapshot carries that snapshot's live
    /// datoms as its prefix rather than a transaction-ordered history; every
    /// one of them is at or before the snapshot's basis, so the same scan
    /// still stops at the boundary for any `t` at or after it.
    pub fn recorded_since(&self, t: u64) -> impl Iterator<Item = &Datom> {
        let mut start = self.recorded.len();
        while start > 0
            && self
                .recorded
                .get(start - 1)
                .is_some_and(|datom| datom.tx.sequence() > t)
        {
            start -= 1;
        }
        // Indexed access rather than `iter().skip(start)`: the persistent
        // vector's iterator would have to walk the whole prefix to reach the
        // tail, which is the cost this method exists to avoid.
        (start..self.recorded.len())
            .filter_map(|index| self.recorded.get(index))
            .map(AsRef::as_ref)
    }

    /// Returns the as-of view at basis `t`: facts as they stood then.
    #[must_use]
    pub fn as_of(&self, t: u64) -> Self {
        self.with_view(DbView::AsOf(t))
    }

    /// Returns the since view: only live facts added after `t`.
    #[must_use]
    pub fn since(&self, t: u64) -> Self {
        self.with_view(DbView::Since(t))
    }

    /// Returns the history view: all assertions and retractions ever.
    #[must_use]
    pub fn history(&self) -> Self {
        self.with_view(DbView::History)
    }

    /// The `t` ↔ `:db/txInstant` correspondence recorded by this value.
    #[must_use]
    pub const fn instants(&self) -> &TxInstants {
        &self.instants
    }

    /// The commit instant of transaction `t`, when this value has seen it.
    #[must_use]
    pub fn tx_instant(&self, t: u64) -> Option<i64> {
        self.instants.instant(t)
    }

    /// The latest basis committed at or before `instant` (Unix milliseconds).
    ///
    /// Zero when no known transaction is that old, so `as-of` an instant
    /// before the database existed is the empty value and `since` that instant
    /// is everything — the same interpretation Datomic gives out-of-range
    /// instants.
    #[must_use]
    pub fn t_at_instant(&self, instant: i64) -> u64 {
        self.instants.t_at(instant)
    }

    /// Returns the as-of view at wall-clock `instant` (Unix milliseconds):
    /// facts as they stood after the last transaction committed at or before
    /// it.
    #[must_use]
    pub fn as_of_instant(&self, instant: i64) -> Self {
        self.as_of(self.t_at_instant(instant))
    }

    /// Returns the since view at wall-clock `instant` (Unix milliseconds):
    /// only live facts added after the last transaction committed at or before
    /// it.
    #[must_use]
    pub fn since_instant(&self, instant: i64) -> Self {
        self.since(self.t_at_instant(instant))
    }

    fn with_view(&self, view: DbView) -> Self {
        if view == self.view {
            return self.clone();
        }
        // Two views that select the same datoms share one fold, so a view
        // that merely names the current value does not pay to rebuild it.
        // The declared view is kept as asked for: callers distinguish a time
        // view from the current value regardless of what it holds (writes,
        // for instance, are refused on any time view).
        let shared = self.fold_class(view) == self.fold_class(self.view);
        Self {
            view,
            indexes: if shared {
                Arc::clone(&self.indexes)
            } else {
                Arc::new(OnceLock::new())
            },
            stats: if shared {
                Arc::clone(&self.stats)
            } else {
                Arc::new(OnceLock::new())
            },
            ..self.clone()
        }
    }

    /// Canonicalizes a view to the fold that materializes it, so views that
    /// differ only in name share an index and statistics cache.
    ///
    /// Every recorded datom belongs to a transaction at or before the basis,
    /// so an `as-of` at or after the basis excludes nothing; and transaction
    /// numbering starts at one, so a `since` at basis zero excludes nothing
    /// either. Both therefore fold exactly as the current view does.
    const fn fold_class(&self, view: DbView) -> DbView {
        match view {
            DbView::AsOf(t) if t >= self.basis_t => DbView::Current,
            DbView::Since(0) => DbView::Current,
            other => other,
        }
    }

    /// Groups recorded datoms by transaction over the half-open range `[start, end)`.
    #[must_use]
    pub fn tx_range(&self, start: u64, end: Option<u64>) -> Vec<(u64, Vec<Datom>)> {
        let mut by_t: BTreeMap<u64, Vec<Datom>> = BTreeMap::new();
        for datom in &self.recorded {
            let t = datom.tx.sequence();
            if t >= start && end.is_none_or(|end| t < end) {
                by_t.entry(t).or_default().push((**datom).clone());
            }
        }
        by_t.into_iter().collect()
    }

    /// Returns this view's facts, deterministically ordered by EAVT.
    #[must_use]
    pub fn datoms(&self) -> Vec<Datom> {
        self.datoms_at(IndexOrder::Eavt).cloned().collect()
    }

    /// Iterates this view's datoms in one index order.
    ///
    /// AVET covers only indexed/unique attributes and VAET only reference
    /// values, mirroring Datomic's covering-index composition.
    pub fn datoms_at(&self, order: IndexOrder) -> impl Iterator<Item = &Datom> {
        self.indexes()[slot(order)].values().map(AsRef::as_ref)
    }

    /// Iterates this view's datoms for one attribute in AEVT order.
    ///
    /// Unlike AVET, AEVT covers every installed attribute. Callers can use
    /// this as the fallback for attribute predicates that do not have AVET
    /// coverage.
    pub fn datoms_for_attribute(&self, a: AttrId) -> impl Iterator<Item = &Datom> {
        let prefix = key_prefix(IndexOrder::Aevt, None, Some(a), None);
        self.indexes()[slot(IndexOrder::Aevt)]
            .range(prefix.clone()..)
            .take_while(move |(key, _)| key.starts_with(&prefix))
            .map(|(_, datom)| datom.as_ref())
    }

    /// Iterates datoms whose key in `order` starts with `prefix`.
    pub fn datoms_prefix<'a>(
        &'a self,
        order: IndexOrder,
        prefix: &'a [u8],
    ) -> impl Iterator<Item = &'a Datom> {
        self.indexes()[slot(order)]
            .range(prefix.to_vec()..)
            .take_while(move |(key, _)| key.starts_with(prefix))
            .map(|(_, datom)| datom.as_ref())
    }

    /// Iterates datoms in `order` starting from the first key at or after `start`.
    pub fn seek_datoms<'a>(
        &'a self,
        order: IndexOrder,
        start: &[u8],
    ) -> impl Iterator<Item = &'a Datom> {
        self.indexes()[slot(order)]
            .range(start.to_vec()..)
            .map(|(_, datom)| datom.as_ref())
    }

    /// Iterates the AVET index for `a` over the value range `[start, end)`.
    ///
    /// Only indexed/unique attributes appear in AVET.
    ///
    /// # Errors
    ///
    /// Returns [`RangeError::Protected`] for a protected attribute. Sealed
    /// order is byte order, not value order, so a range over one would return
    /// silently wrong answers; the design makes it a loud failure instead.
    pub fn index_range<'a>(
        &'a self,
        a: AttrId,
        start: Option<&Value>,
        end: Option<&'a Value>,
    ) -> Result<impl Iterator<Item = &'a Datom>, RangeError> {
        if self.schema.protection(a).ever_protected() {
            return Err(RangeError::Protected(a));
        }
        let a_prefix = key_prefix(IndexOrder::Avet, None, Some(a), None);
        let start_key = key_prefix(IndexOrder::Avet, None, Some(a), start);
        Ok(self.indexes()[slot(IndexOrder::Avet)]
            .range(start_key..)
            .take_while(move |(key, _)| key.starts_with(&a_prefix))
            .map(|(_, datom)| datom.as_ref())
            .take_while(move |datom| end.is_none_or(|end| datom.v < *end)))
    }

    /// Current values for an entity/attribute pair.
    #[must_use]
    pub fn values(&self, e: EntityId, a: AttrId) -> Vec<Value> {
        let prefix = key_prefix(IndexOrder::Eavt, Some(e), Some(a), None);
        self.datoms_prefix(IndexOrder::Eavt, &prefix)
            .map(|datom| datom.v.clone())
            .collect()
    }

    /// Resolves a unique attribute/value pair to its entity.
    #[must_use]
    pub fn lookup(&self, a: AttrId, v: &Value) -> Option<EntityId> {
        if avet_covered(&self.schema, a) {
            let prefix = key_prefix(IndexOrder::Avet, None, Some(a), Some(v));
            self.datoms_prefix(IndexOrder::Avet, &prefix)
                .next()
                .map(|datom| datom.e)
        } else {
            let prefix = key_prefix(IndexOrder::Aevt, None, Some(a), None);
            self.datoms_prefix(IndexOrder::Aevt, &prefix)
                .find(|datom| datom.v == *v)
                .map(|datom| datom.e)
        }
    }

    /// Applies a committed record, returning a new database value.
    ///
    /// Only meaningful for the current view; time views are read-only.
    ///
    /// A `:db/txInstant` assertion on the transaction entity — which every
    /// commit path materializes — is picked up as this transaction's place on
    /// the wall clock.
    #[must_use]
    pub fn with_transaction(&self, t: u64, datoms: &[Datom]) -> Self {
        match bootstrap::asserted_instant(t, datoms) {
            Some(instant) => self.with_transaction_at(t, instant, datoms),
            None => self.apply_transaction(t, datoms),
        }
    }

    /// Applies a committed record whose commit instant is known separately,
    /// materializing the `:db/txInstant` datom when `datoms` lacks one.
    ///
    /// Replay paths use this so a log written before Corium recorded instants
    /// as datoms still yields instant-named views: the record's timestamp
    /// field becomes the datom it would carry today.
    #[must_use]
    pub fn with_transaction_at(&self, t: u64, instant: i64, datoms: &[Datom]) -> Self {
        let asserted = bootstrap::asserted_instant(t, datoms);
        let mut next = if asserted.is_some() {
            self.apply_transaction(t, datoms)
        } else {
            let mut with_instant = Vec::with_capacity(datoms.len() + 1);
            with_instant.extend_from_slice(datoms);
            with_instant.push(bootstrap::tx_instant_datom(t, instant));
            self.apply_transaction(t, &with_instant)
        };
        next.instants.record(t, asserted.unwrap_or(instant));
        next
    }

    fn apply_transaction(&self, t: u64, datoms: &[Datom]) -> Self {
        debug_assert!(
            self.view == DbView::Current,
            "with_transaction applies only to the current view"
        );
        let mut next = self.clone();
        next.basis_t = t;
        // Schema effects come first, before a single user datom of the same
        // transaction is indexed. That ordering is what makes a transaction
        // that installs an attribute and immediately uses it legal, and it is
        // derived from the record rather than from the order the datoms happen
        // to be listed in.
        let schema_changed = schemadatoms::changes_schema(datoms);
        if schemadatoms::touches_naming(datoms) {
            let mut schema = self.schema.clone();
            let mut idents = (*self.idents).clone();
            schemadatoms::derive(&mut schema, &mut idents, &self.interner, t, datoms);
            next.schema = schema;
            next.idents = Arc::new(idents);
            if schema_changed {
                next.schema_generation = self.schema_generation + 1;
            }
        }
        // Allocate each datom once here: the log entry and every covering
        // index entry derived from it below are handles on this one
        // allocation, not copies of the fact.
        let arrived: Vec<Arc<Datom>> = datoms.iter().cloned().map(Arc::new).collect();
        // `recorded` is a persistent vector: this clone shared its whole spine
        // with the parent, and appending copies only the O(log n) nodes on the
        // tail path — never the entire log, even while `db_before` keeps the
        // parent alive.
        for datom in &arrived {
            next.recorded.push_back_mut(Arc::clone(datom));
        }
        next.indexes = Arc::new(OnceLock::new());
        next.stats = Arc::new(OnceLock::new());
        // Derive indexes incrementally when the parent already built them, so
        // transaction pipelines don't refold the whole history per operation.
        //
        // Not when the schema changed: the parent's fold decided coverage
        // under the parent's schema, so extending it would leave an attribute
        // that just gained AVET covered for post-change datoms only — a
        // silently partial index that reads as authoritative. Dropping the
        // fold costs one rebuild per schema change and keeps coverage total.
        if let Some(parent) = self.indexes.get()
            && !schema_changed
        {
            let mut derived = parent.clone();
            apply_current(&mut derived, arrived.iter(), &self.schema);
            let _ = next.indexes.set(derived);
        }
        next
    }

    /// Computes basic statistics over this view's facts.
    #[must_use]
    pub fn stats(&self) -> DbStats {
        let planner = self.planner_stats();
        DbStats {
            datoms: planner.total_datoms,
            entities: planner.entity_count,
            attributes: planner.per_attr.len(),
        }
    }

    /// Planner statistics for this view, built lazily and cached.
    #[must_use]
    pub fn planner_stats(&self) -> &PlannerStats {
        self.stats.get_or_init(|| {
            let mut stats = PlannerStats::default();
            let mut values: BTreeMap<AttrId, BTreeSet<&Value>> = BTreeMap::new();
            let mut attr_entities: BTreeMap<AttrId, BTreeSet<EntityId>> = BTreeMap::new();
            let mut entities: BTreeSet<EntityId> = BTreeSet::new();
            for datom in self.datoms_at(IndexOrder::Eavt) {
                stats.total_datoms += 1;
                stats.per_attr.entry(datom.a).or_default().count += 1;
                values.entry(datom.a).or_default().insert(&datom.v);
                attr_entities.entry(datom.a).or_default().insert(datom.e);
                entities.insert(datom.e);
            }
            for (a, entry) in &mut stats.per_attr {
                entry.distinct_values = values.get(a).map_or(0, BTreeSet::len);
                entry.distinct_entities = attr_entities.get(a).map_or(0, BTreeSet::len);
                entry.protected = self.schema.is_protected(*a);
            }
            stats.entity_count = entities.len();
            stats
        })
    }

    fn indexes(&self) -> &[Index; 4] {
        self.indexes.get_or_init(|| {
            let mut indexes: [Index; 4] = Default::default();
            match self.view {
                DbView::History => {
                    for datom in &self.recorded {
                        if self.schema.get(datom.a).is_some_and(|a| a.no_history) {
                            continue;
                        }
                        insert_datom(&mut indexes, datom, &self.schema, true);
                    }
                }
                DbView::Current | DbView::AsOf(_) | DbView::Since(_) => {
                    // One fold decides liveness for all four orders. A
                    // retraction cancels the assertion sharing its
                    // `(e, a, v)`, and that is the EAVT key, so the live set
                    // can be folded in EAVT alone and the other three orders
                    // projected from it. Folding once means a fact that is
                    // later retracted churns one tree rather than four, and
                    // nothing that never survives the fold is ever encoded
                    // into AEVT/AVET/VAET.
                    let cutoff = match self.view {
                        DbView::AsOf(t) => Some(t),
                        _ => None,
                    };
                    let mut live = Index::new_sync();
                    for datom in &self.recorded {
                        if cutoff.is_some_and(|t| datom.tx.sequence() > t) {
                            continue;
                        }
                        let key = key_prefix(
                            IndexOrder::Eavt,
                            Some(datom.e),
                            Some(datom.a),
                            Some(&datom.v),
                        );
                        if datom.added {
                            live.insert_mut(key, Arc::clone(datom));
                        } else {
                            live.remove_mut(&key);
                        }
                    }
                    // `since` keeps only the live facts a later transaction
                    // added. Narrowing the live set before projecting costs
                    // one transient tree of keys and handles; filtering the
                    // four built indexes instead — the shape this replaces —
                    // held four whole indexes and four filtered copies of
                    // them alive at once.
                    if let DbView::Since(t) = self.view {
                        live = live
                            .iter()
                            .filter(|(_, datom)| datom.tx.sequence() > t)
                            .map(|(key, datom)| (key.clone(), Arc::clone(datom)))
                            .collect();
                    }
                    for datom in live.values() {
                        for order in [IndexOrder::Aevt, IndexOrder::Avet, IndexOrder::Vaet] {
                            if covered(&self.schema, order, datom) {
                                let key =
                                    key_prefix(order, Some(datom.e), Some(datom.a), Some(&datom.v));
                                indexes[slot(order)].insert_mut(key, Arc::clone(datom));
                            }
                        }
                    }
                    indexes[slot(IndexOrder::Eavt)] = live;
                }
            }
            indexes
        })
    }
}

/// Folds assertions/retractions into current-view indexes.
///
/// Current views key entries by components only (no transaction suffix):
/// at most one live datom exists per `(e, a, v)`, and retractions must
/// erase the assertion regardless of which transactions produced them.
fn apply_current<'a>(
    indexes: &mut [Index; 4],
    datoms: impl Iterator<Item = &'a Arc<Datom>>,
    schema: &Schema,
) {
    for datom in datoms {
        if datom.added {
            insert_datom(indexes, datom, schema, false);
        } else {
            for order in ORDERS {
                if covered(schema, order, datom) {
                    let key = key_prefix(order, Some(datom.e), Some(datom.a), Some(&datom.v));
                    indexes[slot(order)].remove_mut(&key);
                }
            }
        }
    }
}

fn insert_datom(indexes: &mut [Index; 4], datom: &Arc<Datom>, schema: &Schema, with_tx: bool) {
    for order in ORDERS {
        if covered(schema, order, datom) {
            let key = if with_tx {
                datom.key(order)
            } else {
                key_prefix(order, Some(datom.e), Some(datom.a), Some(&datom.v))
            };
            indexes[slot(order)].insert_mut(key, Arc::clone(datom));
        }
    }
}

/// Whether `datom` belongs in `order`'s covering index.
///
/// EAVT and AEVT hold every datom; AVET holds only indexed/unique
/// attributes and VAET only reference values, mirroring Datomic's covering
/// index composition. An indexing pass folding a transaction tail into
/// published segments filters it through the same rule the in-memory
/// indexes use, so the two cannot drift.
#[must_use]
pub fn covered(schema: &Schema, order: IndexOrder, datom: &Datom) -> bool {
    match order {
        IndexOrder::Eavt | IndexOrder::Aevt => true,
        IndexOrder::Avet => avet_covered(schema, datom.a),
        IndexOrder::Vaet => matches!(datom.v, Value::Ref(_)),
    }
}

/// Why a value-ordered index scan is unavailable.
#[derive(Debug, thiserror::Error, Eq, PartialEq)]
pub enum RangeError {
    /// The attribute is, or has been, protected.
    #[error("attribute {0} is protected; sealed values have no value order")]
    Protected(AttrId),
}

/// Whether the attribute participates in the AVET covering index.
#[must_use]
pub fn avet_covered(schema: &Schema, a: AttrId) -> bool {
    schema
        .get(a)
        .is_some_and(|attr| attr.indexed || attr.unique.is_some())
}

/// Counts over one database view.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DbStats {
    /// Facts in the view.
    pub datoms: usize,
    /// Entities having at least one fact in the view.
    pub entities: usize,
    /// Attributes used by facts in the view.
    pub attributes: usize,
}

/// Convenience constructor for schema attributes used during bootstrap/tests.
#[must_use]
pub const fn attribute(
    id: u64,
    value_type: corium_core::ValueType,
    cardinality: Cardinality,
    unique: Option<Unique>,
) -> corium_core::Attribute {
    corium_core::Attribute {
        id: EntityId::new(Partition::Db as u32, id),
        value_type,
        cardinality,
        unique,
        is_component: false,
        indexed: unique.is_some(),
        no_history: false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use corium_core::ValueType;

    fn schema() -> Schema {
        let mut schema = Schema::default();
        schema.insert(attribute(1, ValueType::Str, Cardinality::One, None));
        schema.insert(attribute(
            2,
            ValueType::Long,
            Cardinality::One,
            Some(Unique::Identity),
        ));
        schema.insert(attribute(3, ValueType::Ref, Cardinality::Many, None));
        schema
    }

    fn attr(id: u64) -> AttrId {
        EntityId::new(Partition::Db as u32, id)
    }

    fn entity(id: u64) -> EntityId {
        EntityId::new(Partition::User as u32, id)
    }

    fn tx_entity(t: u64) -> EntityId {
        EntityId::new(Partition::Tx as u32, t)
    }

    fn datom(e: u64, a: u64, v: Value, t: u64, added: bool) -> Datom {
        Datom {
            e: entity(e),
            a: attr(a),
            v,
            tx: tx_entity(t),
            added,
        }
    }

    fn sample() -> Db {
        Db::new(schema())
            .with_transaction(
                1,
                &[
                    datom(1, 1, Value::Str("alice".into()), 1, true),
                    datom(1, 2, Value::Long(7), 1, true),
                ],
            )
            .with_transaction(
                2,
                &[
                    datom(1, 1, Value::Str("alice".into()), 2, false),
                    datom(1, 1, Value::Str("alicia".into()), 2, true),
                    datom(2, 3, Value::Ref(entity(1)), 2, true),
                ],
            )
    }

    #[test]
    fn current_view_folds_retractions() {
        let db = sample();
        assert_eq!(
            db.values(entity(1), attr(1)),
            vec![Value::Str("alicia".into())]
        );
        assert_eq!(db.stats().datoms, 3);
    }

    #[test]
    fn recorded_since_returns_the_transaction_tail() {
        let db = sample();
        assert_eq!(db.recorded_since(2).count(), 0);
        let tail: Vec<_> = db.recorded_since(1).collect();
        assert_eq!(tail.len(), 3);
        assert!(tail.iter().all(|datom| datom.tx.sequence() == 2));
        assert_eq!(db.recorded_since(0).count(), db.recorded_len());

        // A value opened from a published snapshot carries live datoms rather
        // than a transaction-ordered history; the tail after it still scans.
        let snapshot = Db::from_current_snapshot(
            2,
            schema(),
            Idents::default(),
            KeywordInterner::default(),
            db.datoms(),
        )
        .with_transaction(3, &[datom(3, 1, Value::Str("carol".into()), 3, true)]);
        let tail: Vec<_> = snapshot.recorded_since(2).collect();
        assert_eq!(tail.len(), 1);
        assert!(tail.iter().all(|datom| datom.tx.sequence() == 3));
    }

    #[test]
    fn attribute_scan_uses_complete_aevt_coverage() {
        let db = sample();
        let datoms = db.datoms_for_attribute(attr(1)).collect::<Vec<_>>();
        assert_eq!(datoms.len(), 1);
        assert_eq!(datoms[0].v, Value::Str("alicia".into()));
        assert!(datoms.iter().all(|datom| datom.a == attr(1)));
    }

    #[test]
    fn as_of_reconstructs_past_basis() {
        let db = sample().as_of(1);
        assert_eq!(
            db.values(entity(1), attr(1)),
            vec![Value::Str("alice".into())]
        );
        assert_eq!(db.stats().datoms, 2);
    }

    #[test]
    fn since_excludes_older_live_facts() {
        let db = sample().since(1);
        // The long asserted at t=1 is invisible; the renamed string is visible.
        assert_eq!(db.values(entity(1), attr(2)), Vec::<Value>::new());
        assert_eq!(
            db.values(entity(1), attr(1)),
            vec![Value::Str("alicia".into())]
        );
    }

    #[test]
    fn history_exposes_assertions_and_retractions() {
        let db = sample().history();
        let names: Vec<_> = db
            .datoms_prefix(
                IndexOrder::Eavt,
                &key_prefix(IndexOrder::Eavt, Some(entity(1)), Some(attr(1)), None),
            )
            .map(|d| (d.v.clone(), d.added))
            .collect();
        assert_eq!(names.len(), 3);
        assert!(names.contains(&(Value::Str("alice".into()), false)));
    }

    #[test]
    fn since_filters_the_live_set_in_every_order() {
        // The four orders are projections of one filtered live set, so the
        // floor has to hold in all of them and not just the one folded first.
        let db = sample().since(1);
        assert_eq!(db.datoms_at(IndexOrder::Eavt).count(), 2);
        assert_eq!(db.datoms_at(IndexOrder::Aevt).count(), 2);
        // The sole AVET-covered attribute's live datom was asserted at t=1.
        assert_eq!(db.datoms_at(IndexOrder::Avet).count(), 0);
        assert_eq!(db.datoms_at(IndexOrder::Vaet).count(), 1);
        for order in ORDERS {
            assert!(db.datoms_at(order).all(|datom| datom.tx.sequence() > 1));
        }
    }

    #[test]
    fn a_datom_is_allocated_once_and_shared_by_the_log_and_every_index() {
        let db = sample();
        let indexes = db.indexes();
        for (_, eavt) in &indexes[slot(IndexOrder::Eavt)] {
            // AEVT covers every datom, and holds this one by handle rather
            // than as a second copy of the fact.
            let aevt = indexes[slot(IndexOrder::Aevt)]
                .get(&key_prefix(
                    IndexOrder::Aevt,
                    Some(eavt.e),
                    Some(eavt.a),
                    Some(&eavt.v),
                ))
                .expect("AEVT covers every datom");
            assert!(Arc::ptr_eq(eavt, aevt), "AEVT copied the datom");
            assert!(
                db.recorded.iter().any(|entry| Arc::ptr_eq(entry, eavt)),
                "the index copied the datom instead of sharing the log's"
            );
        }
    }

    #[test]
    fn views_that_name_the_current_value_share_its_fold() {
        let db = sample();
        let _ = db.datoms();
        for view in [
            db.as_of(db.basis_t()),
            db.as_of(db.basis_t() + 10),
            db.since(0),
        ] {
            assert!(
                Arc::ptr_eq(&db.indexes, &view.indexes),
                "{:?} refolded the whole history to reach the current value",
                view.view()
            );
            assert!(Arc::ptr_eq(&db.stats, &view.stats));
            assert_eq!(view.datoms(), db.datoms());
        }
        // A view that genuinely selects a different set folds its own.
        for view in [db.as_of(1), db.since(1), db.history()] {
            assert!(!Arc::ptr_eq(&db.indexes, &view.indexes));
        }
    }

    #[test]
    fn a_view_reports_the_time_view_it_was_asked_for() {
        // Sharing a fold must not restate an as-of view as the current value:
        // callers key behaviour off the declared view (writes are refused on
        // any time view) whatever facts it happens to hold.
        let db = sample();
        assert_eq!(db.view(), DbView::Current);
        assert_eq!(db.as_of(db.basis_t()).view(), DbView::AsOf(db.basis_t()));
        assert_eq!(db.since(0).view(), DbView::Since(0));
    }

    #[test]
    fn avet_only_covers_indexed_attributes() {
        let db = sample();
        assert_eq!(db.datoms_at(IndexOrder::Avet).count(), 1);
        assert_eq!(db.datoms_at(IndexOrder::Vaet).count(), 1);
    }

    #[test]
    fn index_range_scans_value_bounds() {
        let db = Db::new(schema()).with_transaction(
            1,
            &[
                datom(1, 2, Value::Long(1), 1, true),
                datom(2, 2, Value::Long(5), 1, true),
                datom(3, 2, Value::Long(9), 1, true),
            ],
        );
        let hits: Vec<_> = db
            .index_range(attr(2), Some(&Value::Long(2)), Some(&Value::Long(9)))
            .expect("an unprotected attribute has a value order")
            .map(|d| d.e)
            .collect();
        assert_eq!(hits, vec![entity(2)]);
    }

    /// The same two transactions as [`sample`], committed at known instants
    /// the way the transactor commits them.
    fn timed_sample() -> Db {
        Db::new(schema())
            .with_transaction_at(
                1,
                1_000,
                &[datom(1, 1, Value::Str("alice".into()), 1, true)],
            )
            .with_transaction_at(
                2,
                2_000,
                &[
                    datom(1, 1, Value::Str("alice".into()), 2, false),
                    datom(1, 1, Value::Str("alicia".into()), 2, true),
                ],
            )
    }

    #[test]
    fn commit_instants_are_recorded_in_both_directions() {
        let db = timed_sample();
        assert_eq!(db.tx_instant(1), Some(1_000));
        assert_eq!(db.tx_instant(2), Some(2_000));
        assert_eq!(db.tx_instant(3), None);
        // Exactly on a commit, before the first, between two, and after the last.
        assert_eq!(db.t_at_instant(1_000), 1);
        assert_eq!(db.t_at_instant(999), 0);
        assert_eq!(db.t_at_instant(1_999), 1);
        assert_eq!(db.t_at_instant(9_999), 2);
    }

    #[test]
    fn instant_named_views_match_their_basis_named_equivalents() {
        let db = timed_sample();
        assert_eq!(db.as_of_instant(1_500).datoms(), db.as_of(1).datoms());
        assert_eq!(db.since_instant(1_500).datoms(), db.since(1).datoms());
        // An instant older than the database is the empty value; `since` that
        // same instant is therefore everything.
        assert!(db.as_of_instant(0).datoms().is_empty());
        assert_eq!(db.since_instant(0).datoms(), db.since(0).datoms());
    }

    #[test]
    fn transaction_time_is_queryable_data() {
        let db = timed_sample();
        let instants: Vec<_> = db
            .datoms_for_attribute(bootstrap::TX_INSTANT)
            .map(|datom| (datom.e, datom.v.clone()))
            .collect();
        assert_eq!(
            instants,
            vec![
                (tx_entity(1), Value::Instant(1_000)),
                (tx_entity(2), Value::Instant(2_000)),
            ]
        );
        // Indexed, so an instant range is an AVET seek rather than a scan.
        assert!(avet_covered(db.schema(), bootstrap::TX_INSTANT));
    }

    #[test]
    fn derived_views_keep_the_whole_transaction_time_correspondence() {
        // Resolution must not depend on which view names the instant: a
        // `since` view hides older datoms, but the instants they were
        // committed at still name the same transactions.
        let db = timed_sample();
        for view in [db.as_of(1), db.since(2), db.history()] {
            assert_eq!(view.tx_instant(1), Some(1_000));
            assert_eq!(view.t_at_instant(2_500), 2);
        }
    }

    #[test]
    fn an_instant_supplied_with_the_datoms_wins_over_the_replayed_one() {
        // A log record written by a transactor that already materialized the
        // datom must not gain a second, contradictory one.
        let db = Db::new(schema()).with_transaction_at(
            1,
            5_000,
            &[
                datom(1, 1, Value::Str("alice".into()), 1, true),
                bootstrap::tx_instant_datom(1, 1_000),
            ],
        );
        assert_eq!(db.values(tx_entity(1), bootstrap::TX_INSTANT).len(), 1);
        assert_eq!(db.tx_instant(1), Some(1_000));
    }

    #[test]
    fn snapshot_ignores_tx_instant_values_on_non_transaction_entities() {
        let user = entity(1);
        let db = Db::from_current_snapshot(
            1,
            schema(),
            Idents::default(),
            KeywordInterner::default(),
            vec![
                bootstrap::tx_instant_datom(1, 1_000),
                Datom {
                    e: user,
                    a: bootstrap::TX_INSTANT,
                    v: Value::Instant(9_000),
                    tx: tx_entity(1),
                    added: true,
                },
            ],
        );
        assert_eq!(db.tx_instant(1), Some(1_000));
    }

    #[test]
    fn tx_range_groups_by_transaction() {
        let ranged = sample().tx_range(2, None);
        assert_eq!(ranged.len(), 1);
        assert_eq!(ranged[0].0, 2);
        assert_eq!(ranged[0].1.len(), 3);
    }

    #[test]
    fn incremental_indexes_match_rebuilt_indexes() {
        let base = Db::new(schema());
        let tx1 = [datom(1, 1, Value::Str("a".into()), 1, true)];
        let tx2 = [
            datom(1, 1, Value::Str("a".into()), 2, false),
            datom(1, 1, Value::Str("b".into()), 2, true),
        ];
        // Force the parent cache so with_transaction derives incrementally.
        let warm = base.with_transaction(1, &tx1);
        let _ = warm.datoms();
        let incremental = warm.with_transaction(2, &tx2);
        let cold = base.with_transaction(1, &tx1).with_transaction(2, &tx2);
        assert_eq!(incremental.datoms(), cold.datoms());
    }

    #[test]
    fn with_transaction_leaves_parent_value_unchanged() {
        // `recorded` is a persistent vector appended via copy-on-write; a
        // derived value must never mutate the log or indexes still observed
        // through the parent (e.g. `db_before` in a `TxReport`).
        let parent = Db::new(schema())
            .with_transaction(1, &[datom(1, 1, Value::Str("alice".into()), 1, true)]);
        // Materialize the parent's indexes so the child derives incrementally.
        let parent_datoms = parent.datoms();
        let parent_recorded = parent.recorded_len();

        let child = parent.with_transaction(
            2,
            &[
                datom(1, 1, Value::Str("alice".into()), 2, false),
                datom(1, 1, Value::Str("alicia".into()), 2, true),
            ],
        );

        // Parent is frozen at its own basis, log length, and live facts.
        assert_eq!(parent.basis_t(), 1);
        assert_eq!(parent.recorded_len(), parent_recorded);
        assert_eq!(parent.datoms(), parent_datoms);
        assert_eq!(
            parent.values(entity(1), attr(1)),
            vec![Value::Str("alice".into())]
        );
        // Child reflects the new transaction.
        assert_eq!(child.basis_t(), 2);
        assert_eq!(
            child.values(entity(1), attr(1)),
            vec![Value::Str("alicia".into())]
        );
    }

    #[test]
    fn planner_stats_count_attributes() {
        let stats_owner = sample();
        let stats = stats_owner.planner_stats();
        assert_eq!(stats.total_datoms, 3);
        assert_eq!(stats.per_attr[&attr(1)].count, 1);
        assert!(stats.estimate(false, Some(attr(1)), false) <= stats.total_datoms);
    }
}
