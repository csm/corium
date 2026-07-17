//! Immutable database values: time views, covering-index access, naming,
//! per-attribute statistics, and bootstrap metadata.
//!
//! A [`Db`] is a value: cheap to clone, never mutated in place. Time views
//! ([`Db::as_of`], [`Db::since`], [`Db::history`]) wrap the same recorded
//! datoms with a different fold policy — no copying of facts. The four
//! covering indexes for a view are materialized lazily on first read and
//! shared by every clone of that value.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, OnceLock};

use corium_core::{
    AttrId, Cardinality, Datom, EntityId, IndexOrder, Keyword, KeywordInterner, Partition, Schema,
    Unique, Value, encoding::Encodable,
};

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
            (false, Some(stats)) if v_bound => {
                (stats.count / stats.distinct_values.max(1)).max(1)
            }
            (false, Some(stats)) => stats.count.max(1),
            // Unknown attribute constant: nothing will match.
            (false, None) if a.is_some() => 1,
            (false, None) => self.total_datoms.max(1),
        }
    }
}

type Index = BTreeMap<Vec<u8>, Datom>;

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

/// An immutable value of a database at one basis transaction and time view.
#[derive(Clone, Debug, Default)]
pub struct Db {
    basis_t: u64,
    schema: Schema,
    recorded: Arc<Vec<Datom>>,
    idents: Arc<Idents>,
    interner: Arc<KeywordInterner>,
    view: DbView,
    indexes: Arc<OnceLock<[Index; 4]>>,
    stats: Arc<OnceLock<PlannerStats>>,
}

impl Db {
    /// Creates an empty database with the supplied schema.
    #[must_use]
    pub fn new(schema: Schema) -> Self {
        Self {
            schema,
            ..Self::default()
        }
    }

    /// Attaches ident and keyword naming registries, returning the named value.
    #[must_use]
    pub fn with_naming(mut self, idents: Idents, interner: KeywordInterner) -> Self {
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

    /// Every recorded assertion and retraction, in transaction order.
    #[must_use]
    pub fn recorded_datoms(&self) -> &[Datom] {
        &self.recorded
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

    fn with_view(&self, view: DbView) -> Self {
        if view == self.view {
            return self.clone();
        }
        Self {
            view,
            indexes: Arc::new(OnceLock::new()),
            stats: Arc::new(OnceLock::new()),
            ..self.clone()
        }
    }

    /// Groups recorded datoms by transaction over the half-open range `[start, end)`.
    #[must_use]
    pub fn tx_range(&self, start: u64, end: Option<u64>) -> Vec<(u64, Vec<Datom>)> {
        let mut by_t: BTreeMap<u64, Vec<Datom>> = BTreeMap::new();
        for datom in self.recorded.iter() {
            let t = datom.tx.sequence();
            if t >= start && end.is_none_or(|end| t < end) {
                by_t.entry(t).or_default().push(datom.clone());
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
        self.indexes()[slot(order)].values()
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
            .map(|(_, datom)| datom)
    }

    /// Iterates datoms in `order` starting from the first key at or after `start`.
    pub fn seek_datoms<'a>(
        &'a self,
        order: IndexOrder,
        start: &[u8],
    ) -> impl Iterator<Item = &'a Datom> {
        self.indexes()[slot(order)]
            .range(start.to_vec()..)
            .map(|(_, datom)| datom)
    }

    /// Iterates the AVET index for `a` over the value range `[start, end)`.
    ///
    /// Only indexed/unique attributes appear in AVET.
    pub fn index_range<'a>(
        &'a self,
        a: AttrId,
        start: Option<&Value>,
        end: Option<&'a Value>,
    ) -> impl Iterator<Item = &'a Datom> {
        let a_prefix = key_prefix(IndexOrder::Avet, None, Some(a), None);
        let start_key = key_prefix(IndexOrder::Avet, None, Some(a), start);
        self.indexes()[slot(IndexOrder::Avet)]
            .range(start_key..)
            .take_while(move |(key, _)| key.starts_with(&a_prefix))
            .map(|(_, datom)| datom)
            .take_while(move |datom| end.is_none_or(|end| datom.v < *end))
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
    #[must_use]
    pub fn with_transaction(&self, t: u64, datoms: &[Datom]) -> Self {
        debug_assert!(
            self.view == DbView::Current,
            "with_transaction applies only to the current view"
        );
        let mut next = self.clone();
        next.basis_t = t;
        Arc::make_mut(&mut next.recorded).extend_from_slice(datoms);
        next.indexes = Arc::new(OnceLock::new());
        next.stats = Arc::new(OnceLock::new());
        // Derive indexes incrementally when the parent already built them, so
        // transaction pipelines don't refold the whole history per operation.
        if let Some(parent) = self.indexes.get() {
            let mut derived = parent.clone();
            apply_current(&mut derived, datoms.iter(), &self.schema);
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
                    for datom in self.recorded.iter() {
                        if self.schema.get(datom.a).is_some_and(|a| a.no_history) {
                            continue;
                        }
                        insert_datom(&mut indexes, datom, &self.schema, true);
                    }
                }
                DbView::Current | DbView::AsOf(_) | DbView::Since(_) => {
                    let cutoff = match self.view {
                        DbView::AsOf(t) => Some(t),
                        _ => None,
                    };
                    let filtered = self
                        .recorded
                        .iter()
                        .filter(|d| cutoff.is_none_or(|t| d.tx.sequence() <= t));
                    apply_current(&mut indexes, filtered, &self.schema);
                    if let DbView::Since(t) = self.view {
                        for index in &mut indexes {
                            index.retain(|_, datom| datom.tx.sequence() > t);
                        }
                    }
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
    datoms: impl Iterator<Item = &'a Datom>,
    schema: &Schema,
) {
    for datom in datoms {
        if datom.added {
            insert_datom(indexes, datom, schema, false);
        } else {
            for order in ORDERS {
                if covered(schema, order, datom) {
                    let key = key_prefix(order, Some(datom.e), Some(datom.a), Some(&datom.v));
                    indexes[slot(order)].remove(&key);
                }
            }
        }
    }
}

fn insert_datom(indexes: &mut [Index; 4], datom: &Datom, schema: &Schema, with_tx: bool) {
    for order in ORDERS {
        if covered(schema, order, datom) {
            let key = if with_tx {
                datom.key(order)
            } else {
                key_prefix(order, Some(datom.e), Some(datom.a), Some(&datom.v))
            };
            indexes[slot(order)].insert(key, datom.clone());
        }
    }
}

fn covered(schema: &Schema, order: IndexOrder, datom: &Datom) -> bool {
    match order {
        IndexOrder::Eavt | IndexOrder::Aevt => true,
        IndexOrder::Avet => avet_covered(schema, datom.a),
        IndexOrder::Vaet => matches!(datom.v, Value::Ref(_)),
    }
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
        assert_eq!(db.values(entity(1), attr(1)), vec![Value::Str("alicia".into())]);
        assert_eq!(db.stats().datoms, 3);
    }

    #[test]
    fn as_of_reconstructs_past_basis() {
        let db = sample().as_of(1);
        assert_eq!(db.values(entity(1), attr(1)), vec![Value::Str("alice".into())]);
        assert_eq!(db.stats().datoms, 2);
    }

    #[test]
    fn since_excludes_older_live_facts() {
        let db = sample().since(1);
        // The long asserted at t=1 is invisible; the renamed string is visible.
        assert_eq!(db.values(entity(1), attr(2)), Vec::<Value>::new());
        assert_eq!(db.values(entity(1), attr(1)), vec![Value::Str("alicia".into())]);
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
            .map(|d| d.e)
            .collect();
        assert_eq!(hits, vec![entity(2)]);
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
    fn planner_stats_count_attributes() {
        let stats_owner = sample();
        let stats = stats_owner.planner_stats();
        assert_eq!(stats.total_datoms, 3);
        assert_eq!(stats.per_attr[&attr(1)].count, 1);
        assert!(stats.estimate(false, Some(attr(1)), false) <= stats.total_datoms);
    }
}
