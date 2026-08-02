//! Fixed-basis impact scans for schema-update planning
//! (`docs/design/schema-migrations.md`).
//!
//! Every function here reads one immutable [`Db`] value, so a whole plan
//! observes one basis and one schema: the counts in a plan cannot disagree with
//! each other. Scans go through AEVT, which covers every installed attribute,
//! rather than AVET, which covers only attributes that already have the
//! coverage a plan may be about to request.
//!
//! Counts are exact and unbounded; samples are bounded. An estimate is never
//! returned where an exact count is meant — a caller that cannot get an exact
//! answer from this database value gets [`HistoryImpact::available`] `false`
//! instead of a guess.

use std::collections::{BTreeMap, BTreeSet};

use corium_core::{AttrId, EntityId, Value};

use crate::{Db, DbView};

/// How many representative entity ids a scan collects for a violation.
pub const DEFAULT_SAMPLE_LIMIT: usize = 5;

/// Size of the data an attribute currently carries.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AttributeImpact {
    /// Current datoms carrying the attribute.
    pub datoms: u64,
    /// Distinct entities carrying it.
    pub entities: u64,
    /// Distinct current values.
    pub distinct_values: u64,
}

/// Entities that would violate `cardinality one`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CardinalityConflicts {
    /// Entities holding more than one current value.
    pub entities: u64,
    /// Current datoms held by those entities.
    pub datoms: u64,
    /// Bounded sample of conflicting entities.
    pub sample: Vec<EntityId>,
}

impl CardinalityConflicts {
    /// Whether any entity conflicts.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.entities == 0
    }
}

/// Values shared by more than one entity, which uniqueness would reject.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DuplicateValues {
    /// Distinct values held by more than one entity.
    pub groups: u64,
    /// Entities involved in those groups.
    pub entities: u64,
    /// Bounded sample of entities holding a duplicated value.
    pub sample: Vec<EntityId>,
}

impl DuplicateValues {
    /// Whether the attribute's current values are already unique.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.groups == 0
    }
}

/// What this database value can say about an attribute's history.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HistoryImpact {
    /// Assertions and retractions the value has recorded for the attribute.
    pub datoms: u64,
    /// Whether that count is the attribute's complete history.
    ///
    /// False when the attribute omits history (`:db/noHistory`) or when the
    /// value was opened from a current-state snapshot, whose prefix is the
    /// live facts at its basis rather than a transaction-ordered log.
    pub available: bool,
}

/// Current datoms, entities, and distinct values for one attribute.
#[must_use]
pub fn attribute_impact(db: &Db, a: AttrId) -> AttributeImpact {
    let mut entities = BTreeSet::new();
    let mut values = BTreeSet::new();
    let mut datoms: u64 = 0;
    for datom in db.datoms_for_attribute(a) {
        datoms = datoms.saturating_add(1);
        entities.insert(datom.e);
        values.insert(&datom.v);
    }
    AttributeImpact {
        datoms,
        entities: count(entities.len()),
        distinct_values: count(values.len()),
    }
}

/// Entities holding more than one current value for `a`.
///
/// This is the exact precondition for collapsing `cardinality many` to `one`.
/// The planner never picks a winner, so the sample exists to be resolved by
/// hand or by an explicit policy, not to be applied.
#[must_use]
pub fn cardinality_conflicts(db: &Db, a: AttrId, sample_limit: usize) -> CardinalityConflicts {
    let mut per_entity: BTreeMap<EntityId, u64> = BTreeMap::new();
    for datom in db.datoms_for_attribute(a) {
        *per_entity.entry(datom.e).or_default() += 1;
    }
    let mut conflicts = CardinalityConflicts::default();
    for (entity, held) in per_entity {
        if held > 1 {
            conflicts.entities = conflicts.entities.saturating_add(1);
            conflicts.datoms = conflicts.datoms.saturating_add(held);
            if conflicts.sample.len() < sample_limit {
                conflicts.sample.push(entity);
            }
        }
    }
    conflicts
}

/// Values of `a` held by more than one entity.
///
/// Both `:db.unique/identity` and `:db.unique/value` require one entity per
/// value, so this is the precondition for either mode.
#[must_use]
pub fn duplicate_values(db: &Db, a: AttrId, sample_limit: usize) -> DuplicateValues {
    let mut per_value: BTreeMap<&Value, BTreeSet<EntityId>> = BTreeMap::new();
    for datom in db.datoms_for_attribute(a) {
        per_value.entry(&datom.v).or_default().insert(datom.e);
    }
    let mut duplicates = DuplicateValues::default();
    for holders in per_value.into_values() {
        if holders.len() > 1 {
            duplicates.groups = duplicates.groups.saturating_add(1);
            duplicates.entities = duplicates.entities.saturating_add(count(holders.len()));
            for entity in holders {
                if duplicates.sample.len() >= sample_limit {
                    break;
                }
                duplicates.sample.push(entity);
            }
        }
    }
    duplicates
}

/// Current reference datoms carried by `a`.
///
/// The population that would acquire — or lose — component cascade semantics.
#[must_use]
pub fn live_refs(db: &Db, a: AttrId) -> u64 {
    count(
        db.datoms_for_attribute(a)
            .filter(|datom| matches!(datom.v, Value::Ref(_)))
            .count(),
    )
}

/// Recorded history for `a`, and whether it is complete.
///
/// The scan reads the recorded log directly rather than materializing a
/// history view: a plan wants one attribute's count, not a fold of every fact
/// the value has ever seen.
#[must_use]
pub fn history_impact(db: &Db, a: AttrId) -> HistoryImpact {
    let omits_history = db.schema().get(a).is_some_and(|attr| attr.no_history);
    let cutoff = match db.view() {
        DbView::AsOf(t) => Some(t),
        _ => None,
    };
    let datoms = db
        .recorded_datoms()
        .filter(|datom| datom.a == a && cutoff.is_none_or(|t| datom.tx.sequence() <= t))
        .count();
    HistoryImpact {
        datoms: count(datoms),
        // A value that folded a complete log knows the whole history. One
        // opened from a published current-state snapshot carries only the live
        // facts at its basis, so its retractions are missing and the count is
        // a floor rather than an answer.
        available: !omits_history && db.has_complete_history(),
    }
}

fn count(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use corium_core::{Cardinality, Datom, KeywordInterner, Partition, Schema, ValueType};

    use crate::{Idents, attribute};

    fn attr(id: u64) -> AttrId {
        EntityId::new(Partition::Db as u32, id)
    }

    fn entity(id: u64) -> EntityId {
        EntityId::new(Partition::User as u32, id)
    }

    fn datom(e: u64, a: u64, v: Value, t: u64, added: bool) -> Datom {
        Datom {
            e: entity(e),
            a: attr(a),
            v,
            tx: EntityId::new(Partition::Tx as u32, t),
            added,
        }
    }

    fn schema() -> Schema {
        let mut schema = Schema::default();
        // 100: plain string; 101: cardinality-many string; 102: ref.
        schema.insert(attribute(100, ValueType::Str, Cardinality::One, None));
        schema.insert(attribute(101, ValueType::Str, Cardinality::Many, None));
        schema.insert(attribute(102, ValueType::Ref, Cardinality::Many, None));
        let mut no_history = attribute(103, ValueType::Str, Cardinality::One, None);
        no_history.no_history = true;
        schema.insert(no_history);
        schema
    }

    fn sample() -> Db {
        Db::new(schema())
            .with_transaction_at(
                1,
                1_000,
                &[
                    datom(1, 100, Value::Str("alice".into()), 1, true),
                    datom(2, 100, Value::Str("bob".into()), 1, true),
                    datom(3, 100, Value::Str("alice".into()), 1, true),
                    datom(1, 101, Value::Str("x".into()), 1, true),
                    datom(1, 101, Value::Str("y".into()), 1, true),
                    datom(2, 101, Value::Str("x".into()), 1, true),
                    datom(1, 102, Value::Ref(entity(2)), 1, true),
                ],
            )
            .with_transaction_at(
                2,
                2_000,
                &[
                    datom(3, 100, Value::Str("alice".into()), 2, false),
                    datom(3, 100, Value::Str("carol".into()), 2, true),
                ],
            )
    }

    #[test]
    fn attribute_impact_counts_the_current_view_only() {
        let impact = attribute_impact(&sample(), attr(100));
        assert_eq!(
            impact,
            AttributeImpact {
                datoms: 3,
                entities: 3,
                // "alice" was retracted from entity 3, so only two values live.
                distinct_values: 3,
            }
        );
    }

    #[test]
    fn cardinality_conflicts_find_entities_with_several_values() {
        let conflicts = cardinality_conflicts(&sample(), attr(101), DEFAULT_SAMPLE_LIMIT);
        assert_eq!(conflicts.entities, 1);
        assert_eq!(conflicts.datoms, 2);
        assert_eq!(conflicts.sample, vec![entity(1)]);
        assert!(cardinality_conflicts(&sample(), attr(100), DEFAULT_SAMPLE_LIMIT).is_empty());
    }

    #[test]
    fn duplicate_values_find_values_shared_by_entities() {
        let duplicates = duplicate_values(&sample(), attr(101), DEFAULT_SAMPLE_LIMIT);
        assert_eq!(duplicates.groups, 1);
        assert_eq!(duplicates.entities, 2);
        assert_eq!(duplicates.sample, vec![entity(1), entity(2)]);
        // A retraction removes its value from the duplicate group.
        assert!(duplicate_values(&sample(), attr(100), DEFAULT_SAMPLE_LIMIT).is_empty());
    }

    #[test]
    fn samples_are_bounded_but_counts_are_not() {
        let mut datoms = Vec::new();
        for id in 0..50 {
            datoms.push(datom(id, 101, Value::Str("shared".into()), 1, true));
            datoms.push(datom(id, 101, Value::Str("other".into()), 1, true));
        }
        let db = Db::new(schema()).with_transaction_at(1, 1_000, &datoms);
        let duplicates = duplicate_values(&db, attr(101), DEFAULT_SAMPLE_LIMIT);
        assert_eq!(duplicates.groups, 2);
        assert_eq!(duplicates.entities, 100);
        assert_eq!(duplicates.sample.len(), DEFAULT_SAMPLE_LIMIT);
        let conflicts = cardinality_conflicts(&db, attr(101), DEFAULT_SAMPLE_LIMIT);
        assert_eq!(conflicts.entities, 50);
        assert_eq!(conflicts.sample.len(), DEFAULT_SAMPLE_LIMIT);
    }

    #[test]
    fn live_refs_count_reference_values() {
        assert_eq!(live_refs(&sample(), attr(102)), 1);
        assert_eq!(live_refs(&sample(), attr(100)), 0);
    }

    #[test]
    fn history_is_exact_for_a_replayed_log_and_unavailable_for_a_snapshot() {
        let db = sample();
        let history = history_impact(&db, attr(100));
        // Three assertions, one retraction, one replacement.
        assert_eq!(history.datoms, 5);
        assert!(history.available);
        // `:db/noHistory` attributes cannot answer the question at all.
        assert!(!history_impact(&db, attr(103)).available);

        let snapshot = Db::from_current_snapshot(
            db.basis_t(),
            schema(),
            Idents::default(),
            KeywordInterner::default(),
            db.datoms(),
        );
        assert!(!history_impact(&snapshot, attr(100)).available);
        // The tail replayed onto a snapshot does not restore the missing
        // prefix, so the value still cannot answer exactly.
        let replayed = snapshot.with_transaction_at(
            3,
            3_000,
            &[datom(4, 100, Value::Str("dave".into()), 3, true)],
        );
        assert!(!history_impact(&replayed, attr(100)).available);
    }

    #[test]
    fn history_counts_respect_an_as_of_basis() {
        let db = sample();
        assert_eq!(history_impact(&db.as_of(1), attr(100)).datoms, 3);
        assert_eq!(history_impact(&db.as_of(2), attr(100)).datoms, 5);
    }

    #[test]
    fn scans_read_one_basis_even_as_the_database_advances() {
        let db = sample();
        let before = attribute_impact(&db, attr(100));
        let _later = db.with_transaction_at(
            3,
            3_000,
            &[datom(4, 100, Value::Str("dave".into()), 3, true)],
        );
        assert_eq!(attribute_impact(&db, attr(100)), before);
    }

    #[test]
    fn as_of_views_scan_their_own_basis() {
        let db = sample().as_of(1);
        let impact = attribute_impact(&db, attr(100));
        assert_eq!(impact.datoms, 3);
        let duplicates = duplicate_values(&db, attr(100), DEFAULT_SAMPLE_LIMIT);
        // At basis 1, entities 1 and 3 both held "alice".
        assert_eq!(duplicates.groups, 1);
        assert_eq!(duplicates.entities, 2);
    }
}
