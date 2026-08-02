//! Model tests for the schema-update impact scans.
//!
//! Each scan reads AEVT for one attribute. The model recomputes the same
//! answer by brute force over the view's EAVT facts, so an indexing bug or a
//! missed retraction shows up as a disagreement rather than as a plausible
//! number in a plan.

use std::collections::{BTreeMap, BTreeSet};

use corium_core::{
    Attribute, Cardinality, Datom, EntityId, IndexOrder, Partition, Schema, Unique, Value,
    ValueType,
};
use corium_db::impact::{
    DEFAULT_SAMPLE_LIMIT, attribute_impact, cardinality_conflicts, duplicate_values, live_refs,
};
use corium_db::Db;
use proptest::prelude::*;

const ATTRS: [u64; 3] = [100, 101, 102];

fn attr(sequence: u64) -> EntityId {
    EntityId::new(Partition::Db as u32, sequence)
}

fn schema() -> Schema {
    let mut schema = Schema::default();
    for sequence in ATTRS {
        schema.insert(Attribute {
            id: attr(sequence),
            // 102 holds references so component fan-out can be counted.
            value_type: if sequence == 102 {
                ValueType::Ref
            } else {
                ValueType::Long
            },
            cardinality: Cardinality::Many,
            unique: None,
            is_component: false,
            // One indexed attribute, so AVET coverage differs across the
            // fixture and the scans cannot silently depend on it.
            indexed: sequence == 101,
            no_history: false,
        });
    }
    schema
}

/// A generated transaction operation.
#[derive(Clone, Debug)]
struct Op {
    e: u64,
    a: u64,
    v: i64,
    added: bool,
}

fn operations() -> impl Strategy<Value = Vec<Op>> {
    prop::collection::vec(
        (0_u64..6, prop::sample::select(ATTRS.to_vec()), 0_i64..4, any::<bool>()).prop_map(
            |(e, a, v, added)| Op { e, a, v, added },
        ),
        0..40,
    )
}

fn build(ops: &[Op]) -> Db {
    let mut db = Db::new(schema());
    for (index, op) in ops.iter().enumerate() {
        let t = u64::try_from(index).expect("small fixture") + 1;
        let e = EntityId::new(Partition::User as u32, op.e);
        let value = if op.a == 102 {
            Value::Ref(EntityId::new(Partition::User as u32, op.v.unsigned_abs()))
        } else {
            Value::Long(op.v)
        };
        db = db.with_transaction_at(
            t,
            i64::try_from(t).expect("small fixture") * 1_000,
            &[Datom {
                e,
                a: attr(op.a),
                v: value,
                tx: EntityId::new(Partition::Tx as u32, t),
                added: op.added,
            }],
        );
    }
    db
}

/// The live facts of `a`, recomputed from the view's EAVT order.
fn live(db: &Db, a: EntityId) -> Vec<(EntityId, Value)> {
    db.datoms_at(IndexOrder::Eavt)
        .filter(|datom| datom.a == a)
        .map(|datom| (datom.e, datom.v.clone()))
        .collect()
}

proptest! {
    #[test]
    fn attribute_counts_match_a_brute_force_scan(ops in operations()) {
        let db = build(&ops);
        for sequence in ATTRS {
            let a = attr(sequence);
            let facts = live(&db, a);
            let impact = attribute_impact(&db, a);
            prop_assert_eq!(impact.datoms, facts.len() as u64);
            prop_assert_eq!(
                impact.entities,
                facts.iter().map(|(e, _)| *e).collect::<BTreeSet<_>>().len() as u64
            );
            prop_assert_eq!(
                impact.distinct_values,
                facts.iter().map(|(_, v)| v.clone()).collect::<BTreeSet<_>>().len() as u64
            );
        }
    }

    #[test]
    fn cardinality_conflicts_match_a_brute_force_scan(ops in operations()) {
        let db = build(&ops);
        for sequence in ATTRS {
            let a = attr(sequence);
            let mut per_entity: BTreeMap<EntityId, u64> = BTreeMap::new();
            for (e, _) in live(&db, a) {
                *per_entity.entry(e).or_default() += 1;
            }
            let expected: Vec<EntityId> = per_entity
                .iter()
                .filter(|(_, held)| **held > 1)
                .map(|(e, _)| *e)
                .collect();
            let conflicts = cardinality_conflicts(&db, a, DEFAULT_SAMPLE_LIMIT);
            prop_assert_eq!(conflicts.entities, expected.len() as u64);
            prop_assert_eq!(
                conflicts.datoms,
                per_entity.values().filter(|held| **held > 1).sum::<u64>()
            );
            // The sample is bounded, and every sampled entity really conflicts.
            prop_assert!(conflicts.sample.len() <= DEFAULT_SAMPLE_LIMIT);
            for entity in &conflicts.sample {
                prop_assert!(expected.contains(entity));
            }
        }
    }

    #[test]
    fn duplicate_values_match_a_brute_force_scan(ops in operations()) {
        let db = build(&ops);
        for sequence in ATTRS {
            let a = attr(sequence);
            let mut per_value: BTreeMap<Value, BTreeSet<EntityId>> = BTreeMap::new();
            for (e, v) in live(&db, a) {
                per_value.entry(v).or_default().insert(e);
            }
            let duplicated: Vec<&BTreeSet<EntityId>> = per_value
                .values()
                .filter(|holders| holders.len() > 1)
                .collect();
            let duplicates = duplicate_values(&db, a, DEFAULT_SAMPLE_LIMIT);
            prop_assert_eq!(duplicates.groups, duplicated.len() as u64);
            prop_assert_eq!(
                duplicates.entities,
                duplicated.iter().map(|holders| holders.len() as u64).sum::<u64>()
            );
            prop_assert!(duplicates.sample.len() <= DEFAULT_SAMPLE_LIMIT);
            for entity in &duplicates.sample {
                prop_assert!(duplicated.iter().any(|holders| holders.contains(entity)));
            }
            // Uniqueness is installable exactly when nothing is duplicated.
            prop_assert_eq!(
                duplicates.is_empty(),
                per_value.values().all(|holders| holders.len() == 1)
            );
        }
    }

    #[test]
    fn live_refs_match_a_brute_force_scan(ops in operations()) {
        let db = build(&ops);
        for sequence in ATTRS {
            let a = attr(sequence);
            let expected = live(&db, a)
                .iter()
                .filter(|(_, value)| matches!(value, Value::Ref(_)))
                .count();
            prop_assert_eq!(live_refs(&db, a), expected as u64);
        }
    }

    #[test]
    fn scans_agree_with_the_view_they_are_given(ops in operations()) {
        let db = build(&ops);
        if db.basis_t() < 2 {
            return Ok(());
        }
        // An `as-of` view answers about its own basis, not the value's.
        let past = db.as_of(db.basis_t() - 1);
        for sequence in ATTRS {
            let a = attr(sequence);
            prop_assert_eq!(attribute_impact(&past, a).datoms, live(&past, a).len() as u64);
        }
    }

    #[test]
    fn a_unique_attribute_is_scanned_through_the_same_path(ops in operations()) {
        // Attribute 101 is AVET-covered and 100 is not; both are scanned via
        // AEVT, so their answers must not depend on that coverage.
        let db = build(&ops);
        let covered = attribute_impact(&db, attr(101));
        prop_assert_eq!(covered.datoms, live(&db, attr(101)).len() as u64);
        let uncovered = attribute_impact(&db, attr(100));
        prop_assert_eq!(uncovered.datoms, live(&db, attr(100)).len() as u64);
        let _ = Unique::Identity;
    }
}
