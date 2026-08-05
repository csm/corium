//! Property tests over installed/desired schema pairs.
//!
//! Two invariants matter most: every difference between the two schemas is
//! classified exactly once, and no change this command cannot execute is ever
//! presented as executable.

use std::collections::{BTreeMap, BTreeSet};

use corium_core::migration::{ChangeKind, ExecutionClass, SchemaProperty};
use corium_core::{
    Attribute, Cardinality, EntityId, Keyword, KeywordInterner, Partition, Schema, Unique,
    ValueType,
};
use corium_db::Db;
use corium_db::Idents;
use corium_forms::desired::{DesiredAttribute, DesiredSchema};
use corium_forms::planner::{PlanOptions, plan};
use corium_forms::schemaform::FIRST_ATTR_ID;
use proptest::prelude::*;

/// The property set of one attribute, in either schema.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Shape {
    value_type: ValueType,
    cardinality: Cardinality,
    unique: Option<Unique>,
    indexed: bool,
    is_component: bool,
    no_history: bool,
}

fn shape() -> impl Strategy<Value = Shape> {
    (
        prop::sample::select(vec![
            ValueType::Str,
            ValueType::Long,
            ValueType::Ref,
            ValueType::Keyword,
        ]),
        prop::sample::select(vec![Cardinality::One, Cardinality::Many]),
        prop::sample::select(vec![None, Some(Unique::Identity), Some(Unique::Value)]),
        any::<bool>(),
        any::<bool>(),
        any::<bool>(),
    )
        .prop_map(
            |(value_type, cardinality, unique, index, is_component, no_history)| Shape {
                value_type,
                cardinality,
                unique,
                // Uniqueness implies AVET coverage in both schemas, so the
                // pair never differs on a property neither side can express.
                indexed: index || unique.is_some(),
                is_component,
                no_history,
            },
        )
}

/// Names shared between the two schemas, plus names unique to each.
fn schema_pair() -> impl Strategy<Value = (BTreeMap<String, Shape>, BTreeMap<String, Shape>)> {
    prop::collection::vec((0_u8..8, shape(), shape(), 0_u8..3), 1..8).prop_map(|entries| {
        let mut installed = BTreeMap::new();
        let mut desired = BTreeMap::new();
        for (index, current, wanted, presence) in entries {
            let name = format!("a{index}");
            match presence {
                // In both schemas.
                0 => {
                    installed.insert(name.clone(), current);
                    desired.insert(name, wanted);
                }
                // Installed only: unmanaged, or a retirement under --prune.
                1 => {
                    installed.insert(name, current);
                }
                // Desired only: an addition.
                _ => {
                    desired.insert(name, wanted);
                }
            }
        }
        (installed, desired)
    })
}

fn database(installed: &BTreeMap<String, Shape>) -> Db {
    let mut schema = Schema::default();
    let mut idents = Idents::default();
    for (offset, (name, shape)) in installed.iter().enumerate() {
        let id = EntityId::new(
            Partition::Db as u32,
            FIRST_ATTR_ID + u64::try_from(offset).expect("small fixture"),
        );
        schema.insert(Attribute {
            id,
            value_type: shape.value_type,
            cardinality: shape.cardinality,
            unique: shape.unique,
            is_component: shape.is_component,
            indexed: shape.indexed,
            no_history: shape.no_history,
        });
        idents.insert(Keyword::new(Some("t"), name), id);
    }
    Db::new(schema).with_naming(idents, KeywordInterner::default())
}

fn desired_schema(desired: &BTreeMap<String, Shape>) -> DesiredSchema {
    DesiredSchema::new(
        desired
            .iter()
            .map(|(name, shape)| DesiredAttribute {
                ident: Keyword::new(Some("t"), name),
                value_type: shape.value_type,
                cardinality: shape.cardinality,
                unique: shape.unique,
                indexed: shape.indexed,
                is_component: shape.is_component,
                no_history: shape.no_history,
                doc: None,
                protection: None,
            })
            .collect(),
    )
    .expect("generated idents are distinct")
}

/// The properties in which two shapes differ.
fn differences(installed: &Shape, desired: &Shape) -> BTreeSet<SchemaProperty> {
    let mut properties = BTreeSet::new();
    if installed.value_type != desired.value_type {
        properties.insert(SchemaProperty::ValueType);
    }
    if installed.cardinality != desired.cardinality {
        properties.insert(SchemaProperty::Cardinality);
    }
    if installed.unique != desired.unique {
        properties.insert(SchemaProperty::Unique);
    }
    if installed.indexed != desired.indexed {
        properties.insert(SchemaProperty::Index);
    }
    if installed.is_component != desired.is_component {
        properties.insert(SchemaProperty::Component);
    }
    if installed.no_history != desired.no_history {
        properties.insert(SchemaProperty::NoHistory);
    }
    properties
}

proptest! {
    #[test]
    fn every_difference_is_classified_exactly_once((installed, desired) in schema_pair()) {
        let db = database(&installed);
        let file = desired_schema(&desired);
        for prune in [false, true] {
            let planned = plan(
                &file,
                &db,
                &PlanOptions::new("t").with_prune(prune),
            )
            .expect("generated schemas never name an engine attribute");

            let mut expected: BTreeSet<(String, ChangeKind)> = BTreeSet::new();
            for (name, wanted) in &desired {
                let ident = format!(":t/{name}");
                match installed.get(name) {
                    None => {
                        expected.insert((ident, ChangeKind::Add));
                    }
                    Some(current) => {
                        for property in differences(current, wanted) {
                            expected.insert((ident.clone(), ChangeKind::Property(property)));
                        }
                    }
                }
            }
            let mut expected_unmanaged = BTreeSet::new();
            for name in installed.keys() {
                if desired.contains_key(name) {
                    continue;
                }
                let ident = format!(":t/{name}");
                if prune {
                    expected_unmanaged.insert(ident.clone());
                    expected.insert((ident, ChangeKind::Retire));
                } else {
                    expected_unmanaged.insert(ident);
                }
            }

            let planned_changes: BTreeSet<(String, ChangeKind)> = planned
                .steps
                .iter()
                .map(|step| (step.ident.clone(), step.kind))
                .collect();
            prop_assert_eq!(planned_changes.len(), planned.steps.len(), "a change was planned twice");
            prop_assert_eq!(&planned_changes, &expected);

            let unmanaged: BTreeSet<String> = planned.unmanaged.iter().cloned().collect();
            if prune {
                prop_assert!(unmanaged.is_empty());
            } else {
                prop_assert_eq!(&unmanaged, &expected_unmanaged);
            }
        }
    }

    #[test]
    fn unsupported_changes_are_never_presented_as_executable(
        (installed, desired) in schema_pair()
    ) {
        let db = database(&installed);
        let planned = plan(
            &desired_schema(&desired),
            &db,
            &PlanOptions::new("t").with_prune(true),
        )
        .expect("plans");
        for step in &planned.steps {
            // A value-type change is destructive, and no allowance exists for
            // it: it can never be enabled by any flag combination.
            if step.kind == ChangeKind::Property(SchemaProperty::ValueType) {
                prop_assert_eq!(step.class, ExecutionClass::Destructive);
                prop_assert!(!step.class.executable());
            }
            prop_assert!(
                step.class != ExecutionClass::Destructive
                    || step.kind == ChangeKind::Property(SchemaProperty::ValueType)
            );
            // Every blocked step is reported as blocked in exactly one way.
            let blocked = step.blocked.is_some() || !step.class.executable();
            prop_assert_eq!(
                blocked,
                planned.blocked_steps().any(|other| other.key == step.key)
            );
        }
    }

    #[test]
    fn planning_is_deterministic_and_digests_follow_the_logical_plan(
        (installed, desired) in schema_pair()
    ) {
        let db = database(&installed);
        let file = desired_schema(&desired);
        let options = PlanOptions::new("t");
        let first = plan(&file, &db, &options).expect("plans");
        let second = plan(&file, &db, &options).expect("plans");
        prop_assert_eq!(&first, &second);
        prop_assert_eq!(first.digest(), second.digest());

        // Step keys are unique and sorted, so a resumable plan can key work by
        // them.
        let keys: Vec<&String> = first.steps.iter().map(|step| &step.key).collect();
        let mut sorted = keys.clone();
        sorted.sort();
        sorted.dedup();
        prop_assert_eq!(keys, sorted);

        // Pruning is a logical difference whenever it changes the step set.
        let pruned = plan(&file, &db, &options.clone().with_prune(true)).expect("plans");
        if pruned.steps != first.steps {
            prop_assert_ne!(first.digest(), pruned.digest());
        }
    }
}
