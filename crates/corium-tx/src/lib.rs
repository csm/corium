//! Pure transaction expansion, entity resolution, and validation.

use std::collections::{BTreeMap, BTreeSet};

use corium_core::{Cardinality, Datom, EntityId, Partition, Unique, Value};
use corium_db::{Db, FIRST_USER_ID};
use thiserror::Error;

/// A temporary entity identifier scoped to one transaction.
pub type TempId = String;

/// An entity position accepted by transaction operations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EntityRef {
    /// A concrete entity id.
    Id(EntityId),
    /// A transaction-local identifier.
    Temp(TempId),
    /// A unique attribute/value lookup.
    Lookup(EntityId, Value),
}

/// A transaction operation after boundary conversion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TxOp {
    /// Assert a fact.
    Add(EntityRef, EntityId, Value),
    /// Retract a fact.
    Retract(EntityRef, EntityId, Value),
    /// Compare and swap a cardinality-one value.
    Cas(EntityRef, EntityId, Option<Value>, Value),
    /// Recursively retract an entity and its component children.
    RetractEntity(EntityRef),
}

/// A map-form entity; each `(attribute, values)` entry expands to additions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EntityMap {
    /// Entity position.
    pub entity: EntityRef,
    /// Attribute values.
    pub attributes: Vec<(EntityId, Vec<Value>)>,
}

/// Transaction input supporting list and map forms.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TxItem {
    /// List-form operation.
    Op(TxOp),
    /// Map-form entity.
    Map(EntityMap),
}

/// Successfully prepared transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PreparedTx {
    /// Resolved datoms.
    pub datoms: Vec<Datom>,
    /// Allocations/upserts for caller tempids.
    pub tempids: BTreeMap<TempId, EntityId>,
}

/// Transaction validation error.
#[derive(Debug, Error, Eq, PartialEq)]
pub enum TxError {
    /// Attribute is absent from schema.
    #[error("unknown attribute {0:?}")]
    UnknownAttribute(EntityId),
    /// Value does not match attribute type.
    #[error("value has wrong type for attribute {0:?}")]
    TypeMismatch(EntityId),
    /// A lookup ref did not resolve.
    #[error("lookup ref did not resolve")]
    LookupNotFound,
    /// Lookup refs require unique attributes.
    #[error("lookup attribute is not unique")]
    LookupNotUnique,
    /// A uniqueness constraint would be violated.
    #[error("unique value conflict")]
    UniqueConflict,
    /// CAS old value did not match.
    #[error("compare-and-swap failed")]
    CasFailed,
    /// CAS is only valid for cardinality one.
    #[error("compare-and-swap requires cardinality one")]
    CasCardinality,
}

/// Expands and validates transaction input against `db`.
///
/// `tx` is the already allocated transaction entity id. Allocation begins at
/// `next_user_sequence`, making the function deterministic and easy to model-test.
///
/// # Errors
///
/// Returns [`TxError`] when entity resolution, schema validation, uniqueness,
/// or a built-in operation fails.
#[allow(clippy::too_many_lines)]
pub fn prepare(
    db: &Db,
    items: impl IntoIterator<Item = TxItem>,
    tx: EntityId,
    next_user_sequence: u64,
) -> Result<PreparedTx, TxError> {
    let mut ops = Vec::new();
    for item in items {
        match item {
            TxItem::Op(op) => ops.push(op),
            TxItem::Map(map) => {
                for (a, values) in map.attributes {
                    for value in values {
                        ops.push(TxOp::Add(map.entity.clone(), a, value));
                    }
                }
            }
        }
    }
    let mut tempids = BTreeMap::new();
    // Identity assertions unify a tempid with an existing entity before allocation.
    for op in &ops {
        if let TxOp::Add(EntityRef::Temp(temp), a, value) = op {
            if db.schema().get(*a).and_then(|x| x.unique) == Some(Unique::Identity) {
                if let Some(e) = db.lookup(*a, value) {
                    tempids.insert(temp.clone(), e);
                }
            }
        }
    }
    let mut next = next_user_sequence.max(FIRST_USER_ID);
    for op in &ops {
        let entity = match op {
            TxOp::Add(e, ..) | TxOp::Retract(e, ..) | TxOp::Cas(e, ..) | TxOp::RetractEntity(e) => {
                e
            }
        };
        if let EntityRef::Temp(temp) = entity {
            tempids.entry(temp.clone()).or_insert_with(|| {
                let e = EntityId::new(Partition::User as u32, next);
                next += 1;
                e
            });
        }
    }
    let resolve = |entity: &EntityRef| -> Result<EntityId, TxError> {
        match entity {
            EntityRef::Id(e) => Ok(*e),
            EntityRef::Temp(t) => Ok(tempids[t]),
            EntityRef::Lookup(a, v) => {
                let attr = db.schema().get(*a).ok_or(TxError::UnknownAttribute(*a))?;
                if attr.unique.is_none() {
                    return Err(TxError::LookupNotUnique);
                }
                db.lookup(*a, v).ok_or(TxError::LookupNotFound)
            }
        }
    };
    let mut datoms = Vec::new();
    let mut working = db.clone();
    for op in ops {
        let start = datoms.len();
        match op {
            TxOp::Add(entity, a, v) => {
                let e = resolve(&entity)?;
                validate(&working, a, &v)?;
                if let Some(attr) = working.schema().get(a) {
                    if attr.unique.is_some()
                        && working.lookup(a, &v).is_some_and(|owner| owner != e)
                    {
                        return Err(TxError::UniqueConflict);
                    }
                    let current = working.values(e, a);
                    // Re-asserting a present fact is a no-op: no datom recorded.
                    if current.contains(&v) {
                        continue;
                    }
                    if attr.cardinality == Cardinality::One {
                        for old in current {
                            datoms.push(Datom {
                                e,
                                a,
                                v: old,
                                tx,
                                added: false,
                            });
                        }
                    }
                }
                datoms.push(Datom {
                    e,
                    a,
                    v,
                    tx,
                    added: true,
                });
            }
            TxOp::Retract(entity, a, v) => {
                validate(&working, a, &v)?;
                let e = resolve(&entity)?;
                // Retracting an absent fact is a no-op: no datom recorded.
                if working.values(e, a).contains(&v) {
                    datoms.push(Datom {
                        e,
                        a,
                        v,
                        tx,
                        added: false,
                    });
                }
            }
            TxOp::Cas(entity, a, old, new) => {
                validate(&working, a, &new)?;
                let e = resolve(&entity)?;
                if working
                    .schema()
                    .get(a)
                    .is_none_or(|x| x.cardinality != Cardinality::One)
                {
                    return Err(TxError::CasCardinality);
                }
                let current = working.values(e, a).into_iter().next();
                if current != old {
                    return Err(TxError::CasFailed);
                }
                if let Some(value) = current {
                    datoms.push(Datom {
                        e,
                        a,
                        v: value,
                        tx,
                        added: false,
                    });
                }
                datoms.push(Datom {
                    e,
                    a,
                    v: new,
                    tx,
                    added: true,
                });
            }
            TxOp::RetractEntity(entity) => {
                let mut facts = BTreeSet::new();
                collect_entity_retractions(
                    &working,
                    resolve(&entity)?,
                    &mut facts,
                    &mut BTreeSet::new(),
                );
                datoms.extend(facts.into_iter().map(|(e, a, v)| Datom {
                    e,
                    a,
                    v,
                    tx,
                    added: false,
                }));
            }
        }
        working = working.with_transaction(working.basis_t() + 1, &datoms[start..]);
    }
    Ok(PreparedTx { datoms, tempids })
}

fn validate(db: &Db, a: EntityId, value: &Value) -> Result<(), TxError> {
    let attr = db.schema().get(a).ok_or(TxError::UnknownAttribute(a))?;
    if !value.has_type(attr.value_type) {
        return Err(TxError::TypeMismatch(a));
    }
    Ok(())
}

/// Collects the current facts removed by `:db/retractEntity` for `e`:
/// the entity's own datoms, incoming references to it, and (recursively)
/// its component children. Deduplicated by `(e, a, v)` because a component
/// child's outgoing-ref datom is also an incoming reference to the child.
fn collect_entity_retractions(
    db: &Db,
    e: EntityId,
    facts: &mut BTreeSet<(EntityId, EntityId, Value)>,
    seen: &mut BTreeSet<EntityId>,
) {
    if !seen.insert(e) {
        return;
    }
    for datom in db.datoms() {
        if datom.e == e {
            if db.schema().get(datom.a).is_some_and(|a| a.is_component) {
                if let Value::Ref(child) = &datom.v {
                    collect_entity_retractions(db, *child, facts, seen);
                }
            }
            facts.insert((datom.e, datom.a, datom.v));
        } else if datom.v == Value::Ref(e) {
            facts.insert((datom.e, datom.a, datom.v));
        }
    }
}
