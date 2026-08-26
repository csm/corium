//! Pure transaction expansion, entity resolution, and validation.

pub mod saga;

use std::collections::{BTreeMap, BTreeSet};

use corium_core::{Cardinality, Datom, EntityId, Partition, Unique, Value};
use corium_db::{Db, FIRST_USER_ID, bootstrap};
use thiserror::Error;

/// A temporary entity identifier scoped to one transaction.
pub type TempId = String;

/// The reserved tempid naming the transaction's own entity.
///
/// Asserting against it attaches metadata to the transaction — the audit
/// trail's who/why alongside the engine's `:db/txInstant` when. Corium spells
/// it as Datomic does so ported transaction data works unchanged; the EDN
/// boundary also accepts `:db/current-tx` in entity position.
pub const TX_TEMPID: &str = "datomic.tx";

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
    /// Transaction data asserted a `:db/txInstant` that does not advance the
    /// transaction clock. Instants are monotone by construction, so a
    /// backdated commit would break the ordering `as-of` an instant relies on.
    #[error("supplied :db/txInstant {supplied} is not after the last commit's {last}")]
    TxInstantNotMonotonic {
        /// Instant asserted by the transaction data.
        supplied: i64,
        /// Instant of the previous commit.
        last: i64,
    },
    /// `:db/txInstant` was retracted, changed, attached to another entity, or
    /// supplied more than once. It is engine-owned data except for one
    /// explicit assertion on the transaction currently being prepared.
    #[error(":db/txInstant may only be added once to the current transaction entity")]
    InvalidTxInstantOperation,
    /// A plaintext value was asserted on a protected attribute.
    ///
    /// A client that does not understand protection cannot accidentally write
    /// plaintext into a protected attribute (ADR-0018).
    #[error("attribute {0} is protected; assertions must be sealed")]
    UnprotectedValue(EntityId),
    /// A sealed value was asserted on an attribute that is not protected.
    #[error("attribute {0} is not protected; assertions must be plaintext")]
    UnexpectedProtection(EntityId),
    /// An assertion used a class or epoch that is no longer current.
    ///
    /// The writing peer sealed against a schema the transactor has since
    /// moved past; it retries against the new one rather than storing a value
    /// in the stale form.
    #[error(
        "attribute {attr} is protected by class {expected_class} at epoch \
         {expected_epoch}; the assertion is sealed under class {class} epoch {epoch}"
    )]
    StaleProtectionForm {
        /// Attribute being asserted on.
        attr: EntityId,
        /// Class the schema requires now.
        expected_class: EntityId,
        /// Epoch the schema requires now.
        expected_epoch: u32,
        /// Class the value names.
        class: EntityId,
        /// Epoch the value names.
        epoch: u32,
    },
    /// An ordinary transaction touched the schema vocabulary.
    ///
    /// Attribute metadata is data, but it is not data a `Transact` may write:
    /// changing it is an administrative action with its own plan, its own
    /// preconditions, and its own `alter-schema` authority
    /// (`docs/design/schema-migrations.md`). Letting a writer assert
    /// `:db/valueType` would be exactly the silent broadening that separation
    /// exists to prevent.
    #[error("attribute {0} is schema metadata; use a schema update to change it")]
    SchemaAttribute(EntityId),
    /// A new fact was asserted on a retired attribute.
    ///
    /// Retirement is forward-only and refuses assertions alone: every existing
    /// fact stays readable, and retracting one stays legal, because an
    /// immutable database cannot make an attribute disappear from history.
    #[error("attribute {0} is retired and refuses new assertions")]
    RetiredAttribute(EntityId),
    /// A write would leave the saga registry in a state no reader could
    /// rely on (see [`saga`]).
    #[error("{0}")]
    Saga(#[from] saga::SagaViolation),
    /// A retraction or `:db/cas` old value named a class the attribute has
    /// never been sealed under — a form it never had.
    #[error("attribute {attr} was never sealed under class {class}")]
    ForeignProtectionClass {
        /// Attribute being retracted from.
        attr: EntityId,
        /// Class the value names.
        class: EntityId,
    },
}

/// Checks the form an assertion must use on `a`, without holding any key.
///
/// Assertions use the form the attribute has *now*: sealed under the current
/// class and epoch if it is protected, plaintext if it is not. The value type
/// is checked separately by [`validate`], which reads a sealed value's
/// declared type from its cleartext header.
fn validate_assertion_form(db: &Db, a: EntityId, value: &Value) -> Result<(), TxError> {
    let schema = db.schema();
    match (schema.protection(a).current(), value) {
        (Some(expected_class), Value::Sealed(sealed)) => {
            let expected_epoch = schema
                .class(expected_class)
                .map_or(sealed.epoch, |class| class.current_epoch);
            if sealed.class == expected_class && sealed.epoch == expected_epoch {
                Ok(())
            } else {
                Err(TxError::StaleProtectionForm {
                    attr: a,
                    expected_class,
                    expected_epoch,
                    class: sealed.class,
                    epoch: sealed.epoch,
                })
            }
        }
        (Some(_), _) => Err(TxError::UnprotectedValue(a)),
        (None, Value::Sealed(_)) => Err(TxError::UnexpectedProtection(a)),
        (None, _) => Ok(()),
    }
}

/// Checks the form a retraction or `:db/cas` old value may use on `a`.
///
/// Any form the attribute has ever had is accepted — plaintext, or sealed
/// under any class in its timeline at any epoch. A fact can only be retracted
/// by naming the bytes it was asserted as, and those bytes do not change when
/// the schema does.
fn validate_historical_form(db: &Db, a: EntityId, value: &Value) -> Result<(), TxError> {
    let Value::Sealed(sealed) = value else {
        return Ok(());
    };
    if db
        .schema()
        .protection(a)
        .classes()
        .any(|class| class == sealed.class)
    {
        Ok(())
    } else {
        Err(TxError::ForeignProtectionClass {
            attr: a,
            class: sealed.class,
        })
    }
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
        if let TxOp::Add(EntityRef::Temp(temp), a, value) = op
            && temp != TX_TEMPID
            && db.schema().get(*a).and_then(|x| x.unique) == Some(Unique::Identity)
            && let Some(e) = db.lookup(*a, value)
        {
            tempids.insert(temp.clone(), e);
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
            // The transaction's own entity is already allocated; metadata
            // assertions resolve to it rather than to a fresh id.
            if temp == TX_TEMPID {
                tempids.insert(temp.clone(), tx);
                continue;
            }
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
    let mut tx_instant_asserted = false;
    for op in ops {
        let start = datoms.len();
        match op {
            TxOp::Add(entity, a, v) => {
                let e = resolve(&entity)?;
                if a == bootstrap::TX_INSTANT {
                    if e != tx || tx_instant_asserted {
                        return Err(TxError::InvalidTxInstantOperation);
                    }
                    tx_instant_asserted = true;
                }
                validate(&working, e, a, &v)?;
                validate_not_retired(&working, a)?;
                validate_assertion_form(&working, a, &v)?;
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
                if a == bootstrap::TX_INSTANT {
                    return Err(TxError::InvalidTxInstantOperation);
                }
                let e = resolve(&entity)?;
                validate(&working, e, a, &v)?;
                validate_historical_form(&working, a, &v)?;
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
                if a == bootstrap::TX_INSTANT {
                    return Err(TxError::InvalidTxInstantOperation);
                }
                let e = resolve(&entity)?;
                validate(&working, e, a, &new)?;
                validate_not_retired(&working, a)?;
                validate_assertion_form(&working, a, &new)?;
                if let Some(old) = &old {
                    validate_historical_form(&working, a, old)?;
                }
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
                if facts.iter().any(|(_, a, _)| *a == bootstrap::TX_INSTANT) {
                    return Err(TxError::InvalidTxInstantOperation);
                }
                // Retracting an attribute entity would uninstall an attribute
                // whose facts are still stored and still have to be decoded.
                // Schema removal is retirement, which keeps the metadata.
                if let Some((_, a, _)) = facts
                    .iter()
                    .find(|(e, a, _)| !writable_by_transaction(*e, *a))
                {
                    return Err(TxError::SchemaAttribute(*a));
                }
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
    // The registry's invariants are properties of the whole transaction — a
    // transition is a retraction and an assertion together — so they are
    // checked against what the transaction leaves behind, not op by op.
    saga::validate(db, &datoms)?;
    Ok(PreparedTx { datoms, tempids })
}

fn validate(db: &Db, e: EntityId, a: EntityId, value: &Value) -> Result<(), TxError> {
    if !writable_by_transaction(e, a) {
        return Err(TxError::SchemaAttribute(a));
    }
    let attr = db.schema().get(a).ok_or(TxError::UnknownAttribute(a))?;
    if !value.has_type(attr.value_type) {
        return Err(TxError::TypeMismatch(a));
    }
    Ok(())
}

/// Whether an ordinary transaction may write `a` on `e`.
///
/// `:db/ident` does double duty. On an attribute entity it is schema — the
/// name an attribute is known by — and changing it belongs to a schema update.
/// On any other entity it is an ordinary name: how a database function or an
/// enumerated value is labelled, and how Datomic-shaped data has always spelt
/// it. The two are told apart by partition, because `:db.part/db` is where
/// schema entities live.
///
/// Every other vocabulary attribute describes an attribute and is refused
/// outright, as is the schema-update audit trail: a transaction must not be
/// able to claim it was a schema update.
fn writable_by_transaction(e: EntityId, a: EntityId) -> bool {
    if bootstrap::is_audit_attribute(a) {
        return false;
    }
    if !bootstrap::is_schema_attribute(a) {
        return true;
    }
    a == bootstrap::IDENT && e.partition() != Partition::Db as u32
}

/// Checks that `a` still accepts new facts.
///
/// Only assertions are gated. A retired attribute keeps every fact it holds,
/// and retracting one — including through `:db/cas` — stays legal, which is
/// what makes retirement a usable step in cutting an application over to a
/// replacement attribute.
fn validate_not_retired(db: &Db, a: EntityId) -> Result<(), TxError> {
    if db.schema().is_retired(a) {
        return Err(TxError::RetiredAttribute(a));
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
            if db.schema().get(datom.a).is_some_and(|a| a.is_component)
                && let Value::Ref(child) = &datom.v
            {
                collect_entity_retractions(db, *child, facts, seen);
            }
            facts.insert((datom.e, datom.a, datom.v));
        } else if datom.v == Value::Ref(e) {
            facts.insert((datom.e, datom.a, datom.v));
        }
    }
}
