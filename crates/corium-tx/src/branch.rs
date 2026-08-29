//! What a saga branch's own pipeline refuses (ADR-0023).
//!
//! A step is an ordinary transaction. It resolves tempids, upserts, honors
//! `:db/cas`, validates against the branch's schema, and lands durably in the
//! branch log exactly like a transaction against any database — that is the
//! point of hosting a saga on a branch rather than inventing a second write
//! path. The rules here are the small set of additional refusals that make
//! the branch's novelty *mergeable*, and each one exists because a reader or
//! the merge would otherwise be told something untrue:
//!
//! * **Allocations come from the leased blocks.** Branch-created entities
//!   keep their ids verbatim through the merge, which is the whole reason ids
//!   are leased rather than remapped. An id from outside the blocks is one
//!   the parent's allocator still believes is free, so it would either
//!   collide at merge or silently rename an entity a branch reader had
//!   already resolved.
//! * **Reservations bind the saga, and only the saga.** A saga that declares
//!   `:db.saga/reserves` has told tier-1 readers that everything outside the
//!   set is untouched by it. That promise is kept here, at step time, or not
//!   at all: the merge scan narrows to the reserved set precisely because
//!   these checks guarantee novelty touches nothing else pre-existing.
//! * **Refs into the parent graph close over the reserved set.** Corium
//!   indexes refs in reverse, so a branch-created entity merely *pointing at*
//!   a pre-existing entity changes what reverse navigation from it returns
//!   after the merge. Without this rule "X is outside the reserved set" would
//!   be false in exactly the way a reader cannot see coming.
//! * **The registry stays in the parent.** A step that wrote `:db.saga/*`
//!   would smuggle a saga entry into the parent through the merge — a nested
//!   saga by accident, which v1 does not have a merge story for.
//! * **Schema stays in the parent.** Schema migration has its own plan/apply
//!   lifecycle, and mixing the two inside a merge is needless coupling.
//!   Ordinary transaction data cannot install an attribute in any database
//!   ([`crate::prepare`] refuses the vocabulary outright); what this adds is
//!   the refusal of an entity minted in `:db.part/db` at all.
//!
//! Nothing here is about *conflict*: a step is validated against the branch,
//! never against the parent, and what the parent has been doing since `t₀` is
//! the merge's question. These rules only bound what the branch may say.

use corium_core::{AttrId, Datom, EntityId, Partition, Value};
use corium_db::saga::{self, IdGrant, SagaEntry};
use thiserror::Error;

/// What a branch's step checks are enforced against: the saga's leased id
/// blocks and its declared reservation set, as the parent registry records
/// them.
///
/// The rules are read from the parent at step time rather than fixed when the
/// branch opens, because an unsealed saga's reservation set may grow — by an
/// ordinary parent transaction, observable to readers with a basis — while
/// the branch is running.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BranchRules {
    /// Entity-id blocks leased to this branch.
    pub grants: Vec<IdGrant>,
    /// The checked reservation set: pre-existing entities, and attribute
    /// entities naming whole attributes. Empty means unreserved, which binds
    /// nothing.
    pub reserves: Vec<EntityId>,
}

impl BranchRules {
    /// The rules a registry entry states.
    #[must_use]
    pub fn of(entry: &SagaEntry) -> Self {
        Self {
            grants: entry.grants.clone(),
            reserves: entry.reserves.clone(),
        }
    }

    /// The lowest sequence any leased block covers, which is the boundary
    /// between the parent's id space and this branch's.
    ///
    /// Blocks are carved off the parent's allocator when the saga opens, so
    /// every id the parent had issued by `t₀` sits below it.
    #[must_use]
    fn grant_floor(&self) -> u64 {
        self.grants
            .iter()
            .filter_map(|grant| grant.start)
            .filter_map(|start| u64::try_from(start).ok())
            .min()
            .unwrap_or(u64::MAX)
    }

    /// Whether `entity` is one of this branch's own allocations.
    #[must_use]
    fn is_branch_entity(&self, entity: EntityId) -> bool {
        self.grants.iter().any(|grant| grant.holds(entity))
    }

    /// Whether `entity` is a user entity that predates the branch.
    ///
    /// Ids below the leased blocks are the parent's — whether or not any
    /// datom currently uses them, since naming an unused id directly is a
    /// liberty transaction data has always had, and a step that takes it is
    /// writing on the parent's side of the boundary.
    #[must_use]
    fn is_pre_existing(&self, entity: EntityId) -> bool {
        entity.partition() == Partition::User as u32
            && entity.sequence() < self.grant_floor()
            && !self.is_branch_entity(entity)
    }

    /// Whether the reservation set covers writing `a` on `e`.
    #[must_use]
    fn reserves(&self, e: EntityId, a: AttrId) -> bool {
        self.reserves.contains(&e) || self.reserves.contains(&a)
    }
}

/// A step that would say something about the parent's graph the saga did not
/// declare, or claim ids it was not leased.
#[derive(Clone, Debug, Eq, PartialEq, Error)]
pub enum StepViolation {
    /// The branch holds no id blocks, so it can create nothing.
    #[error("this saga's branch holds no entity-id grants")]
    NoGrant,
    /// A step named an id outside the branch's leased blocks.
    #[error("entity {0:?} is outside the entity-id blocks leased to this saga")]
    UngrantedId(EntityId),
    /// A step wrote a pre-existing entity the saga never reserved.
    #[error("entity {entity:?} is not in this saga's reservation set (attribute {attribute:?})")]
    Unreserved {
        /// The pre-existing entity written.
        entity: EntityId,
        /// Attribute the step wrote on it.
        attribute: AttrId,
    },
    /// A step referenced a pre-existing entity the saga never reserved.
    #[error(
        "reference to {target:?} leaves this saga's reservation set (from {entity:?} \
         under {attribute:?})"
    )]
    UnreservedRef {
        /// Entity carrying the reference.
        entity: EntityId,
        /// Attribute the reference was asserted under.
        attribute: AttrId,
        /// The pre-existing entity referenced.
        target: EntityId,
    },
    /// A step wrote the saga registry, which lives in the parent.
    #[error("the saga registry is parent data; a branch step cannot write {0:?}")]
    RegistryWrite(AttrId),
    /// A step created an entity in the schema partition.
    #[error("schema changes are refused on a saga branch (entity {0:?})")]
    SchemaEntity(EntityId),
}

/// Checks one step's datoms against the branch's `rules`.
///
/// # Errors
/// Returns the first [`StepViolation`] the step commits, in datom order, so
/// the same step always fails for the same reason.
pub fn validate_step(datoms: &[Datom], rules: &BranchRules) -> Result<(), StepViolation> {
    if rules.grants.is_empty() {
        return Err(StepViolation::NoGrant);
    }
    let reserved = !rules.reserves.is_empty();
    for datom in datoms {
        if saga::is_registry_attribute(datom.a) {
            return Err(StepViolation::RegistryWrite(datom.a));
        }
        if datom.e.partition() == Partition::Db as u32 {
            return Err(StepViolation::SchemaEntity(datom.e));
        }
        let target = match &datom.v {
            Value::Ref(entity) => Some(*entity),
            _ => None,
        };
        // The step's own transaction entity carries its instant and metadata;
        // it lives in the tx partition, never merges, and is nobody's
        // reservation.
        if datom.e.partition() == Partition::User as u32
            && !rules.is_branch_entity(datom.e)
            && !rules.is_pre_existing(datom.e)
        {
            return Err(StepViolation::UngrantedId(datom.e));
        }
        if let Some(target) = target
            && target.partition() == Partition::User as u32
            && !rules.is_branch_entity(target)
            && !rules.is_pre_existing(target)
        {
            return Err(StepViolation::UngrantedId(target));
        }
        if !reserved {
            continue;
        }
        if rules.is_pre_existing(datom.e) && !rules.reserves(datom.e, datom.a) {
            return Err(StepViolation::Unreserved {
                entity: datom.e,
                attribute: datom.a,
            });
        }
        if let Some(target) = target
            && rules.is_pre_existing(target)
            && !rules.reserves(target, datom.a)
        {
            return Err(StepViolation::UnreservedRef {
                entity: datom.e,
                attribute: datom.a,
                target,
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use corium_core::Value;
    use corium_db::bootstrap;

    fn grant(start: i64, length: i64) -> IdGrant {
        IdGrant {
            entity: EntityId::new(Partition::User as u32, 900),
            partition: Some(i64::from(Partition::User as u32)),
            start: Some(start),
            length: Some(length),
        }
    }

    fn rules(reserves: Vec<EntityId>) -> BranchRules {
        BranchRules {
            grants: vec![grant(10_000, 1_000)],
            reserves,
        }
    }

    fn user(sequence: u64) -> EntityId {
        EntityId::new(Partition::User as u32, sequence)
    }

    fn datom(e: EntityId, a: AttrId, v: Value) -> Datom {
        Datom {
            e,
            a,
            v,
            tx: EntityId::new(Partition::Tx as u32, 5),
            added: true,
        }
    }

    const NAME: AttrId = EntityId::new(Partition::Db as u32, 900);
    const OWNER: AttrId = EntityId::new(Partition::Db as u32, 901);

    #[test]
    fn a_branch_with_no_block_can_write_nothing() {
        let rules = BranchRules::default();
        let step = [datom(user(10_000), NAME, Value::Str("x".into()))];
        assert_eq!(validate_step(&step, &rules), Err(StepViolation::NoGrant));
    }

    #[test]
    fn novelty_comes_from_the_leased_block() {
        let rules = rules(Vec::new());
        let inside = [datom(user(10_500), NAME, Value::Str("x".into()))];
        assert_eq!(validate_step(&inside, &rules), Ok(()));
        // Above the block: unleased space, or somebody else's lease.
        let outside = [datom(user(20_000), NAME, Value::Str("x".into()))];
        assert_eq!(
            validate_step(&outside, &rules),
            Err(StepViolation::UngrantedId(user(20_000)))
        );
    }

    #[test]
    fn an_unreserved_saga_may_write_pre_existing_entities() {
        let rules = rules(Vec::new());
        let step = [datom(user(500), NAME, Value::Str("x".into()))];
        assert_eq!(validate_step(&step, &rules), Ok(()));
    }

    #[test]
    fn a_reserved_saga_writes_only_what_it_reserved() {
        let rules = rules(vec![user(500)]);
        assert_eq!(
            validate_step(&[datom(user(500), NAME, Value::Str("x".into()))], &rules),
            Ok(())
        );
        assert_eq!(
            validate_step(&[datom(user(501), NAME, Value::Str("x".into()))], &rules),
            Err(StepViolation::Unreserved {
                entity: user(501),
                attribute: NAME,
            })
        );
    }

    #[test]
    fn reserving_an_attribute_covers_every_entity_under_it() {
        let rules = rules(vec![NAME]);
        assert_eq!(
            validate_step(&[datom(user(501), NAME, Value::Str("x".into()))], &rules),
            Ok(())
        );
        assert_eq!(
            validate_step(&[datom(user(501), OWNER, Value::Str("x".into()))], &rules),
            Err(StepViolation::Unreserved {
                entity: user(501),
                attribute: OWNER,
            })
        );
    }

    #[test]
    fn refs_out_of_the_reserved_set_are_refused() {
        let rules = rules(vec![user(500)]);
        // Branch novelty may point at a reserved entity.
        assert_eq!(
            validate_step(&[datom(user(10_001), OWNER, Value::Ref(user(500)))], &rules),
            Ok(())
        );
        // Pointing anywhere else in the parent graph would change what
        // reverse-ref navigation from that entity returns after the merge.
        assert_eq!(
            validate_step(&[datom(user(10_001), OWNER, Value::Ref(user(501)))], &rules),
            Err(StepViolation::UnreservedRef {
                entity: user(10_001),
                attribute: OWNER,
                target: user(501),
            })
        );
        // Branch-created entities point at each other freely.
        assert_eq!(
            validate_step(
                &[datom(user(10_001), OWNER, Value::Ref(user(10_002)))],
                &rules
            ),
            Ok(())
        );
    }

    #[test]
    fn the_registry_stays_in_the_parent() {
        let rules = rules(Vec::new());
        let step = [datom(user(10_001), bootstrap::SAGA_ID, Value::Uuid(3))];
        assert_eq!(
            validate_step(&step, &rules),
            Err(StepViolation::RegistryWrite(bootstrap::SAGA_ID))
        );
    }

    #[test]
    fn a_steps_own_transaction_entity_is_not_novelty() {
        let rules = rules(vec![user(500)]);
        let tx = EntityId::new(Partition::Tx as u32, 5);
        let step = [datom(tx, bootstrap::TX_INSTANT, Value::Instant(1))];
        assert_eq!(validate_step(&step, &rules), Ok(()));
    }
}
