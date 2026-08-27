//! Registry invariants an ordinary transaction must not break (ADR-0023).
//!
//! Saga lifecycle transitions are ordinary transactions over the `:db.saga/*`
//! vocabulary — that is the whole visibility story, and this module does not
//! take it back. Opening, extending, aborting, and committing stay plain tx
//! data any client can compose, any peer can watch as a tx-report, and any
//! Datalog query can read. What is checked here is only what the *shape* of a
//! registry entry promises to a reader who has no other way to find out:
//!
//! * a saga's status is one of the four the engine defines, and it moves only
//!   from `:open` to one of the three terminal states. Without this, "the
//!   registry says `:committed`" — the sentence a branch reader's whole
//!   adaptation contract rests on — would be revocable;
//! * an entry is complete when it is created: mandatory expiry is what keeps
//!   an abandoned saga from pinning `t₀`-era segments forever, so an entry
//!   with no deadline is not a saga, it is a leak;
//! * identity, opening basis, owner, and the sealed flag are fixed once
//!   written, because every reader-facing guarantee is stated relative to
//!   them;
//! * a finished saga accepts only its external-compensation ledger, which is
//!   the one thing the design says outlives it;
//! * `:db.saga/merged-tx` and `:db.saga/steps` are the merge's own record, so
//!   they may only be written by the transaction that flips the saga to
//!   `:committed` — the flip and the record are one append or neither;
//! * entity-id grants are the parent allocator's leases. A transaction that
//!   could mint them could hand itself ids the allocator still believes are
//!   free — and, for the same reason, no transaction may name an id inside a
//!   block already leased to an open saga.
//!
//! Everything else about a registry entry — description, footprint,
//! reservations on an unsealed saga, conflict reports, compensation
//! registrations, the ledger — is data, written and rewritten by whoever holds
//! transact rights, exactly like the application facts beside it.
//!
//! The checks run against the pre-transaction database and the datoms the
//! transaction leaves behind, so they see cardinality-one changes the way the
//! database will: a retraction of the old value and an assertion of the new.

use std::collections::BTreeSet;

use corium_core::{AttrId, Datom, EntityId, Value};
use corium_db::saga::{self, SagaStatus};
use corium_db::{Db, bootstrap};
use thiserror::Error;

/// A transaction that would leave the saga registry in a state no reader
/// could rely on.
#[derive(Clone, Debug, Eq, PartialEq, Error)]
pub enum SagaViolation {
    /// Registry attributes were written on an entity that is not a saga and
    /// is not being made one.
    #[error("entity {0:?} carries saga registry attributes without a :db.saga/id")]
    NotASaga(EntityId),
    /// A new saga entry was missing a mandatory field.
    #[error("opening a saga requires {0}")]
    IncompleteOpen(&'static str),
    /// A new saga entry was created in a state other than `:open`.
    #[error("a saga is created :db.saga.status/open, not {0}")]
    OpensFinished(SagaStatus),
    /// A status transition no saga may make.
    #[error("a saga cannot move from {from} to {to}")]
    IllegalTransition {
        /// Status the saga holds now.
        from: SagaStatus,
        /// Status the transaction asserts.
        to: SagaStatus,
    },
    /// A status was retracted without a replacement, leaving a saga with no
    /// state at all.
    #[error("a saga cannot be left without a :db.saga/status")]
    StatusCleared,
    /// A saga was written without a status, or with an unresolvable one.
    #[error("saga {0:?} has no status to transition from")]
    StatusMissing(EntityId),
    /// A fact that fixes what the entry means was changed after the fact.
    #[error("{0} is fixed when a saga opens")]
    ImmutableField(&'static str),
    /// A finished saga was written to, beyond its compensation ledger.
    #[error("saga is {status} and accepts only :db.saga/compensations, not {attribute}")]
    Finished {
        /// The saga's terminal (or unrecognized) status.
        status: SagaStatus,
        /// Attribute the transaction wrote.
        attribute: &'static str,
    },
    /// A sealed saga's reservation set was extended.
    #[error("saga is :db.saga/sealed; its reservation set is fixed at open")]
    SealedReservation,
    /// The merge's own record was written outside the commit transaction.
    #[error("{0} is written by the transaction that commits the saga")]
    MergeRecordWithoutCommit(&'static str),
    /// Entity-id grants were written by an ordinary transaction.
    #[error("entity-id grants are leased by the transactor, not written as transaction data")]
    GrantNotLeased,
    /// A transaction named an entity inside a block leased to an open saga.
    #[error("entity {0:?} is inside an entity-id block leased to an open saga")]
    GrantedId(EntityId),
}

/// Checks the registry writes in `datoms` against the pre-transaction `db`.
///
/// # Errors
/// Returns the first [`SagaViolation`] found, scanning saga entities in id
/// order so the same transaction always fails for the same reason.
pub fn validate(db: &Db, datoms: &[Datom]) -> Result<(), SagaViolation> {
    if datoms
        .iter()
        .any(|datom| saga::is_grant_attribute(datom.a) || datom.a == bootstrap::SAGA_ID_GRANTS)
    {
        return Err(SagaViolation::GrantNotLeased);
    }
    validate_granted_ids(db, datoms)?;
    let mut entities: BTreeSet<EntityId> = BTreeSet::new();
    for datom in datoms {
        if saga::is_saga_attribute(datom.a) {
            entities.insert(datom.e);
        }
    }
    for entity in entities {
        validate_entity(db, entity, datoms)?;
    }
    Ok(())
}

/// Refuses a transaction that names an id inside a live grant.
///
/// A transaction may name any entity id directly, which is the one way an
/// ordinary parent write could land on top of a branch's unspent allocations
/// — the branch's ids are promised, not yet used, so nothing in the parent's
/// datoms would collide to reveal it. This is allocator integrity rather than
/// a lock: leased id space names no user-visible entity, and the refusal ends
/// with the saga, because a committed saga's ids are then ordinary entities.
///
/// Both positions are checked. A ref *value* naming a granted id would attach
/// parent data to an entity the branch has not created yet — and, at merge,
/// to whichever entity the branch happened to give that id.
fn validate_granted_ids(db: &Db, datoms: &[Datom]) -> Result<(), SagaViolation> {
    let grants = saga::live_grants(db);
    if grants.is_empty() {
        return Ok(());
    }
    for datom in datoms {
        let referenced = match &datom.v {
            Value::Ref(entity) => Some(*entity),
            _ => None,
        };
        for entity in [Some(datom.e), referenced].into_iter().flatten() {
            if grants.iter().any(|grant| grant.holds(entity)) {
                return Err(SagaViolation::GrantedId(entity));
            }
        }
    }
    Ok(())
}

fn validate_entity(db: &Db, entity: EntityId, datoms: &[Datom]) -> Result<(), SagaViolation> {
    let writes: Vec<&Datom> = datoms
        .iter()
        .filter(|datom| datom.e == entity && saga::is_saga_attribute(datom.a))
        .collect();
    let asserted = |a: AttrId| writes.iter().any(|datom| datom.a == a && datom.added);
    let retracted = |a: AttrId| writes.iter().any(|datom| datom.a == a && !datom.added);
    let status = saga::asserted_status(db, entity, datoms);

    let Some(prior) = saga::entry_at(db, entity) else {
        // A new entry. `:db.saga/id` is what makes the entity a saga, so
        // nothing else in the vocabulary means anything without it.
        if !asserted(bootstrap::SAGA_ID) {
            return Err(SagaViolation::NotASaga(entity));
        }
        match status {
            Some(SagaStatus::Open) => {}
            Some(other) => return Err(SagaViolation::OpensFinished(other)),
            None => return Err(SagaViolation::IncompleteOpen(":db.saga/status")),
        }
        for (present, name) in [
            (asserted(bootstrap::SAGA_BASIS_T), ":db.saga/basis-t"),
            (asserted(bootstrap::SAGA_OWNER), ":db.saga/owner"),
            (asserted(bootstrap::SAGA_EXPIRES_AT), ":db.saga/expires-at"),
        ] {
            if !present {
                return Err(SagaViolation::IncompleteOpen(name));
            }
        }
        return check_merge_record(&writes, status.as_ref());
    };

    if asserted(bootstrap::SAGA_ID) || retracted(bootstrap::SAGA_ID) {
        return Err(SagaViolation::ImmutableField(":db.saga/id"));
    }
    for (attribute, name) in [
        (bootstrap::SAGA_BASIS_T, ":db.saga/basis-t"),
        (bootstrap::SAGA_OWNER, ":db.saga/owner"),
        (bootstrap::SAGA_SEALED, ":db.saga/sealed"),
    ] {
        if asserted(attribute) || retracted(attribute) {
            return Err(SagaViolation::ImmutableField(name));
        }
    }

    let Some(current) = prior.status else {
        return Err(SagaViolation::StatusMissing(entity));
    };
    if let Some(next) = &status {
        if !current.may_become(next) {
            return Err(SagaViolation::IllegalTransition {
                from: current,
                to: next.clone(),
            });
        }
    } else {
        if retracted(bootstrap::SAGA_STATUS) {
            return Err(SagaViolation::StatusCleared);
        }
        // No transition: the saga must still be open to be written to at all.
        if let Some(datom) = writes
            .iter()
            .find(|datom| datom.a != bootstrap::SAGA_COMPENSATIONS)
            && !matches!(current, SagaStatus::Open)
        {
            return Err(SagaViolation::Finished {
                status: current,
                attribute: name_of(datom.a),
            });
        }
    }

    if prior.sealed && asserted(bootstrap::SAGA_RESERVES) {
        return Err(SagaViolation::SealedReservation);
    }
    check_merge_record(&writes, status.as_ref())
}

/// `:db.saga/merged-tx` and `:db.saga/steps` describe a merge that happened,
/// so the transaction writing them is the one that made it happen.
fn check_merge_record(writes: &[&Datom], status: Option<&SagaStatus>) -> Result<(), SagaViolation> {
    if matches!(status, Some(SagaStatus::Committed)) {
        return Ok(());
    }
    for (attribute, name) in [
        (bootstrap::SAGA_MERGED_TX, ":db.saga/merged-tx"),
        (bootstrap::SAGA_STEPS, ":db.saga/steps"),
    ] {
        if writes
            .iter()
            .any(|datom| datom.a == attribute && datom.added)
        {
            return Err(SagaViolation::MergeRecordWithoutCommit(name));
        }
    }
    Ok(())
}

/// The registry attribute's keyword, for an error a person has to read.
fn name_of(a: AttrId) -> &'static str {
    match a {
        bootstrap::SAGA_ID => ":db.saga/id",
        bootstrap::SAGA_STATUS => ":db.saga/status",
        bootstrap::SAGA_BASIS_T => ":db.saga/basis-t",
        bootstrap::SAGA_DESCRIPTION => ":db.saga/description",
        bootstrap::SAGA_OWNER => ":db.saga/owner",
        bootstrap::SAGA_EXPIRES_AT => ":db.saga/expires-at",
        bootstrap::SAGA_ID_GRANTS => ":db.saga/id-grants",
        bootstrap::SAGA_FOOTPRINT => ":db.saga/footprint",
        bootstrap::SAGA_RESERVES => ":db.saga/reserves",
        bootstrap::SAGA_SEALED => ":db.saga/sealed",
        bootstrap::SAGA_MERGED_TX => ":db.saga/merged-tx",
        bootstrap::SAGA_STEPS => ":db.saga/steps",
        bootstrap::SAGA_CONFLICT_REPORT => ":db.saga/conflict-report",
        bootstrap::SAGA_ON_ABORT_TX => ":db.saga/on-abort-tx",
        bootstrap::SAGA_ON_ABORT_FN => ":db.saga/on-abort-fn",
        bootstrap::SAGA_ON_ABORT_ERROR => ":db.saga/on-abort-error",
        bootstrap::SAGA_COMPENSATIONS => ":db.saga/compensations",
        _ => "a saga registry attribute",
    }
}
