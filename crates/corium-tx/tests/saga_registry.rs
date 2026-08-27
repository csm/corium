//! What an ordinary transaction may and may not do to a saga registry entry
//! (ADR-0023, `docs/design/long-running-transactions.md`).

use std::sync::Arc;

use corium_core::{EntityId, KeywordInterner, Partition, Schema, Value};
use corium_db::saga::{self, SagaStatus};
use corium_db::{Db, Idents, bootstrap};
use corium_tx::saga::SagaViolation;
use corium_tx::{EntityRef, TxError, TxItem, TxOp, prepare};

/// A database with the engine vocabulary installed and every saga status
/// keyword interned, as the transactor's boundary interns them before
/// preparing a transaction that names one.
/// An ordinary user attribute, so a test can write data that is not schema
/// vocabulary and not registry vocabulary.
const NOTE: EntityId = EntityId::new(Partition::Db as u32, 5_000);

fn fixture() -> Db {
    let mut schema = Schema::default();
    let mut idents = Idents::default();
    bootstrap::install(&mut schema, &mut idents);
    schema.insert(corium_core::Attribute {
        id: NOTE,
        value_type: corium_core::ValueType::Str,
        cardinality: corium_core::Cardinality::One,
        unique: None,
        is_component: false,
        indexed: false,
        no_history: false,
    });
    let mut interner = KeywordInterner::default();
    for status in [
        SagaStatus::Open,
        SagaStatus::Committed,
        SagaStatus::Aborted,
        SagaStatus::Expired,
    ] {
        interner.intern(status.keyword());
    }
    interner.intern(corium_core::Keyword::new(Some("db.saga.status"), "paused"));
    Db::new(schema).with_naming(idents, interner)
}

fn status(db: &Db, status: &SagaStatus) -> Value {
    Value::Keyword(
        db.interner()
            .get(&status.keyword())
            .expect("status keywords are interned by the fixture"),
    )
}

fn tx(t: u64) -> EntityId {
    EntityId::new(Partition::Tx as u32, t)
}

fn add(entity: &str, a: EntityId, v: Value) -> TxItem {
    TxItem::Op(TxOp::Add(EntityRef::Temp(entity.into()), a, v))
}

fn add_to(entity: EntityId, a: EntityId, v: Value) -> TxItem {
    TxItem::Op(TxOp::Add(EntityRef::Id(entity), a, v))
}

/// Opening tx data for a complete registry entry, as the peer builds it.
fn open_forms(db: &Db, id: u128, expires_at: i64) -> Vec<TxItem> {
    vec![
        add("saga", bootstrap::SAGA_ID, Value::Uuid(id)),
        add(
            "saga",
            bootstrap::SAGA_STATUS,
            status(db, &SagaStatus::Open),
        ),
        add("saga", bootstrap::SAGA_BASIS_T, Value::Long(7)),
        add(
            "saga",
            bootstrap::SAGA_OWNER,
            Value::Str(Arc::from("alice")),
        ),
        add(
            "saga",
            bootstrap::SAGA_EXPIRES_AT,
            Value::Instant(expires_at),
        ),
    ]
}

/// Opens a saga and returns the database holding it plus its entity.
fn opened(db: &Db, id: u128) -> (Db, EntityId) {
    let prepared = prepare(db, open_forms(db, id, 10_000), tx(1), 1_000).expect("the open commits");
    let entity = prepared.tempids["saga"];
    (db.with_transaction(1, &prepared.datoms), entity)
}

fn violation(error: TxError) -> SagaViolation {
    match error {
        TxError::Saga(violation) => violation,
        other => panic!("expected a saga violation, got {other:?}"),
    }
}

#[test]
fn opening_writes_an_entry_the_read_model_folds_back() {
    let db = fixture();
    let (db, entity) = opened(&db, 42);
    let entry = saga::entry(&db, 42).expect("the registry holds the saga");
    assert_eq!(entry.entity, entity);
    assert_eq!(entry.status, Some(SagaStatus::Open));
    assert_eq!(entry.basis_t, Some(7));
    assert_eq!(entry.owner.as_deref(), Some("alice"));
    assert_eq!(entry.expires_at, Some(10_000));
    assert!(!entry.sealed);
}

#[test]
fn an_open_entry_needs_a_deadline_an_owner_and_a_basis() {
    let db = fixture();
    for (index, missing) in [
        bootstrap::SAGA_BASIS_T,
        bootstrap::SAGA_OWNER,
        bootstrap::SAGA_EXPIRES_AT,
    ]
    .into_iter()
    .enumerate()
    {
        let forms: Vec<TxItem> = open_forms(&db, 1, 10_000)
            .into_iter()
            .filter(|item| !matches!(item, TxItem::Op(TxOp::Add(_, a, _)) if *a == missing))
            .collect();
        let error = prepare(&db, forms, tx(1), 1_000).expect_err("the open is incomplete");
        assert!(
            matches!(violation(error), SagaViolation::IncompleteOpen(_)),
            "field {index} should be required"
        );
    }
}

#[test]
fn registry_facts_without_an_id_are_not_a_saga() {
    let db = fixture();
    let error = prepare(
        &db,
        vec![add(
            "saga",
            bootstrap::SAGA_DESCRIPTION,
            Value::Str(Arc::from("nightly repair")),
        )],
        tx(1),
        1_000,
    )
    .expect_err("registry attributes need a saga");
    assert!(matches!(violation(error), SagaViolation::NotASaga(_)));
}

#[test]
fn a_saga_is_created_open() {
    let db = fixture();
    let forms: Vec<TxItem> = open_forms(&db, 1, 10_000)
        .into_iter()
        .map(|item| match item {
            TxItem::Op(TxOp::Add(entity, a, _)) if a == bootstrap::SAGA_STATUS => {
                TxItem::Op(TxOp::Add(entity, a, status(&db, &SagaStatus::Committed)))
            }
            other => other,
        })
        .collect();
    let error = prepare(&db, forms, tx(1), 1_000).expect_err("a saga cannot open finished");
    assert!(matches!(
        violation(error),
        SagaViolation::OpensFinished(SagaStatus::Committed)
    ));
}

#[test]
fn an_open_saga_reaches_each_terminal_state() {
    for terminal in [
        SagaStatus::Committed,
        SagaStatus::Aborted,
        SagaStatus::Expired,
    ] {
        let db = fixture();
        let (db, entity) = opened(&db, 1);
        let mut forms = vec![add_to(
            entity,
            bootstrap::SAGA_STATUS,
            status(&db, &terminal),
        )];
        if matches!(terminal, SagaStatus::Committed) {
            forms.push(add_to(entity, bootstrap::SAGA_STEPS, Value::Long(3)));
        }
        let prepared = prepare(&db, forms, tx(2), 1_100).expect("the flip commits");
        let db = db.with_transaction(2, &prepared.datoms);
        assert_eq!(
            saga::entry(&db, 1).expect("the entry survives").status,
            Some(terminal)
        );
    }
}

#[test]
fn a_finished_saga_never_moves_again() {
    let db = fixture();
    let (db, entity) = opened(&db, 1);
    let prepared = prepare(
        &db,
        vec![add_to(
            entity,
            bootstrap::SAGA_STATUS,
            status(&db, &SagaStatus::Committed),
        )],
        tx(2),
        1_100,
    )
    .expect("the commit flip lands");
    let db = db.with_transaction(2, &prepared.datoms);

    // An abort arriving after the merge committed fails with "already
    // committed"; it does not report success.
    let error = prepare(
        &db,
        vec![add_to(
            entity,
            bootstrap::SAGA_STATUS,
            status(&db, &SagaStatus::Aborted),
        )],
        tx(3),
        1_100,
    )
    .expect_err("a committed saga cannot abort");
    assert!(matches!(
        violation(error),
        SagaViolation::IllegalTransition {
            from: SagaStatus::Committed,
            to: SagaStatus::Aborted,
        }
    ));

    // Nor does it accept an extension, or any other registry edit.
    let error = prepare(
        &db,
        vec![add_to(
            entity,
            bootstrap::SAGA_EXPIRES_AT,
            Value::Instant(99_000),
        )],
        tx(3),
        1_100,
    )
    .expect_err("a committed saga cannot be extended");
    assert!(matches!(violation(error), SagaViolation::Finished { .. }));
}

#[test]
fn a_status_outside_the_vocabulary_is_refused() {
    let db = fixture();
    let (db, entity) = opened(&db, 1);
    let paused = Value::Keyword(
        db.interner()
            .get(&corium_core::Keyword::new(Some("db.saga.status"), "paused"))
            .expect("the fixture interns it"),
    );
    let error = prepare(
        &db,
        vec![add_to(entity, bootstrap::SAGA_STATUS, paused)],
        tx(2),
        1_100,
    )
    .expect_err("the engine defines four statuses");
    assert!(matches!(
        violation(error),
        SagaViolation::IllegalTransition { .. }
    ));
}

#[test]
fn a_saga_cannot_be_left_without_a_status() {
    let db = fixture();
    let (db, entity) = opened(&db, 1);
    let open = status(&db, &SagaStatus::Open);
    let error = prepare(
        &db,
        vec![TxItem::Op(TxOp::Retract(
            EntityRef::Id(entity),
            bootstrap::SAGA_STATUS,
            open,
        ))],
        tx(2),
        1_100,
    )
    .expect_err("a saga always has a status");
    assert!(matches!(violation(error), SagaViolation::StatusCleared));
}

#[test]
fn identity_basis_owner_and_sealing_are_fixed_at_open() {
    let db = fixture();
    let (db, entity) = opened(&db, 1);
    for (attribute, value) in [
        (bootstrap::SAGA_ID, Value::Uuid(2)),
        (bootstrap::SAGA_BASIS_T, Value::Long(9)),
        (bootstrap::SAGA_OWNER, Value::Str(Arc::from("mallory"))),
        (bootstrap::SAGA_SEALED, Value::Bool(true)),
    ] {
        let error = prepare(&db, vec![add_to(entity, attribute, value)], tx(2), 1_000)
            .expect_err("the field is fixed at open");
        assert!(matches!(violation(error), SagaViolation::ImmutableField(_)));
    }
}

#[test]
fn a_sealed_saga_refuses_a_wider_reservation_set() {
    let db = fixture();
    let target = EntityId::new(Partition::User as u32, 5_000);
    let later = EntityId::new(Partition::User as u32, 5_001);
    let mut forms = open_forms(&db, 1, 10_000);
    forms.push(add("saga", bootstrap::SAGA_SEALED, Value::Bool(true)));
    forms.push(add("saga", bootstrap::SAGA_RESERVES, Value::Ref(target)));
    let prepared = prepare(&db, forms, tx(1), 1_000).expect("a sealed saga opens");
    let entity = prepared.tempids["saga"];
    let db = db.with_transaction(1, &prepared.datoms);

    let error = prepare(
        &db,
        vec![add_to(entity, bootstrap::SAGA_RESERVES, Value::Ref(later))],
        tx(2),
        1_000,
    )
    .expect_err("a sealed set is fixed");
    assert!(matches!(violation(error), SagaViolation::SealedReservation));
}

#[test]
fn an_unsealed_saga_extends_its_reservation_set() {
    let db = fixture();
    let (db, entity) = opened(&db, 1);
    let later = EntityId::new(Partition::User as u32, 5_001);
    let prepared = prepare(
        &db,
        vec![add_to(entity, bootstrap::SAGA_RESERVES, Value::Ref(later))],
        tx(2),
        1_000,
    )
    .expect("an unsealed set grows by ordinary transaction");
    let db = db.with_transaction(2, &prepared.datoms);
    assert_eq!(
        saga::entry(&db, 1).expect("the entry survives").reserves,
        vec![later]
    );
}

#[test]
fn the_merge_record_rides_with_the_commit_flip() {
    let db = fixture();
    let (db, entity) = opened(&db, 1);
    let error = prepare(
        &db,
        vec![add_to(entity, bootstrap::SAGA_STEPS, Value::Long(3))],
        tx(2),
        1_000,
    )
    .expect_err("the merge record needs the flip");
    assert!(matches!(
        violation(error),
        SagaViolation::MergeRecordWithoutCommit(_)
    ));
}

#[test]
fn entity_id_grants_are_not_transaction_data() {
    let db = fixture();
    let (db, entity) = opened(&db, 1);
    let grant = EntityId::new(Partition::User as u32, 6_000);
    let error = prepare(
        &db,
        vec![
            add_to(entity, bootstrap::SAGA_ID_GRANTS, Value::Ref(grant)),
            add_to(grant, bootstrap::SAGA_GRANT_PARTITION, Value::Long(3)),
        ],
        tx(2),
        1_000,
    )
    .expect_err("grants are leased, not asserted");
    assert!(matches!(violation(error), SagaViolation::GrantNotLeased));
}

/// Grants are minted by the transactor, not by transaction data, so a test
/// that needs a live grant has to write one the way the allocator does:
/// straight into the database value.
fn with_grant(db: &Db, saga: EntityId, start: u64, length: u64) -> (Db, EntityId) {
    let grant = EntityId::new(Partition::User as u32, 5_000);
    let datom = |e: EntityId, a: EntityId, v: Value| corium_core::Datom {
        e,
        a,
        v,
        tx: tx(2),
        added: true,
    };
    let db = db.with_transaction(
        2,
        &[
            datom(saga, bootstrap::SAGA_ID_GRANTS, Value::Ref(grant)),
            datom(
                grant,
                bootstrap::SAGA_GRANT_PARTITION,
                Value::Long(i64::from(Partition::User as u32)),
            ),
            datom(
                grant,
                bootstrap::SAGA_GRANT_START,
                Value::Long(i64::try_from(start).expect("a start")),
            ),
            datom(
                grant,
                bootstrap::SAGA_GRANT_LENGTH,
                Value::Long(i64::try_from(length).expect("a length")),
            ),
        ],
    );
    (db, EntityId::new(Partition::User as u32, start + 3))
}

#[test]
fn an_open_sagas_leased_ids_are_refused_to_everyone_else() {
    let db = fixture();
    let (db, saga) = opened(&db, 1);
    let (db, leased) = with_grant(&db, saga, 8_000, 1_000);

    // Naming a leased id directly is the one way a parent write could land
    // on top of allocations the branch has been promised but not yet spent.
    let error = prepare(
        &db,
        vec![add_to(leased, NOTE, Value::Str(Arc::from("mine")))],
        tx(3),
        1_000,
    )
    .expect_err("leased ids are the allocator's promise");
    assert!(matches!(violation(error), SagaViolation::GrantedId(e) if e == leased));

    // So is referencing one: a ref value would attach parent data to an
    // entity the branch has not created yet.
    let error = prepare(
        &db,
        vec![add_to(
            EntityId::new(Partition::User as u32, 1_100),
            bootstrap::SAGA_ON_ABORT_FN,
            Value::Ref(leased),
        )],
        tx(3),
        1_000,
    )
    .expect_err("a ref into a leased block is the same write");
    assert!(matches!(violation(error), SagaViolation::GrantedId(e) if e == leased));

    // An id outside the block is nobody's business but the writer's.
    let outside = EntityId::new(Partition::User as u32, 9_000);
    prepare(
        &db,
        vec![add_to(outside, NOTE, Value::Str(Arc::from("fine")))],
        tx(3),
        1_000,
    )
    .expect("ids outside every block are ordinary");
}

#[test]
fn the_transaction_that_commits_a_saga_may_write_its_block() {
    let db = fixture();
    let (db, saga) = opened(&db, 1);
    let (db, leased) = with_grant(&db, saga, 8_000, 1_000);
    // This is the merge: branch novelty and the flip to `:committed` in one
    // append. The refusal above protects unspent leases, and this lease is
    // spent by the time the transaction ends.
    prepare(
        &db,
        vec![
            add_to(
                saga,
                bootstrap::SAGA_STATUS,
                status(&db, &SagaStatus::Committed),
            ),
            add_to(saga, bootstrap::SAGA_MERGED_TX, Value::Ref(tx(3))),
            add_to(leased, NOTE, Value::Str(Arc::from("merged"))),
        ],
        tx(3),
        1_000,
    )
    .expect("the merge carries the branch's own ids");
}

#[test]
fn the_compensation_ledger_outlives_the_saga() {
    let db = fixture();
    let (db, entity) = opened(&db, 1);
    let prepared = prepare(
        &db,
        vec![add_to(
            entity,
            bootstrap::SAGA_STATUS,
            status(&db, &SagaStatus::Aborted),
        )],
        tx(2),
        1_000,
    )
    .expect("the abort lands");
    let db = db.with_transaction(2, &prepared.datoms);

    let prepared = prepare(
        &db,
        vec![
            TxItem::Op(TxOp::Add(
                EntityRef::Id(entity),
                bootstrap::SAGA_COMPENSATIONS,
                Value::Ref(EntityId::new(Partition::User as u32, 7_000)),
            )),
            add_to(
                EntityId::new(Partition::User as u32, 7_000),
                bootstrap::SAGA_COMPENSATION_KEY,
                Value::Str(Arc::from("refund:1234")),
            ),
        ],
        tx(3),
        7_001,
    )
    .expect("an aborted saga still takes ledger entries");
    let db = db.with_transaction(3, &prepared.datoms);
    let entry = saga::entry(&db, 1).expect("the entry survives");
    assert_eq!(entry.status, Some(SagaStatus::Aborted));
    assert_eq!(entry.compensations.len(), 1);
    assert_eq!(entry.compensations[0].key.as_deref(), Some("refund:1234"));
}

#[test]
fn ordinary_data_is_untouched_by_the_registry_checks() {
    let db = fixture();
    let prepared = prepare(
        &db,
        vec![TxItem::Op(TxOp::Add(
            EntityRef::Temp("note".into()),
            bootstrap::DOC,
            Value::Str(Arc::from("not a saga")),
        ))],
        tx(1),
        1_000,
    );
    // `:db/doc` is schema vocabulary, so this fails for its own reason — the
    // point is that it is not a saga violation.
    assert!(!matches!(prepared, Err(TxError::Saga(_))));
}
