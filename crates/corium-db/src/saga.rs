//! The saga registry as a read model over a database value (ADR-0023).
//!
//! A saga is an entity in the parent database carrying the `:db.saga/*`
//! vocabulary [`crate::bootstrap`] installs: its id, status, opening basis,
//! owner, deadline, declared footprint and reserved set, the entity-id blocks
//! leased to its branch, and — once it finishes — how it finished. Opening,
//! extending, and finishing a saga are ordinary transactions over those
//! attributes, so *reading* the registry is ordinary querying: everything here
//! is a convenience over [`Db::values`] and [`Db::lookup`], not a privileged
//! path, and a caller who would rather write Datalog is not missing anything.
//!
//! The read model exists because three surfaces need the same fold — the CLI's
//! `saga status`, the `corium_sys.sagas` SQL relations, and the transition
//! checks `corium-tx` applies before a registry write commits — and each
//! deriving it from raw datoms would be three chances to disagree about, say,
//! whether a saga with no `:db.saga/status` datom is open.
//!
//! Every read here folds *current* values, so it means what it says in a
//! current or `as-of` view — a `since` view answers "what changed", and a
//! history view holds a saga's assertions and its retractions side by side,
//! where "the status" is not a question with one answer. Callers that want the
//! transitions themselves read the datoms.
//!
//! It is deliberately total about absence. A registry entry is data written by
//! a transaction, and a transaction can be interrupted, restored from a backup
//! taken mid-flight, or (in a database whose writer predates this vocabulary)
//! never have existed at all. Every field but the id is therefore an
//! `Option`, and a status keyword outside the four the engine knows reads back
//! as [`SagaStatus::Unknown`] rather than as no status: refusing to model what
//! the database actually holds would hide exactly the entries an operator
//! opens this surface to find.

use corium_core::{AttrId, Datom, EntityId, Keyword, Value};

use crate::{Db, bootstrap};

/// Namespace shared by the status enum values (`:db.saga.status/open`).
pub const STATUS_NAMESPACE: &str = "db.saga.status";

/// Namespace shared by the compensation-ledger status values.
pub const COMPENSATION_STATUS_NAMESPACE: &str = "db.saga.compensation.status";

/// How many entity ids a branch is leased in one block.
///
/// Sequences are 42 bits wide per partition, so a block this size is a
/// rounding error in the space and comfortably more novelty than a saga is
/// expected to create; the point of leasing generously is that a branch never
/// has to interrupt a step to ask for more. Blocks belonging to sagas that
/// never merged are simply abandoned — a hole in a sequence, which the
/// allocator has always been free to leave.
pub const DEFAULT_ID_BLOCK: i64 = 1 << 20;

/// Separator between a parent database's name and its saga branches.
///
/// A dot is not legal in a database name, which is the point: the branch
/// namespace cannot collide with a database somebody creates, so a branch is
/// never listed, never stood by for, and never created by name.
/// What separates a parent database's name from the saga id in a branch
/// name. Public so a store scan can build the prefix branches share.
pub const BRANCH_INFIX: &str = ".saga.";

/// The name a saga's branch is hosted under.
///
/// Branch naming lives beside the registry because it is derived from it: a
/// saga's id names its branch, and every surface that can read the registry
/// can address the branch without being told where it is.
#[must_use]
pub fn branch_name(parent: &str, saga: u128) -> String {
    format!("{parent}{BRANCH_INFIX}{saga:032x}")
}

/// The parent database and saga id a branch name carries.
#[must_use]
pub fn parse_branch_name(name: &str) -> Option<(&str, u128)> {
    let (parent, id) = name.rsplit_once(BRANCH_INFIX)?;
    if parent.is_empty() || id.len() != 32 {
        return None;
    }
    Some((parent, u128::from_str_radix(id, 16).ok()?))
}

/// Whether `name` names a saga branch rather than a database.
#[must_use]
pub fn is_branch_name(name: &str) -> bool {
    parse_branch_name(name).is_some()
}

/// Lifecycle state of a saga.
///
/// The three terminal states are distinct on purpose: `:aborted` is a
/// decision, `:expired` is the system ending abandoned work, and a returning
/// owner can tell which happened to them.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SagaStatus {
    /// `:db.saga.status/open` — steps may still be transacted.
    Open,
    /// `:db.saga.status/committed` — the branch merged into the parent.
    Committed,
    /// `:db.saga.status/aborted` — the owner ended it.
    Aborted,
    /// `:db.saga.status/expired` — the deadline passed, or the branch was
    /// gone when the database opened.
    Expired,
    /// A status keyword the engine does not define, kept verbatim.
    Unknown(Keyword),
}

impl SagaStatus {
    /// The keyword this status is stored as.
    #[must_use]
    pub fn keyword(&self) -> Keyword {
        match self {
            Self::Unknown(keyword) => keyword.clone(),
            other => Keyword::new(Some(STATUS_NAMESPACE), other.name()),
        }
    }

    /// The status a keyword names, with unknown keywords preserved.
    #[must_use]
    pub fn from_keyword(keyword: &Keyword) -> Self {
        if keyword.namespace.as_deref() == Some(STATUS_NAMESPACE) {
            match keyword.name.as_str() {
                "open" => return Self::Open,
                "committed" => return Self::Committed,
                "aborted" => return Self::Aborted,
                "expired" => return Self::Expired,
                _ => {}
            }
        }
        Self::Unknown(keyword.clone())
    }

    /// The bare name, as prose abbreviates it (`open`, `committed`, …).
    ///
    /// An unknown status renders as its whole keyword, since its name alone
    /// would misrepresent it as one of the engine's own.
    #[must_use]
    pub fn name(&self) -> &str {
        match self {
            Self::Open => "open",
            Self::Committed => "committed",
            Self::Aborted => "aborted",
            Self::Expired => "expired",
            Self::Unknown(keyword) => keyword.name.as_str(),
        }
    }

    /// Whether the saga has finished: no transition leaves a terminal state.
    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Committed | Self::Aborted | Self::Expired)
    }

    /// Whether `self` may transition to `next`.
    ///
    /// Open is the only state with successors, and it has exactly the three
    /// terminal ones. A status the engine does not define is final in both
    /// directions: it cannot be written, because the engine would not know
    /// what it promised, and it is not written over, because it cannot know
    /// what the entry it found means.
    #[must_use]
    pub fn may_become(&self, next: &Self) -> bool {
        matches!(self, Self::Open) && next.is_terminal()
    }
}

impl std::fmt::Display for SagaStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.keyword())
    }
}

/// One entity-id block leased to a saga's branch.
///
/// Branch allocations survive the merge verbatim, so the block is the
/// parent allocator's promise that nothing else will claim those ids.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IdGrant {
    /// The grant's own (component) entity.
    pub entity: EntityId,
    /// Partition the block belongs to.
    pub partition: Option<i64>,
    /// First sequence number in the block.
    pub start: Option<i64>,
    /// How many sequence numbers the block covers.
    pub length: Option<i64>,
}

impl IdGrant {
    /// One past the last sequence in the block, when it is fully recorded.
    #[must_use]
    pub fn end(&self) -> Option<i64> {
        self.start
            .zip(self.length)
            .and_then(|(start, length)| start.checked_add(length))
    }

    /// Whether `sequence` in `partition` falls inside this block.
    #[must_use]
    pub fn contains(&self, partition: i64, sequence: i64) -> bool {
        self.partition == Some(partition)
            && self.start.is_some_and(|start| sequence >= start)
            && self.end().is_some_and(|end| sequence < end)
    }

    /// Whether `entity` falls inside this block.
    #[must_use]
    pub fn holds(&self, entity: EntityId) -> bool {
        let Ok(sequence) = i64::try_from(entity.sequence()) else {
            return false;
        };
        self.contains(i64::from(entity.partition()), sequence)
    }
}

/// One entry in a saga's external-compensation ledger.
///
/// The engine never executes these; they are the orchestrator's durable
/// bookkeeping about reverse progress it performed outside the database.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Compensation {
    /// The entry's own (component) entity.
    pub entity: EntityId,
    /// Application key naming the external effect being compensated.
    pub key: Option<String>,
    /// Orchestrator-defined status keyword.
    pub status: Option<Keyword>,
    /// Free-form detail (EDN or text) the orchestrator recorded.
    pub detail: Option<String>,
    /// When the compensation finished, if it did.
    pub completed_at: Option<i64>,
    /// Why it failed, if it failed.
    pub error: Option<String>,
}

/// A saga registry entry, folded from the facts on one saga entity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SagaEntry {
    /// The saga entity in the parent database.
    pub entity: EntityId,
    /// `:db.saga/id` — the saga's name on every surface.
    pub id: u128,
    /// Current lifecycle state.
    pub status: Option<SagaStatus>,
    /// Parent basis the branch is rooted at.
    pub basis_t: Option<i64>,
    /// Human-readable purpose.
    pub description: Option<String>,
    /// Authenticated principal that opened it.
    pub owner: Option<String>,
    /// Deadline, extendable while open.
    pub expires_at: Option<i64>,
    /// Entity-id blocks leased to the branch.
    pub grants: Vec<IdGrant>,
    /// Advisory declared touch-set.
    pub footprint: Vec<EntityId>,
    /// Checked reservation set.
    pub reserves: Vec<EntityId>,
    /// Whether the reservation set is fixed at open.
    pub sealed: bool,
    /// The merge transaction, once committed.
    pub merged_tx: Option<EntityId>,
    /// Branch transactions squashed by the merge.
    pub steps: Option<i64>,
    /// EDN report from the latest failed merge attempt.
    pub conflict_report: Option<String>,
    /// Static compensating transaction data (EDN).
    pub on_abort_tx: Option<String>,
    /// `:db/fn` entity invoked as the compensating transaction.
    pub on_abort_fn: Option<EntityId>,
    /// Why a system-time compensation did not land.
    pub on_abort_error: Option<String>,
    /// External-compensation ledger, ordered by entity id.
    pub compensations: Vec<Compensation>,
}

impl SagaEntry {
    /// Whether the saga is open (an entry with no status is not).
    #[must_use]
    pub fn is_open(&self) -> bool {
        matches!(self.status, Some(SagaStatus::Open))
    }

    /// Whether an open saga's deadline has passed at `now` (epoch millis).
    ///
    /// An open saga with no deadline is overdue at any instant: expiry is
    /// mandatory, so a missing one is a registry entry the sweep must end,
    /// not a saga with permission to run forever.
    #[must_use]
    pub fn is_overdue(&self, now: i64) -> bool {
        self.is_open() && self.expires_at.is_none_or(|expires_at| expires_at <= now)
    }
}

/// Whether `a` belongs to the saga registry vocabulary.
///
/// Covers the saga entity's own attributes and those of its component
/// children, since a write to a grant or ledger entry is a registry write
/// wherever the entity sits.
#[must_use]
pub fn is_registry_attribute(a: AttrId) -> bool {
    is_saga_attribute(a) || is_grant_attribute(a) || is_compensation_attribute(a)
}

/// Whether `a` is an attribute of the saga entity itself.
#[must_use]
pub fn is_saga_attribute(a: AttrId) -> bool {
    matches!(
        a,
        bootstrap::SAGA_ID
            | bootstrap::SAGA_STATUS
            | bootstrap::SAGA_BASIS_T
            | bootstrap::SAGA_DESCRIPTION
            | bootstrap::SAGA_OWNER
            | bootstrap::SAGA_EXPIRES_AT
            | bootstrap::SAGA_ID_GRANTS
            | bootstrap::SAGA_FOOTPRINT
            | bootstrap::SAGA_RESERVES
            | bootstrap::SAGA_SEALED
            | bootstrap::SAGA_MERGED_TX
            | bootstrap::SAGA_STEPS
            | bootstrap::SAGA_CONFLICT_REPORT
            | bootstrap::SAGA_ON_ABORT_TX
            | bootstrap::SAGA_ON_ABORT_FN
            | bootstrap::SAGA_ON_ABORT_ERROR
            | bootstrap::SAGA_COMPENSATIONS
    )
}

/// Whether `a` is an attribute of an entity-id grant.
#[must_use]
pub fn is_grant_attribute(a: AttrId) -> bool {
    matches!(
        a,
        bootstrap::SAGA_GRANT_PARTITION
            | bootstrap::SAGA_GRANT_START
            | bootstrap::SAGA_GRANT_LENGTH
    )
}

/// Whether `a` is an attribute of a compensation-ledger entry.
#[must_use]
pub fn is_compensation_attribute(a: AttrId) -> bool {
    matches!(
        a,
        bootstrap::SAGA_COMPENSATION_KEY
            | bootstrap::SAGA_COMPENSATION_STATUS
            | bootstrap::SAGA_COMPENSATION_DETAIL
            | bootstrap::SAGA_COMPENSATION_COMPLETED_AT
            | bootstrap::SAGA_COMPENSATION_ERROR
    )
}

/// The entity carrying `id`, if this database has that saga.
#[must_use]
pub fn entity(db: &Db, id: u128) -> Option<EntityId> {
    db.lookup(bootstrap::SAGA_ID, &Value::Uuid(id))
}

/// The registry entry for `id`.
#[must_use]
pub fn entry(db: &Db, id: u128) -> Option<SagaEntry> {
    entity(db, id).and_then(|entity| entry_at(db, entity))
}

/// The registry entry on `entity`, if it is a saga.
#[must_use]
pub fn entry_at(db: &Db, entity: EntityId) -> Option<SagaEntry> {
    let id = field(db, entity, bootstrap::SAGA_ID, uuid)?;
    Some(SagaEntry {
        entity,
        id,
        status: field(db, entity, bootstrap::SAGA_STATUS, |value| {
            keyword(db, value)
        })
        .map(|keyword| SagaStatus::from_keyword(&keyword)),
        basis_t: field(db, entity, bootstrap::SAGA_BASIS_T, long),
        description: field(db, entity, bootstrap::SAGA_DESCRIPTION, text),
        owner: field(db, entity, bootstrap::SAGA_OWNER, text),
        expires_at: field(db, entity, bootstrap::SAGA_EXPIRES_AT, instant),
        grants: refs(db, entity, bootstrap::SAGA_ID_GRANTS)
            .into_iter()
            .map(|grant| grant_at(db, grant))
            .collect(),
        footprint: refs(db, entity, bootstrap::SAGA_FOOTPRINT),
        reserves: refs(db, entity, bootstrap::SAGA_RESERVES),
        sealed: field(db, entity, bootstrap::SAGA_SEALED, boolean).unwrap_or(false),
        merged_tx: field(db, entity, bootstrap::SAGA_MERGED_TX, reference),
        steps: field(db, entity, bootstrap::SAGA_STEPS, long),
        conflict_report: field(db, entity, bootstrap::SAGA_CONFLICT_REPORT, text),
        on_abort_tx: field(db, entity, bootstrap::SAGA_ON_ABORT_TX, text),
        on_abort_fn: field(db, entity, bootstrap::SAGA_ON_ABORT_FN, reference),
        on_abort_error: field(db, entity, bootstrap::SAGA_ON_ABORT_ERROR, text),
        compensations: refs(db, entity, bootstrap::SAGA_COMPENSATIONS)
            .into_iter()
            .map(|entry| compensation_at(db, entry))
            .collect(),
    })
}

/// Every registry entry in `db`, ordered by saga entity.
#[must_use]
pub fn entries(db: &Db) -> Vec<SagaEntry> {
    let mut entities: Vec<EntityId> = db
        .datoms_for_attribute(bootstrap::SAGA_ID)
        .map(|datom| datom.e)
        .collect();
    entities.sort_unstable();
    entities.dedup();
    entities
        .into_iter()
        .filter_map(|entity| entry_at(db, entity))
        .collect()
}

/// Every open saga, ordered by saga entity.
#[must_use]
pub fn open_entries(db: &Db) -> Vec<SagaEntry> {
    entries(db).into_iter().filter(SagaEntry::is_open).collect()
}

/// Every open saga whose deadline has passed at `now` — the sweep's input.
#[must_use]
pub fn overdue_entries(db: &Db, now: i64) -> Vec<SagaEntry> {
    entries(db)
        .into_iter()
        .filter(|entry| entry.is_overdue(now))
        .collect()
}

/// Open sagas that declare `entity` in their footprint or reserved set.
///
/// The two answers this returns differ in kind, and callers must keep them
/// apart: a footprint hit is advisory (the saga said it would touch this),
/// while a reservation hit is checked (the saga *can* only touch this and
/// its other reserved entities). Neither absence means "untouched" unless
/// the saga reserves.
#[must_use]
pub fn declaring(db: &Db, entity: EntityId) -> Vec<SagaEntry> {
    open_entries(db)
        .into_iter()
        .filter(|saga| saga.footprint.contains(&entity) || saga.reserves.contains(&entity))
        .collect()
}

/// Every entity-id block the registry records, whatever the saga's state.
///
/// The allocator reads this and not just the open sagas' blocks, because a
/// block that has been leased is spent: a committed saga's ids are live
/// entities, and an abandoned one's are a hole the allocator must step over
/// rather than a range it may reissue.
#[must_use]
pub fn grants(db: &Db) -> Vec<IdGrant> {
    let mut entities: Vec<EntityId> = db
        .datoms_for_attribute(bootstrap::SAGA_GRANT_START)
        .map(|datom| datom.e)
        .collect();
    entities.sort_unstable();
    entities.dedup();
    entities
        .into_iter()
        .map(|entity| grant_at(db, entity))
        .collect()
}

/// The blocks leased to sagas that are still open — the ranges an ordinary
/// parent transaction may not name an entity in.
///
/// The restriction lifts when the saga finishes, and it must: a committed
/// saga's ids are ordinary entities in the parent afterwards, and writing
/// them is ordinary work.
#[must_use]
pub fn live_grants(db: &Db) -> Vec<IdGrant> {
    open_entries(db)
        .into_iter()
        .flat_map(|entry| entry.grants)
        .collect()
}

/// The first sequence in `partition` no leased block covers.
///
/// This is the floor the parent's allocator resumes from after a restart.
/// Nothing in the parent's datoms records a leased-but-unused block, so an
/// allocator that trusted only the ids it can see would hand out the very
/// range it promised a branch.
#[must_use]
pub fn grant_ceiling(db: &Db, partition: u32) -> u64 {
    grants(db)
        .into_iter()
        .filter(|grant| grant.partition == Some(i64::from(partition)))
        .filter_map(|grant| grant.end())
        .map(|end| u64::try_from(end).unwrap_or(0))
        .max()
        .unwrap_or(0)
}

fn grant_at(db: &Db, entity: EntityId) -> IdGrant {
    IdGrant {
        entity,
        partition: field(db, entity, bootstrap::SAGA_GRANT_PARTITION, long),
        start: field(db, entity, bootstrap::SAGA_GRANT_START, long),
        length: field(db, entity, bootstrap::SAGA_GRANT_LENGTH, long),
    }
}

fn compensation_at(db: &Db, entity: EntityId) -> Compensation {
    Compensation {
        entity,
        key: field(db, entity, bootstrap::SAGA_COMPENSATION_KEY, text),
        status: field(db, entity, bootstrap::SAGA_COMPENSATION_STATUS, |value| {
            keyword(db, value)
        }),
        detail: field(db, entity, bootstrap::SAGA_COMPENSATION_DETAIL, text),
        completed_at: field(
            db,
            entity,
            bootstrap::SAGA_COMPENSATION_COMPLETED_AT,
            instant,
        ),
        error: field(db, entity, bootstrap::SAGA_COMPENSATION_ERROR, text),
    }
}

/// The single current value of `a` on `e`, read through `parse`.
///
/// Every registry field is cardinality one, so a second value would be a
/// database that contradicts itself; taking the first keeps this total in
/// the face of one, rather than deciding which contradiction to believe.
fn field<T>(db: &Db, e: EntityId, a: AttrId, parse: impl Fn(&Value) -> Option<T>) -> Option<T> {
    db.values(e, a).first().and_then(parse)
}

/// Every ref value of `a` on `e`, ordered by entity id.
fn refs(db: &Db, e: EntityId, a: AttrId) -> Vec<EntityId> {
    let mut values: Vec<EntityId> = db.values(e, a).iter().filter_map(reference).collect();
    values.sort_unstable();
    values
}

fn uuid(value: &Value) -> Option<u128> {
    match value {
        Value::Uuid(id) => Some(*id),
        _ => None,
    }
}

fn long(value: &Value) -> Option<i64> {
    match value {
        Value::Long(number) => Some(*number),
        _ => None,
    }
}

fn instant(value: &Value) -> Option<i64> {
    match value {
        Value::Instant(millis) => Some(*millis),
        _ => None,
    }
}

fn text(value: &Value) -> Option<String> {
    match value {
        Value::Str(text) => Some(text.to_string()),
        _ => None,
    }
}

fn boolean(value: &Value) -> Option<bool> {
    match value {
        Value::Bool(flag) => Some(*flag),
        _ => None,
    }
}

fn reference(value: &Value) -> Option<EntityId> {
    match value {
        Value::Ref(entity) => Some(*entity),
        _ => None,
    }
}

fn keyword(db: &Db, value: &Value) -> Option<Keyword> {
    match value {
        Value::Keyword(id) => db.interner().resolve(*id),
        _ => None,
    }
}

/// The status a run of datoms leaves on a saga entity, if they change it.
///
/// Cardinality-one assertions arrive as a retraction of the old value and an
/// assertion of the new one, so the assertion is what the transaction leaves
/// behind; a retraction with no matching assertion clears the status, which
/// [`crate::saga`]'s callers treat as its own kind of change.
#[must_use]
pub fn asserted_status(db: &Db, e: EntityId, datoms: &[Datom]) -> Option<SagaStatus> {
    datoms
        .iter()
        .rfind(|datom| datom.e == e && datom.a == bootstrap::SAGA_STATUS && datom.added)
        .and_then(|datom| keyword(db, &datom.v))
        .map(|keyword| SagaStatus::from_keyword(&keyword))
}

#[cfg(test)]
mod tests {
    use super::*;
    use corium_core::{KeywordInterner, Partition, Schema};

    fn db() -> Db {
        let mut schema = Schema::default();
        let mut idents = crate::Idents::default();
        bootstrap::install(&mut schema, &mut idents);
        Db::new(schema).with_naming(idents, KeywordInterner::default())
    }

    fn entity_id(sequence: u64) -> EntityId {
        EntityId::new(Partition::User as u32, sequence)
    }

    fn datom(e: EntityId, a: AttrId, v: Value) -> Datom {
        Datom {
            e,
            a,
            v,
            tx: EntityId::new(Partition::Tx as u32, 1),
            added: true,
        }
    }

    #[test]
    fn status_keywords_round_trip() {
        for status in [
            SagaStatus::Open,
            SagaStatus::Committed,
            SagaStatus::Aborted,
            SagaStatus::Expired,
        ] {
            assert_eq!(SagaStatus::from_keyword(&status.keyword()), status);
            assert_eq!(
                status.keyword().namespace.as_deref(),
                Some(STATUS_NAMESPACE)
            );
        }
        let odd = Keyword::new(Some("db.saga.status"), "paused");
        assert_eq!(
            SagaStatus::from_keyword(&odd),
            SagaStatus::Unknown(odd.clone())
        );
        let unknown = SagaStatus::from_keyword(&odd);
        assert!(!unknown.is_terminal());
        assert!(!SagaStatus::Open.may_become(&unknown));
        assert!(!unknown.may_become(&SagaStatus::Aborted));
        assert_eq!(unknown.to_string(), ":db.saga.status/paused");
    }

    #[test]
    fn open_is_the_only_state_with_successors() {
        assert!(SagaStatus::Open.may_become(&SagaStatus::Committed));
        assert!(SagaStatus::Open.may_become(&SagaStatus::Aborted));
        assert!(SagaStatus::Open.may_become(&SagaStatus::Expired));
        assert!(!SagaStatus::Open.may_become(&SagaStatus::Open));
        assert!(!SagaStatus::Aborted.may_become(&SagaStatus::Expired));
        assert!(!SagaStatus::Committed.may_become(&SagaStatus::Aborted));
    }

    #[test]
    fn an_entry_folds_the_facts_on_its_entity() {
        let mut db = db();
        let mut interner = db.interner().clone();
        let open = interner.intern(SagaStatus::Open.keyword());
        let idents = db.idents().clone();
        db = db.with_naming(idents, interner);
        let saga = entity_id(1_000);
        let grant = entity_id(1_001);
        let target = entity_id(2_000);
        db = db.with_transaction(
            1,
            &[
                datom(saga, bootstrap::SAGA_ID, Value::Uuid(7)),
                datom(saga, bootstrap::SAGA_STATUS, Value::Keyword(open)),
                datom(saga, bootstrap::SAGA_BASIS_T, Value::Long(42)),
                datom(saga, bootstrap::SAGA_OWNER, Value::Str("alice".into())),
                datom(saga, bootstrap::SAGA_EXPIRES_AT, Value::Instant(1_000)),
                datom(saga, bootstrap::SAGA_RESERVES, Value::Ref(target)),
                datom(saga, bootstrap::SAGA_SEALED, Value::Bool(true)),
                datom(saga, bootstrap::SAGA_ID_GRANTS, Value::Ref(grant)),
                datom(grant, bootstrap::SAGA_GRANT_PARTITION, Value::Long(3)),
                datom(grant, bootstrap::SAGA_GRANT_START, Value::Long(500)),
                datom(grant, bootstrap::SAGA_GRANT_LENGTH, Value::Long(100)),
            ],
        );

        let entry = entry(&db, 7).expect("the saga is in the registry");
        assert_eq!(entry.entity, saga);
        assert_eq!(entry.status, Some(SagaStatus::Open));
        assert_eq!(entry.basis_t, Some(42));
        assert_eq!(entry.owner.as_deref(), Some("alice"));
        assert_eq!(entry.reserves, vec![target]);
        assert!(entry.sealed);
        assert_eq!(entry.grants.len(), 1);
        assert!(entry.grants[0].contains(3, 599));
        assert!(!entry.grants[0].contains(3, 600));
        assert!(!entry.grants[0].contains(4, 550));
        assert!(entry.is_open());
        assert!(entry.is_overdue(1_000));
        assert!(!entry.is_overdue(999));
        assert_eq!(declaring(&db, target), vec![entry.clone()]);
        assert!(declaring(&db, entity_id(2_001)).is_empty());
        assert_eq!(entries(&db), vec![entry]);
        assert_eq!(entry_at(&db, target), None);
    }

    #[test]
    fn an_open_saga_without_a_deadline_is_always_overdue() {
        let mut db = db();
        let mut interner = db.interner().clone();
        let open = interner.intern(SagaStatus::Open.keyword());
        let idents = db.idents().clone();
        db = db.with_naming(idents, interner);
        let saga = entity_id(1_000);
        db = db.with_transaction(
            1,
            &[
                datom(saga, bootstrap::SAGA_ID, Value::Uuid(1)),
                datom(saga, bootstrap::SAGA_STATUS, Value::Keyword(open)),
            ],
        );
        assert_eq!(overdue_entries(&db, 0), entries(&db));
    }

    #[test]
    fn a_branch_name_carries_its_parent_and_saga() {
        let name = branch_name("orders", 0x1234);
        assert_eq!(name, "orders.saga.00000000000000000000000000001234");
        assert_eq!(parse_branch_name(&name), Some(("orders", 0x1234)));
        assert!(is_branch_name(&name));
        // A database name is never a branch name: dots are not legal in one.
        assert_eq!(parse_branch_name("orders"), None);
        assert_eq!(parse_branch_name("orders.saga.short"), None);
        assert_eq!(
            parse_branch_name(".saga.00000000000000000000000000001234"),
            None
        );
        assert!(!is_branch_name("orders_saga_1234"));
    }

    #[test]
    fn registry_attributes_are_recognized_by_group() {
        assert!(is_saga_attribute(bootstrap::SAGA_ID));
        assert!(is_grant_attribute(bootstrap::SAGA_GRANT_START));
        assert!(is_compensation_attribute(
            bootstrap::SAGA_COMPENSATION_ERROR
        ));
        // `:db.saga/guard` is step metadata written on a branch transaction
        // entity, not registry data on the saga entity.
        assert!(!is_registry_attribute(bootstrap::SAGA_GUARD));
        assert!(!is_registry_attribute(bootstrap::TX_INSTANT));
    }
}
