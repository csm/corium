//! What a saga branch merges into its parent, and what stops it (ADR-0023).
//!
//! The merge is the moment a saga's isolation ends, and it is specified in the
//! image of ADR-0020's plan/apply: work observed optimistically at a basis,
//! re-validated inside the single-writer path against the parent's *current*
//! value, with drift surfaced rather than absorbed. This module is the pure
//! half of that — the arithmetic, with no log, no lease, and no transaction.
//!
//! Three ideas carry the whole thing:
//!
//! * **Effects, not inputs.** [`squash`] reduces a run of transactions to the
//!   net facts it left behind. The saga's owner ran those steps against branch
//!   state, watched the results, and may have acted outside the database on
//!   the strength of them; re-running the original forms against a parent that
//!   has moved could commit something nobody ever saw. So the merge replays
//!   what happened, and where the parent has moved in a way that matters it
//!   fails loudly instead of recomputing quietly.
//! * **Both sides squash.** The parent's own novelty since `t₀` is folded by
//!   the same function, so the scan compares two net effects rather than a net
//!   effect against a change log. That is what makes convergence — the parent
//!   independently arriving at the value the branch wants — a non-event: there
//!   is nothing for an owner to decide when both sides ask for the same end
//!   state, and reporting it would be a conflict report nobody can act on.
//! * **A conflict names a unit an owner can answer.** A cardinality-one
//!   `(e, a)` is one unit however many datoms the update took, because "the
//!   branch means this pair to hold `v`" is the sentence a resolution replies
//!   to. A cardinality-many attribute is one unit per fact, because that is
//!   what a set-valued attribute means — different members union, and only a
//!   single member is ever in question.
//!
//! What is *not* here: guards (they need a query engine and the EDN they are
//! written in), the conflict report's rendering, and the transaction itself.
//! Those live in the transactor, which is the only place a merge can actually
//! happen.

use std::collections::{BTreeMap, BTreeSet};

use corium_core::{AttrId, Cardinality, Datom, EntityId, IndexOrder, Partition, Value};
use corium_db::{Db, key_prefix};

/// One fact, named the way both sides of a merge name it.
pub type Fact = (EntityId, AttrId, Value);

/// The net effect of a run of transactions on the state it started from.
///
/// "Net" is the whole point: a fact asserted and later retracted within the
/// run never happened as far as the merge is concerned, and a cardinality-one
/// attribute written five times contributes one assertion and (at most) one
/// retraction of whatever the run started with.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Novelty {
    /// Facts the run left asserted that its starting state did not hold.
    pub asserts: BTreeSet<Fact>,
    /// Facts the run retracted that its starting state did hold.
    pub retracts: BTreeSet<Fact>,
    /// How many transactions the run covered — a saga's step count.
    pub steps: u64,
}

impl Novelty {
    /// Whether the run changed nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.asserts.is_empty() && self.retracts.is_empty()
    }

    /// How many net facts the run leaves to apply.
    #[must_use]
    pub fn len(&self) -> usize {
        self.asserts.len() + self.retracts.len()
    }

    /// Every net fact with the direction it goes in.
    pub fn facts(&self) -> impl Iterator<Item = (&Fact, bool)> {
        self.retracts
            .iter()
            .map(|fact| (fact, false))
            .chain(self.asserts.iter().map(|fact| (fact, true)))
    }
}

/// Folds a run of transactions into the net effect it had on its starting
/// state.
///
/// `datoms` must be every datom the run recorded, in transaction order. The
/// fold needs no access to the starting state, because the recorded datoms
/// already say what it held: a transaction records an assertion only when the
/// fact was absent and a retraction only when it was present, so the *first*
/// thing the run said about a fact reveals what it started as and the *last*
/// says what it ends as. A fact whose first and last words disagree is one the
/// run put back the way it found it.
///
/// Transaction entities are excluded. A step's own `:db/txInstant`, its
/// metadata, and its `:db.saga/guard` declarations belong to the branch's
/// timeline and never merge — squashing rewrites every merged datom onto the
/// single parent transaction, so a step's transaction entity would have
/// nothing left to describe. The step grain stays queryable in the retained
/// branch, which is exactly where the design points auditors.
pub fn squash<'a>(datoms: impl IntoIterator<Item = &'a Datom>) -> Novelty {
    let mut first: BTreeMap<Fact, bool> = BTreeMap::new();
    let mut last: BTreeMap<Fact, bool> = BTreeMap::new();
    let mut transactions: BTreeSet<u64> = BTreeSet::new();
    for datom in datoms {
        transactions.insert(datom.tx.sequence());
        if datom.e.partition() == Partition::Tx as u32 {
            continue;
        }
        let fact = (datom.e, datom.a, datom.v.clone());
        first.entry(fact.clone()).or_insert(datom.added);
        last.insert(fact, datom.added);
    }
    let mut novelty = Novelty {
        steps: transactions.len() as u64,
        ..Novelty::default()
    };
    for (fact, added) in last {
        if first.get(&fact) != Some(&added) {
            continue;
        }
        if added {
            novelty.asserts.insert(fact);
        } else {
            novelty.retracts.insert(fact);
        }
    }
    novelty
}

/// What a conflict — and the resolution answering it — is *about*.
///
/// The distinction is the schema's, not a convenience: a cardinality-one pair
/// holds one value, so the branch and the parent each have exactly one thing
/// to say about it; a cardinality-many pair holds a set, where the two sides
/// only ever disagree one member at a time.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Scope {
    /// A cardinality-one `(e, a)`: the pair as a whole.
    Pair,
    /// A cardinality-many `(e, a, v)`: one member of the set.
    Fact(Value),
}

/// Why a merge cannot proceed on one unit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ConflictKind {
    /// Both sides wrote the unit and they disagree about the result.
    WriteWrite,
    /// The branch asserts a unique value the parent has since given away.
    Uniqueness {
        /// The entity holding the value in the parent now.
        holder: EntityId,
    },
    /// The branch points at an entity the parent has since retracted whole.
    DanglingRef {
        /// The entity that is no longer there.
        target: EntityId,
    },
    /// The branch retracts a value that is no longer the parent's — the
    /// compare-and-swap-shaped conflict.
    RetractionMiss,
}

impl ConflictKind {
    /// The keyword a conflict report spells this kind with.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            Self::WriteWrite => "write-write",
            Self::Uniqueness { .. } => "uniqueness",
            Self::DanglingRef { .. } => "dangling-ref",
            Self::RetractionMiss => "retraction-miss",
        }
    }
}

/// One thing the parent's current value says that the branch's novelty cannot
/// be applied over.
///
/// `branch` and `parent` are what each side says the unit holds — `None` for
/// "nothing", which is a real answer and not missing data. Together they are
/// the report: the owner sees the value they observed on the branch beside the
/// value that is actually there now, which is the observation that makes
/// resolving a conflict consistent with replaying effects.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Conflict {
    /// Which class of collision this is.
    pub kind: ConflictKind,
    /// The entity the unit belongs to.
    pub entity: EntityId,
    /// The attribute the unit belongs to.
    pub attribute: AttrId,
    /// Whether the unit is a cardinality-one pair or one exact fact.
    pub scope: Scope,
    /// What the branch means the unit to hold.
    pub branch: Option<Value>,
    /// What the parent holds now.
    pub parent: Option<Value>,
    /// Whether *override* is available, which only write–write on a
    /// cardinality-one pair ever is (see [`Take::Branch`]).
    pub overridable: bool,
}

impl Conflict {
    /// The unit this conflict is about, which is what a resolution names.
    #[must_use]
    pub fn unit(&self) -> (EntityId, AttrId, Scope) {
        (self.entity, self.attribute, self.scope.clone())
    }
}

/// Which side a resolution keeps.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Take {
    /// *accept-parent* — drop the branch's write from the merge. Available
    /// for every conflict class, because it only ever removes something.
    Parent,
    /// *override* — the branch's value wins: retract what the parent holds,
    /// assert what the branch means. Available only for write–write on a
    /// cardinality-one pair, where both datoms name state the owner has seen
    /// (one on the branch, one in the report) and the write stays inside the
    /// saga's own footprint. Every other class would fabricate a write outside
    /// what was observed or outside what the saga touched.
    Branch,
}

/// An owner's answer to one conflict, fenced to the report that raised it.
///
/// `parent` is the fence and is not optional bookkeeping: a resolution names
/// the parent-side value the report showed, and holds only while that value
/// still stands. Further drift on a resolved unit is a fresh conflict, never
/// silently absorbed — which is what keeps a retry as honest as the first
/// attempt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Resolution {
    /// The entity the unit belongs to.
    pub entity: EntityId,
    /// The attribute the unit belongs to.
    pub attribute: AttrId,
    /// The unit, matching the conflict's.
    pub scope: Scope,
    /// The parent-side value the report showed — the fence.
    pub parent: Option<Value>,
    /// Which side to keep.
    pub take: Take,
}

impl Resolution {
    /// Whether this resolution answers `conflict`.
    #[must_use]
    pub fn answers(&self, conflict: &Conflict) -> bool {
        self.entity == conflict.entity
            && self.attribute == conflict.attribute
            && self.scope == conflict.scope
            && self.parent == conflict.parent
    }
}

/// Why a resolution could not be used.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ResolutionError {
    /// No conflict in the current report matches it — the report it answers
    /// is stale, or it names a unit that never collided.
    Unmatched(Resolution),
    /// It overrides a class that has no exact expansion in observed state.
    NotOverridable(Resolution),
}

/// What the scan and the resolutions between them leave to do.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Resolved {
    /// Conflicts still standing, which is what a report is made of.
    pub outstanding: Vec<Conflict>,
    /// Units to drop from the novelty entirely (*accept-parent*).
    pub accepted: BTreeSet<(EntityId, AttrId, Scope)>,
    /// Units where the branch's value wins (*override*).
    pub overridden: BTreeSet<(EntityId, AttrId, Scope)>,
    /// Resolutions that answered nothing, or asked for something no
    /// resolution may ask.
    pub rejected: Vec<ResolutionError>,
}

/// What the parent's current value must be scanned against.
#[derive(Clone, Copy, Debug)]
pub struct MergeInput<'a> {
    /// The parent's value now, inside the writer.
    pub parent: &'a Db,
    /// The branch's net effect since `t₀`.
    pub branch: &'a Novelty,
    /// The parent's own net effect since `t₀` — its drift.
    pub drift: &'a Novelty,
}

/// Scans the branch's novelty against the parent's current value, returning
/// every unit the merge cannot simply apply.
///
/// The scan is keyed by the units the *branch* wrote, so a saga that reserved
/// its entities pays for its own novelty and not for the parent's: everything
/// the parent did elsewhere is looked up and never found. That narrowing is
/// the reservation set's dividend at merge time, and it needs no separate
/// pass, because the step checks already guaranteed the branch touched nothing
/// else pre-existing. Uniqueness is the exception the design names — a unique
/// value can collide with any entity in the database — so it is looked up
/// globally.
#[must_use]
pub fn scan(input: &MergeInput<'_>) -> Vec<Conflict> {
    let schema = input.parent.schema();
    let many = |a: AttrId| {
        schema
            .get(a)
            .is_some_and(|attr| attr.cardinality == Cardinality::Many)
    };

    // What the branch means each cardinality-one pair to hold. Retractions
    // come first out of [`Novelty::facts`], so a pair the branch both cleared
    // and re-filled ends as the value it re-filled it with.
    //
    // Cardinality-many facts are deliberately absent from this: they cannot
    // race. Both sides squash from the same state at `t₀`, so a member the
    // branch adds is one that state did not hold — which means the parent had
    // nothing there to remove — and a member the branch removes is one that
    // state did hold, which the parent either still holds (the retraction
    // applies) or has already removed (both sides agree it should go). What
    // is left is the union, which is what a set-valued attribute means. The
    // exact-triple race the design names is visible only when the parent's
    // *change log* is scanned rather than its net effect, where churn that
    // ends where it began reads as a collision nobody could act on.
    let mut pairs: BTreeMap<(EntityId, AttrId), Option<Value>> = BTreeMap::new();
    for (fact, added) in input.branch.facts() {
        let (e, a, v) = fact.clone();
        if many(a) {
            continue;
        }
        if added {
            pairs.insert((e, a), Some(v));
        } else {
            pairs.entry((e, a)).or_insert(None);
        }
    }
    let drifted: BTreeSet<(EntityId, AttrId)> = input
        .drift
        .facts()
        .map(|(fact, _)| (fact.0, fact.1))
        .collect();
    // Entities the parent retracted whole. Reading it from the drift and not
    // from "has no datoms" keeps an id that never held a fact — which naming
    // one directly has always been allowed to be — from looking retracted.
    let emptied: BTreeSet<EntityId> = input
        .drift
        .retracts
        .iter()
        .map(|(e, ..)| *e)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .filter(|e| !exists(input.parent, *e))
        .collect();

    let mut conflicts = Vec::new();
    for ((e, a), branch) in &pairs {
        let parent = input.parent.values(*e, *a).into_iter().next();
        // Convergence is not a conflict: if the parent moved to exactly what
        // the branch means the pair to hold, both sides asked for the same
        // end state and there is nothing for an owner to decide.
        if !drifted.contains(&(*e, *a)) || parent == *branch {
            continue;
        }
        let kind = if branch.is_some() {
            ConflictKind::WriteWrite
        } else {
            ConflictKind::RetractionMiss
        };
        conflicts.push(Conflict {
            overridable: kind == ConflictKind::WriteWrite,
            kind,
            entity: *e,
            attribute: *a,
            scope: Scope::Pair,
            branch: branch.clone(),
            parent,
        });
    }
    for (e, a, v) in &input.branch.asserts {
        let scope = if many(*a) {
            Scope::Fact(v.clone())
        } else {
            Scope::Pair
        };
        if schema.get(*a).is_some_and(|attr| attr.unique.is_some())
            && let Some(holder) = input.parent.lookup(*a, v)
            && holder != *e
        {
            conflicts.push(Conflict {
                kind: ConflictKind::Uniqueness { holder },
                entity: *e,
                attribute: *a,
                scope: scope.clone(),
                branch: Some(v.clone()),
                parent: input.parent.values(*e, *a).into_iter().next(),
                overridable: false,
            });
        }
        // Both ends of a reference are checked. The target because a merged
        // ref into a retracted entity is a dangling pointer; the entity
        // because writing an attribute the parent never held on an entity it
        // has retracted would resurrect it through an attribute no
        // write–write scan covers.
        let target = match v {
            Value::Ref(target) if emptied.contains(target) => Some(*target),
            _ if emptied.contains(e) => Some(*e),
            _ => None,
        };
        if let Some(target) = target {
            conflicts.push(Conflict {
                kind: ConflictKind::DanglingRef { target },
                entity: *e,
                attribute: *a,
                scope,
                branch: Some(v.clone()),
                parent: None,
                overridable: false,
            });
        }
    }
    conflicts.sort_by(|left, right| {
        (left.entity, left.attribute, &left.scope, left.kind.name()).cmp(&(
            right.entity,
            right.attribute,
            &right.scope,
            right.kind.name(),
        ))
    });
    conflicts
}

/// Whether the parent still holds any fact about `e`.
fn exists(db: &Db, e: EntityId) -> bool {
    let prefix = key_prefix(IndexOrder::Eavt, Some(e), None, None);
    db.datoms_prefix(IndexOrder::Eavt, &prefix).next().is_some()
}

/// Matches `resolutions` against `conflicts`, reporting what is left.
///
/// A resolution answers every conflict on the unit it names, which is what
/// makes a unit carrying two collisions — a write–write that is also a
/// uniqueness collision, say — answerable at all. *Override* is refused
/// unless every conflict on the unit is overridable, so the more serious
/// class always wins the argument.
#[must_use]
pub fn resolve(conflicts: Vec<Conflict>, resolutions: &[Resolution]) -> Resolved {
    let mut resolved = Resolved::default();
    let mut used = vec![false; resolutions.len()];
    for conflict in conflicts {
        let Some(index) = resolutions
            .iter()
            .position(|resolution| resolution.answers(&conflict))
        else {
            resolved.outstanding.push(conflict);
            continue;
        };
        used[index] = true;
        let resolution = &resolutions[index];
        match resolution.take {
            Take::Parent => {
                resolved.accepted.insert(conflict.unit());
            }
            Take::Branch if conflict.overridable => {
                resolved.overridden.insert(conflict.unit());
            }
            Take::Branch => {
                resolved
                    .rejected
                    .push(ResolutionError::NotOverridable(resolution.clone()));
                resolved.outstanding.push(conflict);
            }
        }
    }
    // An override refused on one conflict does not survive on another: the
    // unit is either the branch's to take or it is not.
    resolved
        .overridden
        .retain(|unit| !resolved.accepted.contains(unit));
    for (index, resolution) in resolutions.iter().enumerate() {
        if !used[index] {
            resolved
                .rejected
                .push(ResolutionError::Unmatched(resolution.clone()));
        }
    }
    resolved
}

/// Applies accepted and overridden units to the branch's novelty.
///
/// *accept-parent* drops the unit entirely, which is why it is available for
/// every class: it only ever removes something from the merge. *Override*
/// drops the unit's retractions and keeps its assertion — the retraction named
/// the value the branch started from, which is no longer what the parent
/// holds, and the parent's current value is retracted by the ordinary
/// cardinality-one rule when the assertion lands.
#[must_use]
pub fn apply(novelty: &Novelty, resolved: &Resolved, parent: &Db) -> Novelty {
    let many = |a: AttrId| {
        parent
            .schema()
            .get(a)
            .is_some_and(|attr| attr.cardinality == Cardinality::Many)
    };
    let unit = |(e, a, v): &Fact| {
        (
            *e,
            *a,
            if many(*a) {
                Scope::Fact(v.clone())
            } else {
                Scope::Pair
            },
        )
    };
    Novelty {
        asserts: novelty
            .asserts
            .iter()
            .filter(|fact| !resolved.accepted.contains(&unit(fact)))
            .cloned()
            .collect(),
        retracts: novelty
            .retracts
            .iter()
            .filter(|fact| {
                let unit = unit(fact);
                !resolved.accepted.contains(&unit) && !resolved.overridden.contains(&unit)
            })
            .cloned()
            .collect(),
        steps: novelty.steps,
    }
}

/// A keyword value the branch holds under an id its own naming cannot
/// resolve.
///
/// Reaching this means the branch's metadata root and its log disagree, which
/// is corruption rather than drift; the merge refuses rather than guessing a
/// name for a value it is about to make canonical.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("branch keyword value {0} has no name in the branch's naming")]
pub struct UnnamedKeyword(pub corium_core::KwId);

/// Rewrites the branch's keyword values into the parent's naming.
///
/// A branch copies the parent's naming when it is first opened and mints its
/// own ids from there, so the two interners agree about every keyword that
/// existed then and about nothing minted since. Entity ids need no such
/// treatment — they are leased precisely so they survive the merge verbatim —
/// and attribute ids are entity ids. Keyword *values* are the one thing whose
/// meaning lives in a table rather than in the datom, so they are the one
/// thing translated: resolved through the branch's naming and interned into
/// the parent's, which mints there whatever the parent has never seen.
///
/// This is the residue of hosting a branch with a naming snapshot of its own.
/// The alternative — refusing steps that mint names — would refuse a saga the
/// ordinary right to enumerate a new status, and merging interners mid-flight
/// is not sound, because both sides allocate ids independently after the
/// snapshot.
///
/// # Errors
/// Returns [`UnnamedKeyword`] when the branch's own naming cannot resolve a
/// keyword its log holds.
pub fn translate(
    novelty: &Novelty,
    branch: &corium_core::KeywordInterner,
    parent: &mut corium_core::KeywordInterner,
) -> Result<Novelty, UnnamedKeyword> {
    let mut translate_fact = |(e, a, v): &Fact| -> Result<Fact, UnnamedKeyword> {
        let Value::Keyword(id) = v else {
            return Ok((*e, *a, v.clone()));
        };
        let keyword = branch.resolve(*id).ok_or(UnnamedKeyword(*id))?;
        Ok((*e, *a, Value::Keyword(parent.intern(keyword))))
    };
    Ok(Novelty {
        asserts: novelty
            .asserts
            .iter()
            .map(&mut translate_fact)
            .collect::<Result<_, _>>()?,
        retracts: novelty
            .retracts
            .iter()
            .map(&mut translate_fact)
            .collect::<Result<_, _>>()?,
        steps: novelty.steps,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use corium_core::{Cardinality, Schema, Unique, ValueType};
    use corium_db::attribute;

    const STATUS: AttrId = EntityId::new(Partition::Db as u32, 100);
    const TAGS: AttrId = EntityId::new(Partition::Db as u32, 101);
    const CODE: AttrId = EntityId::new(Partition::Db as u32, 102);
    const PART_OF: AttrId = EntityId::new(Partition::Db as u32, 103);

    fn db() -> Db {
        let mut schema = Schema::default();
        schema.insert(attribute(100, ValueType::Str, Cardinality::One, None));
        schema.insert(attribute(101, ValueType::Str, Cardinality::Many, None));
        schema.insert(attribute(
            102,
            ValueType::Str,
            Cardinality::One,
            Some(Unique::Identity),
        ));
        schema.insert(attribute(103, ValueType::Ref, Cardinality::One, None));
        Db::new(schema)
    }

    fn user(sequence: u64) -> EntityId {
        EntityId::new(Partition::User as u32, sequence)
    }

    fn text(value: &str) -> Value {
        Value::Str(value.into())
    }

    fn datom(t: u64, e: EntityId, a: AttrId, v: Value, added: bool) -> Datom {
        Datom {
            e,
            a,
            v,
            tx: EntityId::new(Partition::Tx as u32, t),
            added,
        }
    }

    /// Applies `datoms` to `db` transaction by transaction, the way the log
    /// would, so a test's "parent" is a value some transactions actually
    /// produced rather than one assembled by hand.
    fn applied(db: &Db, datoms: &[Datom]) -> Db {
        let mut db = db.clone();
        let mut t = db.basis_t();
        let mut batch: Vec<Datom> = Vec::new();
        for datom in datoms {
            if datom.tx.sequence() != t && !batch.is_empty() {
                db = db.with_transaction(t, &batch);
                batch.clear();
            }
            t = datom.tx.sequence();
            batch.push(datom.clone());
        }
        if !batch.is_empty() {
            db = db.with_transaction(t, &batch);
        }
        db
    }

    #[test]
    fn a_squash_keeps_what_a_run_ended_with_and_drops_what_it_took_back() {
        let order = user(1_000);
        let novelty = squash(&[
            // A cardinality-one value written twice: the intermediate value
            // is not something the parent ever needs to hear about.
            datom(11, order, STATUS, text("draft"), true),
            datom(12, order, STATUS, text("draft"), false),
            datom(12, order, STATUS, text("packed"), true),
            // Added and taken back inside the run: not novelty at all.
            datom(12, order, TAGS, text("rush"), true),
            datom(13, order, TAGS, text("rush"), false),
            // A value the run found and removed.
            datom(13, order, TAGS, text("stale"), false),
            // A step's own transaction entity stays in the branch.
            datom(
                13,
                EntityId::new(Partition::Tx as u32, 13),
                STATUS,
                text("x"),
                true,
            ),
        ]);
        assert_eq!(
            novelty.asserts,
            [(order, STATUS, text("packed"))].into_iter().collect()
        );
        assert_eq!(
            novelty.retracts,
            [(order, TAGS, text("stale"))].into_iter().collect()
        );
        assert_eq!(novelty.steps, 3, "three transactions, however many datoms");
    }

    #[test]
    fn an_untouched_parent_leaves_nothing_to_resolve() {
        let order = user(1_000);
        let parent = applied(&db(), &[datom(1, order, STATUS, text("draft"), true)]);
        let branch = squash(&[
            datom(2, order, STATUS, text("draft"), false),
            datom(2, order, STATUS, text("packed"), true),
        ]);
        let conflicts = scan(&MergeInput {
            parent: &parent,
            branch: &branch,
            drift: &Novelty::default(),
        });
        assert!(conflicts.is_empty(), "{conflicts:?}");
    }

    #[test]
    fn a_pair_both_sides_moved_is_one_conflict_with_both_values() {
        let order = user(1_000);
        let base = applied(&db(), &[datom(1, order, STATUS, text("draft"), true)]);
        let drift_datoms = [
            datom(2, order, STATUS, text("draft"), false),
            datom(2, order, STATUS, text("cancelled"), true),
        ];
        let parent = applied(&base, &drift_datoms);
        let branch = squash(&[
            datom(2, order, STATUS, text("draft"), false),
            datom(2, order, STATUS, text("packed"), true),
        ]);
        let conflicts = scan(&MergeInput {
            parent: &parent,
            branch: &branch,
            drift: &squash(&drift_datoms),
        });
        // One unit, not two: the retraction of the value the branch started
        // from and the assertion of the value it wants are the same decision.
        assert_eq!(conflicts.len(), 1, "{conflicts:?}");
        let conflict = &conflicts[0];
        assert_eq!(conflict.kind, ConflictKind::WriteWrite);
        assert_eq!(conflict.scope, Scope::Pair);
        assert_eq!(conflict.branch, Some(text("packed")));
        assert_eq!(conflict.parent, Some(text("cancelled")));
        assert!(conflict.overridable);

        // Accept-parent drops the whole unit; override keeps the branch's
        // value and drops the retraction that would now miss.
        let fence = |take| Resolution {
            entity: order,
            attribute: STATUS,
            scope: Scope::Pair,
            parent: Some(text("cancelled")),
            take,
        };
        let accepted = resolve(conflicts.clone(), &[fence(Take::Parent)]);
        assert!(accepted.outstanding.is_empty());
        assert!(apply(&branch, &accepted, &parent).is_empty());

        let overridden = resolve(conflicts.clone(), &[fence(Take::Branch)]);
        assert!(overridden.outstanding.is_empty());
        let merged = apply(&branch, &overridden, &parent);
        assert_eq!(
            merged.asserts,
            [(order, STATUS, text("packed"))].into_iter().collect()
        );
        assert!(merged.retracts.is_empty());

        // The fence is the report's value, not a formality: once the parent
        // moves again the answer no longer applies.
        let stale = resolve(
            conflicts,
            &[Resolution {
                parent: Some(text("draft")),
                ..fence(Take::Parent)
            }],
        );
        assert_eq!(stale.outstanding.len(), 1);
        assert!(matches!(
            stale.rejected.as_slice(),
            [ResolutionError::Unmatched(_)]
        ));
    }

    #[test]
    fn a_parent_that_landed_on_the_branchs_value_is_not_a_conflict() {
        let order = user(1_000);
        let base = applied(&db(), &[datom(1, order, STATUS, text("draft"), true)]);
        let drift_datoms = [
            datom(2, order, STATUS, text("draft"), false),
            datom(2, order, STATUS, text("packed"), true),
        ];
        let parent = applied(&base, &drift_datoms);
        let branch = squash(&[
            datom(2, order, STATUS, text("draft"), false),
            datom(2, order, STATUS, text("packed"), true),
        ]);
        let conflicts = scan(&MergeInput {
            parent: &parent,
            branch: &branch,
            drift: &squash(&drift_datoms),
        });
        assert!(
            conflicts.is_empty(),
            "both sides asked for the same end state: {conflicts:?}"
        );
    }

    #[test]
    fn a_retraction_the_parent_has_already_outrun_is_the_cas_shaped_conflict() {
        let order = user(1_000);
        let base = applied(&db(), &[datom(1, order, STATUS, text("draft"), true)]);
        let drift_datoms = [
            datom(2, order, STATUS, text("draft"), false),
            datom(2, order, STATUS, text("cancelled"), true),
        ];
        let parent = applied(&base, &drift_datoms);
        let branch = squash(&[datom(2, order, STATUS, text("draft"), false)]);
        let conflicts = scan(&MergeInput {
            parent: &parent,
            branch: &branch,
            drift: &squash(&drift_datoms),
        });
        assert_eq!(conflicts.len(), 1, "{conflicts:?}");
        assert_eq!(conflicts[0].kind, ConflictKind::RetractionMiss);
        assert_eq!(conflicts[0].branch, None);
        assert_eq!(conflicts[0].parent, Some(text("cancelled")));
        assert!(
            !conflicts[0].overridable,
            "overriding it would retract a value the branch never held"
        );
        // Asking for the branch anyway is refused, and the conflict stands.
        let refused = resolve(
            conflicts,
            &[Resolution {
                entity: order,
                attribute: STATUS,
                scope: Scope::Pair,
                parent: Some(text("cancelled")),
                take: Take::Branch,
            }],
        );
        assert_eq!(refused.outstanding.len(), 1);
        assert!(matches!(
            refused.rejected.as_slice(),
            [ResolutionError::NotOverridable(_)]
        ));
    }

    #[test]
    fn set_valued_attributes_union_instead_of_colliding() {
        let order = user(1_000);
        let base = applied(&db(), &[datom(1, order, TAGS, text("gift"), true)]);
        let drift_datoms = [
            // The parent adds a member of its own and removes the one both
            // sides started with.
            datom(2, order, TAGS, text("priority"), true),
            datom(2, order, TAGS, text("gift"), false),
        ];
        let parent = applied(&base, &drift_datoms);
        let branch = squash(&[
            // A member neither side had: this is what adding to a set means.
            datom(2, order, TAGS, text("fragile"), true),
            // And the branch removed the shared member too, which the parent
            // has already done for it.
            datom(2, order, TAGS, text("gift"), false),
        ]);
        let conflicts = scan(&MergeInput {
            parent: &parent,
            branch: &branch,
            drift: &squash(&drift_datoms),
        });
        assert!(
            conflicts.is_empty(),
            "members of a set do not race: {conflicts:?}"
        );
        // Nothing was dropped either: the merge still carries both halves,
        // and the parent's own addition survives because sets union.
        let merged = apply(&branch, &Resolved::default(), &parent);
        assert_eq!(
            merged.asserts,
            [(order, TAGS, text("fragile"))].into_iter().collect()
        );
        assert_eq!(
            merged.retracts,
            [(order, TAGS, text("gift"))].into_iter().collect()
        );
    }

    #[test]
    fn a_unique_value_the_parent_gave_away_names_who_holds_it() {
        let mine = user(1_000);
        let theirs = user(1_001);
        let drift_datoms = [datom(2, theirs, CODE, text("A-1"), true)];
        let parent = applied(&db(), &drift_datoms);
        let branch = squash(&[datom(2, mine, CODE, text("A-1"), true)]);
        let conflicts = scan(&MergeInput {
            parent: &parent,
            branch: &branch,
            drift: &squash(&drift_datoms),
        });
        assert_eq!(conflicts.len(), 1, "{conflicts:?}");
        assert_eq!(
            conflicts[0].kind,
            ConflictKind::Uniqueness { holder: theirs }
        );
        assert!(
            !conflicts[0].overridable,
            "evicting the holder would edit an entity the saga never wrote"
        );
    }

    #[test]
    fn a_reference_into_an_entity_the_parent_retracted_is_dangling() {
        let customer = user(1_000);
        let order = user(1_001);
        let base = applied(&db(), &[datom(1, customer, STATUS, text("active"), true)]);
        let drift_datoms = [datom(2, customer, STATUS, text("active"), false)];
        let parent = applied(&base, &drift_datoms);
        let branch = squash(&[datom(2, order, PART_OF, Value::Ref(customer), true)]);
        let conflicts = scan(&MergeInput {
            parent: &parent,
            branch: &branch,
            drift: &squash(&drift_datoms),
        });
        assert_eq!(conflicts.len(), 1, "{conflicts:?}");
        assert_eq!(
            conflicts[0].kind,
            ConflictKind::DanglingRef { target: customer }
        );
        assert!(!conflicts[0].overridable);
    }

    #[test]
    fn a_branchs_keyword_values_are_renamed_into_the_parents_naming() {
        use corium_core::{Keyword, KeywordInterner};

        // The parent and the branch each minted a keyword after the branch
        // copied the parent's naming, so the same id means different words.
        let mut parent = KeywordInterner::default();
        let shared = parent.intern(Keyword::new(Some("order.status"), "draft"));
        let mut branch = parent.clone();
        let branch_only = branch.intern(Keyword::new(Some("order.status"), "packed"));
        let parent_only = parent.intern(Keyword::new(Some("order.status"), "cancelled"));
        assert_eq!(
            branch_only, parent_only,
            "this test is only interesting while the two sides collide"
        );

        let order = user(1_000);
        let novelty = Novelty {
            asserts: [
                (order, STATUS, Value::Keyword(branch_only)),
                (order, TAGS, Value::Keyword(shared)),
            ]
            .into_iter()
            .collect(),
            retracts: BTreeSet::new(),
            steps: 1,
        };
        let translated = translate(&novelty, &branch, &mut parent).expect("named on both sides");
        let renamed: Vec<Keyword> = translated
            .asserts
            .iter()
            .filter_map(|(_, _, value)| match value {
                Value::Keyword(id) => parent.resolve(*id),
                _ => None,
            })
            .collect();
        assert!(
            renamed.contains(&Keyword::new(Some("order.status"), "packed")),
            "{renamed:?}"
        );
        assert!(
            renamed.contains(&Keyword::new(Some("order.status"), "draft")),
            "{renamed:?}"
        );
        assert!(
            !renamed.contains(&Keyword::new(Some("order.status"), "cancelled")),
            "the branch's word was silently replaced by the parent's: {renamed:?}"
        );
    }
}
