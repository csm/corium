//! The saga merge's boundary: guards, resolutions, and the conflict report
//! (ADR-0023).
//!
//! [`corium_tx::merge`] does the arithmetic — squash both sides, compare the
//! net effects, say what collides. What that arithmetic cannot do is talk to
//! anyone, and a merge is a conversation: the owner declares read dependencies
//! the engine does not track, reads a report about drift it did not expect,
//! and answers it. This module is where those three things are written in EDN
//! and read back.
//!
//! **Guards are how a saga makes its read dependencies explicit.** The engine
//! tracks write sets, not read sets (see the v1 limits), so serializability
//! beyond write–write is opt-in and visible rather than silent. A guard comes
//! from one of two places and both are evaluated at commit: the commit request,
//! for a dependency the committer knows about now, and `:db.saga/guard`
//! metadata on the branch step that established it, for one that has to
//! outlive the process that noticed it. A crashed owner — or a different
//! process resuming the saga — inherits the declared guards for free, and a
//! step-declared guard that fails is reported with the step that declared it,
//! which points at the read that actually mattered.
//!
//! **A conflict report is a plan document** in ADR-0020's sense: it names each
//! unit that collided with both sides' values, so the owner can decide against
//! state they have now seen. Reading it is what makes resolving consistent
//! with replaying effects — the alternative would be deciding against state
//! nobody observed, which is exactly what the merge refuses to do on its own.

use corium_core::{AttrId, EntityId, Value};
use corium_db::saga::SagaEntry;
use corium_db::{Db, bootstrap};
use corium_forms::txforms::{tx_attribute, tx_entity, tx_value};
use corium_query::QInput;
use corium_query::boundary::value_to_edn;
use corium_query::edn::{Edn, read_all};
use corium_tx::merge::{Conflict, ConflictKind, Novelty, Resolution, ResolutionError, Scope, Take};
use corium_tx::{EntityRef, TxItem, TxOp};

/// A read dependency a saga declared, with where it came from.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Guard {
    /// The branch step that declared it, when it came from step metadata.
    /// Absent for a guard supplied with the commit request.
    pub step: Option<u64>,
    /// The guard as written.
    pub text: String,
}

impl Guard {
    /// A guard supplied with the commit request.
    #[must_use]
    pub fn requested(text: impl Into<String>) -> Self {
        Self {
            step: None,
            text: text.into(),
        }
    }
}

/// The `:db.saga/guard` declarations the branch's steps left behind.
///
/// They live on step transaction entities, which never merge, so this reads
/// them from the branch rather than from the novelty — which is the point of
/// declaring them there: the contract is durable in the branch's log instead
/// of living only in the memory of whichever process happened to notice the
/// dependency.
#[must_use]
pub fn branch_guards(branch: &Db, basis_t: u64) -> Vec<Guard> {
    let mut guards: Vec<Guard> = branch
        .datoms_for_attribute(bootstrap::SAGA_GUARD)
        .filter(|datom| {
            datom.e.partition() == corium_core::Partition::Tx as u32 && datom.e.sequence() > basis_t
        })
        .filter_map(|datom| match &datom.v {
            Value::Str(text) => Some(Guard {
                step: Some(datom.e.sequence()),
                text: text.to_string(),
            }),
            _ => None,
        })
        .collect();
    guards.sort_by(|left, right| (left.step, &left.text).cmp(&(right.step, &right.text)));
    guards.dedup();
    guards
}

/// Evaluates one guard against the parent's current value.
///
/// Two shapes, told apart by the form:
///
/// * `[:db/cas <entity> <attribute> <value>]` — the compare half of a
///   compare-and-swap. The swap half is the saga's novelty, which is why only
///   the comparison is written here: the guard says what the parent must still
///   hold for the merge to mean what the owner thinks it means. `nil` asserts
///   the pair holds nothing.
/// * `{:guard <query>}` — a Datalog query over the parent, holding when it
///   returns at least one row; `{:guard <query> :expect :none}` holds when it
///   returns none, which is how "nobody else added to this set" is written.
///
/// # Errors
/// Returns the reason the guard does not hold, or why it could not be read.
/// A malformed guard fails the merge rather than passing quietly: a
/// precondition nobody can evaluate has not been met.
pub fn evaluate(parent: &Db, guard: &Guard) -> Result<(), String> {
    let forms = read_all(&guard.text).map_err(|error| format!("guard is not EDN: {error}"))?;
    let [form] = forms.as_slice() else {
        return Err(format!(
            "a guard is one form, not {} ({})",
            forms.len(),
            guard.text
        ));
    };
    match form {
        Edn::Vector(items) | Edn::List(items) => value_guard(parent, items),
        Edn::Map(_) => query_guard(parent, form),
        other => Err(format!("unrecognized guard {other}")),
    }
}

fn value_guard(parent: &Db, items: &[Edn]) -> Result<(), String> {
    let [form, entity, attribute, expected] = items else {
        return Err(format!(
            "a value guard is [:db/cas <entity> <attribute> <value>], got {} elements",
            items.len()
        ));
    };
    if form.as_keyword().map(ToString::to_string).as_deref() != Some(":db/cas") {
        return Err(format!("unrecognized guard operation {form}"));
    }
    let entity = match tx_entity(parent, entity).map_err(|error| error.to_string())? {
        EntityRef::Id(entity) => entity,
        EntityRef::Lookup(a, v) => parent
            .lookup(a, &v)
            .ok_or_else(|| format!("guard lookup ref {entity} does not resolve in the parent"))?,
        EntityRef::Temp(name) => {
            return Err(format!("a guard cannot name a tempid ({name})"));
        }
    };
    let attribute = tx_attribute(parent, attribute).map_err(|error| error.to_string())?;
    // Against a private naming: a guard reads the parent, and a keyword it
    // names that the parent has never interned cannot be any current value —
    // which is the guard failing, not the parent growing a name.
    let mut naming = parent.interner().clone();
    let expected = match expected {
        Edn::Nil => None,
        form => Some(
            tx_value(parent, &mut naming, attribute, form).map_err(|error| error.to_string())?,
        ),
    };
    let held = parent.values(entity, attribute).into_iter().next();
    if held == expected {
        return Ok(());
    }
    Err(format!(
        "{} of {} is {}, not {}",
        render_attribute(parent, attribute),
        entity.raw(),
        render_value(parent, held.as_ref()),
        render_value(parent, expected.as_ref()),
    ))
}

fn query_guard(parent: &Db, form: &Edn) -> Result<(), String> {
    let query = form
        .get(&Edn::keyword("guard"))
        .ok_or_else(|| format!("a query guard is {{:guard <query>}}, got {form}"))?;
    let expect = form.get(&Edn::keyword("expect"));
    let none = match expect.map(ToString::to_string).as_deref() {
        None | Some(":any") => false,
        Some(":none") => true,
        Some(other) => return Err(format!(":expect is :any or :none, not {other}")),
    };
    let rows = corium_query::q(query, &[QInput::Db(parent)])
        .map_err(|error| format!("guard query failed: {error}"))?;
    let empty = match &rows {
        Edn::Vector(items) | Edn::List(items) | Edn::Set(items) => items.is_empty(),
        Edn::Nil => true,
        _ => false,
    };
    if empty == none {
        return Ok(());
    }
    Err(if none {
        format!("guard query expected no results and found {rows}")
    } else {
        "guard query expected a result and found none".to_owned()
    })
}

/// Reads the per-conflict resolutions a commit request carries.
///
/// Each one is `{:e <entity> :a <attribute> :parent <value> :take :parent}`,
/// with `:v <value>` naming the single fact when the conflict is on a
/// cardinality-many attribute, and `:take :branch` asking for an override.
/// `:parent` is the fence — the value the report showed — and omitting it
/// reads as `nil`, which answers only a conflict whose parent side really is
/// empty. Nothing here has to be enforced: a resolution that does not name
/// what the report showed simply matches no conflict, so the next round of
/// drift is reported rather than silently absorbed.
///
/// # Errors
/// Returns the reason a resolution could not be read.
pub fn parse_resolutions(parent: &Db, forms: &[Edn]) -> Result<Vec<Resolution>, String> {
    forms.iter().map(|form| resolution(parent, form)).collect()
}

fn resolution(parent: &Db, form: &Edn) -> Result<Resolution, String> {
    let field = |key: &str| form.get(&Edn::keyword(key));
    let entity = field("e").ok_or_else(|| format!("a resolution needs :e ({form})"))?;
    let attribute = field("a").ok_or_else(|| format!("a resolution needs :a ({form})"))?;
    let entity = match tx_entity(parent, entity).map_err(|error| error.to_string())? {
        EntityRef::Id(entity) => entity,
        EntityRef::Lookup(a, v) => parent
            .lookup(a, &v)
            .ok_or_else(|| format!("resolution lookup ref {entity} does not resolve"))?,
        EntityRef::Temp(name) => return Err(format!("a resolution cannot name a tempid ({name})")),
    };
    let attribute = tx_attribute(parent, attribute).map_err(|error| error.to_string())?;
    let mut naming = parent.interner().clone();
    let mut value = |key: &str| -> Result<Option<Value>, String> {
        match field(key) {
            None | Some(Edn::Nil) => Ok(None),
            Some(form) => tx_value(parent, &mut naming, attribute, form)
                .map(Some)
                .map_err(|error| error.to_string()),
        }
    };
    let scope = match value("v")? {
        Some(fact) => Scope::Fact(fact),
        None => Scope::Pair,
    };
    let take = match field("take").map(ToString::to_string).as_deref() {
        Some(":parent") => Take::Parent,
        Some(":branch") => Take::Branch,
        Some(other) => return Err(format!(":take is :parent or :branch, not {other}")),
        None => return Err(format!("a resolution needs :take ({form})")),
    };
    Ok(Resolution {
        entity,
        attribute,
        scope,
        parent: value("parent")?,
        take,
    })
}

/// The EDN conflict report a failed merge leaves on the registry entry.
///
/// It is written to be answered: every conflict names the unit, both sides'
/// values, and which resolutions that class admits, so the owner's reply is a
/// transcription rather than a reconstruction.
#[must_use]
pub fn report(
    parent: &Db,
    saga: &SagaEntry,
    novelty: &Novelty,
    conflicts: &[Conflict],
    failed_guards: &[(Guard, String)],
    rejected: &[ResolutionError],
) -> String {
    let mut fields = vec![
        (Edn::keyword("saga"), Edn::Str(format!("{:032x}", saga.id))),
        (
            Edn::keyword("basis-t"),
            Edn::Long(saga.basis_t.unwrap_or_default()),
        ),
        (
            Edn::keyword("parent-basis-t"),
            Edn::Long(i64::try_from(parent.basis_t()).unwrap_or(i64::MAX)),
        ),
        (
            Edn::keyword("steps"),
            Edn::Long(i64::try_from(novelty.steps).unwrap_or(i64::MAX)),
        ),
        (
            Edn::keyword("datoms"),
            Edn::Long(i64::try_from(novelty.len()).unwrap_or(i64::MAX)),
        ),
        (
            Edn::keyword("conflicts"),
            Edn::Vector(
                conflicts
                    .iter()
                    .map(|conflict| conflict_edn(parent, conflict))
                    .collect(),
            ),
        ),
    ];
    if !failed_guards.is_empty() {
        fields.push((
            Edn::keyword("guards"),
            Edn::Vector(
                failed_guards
                    .iter()
                    .map(|(guard, why)| {
                        let mut entry = vec![
                            (Edn::keyword("guard"), Edn::Str(guard.text.clone())),
                            (Edn::keyword("failed"), Edn::Str(why.clone())),
                        ];
                        if let Some(step) = guard.step {
                            entry.push((
                                Edn::keyword("step"),
                                Edn::Long(i64::try_from(step).unwrap_or(i64::MAX)),
                            ));
                        }
                        Edn::Map(entry)
                    })
                    .collect(),
            ),
        ));
    }
    if !rejected.is_empty() {
        fields.push((
            Edn::keyword("rejected"),
            Edn::Vector(
                rejected
                    .iter()
                    .map(|rejected| match rejected {
                        ResolutionError::Unmatched(resolution) => Edn::Map(vec![
                            (
                                Edn::keyword("resolution"),
                                resolution_edn(parent, resolution),
                            ),
                            (
                                Edn::keyword("reason"),
                                Edn::Str(
                                    "no conflict in this report matches it; the report it \
                                     answers is stale"
                                        .to_owned(),
                                ),
                            ),
                        ]),
                        ResolutionError::NotOverridable(resolution) => Edn::Map(vec![
                            (
                                Edn::keyword("resolution"),
                                resolution_edn(parent, resolution),
                            ),
                            (
                                Edn::keyword("reason"),
                                Edn::Str(
                                    "this conflict class has no override; the branch's value \
                                     would be a write nobody observed"
                                        .to_owned(),
                                ),
                            ),
                        ]),
                    })
                    .collect(),
            ),
        ));
    }
    Edn::Map(fields).to_string()
}

fn conflict_edn(parent: &Db, conflict: &Conflict) -> Edn {
    let mut fields = vec![
        (Edn::keyword("type"), Edn::keyword(conflict.kind.name())),
        (Edn::keyword("e"), Edn::Long(entity_long(conflict.entity))),
        (
            Edn::keyword("a"),
            render_attribute_edn(parent, conflict.attribute),
        ),
    ];
    if let Scope::Fact(value) = &conflict.scope {
        fields.push((Edn::keyword("v"), value_to_edn(parent, value)));
    }
    fields.push((
        Edn::keyword("branch"),
        conflict
            .branch
            .as_ref()
            .map_or(Edn::Nil, |value| value_to_edn(parent, value)),
    ));
    fields.push((
        Edn::keyword("parent"),
        conflict
            .parent
            .as_ref()
            .map_or(Edn::Nil, |value| value_to_edn(parent, value)),
    ));
    match &conflict.kind {
        ConflictKind::Uniqueness { holder } => {
            fields.push((Edn::keyword("holder"), Edn::Long(entity_long(*holder))));
        }
        ConflictKind::DanglingRef { target } => {
            fields.push((Edn::keyword("target"), Edn::Long(entity_long(*target))));
        }
        ConflictKind::WriteWrite | ConflictKind::RetractionMiss => {}
    }
    let mut takes = vec![Edn::keyword("parent")];
    if conflict.overridable {
        takes.push(Edn::keyword("branch"));
    }
    fields.push((Edn::keyword("resolutions"), Edn::Vector(takes)));
    Edn::Map(fields)
}

fn resolution_edn(parent: &Db, resolution: &Resolution) -> Edn {
    let mut fields = vec![
        (Edn::keyword("e"), Edn::Long(entity_long(resolution.entity))),
        (
            Edn::keyword("a"),
            render_attribute_edn(parent, resolution.attribute),
        ),
    ];
    if let Scope::Fact(value) = &resolution.scope {
        fields.push((Edn::keyword("v"), value_to_edn(parent, value)));
    }
    fields.push((
        Edn::keyword("parent"),
        resolution
            .parent
            .as_ref()
            .map_or(Edn::Nil, |value| value_to_edn(parent, value)),
    ));
    fields.push((
        Edn::keyword("take"),
        Edn::keyword(match resolution.take {
            Take::Parent => "parent",
            Take::Branch => "branch",
        }),
    ));
    Edn::Map(fields)
}

fn entity_long(entity: EntityId) -> i64 {
    i64::try_from(entity.raw()).unwrap_or(i64::MAX)
}

fn render_attribute_edn(db: &Db, a: AttrId) -> Edn {
    db.idents().ident(a).map_or_else(
        || Edn::Long(entity_long(a)),
        |name| Edn::Keyword(name.clone()),
    )
}

fn render_attribute(db: &Db, a: AttrId) -> String {
    render_attribute_edn(db, a).to_string()
}

fn render_value(db: &Db, value: Option<&Value>) -> String {
    value.map_or_else(
        || "nil".to_owned(),
        |value| value_to_edn(db, value).to_string(),
    )
}

/// The transaction items a merge applies: the branch's net effect, retractions
/// first.
///
/// Retractions lead so that a cardinality-one update reads the way it happened
/// — the branch's starting value goes, the branch's value arrives — rather
/// than relying on the assertion to sweep the old value out from under a
/// retraction that would then miss. Every position is a concrete entity id:
/// ids were leased precisely so the branch's allocations survive the merge
/// verbatim, and nothing here is resolved a second time.
#[must_use]
pub fn novelty_items(novelty: &Novelty) -> Vec<TxItem> {
    novelty
        .retracts
        .iter()
        .map(|(e, a, v)| TxItem::Op(TxOp::Retract(EntityRef::Id(*e), *a, v.clone())))
        .chain(
            novelty
                .asserts
                .iter()
                .map(|(e, a, v)| TxItem::Op(TxOp::Add(EntityRef::Id(*e), *a, v.clone()))),
        )
        .collect()
}
