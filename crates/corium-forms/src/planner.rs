//! The read-only schema-update planner (`docs/design/schema-migrations.md`).
//!
//! Planning is a pure function of a desired schema and one immutable database
//! value:
//!
//! 1. read the installed schema at one basis;
//! 2. match exact idents and compute *property-level* changes;
//! 3. measure each change's impact at that same basis;
//! 4. build the dependency graph and hash the logical plan.
//!
//! Nothing here writes. The measurements travel beside the plan as advisory
//! data and never enter [`SchemaPlan::digest`], so an ordinary write that
//! moves a count cannot invalidate an additive plan whose preconditions still
//! hold.
//!
//! By default a desired file manages only the declarations it contains:
//! installed attributes it does not mention are reported as `unmanaged` and
//! left alone. `prune` turns them into retirement requests instead. Engine
//! attributes are never managed by a user file.

use corium_core::migration::{
    AckCode, BlockedReason, ChangeKind, ExecutionClass, InstalledAttribute, InstalledSchema,
    Observation, PlanStep, Risk, SchemaPlan, SchemaProperty, cardinality_name, flag, unique_name,
    value_type_name,
};
use corium_core::{Cardinality, Partition};
use corium_db::impact::{
    self, DEFAULT_SAMPLE_LIMIT, attribute_impact, cardinality_conflicts, duplicate_values,
    history_impact, live_refs,
};
use corium_db::Db;
use thiserror::Error;

use crate::desired::{DesiredAttribute, DesiredSchema};
use crate::schemaform::FIRST_ATTR_ID;

/// How a plan is computed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlanOptions {
    /// Database the plan names.
    pub database: String,
    /// Whether installed attributes absent from the file become retirements.
    pub prune: bool,
    /// How many representative entity ids a violation reports.
    pub sample_limit: usize,
}

impl PlanOptions {
    /// Default options for one database: no pruning, five-entity samples.
    #[must_use]
    pub fn new(database: impl Into<String>) -> Self {
        Self {
            database: database.into(),
            prune: false,
            sample_limit: DEFAULT_SAMPLE_LIMIT,
        }
    }

    /// Turns absent installed attributes into retirement requests.
    #[must_use]
    pub const fn with_prune(mut self, prune: bool) -> Self {
        self.prune = prune;
        self
    }
}

/// A desired file that cannot be planned at all.
#[derive(Clone, Debug, Eq, PartialEq, Error)]
pub enum PlanError {
    /// The file declares an attribute the engine owns.
    #[error("{0} is an engine attribute and cannot be managed by a schema file")]
    EngineAttribute(String),
}

/// Reads the installed schema of a database value.
///
/// Attribute entity ids come from the database, never from file order. An
/// attribute below [`FIRST_ATTR_ID`] in the db partition belongs to the engine,
/// as does one no ident names — neither can be reached from a user file.
#[must_use]
pub fn installed_schema(db: &Db) -> InstalledSchema {
    let mut attrs = Vec::new();
    for (id, attribute) in db.schema().iter() {
        let engine = is_engine_attribute(*id);
        let named = db.idents().ident(*id);
        let ident = named.map_or_else(
            || format!("<unnamed attribute {}:{}>", id.partition(), id.sequence()),
            ToString::to_string,
        );
        attrs.push(InstalledAttribute {
            ident,
            id: *id,
            value_type: attribute.value_type,
            cardinality: attribute.cardinality,
            unique: attribute.unique,
            indexed: attribute.indexed,
            is_component: attribute.is_component,
            no_history: attribute.no_history,
            // `:db/doc`, retirement, and protection timelines are not yet
            // carried by the installed schema; see `InstalledSchema::
            // doc_tracked` and the plan note the planner adds for docs.
            doc: None,
            retired: false,
            ever_protected: false,
            engine: engine || named.is_none(),
        });
    }
    InstalledSchema::new(attrs)
        .with_doc_tracking(false)
        .with_generation(None)
}

/// Whether the engine owns this attribute id.
#[must_use]
pub fn is_engine_attribute(id: corium_core::AttrId) -> bool {
    id.partition() == Partition::Db as u32 && id.sequence() < FIRST_ATTR_ID
}

/// Computes a deterministic plan for `desired` against `db`.
///
/// # Errors
/// Returns [`PlanError::EngineAttribute`] when the file declares an attribute
/// the engine owns.
pub fn plan(
    desired: &DesiredSchema,
    db: &Db,
    options: &PlanOptions,
) -> Result<SchemaPlan, PlanError> {
    plan_against(desired, db, &installed_schema(db), options)
}

/// Computes a plan against an installed schema read separately from `db`.
///
/// The installed schema and the database value must describe the same basis:
/// `db` supplies the facts every impact scan measures, and `installed`
/// supplies the metadata every change is computed from. Callers that can read
/// more installed state than [`installed_schema`] recovers from a `Db` —
/// documentation, retirement, or a protection timeline — pass it here.
///
/// # Errors
/// Returns [`PlanError::EngineAttribute`] when the file declares an attribute
/// the engine owns.
pub fn plan_against(
    desired: &DesiredSchema,
    db: &Db,
    installed: &InstalledSchema,
    options: &PlanOptions,
) -> Result<SchemaPlan, PlanError> {
    let mut steps = Vec::new();
    let mut notes = Vec::new();

    for (ident, declaration) in desired.iter() {
        match installed.get(ident) {
            None => steps.push(add_step(ident, declaration)),
            Some(current) if current.engine => {
                return Err(PlanError::EngineAttribute(ident.clone()));
            }
            Some(current) if current.retired => {
                // Unreachable until retirement lands; stated rather than
                // silently planned as an ordinary property change.
                notes.push(format!(
                    "{ident} is retired; reinstating a retired attribute is not supported"
                ));
            }
            Some(current) => {
                steps.extend(property_steps(
                    current,
                    declaration,
                    db,
                    installed,
                    options.sample_limit,
                ));
            }
        }
    }

    let mut unmanaged = Vec::new();
    for current in installed.managed() {
        if desired.get(&current.ident).is_some() || current.retired {
            continue;
        }
        if options.prune {
            steps.push(retire_step(current, db));
        } else {
            unmanaged.push(current.ident.clone());
        }
    }

    steps.sort_by(|left, right| left.key.cmp(&right.key));

    if !desired.is_empty() && !installed.doc_tracked() {
        notes.push(
            "documentation is not recorded by the installed schema in this build, so :db/doc \
             differences are not planned"
                .to_owned(),
        );
    }
    if !db.has_complete_history() {
        notes.push(
            "this database value was opened from a published snapshot, so historical counts are \
             a floor rather than an exact answer"
                .to_owned(),
        );
    }

    Ok(SchemaPlan {
        database: options.database.clone(),
        basis_t: db.basis_t(),
        schema_generation: installed.generation(),
        desired_digest: desired.digest(),
        installed_fingerprint: installed.fingerprint(),
        prune: options.prune,
        steps,
        unmanaged,
        notes,
    })
}

fn add_step(ident: &str, declaration: &DesiredAttribute) -> PlanStep {
    // An attribute the database does not have cannot carry a fact, so nothing
    // is inspected or rewritten even when the declaration requests uniqueness
    // or an index.
    PlanStep {
        key: PlanStep::key_for(ident, ChangeKind::Add),
        ident: ident.to_owned(),
        kind: ChangeKind::Add,
        installed: None,
        desired: Some(declaration.rendering()),
        class: ExecutionClass::Additive,
        risk: Risk::Low,
        acks: Vec::new(),
        blocked: None,
        depends_on: Vec::new(),
        summary: declaration.rendering(),
        recipe: Vec::new(),
        observations: Vec::new(),
        notes: Vec::new(),
    }
}

fn retire_step(current: &InstalledAttribute, db: &Db) -> PlanStep {
    let impact = attribute_impact(db, current.id);
    let history = history_impact(db, current.id);
    let live = impact.datoms > 0;
    let mut observations = vec![
        Observation::count("current-datoms", impact.datoms),
        Observation::count("current-entities", impact.entities),
    ];
    observations.push(history_observation(history));
    PlanStep {
        key: PlanStep::key_for(&current.ident, ChangeKind::Retire),
        ident: current.ident.clone(),
        kind: ChangeKind::Retire,
        installed: Some(current.rendering()),
        desired: None,
        // Retirement refuses new assertions from a basis onward; existing
        // facts stay readable and retractable, and nothing is deleted.
        class: ExecutionClass::ValidateReindex,
        risk: Risk::High,
        acks: if live {
            vec![AckCode::RetireLiveAttribute]
        } else {
            Vec::new()
        },
        blocked: None,
        depends_on: Vec::new(),
        summary: "retire".to_owned(),
        recipe: Vec::new(),
        observations,
        notes: vec![
            "the ident, metadata, and history stay readable at every later basis".to_owned(),
            "new assertions are refused; retractions of existing values remain legal".to_owned(),
            "retracting the remaining current facts is separate rewrite work".to_owned(),
        ],
    }
}

fn property_steps(
    current: &InstalledAttribute,
    declaration: &DesiredAttribute,
    db: &Db,
    installed: &InstalledSchema,
    sample_limit: usize,
) -> Vec<PlanStep> {
    let mut steps = Vec::new();
    let ident = current.ident.as_str();

    // A value-type change is refused, but the other differences on the same
    // attribute are still classified — and made to depend on it, so the plan
    // shows plainly that none of them can run first.
    let type_step = (current.value_type != declaration.value_type).then(|| {
        value_type_step(current, declaration, db)
    });
    let blocked_by: Vec<String> = type_step
        .as_ref()
        .map(|step| vec![step.key.clone()])
        .unwrap_or_default();
    if let Some(step) = type_step {
        steps.push(step);
    }

    if current.cardinality != declaration.cardinality {
        steps.push(cardinality_step(
            current,
            declaration,
            db,
            sample_limit,
            &blocked_by,
        ));
    }

    // AVET coverage is planned before the constraint that depends on it: the
    // uniqueness step is only ready once the backfill covers its basis.
    let index_key = (current.indexed != declaration.indexed).then(|| {
        PlanStep::key_for(ident, ChangeKind::Property(SchemaProperty::Index))
    });
    if current.indexed != declaration.indexed {
        steps.push(index_step(current, declaration, db, &blocked_by));
    }
    if current.unique != declaration.unique {
        let mut depends = blocked_by.clone();
        if declaration.unique.is_some()
            && let Some(key) = &index_key
        {
            depends.push(key.clone());
        }
        steps.push(unique_step(current, declaration, db, sample_limit, &depends));
    }
    if current.is_component != declaration.is_component {
        steps.push(component_step(current, declaration, db, &blocked_by));
    }
    if current.no_history != declaration.no_history {
        steps.push(no_history_step(current, declaration, db, &blocked_by));
    }
    if installed.doc_tracked() && current.doc != declaration.doc {
        steps.push(doc_step(current, declaration, &blocked_by));
    }
    steps
}

fn value_type_step(
    current: &InstalledAttribute,
    declaration: &DesiredAttribute,
    db: &Db,
) -> PlanStep {
    let impact = attribute_impact(db, current.id);
    let installed_type = value_type_name(current.value_type);
    let desired_type = value_type_name(declaration.value_type);
    let shadow = format!("{}-{desired_type}", current.ident);
    PlanStep {
        key: PlanStep::key_for(
            &current.ident,
            ChangeKind::Property(SchemaProperty::ValueType),
        ),
        ident: current.ident.clone(),
        kind: ChangeKind::Property(SchemaProperty::ValueType),
        installed: Some(installed_type.to_owned()),
        desired: Some(desired_type.to_owned()),
        // Old retractions and CAS operands keep their old type, so mutating
        // the type in place would claim history had always been the new one.
        // `--allow rewrite` can never enable this.
        class: ExecutionClass::Destructive,
        risk: Risk::High,
        acks: Vec::new(),
        blocked: Some(BlockedReason::ValueTypeMutation),
        depends_on: Vec::new(),
        summary: format!("{installed_type} -> {desired_type}"),
        recipe: vec![
            format!("add {shadow} with :db/valueType :db.type/{desired_type}"),
            "convert and assert current values under an explicit conversion".to_owned(),
            "verify counts, rejected values, and dual-read/dual-write state".to_owned(),
            format!("cut application reads and writes over to {shadow}"),
            format!(
                "retire {}, optionally retracting its current facts",
                current.ident
            ),
        ],
        observations: vec![
            Observation::count("current-datoms", impact.datoms),
            Observation::count("distinct-values", impact.distinct_values),
        ],
        notes: vec![
            "the conversion policy cannot be inferred from the two type names".to_owned(),
        ],
    }
}

fn cardinality_step(
    current: &InstalledAttribute,
    declaration: &DesiredAttribute,
    db: &Db,
    sample_limit: usize,
    depends_on: &[String],
) -> PlanStep {
    let impact = attribute_impact(db, current.id);
    let widening = declaration.cardinality == Cardinality::Many;
    let conflicts = if widening {
        impact::CardinalityConflicts::default()
    } else {
        cardinality_conflicts(db, current.id, sample_limit)
    };
    let mut observations = vec![Observation::count("current-datoms", impact.datoms)];
    let (class, risk, blocked) = if widening {
        // Every existing value already satisfies `many`.
        (ExecutionClass::Additive, Risk::Low, None)
    } else if conflicts.is_empty() {
        (ExecutionClass::ValidateReindex, Risk::Medium, None)
    } else {
        (
            ExecutionClass::Rewrite,
            Risk::High,
            Some(BlockedReason::CardinalityConflicts),
        )
    };
    if !widening {
        observations.push(Observation::count("conflicting-entities", conflicts.entities));
        observations.push(Observation::count("conflicting-datoms", conflicts.datoms));
        if !conflicts.sample.is_empty() {
            observations.push(Observation::entities(
                "conflicting-entity-sample",
                conflicts.sample,
            ));
        }
    }
    PlanStep {
        key: PlanStep::key_for(
            &current.ident,
            ChangeKind::Property(SchemaProperty::Cardinality),
        ),
        ident: current.ident.clone(),
        kind: ChangeKind::Property(SchemaProperty::Cardinality),
        installed: Some(cardinality_name(current.cardinality).to_owned()),
        desired: Some(cardinality_name(declaration.cardinality).to_owned()),
        class,
        risk,
        acks: Vec::new(),
        blocked,
        depends_on: depends_on.to_vec(),
        summary: format!(
            "cardinality {} -> {}",
            cardinality_name(current.cardinality),
            cardinality_name(declaration.cardinality)
        ),
        recipe: if blocked.is_some() {
            vec!["choose a winning value per conflicting entity, retract the rest".to_owned()]
        } else {
            Vec::new()
        },
        observations,
        notes: if blocked.is_some() {
            vec!["a winner is never chosen implicitly".to_owned()]
        } else {
            Vec::new()
        },
    }
}

fn index_step(
    current: &InstalledAttribute,
    declaration: &DesiredAttribute,
    db: &Db,
    depends_on: &[String],
) -> PlanStep {
    let impact = attribute_impact(db, current.id);
    let enabling = declaration.indexed;
    let blocked = (enabling && current.ever_protected).then_some(BlockedReason::EverProtected);
    let mut observations = vec![Observation::count("distinct-values", impact.distinct_values)];
    let mut notes = Vec::new();
    if enabling {
        observations.insert(0, Observation::count("avet-backfill-datoms", impact.datoms));
        notes.push(
            "coverage is requested first and becomes ready only once the backfill reaches its \
             basis; queries use the AEVT fallback until then"
                .to_owned(),
        );
    } else {
        notes.push(
            "the planner stops reading through AVET immediately; a rebuild reclaims the stale \
             coverage"
                .to_owned(),
        );
    }
    PlanStep {
        key: PlanStep::key_for(&current.ident, ChangeKind::Property(SchemaProperty::Index)),
        ident: current.ident.clone(),
        kind: ChangeKind::Property(SchemaProperty::Index),
        installed: Some(flag(current.indexed).to_owned()),
        desired: Some(flag(declaration.indexed).to_owned()),
        // Existing facts stay valid; the work is a covering-index build.
        class: ExecutionClass::ValidateReindex,
        risk: Risk::Low,
        acks: Vec::new(),
        blocked,
        depends_on: depends_on.to_vec(),
        summary: format!(
            "index {} -> {}",
            flag(current.indexed),
            flag(declaration.indexed)
        ),
        recipe: Vec::new(),
        observations,
        notes,
    }
}

fn unique_step(
    current: &InstalledAttribute,
    declaration: &DesiredAttribute,
    db: &Db,
    sample_limit: usize,
    depends_on: &[String],
) -> PlanStep {
    let installed_mode = unique_name(current.unique);
    let desired_mode = unique_name(declaration.unique);
    let mut observations = Vec::new();
    let mut notes = Vec::new();
    let mut acks = Vec::new();
    let mut blocked = None;
    let mut risk = Risk::Medium;

    match (current.unique, declaration.unique) {
        (None, Some(_)) => {
            let mut duplicates = duplicate_values(db, current.id, sample_limit);
            observations.push(Observation::count("duplicate-values", duplicates.groups));
            observations.push(Observation::count("duplicate-entities", duplicates.entities));
            let sample = std::mem::take(&mut duplicates.sample);
            if !sample.is_empty() {
                observations.push(Observation::entities("duplicate-entity-sample", sample));
            }
            if current.ever_protected {
                blocked = Some(BlockedReason::EverProtected);
                risk = Risk::High;
            } else if !duplicates.is_empty() {
                blocked = Some(BlockedReason::UniqueDuplicates);
                risk = Risk::High;
            }
            notes.push(
                "the constraint is installed pending: new writes are checked through the AEVT \
                 fallback while AVET is rebuilt, and the tail is revalidated before activation"
                    .to_owned(),
            );
        }
        (Some(_), None) => {
            let impact = attribute_impact(db, current.id);
            observations.push(Observation::count("current-datoms", impact.datoms));
            notes.push(if declaration.indexed {
                "AVET coverage is still requested, so nothing is rebuilt".to_owned()
            } else {
                "AVET coverage is no longer requested and is rebuilt without it".to_owned()
            });
        }
        (Some(_), Some(_)) => {
            // Upsert versus conflict is a write-semantics change with no data
            // work at all; the whole current population is affected.
            let impact = attribute_impact(db, current.id);
            observations.push(Observation::count("current-datoms", impact.datoms));
            observations.push(Observation::count("current-entities", impact.entities));
            acks.push(AckCode::UniqueModeChange);
            risk = Risk::High;
        }
        (None, None) => unreachable!("unique_step is only reached for a difference"),
    }

    PlanStep {
        key: PlanStep::key_for(&current.ident, ChangeKind::Property(SchemaProperty::Unique)),
        ident: current.ident.clone(),
        kind: ChangeKind::Property(SchemaProperty::Unique),
        installed: Some(installed_mode.to_owned()),
        desired: Some(desired_mode.to_owned()),
        class: ExecutionClass::ValidateReindex,
        risk,
        acks,
        blocked,
        depends_on: depends_on.to_vec(),
        summary: format!("unique {installed_mode} -> {desired_mode}"),
        recipe: if blocked == Some(BlockedReason::UniqueDuplicates) {
            vec!["resolve the duplicate values, then replan".to_owned()]
        } else {
            Vec::new()
        },
        observations,
        notes,
    }
}

fn component_step(
    current: &InstalledAttribute,
    declaration: &DesiredAttribute,
    db: &Db,
    depends_on: &[String],
) -> PlanStep {
    let refs = live_refs(db, current.id);
    let enabling = declaration.is_component;
    PlanStep {
        key: PlanStep::key_for(
            &current.ident,
            ChangeKind::Property(SchemaProperty::Component),
        ),
        ident: current.ident.clone(),
        kind: ChangeKind::Property(SchemaProperty::Component),
        installed: Some(flag(current.is_component).to_owned()),
        desired: Some(flag(declaration.is_component).to_owned()),
        // Metadata-only, yet every live reference changes meaning, so the
        // fan-out is measured before the flag is accepted.
        class: ExecutionClass::ValidateReindex,
        risk: if refs > 0 { Risk::High } else { Risk::Medium },
        acks: vec![if enabling {
            AckCode::ComponentEnable
        } else {
            AckCode::ComponentDisable
        }],
        blocked: None,
        depends_on: depends_on.to_vec(),
        summary: format!(
            "component {} -> {}",
            flag(current.is_component),
            flag(declaration.is_component)
        ),
        recipe: Vec::new(),
        observations: vec![Observation::count("live-refs", refs)],
        notes: vec![
            "existing facts are not rewritten; future pull and retract-entity semantics change"
                .to_owned(),
        ],
    }
}

fn no_history_step(
    current: &InstalledAttribute,
    declaration: &DesiredAttribute,
    db: &Db,
    depends_on: &[String],
) -> PlanStep {
    let history = history_impact(db, current.id);
    let enabling = declaration.no_history;
    let mut notes = Vec::new();
    if enabling {
        notes.push(
            "forward-only: history already recorded is kept, and none is recorded from this \
             basis onward"
                .to_owned(),
        );
    } else {
        notes.push(format!(
            "history resumes at this basis; the interval up to basis {} stays omitted and cannot \
             be reconstructed",
            db.basis_t()
        ));
    }
    PlanStep {
        key: PlanStep::key_for(
            &current.ident,
            ChangeKind::Property(SchemaProperty::NoHistory),
        ),
        ident: current.ident.clone(),
        kind: ChangeKind::Property(SchemaProperty::NoHistory),
        installed: Some(flag(current.no_history).to_owned()),
        desired: Some(flag(declaration.no_history).to_owned()),
        // Nothing stored is inspected or rewritten: the flag only decides
        // what later transactions retain.
        class: ExecutionClass::Additive,
        risk: if enabling { Risk::High } else { Risk::Medium },
        acks: vec![if enabling {
            AckCode::NoHistoryEnable
        } else {
            AckCode::NoHistoryDisable
        }],
        blocked: None,
        depends_on: depends_on.to_vec(),
        summary: format!(
            "no-history {} -> {}",
            flag(current.no_history),
            flag(declaration.no_history)
        ),
        recipe: Vec::new(),
        observations: vec![history_observation(history)],
        notes,
    }
}

fn doc_step(
    current: &InstalledAttribute,
    declaration: &DesiredAttribute,
    depends_on: &[String],
) -> PlanStep {
    PlanStep {
        key: PlanStep::key_for(&current.ident, ChangeKind::Property(SchemaProperty::Doc)),
        ident: current.ident.clone(),
        kind: ChangeKind::Property(SchemaProperty::Doc),
        installed: current.doc.clone(),
        desired: declaration.doc.clone(),
        class: ExecutionClass::Additive,
        risk: Risk::Low,
        acks: Vec::new(),
        blocked: None,
        depends_on: depends_on.to_vec(),
        summary: match (&current.doc, &declaration.doc) {
            (None, Some(_)) => "doc added".to_owned(),
            (Some(_), None) => "doc retracted".to_owned(),
            _ => "doc replaced".to_owned(),
        },
        recipe: Vec::new(),
        observations: Vec::new(),
        notes: Vec::new(),
    }
}

fn history_observation(history: impact::HistoryImpact) -> Observation {
    if history.available {
        Observation::count("recorded-history-datoms", history.datoms)
    } else {
        Observation::unavailable(
            "recorded-history-datoms",
            "an exact historical count is not available from this database value",
        )
    }
}

