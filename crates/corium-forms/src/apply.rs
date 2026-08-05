//! Compiling a reviewed [`SchemaPlan`] into the datoms that install it.
//!
//! Applying a schema plan is not a transaction a client composes: it is the
//! deterministic image of a plan the operator already read
//! (`docs/design/schema-migrations.md`). This module is that function. Given
//! the plan, the desired schema it came from, and the database value it was
//! computed against, it emits the schema datoms — and nothing else.
//!
//! Two properties make the result safe to append directly, without going
//! through `corium_tx::prepare`:
//!
//! * every datom names a concrete attribute entity, so there is no tempid,
//!   upsert, or lookup ref to resolve; and
//! * every change is metadata about an attribute, so no user fact is read,
//!   rewritten, or removed.
//!
//! **Clearing a seeded property.** Attributes supplied at database creation
//! form the immutable pre-basis schema seed: they are real schema, but they
//! are not datoms. Turning off a property the seed set therefore has nothing
//! to retract, so the compiler emits the retraction naming the *installed*
//! value anyway. Folding it removes nothing (no such key is live) and derives
//! the cleared property, which is exactly the intent. A history view shows
//! that retraction without a matching assertion — the honest rendering of
//! "this was true from the seed, and stopped being true here".

use corium_core::migration::{ChangeKind, SchemaPlan, SchemaProperty};
use corium_core::{
    AttrId, Cardinality, Datom, EntityId, Keyword, KeywordInterner, Partition, Unique, Value,
    ValueType, migration::InstalledSchema,
};
use corium_db::{Db, bootstrap};
use thiserror::Error;

use crate::desired::{DesiredAttribute, DesiredSchema};
use crate::schemaform::FIRST_ATTR_ID;

/// A plan that cannot be turned into a schema transaction.
#[derive(Clone, Debug, Eq, PartialEq, Error)]
pub enum ApplyError {
    /// A step names an attribute the desired schema does not declare, so
    /// there is nothing to install. The plan and the file disagree.
    #[error("{0} is planned but not declared by the schema file")]
    UndeclaredAttribute(String),
    /// A step names an installed attribute that is no longer there.
    #[error("{0} is planned but is no longer installed")]
    MissingAttribute(String),
    /// A step this command can never execute reached the compiler.
    #[error("{ident} {summary}: this change cannot be executed")]
    NotExecutable {
        /// Attribute the step names.
        ident: String,
        /// The step's one-line summary.
        summary: String,
    },
    /// `:db/protection` names a class the database does not have.
    ///
    /// A class is an entity with a key identity and sealing policy. Schema
    /// update can point an attribute at one, but installing a class is
    /// create-time work.
    #[error("attribute {attr} names protection class {class}, which this database does not have")]
    UnknownProtectionClass {
        /// The attribute carrying the `:db/protection`.
        attr: String,
        /// The class it names.
        class: String,
    },
    /// Attribute ids ran past the db partition's sequence range.
    #[error("the db partition has no attribute id left to allocate")]
    IdSpaceExhausted,
}

/// The schema transaction a plan compiles to.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SchemaTransaction {
    /// The datoms to append, in deterministic step order.
    pub datoms: Vec<Datom>,
    /// Attributes installed by this transaction, as `(ident, id)`.
    ///
    /// Reported so the caller can tell an operator which ids were allocated
    /// without decoding the datoms back.
    pub installed: Vec<(String, AttrId)>,
}

impl SchemaTransaction {
    /// Whether the transaction would change nothing.
    ///
    /// Re-applying the same desired schema to a database that already matches
    /// it compiles to no datoms, which is what makes apply idempotent.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.datoms.is_empty()
    }
}

/// The audit record an applied schema update writes on its transaction entity.
///
/// Counts and digests, never application values: a plan may carry samples for
/// diagnosis, but the durable audit trail must not become a copy of the data
/// the change touched.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AuditRecord {
    /// Identity that requested the change.
    pub requester: String,
    /// Tool and protocol version that submitted it.
    pub tool: String,
}

/// Compiles `plan` into the datoms that install it.
///
/// `tx` is the transaction entity the datoms are attributed to, and `t` its
/// basis. `interner` gains any keyword the vocabulary needs; the caller is
/// responsible for making those names durable before the datoms that use them,
/// exactly as it already is for keyword values in ordinary transactions.
///
/// # Errors
/// Returns [`ApplyError`] when the plan and the desired schema disagree, when
/// a step cannot be executed, or when a named protection class is not
/// installed.
pub fn compile(
    plan: &SchemaPlan,
    desired: &DesiredSchema,
    db: &Db,
    installed: &InstalledSchema,
    interner: &mut KeywordInterner,
    tx: EntityId,
) -> Result<SchemaTransaction, ApplyError> {
    let mut out = SchemaTransaction::default();
    let mut next_id = next_attribute_id(db, installed)?;

    for step in &plan.steps {
        if step.blocked.is_some() || !step.class.executable() {
            return Err(ApplyError::NotExecutable {
                ident: step.ident.clone(),
                summary: step.summary.clone(),
            });
        }
        match step.kind {
            ChangeKind::Add => {
                let declaration = desired
                    .get(&step.ident)
                    .ok_or_else(|| ApplyError::UndeclaredAttribute(step.ident.clone()))?;
                let id = next_id;
                next_id = advance(next_id)?;
                out.datoms.extend(install_attribute(
                    id,
                    declaration,
                    db,
                    interner,
                    tx,
                    &step.ident,
                )?);
                out.installed.push((step.ident.clone(), id));
            }
            ChangeKind::Retire => {
                let current = installed
                    .get(&step.ident)
                    .ok_or_else(|| ApplyError::MissingAttribute(step.ident.clone()))?;
                out.datoms.push(assert(
                    current.id,
                    bootstrap::RETIRED,
                    Value::Bool(true),
                    tx,
                ));
            }
            ChangeKind::Property(property) => {
                let current = installed
                    .get(&step.ident)
                    .ok_or_else(|| ApplyError::MissingAttribute(step.ident.clone()))?;
                let declaration = desired
                    .get(&step.ident)
                    .ok_or_else(|| ApplyError::UndeclaredAttribute(step.ident.clone()))?;
                out.datoms.extend(change_property(
                    property,
                    current.id,
                    installed_value(property, &step.ident, installed, interner),
                    declaration,
                    db,
                    interner,
                    tx,
                    &step.ident,
                )?);
            }
        }
    }
    Ok(out)
}

/// The audit datoms an applied schema update attaches to its own transaction.
#[must_use]
pub fn audit_datoms(plan: &SchemaPlan, audit: &AuditRecord, tx: EntityId) -> Vec<Datom> {
    let mut datoms = vec![
        assert(
            tx,
            bootstrap::UPDATE_DESIRED_DIGEST,
            Value::Str(plan.desired_digest.as_str().into()),
            tx,
        ),
        assert(
            tx,
            bootstrap::UPDATE_PLAN_DIGEST,
            Value::Str(plan.digest().as_str().into()),
            tx,
        ),
        assert(
            tx,
            bootstrap::UPDATE_OBSERVED_BASIS,
            Value::Long(i64::try_from(plan.basis_t).unwrap_or(i64::MAX)),
            tx,
        ),
    ];
    if !audit.requester.is_empty() {
        datoms.push(assert(
            tx,
            bootstrap::UPDATE_REQUESTER,
            Value::Str(audit.requester.as_str().into()),
            tx,
        ));
    }
    if !audit.tool.is_empty() {
        datoms.push(assert(
            tx,
            bootstrap::UPDATE_TOOL,
            Value::Str(audit.tool.as_str().into()),
            tx,
        ));
    }
    for class in plan
        .steps
        .iter()
        .map(|step| step.class)
        .collect::<std::collections::BTreeSet<_>>()
    {
        datoms.push(assert(
            tx,
            bootstrap::UPDATE_CLASS,
            Value::Str(class.as_str().into()),
            tx,
        ));
    }
    for ack in plan.required_acks() {
        datoms.push(assert(
            tx,
            bootstrap::UPDATE_ACK,
            Value::Str(ack.as_str().into()),
            tx,
        ));
    }
    datoms
}

/// The lowest attribute id no installed entity in the db partition uses.
///
/// Database *creation* keeps its positional allocation, so embedded, WASM,
/// authz-bootstrap and fixture databases stay byte-for-byte reproducible.
/// Updates allocate above the durable maximum instead — the ident registry is
/// consulted as well as the schema, because a protection class is a named db
/// entity that holds no attribute record.
fn next_attribute_id(db: &Db, installed: &InstalledSchema) -> Result<AttrId, ApplyError> {
    let db_partition = Partition::Db as u32;
    let mut highest = FIRST_ATTR_ID;
    let mut consider = |id: EntityId| {
        if id.partition() == db_partition {
            highest = highest.max(id.sequence() + 1);
        }
    };
    for (id, _) in db.schema().iter() {
        consider(*id);
    }
    for (class, _) in db.schema().classes() {
        consider(*class);
    }
    for (_, id) in db.idents().iter() {
        consider(*id);
    }
    for attribute in installed.iter() {
        consider(attribute.id);
    }
    let id = EntityId::new(db_partition, highest);
    if id.sequence() != highest {
        return Err(ApplyError::IdSpaceExhausted);
    }
    Ok(id)
}

fn advance(id: AttrId) -> Result<AttrId, ApplyError> {
    let next = EntityId::new(id.partition(), id.sequence() + 1);
    if next.sequence() != id.sequence() + 1 {
        return Err(ApplyError::IdSpaceExhausted);
    }
    Ok(next)
}

/// The datoms installing a brand-new attribute.
///
/// Only the properties that differ from the engine's defaults are written, so
/// the stored metadata of an attribute reads as its declaration rather than as
/// a form with every field filled in.
fn install_attribute(
    id: AttrId,
    declaration: &DesiredAttribute,
    db: &Db,
    interner: &mut KeywordInterner,
    tx: EntityId,
    ident: &str,
) -> Result<Vec<Datom>, ApplyError> {
    let mut datoms = vec![
        assert(
            id,
            bootstrap::IDENT,
            keyword_value(interner, declaration.ident.clone()),
            tx,
        ),
        assert(
            id,
            bootstrap::VALUE_TYPE,
            value_type_value(interner, declaration.value_type),
            tx,
        ),
    ];
    if declaration.cardinality == Cardinality::Many {
        datoms.push(assert(
            id,
            bootstrap::CARDINALITY,
            cardinality_value(interner, Cardinality::Many),
            tx,
        ));
    }
    if let Some(unique) = declaration.unique {
        datoms.push(assert(
            id,
            bootstrap::UNIQUE,
            unique_value(interner, unique),
            tx,
        ));
    } else if declaration.indexed {
        // Uniqueness already implies coverage; writing both would store a
        // redundant fact.
        datoms.push(assert(id, bootstrap::INDEX, Value::Bool(true), tx));
    }
    if declaration.is_component {
        datoms.push(assert(id, bootstrap::IS_COMPONENT, Value::Bool(true), tx));
    }
    if declaration.no_history {
        datoms.push(assert(id, bootstrap::NO_HISTORY, Value::Bool(true), tx));
    }
    if let Some(doc) = &declaration.doc {
        datoms.push(assert(
            id,
            bootstrap::DOC,
            Value::Str(doc.as_str().into()),
            tx,
        ));
    }
    if let Some(class) = &declaration.protection {
        datoms.push(assert(
            id,
            bootstrap::PROTECTION,
            Value::Ref(resolve_class(db, class, ident)?),
            tx,
        ));
    }
    Ok(datoms)
}

/// The datoms changing one property of an installed attribute.
#[allow(clippy::too_many_arguments)]
fn change_property(
    property: SchemaProperty,
    id: AttrId,
    installed: Option<Value>,
    declaration: &DesiredAttribute,
    db: &Db,
    interner: &mut KeywordInterner,
    tx: EntityId,
    ident: &str,
) -> Result<Vec<Datom>, ApplyError> {
    let a = attribute_of(property);
    let desired = match property {
        SchemaProperty::ValueType => {
            // The planner classifies this as destructive and never emits an
            // executable step; reaching here means the plan was built by
            // something that did not.
            return Err(ApplyError::NotExecutable {
                ident: ident.to_owned(),
                summary: "value type mutation".to_owned(),
            });
        }
        SchemaProperty::Cardinality => Some(cardinality_value(interner, declaration.cardinality)),
        SchemaProperty::Unique => declaration
            .unique
            .map(|unique| unique_value(interner, unique)),
        // `:db/index` is a boolean, so turning coverage off is an assertion of
        // `false` rather than a retraction: the requested state stays legible
        // in the log either way.
        SchemaProperty::Index => Some(Value::Bool(declaration.indexed)),
        SchemaProperty::Component => Some(Value::Bool(declaration.is_component)),
        SchemaProperty::NoHistory => Some(Value::Bool(declaration.no_history)),
        SchemaProperty::Doc => declaration
            .doc
            .as_ref()
            .map(|doc| Value::Str(doc.as_str().into())),
        SchemaProperty::Protection => match &declaration.protection {
            Some(class) => Some(Value::Ref(resolve_class(db, class, ident)?)),
            None => None,
        },
    };

    // Protection is the one property whose installed value lives in a
    // timeline rather than in the attribute record, so it is resolved from the
    // database. Unprotecting *must* produce a datom — without one the
    // transaction would carry no evidence that sealing stopped, and the
    // timeline would never gain its closing entry.
    let installed = if property == SchemaProperty::Protection {
        db.schema().protection(id).current().map(Value::Ref)
    } else {
        installed
    };

    let mut datoms = Vec::new();
    // Retract the installed value first. For a cardinality-one attribute this
    // is what a commit would record anyway; for a property that came from the
    // pre-basis seed it is what makes "off" expressible at all.
    if let Some(installed) = installed
        && Some(&installed) != desired.as_ref()
    {
        datoms.push(retract(id, a, installed, tx));
    }
    if let Some(desired) = desired {
        datoms.push(assert(id, a, desired, tx));
    }
    Ok(datoms)
}

/// The value the installed schema currently holds for `property`.
///
/// `None` means the property holds nothing to retract — a boolean that is
/// `false`, or an absent uniqueness mode, documentation, or class.
fn installed_value(
    property: SchemaProperty,
    ident: &str,
    installed: &InstalledSchema,
    interner: &mut KeywordInterner,
) -> Option<Value> {
    let current = installed.get(ident)?;
    match property {
        SchemaProperty::ValueType => Some(value_type_value(interner, current.value_type)),
        SchemaProperty::Cardinality => Some(cardinality_value(interner, current.cardinality)),
        SchemaProperty::Unique => current.unique.map(|unique| unique_value(interner, unique)),
        SchemaProperty::Index => current.indexed.then_some(Value::Bool(true)),
        SchemaProperty::Component => current.is_component.then_some(Value::Bool(true)),
        SchemaProperty::NoHistory => current.no_history.then_some(Value::Bool(true)),
        SchemaProperty::Doc => current
            .doc
            .as_ref()
            .map(|doc| Value::Str(doc.as_str().into())),
        // Resolved from the protection timeline in `change_property`, not from
        // the attribute record.
        SchemaProperty::Protection => None,
    }
}

/// The vocabulary attribute a property is stored under.
const fn attribute_of(property: SchemaProperty) -> AttrId {
    match property {
        SchemaProperty::ValueType => bootstrap::VALUE_TYPE,
        SchemaProperty::Cardinality => bootstrap::CARDINALITY,
        SchemaProperty::Unique => bootstrap::UNIQUE,
        SchemaProperty::Index => bootstrap::INDEX,
        SchemaProperty::Component => bootstrap::IS_COMPONENT,
        SchemaProperty::NoHistory => bootstrap::NO_HISTORY,
        SchemaProperty::Doc => bootstrap::DOC,
        SchemaProperty::Protection => bootstrap::PROTECTION,
    }
}

fn resolve_class(db: &Db, class: &Keyword, attr: &str) -> Result<EntityId, ApplyError> {
    let id = db
        .idents()
        .entid(class)
        .ok_or_else(|| ApplyError::UnknownProtectionClass {
            attr: attr.to_owned(),
            class: class.to_string(),
        })?;
    if db.schema().class(id).is_none() {
        return Err(ApplyError::UnknownProtectionClass {
            attr: attr.to_owned(),
            class: class.to_string(),
        });
    }
    Ok(id)
}

fn keyword_value(interner: &mut KeywordInterner, keyword: Keyword) -> Value {
    Value::Keyword(interner.intern(keyword))
}

fn value_type_value(interner: &mut KeywordInterner, value_type: ValueType) -> Value {
    keyword_value(
        interner,
        Keyword::new(
            Some("db.type"),
            corium_core::migration::value_type_name(value_type),
        ),
    )
}

fn cardinality_value(interner: &mut KeywordInterner, cardinality: Cardinality) -> Value {
    keyword_value(
        interner,
        Keyword::new(
            Some("db.cardinality"),
            corium_core::migration::cardinality_name(cardinality),
        ),
    )
}

fn unique_value(interner: &mut KeywordInterner, unique: Unique) -> Value {
    keyword_value(
        interner,
        Keyword::new(
            Some("db.unique"),
            corium_core::migration::unique_name(Some(unique)),
        ),
    )
}

fn assert(e: EntityId, a: AttrId, v: Value, tx: EntityId) -> Datom {
    Datom {
        e,
        a,
        v,
        tx,
        added: true,
    }
}

fn retract(e: EntityId, a: AttrId, v: Value, tx: EntityId) -> Datom {
    Datom {
        e,
        a,
        v,
        tx,
        added: false,
    }
}
