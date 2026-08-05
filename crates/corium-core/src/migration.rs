//! Property-level schema change and plan types shared by clients and the
//! transactor (`docs/design/schema-migrations.md`).
//!
//! A schema update is described as a set of **property-level changes** rather
//! than an attribute-level `changed` flag: one attribute can need both
//! uniqueness validation and an AVET backfill, and those are separate units of
//! work with separate preconditions. Every change carries an
//! [`ExecutionClass`] (what must happen to existing facts), a [`Risk`] (how
//! much meaning changes), the acknowledgement codes it demands, and the
//! observations measured at one basis.
//!
//! Observations are deliberately *not* part of [`SchemaPlan::digest`]. A plan
//! is invalidated by schema drift or a failed safety precondition, not by an
//! unrelated write that moves a count — otherwise a busy database could never
//! add an attribute.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use sha2::{Digest, Sha256};

use crate::{AttrId, Cardinality, EntityId, Unique, ValueType};

/// Version of the machine-readable plan contract (`--json`).
pub const PLAN_FORMAT_VERSION: u32 = 1;

/// How a change affects facts already stored in the database.
///
/// The classes are ordered by the authority they demand: `additive` needs no
/// allowance, and `destructive` has none because `schema update` cannot run
/// one.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum ExecutionClass {
    /// No existing fact must be inspected or rewritten.
    Additive,
    /// Existing facts remain valid, but a bounded scan, constraint validation,
    /// or covering-index rebuild is required.
    ValidateReindex,
    /// Current facts must change before the desired schema can become active.
    Rewrite,
    /// Information or historical interpretation would be lost.
    Destructive,
}

impl ExecutionClass {
    /// Stable wire/flag name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Additive => "additive",
            Self::ValidateReindex => "validate-reindex",
            Self::Rewrite => "rewrite",
            Self::Destructive => "destructive",
        }
    }

    /// Parses a stable name, as accepted by `--allow`.
    #[must_use]
    pub fn parse(text: &str) -> Option<Self> {
        match text {
            "additive" => Some(Self::Additive),
            "validate-reindex" => Some(Self::ValidateReindex),
            "rewrite" => Some(Self::Rewrite),
            "destructive" => Some(Self::Destructive),
            _ => None,
        }
    }

    /// Whether a change of this class may be applied without `--allow`.
    #[must_use]
    pub const fn allowed_by_default(self) -> bool {
        matches!(self, Self::Additive)
    }

    /// Whether `schema update` can ever execute a change of this class.
    ///
    /// `destructive` has no allowance flag: the planner emits a replacement
    /// recipe instead of a runnable step.
    #[must_use]
    pub const fn executable(self) -> bool {
        !matches!(self, Self::Destructive)
    }
}

impl fmt::Display for ExecutionClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// How much a change alters the meaning of existing data.
///
/// Reported separately from [`ExecutionClass`]: an AVET backfill can be
/// operationally expensive yet semantically low risk, while a metadata-only
/// `isComponent` flip can be cheap and high risk.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum Risk {
    /// Nothing already stored changes meaning.
    Low,
    /// Future write or read behaviour changes.
    Medium,
    /// Existing facts acquire new semantics, or information becomes
    /// unreachable.
    High,
}

impl Risk {
    /// Stable wire name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
        }
    }
}

impl fmt::Display for Risk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A stable, kebab-case acknowledgement code.
///
/// Allowing an execution class says what work may run. An acknowledgement says
/// the operator understood what the change *means*. Both human and JSON plans
/// print the exact code next to every change that requires one.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum AckCode {
    /// An attribute acquires component cascade semantics.
    ComponentEnable,
    /// An attribute loses component cascade semantics.
    ComponentDisable,
    /// Uniqueness switches between `identity` and `value` upsert/conflict
    /// behaviour.
    UniqueModeChange,
    /// History storage is switched off; the omitted interval starts here.
    NoHistoryEnable,
    /// History storage is switched back on; the omitted interval cannot be
    /// reconstructed.
    NoHistoryDisable,
    /// An attribute with live facts is retired.
    RetireLiveAttribute,
    /// A protection-class change is forward-only.
    ProtectionForwardOnly,
}

impl AckCode {
    /// Stable code as printed in plans and accepted by `--ack`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ComponentEnable => "component-enable",
            Self::ComponentDisable => "component-disable",
            Self::UniqueModeChange => "unique-mode-change",
            Self::NoHistoryEnable => "no-history-enable",
            Self::NoHistoryDisable => "no-history-disable",
            Self::RetireLiveAttribute => "retire-live-attribute",
            Self::ProtectionForwardOnly => "protection-forward-only",
        }
    }

    /// Parses a stable code supplied to `--ack`.
    #[must_use]
    pub fn parse(text: &str) -> Option<Self> {
        [
            Self::ComponentEnable,
            Self::ComponentDisable,
            Self::UniqueModeChange,
            Self::NoHistoryEnable,
            Self::NoHistoryDisable,
            Self::RetireLiveAttribute,
            Self::ProtectionForwardOnly,
        ]
        .into_iter()
        .find(|code| code.as_str() == text)
    }

    /// One-line explanation of what acknowledging this code accepts.
    #[must_use]
    pub const fn meaning(self) -> &'static str {
        match self {
            Self::ComponentEnable => {
                "existing references acquire cascade retract and pull semantics"
            }
            Self::ComponentDisable => "existing references lose cascade retract and pull semantics",
            Self::UniqueModeChange => "upsert and conflict behaviour changes for future writes",
            Self::NoHistoryEnable => "history stops being recorded from this transaction onward",
            Self::NoHistoryDisable => {
                "history resumes, but the interval already omitted cannot be reconstructed"
            }
            Self::RetireLiveAttribute => {
                "new assertions are refused while existing facts stay readable"
            }
            Self::ProtectionForwardOnly => {
                "protection changes are forward-only and cannot re-seal existing facts"
            }
        }
    }
}

impl fmt::Display for AckCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// An attribute property the planner compares.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum SchemaProperty {
    /// `:db/valueType`.
    ValueType,
    /// `:db/cardinality`.
    Cardinality,
    /// `:db/unique`.
    Unique,
    /// `:db/index`.
    Index,
    /// `:db/isComponent`.
    Component,
    /// `:db/noHistory`.
    NoHistory,
    /// `:db/doc`.
    Doc,
    /// `:db/protection` (attribute protection classes).
    Protection,
}

impl SchemaProperty {
    /// Stable wire name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ValueType => "value-type",
            Self::Cardinality => "cardinality",
            Self::Unique => "unique",
            Self::Index => "index",
            Self::Component => "component",
            Self::NoHistory => "no-history",
            Self::Doc => "doc",
            Self::Protection => "protection",
        }
    }

    /// The `:db/…` ident this property is stored under.
    #[must_use]
    pub const fn ident(self) -> &'static str {
        match self {
            Self::ValueType => ":db/valueType",
            Self::Cardinality => ":db/cardinality",
            Self::Unique => ":db/unique",
            Self::Index => ":db/index",
            Self::Component => ":db/isComponent",
            Self::NoHistory => ":db/noHistory",
            Self::Doc => ":db/doc",
            Self::Protection => ":db/protection",
        }
    }
}

impl fmt::Display for SchemaProperty {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What a plan step does to an attribute.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum ChangeKind {
    /// Install an attribute the database does not have.
    Add,
    /// Retire an installed attribute (`--prune`).
    Retire,
    /// Change one property of an installed attribute.
    Property(SchemaProperty),
}

impl ChangeKind {
    /// Stable wire name; property changes are named by their property.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Add => "add",
            Self::Retire => "retire",
            Self::Property(property) => property.as_str(),
        }
    }
}

/// Why a step can never be executed by `schema update`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BlockedReason {
    /// Rewriting an attribute's value type in place would reinterpret history
    /// and invalidate old retractions and CAS operands.
    ValueTypeMutation,
    /// Uniqueness cannot be installed while duplicate values exist.
    UniqueDuplicates,
    /// Cardinality cannot collapse while entities hold several values; a
    /// value-selection policy is required and is never inferred.
    CardinalityConflicts,
    /// An attribute that has ever been protected can never gain AVET
    /// coverage: its sealed datoms cannot be ordered by value.
    EverProtected,
    /// A declaration asks for protection alongside something protected datoms
    /// cannot have. Ciphertext order is not value order, so a protected
    /// attribute is absent from AVET and VAET: it can be neither indexed nor
    /// unique, and a `ref` can never be protected at all (ADR-0018).
    ProtectionConflict,
}

impl BlockedReason {
    /// Stable wire code.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ValueTypeMutation => "value-type-mutation",
            Self::UniqueDuplicates => "unique-duplicates",
            Self::CardinalityConflicts => "cardinality-conflicts",
            Self::EverProtected => "ever-protected",
            Self::ProtectionConflict => "protection-conflict",
        }
    }

    /// Human explanation printed under the change.
    #[must_use]
    pub const fn message(self) -> &'static str {
        match self {
            Self::ValueTypeMutation => "direct type mutation is not executable",
            Self::UniqueDuplicates => "duplicate values must be resolved before uniqueness",
            Self::CardinalityConflicts => {
                "an explicit value-selection policy is required for conflicting entities"
            }
            Self::EverProtected => {
                "an attribute that has ever been protected can never gain index or unique coverage"
            }
            Self::ProtectionConflict => {
                "a protected attribute cannot be indexed, unique, or a reference"
            }
        }
    }
}

impl fmt::Display for BlockedReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A measurement taken at the observed basis.
///
/// Advisory review and audit data: carried beside the logical plan, never
/// hashed into its digest.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Observation {
    /// Stable label, for example `current-datoms`.
    pub label: String,
    /// The measured value.
    pub value: ObservationValue,
}

impl Observation {
    /// An exact count.
    #[must_use]
    pub fn count(label: &str, count: u64) -> Self {
        Self {
            label: label.to_owned(),
            value: ObservationValue::Count(count),
        }
    }

    /// A bounded sample of entity ids.
    #[must_use]
    pub fn entities(label: &str, entities: Vec<EntityId>) -> Self {
        Self {
            label: label.to_owned(),
            value: ObservationValue::Entities(entities),
        }
    }

    /// A measurement this database value cannot answer exactly.
    #[must_use]
    pub fn unavailable(label: &str, why: &str) -> Self {
        Self {
            label: label.to_owned(),
            value: ObservationValue::Unavailable(why.to_owned()),
        }
    }
}

/// The value of an [`Observation`].
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ObservationValue {
    /// An exact count at the observed basis. Counts are never truncated.
    Count(u64),
    /// An approximate figure, explicitly labelled as such.
    Estimate(u64),
    /// A bounded sample of representative entity ids.
    Entities(Vec<EntityId>),
    /// The measurement is not available from this database value.
    Unavailable(String),
}

impl fmt::Display for ObservationValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Count(count) => f.write_str(&group_digits(*count)),
            Self::Estimate(count) => write!(f, "~{}", group_digits(*count)),
            Self::Entities(entities) => {
                let rendered: Vec<String> = entities
                    .iter()
                    .map(|entity| entity.sequence().to_string())
                    .collect();
                f.write_str(&rendered.join(", "))
            }
            Self::Unavailable(why) => write!(f, "unavailable ({why})"),
        }
    }
}

/// Renders a count with thousands separators, as plans print them.
#[must_use]
pub fn group_digits(value: u64) -> String {
    let digits = value.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            out.push(',');
        }
        out.push(digit);
    }
    out
}

/// One unit of planned work.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlanStep {
    /// Stable step key (`:person/email#unique`), used to resume a partially
    /// completed plan rather than replay it.
    pub key: String,
    /// Canonical ident the step applies to.
    pub ident: String,
    /// What the step changes.
    pub kind: ChangeKind,
    /// Rendered installed value, absent when the attribute is being added.
    pub installed: Option<String>,
    /// Rendered desired value, absent when the attribute is being retired.
    pub desired: Option<String>,
    /// What must happen to existing facts.
    pub class: ExecutionClass,
    /// How much meaning changes.
    pub risk: Risk,
    /// Acknowledgement codes this change requires.
    pub acks: Vec<AckCode>,
    /// Set when the step can never be executed by `schema update`.
    pub blocked: Option<BlockedReason>,
    /// Step keys that must complete first.
    pub depends_on: Vec<String>,
    /// One-line human description (`cardinality one -> many`).
    pub summary: String,
    /// Advisory recipe lines for changes this command refuses to run.
    pub recipe: Vec<String>,
    /// Measurements at the observed basis. Not digested.
    pub observations: Vec<Observation>,
    /// Advisory notes, such as history limitations. Not digested.
    pub notes: Vec<String>,
}

impl PlanStep {
    /// Builds the stable key for a change.
    #[must_use]
    pub fn key_for(ident: &str, kind: ChangeKind) -> String {
        format!("{ident}#{}", kind.as_str())
    }
}

/// A deterministic, read-only schema update plan at one observed basis.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SchemaPlan {
    /// Database the plan was computed against.
    pub database: String,
    /// Basis the installed schema and observations were read at.
    pub basis_t: u64,
    /// Schema generation of the installed schema, when the database tracks
    /// one. `None` until schema transactions land.
    pub schema_generation: Option<u64>,
    /// Digest of the normalized desired schema.
    pub desired_digest: String,
    /// Fingerprint of the installed schema the plan was computed against.
    pub installed_fingerprint: String,
    /// Whether absent installed attributes were turned into retirements.
    pub prune: bool,
    /// Steps in deterministic (ident, property) order.
    pub steps: Vec<PlanStep>,
    /// Installed user attributes the desired file does not mention.
    pub unmanaged: Vec<String>,
    /// Plan-level advisory notes. Not digested.
    pub notes: Vec<String>,
}

impl SchemaPlan {
    /// Whether the plan contains any change at all.
    #[must_use]
    pub fn has_changes(&self) -> bool {
        !self.steps.is_empty()
    }

    /// Execution classes above `additive` that the plan contains.
    #[must_use]
    pub fn required_classes(&self) -> BTreeSet<ExecutionClass> {
        self.steps
            .iter()
            .map(|step| step.class)
            .filter(|class| !class.allowed_by_default())
            .collect()
    }

    /// The `--allow` flags an apply would need.
    ///
    /// A `destructive` class is deliberately absent: it has no allowance, and
    /// offering one would suggest the change is a flag away from running.
    #[must_use]
    pub fn required_allowances(&self) -> BTreeSet<ExecutionClass> {
        self.required_classes()
            .into_iter()
            .filter(|class| class.executable())
            .collect()
    }

    /// Every acknowledgement code the plan requires.
    #[must_use]
    pub fn required_acks(&self) -> BTreeSet<AckCode> {
        self.steps
            .iter()
            .flat_map(|step| step.acks.iter().copied())
            .collect()
    }

    /// Steps this command can never execute.
    pub fn blocked_steps(&self) -> impl Iterator<Item = &PlanStep> {
        self.steps
            .iter()
            .filter(|step| step.blocked.is_some() || !step.class.executable())
    }

    /// Steps of one execution class, in plan order.
    pub fn steps_in(&self, class: ExecutionClass) -> impl Iterator<Item = &PlanStep> {
        self.steps.iter().filter(move |step| step.class == class)
    }

    /// The plan digest: a hash of the logical plan only.
    ///
    /// Covers the desired digest, the installed-schema fingerprint, the
    /// `--prune` mode, and every step's identity, class, risk, values,
    /// acknowledgements, blocked reason, and dependencies. It deliberately
    /// excludes the database name, the observed basis, and every observation,
    /// so that ordinary data writes cannot invalidate a plan whose
    /// preconditions still hold.
    #[must_use]
    pub fn digest(&self) -> String {
        let mut canonical = Canonical::default();
        canonical.field("corium.schema-plan");
        canonical.field(&PLAN_FORMAT_VERSION.to_string());
        canonical.field(&self.desired_digest);
        canonical.field(&self.installed_fingerprint);
        canonical.field(if self.prune { "prune" } else { "no-prune" });
        let mut ordered: Vec<&PlanStep> = self.steps.iter().collect();
        ordered.sort_by(|left, right| left.key.cmp(&right.key));
        canonical.field(&ordered.len().to_string());
        for step in ordered {
            canonical.field(&step.key);
            canonical.field(&step.ident);
            canonical.field(step.kind.as_str());
            canonical.optional(step.installed.as_deref());
            canonical.optional(step.desired.as_deref());
            canonical.field(step.class.as_str());
            canonical.field(step.risk.as_str());
            let mut acks: Vec<&str> = step.acks.iter().map(|ack| ack.as_str()).collect();
            acks.sort_unstable();
            canonical.field(&acks.len().to_string());
            for ack in acks {
                canonical.field(ack);
            }
            canonical.optional(step.blocked.map(BlockedReason::as_str));
            let mut depends: Vec<&str> = step.depends_on.iter().map(String::as_str).collect();
            depends.sort_unstable();
            canonical.field(&depends.len().to_string());
            for dependency in depends {
                canonical.field(dependency);
            }
        }
        let mut unmanaged: Vec<&str> = self.unmanaged.iter().map(String::as_str).collect();
        unmanaged.sort_unstable();
        canonical.field(&unmanaged.len().to_string());
        for ident in unmanaged {
            canonical.field(ident);
        }
        canonical.finish()
    }
}

/// Attribute state as installed in a database.
///
/// Richer than [`crate::Attribute`]: it carries the ident, the facts a desired
/// file cannot erase (whether the attribute has ever been protected), and the
/// retirement and readiness metadata schema updates need.
// The boolean properties are the schema model's own (`:db/index`,
// `:db/isComponent`, `:db/noHistory`) plus the two installed-only facts a
// desired file cannot erase; collapsing them into flag sets would only hide
// which property a change names.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstalledAttribute {
    /// Canonical ident, rendered (`:person/email`).
    pub ident: String,
    /// Attribute entity id. Never inferred from file order after creation.
    pub id: AttrId,
    /// Stored value type.
    pub value_type: ValueType,
    /// Cardinality.
    pub cardinality: Cardinality,
    /// Uniqueness mode.
    pub unique: Option<Unique>,
    /// Whether AVET coverage is requested.
    pub indexed: bool,
    /// Component reference flag.
    pub is_component: bool,
    /// Whether history storage is disabled.
    pub no_history: bool,
    /// Documentation, when the installed schema records it.
    pub doc: Option<String>,
    /// Whether the attribute has been retired.
    pub retired: bool,
    /// The protection class sealing new values now, by class ident.
    pub protection: Option<String>,
    /// Whether the attribute has ever held sealed datoms. Permanent: an
    /// ever-protected attribute can never later gain index or unique coverage.
    pub ever_protected: bool,
    /// Whether the engine owns the attribute. Engine attributes are never
    /// managed by a user file.
    pub engine: bool,
}

impl InstalledAttribute {
    /// A one-line rendering of the installed declaration, as plans print it.
    #[must_use]
    pub fn rendering(&self) -> String {
        use std::fmt::Write as _;

        let mut out = format!(
            "{} cardinality-{}",
            value_type_name(self.value_type),
            cardinality_name(self.cardinality)
        );
        if let Some(unique) = self.unique {
            let _ = write!(out, " unique-{}", unique_name(Some(unique)));
        } else if self.indexed {
            out.push_str(" indexed");
        }
        if self.is_component {
            out.push_str(" component");
        }
        if self.no_history {
            out.push_str(" no-history");
        }
        if let Some(class) = &self.protection {
            let _ = write!(out, " protected-{class}");
        }
        if self.retired {
            out.push_str(" retired");
        }
        out
    }

    /// The rendered protection class, or `none`.
    #[must_use]
    pub fn protection_name(&self) -> String {
        self.protection.clone().unwrap_or_else(|| "none".to_owned())
    }
}

/// The installed schema a plan is computed against.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct InstalledSchema {
    attrs: BTreeMap<String, InstalledAttribute>,
    doc_tracked: bool,
    generation: Option<u64>,
}

impl InstalledSchema {
    /// Creates an installed schema that does not yet track `:db/doc` or a
    /// schema generation.
    #[must_use]
    pub fn new(attrs: Vec<InstalledAttribute>) -> Self {
        Self {
            attrs: attrs
                .into_iter()
                .map(|attr| (attr.ident.clone(), attr))
                .collect(),
            doc_tracked: false,
            generation: None,
        }
    }

    /// Declares that the source records `:db/doc`, so documentation
    /// differences are plannable rather than unknown.
    #[must_use]
    pub const fn with_doc_tracking(mut self, tracked: bool) -> Self {
        self.doc_tracked = tracked;
        self
    }

    /// Attaches the schema generation this snapshot represents.
    #[must_use]
    pub const fn with_generation(mut self, generation: Option<u64>) -> Self {
        self.generation = generation;
        self
    }

    /// Whether documentation differences can be planned.
    #[must_use]
    pub const fn doc_tracked(&self) -> bool {
        self.doc_tracked
    }

    /// The schema generation, when tracked.
    #[must_use]
    pub const fn generation(&self) -> Option<u64> {
        self.generation
    }

    /// Looks up an installed attribute by rendered ident.
    #[must_use]
    pub fn get(&self, ident: &str) -> Option<&InstalledAttribute> {
        self.attrs.get(ident)
    }

    /// Iterates installed attributes in ident order.
    pub fn iter(&self) -> impl Iterator<Item = &InstalledAttribute> {
        self.attrs.values()
    }

    /// Iterates the attributes a user file may manage, in ident order.
    pub fn managed(&self) -> impl Iterator<Item = &InstalledAttribute> {
        self.attrs.values().filter(|attr| !attr.engine)
    }

    /// A fingerprint of the whole installed schema.
    ///
    /// The transactor compares this under its commit queue: any schema drift
    /// between plan and apply changes it and aborts the apply.
    #[must_use]
    pub fn fingerprint(&self) -> String {
        let mut canonical = Canonical::default();
        canonical.field("corium.installed-schema");
        canonical.field(&PLAN_FORMAT_VERSION.to_string());
        canonical.field(&self.attrs.len().to_string());
        for attr in self.attrs.values() {
            canonical.field(&attr.ident);
            canonical.field(&attr.id.partition().to_string());
            canonical.field(&attr.id.sequence().to_string());
            canonical.field(value_type_name(attr.value_type));
            canonical.field(cardinality_name(attr.cardinality));
            canonical.field(unique_name(attr.unique));
            canonical.field(flag(attr.indexed));
            canonical.field(flag(attr.is_component));
            canonical.field(flag(attr.no_history));
            canonical.optional(attr.doc.as_deref());
            canonical.field(flag(attr.retired));
            canonical.optional(attr.protection.as_deref());
            canonical.field(flag(attr.ever_protected));
            canonical.field(flag(attr.engine));
        }
        canonical.finish()
    }
}

/// Canonical name of a value type as schema files and plans spell it.
#[must_use]
pub const fn value_type_name(value_type: ValueType) -> &'static str {
    match value_type {
        ValueType::Bool => "boolean",
        ValueType::Long => "long",
        ValueType::Double => "double",
        ValueType::Instant => "instant",
        ValueType::Uuid => "uuid",
        ValueType::Keyword => "keyword",
        ValueType::Str => "string",
        ValueType::Bytes => "bytes",
        ValueType::Ref => "ref",
    }
}

/// Canonical name of a cardinality.
#[must_use]
pub const fn cardinality_name(cardinality: Cardinality) -> &'static str {
    match cardinality {
        Cardinality::One => "one",
        Cardinality::Many => "many",
    }
}

/// Canonical name of a uniqueness mode, or `none`.
#[must_use]
pub const fn unique_name(unique: Option<Unique>) -> &'static str {
    match unique {
        None => "none",
        Some(Unique::Identity) => "identity",
        Some(Unique::Value) => "value",
    }
}

/// Canonical name of a boolean property value.
#[must_use]
pub const fn flag(value: bool) -> &'static str {
    if value { "true" } else { "false" }
}

/// Length-prefixed canonical encoder.
///
/// Every field is written as `<byte length>:<bytes>\n`, so no combination of
/// idents, values, or documentation strings can produce two different plans
/// with one encoding.
#[derive(Default)]
struct Canonical {
    out: Vec<u8>,
}

impl Canonical {
    fn field(&mut self, value: &str) {
        self.out
            .extend_from_slice(value.len().to_string().as_bytes());
        self.out.push(b':');
        self.out.extend_from_slice(value.as_bytes());
        self.out.push(b'\n');
    }

    fn optional(&mut self, value: Option<&str>) {
        match value {
            Some(value) => {
                self.field("some");
                self.field(value);
            }
            None => self.field("none"),
        }
    }

    fn finish(self) -> String {
        digest_hex(&self.out)
    }
}

/// Hashes canonical bytes into a printable `sha256:` digest.
#[must_use]
pub fn digest_hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let hash = Sha256::digest(bytes);
    let mut out = String::with_capacity(7 + hash.len() * 2);
    out.push_str("sha256:");
    for byte in hash {
        let _ = write!(out, "{byte:02x}");
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{EntityId, Partition};

    fn attr(sequence: u64) -> AttrId {
        EntityId::new(Partition::Db as u32, sequence)
    }

    fn installed(ident: &str, sequence: u64) -> InstalledAttribute {
        InstalledAttribute {
            ident: ident.to_owned(),
            id: attr(sequence),
            value_type: ValueType::Str,
            cardinality: Cardinality::One,
            unique: None,
            indexed: false,
            is_component: false,
            no_history: false,
            doc: None,
            retired: false,
            protection: None,
            ever_protected: false,
            engine: false,
        }
    }

    fn step(key: &str, class: ExecutionClass) -> PlanStep {
        PlanStep {
            key: key.to_owned(),
            ident: ":person/email".to_owned(),
            kind: ChangeKind::Property(SchemaProperty::Unique),
            installed: Some("none".to_owned()),
            desired: Some("identity".to_owned()),
            class,
            risk: Risk::Medium,
            acks: Vec::new(),
            blocked: None,
            depends_on: Vec::new(),
            summary: "unique none -> identity".to_owned(),
            recipe: Vec::new(),
            observations: vec![Observation::count("duplicate-values", 0)],
            notes: Vec::new(),
        }
    }

    fn plan(steps: Vec<PlanStep>) -> SchemaPlan {
        SchemaPlan {
            database: "people".to_owned(),
            basis_t: 418,
            schema_generation: None,
            desired_digest: "sha256:desired".to_owned(),
            installed_fingerprint: "sha256:installed".to_owned(),
            prune: false,
            steps,
            unmanaged: Vec::new(),
            notes: Vec::new(),
        }
    }

    #[test]
    fn observations_do_not_change_the_plan_digest() {
        let base = plan(vec![step(
            ":person/email#unique",
            ExecutionClass::ValidateReindex,
        )]);
        let mut drifted = base.clone();
        drifted.basis_t = 999;
        drifted.database = "other".to_owned();
        drifted.steps[0].observations = vec![Observation::count("duplicate-values", 12_204)];
        drifted.steps[0].notes = vec!["history is incomplete".to_owned()];
        drifted.notes = vec!["advisory".to_owned()];
        assert_eq!(base.digest(), drifted.digest());
    }

    #[test]
    fn logical_changes_change_the_plan_digest() {
        let base = plan(vec![step(
            ":person/email#unique",
            ExecutionClass::ValidateReindex,
        )]);
        for mutate in [
            (|plan: &mut SchemaPlan| plan.prune = true) as fn(&mut SchemaPlan),
            |plan| plan.desired_digest = "sha256:other".to_owned(),
            |plan| plan.installed_fingerprint = "sha256:other".to_owned(),
            |plan| plan.steps[0].class = ExecutionClass::Rewrite,
            |plan| plan.steps[0].risk = Risk::High,
            |plan| plan.steps[0].acks = vec![AckCode::UniqueModeChange],
            |plan| plan.steps[0].blocked = Some(BlockedReason::UniqueDuplicates),
            |plan| plan.steps[0].desired = Some("value".to_owned()),
            |plan| plan.steps[0].depends_on = vec![":person/email#index".to_owned()],
            |plan| plan.unmanaged = vec![":legacy/import-id".to_owned()],
        ] {
            let mut changed = base.clone();
            mutate(&mut changed);
            assert_ne!(base.digest(), changed.digest(), "{changed:?}");
        }
    }

    #[test]
    fn step_order_does_not_change_the_plan_digest() {
        let first = step(":person/email#unique", ExecutionClass::ValidateReindex);
        let second = step(":person/email#index", ExecutionClass::ValidateReindex);
        let forward = plan(vec![first.clone(), second.clone()]);
        let reverse = plan(vec![second, first]);
        assert_eq!(forward.digest(), reverse.digest());
    }

    #[test]
    fn the_canonical_encoding_cannot_be_confused_by_separators() {
        // Two attributes whose idents and docs concatenate to the same text
        // must not fingerprint alike.
        let mut left = installed(":a/b", 100);
        left.doc = Some("c".to_owned());
        let mut right = installed(":a", 100);
        right.doc = Some("b/c".to_owned());
        assert_ne!(
            InstalledSchema::new(vec![left]).fingerprint(),
            InstalledSchema::new(vec![right]).fingerprint()
        );
    }

    #[test]
    fn fingerprints_follow_every_installed_property() {
        let base = InstalledSchema::new(vec![installed(":person/email", 100)]);
        for mutate in [
            (|attr: &mut InstalledAttribute| attr.value_type = ValueType::Long)
                as fn(&mut InstalledAttribute),
            |attr| attr.cardinality = Cardinality::Many,
            |attr| attr.unique = Some(Unique::Identity),
            |attr| attr.indexed = true,
            |attr| attr.is_component = true,
            |attr| attr.no_history = true,
            |attr| attr.doc = Some("who".to_owned()),
            |attr| attr.retired = true,
            |attr| attr.protection = Some(":protect/pii".to_owned()),
            |attr| attr.ever_protected = true,
            |attr| attr.engine = true,
            |attr| attr.id = attr_with_sequence(101),
        ] {
            let mut changed = installed(":person/email", 100);
            mutate(&mut changed);
            assert_ne!(
                base.fingerprint(),
                InstalledSchema::new(vec![changed]).fingerprint()
            );
        }
    }

    fn attr_with_sequence(sequence: u64) -> AttrId {
        attr(sequence)
    }

    #[test]
    fn required_classes_and_acks_summarize_the_plan() {
        let mut reindex = step(":person/email#unique", ExecutionClass::ValidateReindex);
        reindex.acks = vec![AckCode::UniqueModeChange];
        let additive = step(":person/tags#cardinality", ExecutionClass::Additive);
        let plan = plan(vec![additive, reindex]);
        assert_eq!(
            plan.required_classes().into_iter().collect::<Vec<_>>(),
            vec![ExecutionClass::ValidateReindex]
        );
        assert_eq!(
            plan.required_acks().into_iter().collect::<Vec<_>>(),
            vec![AckCode::UniqueModeChange]
        );
    }

    #[test]
    fn stable_names_round_trip() {
        for class in [
            ExecutionClass::Additive,
            ExecutionClass::ValidateReindex,
            ExecutionClass::Rewrite,
            ExecutionClass::Destructive,
        ] {
            assert_eq!(ExecutionClass::parse(class.as_str()), Some(class));
        }
        for ack in [
            AckCode::ComponentEnable,
            AckCode::ComponentDisable,
            AckCode::UniqueModeChange,
            AckCode::NoHistoryEnable,
            AckCode::NoHistoryDisable,
            AckCode::RetireLiveAttribute,
            AckCode::ProtectionForwardOnly,
        ] {
            assert_eq!(AckCode::parse(ack.as_str()), Some(ack));
        }
        assert_eq!(ExecutionClass::parse("nonsense"), None);
        assert_eq!(AckCode::parse("nonsense"), None);
    }

    #[test]
    fn counts_are_grouped_for_display() {
        assert_eq!(group_digits(0), "0");
        assert_eq!(group_digits(999), "999");
        assert_eq!(group_digits(12_204), "12,204");
        assert_eq!(group_digits(1_000_000), "1,000,000");
    }
}
