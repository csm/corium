//! `corium schema update`: plan a declarative schema change, and (once schema
//! transactions land) apply the exact plan that was reviewed.
//!
//! The command is plan-first by design. Reading a schema file is not
//! permission to remove data, collapse cardinality, or reinterpret values:
//! without `--apply` nothing is written, and `--apply` must name the digest
//! the plan printed. See `docs/design/schema-migrations.md`.
//!
//! Two renderings are produced from one [`SchemaPlan`]: a human report and a
//! versioned JSON document. Scripts must read the JSON — the human rendering
//! is not a contract.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use clap::{Args, Subcommand};
use corium_core::migration::{
    AckCode, BlockedReason, ChangeKind, ExecutionClass, Observation, ObservationValue,
    PLAN_FORMAT_VERSION, PlanStep, SchemaPlan,
};
use corium_forms::desired::DesiredSchema;
use corium_forms::planner::{PlanOptions, plan as build_plan};
use corium_peer::Connection;

use crate::ClientFlags;

/// Declarative schema inspection and update.
#[derive(Subcommand)]
pub(crate) enum SchemaCommand {
    /// Compare a schema file with the schema installed in a database and
    /// print the plan. Read-only unless `--apply` is given.
    Update(UpdateArgs),
}

/// Arguments of `corium schema update`.
// The booleans are independent command-line flags; grouping them into a mode
// enum would misrepresent combinations like `--json --prune --apply`.
#[allow(clippy::struct_excessive_bools)]
#[derive(Args)]
pub(crate) struct UpdateArgs {
    /// Database name.
    pub(crate) db: String,
    /// Desired schema file: `.toml` for hierarchical TOML, otherwise EDN.
    #[arg(long)]
    pub(crate) schema: PathBuf,
    /// Retire installed attributes the file does not declare, instead of
    /// reporting them as unmanaged.
    #[arg(long)]
    pub(crate) prune: bool,
    /// Emit the versioned machine-readable plan instead of the human report.
    #[arg(long)]
    pub(crate) json: bool,
    /// Exit 0 when nothing would change and 2 when changes are planned,
    /// instead of always exiting 0 for a successful plan.
    #[arg(long)]
    pub(crate) detailed_exit_code: bool,
    /// Apply the plan. Requires `--plan` with the digest the plan printed.
    #[arg(long, requires = "plan")]
    pub(crate) apply: bool,
    /// The plan digest to apply, as printed by the read-only plan.
    #[arg(long)]
    pub(crate) plan: Option<String>,
    /// Permit changes of an execution class above `additive` (repeatable).
    /// `destructive` has no allowance: this command cannot run one.
    #[arg(long = "allow", value_name = "CLASS")]
    pub(crate) allow: Vec<String>,
    /// Acknowledge a semantic change by its stable code (repeatable).
    #[arg(long = "ack", value_name = "CHANGE-CODE")]
    pub(crate) ack: Vec<String>,
    #[command(flatten)]
    pub(crate) client: ClientFlags,
}

/// Stable error codes reported with `--json`.
const ERROR_PARSE: &str = "parse-error";
const ERROR_CONNECT: &str = "connect-error";
const ERROR_PLAN: &str = "plan-error";
const ERROR_PLAN_MISMATCH: &str = "plan-mismatch";
const ERROR_ALLOW_REQUIRED: &str = "allow-required";
const ERROR_ACK_REQUIRED: &str = "ack-required";
const ERROR_BLOCKED_CHANGE: &str = "blocked-change";
const ERROR_APPLY_UNSUPPORTED: &str = "apply-unsupported";

/// Why `--apply` cannot run yet.
///
/// Schema is not basis-versioned data in this build: attribute metadata is
/// fixed when a database is created, so there is no atomic, durable,
/// peer-visible operation that changes it. Planning is exact and useful
/// without that; applying would not be.
const APPLY_UNSUPPORTED: &str = "applying a schema plan requires transactional schema changes, \
     which this build does not have: attribute metadata is fixed at database creation. Plan \
     output is complete and safe to review; re-run without --apply.";

/// A failure carrying the stable code its JSON rendering reports.
#[derive(Debug)]
struct CommandError {
    code: &'static str,
    message: String,
}

impl CommandError {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

/// Runs `corium schema`, returning the process exit code.
///
/// # Errors
/// Returns the message printed by `main` for failures that are not rendered
/// as JSON (JSON errors are printed here and reported as a failing code).
pub(crate) async fn run(command: SchemaCommand) -> Result<ExitCode, String> {
    match command {
        SchemaCommand::Update(args) => {
            let json = args.json;
            match run_update(args).await {
                Ok(code) => Ok(code),
                Err(error) => {
                    if json {
                        println!("{}", render_error_json(error.code, &error.message));
                        Ok(ExitCode::FAILURE)
                    } else {
                        Err(error.message)
                    }
                }
            }
        }
    }
}

async fn run_update(args: UpdateArgs) -> Result<ExitCode, CommandError> {
    let desired = read_desired(&args.schema)?;
    let allowed = parse_allowances(&args.allow)?;
    let acknowledged = parse_acks(&args.ack)?;

    let config = args
        .client
        .connect_config(args.db.clone())
        .await
        .map_err(|error| CommandError::new(ERROR_CONNECT, error))?;
    let connection = Connection::connect(config)
        .await
        .map_err(|error| CommandError::new(ERROR_CONNECT, error.to_string()))?;
    // One immutable value: every count in the plan is measured at one basis
    // and one schema, so the plan cannot disagree with itself.
    let db = connection
        .sync()
        .await
        .map_err(|error| CommandError::new(ERROR_CONNECT, error.to_string()))?;

    let options = PlanOptions::new(args.db.clone()).with_prune(args.prune);
    let plan = build_plan(&desired, &db, &options)
        .map_err(|error| CommandError::new(ERROR_PLAN, error.to_string()))?;

    if args.apply {
        // Every precondition this side can check is checked first, so an
        // operator learns about a stale plan or a missing acknowledgement even
        // while the operation itself is unavailable.
        check_apply(&plan, args.plan.as_deref(), &allowed, &acknowledged)?;
        return Err(CommandError::new(ERROR_APPLY_UNSUPPORTED, APPLY_UNSUPPORTED));
    }

    if args.json {
        println!("{}", render_json(&plan));
    } else {
        print!("{}", render_human(&plan));
    }
    Ok(if args.detailed_exit_code && plan.has_changes() {
        ExitCode::from(2)
    } else {
        ExitCode::SUCCESS
    })
}

/// Validates every precondition `--apply` can check without writing.
///
/// The digest check happens first, so a stale plan is never partially
/// validated; blocked changes are refused before allowances, because no
/// allowance can ever enable them.
fn check_apply(
    plan: &SchemaPlan,
    requested_digest: Option<&str>,
    allowed: &BTreeSet<ExecutionClass>,
    acknowledged: &BTreeSet<AckCode>,
) -> Result<(), CommandError> {
    let digest = plan.digest();
    match requested_digest {
        Some(requested) if requested == digest => {}
        Some(requested) => {
            return Err(CommandError::new(
                ERROR_PLAN_MISMATCH,
                format!(
                    "plan {requested} is not the plan for this database and schema file \
                     (current plan is {digest}); review the new plan and re-run"
                ),
            ));
        }
        None => {
            return Err(CommandError::new(
                ERROR_PLAN_MISMATCH,
                "--apply requires --plan <digest>".to_owned(),
            ));
        }
    }

    let blocked: Vec<&PlanStep> = plan.blocked_steps().collect();
    if let Some(step) = blocked.first() {
        let reason = step.blocked.map_or_else(
            || format!("{} changes cannot be executed", step.class),
            |blocked| blocked.message().to_owned(),
        );
        return Err(CommandError::new(
            ERROR_BLOCKED_CHANGE,
            format!("{} {}: {reason}", step.ident, step.summary),
        ));
    }

    let missing_classes: Vec<ExecutionClass> = plan
        .required_allowances()
        .into_iter()
        .filter(|class| !allowed.contains(class))
        .collect();
    if !missing_classes.is_empty() {
        let flags: Vec<String> = missing_classes
            .iter()
            .map(|class| format!("--allow {class}"))
            .collect();
        return Err(CommandError::new(
            ERROR_ALLOW_REQUIRED,
            format!("this plan requires {}", flags.join(" ")),
        ));
    }

    let missing_acks: Vec<AckCode> = plan
        .required_acks()
        .into_iter()
        .filter(|ack| !acknowledged.contains(ack))
        .collect();
    if !missing_acks.is_empty() {
        let flags: Vec<String> = missing_acks
            .iter()
            .map(|ack| format!("--ack {ack}"))
            .collect();
        return Err(CommandError::new(
            ERROR_ACK_REQUIRED,
            format!("this plan requires {}", flags.join(" ")),
        ));
    }

    Ok(())
}

fn read_desired(path: &Path) -> Result<DesiredSchema, CommandError> {
    let text = std::fs::read_to_string(path)
        .map_err(|error| CommandError::new(ERROR_PARSE, format!("cannot read {}: {error}", path.display())))?;
    if path
        .extension()
        .and_then(std::ffi::OsStr::to_str)
        .is_some_and(|extension| extension.eq_ignore_ascii_case("toml"))
    {
        return DesiredSchema::from_toml(&text)
            .map_err(|error| CommandError::new(ERROR_PARSE, format!("bad schema TOML: {error}")));
    }
    let forms = crate::read_schema_edn(&text)
        .map_err(|error| CommandError::new(ERROR_PARSE, error))?;
    DesiredSchema::from_edn(&forms)
        .map_err(|error| CommandError::new(ERROR_PARSE, format!("bad schema EDN: {error}")))
}

fn parse_allowances(values: &[String]) -> Result<BTreeSet<ExecutionClass>, CommandError> {
    let mut allowed = BTreeSet::new();
    for value in values {
        let class = ExecutionClass::parse(value).ok_or_else(|| {
            CommandError::new(
                ERROR_PARSE,
                format!("unknown execution class {value:?}; expected validate-reindex or rewrite"),
            )
        })?;
        if !class.executable() {
            return Err(CommandError::new(
                ERROR_PARSE,
                format!("{class} changes cannot be allowed: this command never runs them"),
            ));
        }
        allowed.insert(class);
    }
    Ok(allowed)
}

fn parse_acks(values: &[String]) -> Result<BTreeSet<AckCode>, CommandError> {
    values
        .iter()
        .map(|value| {
            AckCode::parse(value)
                .ok_or_else(|| CommandError::new(ERROR_PARSE, format!("unknown change code {value:?}")))
        })
        .collect()
}

/// The marker a change kind prints in the human report.
const fn marker(kind: ChangeKind) -> char {
    match kind {
        ChangeKind::Add => '+',
        ChangeKind::Retire => '-',
        ChangeKind::Property(_) => '~',
    }
}

/// Renders the human plan.
#[must_use]
pub(crate) fn render_human(plan: &SchemaPlan) -> String {
    use std::fmt::Write as _;

    let mut out = String::new();
    let generation = plan
        .schema_generation
        .map_or_else(|| "unknown".to_owned(), |generation| generation.to_string());
    let _ = writeln!(
        out,
        "database: {}  basis: {}  schema-generation: {generation}",
        plan.database, plan.basis_t
    );
    let _ = writeln!(out, "desired:  {}", plan.desired_digest);
    let _ = writeln!(out, "plan:     {}", plan.digest());

    for class in [
        ExecutionClass::Additive,
        ExecutionClass::ValidateReindex,
        ExecutionClass::Rewrite,
        ExecutionClass::Destructive,
    ] {
        let steps: Vec<&PlanStep> = plan.steps_in(class).collect();
        if steps.is_empty() {
            continue;
        }
        let heading = class.as_str().to_uppercase();
        let _ = writeln!(out);
        if class.executable() {
            let _ = writeln!(out, "{heading}");
        } else {
            let _ = writeln!(out, "{heading} (blocked)");
        }
        for step in steps {
            push_step_report(&mut out, step);
        }
    }

    if !plan.unmanaged.is_empty() {
        let _ = writeln!(out);
        let _ = writeln!(out, "UNMANAGED");
        for ident in &plan.unmanaged {
            let _ = writeln!(out, "    {ident}    use --prune to retire");
        }
    }

    if !plan.notes.is_empty() {
        let _ = writeln!(out);
        let _ = writeln!(out, "NOTES");
        for note in &plan.notes {
            let _ = writeln!(out, "    {note}");
        }
    }

    let _ = writeln!(out);
    if plan.has_changes() {
        let _ = writeln!(
            out,
            "{} change(s) planned. Nothing was written.",
            plan.steps.len()
        );
        let blocked = plan.blocked_steps().count();
        if blocked > 0 {
            // Offering an apply invocation for a plan that contains a change
            // no flag can enable would misdescribe it as one option away.
            let _ = writeln!(
                out,
                "{blocked} of them cannot be executed by this command. Resolve them in the \
                 database, or remove them from the file, before applying the rest."
            );
        } else {
            let mut requirements: Vec<String> = plan
                .required_allowances()
                .into_iter()
                .map(|class| format!("--allow {class}"))
                .collect();
            requirements.extend(
                plan.required_acks()
                    .into_iter()
                    .map(|ack| format!("--ack {ack}")),
            );
            let _ = writeln!(
                out,
                "To apply: corium schema update {} --schema <file>{} --apply --plan {}{}",
                plan.database,
                if plan.prune { " --prune" } else { "" },
                plan.digest(),
                if requirements.is_empty() {
                    String::new()
                } else {
                    format!(" {}", requirements.join(" "))
                }
            );
        }
        let _ = writeln!(out, "Note: {APPLY_UNSUPPORTED}");
    } else {
        let _ = writeln!(out, "No changes. Nothing was written.");
    }
    out
}

/// Renders one step's detail lines under its heading.
fn push_step_report(out: &mut String, step: &PlanStep) {
    use std::fmt::Write as _;

    let _ = writeln!(
        out,
        "  {} {} {}",
        marker(step.kind),
        step.ident,
        step.summary
    );
    if let Some(reason) = step.blocked {
        let _ = writeln!(out, "      blocked: {}", reason.message());
    }
    for observation in &step.observations {
        let _ = writeln!(
            out,
            "      {}: {}",
            observation.label.replace('-', " "),
            observation.value
        );
    }
    for ack in &step.acks {
        let _ = writeln!(out, "      [ack: {ack}] {}", ack.meaning());
    }
    if !step.depends_on.is_empty() {
        let _ = writeln!(out, "      after: {}", step.depends_on.join(", "));
    }
    for note in &step.notes {
        let _ = writeln!(out, "      note: {note}");
    }
    for line in &step.recipe {
        let _ = writeln!(out, "      recipe: {line}");
    }
}

/// Renders the versioned machine-readable plan.
#[must_use]
pub(crate) fn render_json(plan: &SchemaPlan) -> String {
    let mut out = String::new();
    out.push_str("{\"version\":");
    out.push_str(&PLAN_FORMAT_VERSION.to_string());
    push_string_field(&mut out, "database", &plan.database);
    out.push_str(",\"basis_t\":");
    out.push_str(&plan.basis_t.to_string());
    out.push_str(",\"schema_generation\":");
    match plan.schema_generation {
        Some(generation) => out.push_str(&generation.to_string()),
        None => out.push_str("null"),
    }
    push_string_field(&mut out, "desired_digest", &plan.desired_digest);
    push_string_field(&mut out, "installed_fingerprint", &plan.installed_fingerprint);
    push_string_field(&mut out, "plan_digest", &plan.digest());
    out.push_str(",\"prune\":");
    out.push_str(if plan.prune { "true" } else { "false" });
    out.push_str(",\"has_changes\":");
    out.push_str(if plan.has_changes() { "true" } else { "false" });

    out.push_str(",\"changes\":[");
    for (index, step) in plan.steps.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        push_step(&mut out, step);
    }
    out.push(']');

    out.push_str(",\"unmanaged\":");
    push_string_array(&mut out, plan.unmanaged.iter().map(String::as_str));
    out.push_str(",\"notes\":");
    push_string_array(&mut out, plan.notes.iter().map(String::as_str));

    out.push_str(",\"required_allowances\":");
    push_string_array(
        &mut out,
        plan.required_allowances().iter().map(|class| class.as_str()),
    );
    out.push_str(",\"required_acks\":");
    push_string_array(&mut out, plan.required_acks().iter().map(|ack| ack.as_str()));

    out.push_str(",\"apply\":{\"supported\":false");
    push_string_field(&mut out, "reason", APPLY_UNSUPPORTED);
    out.push('}');
    out.push('}');
    out
}

fn push_step(out: &mut String, step: &PlanStep) {
    out.push_str("{\"key\":");
    push_json_string(out, &step.key);
    push_string_field(out, "ident", &step.ident);
    push_string_field(out, "change", step.kind.as_str());
    out.push_str(",\"installed\":");
    push_optional_string(out, step.installed.as_deref());
    out.push_str(",\"desired\":");
    push_optional_string(out, step.desired.as_deref());
    push_string_field(out, "execution_class", step.class.as_str());
    push_string_field(out, "risk", step.risk.as_str());
    push_string_field(out, "summary", &step.summary);
    out.push_str(",\"executable\":");
    out.push_str(if step.class.executable() && step.blocked.is_none() {
        "true"
    } else {
        "false"
    });
    out.push_str(",\"blocked\":");
    push_optional_string(out, step.blocked.map(BlockedReason::as_str));
    out.push_str(",\"acks\":");
    push_string_array(out, step.acks.iter().map(|ack| ack.as_str()));
    out.push_str(",\"depends_on\":");
    push_string_array(out, step.depends_on.iter().map(String::as_str));
    out.push_str(",\"recipe\":");
    push_string_array(out, step.recipe.iter().map(String::as_str));
    out.push_str(",\"notes\":");
    push_string_array(out, step.notes.iter().map(String::as_str));
    out.push_str(",\"observations\":[");
    for (index, observation) in step.observations.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        push_observation(out, observation);
    }
    out.push_str("]}");
}

fn push_observation(out: &mut String, observation: &Observation) {
    out.push_str("{\"label\":");
    push_json_string(out, &observation.label);
    match &observation.value {
        ObservationValue::Count(count) => {
            out.push_str(",\"kind\":\"count\",\"value\":");
            out.push_str(&count.to_string());
        }
        ObservationValue::Estimate(count) => {
            out.push_str(",\"kind\":\"estimate\",\"value\":");
            out.push_str(&count.to_string());
        }
        ObservationValue::Entities(entities) => {
            out.push_str(",\"kind\":\"entities\",\"value\":[");
            for (index, entity) in entities.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push_str(&entity.sequence().to_string());
            }
            out.push(']');
        }
        ObservationValue::Unavailable(why) => {
            out.push_str(",\"kind\":\"unavailable\",\"value\":");
            push_json_string(out, why);
        }
    }
    out.push('}');
}

/// Renders a failure as the same versioned JSON document shape.
#[must_use]
pub(crate) fn render_error_json(code: &str, message: &str) -> String {
    let mut out = String::new();
    out.push_str("{\"version\":");
    out.push_str(&PLAN_FORMAT_VERSION.to_string());
    out.push_str(",\"error\":{\"code\":");
    push_json_string(&mut out, code);
    push_string_field(&mut out, "message", message);
    out.push_str("}}");
    out
}

fn push_string_field(out: &mut String, name: &str, value: &str) {
    out.push(',');
    push_json_string(out, name);
    out.push(':');
    push_json_string(out, value);
}

fn push_optional_string(out: &mut String, value: Option<&str>) {
    match value {
        Some(value) => push_json_string(out, value),
        None => out.push_str("null"),
    }
}

fn push_string_array<'a>(out: &mut String, values: impl Iterator<Item = &'a str>) {
    out.push('[');
    for (index, value) in values.enumerate() {
        if index > 0 {
            out.push(',');
        }
        push_json_string(out, value);
    }
    out.push(']');
}

fn push_json_string(out: &mut String, value: &str) {
    use std::fmt::Write as _;

    out.push('"');
    for character in value.chars() {
        match character {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            control if control < ' ' => {
                let _ = write!(out, "\\u{:04x}", control as u32);
            }
            other => out.push(other),
        }
    }
    out.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;
    use corium_core::migration::{Risk, SchemaProperty};

    fn sample_plan() -> SchemaPlan {
        SchemaPlan {
            database: "people".to_owned(),
            basis_t: 418,
            schema_generation: None,
            desired_digest: "sha256:desired".to_owned(),
            installed_fingerprint: "sha256:installed".to_owned(),
            prune: false,
            steps: vec![
                PlanStep {
                    key: ":person/email#add".to_owned(),
                    ident: ":person/email".to_owned(),
                    kind: ChangeKind::Add,
                    installed: None,
                    desired: Some("string cardinality-one".to_owned()),
                    class: ExecutionClass::Additive,
                    risk: Risk::Low,
                    acks: Vec::new(),
                    blocked: None,
                    depends_on: Vec::new(),
                    summary: "string cardinality-one".to_owned(),
                    recipe: Vec::new(),
                    observations: Vec::new(),
                    notes: Vec::new(),
                },
                PlanStep {
                    key: ":person/address#component".to_owned(),
                    ident: ":person/address".to_owned(),
                    kind: ChangeKind::Property(SchemaProperty::Component),
                    installed: Some("false".to_owned()),
                    desired: Some("true".to_owned()),
                    class: ExecutionClass::ValidateReindex,
                    risk: Risk::High,
                    acks: vec![AckCode::ComponentEnable],
                    blocked: None,
                    depends_on: Vec::new(),
                    summary: "component false -> true".to_owned(),
                    recipe: Vec::new(),
                    observations: vec![Observation::count("live-refs", 8_109)],
                    notes: Vec::new(),
                },
                PlanStep {
                    key: ":person/age#value-type".to_owned(),
                    ident: ":person/age".to_owned(),
                    kind: ChangeKind::Property(SchemaProperty::ValueType),
                    installed: Some("long".to_owned()),
                    desired: Some("string".to_owned()),
                    class: ExecutionClass::Destructive,
                    risk: Risk::High,
                    acks: Vec::new(),
                    blocked: Some(BlockedReason::ValueTypeMutation),
                    depends_on: Vec::new(),
                    summary: "long -> string".to_owned(),
                    recipe: vec!["add :person/age-string".to_owned()],
                    observations: vec![Observation::count("current-datoms", 8_109)],
                    notes: Vec::new(),
                },
            ],
            unmanaged: vec![":legacy/import-id".to_owned()],
            notes: vec!["a note".to_owned()],
        }
    }

    #[test]
    fn the_human_plan_groups_by_execution_class() {
        let rendered = render_human(&sample_plan());
        assert!(rendered.contains("database: people  basis: 418  schema-generation: unknown"));
        assert!(rendered.contains("ADDITIVE\n  + :person/email string cardinality-one"));
        assert!(rendered.contains("VALIDATE-REINDEX\n  ~ :person/address component false -> true"));
        assert!(rendered.contains("live refs: 8,109"));
        assert!(rendered.contains("[ack: component-enable]"));
        assert!(rendered.contains("DESTRUCTIVE (blocked)"));
        assert!(rendered.contains("blocked: direct type mutation is not executable"));
        assert!(rendered.contains("recipe: add :person/age-string"));
        assert!(rendered.contains("UNMANAGED\n    :legacy/import-id    use --prune to retire"));
        assert!(rendered.contains("Nothing was written."));
    }

    #[test]
    fn the_human_plan_prints_the_exact_apply_invocation() {
        let mut plan = sample_plan();
        plan.steps.retain(|step| step.class.executable());
        let rendered = render_human(&plan);
        assert!(
            rendered.contains(&format!(
                "corium schema update people --schema <file> --apply --plan {} \
                 --allow validate-reindex --ack component-enable",
                plan.digest()
            )),
            "{rendered}"
        );
    }

    #[test]
    fn a_blocked_plan_is_not_offered_an_apply_invocation() {
        // The sample plan contains a value-type change, which no flag enables.
        let rendered = render_human(&sample_plan());
        assert!(!rendered.contains("To apply:"), "{rendered}");
        assert!(
            rendered.contains("1 of them cannot be executed by this command"),
            "{rendered}"
        );
    }

    #[test]
    fn an_empty_plan_says_so() {
        let mut plan = sample_plan();
        plan.steps.clear();
        plan.unmanaged.clear();
        let rendered = render_human(&plan);
        assert!(rendered.contains("No changes. Nothing was written."));
        assert!(!rendered.contains("ADDITIVE"));
    }

    #[test]
    fn the_json_plan_carries_the_stable_contract() {
        let json = render_json(&sample_plan());
        assert!(json.starts_with("{\"version\":1,\"database\":\"people\""));
        assert!(json.contains("\"basis_t\":418"));
        assert!(json.contains("\"schema_generation\":null"));
        assert!(json.contains("\"plan_digest\":\"sha256:"));
        assert!(json.contains("\"key\":\":person/address#component\""));
        assert!(json.contains("\"change\":\"component\""));
        assert!(json.contains("\"execution_class\":\"validate-reindex\""));
        assert!(json.contains("\"risk\":\"high\""));
        assert!(json.contains("\"acks\":[\"component-enable\"]"));
        assert!(json.contains("\"blocked\":\"value-type-mutation\""));
        assert!(json.contains("\"executable\":false"));
        assert!(json.contains("{\"label\":\"live-refs\",\"kind\":\"count\",\"value\":8109}"));
        assert!(json.contains("\"unmanaged\":[\":legacy/import-id\"]"));
        assert!(json.contains("\"required_allowances\":[\"validate-reindex\"]"));
        assert!(json.contains("\"required_acks\":[\"component-enable\"]"));
        assert!(json.contains("\"apply\":{\"supported\":false"));
    }

    #[test]
    fn json_strings_escape_control_characters_and_quotes() {
        let mut out = String::new();
        push_json_string(&mut out, "a\"b\\c\nd\te\u{1}");
        assert_eq!(out, "\"a\\\"b\\\\c\\nd\\te\\u0001\"");
    }

    #[test]
    fn json_errors_carry_stable_codes() {
        let rendered = render_error_json(ERROR_PLAN_MISMATCH, "stale");
        assert_eq!(
            rendered,
            "{\"version\":1,\"error\":{\"code\":\"plan-mismatch\",\"message\":\"stale\"}}"
        );
    }

    #[test]
    fn apply_requires_the_exact_plan_digest() {
        let plan = sample_plan();
        let error = check_apply(&plan, Some("sha256:stale"), &BTreeSet::new(), &BTreeSet::new())
            .expect_err("a stale digest is refused");
        assert_eq!(error.code, ERROR_PLAN_MISMATCH);
        assert!(error.message.contains("review the new plan"));

        let error = check_apply(&plan, None, &BTreeSet::new(), &BTreeSet::new())
            .expect_err("a missing digest is refused");
        assert_eq!(error.code, ERROR_PLAN_MISMATCH);
    }

    #[test]
    fn apply_refuses_blocked_changes_before_asking_for_allowances() {
        let plan = sample_plan();
        let error = check_apply(
            &plan,
            Some(&plan.digest()),
            &BTreeSet::new(),
            &BTreeSet::new(),
        )
        .expect_err("a destructive change is refused");
        assert_eq!(error.code, ERROR_BLOCKED_CHANGE);
        assert!(error.message.contains(":person/age"));
    }

    #[test]
    fn apply_demands_allowances_then_acknowledgements_then_reports_the_gap() {
        let mut plan = sample_plan();
        plan.steps.retain(|step| step.class.executable());

        let error = check_apply(
            &plan,
            Some(&plan.digest()),
            &BTreeSet::new(),
            &BTreeSet::new(),
        )
        .expect_err("an unallowed class is refused");
        assert_eq!(error.code, ERROR_ALLOW_REQUIRED);
        assert!(error.message.contains("--allow validate-reindex"));

        let allowed = BTreeSet::from([ExecutionClass::ValidateReindex]);
        let error = check_apply(&plan, Some(&plan.digest()), &allowed, &BTreeSet::new())
            .expect_err("an unacknowledged change is refused");
        assert_eq!(error.code, ERROR_ACK_REQUIRED);
        assert!(error.message.contains("--ack component-enable"));

        // With the digest, the allowance, and the acknowledgement all present,
        // nothing is left for this side to refuse.
        let acknowledged = BTreeSet::from([AckCode::ComponentEnable]);
        assert!(
            check_apply(&plan, Some(&plan.digest()), &allowed, &acknowledged).is_ok(),
            "a fully authorized plan passes its local preconditions"
        );
        // The operation itself is still unavailable in this build; the plan
        // and the JSON contract both say so with a stable code.
        assert!(APPLY_UNSUPPORTED.contains("transactional schema changes"));
    }

    #[test]
    fn allowances_and_acknowledgements_are_parsed_by_stable_name() {
        assert_eq!(
            parse_allowances(&["validate-reindex".to_owned(), "rewrite".to_owned()])
                .expect("parses")
                .len(),
            2
        );
        assert_eq!(
            parse_allowances(&["destructive".to_owned()])
                .expect_err("destructive has no allowance")
                .code,
            ERROR_PARSE
        );
        assert_eq!(
            parse_allowances(&["nonsense".to_owned()])
                .expect_err("unknown class")
                .code,
            ERROR_PARSE
        );
        assert_eq!(
            parse_acks(&["component-enable".to_owned()])
                .expect("parses")
                .len(),
            1
        );
        assert_eq!(
            parse_acks(&["nonsense".to_owned()])
                .expect_err("unknown code")
                .code,
            ERROR_PARSE
        );
    }
}
