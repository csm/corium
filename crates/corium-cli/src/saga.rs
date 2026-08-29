//! `corium saga`: open, list, inspect, extend, commit, and abort sagas
//! (ADR-0023).
//!
//! Every one of these is an ordinary transaction against the parent database's
//! registry, so this module is a thin front for
//! [`corium_peer::saga`] — argument parsing, one EDN map per line of output,
//! and the error the registry gives back. Nothing here is privileged: the same
//! effects are available to any client that transacts the same forms, and
//! `SELECT … FROM corium_sys.sagas` answers the read-only questions from SQL.
//!
//! `step` and `log` work on the saga's branch — the overlay database its
//! partial progress lives in — which is an ordinary connection under a name
//! derived from the saga id, so `corium console <db> --saga <id>` gets the
//! whole read surface over it for free.
//!
//! `commit` is the exception to "thin front for a transaction": a merge is
//! composed inside the transactor, so this sends the request and prints what
//! came back. A refused merge is not a crashed command — it prints the
//! conflict report and exits non-zero, so a script can tell "the parent moved"
//! from "the database is unreachable", and a person has the document they need
//! in order to answer.

use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use clap::{Args, Subcommand};
use corium_core::{EntityId, Keyword};
use corium_db::Db;
use corium_db::saga::{SagaEntry, SagaStatus};
use corium_peer::Connection;
use corium_peer::saga::{
    MergeOutcome, SagaBranch, SagaOptions, format_saga_id, parse_saga_id, registry_installed,
};
use corium_query::edn::{Edn, read_all};

use crate::{ClientFlags, instant, parse_duration};

/// Saga registry commands.
#[derive(Subcommand)]
pub(crate) enum SagaCommand {
    /// Open a saga: a durable, expiring record of work in flight.
    Open(OpenArgs),
    /// List the sagas a database's registry holds.
    List(ListArgs),
    /// Print one saga, including its external-compensation ledger.
    Status(StatusArgs),
    /// Move an open saga's deadline further out.
    Extend(ExtendArgs),
    /// Merge an open saga's branch into the parent as one transaction.
    Commit(CommitArgs),
    /// Abort an open saga, optionally recording a compensating transaction.
    Abort(AbortArgs),
    /// Transact one step against an open saga's branch.
    Step(StepArgs),
    /// Print an open saga's step history, newest step last.
    Log(LogArgs),
}

/// How long a saga lives when the caller does not say.
const DEFAULT_TTL: &str = "24h";

/// Arguments of `corium saga open`.
#[derive(Args)]
pub(crate) struct OpenArgs {
    /// Database name.
    db: String,
    /// What the saga is for.
    #[arg(long)]
    description: Option<String>,
    /// Principal recorded as the saga's owner, and as whom an expiry-time
    /// compensation is authorized. Defaults to `$USER`. The transactor does
    /// not yet check it against the authenticated principal.
    #[arg(long)]
    owner: Option<String>,
    /// How long the saga may run before the expiry sweep ends it, for example
    /// `7d`. Extendable while it is open.
    #[arg(long, default_value = DEFAULT_TTL, conflicts_with = "expires_at")]
    ttl: String,
    /// Deadline as a UTC timestamp, instead of a duration from now.
    #[arg(long)]
    expires_at: Option<String>,
    /// Advisory declaration of an entity (`1005`) or whole attribute
    /// (`:order/status`) the saga expects to touch (repeatable). Readers may
    /// warn on it; nothing enforces it.
    #[arg(long = "footprint", value_name = "ENTITY")]
    footprint: Vec<String>,
    /// Checked reservation of an entity or whole attribute (repeatable).
    /// Unlike a footprint this binds the saga's own writes, so readers can
    /// rely on "outside the set means untouched".
    #[arg(long = "reserve", value_name = "ENTITY")]
    reserve: Vec<String>,
    /// Fix the reservation set at open, so it can never grow.
    #[arg(long)]
    sealed: bool,
    /// Compensation applied atomically with a later abort or expiry: either
    /// EDN transaction data, or the ident of a `:db/fn` entity to invoke.
    #[arg(long = "on-abort", value_name = "EDN|IDENT")]
    on_abort: Option<String>,
    /// Open under this saga id instead of a freshly minted one.
    #[arg(long)]
    id: Option<String>,
    #[command(flatten)]
    client: ClientFlags,
}

/// Arguments of `corium saga list`.
#[derive(Args)]
pub(crate) struct ListArgs {
    /// Database name.
    db: String,
    /// Only sagas in this state (`open`, `committed`, `aborted`, `expired`).
    #[arg(long)]
    status: Option<String>,
    #[command(flatten)]
    client: ClientFlags,
}

/// Arguments of `corium saga status`.
#[derive(Args)]
pub(crate) struct StatusArgs {
    /// Database name.
    db: String,
    /// Saga id, with or without dashes.
    id: String,
    #[command(flatten)]
    client: ClientFlags,
}

/// Arguments of `corium saga extend`.
#[derive(Args)]
pub(crate) struct ExtendArgs {
    /// Database name.
    db: String,
    /// Saga id.
    id: String,
    /// New deadline as a duration from now, for example `7d`.
    #[arg(long, default_value = DEFAULT_TTL, conflicts_with = "expires_at")]
    ttl: String,
    /// New deadline as a UTC timestamp.
    #[arg(long)]
    expires_at: Option<String>,
    #[command(flatten)]
    client: ClientFlags,
}

/// Arguments of `corium saga commit`.
#[derive(Args)]
pub(crate) struct CommitArgs {
    /// Database name.
    db: String,
    /// Saga id.
    id: String,
    /// A read dependency to check against the parent before merging
    /// (repeatable): either `[:db/cas <entity> <attribute> <value>]`, the
    /// compare half of a compare-and-swap, or `{:guard <query>}`, a Datalog
    /// query that must return a row — `{:guard <query> :expect :none}` for one
    /// that must not. Guards the branch's steps declared as `:db.saga/guard`
    /// metadata are checked as well; these are the ones only the committer
    /// knows about.
    #[arg(long = "guard", value_name = "EDN")]
    guard: Vec<String>,
    /// An answer to one conflict from the last report (repeatable):
    /// `{:e <entity> :a <attribute> :parent <value> :take :parent}` drops the
    /// branch's write and `:take :branch` overrides it, which only a
    /// write-write conflict on a cardinality-one attribute admits. Copy `:v`
    /// across too when the report carries one: it names the single fact a
    /// conflict on a cardinality-many attribute is about. `:parent` is the
    /// value the report showed and is what fences the answer: if the parent
    /// has moved again, the merge refuses rather than absorbing the
    /// difference.
    #[arg(long = "resolve", value_name = "EDN")]
    resolve: Vec<String>,
    #[command(flatten)]
    client: ClientFlags,
}

/// Arguments of `corium saga abort`.
#[derive(Args)]
pub(crate) struct AbortArgs {
    /// Database name.
    db: String,
    /// Saga id.
    id: String,
    /// EDN transaction data applied atomically with the abort, replacing
    /// whatever compensation the saga registered at open. It is fresh
    /// transaction data, validated like any other — a deliberately authored
    /// failure record, never a partial landing of saga work.
    #[arg(long = "compensate", value_name = "EDN")]
    compensate: Option<String>,
    #[command(flatten)]
    client: ClientFlags,
}

/// Arguments of `corium saga step`.
#[derive(Args)]
pub(crate) struct StepArgs {
    /// Database name.
    db: String,
    /// Saga id.
    id: String,
    /// Transaction forms to apply to the branch, or `-` to read them from
    /// standard input. A step is an ordinary transaction: it may create
    /// entities (from the block the saga was leased), write pre-existing
    /// ones, and carry transaction metadata.
    #[arg(value_name = "EDN")]
    tx_data: String,
    #[command(flatten)]
    client: ClientFlags,
}

/// Arguments of `corium saga log`.
#[derive(Args)]
pub(crate) struct LogArgs {
    /// Database name.
    db: String,
    /// Saga id.
    id: String,
    /// Print how many datoms each step carried as well as its number.
    #[arg(long)]
    datoms: bool,
    #[command(flatten)]
    client: ClientFlags,
}

/// Runs one `corium saga` command.
///
/// # Errors
/// Returns the message to print on failure; the registry's own refusals
/// ("cannot abort saga …: it is committed") come through unchanged.
pub(crate) async fn run(command: SagaCommand) -> Result<ExitCode, String> {
    // `commit` is the one subcommand with an outcome rather than just a
    // result: a merge the parent refuses is a legitimate answer that a script
    // must be able to see in the exit code.
    if let SagaCommand::Commit(args) = command {
        return commit(args).await;
    }
    match command {
        SagaCommand::Open(args) => open(args).await,
        SagaCommand::List(args) => list(args).await,
        SagaCommand::Status(args) => status(args).await,
        SagaCommand::Extend(args) => extend(args).await,
        SagaCommand::Abort(args) => abort(args).await,
        SagaCommand::Step(args) => step(args).await,
        SagaCommand::Log(args) => log(args).await,
        SagaCommand::Commit(_) => unreachable!("handled above"),
    }
    .map(|()| ExitCode::SUCCESS)
}

/// Connects to the branch of the saga `id` names, for a caller that already
/// has a connection to the parent.
///
/// Shared with `corium console --saga`, because pointing the console at a
/// branch is the same act as pointing anything else at one: the branch is a
/// database value, and this is a connection to it.
pub(crate) async fn branch(connection: &Connection, id: &str) -> Result<SagaBranch, String> {
    let id = parse_saga_id(id).ok_or_else(|| format!("invalid saga id {id:?}"))?;
    connection
        .saga_branch(id)
        .await
        .map_err(|error| error.to_string())
}

async fn connect(db: String, client: &ClientFlags) -> Result<Connection, String> {
    let config = client.connect_config(db).await?;
    Connection::connect(config)
        .await
        .map_err(|error| error.to_string())
}

async fn open(args: OpenArgs) -> Result<(), String> {
    let connection = connect(args.db.clone(), &args.client).await?;
    let db = connection.sync().await.map_err(|error| error.to_string())?;
    require_registry(&db)?;

    let expires_at = deadline(args.expires_at.as_deref(), &args.ttl)?;
    let owner = args.owner.unwrap_or_else(default_owner);
    let mut options = SagaOptions::new(owner, expires_at);
    options.description = args.description;
    options.sealed = args.sealed;
    for text in &args.footprint {
        options.footprint.push(entity_of(&db, text)?);
    }
    for text in &args.reserve {
        options.reserves.push(entity_of(&db, text)?);
    }
    if let Some(id) = &args.id {
        options.id = Some(parse_saga_id(id).ok_or_else(|| format!("invalid saga id {id:?}"))?);
    }
    if let Some(on_abort) = &args.on_abort {
        // An ident names a `:db/fn` to invoke; anything else is the tx data
        // itself, checked here so a typo fails at open rather than at abort.
        if let Some(db_fn) = ident_entity(&db, on_abort) {
            options.on_abort_fn = Some(db_fn);
        } else {
            read_all(on_abort)
                .map_err(|error| format!("--on-abort is neither an ident nor EDN: {error}"))?;
            options.on_abort_tx = Some(on_abort.clone());
        }
    }

    let opened = connection
        .saga_open(&options)
        .await
        .map_err(|error| error.to_string())?;
    println!("{}", render(&opened.entry));
    Ok(())
}

async fn list(args: ListArgs) -> Result<(), String> {
    let connection = connect(args.db.clone(), &args.client).await?;
    let db = connection.sync().await.map_err(|error| error.to_string())?;
    require_registry(&db)?;
    let wanted = args.status.as_deref().map(parse_status).transpose()?;
    for entry in corium_db::saga::entries(&db) {
        if wanted
            .as_ref()
            .is_none_or(|wanted| entry.status.as_ref() == Some(wanted))
        {
            println!("{}", render(&entry));
        }
    }
    Ok(())
}

async fn status(args: StatusArgs) -> Result<(), String> {
    let connection = connect(args.db.clone(), &args.client).await?;
    let db = connection.sync().await.map_err(|error| error.to_string())?;
    require_registry(&db)?;
    let id = parse_saga_id(&args.id).ok_or_else(|| format!("invalid saga id {:?}", args.id))?;
    let entry = corium_db::saga::entry(&db, id)
        .ok_or_else(|| format!("no saga {} in {}", format_saga_id(id), args.db))?;
    println!("{}", render_full(&entry));
    for compensation in &entry.compensations {
        println!(
            "{{:compensation {:?} :status {} :detail {} :completed-at {} :error {}}}",
            compensation.key.as_deref().unwrap_or_default(),
            compensation
                .status
                .as_ref()
                .map_or_else(|| "nil".to_owned(), Keyword::to_string),
            optional_string(compensation.detail.as_deref()),
            optional_instant(compensation.completed_at),
            optional_string(compensation.error.as_deref()),
        );
    }
    Ok(())
}

async fn extend(args: ExtendArgs) -> Result<(), String> {
    let connection = connect(args.db.clone(), &args.client).await?;
    let id = parse_saga_id(&args.id).ok_or_else(|| format!("invalid saga id {:?}", args.id))?;
    let expires_at = deadline(args.expires_at.as_deref(), &args.ttl)?;
    let extended = connection
        .saga_extend(id, expires_at)
        .await
        .map_err(|error| error.to_string())?;
    println!("{}", render(&extended.entry));
    Ok(())
}

async fn commit(args: CommitArgs) -> Result<ExitCode, String> {
    let connection = connect(args.db.clone(), &args.client).await?;
    let id = parse_saga_id(&args.id).ok_or_else(|| format!("invalid saga id {:?}", args.id))?;
    let guards = forms_of(&args.guard, "--guard")?;
    let resolutions = forms_of(&args.resolve, "--resolve")?;
    let outcome = connection
        .saga_commit(id, guards, resolutions)
        .await
        .map_err(|error| error.to_string())?;
    match outcome {
        MergeOutcome::Committed(report) => {
            println!(
                "{{:saga {:?} :committed true :t {} :basis-before {} :steps {} :datoms {}}}",
                format_saga_id(id),
                report.basis_t,
                report.basis_before,
                report.steps,
                report.datoms,
            );
            Ok(ExitCode::SUCCESS)
        }
        MergeOutcome::Conflict(report) => {
            println!(
                "{{:saga {:?} :committed false :t {} :steps {} :datoms {}}}",
                format_saga_id(id),
                report.basis_t,
                report.steps,
                report.datoms,
            );
            println!("{}", report.report);
            Ok(ExitCode::FAILURE)
        }
    }
}

/// Reads one EDN form per repeated flag, so `--guard` twice is two guards
/// rather than one form somebody has to bracket by hand.
fn forms_of(texts: &[String], flag: &str) -> Result<Vec<Edn>, String> {
    texts
        .iter()
        .map(|text| {
            let forms = read_all(text).map_err(|error| format!("{flag} is not EDN: {error}"))?;
            match forms.as_slice() {
                [form] => Ok(form.clone()),
                other => Err(format!(
                    "{flag} takes one form, not {} (repeat the flag)",
                    other.len()
                )),
            }
        })
        .collect()
}

async fn abort(args: AbortArgs) -> Result<(), String> {
    let connection = connect(args.db.clone(), &args.client).await?;
    let id = parse_saga_id(&args.id).ok_or_else(|| format!("invalid saga id {:?}", args.id))?;
    let compensation: Vec<Edn> = match &args.compensate {
        Some(text) => {
            read_all(text).map_err(|error| format!("--compensate is not EDN: {error}"))?
        }
        None => Vec::new(),
    };
    let aborted = connection
        .saga_abort_with(id, compensation)
        .await
        .map_err(|error| error.to_string())?;
    println!("{}", render(&aborted.entry));
    Ok(())
}

async fn step(args: StepArgs) -> Result<(), String> {
    let connection = connect(args.db.clone(), &args.client).await?;
    let text = if args.tx_data == "-" {
        std::io::read_to_string(std::io::stdin())
            .map_err(|error| format!("cannot read transaction data: {error}"))?
    } else {
        args.tx_data.clone()
    };
    let forms = tx_forms(&text)?;
    let branch = branch(&connection, &args.id).await?;
    let applied = branch
        .step(forms)
        .await
        .map_err(|error| error.to_string())?;
    println!(
        "{{:saga {:?} :t {} :basis-before {} :tempids {}}}",
        format_saga_id(branch.saga_id()),
        applied.basis_t,
        applied.basis_before,
        applied.tempids.len()
    );
    Ok(())
}

/// Reads transaction data written either as a sequence of forms or as one
/// bracketed vector of them, since both are how people write it.
///
/// The ambiguity is only apparent: a bare `[:db/add …]` form starts with a
/// keyword, while a vector *of* forms starts with a map or a vector.
fn tx_forms(text: &str) -> Result<Vec<Edn>, String> {
    let read = read_all(text).map_err(|error| format!("transaction data is not EDN: {error}"))?;
    if let [Edn::Vector(items) | Edn::List(items)] = read.as_slice()
        && matches!(
            items.first(),
            Some(Edn::Map(_) | Edn::Vector(_) | Edn::List(_))
        )
    {
        return Ok(items.clone());
    }
    Ok(read)
}

async fn log(args: LogArgs) -> Result<(), String> {
    let connection = connect(args.db.clone(), &args.client).await?;
    let branch = branch(&connection, &args.id).await?;
    branch.sync().await.map_err(|error| error.to_string())?;
    // Only the branch's own transactions: the history below `t₀` is the
    // parent's, and `corium log` already prints that.
    for record in branch.steps() {
        if args.datoms {
            println!(
                "{{:t {} :instant {} :datoms {}}}",
                record.t,
                instant::format_instant(record.tx_instant),
                record.datoms.len()
            );
        } else {
            println!(
                "{{:t {} :instant {}}}",
                record.t,
                instant::format_instant(record.tx_instant)
            );
        }
    }
    Ok(())
}

/// Refuses a database whose writer predates the registry, rather than
/// reporting it as a database with no sagas.
fn require_registry(db: &Db) -> Result<(), String> {
    if registry_installed(db) {
        Ok(())
    } else {
        Err(
            "this database has no saga registry: it was created before the \
             `:db.saga/*` vocabulary existed"
                .to_owned(),
        )
    }
}

/// The deadline named by an explicit timestamp, or by a duration from now.
fn deadline(expires_at: Option<&str>, ttl: &str) -> Result<i64, String> {
    if let Some(text) = expires_at {
        return instant::parse_instant(text)
            .ok_or_else(|| format!("invalid timestamp {text:?}; use `YYYY-MM-DD HH:MM:SS`"));
    }
    let ttl = parse_duration(ttl)?;
    let now = i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| "the system clock is before the Unix epoch".to_owned())?
            .as_millis(),
    )
    .unwrap_or(i64::MAX);
    let millis = i64::try_from(ttl.as_millis()).unwrap_or(i64::MAX);
    Ok(now.saturating_add(millis))
}

/// The principal a saga is recorded under when the caller does not name one.
fn default_owner() -> String {
    std::env::var("USER")
        .or_else(|_| std::env::var("USERNAME"))
        .unwrap_or_else(|_| "unknown".to_owned())
}

/// An entity named by raw id (`1005`) or by ident (`:order/status`).
fn entity_of(db: &Db, text: &str) -> Result<EntityId, String> {
    if let Some(entity) = ident_entity(db, text) {
        return Ok(entity);
    }
    text.parse::<u64>()
        .map(EntityId::from_raw)
        .map_err(|_| format!("{text:?} is neither an entity id nor an installed ident"))
}

fn ident_entity(db: &Db, text: &str) -> Option<EntityId> {
    let keyword = text.strip_prefix(':')?;
    db.idents().entid(&Keyword::parse(keyword))
}

fn parse_status(text: &str) -> Result<SagaStatus, String> {
    let status = SagaStatus::from_keyword(&Keyword::new(
        Some(corium_db::saga::STATUS_NAMESPACE),
        text.trim_start_matches(':')
            .rsplit('/')
            .next()
            .unwrap_or(text),
    ));
    if matches!(status, SagaStatus::Unknown(_)) {
        return Err(format!(
            "unknown saga status {text:?}; use open, committed, aborted, or expired"
        ));
    }
    Ok(status)
}

/// One line per saga, in the EDN map form every other `corium` command uses.
fn render(entry: &SagaEntry) -> String {
    format!(
        "{{:saga {:?} :status {} :basis-t {} :owner {} :expires-at {} :sealed {} \
         :reserves {} :footprint {} :compensations {}}}",
        format_saga_id(entry.id),
        entry
            .status
            .as_ref()
            .map_or_else(|| "nil".to_owned(), SagaStatus::to_string),
        optional_long(entry.basis_t),
        optional_string(entry.owner.as_deref()),
        optional_instant(entry.expires_at),
        entry.sealed,
        entry.reserves.len(),
        entry.footprint.len(),
        entry.compensations.len(),
    )
}

/// The whole entry, for `saga status`: everything in [`render`] plus what
/// only matters once something has gone right or wrong.
fn render_full(entry: &SagaEntry) -> String {
    let mut rendered = render(entry);
    rendered.pop();
    format!(
        "{rendered} :description {} :merged-tx {} :steps {} :conflict-report {} \
         :on-abort-tx {} :on-abort-fn {} :on-abort-error {}}}",
        optional_string(entry.description.as_deref()),
        entry
            .merged_tx
            .map_or_else(|| "nil".to_owned(), |tx| tx.raw().to_string()),
        optional_long(entry.steps),
        optional_string(entry.conflict_report.as_deref()),
        optional_string(entry.on_abort_tx.as_deref()),
        entry
            .on_abort_fn
            .map_or_else(|| "nil".to_owned(), |e| e.raw().to_string()),
        optional_string(entry.on_abort_error.as_deref()),
    )
}

fn optional_string(value: Option<&str>) -> String {
    value.map_or_else(|| "nil".to_owned(), |value| format!("{value:?}"))
}

fn optional_long(value: Option<i64>) -> String {
    value.map_or_else(|| "nil".to_owned(), |value| value.to_string())
}

fn optional_instant(value: Option<i64>) -> String {
    value.map_or_else(
        || "nil".to_owned(),
        |millis| format!("{:?}", instant::format_instant(millis)),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_status_filter_accepts_every_spelling_of_a_state() {
        for text in ["open", ":open", ":db.saga.status/open"] {
            assert_eq!(parse_status(text).expect("parses"), SagaStatus::Open);
        }
        assert!(parse_status("paused").is_err());
    }

    #[test]
    fn a_deadline_comes_from_a_timestamp_or_a_duration() {
        assert_eq!(
            deadline(Some("2026-08-26 10:00:00"), DEFAULT_TTL).expect("parses"),
            1_787_738_400_000
        );
        let from_ttl = deadline(None, "1h").expect("parses");
        let now = i64::try_from(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("after the epoch")
                .as_millis(),
        )
        .expect("fits");
        assert!((from_ttl - now - 3_600_000).abs() < 5_000);
        assert!(deadline(Some("yesterday"), DEFAULT_TTL).is_err());
    }

    #[test]
    fn a_rendered_entry_is_one_edn_map() {
        let entry = SagaEntry {
            entity: EntityId::from_raw(1_000),
            id: 42,
            status: Some(SagaStatus::Open),
            basis_t: Some(7),
            description: Some("repair".into()),
            owner: Some("alice".into()),
            expires_at: Some(0),
            grants: Vec::new(),
            footprint: Vec::new(),
            reserves: Vec::new(),
            sealed: false,
            merged_tx: None,
            steps: None,
            conflict_report: None,
            on_abort_tx: None,
            on_abort_fn: None,
            on_abort_error: None,
            compensations: Vec::new(),
        };
        let rendered = render(&entry);
        assert!(
            rendered.starts_with('{') && rendered.ends_with('}'),
            "{rendered}"
        );
        assert!(
            rendered.contains(":status :db.saga.status/open"),
            "{rendered}"
        );
        assert!(
            rendered.contains(":expires-at \"1970-01-01 00:00:00.000\""),
            "{rendered}"
        );
        let full = render_full(&entry);
        assert!(full.ends_with(":on-abort-error nil}"), "{full}");
        assert!(full.contains(":description \"repair\""), "{full}");
    }
}
