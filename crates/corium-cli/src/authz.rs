//! `corium authz *` — creating and operating the self-hosted authorization
//! database (see `docs/design/auth.md`).
//!
//! Everything here speaks to an ordinary Corium database over the ordinary
//! client path: `init` creates it with the reserved schema, `grant`/`revoke`
//! transact relationship tuples, and `check`/`status` read a snapshot and run
//! the same evaluator a server runs, so an operator can ask "why is this
//! allowed?" without attaching to a running transactor's decisions.

use std::sync::Arc;

use clap::Subcommand;
use corium_authz::model::{action_from_name, action_names};
use corium_authz::source::MemoryPolicySource;
use corium_authz::{Policy, SystemDbAuthorizer, bootstrap, schema};
use corium_peer::{Admin, Connection};
use corium_protocol::authz::{Access, Action, Principal};
use corium_query::edn::Edn;

use crate::ClientFlags;

/// Operations on the authorization database.
#[derive(Subcommand)]
pub enum AuthzCommand {
    /// Create the authorization database with the reserved schema, the default
    /// action-to-relation permissions, and a first administrator.
    ///
    /// Run this once, against a transactor started *without* `--authz-db`;
    /// then restart the servers with `--authz-db` to enforce the policy.
    ///
    /// The administrator defaults to the identity a `--serve-token` /
    /// development-token client presents (`operator`, vouched for by
    /// `static-token`), so the CLI that created the database can still
    /// administer it once enforcement is on. Point `--admin`/`--provider` at
    /// your real identity — or pass `--no-admin` and write the tuples
    /// yourself — when that is not what you want.
    Init {
        /// Authorization database name.
        #[arg(long, default_value = schema::DEFAULT_AUTHZ_DB)]
        db: String,
        /// Subject id of the first administrator: made `owner` of `catalog:*`
        /// and of every database.
        #[arg(long, default_value = "operator", conflicts_with = "no_admin")]
        admin: String,
        /// Identity provider that must vouch for the administrator. `any`
        /// accepts the subject id from every provider.
        #[arg(long, default_value = "static-token", conflicts_with = "no_admin")]
        provider: String,
        /// Install schema and permissions only, granting nobody anything.
        #[arg(long)]
        no_admin: bool,
        #[command(flatten)]
        client: ClientFlags,
    },
    /// Assert the relationship tuple `subject relation object`.
    ///
    /// `corium authz grant alice writer database:music`
    Grant {
        /// Subject: `user:alice`, `group:eng`, `group:eng#member`, `role:ops`
        /// (a bare name is read as `user:<name>`).
        subject: String,
        /// Relation name, e.g. `owner`, `writer`, `viewer`, `member`, `parent`.
        relation: String,
        /// Object: `database:music`, `tenant:acme`, `catalog:*`, `database:*`.
        object: String,
        /// Authorization database name.
        #[arg(long, default_value = schema::DEFAULT_AUTHZ_DB)]
        db: String,
        #[command(flatten)]
        client: ClientFlags,
    },
    /// Retract the relationship tuple `subject relation object`.
    Revoke {
        /// Subject the tuple names.
        subject: String,
        /// Relation the tuple names.
        relation: String,
        /// Object the tuple names.
        object: String,
        /// Authorization database name.
        #[arg(long, default_value = schema::DEFAULT_AUTHZ_DB)]
        db: String,
        #[command(flatten)]
        client: ClientFlags,
    },
    /// Ask what the policy would decide, printing the matched path.
    Check {
        /// Subject id of the principal to test.
        subject: String,
        /// Action name, e.g. `query`, `transact`, `create-database`.
        action: String,
        /// Target database; omit for catalog-wide actions.
        #[arg(long)]
        database: Option<String>,
        /// Identity provider that vouched for the subject.
        #[arg(long, default_value = "oidc")]
        provider: String,
        /// Role the principal's credentials assert (repeatable).
        #[arg(long = "role")]
        roles: Vec<String>,
        /// Claim the principal carries, as `key=value` (repeatable).
        #[arg(long = "claim")]
        claims: Vec<String>,
        /// Authorization database name.
        #[arg(long, default_value = schema::DEFAULT_AUTHZ_DB)]
        db: String,
        #[command(flatten)]
        client: ClientFlags,
    },
    /// Print the compiled policy's basis and entity counts.
    Status {
        /// Authorization database name.
        #[arg(long, default_value = schema::DEFAULT_AUTHZ_DB)]
        db: String,
        #[command(flatten)]
        client: ClientFlags,
    },
}

/// Runs an `authz` subcommand.
pub async fn run(command: AuthzCommand) -> Result<(), String> {
    match command {
        AuthzCommand::Init {
            db,
            admin,
            provider,
            no_admin,
            client,
        } => {
            let admin = if no_admin { None } else { Some(admin) };
            let provider = match provider.as_str() {
                "any" | "*" => None,
                provider => Some(provider.to_owned()),
            };
            init(&db, admin.as_deref(), provider.as_deref(), &client).await
        }
        AuthzCommand::Grant {
            subject,
            relation,
            object,
            db,
            client,
        } => grant(&db, &subject, &relation, &object, &client).await,
        AuthzCommand::Revoke {
            subject,
            relation,
            object,
            db,
            client,
        } => revoke(&db, &subject, &relation, &object, &client).await,
        AuthzCommand::Check {
            subject,
            action,
            database,
            provider,
            roles,
            claims,
            db,
            client,
        } => {
            check(
                &db,
                &subject,
                &action,
                database.as_deref(),
                &provider,
                &roles,
                &claims,
                &client,
            )
            .await
        }
        AuthzCommand::Status { db, client } => status(&db, &client).await,
    }
}

async fn init(
    db: &str,
    admin: Option<&str>,
    provider: Option<&str>,
    client: &ClientFlags,
) -> Result<(), String> {
    let mut catalog = Admin::connect(&client.primary(), client.token(), client.tls()?)
        .await
        .map_err(|error| format!("cannot connect to transactor: {error}"))?;
    let created = catalog
        .create_database(db, &schema::schema_forms())
        .await
        .map_err(|error| format!("cannot create {db:?}: {error}"))?;

    let connection = connect(db, client).await?;
    let mut forms = Vec::new();
    if created {
        forms.extend(schema::default_permission_forms());
    }
    if let Some(admin) = admin {
        let snapshot = connection.db();
        if let Some(provider) = provider {
            forms.push(bootstrap::principal_form(admin, Some(provider), &[]));
        }
        // Owner of the catalog *and* of every database: the catalog grant
        // covers create/delete, the database wildcard covers everything inside
        // databases that exist now or later.
        for object in ["catalog:*", "database:*"] {
            if bootstrap::find_tuple(&snapshot, admin, "owner", object).is_none() {
                forms.push(bootstrap::tuple_form(admin, "owner", object));
            }
        }
    }
    if !forms.is_empty() {
        connection
            .transact(forms)
            .await
            .map_err(|error| format!("cannot install policy: {error}"))?;
    }
    let basis = connection
        .sync()
        .await
        .map_err(|error| error.to_string())?
        .basis_t();
    println!(
        "{{:authz-db {db:?} :created {created} :admin {} :provider {} :authz-t {basis}}}",
        admin.map_or_else(|| "nil".to_owned(), |admin| format!("{admin:?}")),
        provider.map_or_else(|| "nil".to_owned(), |provider| format!("{provider:?}"))
    );
    if created {
        eprintln!(
            "corium authz: {db:?} is ready; restart the transactor and peer servers \
             with --authz-db {db} to enforce it"
        );
        match admin {
            Some(admin) => eprintln!(
                "corium authz: {admin:?} owns catalog:* and database:*; \
                 grant others with `corium authz grant <subject> <relation> <object>`"
            ),
            None => eprintln!(
                "corium authz: nobody holds any relation yet — every request will be denied \
                 once --authz-db is enabled"
            ),
        }
    }
    Ok(())
}

async fn grant(
    db: &str,
    subject: &str,
    relation: &str,
    object: &str,
    client: &ClientFlags,
) -> Result<(), String> {
    let connection = connect(db, client).await?;
    if bootstrap::find_tuple(&connection.db(), subject, relation, object).is_some() {
        println!("{{:granted false :reason \"tuple already present\"}}");
        return Ok(());
    }
    let result = connection
        .transact(vec![bootstrap::tuple_form(subject, relation, object)])
        .await
        .map_err(|error| format!("cannot grant: {error}"))?;
    println!(
        "{{:granted true :subject {subject:?} :relation {relation:?} :object {object:?} :authz-t {}}}",
        result.basis_t
    );
    Ok(())
}

async fn revoke(
    db: &str,
    subject: &str,
    relation: &str,
    object: &str,
    client: &ClientFlags,
) -> Result<(), String> {
    let connection = connect(db, client).await?;
    let snapshot = connection
        .sync()
        .await
        .map_err(|error| format!("cannot read {db:?}: {error}"))?;
    let Some(entity) = bootstrap::find_tuple(&snapshot, subject, relation, object) else {
        println!("{{:revoked false :reason \"no such tuple\"}}");
        return Ok(());
    };
    let result = connection
        .transact(vec![bootstrap::retract_entity_form(entity)])
        .await
        .map_err(|error| format!("cannot revoke: {error}"))?;
    println!(
        "{{:revoked true :subject {subject:?} :relation {relation:?} :object {object:?} :authz-t {}}}",
        result.basis_t
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn check(
    db: &str,
    subject: &str,
    action: &str,
    database: Option<&str>,
    provider: &str,
    roles: &[String],
    claims: &[String],
    client: &ClientFlags,
) -> Result<(), String> {
    let action = action_from_name(action).ok_or_else(|| {
        format!(
            "unknown action {action:?}; expected one of {}",
            action_names().join(", ")
        )
    })?;
    let mut principal = Principal::new(provider, subject);
    for role in roles {
        principal = principal.with_role(role.clone());
    }
    for claim in claims {
        let (key, value) = claim
            .split_once('=')
            .ok_or_else(|| format!("claim {claim:?} is not key=value"))?;
        principal = principal.with_claim(key, value);
    }
    let access = match database {
        Some(database) => Access::on(action, database),
        None => Access::catalog(action),
    };

    let snapshot = connect(db, client)
        .await?
        .sync()
        .await
        .map_err(|error| format!("cannot read {db:?}: {error}"))?;
    let authorizer =
        SystemDbAuthorizer::new(Arc::new(MemoryPolicySource::new(db.to_owned(), snapshot)));
    let decision = authorizer.check(&principal, &access).await;
    println!(
        "{{:decision {} :subject {subject:?} :action {} :object {:?} :authz-t {} :path {} :views [{}]{}}}",
        if decision.is_allowed() {
            if decision.filter().is_some() {
                ":allow-filtered"
            } else {
                ":allow"
            }
        } else {
            ":deny"
        },
        action_label(action),
        decision.object,
        decision.authz_t,
        decision
            .path
            .as_ref()
            .map_or_else(|| "nil".to_owned(), |path| format!("{path:?}")),
        decision
            .views
            .iter()
            .map(|view| format!("{view:?}"))
            .collect::<Vec<_>>()
            .join(" "),
        decision
            .reason
            .as_ref()
            .map_or_else(String::new, |reason| format!(" :reason {reason:?}"))
    );
    Ok(())
}

async fn status(db: &str, client: &ClientFlags) -> Result<(), String> {
    let snapshot = connect(db, client)
        .await?
        .sync()
        .await
        .map_err(|error| format!("cannot read {db:?}: {error}"))?;
    let policy =
        Policy::compile(&snapshot).map_err(|error| format!("{db:?} is not usable: {error}"))?;
    let stats = policy.stats();
    println!(
        "{{:authz-db {db:?} :authz-t {} :principals {} :objects {} :tuples {} :permissions {} :rewrites {} :views {} :bindings {}}}",
        policy.basis_t(),
        stats.principals,
        stats.objects,
        stats.tuples,
        stats.permissions,
        stats.rewrites,
        stats.views,
        stats.bindings
    );
    Ok(())
}

async fn connect(db: &str, client: &ClientFlags) -> Result<Connection, String> {
    let config = client.connect_config(db.to_owned()).await?;
    Connection::connect(config)
        .await
        .map_err(|error| format!("cannot open {db:?}: {error}"))
}

fn action_label(action: Action) -> Edn {
    Edn::keyword(corium_authz::model::action_name(action))
}
