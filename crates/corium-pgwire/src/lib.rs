//! A `PostgreSQL` wire-protocol front end for Corium SQL.
//!
//! [`serve`] accepts `PostgreSQL` client connections. Reads run through
//! [`corium_sql::SqlSession`] against an immutable [`corium_db::Db`] value
//! obtained from a [`DbCatalog`]. Supported DML is planned into ordinary
//! Corium transaction forms and committed through the catalog.
//!
//! One server exposes every database the catalog offers (subject to the
//! catalog's own whitelist). A connection selects its database with the
//! standard startup `database` parameter and can switch at any time with
//! `USE <database>`; `SHOW DATABASES` lists what is available. The catalog is
//! expected to open and cache databases lazily and share them across
//! connections.
//!
//! Both simple and extended query sub-protocols are supported, including
//! typed bound inputs and text or binary results. Mutations are
//! autocommit-only: explicit transaction blocks allow reads but reject writes
//! until atomic multi-statement transactions are implemented.

mod protocol;
mod types;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::future::Future;
use std::sync::Arc;

use corium_core::EntityId;
use corium_db::Db;
use corium_db::protect::Hydrator;
use corium_db::read::{AttrVisibility, ReadContext};
use corium_protocol::authz::{
    Access, Action, Credentials, Guard, KeyGrant, KeyPolicyMode, Principal, ReadGrant,
};
use corium_query::edn::Edn;
use corium_sql::{MutationKind, SqlColumn, SqlError, SqlSession, SqlType};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;

use protocol::{BackendWriter, ErrorFields, FieldDescription, Frontend, FrontendReader};

/// A database the catalog cannot hand back.
#[derive(Debug, Error)]
pub enum CatalogError {
    /// No such database, or it is not permitted by the catalog's whitelist.
    #[error("database {0:?} is not available")]
    NotFound(String),
    /// The database exists but could not be reached or opened.
    #[error("{0}")]
    Unavailable(String),
    /// This catalog intentionally exposes snapshots without a write path.
    #[error("database {0:?} is read-only through this catalog")]
    ReadOnly(String),
    /// A conditional transaction lost a concurrency race.
    #[error("{0}")]
    Conflict(String),
    /// The transactor rejected validly transported transaction data.
    #[error("{0}")]
    Rejected(String),
    /// The configured Corium principal is not allowed to transact.
    #[error("{0}")]
    Denied(String),
    /// The remote component does not support the requested write capability
    /// or protocol version.
    #[error("{0}")]
    Unsupported(String),
}

/// Result of a catalog transaction, synchronized through its committed basis.
pub struct CatalogTxResult {
    /// Database value including the committed transaction.
    pub db_after: Db,
    /// Tempids allocated by the transaction.
    pub tempids: BTreeMap<String, EntityId>,
}

/// Supplies the databases the server exposes.
///
/// Implementations are expected to open databases lazily and cache them so a
/// database is shared across all client connections. [`db`](DbCatalog::db)
/// returns a fresh immutable snapshot each call, the same way the `corium sql`
/// shell captures a current `Db` per statement.
#[async_trait::async_trait]
pub trait DbCatalog: Send + Sync + 'static {
    /// Names of the databases clients may connect to.
    ///
    /// # Errors
    /// Returns [`CatalogError`] when the catalog cannot be enumerated.
    async fn list(&self) -> Result<Vec<String>, CatalogError>;

    /// A current snapshot of the database named `name`.
    ///
    /// # Errors
    /// Returns [`CatalogError::NotFound`] when the database is unknown or not
    /// permitted, and [`CatalogError::Unavailable`] when it cannot be opened.
    async fn db(&self, name: &str) -> Result<Db, CatalogError>;

    /// The class keys this process can resolve for `name`.
    ///
    /// What a given *principal* may hydrate is decided per request from this
    /// set and the authorization policy; a catalog only reports what it
    /// holds. The default holds none, which is the correct answer for a
    /// catalog serving snapshots it never had keys for.
    async fn hydrator(&self, _name: &str) -> Result<Arc<Hydrator>, CatalogError> {
        Ok(Arc::new(Hydrator::default()))
    }

    /// Commits forms only if `expected_basis_t` is still current.
    ///
    /// The default keeps read-only catalog implementations source-compatible.
    async fn transact(
        &self,
        name: &str,
        _expected_basis_t: u64,
        _forms: Vec<Edn>,
    ) -> Result<CatalogTxResult, CatalogError> {
        Err(CatalogError::ReadOnly(name.to_owned()))
    }
}

/// Server-wide configuration for the `PostgreSQL` front end.
#[derive(Clone)]
pub struct PgWireConfig {
    /// If set, clients must send this cleartext password to connect. When
    /// `None` and no [`Guard`] is configured, connections are trusted.
    ///
    /// Ignored once `guard` is enforcing: the password field then carries the
    /// caller's own credential, not one shared secret.
    pub password: Option<String>,
    /// `server_version` reported to clients in a `ParameterStatus` message.
    pub server_version: String,
    /// The identity and authorization policy this server enforces.
    ///
    /// `PostgreSQL` has no bearer-token field, so the **password carries the
    /// token**: whatever the client sends is offered to the guard's
    /// [`IdentityProvider`] as a credential, and the startup `user` is
    /// informational. That is what makes a SQL client a Corium principal
    /// rather than an anonymous sharer of the server's own identity.
    pub guard: Guard,
    /// How a principal's key set is chosen, or `None` to derive it from the
    /// guard (see [`KeyPolicyMode`]).
    pub key_policy: Option<KeyPolicyMode>,
}

impl std::fmt::Debug for PgWireConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PgWireConfig")
            .field("password", &self.password.is_some())
            .field("server_version", &self.server_version)
            .field("guard_enabled", &!self.guard.is_disabled())
            .field("key_policy", &self.key_policy)
            .finish()
    }
}

impl Default for PgWireConfig {
    fn default() -> Self {
        Self {
            password: None,
            server_version: concat!("16.0 (corium ", env!("CARGO_PKG_VERSION"), ")").to_owned(),
            guard: Guard::disabled(),
            key_policy: None,
        }
    }
}

/// Serves the `PostgreSQL` wire protocol until `shutdown` resolves.
///
/// Each accepted connection is handled on its own task; per-connection
/// failures are logged and do not stop the server.
///
/// # Errors
/// Returns an error only if accepting a connection fails fatally.
pub async fn serve<C, F>(
    listener: TcpListener,
    catalog: Arc<C>,
    config: PgWireConfig,
    shutdown: F,
) -> std::io::Result<()>
where
    C: DbCatalog,
    F: Future<Output = ()>,
{
    let config = Arc::new(config);
    let hydrators = Arc::new(Hydrators::default());
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            () = &mut shutdown => return Ok(()),
            accepted = listener.accept() => {
                let (stream, peer) = accepted?;
                let catalog = Arc::clone(&catalog);
                let config = Arc::clone(&config);
                let hydrators = Arc::clone(&hydrators);
                tokio::spawn(async move {
                    let (read, write) = stream.into_split();
                    let mut session = ConnectionSession::new(
                        FrontendReader::new(read),
                        BackendWriter::new(write),
                        catalog,
                        config,
                        hydrators,
                    );
                    if let Err(error) = session.run().await {
                        tracing::debug!(%peer, %error, "pgwire connection closed");
                    }
                });
            }
        }
    }
}

/// A bound portal: the SQL to run and the database it was bound against.
struct Portal {
    sql: String,
    database: Option<String>,
    params: Vec<corium_sql::SqlValue>,
    result_formats: Vec<i16>,
}

struct PreparedStatement {
    sql: String,
    parameter_types: Vec<i32>,
}

/// How a statement should be handled before it ever reaches `SqlSession`.
enum Statement {
    /// A stateless control statement accepted as a no-op with this tag.
    Control(&'static str),
    /// Start an explicit transaction block. Reads are allowed, but writes are
    /// rejected until true multi-statement transactions are implemented.
    Begin,
    /// End an explicit transaction block.
    Commit,
    /// Abandon an explicit transaction block.
    Rollback,
    /// `USE <database>` — switch the connection's active database.
    Use(String),
    /// `SHOW DATABASES` — list the catalog.
    ShowDatabases,
    /// An ordinary read-only query for `SqlSession`.
    Query,
    /// `INSERT`, `UPDATE`, or `DELETE`.
    Mutation,
}

/// One statement's database value and the read it is answered under.
struct Scope {
    db: Db,
    read: ReadContext,
}

/// Cache key for one principal's hydrator: the database, and the key ids the
/// policy granted (`None` for the process's whole keyring).
type HydratorKey = (String, Option<BTreeSet<String>>);

/// Largest number of distinct key sets one server keeps hydrators for.
///
/// Key sets are role-shaped rather than user-shaped, so this is generous; a
/// policy that somehow mints one per principal clears the map instead of
/// growing without bound.
const HYDRATOR_CACHE_CAP: usize = 256;

/// Hydrators shared by every connection, keyed by database and key set.
///
/// A [`Hydrator`] carries a bounded plaintext cache, and pgwire builds a
/// `SqlSession` per *statement*, so deriving a fresh one each time would make
/// a keyed principal re-open every sealed value it reads, on every statement.
#[derive(Default)]
struct Hydrators {
    entries: std::sync::Mutex<HashMap<HydratorKey, Arc<Hydrator>>>,
}

impl Hydrators {
    fn get(&self, key: &HydratorKey) -> Option<Arc<Hydrator>> {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(key)
            .map(Arc::clone)
    }

    fn insert(&self, key: HydratorKey, hydrator: &Arc<Hydrator>) -> Arc<Hydrator> {
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if entries.len() >= HYDRATOR_CACHE_CAP {
            entries.clear();
        }
        Arc::clone(entries.entry(key).or_insert_with(|| Arc::clone(hydrator)))
    }
}

/// A failure while dispatching one statement.
enum Dispatch {
    /// No database is selected for a query.
    NoDatabase,
    /// The catalog could not provide the database.
    Catalog(CatalogError),
    /// `SqlSession` rejected or failed the query.
    Sql(SqlError),
    /// The authorization policy refused this principal.
    Denied(String),
}

/// The per-connection protocol state machine.
struct ConnectionSession<R, W, C> {
    reader: FrontendReader<R>,
    writer: BackendWriter<W>,
    catalog: Arc<C>,
    config: Arc<PgWireConfig>,
    hydrators: Arc<Hydrators>,
    /// The connection's active database, chosen at startup or by `USE`.
    current_db: Option<String>,
    /// The authenticated caller. Every statement is authorized as this
    /// principal, and every read is answered under the view and key set the
    /// policy grants it.
    principal: Principal,
    statements: HashMap<String, PreparedStatement>,
    portals: HashMap<String, Portal>,
    /// Set after an extended-protocol error; frontend messages are ignored
    /// until the next `Sync`.
    failed: bool,
    /// Whether the client has entered an explicit transaction block.
    ///
    /// Corium SQL mutations are autocommit-only for now. Tracking the block
    /// prevents a `BEGIN; INSERT ...; COMMIT` sequence from appearing atomic
    /// while actually committing the insert immediately.
    in_transaction: bool,
    /// Whether an error has aborted the current explicit transaction.
    transaction_failed: bool,
    /// The immutable database value pinned by the first read in the current
    /// explicit transaction.
    transaction_db: Option<(String, Db)>,
}

impl<R, W, C> ConnectionSession<R, W, C>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
    C: DbCatalog,
{
    fn new(
        reader: FrontendReader<R>,
        writer: BackendWriter<W>,
        catalog: Arc<C>,
        config: Arc<PgWireConfig>,
        hydrators: Arc<Hydrators>,
    ) -> Self {
        Self {
            reader,
            writer,
            catalog,
            config,
            hydrators,
            current_db: None,
            principal: Principal::anonymous(),
            statements: HashMap::new(),
            portals: HashMap::new(),
            failed: false,
            in_transaction: false,
            transaction_failed: false,
            transaction_db: None,
        }
    }

    async fn run(&mut self) -> std::io::Result<()> {
        let startup = self.reader.read_startup(&mut self.writer).await?;
        if !self.authenticate().await? {
            return Ok(());
        }
        // The database is validated lazily on first use, so a client may
        // connect with an unknown default and then `USE` a real database.
        self.current_db = startup
            .get("database")
            .filter(|value| !value.is_empty())
            .map(str::to_owned);
        self.send_ready_banner(startup.get("application_name").unwrap_or(""))
            .await?;

        while let Some(message) = self.reader.read_message().await? {
            match message {
                Frontend::Query(sql) => {
                    // After an extended-protocol error the backend ignores
                    // every message until the next `Sync`.
                    if !self.failed {
                        self.simple_query(&sql).await?;
                        self.writer.flush().await?;
                    }
                }
                Frontend::Parse {
                    name,
                    query,
                    parameter_types,
                } => self.handle_parse(name, query, parameter_types),
                Frontend::Bind {
                    portal,
                    statement,
                    parameter_formats,
                    parameters,
                    result_formats,
                } => self.handle_bind(
                    &portal,
                    &statement,
                    &parameter_formats,
                    &parameters,
                    &result_formats,
                ),
                Frontend::Describe { kind, name } => self.handle_describe(kind, &name).await?,
                Frontend::Execute { portal } => self.handle_execute(&portal).await?,
                Frontend::Close { kind, name } => {
                    if !self.failed {
                        if kind == b'S' {
                            self.statements.remove(&name);
                        } else {
                            self.portals.remove(&name);
                        }
                        self.writer.close_complete();
                    }
                }
                Frontend::Sync => {
                    self.failed = false;
                    self.writer.ready_for_query(self.ready_status());
                    self.writer.flush().await?;
                }
                Frontend::Flush => self.writer.flush().await?,
                Frontend::Password(_) => {}
                Frontend::Terminate => break,
            }
        }
        Ok(())
    }

    /// Runs the authentication exchange. Returns `false` if the connection
    /// should be closed.
    ///
    /// With a [`Guard`] configured the password field carries the caller's
    /// own credential — a bearer token, typically a JWT — and the resulting
    /// [`Principal`] is what every statement is then authorized as. Without
    /// one, the legacy shared password applies and the caller stays
    /// anonymous.
    async fn authenticate(&mut self) -> std::io::Result<bool> {
        if self.config.guard.is_disabled() {
            return self.authenticate_with_shared_password().await;
        }
        self.writer.authentication_cleartext_password();
        self.writer.flush().await?;
        let supplied = match self.reader.read_message().await? {
            Some(Frontend::Password(supplied)) => supplied,
            _ => String::new(),
        };
        let credentials = Credentials {
            bearer: (!supplied.is_empty()).then_some(supplied),
            client_cert_subject: None,
        };
        match self.config.guard.authenticate(&credentials) {
            Ok(principal) => {
                self.principal = principal;
                self.writer.authentication_ok();
                Ok(true)
            }
            Err(error) => {
                self.writer.error_response(&ErrorFields {
                    code: "28000",
                    message: &error.to_string(),
                });
                self.writer.flush().await?;
                Ok(false)
            }
        }
    }

    async fn authenticate_with_shared_password(&mut self) -> std::io::Result<bool> {
        let Some(expected) = self.config.password.clone() else {
            self.writer.authentication_ok();
            return Ok(true);
        };
        self.writer.authentication_cleartext_password();
        self.writer.flush().await?;
        match self.reader.read_message().await? {
            Some(Frontend::Password(supplied)) if supplied == expected => {
                self.writer.authentication_ok();
                Ok(true)
            }
            _ => {
                self.writer.error_response(&ErrorFields {
                    code: "28P01",
                    message: "password authentication failed",
                });
                self.writer.flush().await?;
                Ok(false)
            }
        }
    }

    /// Sends the post-authentication parameter status, key data, and the
    /// first `ReadyForQuery`.
    async fn send_ready_banner(&mut self, application_name: &str) -> std::io::Result<()> {
        self.writer
            .parameter_status("server_version", &self.config.server_version);
        self.writer.parameter_status("server_encoding", "UTF8");
        self.writer.parameter_status("client_encoding", "UTF8");
        self.writer.parameter_status("DateStyle", "ISO, MDY");
        self.writer.parameter_status("TimeZone", "UTC");
        self.writer.parameter_status("integer_datetimes", "on");
        self.writer
            .parameter_status("standard_conforming_strings", "on");
        self.writer
            .parameter_status("application_name", application_name);
        self.writer.backend_key_data(0, 0);
        self.writer.ready_for_query(b'I');
        self.writer.flush().await
    }

    /// Handles a simple-query message: run each statement, stopping at the
    /// first error, then report `ReadyForQuery`.
    async fn simple_query(&mut self, sql: &str) -> std::io::Result<()> {
        let statements = split_statements(sql);
        if statements.is_empty() {
            self.writer.empty_query_response();
            self.writer.ready_for_query(self.ready_status());
            return Ok(());
        }
        for statement in statements {
            if !self.run_simple_statement(&statement).await? {
                break;
            }
        }
        self.writer.ready_for_query(self.ready_status());
        Ok(())
    }

    /// Runs one simple-protocol statement. Returns `false` when an error was
    /// reported and the rest of the query string should be abandoned.
    async fn run_simple_statement(&mut self, sql: &str) -> std::io::Result<bool> {
        let statement = classify(sql);
        if self.transaction_failed && !matches!(&statement, Statement::Rollback) {
            self.report_transaction_aborted();
            return Ok(false);
        }
        match statement {
            Statement::Control(tag) => {
                self.writer.command_complete(tag);
                Ok(true)
            }
            Statement::Begin => {
                self.begin_transaction();
                self.writer.command_complete("BEGIN");
                Ok(true)
            }
            Statement::Commit => {
                self.end_transaction();
                self.writer.command_complete("COMMIT");
                Ok(true)
            }
            Statement::Rollback => {
                self.end_transaction();
                self.writer.command_complete("ROLLBACK");
                Ok(true)
            }
            Statement::Use(name) => match self.use_database(&name).await {
                Ok(()) => {
                    self.writer.command_complete("USE");
                    Ok(true)
                }
                Err(error) => {
                    self.report_dispatch(&error);
                    Ok(false)
                }
            },
            Statement::ShowDatabases => match self.show_databases(true, &[]).await {
                Ok(()) => Ok(true),
                Err(error) => {
                    self.report_dispatch(&error);
                    Ok(false)
                }
            },
            Statement::Query => {
                let scope = match self.scope(None, Action::Query).await {
                    Ok(scope) => scope,
                    Err(error) => {
                        self.report_dispatch(&error);
                        return Ok(false);
                    }
                };
                match self.run_statement(&scope, sql, &[], true, &[]).await {
                    Ok(rows) => {
                        self.writer.command_complete(&command_tag(sql, rows));
                        Ok(true)
                    }
                    Err(error) => {
                        self.report_dispatch(&Dispatch::Sql(error));
                        Ok(false)
                    }
                }
            }
            Statement::Mutation => {
                if self.in_transaction {
                    self.report_dispatch(&explicit_transaction_write_error());
                    return Ok(false);
                }
                let Some(database) = self.current_db.clone() else {
                    self.report_dispatch(&Dispatch::NoDatabase);
                    return Ok(false);
                };
                let scope = match self.scope(Some(&database), Action::Transact).await {
                    Ok(scope) => scope,
                    Err(error) => {
                        self.report_dispatch(&error);
                        return Ok(false);
                    }
                };
                match self
                    .run_mutation(&database, &scope, sql, &[], true, &[])
                    .await
                {
                    Ok((kind, rows)) => {
                        self.writer
                            .command_complete(&mutation_command_tag(kind, rows));
                        Ok(true)
                    }
                    Err(error) => {
                        self.report_dispatch(&error);
                        Ok(false)
                    }
                }
            }
        }
    }

    fn handle_parse(&mut self, name: String, query: String, mut parameter_types: Vec<i32>) {
        if self.failed {
            return;
        }
        let inferred_count = placeholder_count(&query);
        if parameter_types.len() > inferred_count {
            self.fail_extended("08P01", "too many parameter types in Parse");
            return;
        }
        parameter_types.resize(inferred_count, 0);
        self.statements.insert(
            name,
            PreparedStatement {
                sql: query,
                parameter_types,
            },
        );
        self.writer.parse_complete();
    }

    fn handle_bind(
        &mut self,
        portal: &str,
        statement: &str,
        parameter_formats: &[i16],
        parameters: &[Option<Vec<u8>>],
        result_formats: &[i16],
    ) {
        if self.failed {
            return;
        }
        if result_formats.iter().any(|format| !matches!(format, 0 | 1)) {
            self.fail_extended("08P01", "result format code must be zero or one");
            return;
        }
        let Some(prepared) = self.statements.get(statement) else {
            self.fail_extended("26000", "prepared statement does not exist");
            return;
        };
        if parameters.len() != prepared.parameter_types.len() {
            self.fail_extended("08P01", "bound parameter count does not match Parse");
            return;
        }
        let formats = match expand_formats(parameter_formats, parameters.len()) {
            Ok(formats) => formats,
            Err(message) => {
                self.fail_extended("08P01", message);
                return;
            }
        };
        let params = prepared
            .parameter_types
            .iter()
            .zip(formats)
            .zip(parameters)
            .map(|((oid, format), value)| types::decode_parameter(*oid, format, value.as_deref()))
            .collect::<Result<Vec<_>, _>>();
        let params = match params {
            Ok(params) => params,
            Err(message) => {
                self.fail_extended("22P02", &message);
                return;
            }
        };
        self.portals.insert(
            portal.to_owned(),
            Portal {
                sql: prepared.sql.clone(),
                database: self.current_db.clone(),
                params,
                result_formats: result_formats.to_vec(),
            },
        );
        self.writer.bind_complete();
    }

    async fn handle_describe(&mut self, kind: u8, name: &str) -> std::io::Result<()> {
        if self.failed {
            return Ok(());
        }
        // `Describe` of a prepared statement first reports its parameters.
        let (sql, database, params, result_formats) = if kind == b'S' {
            let Some((sql, parameter_types)) = self
                .statements
                .get(name)
                .map(|statement| (statement.sql.clone(), statement.parameter_types.clone()))
            else {
                self.fail_extended("26000", "prepared statement does not exist");
                return Ok(());
            };
            self.writer.parameter_description(&parameter_types);
            let params = parameter_types
                .into_iter()
                .map(types::describe_parameter)
                .collect::<Result<Vec<_>, _>>();
            let params = match params {
                Ok(params) => params,
                Err(message) => {
                    self.fail_extended("0A000", &message);
                    return Ok(());
                }
            };
            (sql, self.current_db.clone(), params, Vec::new())
        } else {
            let Some(portal) = self.portals.get(name) else {
                self.fail_extended("34000", "portal does not exist");
                return Ok(());
            };
            (
                portal.sql.clone(),
                portal.database.clone(),
                portal.params.clone(),
                portal.result_formats.clone(),
            )
        };
        match classify(&sql) {
            Statement::ShowDatabases => {
                self.write_row_description(&[database_field()], &result_formats);
            }
            Statement::Query if !sql.trim().is_empty() => {
                let scope = match self.scope(database.as_deref(), Action::Query).await {
                    Ok(scope) => scope,
                    Err(error) => {
                        self.fail_dispatch(&error);
                        return Ok(());
                    }
                };
                match self.describe_columns(&scope, &sql, &params).await {
                    Ok(fields) => {
                        self.write_row_description(&fields, &result_formats);
                    }
                    Err(error) => self.fail_dispatch(&Dispatch::Sql(error)),
                }
            }
            Statement::Mutation => {
                let scope = match self.scope(database.as_deref(), Action::Transact).await {
                    Ok(scope) => scope,
                    Err(error) => {
                        self.fail_dispatch(&error);
                        return Ok(());
                    }
                };
                match SqlSession::with_read(&scope.db, &scope.read) {
                    Ok(session) => match session.mutation_columns(&sql, &params).await {
                        Ok(Some(columns)) if columns.is_empty() => self.writer.no_data(),
                        Ok(Some(columns)) => {
                            self.write_row_description(
                                &columns.iter().map(field_of).collect::<Vec<_>>(),
                                &result_formats,
                            );
                        }
                        Ok(None) => self.writer.no_data(),
                        Err(error) => self.fail_dispatch(&Dispatch::Sql(error)),
                    },
                    Err(error) => self.fail_dispatch(&Dispatch::Sql(error)),
                }
            }
            _ => self.writer.no_data(),
        }
        Ok(())
    }

    async fn handle_execute(&mut self, portal: &str) -> std::io::Result<()> {
        if self.failed {
            return Ok(());
        }
        let Some((sql, database, params, result_formats)) =
            self.portals.get(portal).map(|portal| {
                (
                    portal.sql.clone(),
                    portal.database.clone(),
                    portal.params.clone(),
                    portal.result_formats.clone(),
                )
            })
        else {
            self.fail_extended("34000", "portal does not exist");
            return Ok(());
        };
        if sql.trim().is_empty() {
            self.writer.empty_query_response();
            return Ok(());
        }
        let statement = classify(&sql);
        if self.transaction_failed && !matches!(&statement, Statement::Rollback) {
            self.fail_extended(
                "25P02",
                "current transaction is aborted; commands ignored until end of transaction block",
            );
            return Ok(());
        }
        match statement {
            Statement::Control(tag) => self.writer.command_complete(tag),
            Statement::Begin => {
                self.begin_transaction();
                self.writer.command_complete("BEGIN");
            }
            Statement::Commit => {
                self.end_transaction();
                self.writer.command_complete("COMMIT");
            }
            Statement::Rollback => {
                self.end_transaction();
                self.writer.command_complete("ROLLBACK");
            }
            Statement::Use(name) => match self.use_database(&name).await {
                Ok(()) => self.writer.command_complete("USE"),
                Err(error) => self.fail_dispatch(&error),
            },
            Statement::ShowDatabases => {
                if let Err(error) = self.show_databases(false, &result_formats).await {
                    self.fail_dispatch(&error);
                }
            }
            Statement::Query => {
                let scope = match self.scope(database.as_deref(), Action::Query).await {
                    Ok(scope) => scope,
                    Err(error) => {
                        self.fail_dispatch(&error);
                        return Ok(());
                    }
                };
                match self
                    .run_statement(&scope, &sql, &params, false, &result_formats)
                    .await
                {
                    Ok(rows) => self.writer.command_complete(&command_tag(&sql, rows)),
                    Err(error) => self.fail_dispatch(&Dispatch::Sql(error)),
                }
            }
            Statement::Mutation => {
                if self.in_transaction {
                    self.fail_dispatch(&explicit_transaction_write_error());
                    return Ok(());
                }
                let Some(database) = database.or_else(|| self.current_db.clone()) else {
                    self.fail_dispatch(&Dispatch::NoDatabase);
                    return Ok(());
                };
                let scope = match self.scope(Some(&database), Action::Transact).await {
                    Ok(scope) => scope,
                    Err(error) => {
                        self.fail_dispatch(&error);
                        return Ok(());
                    }
                };
                match self
                    .run_mutation(&database, &scope, &sql, &params, false, &result_formats)
                    .await
                {
                    Ok((kind, rows)) => self
                        .writer
                        .command_complete(&mutation_command_tag(kind, rows)),
                    Err(error) => self.fail_dispatch(&error),
                }
            }
        }
        Ok(())
    }

    /// Validates and activates `name` as the connection's database, warming
    /// the catalog cache in the process.
    async fn use_database(&mut self, name: &str) -> Result<(), Dispatch> {
        // Switching to a database this principal cannot inspect fails here
        // rather than on its first query, which is both friendlier and the
        // reason `SHOW DATABASES` and `USE` agree with each other.
        self.authorize(name, Action::Inspect).await?;
        self.snapshot(Some(name)).await?;
        self.current_db = Some(name.to_owned());
        Ok(())
    }

    /// Emits a one-column `database` result listing the catalog.
    ///
    /// The catalog is filtered to what this principal may inspect: the list
    /// of database names is itself information.
    async fn show_databases(
        &mut self,
        with_row_description: bool,
        result_formats: &[i16],
    ) -> Result<(), Dispatch> {
        let names = self.catalog.list().await.map_err(Dispatch::Catalog)?;
        let mut visible = Vec::with_capacity(names.len());
        for name in names {
            if self.authorize(&name, Action::Inspect).await.is_ok() {
                visible.push(name);
            }
        }
        if with_row_description {
            self.write_row_description(&[database_field()], result_formats);
        }
        for name in &visible {
            self.writer.data_row(&[Some(name.clone().into_bytes())]);
        }
        self.writer.command_complete("SHOW");
        Ok(())
    }

    /// Resolves an immutable snapshot for `database`, falling back to the
    /// connection's active database.
    async fn snapshot(&mut self, database: Option<&str>) -> Result<Db, Dispatch> {
        let name = database
            .map(str::to_owned)
            .or_else(|| self.current_db.clone())
            .ok_or(Dispatch::NoDatabase)?;
        if self.in_transaction {
            if let Some((pinned_name, pinned)) = &self.transaction_db {
                if pinned_name != &name {
                    return Err(Dispatch::Sql(SqlError::Mutation(
                        "cannot switch databases inside an explicit transaction".into(),
                    )));
                }
                return Ok(pinned.clone());
            }
            let db = self.catalog.db(&name).await.map_err(Dispatch::Catalog)?;
            self.transaction_db = Some((name, db.clone()));
            Ok(db)
        } else {
            self.catalog.db(&name).await.map_err(Dispatch::Catalog)
        }
    }

    /// Authorizes `action` on `database` for this connection's principal and
    /// resolves the snapshot to answer it from.
    ///
    /// This is the single place a statement becomes a decision, so no read
    /// path can reach a database value without one.
    async fn scope(&mut self, database: Option<&str>, action: Action) -> Result<Scope, Dispatch> {
        let name = database
            .map(str::to_owned)
            .or_else(|| self.current_db.clone())
            .ok_or(Dispatch::NoDatabase)?;
        let grant = self.authorize(&name, action).await?;
        let db = self.snapshot(Some(&name)).await?;
        let read = self.read_context(&name, &db, &grant).await?;
        Ok(Scope { db, read })
    }

    /// The policy's decision for `action` on `database`.
    async fn authorize(&self, database: &str, action: Action) -> Result<ReadGrant, Dispatch> {
        self.config
            .guard
            .authorize(&self.principal, &Access::on(action, database))
            .await
            .map(Option::unwrap_or_default)
            .map_err(|error| Dispatch::Denied(error.to_string()))
    }

    /// Turns a decision into the read one statement is answered under: the
    /// attributes this principal may see, and the class keys it may hydrate.
    async fn read_context(
        &self,
        database: &str,
        db: &Db,
        grant: &ReadGrant,
    ) -> Result<ReadContext, Dispatch> {
        let mode = self.config.key_policy.unwrap_or({
            if self.config.guard.is_disabled() {
                KeyPolicyMode::ServerWide
            } else {
                KeyPolicyMode::Strict
            }
        });
        let wanted = match (&grant.keys, mode) {
            (KeyGrant::Unrestricted, KeyPolicyMode::ServerWide) => None,
            (KeyGrant::Only(allowed), _) => Some(allowed.clone()),
            // Strict: a decision that names no key grants none.
            (KeyGrant::Unrestricted, KeyPolicyMode::Strict) => Some(BTreeSet::new()),
        };
        let key = (database.to_owned(), wanted);
        let hydrator = if let Some(hit) = self.hydrators.get(&key) {
            hit
        } else {
            let held = self
                .catalog
                .hydrator(database)
                .await
                .map_err(Dispatch::Catalog)?;
            let built = match &key.1 {
                None => held,
                Some(allowed) => {
                    Arc::new(Hydrator::new(held.keys().restrict_to(db.schema(), allowed)))
                }
            };
            self.hydrators.insert(key, &built)
        };
        let mut read = ReadContext::open().with_hydrator(hydrator);
        if let Some(view) = &grant.view {
            // One statement reads one database value, so there is one schema
            // to resolve ids against; the peer server, whose queries may bind
            // several views, resolves against all of them.
            read = read.with_visibility(Arc::new(AttrVisibility::resolve(
                db.schema(),
                db.idents(),
                |ident| view.attribute_visible(&ident.to_string()),
            )));
        }
        Ok(read)
    }

    fn ready_status(&self) -> u8 {
        if self.transaction_failed {
            b'E'
        } else if self.in_transaction {
            b'T'
        } else {
            b'I'
        }
    }

    /// Plans a query and returns its result columns without streaming rows.
    async fn describe_columns(
        &self,
        scope: &Scope,
        sql: &str,
        params: &[corium_sql::SqlValue],
    ) -> Result<Vec<FieldDescription>, SqlError> {
        let session = SqlSession::with_read(&scope.db, &scope.read)?;
        let query = session.query_params(sql, params).await?;
        Ok(query.columns().iter().map(field_of).collect())
    }

    /// Runs one statement, optionally emitting a `RowDescription` first, then
    /// streaming its rows as `DataRow` messages. Returns the row count.
    async fn run_statement(
        &mut self,
        scope: &Scope,
        sql: &str,
        params: &[corium_sql::SqlValue],
        with_row_description: bool,
        result_formats: &[i16],
    ) -> Result<usize, SqlError> {
        let session = SqlSession::with_read(&scope.db, &scope.read)?;
        let mut query = session.query_params(sql, params).await?;
        let columns = query.columns().to_vec();
        let formats = expand_result_formats(result_formats, columns.len())
            .map_err(|message| SqlError::Mutation(message.into()))?;
        if with_row_description {
            let fields = columns.iter().map(field_of).collect::<Vec<_>>();
            self.writer.row_description_with_formats(&fields, &formats);
        }
        let mut count = 0usize;
        while let Some(row) = query.next_row().await? {
            let values = row
                .iter()
                .zip(&columns)
                .zip(&formats)
                .map(|((value, column), format)| {
                    types::encode_result(value, &column.data_type, *format)
                        .map_err(SqlError::Mutation)
                })
                .collect::<Result<Vec<_>, _>>()?;
            self.writer.data_row(&values);
            count += 1;
            // Bound peak memory on large results by flushing periodically.
            if count.is_multiple_of(1024) {
                self.writer
                    .flush()
                    .await
                    .map_err(|error| SqlError::Schema(error.to_string()))?;
            }
        }
        Ok(count)
    }

    async fn run_mutation(
        &mut self,
        database: &str,
        scope: &Scope,
        sql: &str,
        params: &[corium_sql::SqlValue],
        with_row_description: bool,
        result_formats: &[i16],
    ) -> Result<(MutationKind, usize), Dispatch> {
        let db = &scope.db;
        let session = SqlSession::with_read(db, &scope.read).map_err(Dispatch::Sql)?;
        let mutation = session
            .mutation_params(sql, params)
            .await
            .map_err(Dispatch::Sql)?
            .ok_or_else(|| Dispatch::Sql(SqlError::Mutation("expected a mutation".into())))?;
        let (db_after, tempids) = if mutation.is_empty() {
            (db.clone(), BTreeMap::new())
        } else {
            let result = self
                .catalog
                .transact(
                    database,
                    mutation.expected_basis_t(),
                    mutation.forms().to_vec(),
                )
                .await
                .map_err(Dispatch::Catalog)?;
            (result.db_after, result.tempids)
        };
        let returned = mutation
            .finish(&db_after, &tempids)
            .await
            .map_err(Dispatch::Sql)?;
        let formats = expand_result_formats(result_formats, returned.columns.len())
            .map_err(|message| Dispatch::Sql(SqlError::Mutation(message.into())))?;
        if with_row_description && !returned.columns.is_empty() {
            self.writer.row_description_with_formats(
                &returned.columns.iter().map(field_of).collect::<Vec<_>>(),
                &formats,
            );
        }
        for row in returned.rows {
            let values = row
                .iter()
                .zip(&returned.columns)
                .zip(&formats)
                .map(|((value, column), format)| {
                    types::encode_result(value, &column.data_type, *format)
                        .map_err(|message| Dispatch::Sql(SqlError::Mutation(message)))
                })
                .collect::<Result<Vec<_>, _>>()?;
            self.writer.data_row(&values);
        }
        Ok((mutation.kind(), mutation.affected()))
    }

    /// Emits an `ErrorResponse` for a simple-query dispatch failure.
    fn report_dispatch(&mut self, error: &Dispatch) {
        let (code, message) = dispatch_error_fields(error);
        self.writer.error_response(&ErrorFields {
            code,
            message: &message,
        });
        if self.in_transaction {
            self.transaction_failed = true;
        }
    }

    /// Emits an `ErrorResponse` and enters the skip-until-`Sync` state.
    fn fail_dispatch(&mut self, error: &Dispatch) {
        self.report_dispatch(error);
        self.failed = true;
    }

    /// Emits an `ErrorResponse` with an explicit code and enters the
    /// skip-until-`Sync` state.
    fn fail_extended(&mut self, code: &str, message: &str) {
        self.writer.error_response(&ErrorFields { code, message });
        self.failed = true;
        if self.in_transaction {
            self.transaction_failed = true;
        }
    }

    fn report_transaction_aborted(&mut self) {
        self.writer.error_response(&ErrorFields {
            code: "25P02",
            message: "current transaction is aborted; commands ignored until end of transaction block",
        });
    }

    fn begin_transaction(&mut self) {
        self.in_transaction = true;
        self.transaction_failed = false;
        self.transaction_db = None;
    }

    fn end_transaction(&mut self) {
        self.in_transaction = false;
        self.transaction_failed = false;
        self.transaction_db = None;
    }

    fn write_row_description(&mut self, fields: &[FieldDescription], requested: &[i16]) {
        match expand_result_formats(requested, fields.len()) {
            Ok(formats) => self.writer.row_description_with_formats(fields, &formats),
            Err(message) => self.fail_extended("08P01", message),
        }
    }
}

/// The `RowDescription` field for the `SHOW DATABASES` result column.
fn database_field() -> FieldDescription {
    let type_oid = types::type_oid(&SqlType::Text);
    FieldDescription {
        name: "database".to_owned(),
        type_oid,
        type_len: types::type_len(type_oid),
    }
}

/// Builds a `RowDescription` field from a result column.
fn field_of(column: &SqlColumn) -> FieldDescription {
    let type_oid = types::type_oid(&column.data_type);
    FieldDescription {
        name: column.name.clone(),
        type_oid,
        type_len: types::type_len(type_oid),
    }
}

/// The `SQLSTATE` code and message an error is reported with.
fn dispatch_error_fields(error: &Dispatch) -> (&'static str, String) {
    match error {
        Dispatch::NoDatabase => (
            "3D000",
            "no database selected; run \"USE <database>\" first".to_owned(),
        ),
        Dispatch::Catalog(error @ CatalogError::NotFound(_)) => ("3D000", error.to_string()),
        Dispatch::Catalog(error @ CatalogError::Unavailable(_)) => ("08006", error.to_string()),
        Dispatch::Catalog(error @ CatalogError::ReadOnly(_)) => ("25006", error.to_string()),
        Dispatch::Catalog(error @ CatalogError::Conflict(_)) => ("40001", error.to_string()),
        Dispatch::Catalog(error @ CatalogError::Rejected(_)) => ("23000", error.to_string()),
        Dispatch::Catalog(error @ CatalogError::Denied(_)) => ("42501", error.to_string()),
        Dispatch::Catalog(error @ CatalogError::Unsupported(_)) => ("0A000", error.to_string()),
        Dispatch::Sql(error) => (sqlstate_for(error), error.to_string()),
        Dispatch::Denied(reason) => ("42501", reason.clone()),
    }
}

fn explicit_transaction_write_error() -> Dispatch {
    Dispatch::Sql(SqlError::Mutation(
        "writes inside explicit transaction blocks are not supported; use autocommit".to_owned(),
    ))
}

/// Chooses a `SQLSTATE` code for a SQL error.
fn sqlstate_for(error: &SqlError) -> &'static str {
    match error {
        // Missing table / projection problems.
        SqlError::Schema(_) => "42P01",
        SqlError::Mutation(_) => "0A000",
        // Parse, plan, and execution failures.
        SqlError::Parser(_) | SqlError::DataFusion(_) | SqlError::Arrow(_) => "42601",
    }
}

/// The `CommandComplete` tag for a statement that returned `rows` rows.
fn command_tag(sql: &str, rows: usize) -> String {
    if first_keyword(sql).eq_ignore_ascii_case("explain") {
        "EXPLAIN".to_owned()
    } else {
        format!("SELECT {rows}")
    }
}

fn mutation_command_tag(kind: MutationKind, rows: usize) -> String {
    match kind {
        MutationKind::Insert => format!("INSERT 0 {rows}"),
        MutationKind::Update => format!("UPDATE {rows}"),
        MutationKind::Delete => format!("DELETE {rows}"),
    }
}

fn expand_formats(formats: &[i16], count: usize) -> Result<Vec<i16>, &'static str> {
    match formats {
        [] => Ok(vec![0; count]),
        [format] => Ok(vec![*format; count]),
        formats if formats.len() == count => Ok(formats.to_vec()),
        _ => Err("parameter format count must be zero, one, or the parameter count"),
    }
}

fn expand_result_formats(formats: &[i16], count: usize) -> Result<Vec<i16>, &'static str> {
    match formats {
        [] => Ok(vec![0; count]),
        [format] => Ok(vec![*format; count]),
        formats if formats.len() == count => Ok(formats.to_vec()),
        _ => Err("result format count must be zero, one, or the result column count"),
    }
}

/// Returns the largest `PostgreSQL` positional placeholder (`$1`, `$2`, ...).
/// Strings, identifiers, comments, and dollar-quoted bodies are skipped so
/// their contents do not affect the protocol parameter count.
fn placeholder_count(sql: &str) -> usize {
    #[derive(Clone, Copy)]
    enum State {
        Normal,
        Single { backslash_escapes: bool },
        Double,
        LineComment,
        BlockComment(usize),
    }
    let bytes = sql.as_bytes();
    let mut state = State::Normal;
    let mut count = 0usize;
    let mut index = 0usize;
    while index < bytes.len() {
        match state {
            State::Normal if bytes[index..].starts_with(b"--") => {
                state = State::LineComment;
                index += 1;
            }
            State::Normal if bytes[index..].starts_with(b"/*") => {
                state = State::BlockComment(1);
                index += 1;
            }
            State::Normal if bytes[index] == b'\'' => {
                let backslash_escapes = index > 0
                    && matches!(bytes[index - 1], b'e' | b'E')
                    && (index < 2
                        || !matches!(
                            bytes[index - 2],
                            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'_'
                        ));
                state = State::Single { backslash_escapes };
            }
            State::Normal if bytes[index] == b'"' => state = State::Double,
            State::Normal if bytes[index] == b'$' => {
                let start = index + 1;
                let mut end = start;
                while bytes.get(end).is_some_and(u8::is_ascii_digit) {
                    end += 1;
                }
                if end > start
                    && let Ok(number) = sql[start..end].parse::<usize>()
                {
                    count = count.max(number);
                    index = end - 1;
                } else if let Some(delimiter_end) = dollar_quote_end(bytes, index) {
                    let delimiter = &bytes[index..delimiter_end];
                    let body_start = delimiter_end;
                    if let Some(offset) = find_bytes(&bytes[body_start..], delimiter) {
                        index = body_start + offset + delimiter.len() - 1;
                    } else {
                        index = bytes.len();
                    }
                }
            }
            State::Single {
                backslash_escapes: true,
            } if bytes[index] == b'\\' => {
                index += usize::from(index + 1 < bytes.len());
            }
            State::Single { .. } if bytes[index] == b'\'' => {
                if bytes.get(index + 1) == Some(&b'\'') {
                    index += 1;
                } else {
                    state = State::Normal;
                }
            }
            State::Double if bytes[index] == b'"' => {
                if bytes.get(index + 1) == Some(&b'"') {
                    index += 1;
                } else {
                    state = State::Normal;
                }
            }
            State::LineComment if matches!(bytes[index], b'\r' | b'\n') => {
                state = State::Normal;
            }
            State::BlockComment(depth) if bytes[index..].starts_with(b"/*") => {
                state = State::BlockComment(depth + 1);
                index += 1;
            }
            State::BlockComment(depth) if bytes[index..].starts_with(b"*/") => {
                state = if depth == 1 {
                    State::Normal
                } else {
                    State::BlockComment(depth - 1)
                };
                index += 1;
            }
            State::Normal
            | State::Single { .. }
            | State::Double
            | State::LineComment
            | State::BlockComment(_) => {}
        }
        index += 1;
    }
    count
}

fn dollar_quote_end(bytes: &[u8], start: usize) -> Option<usize> {
    let mut end = start + 1;
    if bytes.get(end) == Some(&b'$') {
        return Some(end + 1);
    }
    if !bytes
        .get(end)
        .is_some_and(|byte| byte.is_ascii_alphabetic() || *byte == b'_')
    {
        return None;
    }
    end += 1;
    while bytes
        .get(end)
        .is_some_and(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
    {
        end += 1;
    }
    (bytes.get(end) == Some(&b'$')).then_some(end + 1)
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// Classifies a statement so `USE`, `SHOW DATABASES`, and no-op control
/// statements are handled before reaching `SqlSession`.
fn classify(sql: &str) -> Statement {
    let mut words = sql.split_whitespace();
    let first = words.next().unwrap_or("").to_ascii_uppercase();
    match first.as_str() {
        "USE" => parse_use_target(sql).map_or(Statement::Query, Statement::Use),
        "SHOW"
            if words
                .next()
                .is_some_and(|word| word.eq_ignore_ascii_case("databases")) =>
        {
            Statement::ShowDatabases
        }
        "BEGIN" | "START" => Statement::Begin,
        "COMMIT" | "END" => Statement::Commit,
        "ROLLBACK" | "ABORT" => Statement::Rollback,
        "SET" => Statement::Control("SET"),
        "RESET" => Statement::Control("RESET"),
        "DISCARD" => Statement::Control("DISCARD ALL"),
        "INSERT" | "UPDATE" | "DELETE" => Statement::Mutation,
        _ => Statement::Query,
    }
}

/// Extracts the database name from a `USE <database>` statement.
fn parse_use_target(sql: &str) -> Option<String> {
    let trimmed = sql.trim();
    // `classify` matched `USE` as the first word, so the keyword is exactly
    // the first three bytes.
    let rest = trimmed.get(3..)?.trim().trim_end_matches(';').trim();
    if rest.is_empty() {
        return None;
    }
    Some(unquote(rest))
}

/// Strips one layer of SQL single- or double-quoting, else takes the first
/// whitespace-delimited token.
fn unquote(value: &str) -> String {
    if let Some(inner) = value
        .strip_prefix('"')
        .and_then(|rest| rest.strip_suffix('"'))
    {
        inner.replace("\"\"", "\"")
    } else if let Some(inner) = value
        .strip_prefix('\'')
        .and_then(|rest| rest.strip_suffix('\''))
    {
        inner.replace("''", "'")
    } else {
        value.split_whitespace().next().unwrap_or("").to_owned()
    }
}

/// The first whitespace-delimited token of a statement.
fn first_keyword(sql: &str) -> &str {
    sql.split_whitespace().next().unwrap_or("")
}

/// Splits a query string into individual statements, respecting single- and
/// double-quoted strings and SQL comments. A trailing statement without a
/// terminating semicolon is included.
fn split_statements(input: &str) -> Vec<String> {
    #[derive(Clone, Copy, Eq, PartialEq)]
    enum State {
        Normal,
        SingleQuote,
        DoubleQuote,
        LineComment,
        BlockComment,
    }
    let bytes = input.as_bytes();
    let mut state = State::Normal;
    let mut statements = Vec::new();
    let mut start = 0;
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        let next = bytes.get(index + 1).copied();
        match state {
            State::Normal => match (byte, next) {
                (b'\'', _) => state = State::SingleQuote,
                (b'"', _) => state = State::DoubleQuote,
                (b'-', Some(b'-')) => {
                    state = State::LineComment;
                    index += 1;
                }
                (b'/', Some(b'*')) => {
                    state = State::BlockComment;
                    index += 1;
                }
                (b';', _) => {
                    let statement = input[start..index].trim();
                    if has_sql_content(statement) {
                        statements.push(statement.to_owned());
                    }
                    start = index + 1;
                }
                _ => {}
            },
            State::SingleQuote => {
                if byte == b'\'' {
                    if next == Some(b'\'') {
                        index += 1;
                    } else {
                        state = State::Normal;
                    }
                }
            }
            State::DoubleQuote => {
                if byte == b'"' {
                    if next == Some(b'"') {
                        index += 1;
                    } else {
                        state = State::Normal;
                    }
                }
            }
            State::LineComment if byte == b'\n' => state = State::Normal,
            State::BlockComment if byte == b'*' && next == Some(b'/') => {
                state = State::Normal;
                index += 1;
            }
            State::LineComment | State::BlockComment => {}
        }
        index += 1;
    }
    let remainder = input[start..].trim();
    if has_sql_content(remainder) {
        statements.push(remainder.to_owned());
    }
    statements
}

/// Whether a statement fragment holds anything other than whitespace and SQL
/// comments. A fragment made only of comments is an empty query, not a
/// statement to execute.
fn has_sql_content(segment: &str) -> bool {
    let bytes = segment.as_bytes();
    let mut index = 0;
    while index < bytes.len() {
        let byte = bytes[index];
        match (byte, bytes.get(index + 1).copied()) {
            (b'-', Some(b'-')) => {
                index += 2;
                while index < bytes.len() && bytes[index] != b'\n' {
                    index += 1;
                }
            }
            (b'/', Some(b'*')) => {
                index += 2;
                while index < bytes.len()
                    && !(bytes[index] == b'*' && bytes.get(index + 1) == Some(&b'/'))
                {
                    index += 1;
                }
                index += 2;
            }
            _ if byte.is_ascii_whitespace() => index += 1,
            _ => return true,
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statement_splitter_handles_quotes_and_trailing_statement() {
        let statements = split_statements("SELECT ';'; SELECT \"a;b\"; SELECT 3");
        assert_eq!(statements, vec!["SELECT ';'", "SELECT \"a;b\"", "SELECT 3"]);
    }

    #[test]
    fn empty_query_splits_to_nothing() {
        assert!(split_statements("   ;  -- comment\n").is_empty());
    }

    #[test]
    fn control_statements_are_recognized_case_insensitively() {
        assert!(matches!(classify("begin"), Statement::Begin));
        assert!(matches!(
            classify("  SET client_encoding TO 'UTF8'"),
            Statement::Control("SET")
        ));
        assert!(matches!(classify("COMMIT"), Statement::Commit));
        assert!(matches!(classify("SELECT 1"), Statement::Query));
    }

    #[test]
    fn use_and_show_are_recognized() {
        assert!(matches!(
            classify("show databases"),
            Statement::ShowDatabases
        ));
        match classify("USE \"my-db\"") {
            Statement::Use(name) => assert_eq!(name, "my-db"),
            _ => panic!("expected USE"),
        }
        match classify("use people;") {
            Statement::Use(name) => assert_eq!(name, "people"),
            _ => panic!("expected USE"),
        }
        // A bare `USE` with no target is left to fail as an ordinary query.
        assert!(matches!(classify("USE"), Statement::Query));
    }

    #[test]
    fn command_tag_counts_selects_and_names_explains() {
        assert_eq!(command_tag("SELECT * FROM t", 7), "SELECT 7");
        assert_eq!(command_tag("EXPLAIN SELECT 1", 3), "EXPLAIN");
    }

    #[test]
    fn placeholder_count_skips_every_postgres_quoting_form() {
        let sql = r#"
            SELECT $2, '$99', E'escaped \' $98', "$97",
                   $$ body $96 $$, $tag$ body $95 $tag$
            -- $94
            /* $93 /* nested $92 */ still comment */
            WHERE value = $7
        "#;
        assert_eq!(placeholder_count(sql), 7);
    }
}
