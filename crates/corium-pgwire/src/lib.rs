//! A `PostgreSQL` wire-protocol front end for read-only Corium SQL.
//!
//! [`serve`] accepts `PostgreSQL` client connections and answers their queries
//! by running them through [`corium_sql::SqlSession`] against an immutable
//! [`corium_db::Db`] value obtained from a [`DbCatalog`]. Because every query
//! goes through `SqlSession`, the same read-only guarantee holds: DDL, DML,
//! and session-mutating statements are rejected.
//!
//! One server exposes every database the catalog offers (subject to the
//! catalog's own whitelist). A connection selects its database with the
//! standard startup `database` parameter and can switch at any time with
//! `USE <database>`; `SHOW DATABASES` lists what is available. The catalog is
//! expected to open and cache databases lazily and share them across
//! connections.
//!
//! Both the simple and extended query sub-protocols are supported, in the
//! text wire format. Bound parameters and the binary format are not
//! supported and are reported as errors. A handful of stateless control
//! statements (`BEGIN`, `COMMIT`, `ROLLBACK`, `SET`, `RESET`, `DISCARD`) are
//! accepted as no-ops so ordinary clients and drivers connect cleanly.

mod protocol;
mod types;

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;

use corium_db::Db;
use corium_sql::{SqlColumn, SqlError, SqlSession, SqlType};
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
}

/// Server-wide configuration for the `PostgreSQL` front end.
#[derive(Clone, Debug)]
pub struct PgWireConfig {
    /// If set, clients must send this cleartext password to connect. When
    /// `None`, connections are trusted.
    pub password: Option<String>,
    /// `server_version` reported to clients in a `ParameterStatus` message.
    pub server_version: String,
}

impl Default for PgWireConfig {
    fn default() -> Self {
        Self {
            password: None,
            server_version: concat!("16.0 (corium ", env!("CARGO_PKG_VERSION"), ")").to_owned(),
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
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            () = &mut shutdown => return Ok(()),
            accepted = listener.accept() => {
                let (stream, peer) = accepted?;
                let catalog = Arc::clone(&catalog);
                let config = Arc::clone(&config);
                tokio::spawn(async move {
                    let (read, write) = stream.into_split();
                    let mut session = ConnectionSession::new(
                        FrontendReader::new(read),
                        BackendWriter::new(write),
                        catalog,
                        config,
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
}

/// How a statement should be handled before it ever reaches `SqlSession`.
enum Statement {
    /// A stateless control statement accepted as a no-op with this tag.
    Control(&'static str),
    /// `USE <database>` — switch the connection's active database.
    Use(String),
    /// `SHOW DATABASES` — list the catalog.
    ShowDatabases,
    /// An ordinary read-only query for `SqlSession`.
    Query,
}

/// A failure while dispatching one statement.
enum Dispatch {
    /// No database is selected for a query.
    NoDatabase,
    /// The catalog could not provide the database.
    Catalog(CatalogError),
    /// `SqlSession` rejected or failed the query.
    Sql(SqlError),
}

/// The per-connection protocol state machine.
struct ConnectionSession<R, W, C> {
    reader: FrontendReader<R>,
    writer: BackendWriter<W>,
    catalog: Arc<C>,
    config: Arc<PgWireConfig>,
    /// The connection's active database, chosen at startup or by `USE`.
    current_db: Option<String>,
    statements: HashMap<String, String>,
    portals: HashMap<String, Portal>,
    /// Set after an extended-protocol error; frontend messages are ignored
    /// until the next `Sync`.
    failed: bool,
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
    ) -> Self {
        Self {
            reader,
            writer,
            catalog,
            config,
            current_db: None,
            statements: HashMap::new(),
            portals: HashMap::new(),
            failed: false,
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
                    parameter_count,
                } => self.handle_parse(name, query, parameter_count),
                Frontend::Bind {
                    portal,
                    statement,
                    parameter_count,
                    result_formats,
                } => self.handle_bind(&portal, &statement, parameter_count, &result_formats),
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
                    self.writer.ready_for_query(b'I');
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
    /// should be closed (bad password).
    async fn authenticate(&mut self) -> std::io::Result<bool> {
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
            self.writer.ready_for_query(b'I');
            return Ok(());
        }
        for statement in statements {
            if !self.run_simple_statement(&statement).await? {
                break;
            }
        }
        self.writer.ready_for_query(b'I');
        Ok(())
    }

    /// Runs one simple-protocol statement. Returns `false` when an error was
    /// reported and the rest of the query string should be abandoned.
    async fn run_simple_statement(&mut self, sql: &str) -> std::io::Result<bool> {
        match classify(sql) {
            Statement::Control(tag) => {
                self.writer.command_complete(tag);
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
            Statement::ShowDatabases => match self.show_databases().await {
                Ok(()) => Ok(true),
                Err(error) => {
                    self.report_dispatch(&error);
                    Ok(false)
                }
            },
            Statement::Query => {
                let db = match self.snapshot(None).await {
                    Ok(db) => db,
                    Err(error) => {
                        self.report_dispatch(&error);
                        return Ok(false);
                    }
                };
                match self.run_statement(&db, sql, true).await {
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
        }
    }

    fn handle_parse(&mut self, name: String, query: String, parameter_count: usize) {
        if self.failed {
            return;
        }
        if parameter_count > 0 {
            self.fail_extended("0A000", "bound parameters are not supported");
            return;
        }
        self.statements.insert(name, query);
        self.writer.parse_complete();
    }

    fn handle_bind(
        &mut self,
        portal: &str,
        statement: &str,
        parameter_count: usize,
        result_formats: &[i16],
    ) {
        if self.failed {
            return;
        }
        if parameter_count > 0 {
            self.fail_extended("0A000", "bound parameters are not supported");
            return;
        }
        if result_formats.contains(&1) {
            self.fail_extended("0A000", "binary result format is not supported");
            return;
        }
        let Some(sql) = self.statements.get(statement) else {
            self.fail_extended("26000", "prepared statement does not exist");
            return;
        };
        self.portals.insert(
            portal.to_owned(),
            Portal {
                sql: sql.clone(),
                database: self.current_db.clone(),
            },
        );
        self.writer.bind_complete();
    }

    async fn handle_describe(&mut self, kind: u8, name: &str) -> std::io::Result<()> {
        if self.failed {
            return Ok(());
        }
        // `Describe` of a prepared statement first reports its parameters.
        let (sql, database) = if kind == b'S' {
            self.writer.parameter_description_empty();
            let Some(sql) = self.statements.get(name) else {
                self.fail_extended("26000", "prepared statement does not exist");
                return Ok(());
            };
            (sql.clone(), self.current_db.clone())
        } else {
            let Some(portal) = self.portals.get(name) else {
                self.fail_extended("34000", "portal does not exist");
                return Ok(());
            };
            (portal.sql.clone(), portal.database.clone())
        };
        match classify(&sql) {
            Statement::ShowDatabases => self.writer.row_description(&[database_field()]),
            Statement::Query if !sql.trim().is_empty() => {
                let db = match self.snapshot(database.as_deref()).await {
                    Ok(db) => db,
                    Err(error) => {
                        self.fail_dispatch(&error);
                        return Ok(());
                    }
                };
                match self.describe_columns(&db, &sql).await {
                    Ok(fields) => self.writer.row_description(&fields),
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
        let Some((sql, database)) = self
            .portals
            .get(portal)
            .map(|portal| (portal.sql.clone(), portal.database.clone()))
        else {
            self.fail_extended("34000", "portal does not exist");
            return Ok(());
        };
        if sql.trim().is_empty() {
            self.writer.empty_query_response();
            return Ok(());
        }
        match classify(&sql) {
            Statement::Control(tag) => self.writer.command_complete(tag),
            Statement::Use(name) => match self.use_database(&name).await {
                Ok(()) => self.writer.command_complete("USE"),
                Err(error) => self.fail_dispatch(&error),
            },
            Statement::ShowDatabases => {
                if let Err(error) = self.show_databases().await {
                    self.fail_dispatch(&error);
                }
            }
            Statement::Query => {
                let db = match self.snapshot(database.as_deref()).await {
                    Ok(db) => db,
                    Err(error) => {
                        self.fail_dispatch(&error);
                        return Ok(());
                    }
                };
                match self.run_statement(&db, &sql, false).await {
                    Ok(rows) => self.writer.command_complete(&command_tag(&sql, rows)),
                    Err(error) => self.fail_dispatch(&Dispatch::Sql(error)),
                }
            }
        }
        Ok(())
    }

    /// Validates and activates `name` as the connection's database, warming
    /// the catalog cache in the process.
    async fn use_database(&mut self, name: &str) -> Result<(), Dispatch> {
        self.snapshot(Some(name)).await?;
        self.current_db = Some(name.to_owned());
        Ok(())
    }

    /// Emits a one-column `database` result listing the catalog.
    async fn show_databases(&mut self) -> Result<(), Dispatch> {
        let names = self.catalog.list().await.map_err(Dispatch::Catalog)?;
        self.writer.row_description(&[database_field()]);
        for name in &names {
            self.writer.data_row(&[Some(name.clone().into_bytes())]);
        }
        self.writer.command_complete("SHOW");
        Ok(())
    }

    /// Resolves an immutable snapshot for `database`, falling back to the
    /// connection's active database.
    async fn snapshot(&self, database: Option<&str>) -> Result<Db, Dispatch> {
        let name = database
            .map(str::to_owned)
            .or_else(|| self.current_db.clone())
            .ok_or(Dispatch::NoDatabase)?;
        self.catalog.db(&name).await.map_err(Dispatch::Catalog)
    }

    /// Plans a query and returns its result columns without streaming rows.
    async fn describe_columns(
        &self,
        db: &Db,
        sql: &str,
    ) -> Result<Vec<FieldDescription>, SqlError> {
        let session = SqlSession::new(db)?;
        let query = session.query(sql).await?;
        Ok(query.columns().iter().map(field_of).collect())
    }

    /// Runs one statement, optionally emitting a `RowDescription` first, then
    /// streaming its rows as `DataRow` messages. Returns the row count.
    async fn run_statement(
        &mut self,
        db: &Db,
        sql: &str,
        with_row_description: bool,
    ) -> Result<usize, SqlError> {
        let session = SqlSession::new(db)?;
        let mut query = session.query(sql).await?;
        if with_row_description {
            let fields = query.columns().iter().map(field_of).collect::<Vec<_>>();
            self.writer.row_description(&fields);
        }
        let mut count = 0usize;
        while let Some(row) = query.next_row().await? {
            let values = row.iter().map(types::encode_value).collect::<Vec<_>>();
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

    /// Emits an `ErrorResponse` for a simple-query dispatch failure.
    fn report_dispatch(&mut self, error: &Dispatch) {
        let (code, message) = dispatch_error_fields(error);
        self.writer.error_response(&ErrorFields {
            code,
            message: &message,
        });
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
        Dispatch::Sql(error) => (sqlstate_for(error), error.to_string()),
    }
}

/// Chooses a `SQLSTATE` code for a SQL error.
fn sqlstate_for(error: &SqlError) -> &'static str {
    match error {
        // Missing table / projection problems.
        SqlError::Schema(_) => "42P01",
        // Parse, plan, and execution failures.
        SqlError::DataFusion(_) | SqlError::Arrow(_) => "42601",
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
        "BEGIN" | "START" => Statement::Control("BEGIN"),
        "COMMIT" | "END" => Statement::Control("COMMIT"),
        "ROLLBACK" | "ABORT" => Statement::Control("ROLLBACK"),
        "SET" => Statement::Control("SET"),
        "RESET" => Statement::Control("RESET"),
        "DISCARD" => Statement::Control("DISCARD ALL"),
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
        assert!(matches!(classify("begin"), Statement::Control("BEGIN")));
        assert!(matches!(
            classify("  SET client_encoding TO 'UTF8'"),
            Statement::Control("SET")
        ));
        assert!(matches!(classify("COMMIT"), Statement::Control("COMMIT")));
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
}
