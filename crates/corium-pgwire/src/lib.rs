//! A `PostgreSQL` wire-protocol front end for read-only Corium SQL.
//!
//! [`serve`] accepts `PostgreSQL` client connections and answers their queries
//! by running them through [`corium_sql::SqlSession`] against an immutable
//! [`corium_db::Db`] value supplied by a [`DbSource`]. Because every query
//! goes through `SqlSession`, the same read-only guarantee holds: DDL, DML,
//! and session-mutating statements are rejected.
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
use corium_sql::{SqlColumn, SqlError, SqlSession};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;

use protocol::{BackendWriter, ErrorFields, FieldDescription, Frontend, FrontendReader, Startup};

/// Supplies the immutable database value a query should run against.
///
/// The server calls [`DbSource::db`] to capture a fresh view per query, the
/// same way the `corium sql` shell captures a current `Db` for each
/// statement. A closure returning a `Db` implements this trait directly.
pub trait DbSource: Send + Sync + 'static {
    /// Returns the current local database value to query.
    fn db(&self) -> Db;
}

impl<F: Fn() -> Db + Send + Sync + 'static> DbSource for F {
    fn db(&self) -> Db {
        self()
    }
}

/// Server-wide configuration for the `PostgreSQL` front end.
#[derive(Clone, Debug)]
pub struct PgWireConfig {
    /// Database name advertised to clients (informational only; the server
    /// serves whatever [`DbSource`] returns).
    pub database: String,
    /// If set, clients must send this cleartext password to connect. When
    /// `None`, connections are trusted.
    pub password: Option<String>,
    /// `server_version` reported to clients in a `ParameterStatus` message.
    pub server_version: String,
}

impl Default for PgWireConfig {
    fn default() -> Self {
        Self {
            database: "corium".to_owned(),
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
pub async fn serve<S, F>(
    listener: TcpListener,
    source: Arc<S>,
    config: PgWireConfig,
    shutdown: F,
) -> std::io::Result<()>
where
    S: DbSource,
    F: Future<Output = ()>,
{
    let config = Arc::new(config);
    tokio::pin!(shutdown);
    loop {
        tokio::select! {
            () = &mut shutdown => return Ok(()),
            accepted = listener.accept() => {
                let (stream, peer) = accepted?;
                let source = Arc::clone(&source);
                let config = Arc::clone(&config);
                tokio::spawn(async move {
                    let (read, write) = stream.into_split();
                    let mut session = ConnectionSession::new(
                        FrontendReader::new(read),
                        BackendWriter::new(write),
                        source,
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

/// A bound portal: the SQL to run and the database snapshot to run it against.
struct Portal {
    sql: String,
    db: Db,
}

/// The per-connection protocol state machine.
struct ConnectionSession<R, W, S> {
    reader: FrontendReader<R>,
    writer: BackendWriter<W>,
    source: Arc<S>,
    config: Arc<PgWireConfig>,
    statements: HashMap<String, String>,
    portals: HashMap<String, Portal>,
    /// Set after an extended-protocol error; frontend messages are ignored
    /// until the next `Sync`.
    failed: bool,
}

impl<R, W, S> ConnectionSession<R, W, S>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
    S: DbSource,
{
    fn new(
        reader: FrontendReader<R>,
        writer: BackendWriter<W>,
        source: Arc<S>,
        config: Arc<PgWireConfig>,
    ) -> Self {
        Self {
            reader,
            writer,
            source,
            config,
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
        self.send_ready_banner(&startup).await?;

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
    async fn send_ready_banner(&mut self, startup: &Startup) -> std::io::Result<()> {
        self.writer
            .parameter_status("server_version", &self.config.server_version);
        self.writer.parameter_status("server_encoding", "UTF8");
        self.writer.parameter_status("client_encoding", "UTF8");
        self.writer.parameter_status("DateStyle", "ISO, MDY");
        self.writer.parameter_status("TimeZone", "UTC");
        self.writer.parameter_status("integer_datetimes", "on");
        self.writer
            .parameter_status("standard_conforming_strings", "on");
        self.writer.parameter_status(
            "application_name",
            startup.get("application_name").unwrap_or(""),
        );
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
            if let Some(tag) = control_tag(&statement) {
                self.writer.command_complete(tag);
                continue;
            }
            let db = self.source.db();
            match self.run_statement(&db, &statement, true).await {
                Ok(rows) => self.writer.command_complete(&command_tag(&statement, rows)),
                Err(error) => {
                    self.report_sql_error(&error);
                    break;
                }
            }
        }
        self.writer.ready_for_query(b'I');
        Ok(())
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
                db: self.source.db(),
            },
        );
        self.writer.bind_complete();
    }

    async fn handle_describe(&mut self, kind: u8, name: &str) -> std::io::Result<()> {
        if self.failed {
            return Ok(());
        }
        // `Describe` of a prepared statement first reports its parameters.
        let (sql, db) = if kind == b'S' {
            self.writer.parameter_description_empty();
            let Some(sql) = self.statements.get(name) else {
                self.fail_extended("26000", "prepared statement does not exist");
                return Ok(());
            };
            (sql.clone(), self.source.db())
        } else {
            let Some(portal) = self.portals.get(name) else {
                self.fail_extended("34000", "portal does not exist");
                return Ok(());
            };
            (portal.sql.clone(), portal.db.clone())
        };
        if control_tag(&sql).is_some() || sql.trim().is_empty() {
            self.writer.no_data();
            return Ok(());
        }
        match self.describe_columns(&db, &sql).await {
            Ok(fields) => self.writer.row_description(&fields),
            Err(error) => self.fail_sql(&error),
        }
        Ok(())
    }

    async fn handle_execute(&mut self, portal: &str) -> std::io::Result<()> {
        if self.failed {
            return Ok(());
        }
        let Some(Portal { sql, db }) = self.portals.get(portal).map(|portal| Portal {
            sql: portal.sql.clone(),
            db: portal.db.clone(),
        }) else {
            self.fail_extended("34000", "portal does not exist");
            return Ok(());
        };
        if let Some(tag) = control_tag(&sql) {
            self.writer.command_complete(tag);
            return Ok(());
        }
        if sql.trim().is_empty() {
            self.writer.empty_query_response();
            return Ok(());
        }
        match self.run_statement(&db, &sql, false).await {
            Ok(rows) => self.writer.command_complete(&command_tag(&sql, rows)),
            Err(error) => self.fail_sql(&error),
        }
        Ok(())
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

    /// Emits an `ErrorResponse` for a simple-query failure.
    fn report_sql_error(&mut self, error: &SqlError) {
        self.writer.error_response(&ErrorFields {
            code: sqlstate_for(error),
            message: &error.to_string(),
        });
    }

    /// Emits an `ErrorResponse` for an extended-protocol SQL failure and
    /// enters the skip-until-`Sync` state.
    fn fail_sql(&mut self, error: &SqlError) {
        self.report_sql_error(error);
        self.failed = true;
    }

    /// Emits an `ErrorResponse` with an explicit code and enters the
    /// skip-until-`Sync` state.
    fn fail_extended(&mut self, code: &str, message: &str) {
        self.writer.error_response(&ErrorFields { code, message });
        self.failed = true;
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

/// If `sql` is a stateless control statement this server accepts as a no-op,
/// returns the `CommandComplete` tag to report for it.
fn control_tag(sql: &str) -> Option<&'static str> {
    match first_keyword(sql).to_ascii_uppercase().as_str() {
        "BEGIN" | "START" => Some("BEGIN"),
        "COMMIT" | "END" => Some("COMMIT"),
        "ROLLBACK" | "ABORT" => Some("ROLLBACK"),
        "SET" => Some("SET"),
        "RESET" => Some("RESET"),
        "DISCARD" => Some("DISCARD ALL"),
        _ => None,
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
        assert_eq!(control_tag("begin"), Some("BEGIN"));
        assert_eq!(control_tag("  SET client_encoding TO 'UTF8'"), Some("SET"));
        assert_eq!(control_tag("COMMIT"), Some("COMMIT"));
        assert_eq!(control_tag("SELECT 1"), None);
    }

    #[test]
    fn command_tag_counts_selects_and_names_explains() {
        assert_eq!(command_tag("SELECT * FROM t", 7), "SELECT 7");
        assert_eq!(command_tag("EXPLAIN SELECT 1", 3), "EXPLAIN");
    }
}
