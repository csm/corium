//! End-to-end tests that drive the server with hand-built wire messages.
//!
//! These avoid a real `PostgreSQL` client dependency by speaking just enough of
//! the v3 protocol to run a simple query and read the reply.

use std::sync::Arc;

use corium_core::{
    Attribute, Cardinality, Datom, EntityId, Keyword, KeywordInterner, Partition, Schema, Value,
    ValueType,
};
use corium_db::{Db, Idents};
use corium_forms::txforms::tx_items_from_edn;
use corium_pgwire::{CatalogError, CatalogTxResult, DbCatalog, PgWireConfig, serve};
use corium_protocol::authz::{
    ActionClass, AttributeAllowlist, Grant, Guard, PolicyAuthorizer, Principal, StaticTokens,
    ViewFilter,
};
use corium_query::edn::Edn;
use corium_tx::prepare;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpStream;

/// A catalog serving one fixture database under several names.
struct TestCatalog {
    db: tokio::sync::Mutex<Db>,
    names: Vec<String>,
}

#[async_trait::async_trait]
impl DbCatalog for TestCatalog {
    async fn list(&self) -> Result<Vec<String>, CatalogError> {
        Ok(self.names.clone())
    }

    async fn db(&self, name: &str) -> Result<Db, CatalogError> {
        if self.names.iter().any(|known| known == name) {
            Ok(self.db.lock().await.clone())
        } else {
            Err(CatalogError::NotFound(name.to_owned()))
        }
    }

    async fn transact(
        &self,
        name: &str,
        expected_basis_t: u64,
        forms: Vec<Edn>,
    ) -> Result<CatalogTxResult, CatalogError> {
        if !self.names.iter().any(|known| known == name) {
            return Err(CatalogError::NotFound(name.to_owned()));
        }
        let mut db = self.db.lock().await;
        if db.basis_t() != expected_basis_t {
            return Err(CatalogError::Conflict(format!(
                "expected {expected_basis_t}, current basis is {}",
                db.basis_t()
            )));
        }
        let mut interner = db.interner().clone();
        let items = tx_items_from_edn(&db, &mut interner, &forms)
            .map_err(|error| CatalogError::Rejected(error.to_string()))?;
        let t = db.basis_t() + 1;
        let prepared = prepare(
            &db,
            items,
            EntityId::new(Partition::Tx as u32, t),
            db.next_user_sequence(),
        )
        .map_err(|error| CatalogError::Rejected(error.to_string()))?;
        *db = db.clone().with_transaction(t, &prepared.datoms);
        Ok(CatalogTxResult {
            db_after: db.clone(),
            tempids: prepared.tempids,
        })
    }
}

/// Builds a small two-artist database mirroring the `corium-sql` fixture.
fn fixture() -> Db {
    // At or above `FIRST_ATTR_ID`: the low db-partition range belongs to the
    // engine's own attributes, and `Db::new` installs them over any schema.
    let name = EntityId::from_raw(100);
    let tags = EntityId::from_raw(101);
    let release_year = EntityId::from_raw(102);
    let mut schema = Schema::default();
    schema.insert(Attribute {
        id: name,
        value_type: ValueType::Str,
        cardinality: Cardinality::One,
        unique: None,
        is_component: false,
        indexed: false,
        no_history: false,
    });
    schema.insert(Attribute {
        id: tags,
        value_type: ValueType::Str,
        cardinality: Cardinality::Many,
        unique: None,
        is_component: false,
        indexed: true,
        no_history: false,
    });
    schema.insert(Attribute {
        id: release_year,
        value_type: ValueType::Long,
        cardinality: Cardinality::One,
        unique: None,
        is_component: false,
        indexed: true,
        no_history: false,
    });
    let mut idents = Idents::default();
    idents.insert(Keyword::parse("artist/name"), name);
    idents.insert(Keyword::parse("artist/tags"), tags);
    idents.insert(Keyword::parse("artist/release-year"), release_year);
    let boc = EntityId::from_raw(1_000);
    let tycho = EntityId::from_raw(1_001);
    let tx = EntityId::from_raw(1);
    Db::new(schema)
        .with_naming(idents, KeywordInterner::default())
        .with_transaction(
            1,
            &[
                Datom {
                    e: boc,
                    a: name,
                    v: Value::Str("Boards of Canada".into()),
                    tx,
                    added: true,
                },
                Datom {
                    e: boc,
                    a: release_year,
                    v: Value::Long(1998),
                    tx,
                    added: true,
                },
                Datom {
                    e: tycho,
                    a: name,
                    v: Value::Str("Tycho".into()),
                    tx,
                    added: true,
                },
                Datom {
                    e: tycho,
                    a: release_year,
                    v: Value::Long(2011),
                    tx,
                    added: true,
                },
            ],
        )
}

/// Starts the server on an ephemeral port, returning its address.
async fn start_server(config: PgWireConfig) -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let catalog = Arc::new(TestCatalog {
        db: tokio::sync::Mutex::new(fixture()),
        names: vec!["corium".to_owned(), "people".to_owned()],
    });
    tokio::spawn(async move {
        let _ = serve(listener, catalog, config, std::future::pending::<()>()).await;
    });
    address
}

struct ConflictCatalog {
    db: Db,
}

#[async_trait::async_trait]
impl DbCatalog for ConflictCatalog {
    async fn list(&self) -> Result<Vec<String>, CatalogError> {
        Ok(vec!["corium".into()])
    }

    async fn db(&self, name: &str) -> Result<Db, CatalogError> {
        if name == "corium" {
            Ok(self.db.clone())
        } else {
            Err(CatalogError::NotFound(name.into()))
        }
    }

    async fn transact(
        &self,
        _name: &str,
        expected_basis_t: u64,
        _forms: Vec<Edn>,
    ) -> Result<CatalogTxResult, CatalogError> {
        Err(CatalogError::Conflict(format!(
            "expected basis {expected_basis_t}; current basis advanced"
        )))
    }
}

async fn start_conflict_server() -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let catalog = Arc::new(ConflictCatalog { db: fixture() });
    tokio::spawn(async move {
        let _ = serve(
            listener,
            catalog,
            PgWireConfig::default(),
            std::future::pending::<()>(),
        )
        .await;
    });
    address
}

/// A decoded backend message: tag byte and raw body.
struct Message {
    tag: u8,
    body: Vec<u8>,
}

/// Minimal client that writes frontend messages and reads backend ones.
struct Client {
    stream: TcpStream,
}

impl Client {
    async fn connect(address: std::net::SocketAddr) -> Self {
        Self {
            stream: TcpStream::connect(address).await.unwrap(),
        }
    }

    /// Sends a startup message with the given parameters.
    async fn startup(&mut self, parameters: &[(&str, &str)]) {
        let mut body = Vec::new();
        body.extend_from_slice(&196_608i32.to_be_bytes());
        for (key, value) in parameters {
            body.extend_from_slice(key.as_bytes());
            body.push(0);
            body.extend_from_slice(value.as_bytes());
            body.push(0);
        }
        body.push(0);
        let length = i32::try_from(body.len() + 4).unwrap();
        self.stream.write_all(&length.to_be_bytes()).await.unwrap();
        self.stream.write_all(&body).await.unwrap();
        self.stream.flush().await.unwrap();
    }

    /// Sends a tagged frontend message.
    async fn send(&mut self, tag: u8, body: &[u8]) {
        let length = i32::try_from(body.len() + 4).unwrap();
        self.stream.write_all(&[tag]).await.unwrap();
        self.stream.write_all(&length.to_be_bytes()).await.unwrap();
        self.stream.write_all(body).await.unwrap();
        self.stream.flush().await.unwrap();
    }

    /// Sends a simple query message.
    async fn query(&mut self, sql: &str) {
        let mut body = Vec::new();
        body.extend_from_slice(sql.as_bytes());
        body.push(0);
        self.send(b'Q', &body).await;
    }

    /// Sends a cleartext password message.
    async fn password(&mut self, password: &str) {
        let mut body = Vec::new();
        body.extend_from_slice(password.as_bytes());
        body.push(0);
        self.send(b'p', &body).await;
    }

    /// Reads exactly one backend message.
    async fn read_message(&mut self) -> Message {
        let mut header = [0u8; 5];
        self.stream.read_exact(&mut header).await.unwrap();
        let length = i32::from_be_bytes([header[1], header[2], header[3], header[4]]);
        let body_len = usize::try_from(length).unwrap() - 4;
        let mut body = vec![0u8; body_len];
        self.stream.read_exact(&mut body).await.unwrap();
        Message {
            tag: header[0],
            body,
        }
    }

    /// Reads messages until a `ReadyForQuery`, returning all of them.
    async fn read_until_ready(&mut self) -> Vec<Message> {
        let mut messages = Vec::new();
        loop {
            let message = self.read_message().await;
            let ready = message.tag == b'Z';
            messages.push(message);
            if ready {
                return messages;
            }
        }
    }
}

/// Splits a NUL-delimited backend message body into its string fields.
fn cstrings(body: &[u8]) -> Vec<String> {
    body.split(|byte| *byte == 0)
        .filter(|slice| !slice.is_empty())
        .map(|slice| String::from_utf8_lossy(slice).into_owned())
        .collect()
}

/// Extracts the column names of a `RowDescription` message body.
fn row_description_names(body: &[u8]) -> Vec<String> {
    let count = i16::from_be_bytes([body[0], body[1]]);
    let mut offset = 2;
    let mut names = Vec::new();
    for _ in 0..count {
        let end = body[offset..].iter().position(|byte| *byte == 0).unwrap() + offset;
        names.push(String::from_utf8_lossy(&body[offset..end]).into_owned());
        // Skip the NUL plus the 18 fixed bytes (table oid, column number,
        // type oid, type length, type modifier, format code).
        offset = end + 1 + 18;
    }
    names
}

/// Extracts result format codes from a `RowDescription` message body.
fn row_description_formats(body: &[u8]) -> Vec<i16> {
    let count = i16::from_be_bytes([body[0], body[1]]);
    let mut offset = 2;
    let mut formats = Vec::new();
    for _ in 0..count {
        let end = body[offset..].iter().position(|byte| *byte == 0).unwrap() + offset;
        let format_at = end + 1 + 16;
        formats.push(i16::from_be_bytes([body[format_at], body[format_at + 1]]));
        offset = end + 1 + 18;
    }
    formats
}

/// Extracts raw values from a `DataRow` message body.
fn data_row_bytes(body: &[u8]) -> Vec<Option<Vec<u8>>> {
    let count = i16::from_be_bytes([body[0], body[1]]);
    let mut offset = 2;
    let mut values = Vec::new();
    for _ in 0..count {
        let length = i32::from_be_bytes([
            body[offset],
            body[offset + 1],
            body[offset + 2],
            body[offset + 3],
        ]);
        offset += 4;
        if length < 0 {
            values.push(None);
        } else {
            let end = offset + usize::try_from(length).unwrap();
            values.push(Some(body[offset..end].to_vec()));
            offset = end;
        }
    }
    values
}

/// Extracts the text values of a `DataRow` message body.
fn data_row_values(body: &[u8]) -> Vec<Option<String>> {
    let count = i16::from_be_bytes([body[0], body[1]]);
    let mut offset = 2;
    let mut values = Vec::new();
    for _ in 0..count {
        let length = i32::from_be_bytes([
            body[offset],
            body[offset + 1],
            body[offset + 2],
            body[offset + 3],
        ]);
        offset += 4;
        if length < 0 {
            values.push(None);
        } else {
            let end = offset + usize::try_from(length).unwrap();
            values.push(Some(
                String::from_utf8_lossy(&body[offset..end]).into_owned(),
            ));
            offset = end;
        }
    }
    values
}

#[tokio::test]
async fn simple_query_returns_rows_and_ready() {
    let address = start_server(PgWireConfig::default()).await;
    let mut client = Client::connect(address).await;
    client
        .startup(&[("user", "postgres"), ("database", "corium")])
        .await;

    // Read AuthenticationOk ... ReadyForQuery banner.
    let banner = client.read_until_ready().await;
    assert_eq!(banner.first().unwrap().tag, b'R');
    assert_eq!(banner.last().unwrap().tag, b'Z');

    client
        .query("SELECT name FROM corium.artist ORDER BY name")
        .await;
    let response = client.read_until_ready().await;

    let tags: Vec<u8> = response.iter().map(|message| message.tag).collect();
    // RowDescription, two DataRows, CommandComplete, ReadyForQuery.
    assert_eq!(tags, vec![b'T', b'D', b'D', b'C', b'Z']);

    let description = &response[0];
    assert_eq!(
        row_description_names(&description.body),
        vec!["name".to_owned()]
    );

    let first = data_row_values(&response[1].body);
    let second = data_row_values(&response[2].body);
    assert_eq!(first, vec![Some("Boards of Canada".to_owned())]);
    assert_eq!(second, vec![Some("Tycho".to_owned())]);

    let complete = &response[3];
    assert_eq!(cstrings(&complete.body), vec!["SELECT 2".to_owned()]);
}

#[tokio::test]
async fn cardinality_many_renders_as_array_literal() {
    let address = start_server(PgWireConfig::default()).await;
    let mut client = Client::connect(address).await;
    client
        .startup(&[("user", "postgres"), ("database", "corium")])
        .await;
    client.read_until_ready().await;

    client
        .query("SELECT tags FROM corium.artist WHERE name = 'Boards of Canada'")
        .await;
    let response = client.read_until_ready().await;
    let row = response
        .iter()
        .find(|message| message.tag == b'D')
        .expect("a data row");
    // The fixture has no tags for this artist: an empty list -> '{}'.
    assert_eq!(data_row_values(&row.body), vec![Some("{}".to_owned())]);
}

#[tokio::test]
async fn pgjdbc_sql_keywords_metadata_probe_is_supported() {
    let address = start_server(PgWireConfig::default()).await;
    let mut client = Client::connect(address).await;
    client
        .startup(&[("user", "postgres"), ("database", "corium")])
        .await;
    client.read_until_ready().await;

    client
        .query(
            "select string_agg(word, ',') from pg_catalog.pg_get_keywords() \
             where word <> ALL ('{select,from,where}'::text[])",
        )
        .await;
    let response = client.read_until_ready().await;
    assert_eq!(
        response
            .iter()
            .map(|message| message.tag)
            .collect::<Vec<_>>(),
        vec![b'T', b'D', b'C', b'Z']
    );
    let keywords = data_row_values(&response[1].body)[0]
        .clone()
        .expect("keyword list");
    assert!(keywords.contains("returning"));
    assert!(keywords.contains("vacuum"));

    client.query("SELECT current_schema()").await;
    let response = client.read_until_ready().await;
    assert_eq!(
        data_row_values(
            &response
                .iter()
                .find(|message| message.tag == b'D')
                .expect("current schema row")
                .body
        ),
        vec![Some("corium".to_owned())]
    );

    client.query("select current_catalog").await;
    let response = client.read_until_ready().await;
    assert_eq!(
        data_row_values(
            &response
                .iter()
                .find(|message| message.tag == b'D')
                .expect("current catalog row")
                .body
        ),
        vec![Some("corium".to_owned())]
    );

    client
        .query(
            "SELECT setting FROM pg_catalog.pg_settings \
             WHERE name='default_transaction_isolation'",
        )
        .await;
    let response = client.read_until_ready().await;
    assert_eq!(
        data_row_values(
            &response
                .iter()
                .find(|message| message.tag == b'D')
                .expect("pg setting row")
                .body
        ),
        vec![Some("read committed".to_owned())]
    );

    client.query("SHOW TRANSACTION ISOLATION LEVEL").await;
    let response = client.read_until_ready().await;
    assert_eq!(
        data_row_values(
            &response
                .iter()
                .find(|message| message.tag == b'D')
                .expect("transaction isolation row")
                .body
        ),
        vec![Some("read committed".to_owned())]
    );
}

#[tokio::test]
async fn unsupported_ddl_is_rejected_but_the_session_survives() {
    let address = start_server(PgWireConfig::default()).await;
    let mut client = Client::connect(address).await;
    client
        .startup(&[("user", "postgres"), ("database", "corium")])
        .await;
    client.read_until_ready().await;

    client.query("CREATE TABLE nope (e BIGINT)").await;
    let response = client.read_until_ready().await;
    assert_eq!(response.first().unwrap().tag, b'E');
    assert_eq!(response.last().unwrap().tag, b'Z');

    // The connection is still usable after the error.
    client.query("SELECT 1").await;
    let response = client.read_until_ready().await;
    assert!(response.iter().any(|message| message.tag == b'D'));
}

#[tokio::test]
async fn autocommit_insert_update_delete_and_returning_use_the_write_catalog() {
    let address = start_server(PgWireConfig::default()).await;
    let mut client = Client::connect(address).await;
    client
        .startup(&[("user", "postgres"), ("database", "corium")])
        .await;
    client.read_until_ready().await;

    client
        .query(
            "INSERT INTO corium.artist (name, \"release-year\") \
             VALUES ('Autechre', 1994) RETURNING e, name",
        )
        .await;
    let inserted = client.read_until_ready().await;
    assert_eq!(
        cstrings(
            &inserted
                .iter()
                .find(|message| message.tag == b'C')
                .expect("insert tag")
                .body
        ),
        vec!["INSERT 0 1".to_owned()]
    );
    let inserted_row = inserted
        .iter()
        .find(|message| message.tag == b'D')
        .expect("insert returning row");
    assert_eq!(
        data_row_values(&inserted_row.body)[1],
        Some("Autechre".to_owned())
    );

    client
        .query(
            "UPDATE corium.artist SET name = 'Autechre!' \
             WHERE name = 'Autechre' RETURNING name",
        )
        .await;
    let updated = client.read_until_ready().await;
    assert_eq!(
        data_row_values(
            &updated
                .iter()
                .find(|message| message.tag == b'D')
                .expect("update returning")
                .body
        ),
        vec![Some("Autechre!".to_owned())]
    );

    client
        .query("DELETE FROM corium.artist WHERE name = 'Autechre!' RETURNING name")
        .await;
    let deleted = client.read_until_ready().await;
    assert_eq!(
        data_row_values(
            &deleted
                .iter()
                .find(|message| message.tag == b'D')
                .expect("delete returning")
                .body
        ),
        vec![Some("Autechre!".to_owned())]
    );
    assert_eq!(
        cstrings(
            &deleted
                .iter()
                .find(|message| message.tag == b'C')
                .expect("delete tag")
                .body
        ),
        vec!["DELETE 1".to_owned()]
    );
}

#[tokio::test]
async fn serialization_conflict_is_reported_as_sqlstate_40001() {
    let address = start_conflict_server().await;
    let mut client = Client::connect(address).await;
    client
        .startup(&[("user", "postgres"), ("database", "corium")])
        .await;
    client.read_until_ready().await;

    client
        .query("INSERT INTO corium.artist (name) VALUES ('Racing Writer')")
        .await;
    let response = client.read_until_ready().await;
    let error = response
        .iter()
        .find(|message| message.tag == b'E')
        .expect("serialization error");
    assert!(
        cstrings(&error.body)
            .iter()
            .any(|field| field.contains("40001"))
    );
    assert_eq!(response.last().unwrap().body, vec![b'I']);
}

#[tokio::test]
async fn zero_row_update_returning_still_describes_its_result() {
    let address = start_server(PgWireConfig::default()).await;
    let mut client = Client::connect(address).await;
    client
        .startup(&[("user", "postgres"), ("database", "corium")])
        .await;
    client.read_until_ready().await;

    client
        .query(
            "UPDATE corium.artist SET name = 'Nobody' \
             WHERE name = 'Missing' RETURNING name",
        )
        .await;
    let response = client.read_until_ready().await;
    let tags = response
        .iter()
        .map(|message| message.tag)
        .collect::<Vec<_>>();
    assert_eq!(tags, vec![b'T', b'C', b'Z']);
    assert_eq!(
        row_description_names(
            &response
                .iter()
                .find(|message| message.tag == b'T')
                .expect("row description")
                .body
        ),
        vec!["name".to_owned()]
    );
}

#[tokio::test]
async fn explicit_transaction_control_updates_wire_status() {
    let address = start_server(PgWireConfig::default()).await;
    let mut client = Client::connect(address).await;
    client.startup(&[("user", "postgres")]).await;
    client.read_until_ready().await;

    client.query("BEGIN").await;
    let response = client.read_until_ready().await;
    let complete = response
        .iter()
        .find(|message| message.tag == b'C')
        .expect("command complete");
    assert_eq!(cstrings(&complete.body), vec!["BEGIN".to_owned()]);
    assert_eq!(response.last().unwrap().body, vec![b'T']);
}

#[tokio::test]
async fn explicit_transaction_reads_keep_their_first_snapshot() {
    let address = start_server(PgWireConfig::default()).await;
    let mut first = Client::connect(address).await;
    first
        .startup(&[("user", "postgres"), ("database", "corium")])
        .await;
    first.read_until_ready().await;
    let mut second = Client::connect(address).await;
    second
        .startup(&[("user", "postgres"), ("database", "corium")])
        .await;
    second.read_until_ready().await;

    first
        .query(
            "BEGIN; SELECT \"release-year\" FROM corium.artist \
             WHERE name = 'Boards of Canada'",
        )
        .await;
    let initial = first.read_until_ready().await;
    assert!(
        initial
            .iter()
            .filter(|message| message.tag == b'D')
            .any(|row| data_row_values(&row.body) == vec![Some("1998".into())])
    );

    second
        .query(
            "UPDATE corium.artist SET \"release-year\" = 2000 \
             WHERE name = 'Boards of Canada'",
        )
        .await;
    second.read_until_ready().await;

    first
        .query(
            "SELECT \"release-year\" FROM corium.artist \
             WHERE name = 'Boards of Canada'",
        )
        .await;
    let pinned = first.read_until_ready().await;
    assert!(
        pinned
            .iter()
            .filter(|message| message.tag == b'D')
            .any(|row| data_row_values(&row.body) == vec![Some("1998".into())])
    );

    first.query("ROLLBACK").await;
    first.read_until_ready().await;
    first
        .query(
            "SELECT \"release-year\" FROM corium.artist \
             WHERE name = 'Boards of Canada'",
        )
        .await;
    let current = first.read_until_ready().await;
    assert!(
        current
            .iter()
            .filter(|message| message.tag == b'D')
            .any(|row| data_row_values(&row.body) == vec![Some("2000".into())])
    );
}

#[tokio::test]
async fn explicit_transaction_writes_are_visible_and_commit_atomically() {
    let address = start_server(PgWireConfig::default()).await;
    let mut client = Client::connect(address).await;
    client
        .startup(&[("user", "postgres"), ("database", "corium")])
        .await;
    client.read_until_ready().await;

    client
        .query(
            "BEGIN; \
             INSERT INTO corium.artist (name) VALUES ('First Staged') RETURNING e; \
             BEGIN; \
             INSERT INTO corium.artist (name) VALUES ('Second Staged') RETURNING e; \
             SELECT name FROM corium.artist WHERE name LIKE '% Staged' ORDER BY name; \
             COMMIT",
        )
        .await;
    let response = client.read_until_ready().await;
    assert!(!response.iter().any(|message| message.tag == b'E'));
    assert_eq!(response.last().unwrap().body, vec![b'I']);
    let returned_ids = response
        .iter()
        .filter(|message| message.tag == b'D')
        .filter_map(|message| {
            let values = data_row_values(&message.body);
            values
                .first()
                .and_then(Option::as_deref)
                .and_then(|value| value.parse::<u64>().ok())
        })
        .collect::<Vec<_>>();
    assert_eq!(returned_ids.len(), 2);
    assert_ne!(returned_ids[0], returned_ids[1]);
    let staged_names = response
        .iter()
        .filter(|message| message.tag == b'D')
        .filter_map(|message| data_row_values(&message.body).into_iter().next().flatten())
        .filter(|value| value.ends_with(" Staged"))
        .collect::<Vec<_>>();
    assert_eq!(staged_names, vec!["First Staged", "Second Staged"]);

    client
        .query("SELECT e, name FROM corium.artist WHERE name LIKE '% Staged' ORDER BY name")
        .await;
    let committed = client.read_until_ready().await;
    let committed_rows = committed
        .iter()
        .filter(|message| message.tag == b'D')
        .map(|message| data_row_values(&message.body))
        .collect::<Vec<_>>();
    assert_eq!(committed_rows.len(), 2);
    let first_id = returned_ids[0].to_string();
    let second_id = returned_ids[1].to_string();
    assert_eq!(committed_rows[0][0].as_deref(), Some(first_id.as_str()));
    assert_eq!(committed_rows[1][0].as_deref(), Some(second_id.as_str()));
}

#[tokio::test]
async fn rollback_discards_staged_writes() {
    let address = start_server(PgWireConfig::default()).await;
    let mut client = Client::connect(address).await;
    client
        .startup(&[("user", "postgres"), ("database", "corium")])
        .await;
    client.read_until_ready().await;

    client
        .query("BEGIN; INSERT INTO corium.artist (name) VALUES ('Must Not Commit'); ROLLBACK")
        .await;
    let response = client.read_until_ready().await;
    assert_eq!(response.last().unwrap().body, vec![b'I']);

    client
        .query("SELECT name FROM corium.artist WHERE name = 'Must Not Commit'")
        .await;
    let response = client.read_until_ready().await;
    assert!(!response.iter().any(|message| message.tag == b'D'));
}

#[tokio::test]
async fn cleartext_password_is_required_when_configured() {
    let config = PgWireConfig {
        password: Some("hunter2".to_owned()),
        ..PgWireConfig::default()
    };
    let address = start_server(config).await;
    let mut client = Client::connect(address).await;
    client.startup(&[("user", "postgres")]).await;

    // Server asks for a cleartext password (AuthenticationCleartextPassword).
    let request = client.read_message().await;
    assert_eq!(request.tag, b'R');
    assert_eq!(
        i32::from_be_bytes([
            request.body[0],
            request.body[1],
            request.body[2],
            request.body[3]
        ]),
        3
    );

    client.password("hunter2").await;
    let banner = client.read_until_ready().await;
    assert_eq!(banner.first().unwrap().tag, b'R');
    assert_eq!(banner.last().unwrap().tag, b'Z');
}

#[tokio::test]
async fn extended_protocol_runs_a_parameterless_query() {
    let address = start_server(PgWireConfig::default()).await;
    let mut client = Client::connect(address).await;
    client
        .startup(&[("user", "postgres"), ("database", "corium")])
        .await;
    client.read_until_ready().await;

    // Parse (unnamed statement, no parameter types).
    let mut parse = Vec::new();
    parse.push(0); // statement name
    parse.extend_from_slice(b"SELECT name FROM corium.artist ORDER BY name");
    parse.push(0);
    parse.extend_from_slice(&0i16.to_be_bytes()); // parameter count
    client.send(b'P', &parse).await;

    // Bind (unnamed portal to unnamed statement, no formats/parameters).
    let mut bind = Vec::new();
    bind.push(0); // portal name
    bind.push(0); // statement name
    bind.extend_from_slice(&0i16.to_be_bytes()); // format codes
    bind.extend_from_slice(&0i16.to_be_bytes()); // parameters
    bind.extend_from_slice(&0i16.to_be_bytes()); // result formats
    client.send(b'B', &bind).await;

    // Describe the portal.
    client.send(b'D', &[b'P', 0]).await;

    // Execute the portal (unlimited rows).
    let mut execute = vec![0u8];
    execute.extend_from_slice(&0i32.to_be_bytes());
    client.send(b'E', &execute).await;

    client.send(b'S', &[]).await;

    let response = client.read_until_ready().await;
    let tags: Vec<u8> = response.iter().map(|message| message.tag).collect();
    // ParseComplete, BindComplete, RowDescription, 2x DataRow,
    // CommandComplete, ReadyForQuery.
    assert_eq!(tags, vec![b'1', b'2', b'T', b'D', b'D', b'C', b'Z']);
}

#[tokio::test]
async fn extended_protocol_returns_requested_binary_results() {
    let address = start_server(PgWireConfig::default()).await;
    let mut client = Client::connect(address).await;
    client
        .startup(&[("user", "postgres"), ("database", "corium")])
        .await;
    client.read_until_ready().await;

    let mut parse = vec![0u8];
    parse.extend_from_slice(b"SELECT \"release-year\" FROM corium.artist WHERE name = $1");
    parse.push(0);
    parse.extend_from_slice(&1i16.to_be_bytes());
    parse.extend_from_slice(&25i32.to_be_bytes());
    client.send(b'P', &parse).await;

    let mut bind = vec![0u8, 0u8];
    bind.extend_from_slice(&0i16.to_be_bytes());
    bind.extend_from_slice(&1i16.to_be_bytes());
    bind.extend_from_slice(&16i32.to_be_bytes());
    bind.extend_from_slice(b"Boards of Canada");
    bind.extend_from_slice(&1i16.to_be_bytes());
    bind.extend_from_slice(&1i16.to_be_bytes());
    client.send(b'B', &bind).await;
    client.send(b'D', &[b'P', 0]).await;
    client.send(b'E', &[0, 0, 0, 0, 0]).await;
    client.send(b'S', &[]).await;

    let response = client.read_until_ready().await;
    let description = response
        .iter()
        .find(|message| message.tag == b'T')
        .expect("row description");
    assert_eq!(row_description_formats(&description.body), vec![1]);
    let row = response
        .iter()
        .find(|message| message.tag == b'D')
        .expect("binary data row");
    let values = data_row_bytes(&row.body);
    let [Some(bytes)] = values.as_slice() else {
        panic!("one non-null binary value");
    };
    assert_eq!(
        i64::from_be_bytes(bytes.as_slice().try_into().expect("int8")),
        1998
    );
}

#[tokio::test]
async fn extended_protocol_describes_parameterized_statements_before_bind() {
    let address = start_server(PgWireConfig::default()).await;
    let mut client = Client::connect(address).await;
    client
        .startup(&[("user", "postgres"), ("database", "corium")])
        .await;
    client.read_until_ready().await;

    let mut parse = vec![0u8];
    parse.extend_from_slice(b"SELECT name FROM corium.artist WHERE name = $1");
    parse.push(0);
    parse.extend_from_slice(&1i16.to_be_bytes());
    parse.extend_from_slice(&25i32.to_be_bytes());
    client.send(b'P', &parse).await;
    client.send(b'D', &[b'S', 0]).await;
    client.send(b'S', &[]).await;

    let response = client.read_until_ready().await;
    assert_eq!(
        response
            .iter()
            .map(|message| message.tag)
            .collect::<Vec<_>>(),
        vec![b'1', b't', b'T', b'Z']
    );
    assert_eq!(
        row_description_names(
            &response
                .iter()
                .find(|message| message.tag == b'T')
                .expect("query row description")
                .body
        ),
        vec!["name".to_owned()]
    );

    let mut parse = vec![0u8];
    parse.extend_from_slice(b"UPDATE corium.artist SET name = $1 WHERE name = $2 RETURNING name");
    parse.push(0);
    parse.extend_from_slice(&2i16.to_be_bytes());
    parse.extend_from_slice(&25i32.to_be_bytes());
    parse.extend_from_slice(&25i32.to_be_bytes());
    client.send(b'P', &parse).await;
    client.send(b'D', &[b'S', 0]).await;
    client.send(b'S', &[]).await;

    let response = client.read_until_ready().await;
    assert_eq!(
        response
            .iter()
            .map(|message| message.tag)
            .collect::<Vec<_>>(),
        vec![b'1', b't', b'T', b'Z']
    );
    assert_eq!(
        row_description_names(
            &response
                .iter()
                .find(|message| message.tag == b'T')
                .expect("mutation row description")
                .body
        ),
        vec!["name".to_owned()]
    );
}

#[tokio::test]
async fn extended_protocol_binds_parameters_for_a_mutation() {
    let address = start_server(PgWireConfig::default()).await;
    let mut client = Client::connect(address).await;
    client
        .startup(&[("user", "postgres"), ("database", "corium")])
        .await;
    client.read_until_ready().await;

    let mut parse = vec![0u8];
    parse.extend_from_slice(
        b"INSERT INTO corium.artist (name, \"release-year\") \
          VALUES ($1, $2) RETURNING name",
    );
    parse.push(0);
    parse.extend_from_slice(&2i16.to_be_bytes());
    parse.extend_from_slice(&0i32.to_be_bytes()); // contextually inferred text
    parse.extend_from_slice(&0i32.to_be_bytes()); // contextually inferred bigint
    client.send(b'P', &parse).await;

    let mut bind = vec![0u8, 0u8];
    bind.extend_from_slice(&0i16.to_be_bytes()); // all parameters text format
    bind.extend_from_slice(&2i16.to_be_bytes());
    bind.extend_from_slice(&11i32.to_be_bytes());
    bind.extend_from_slice(b"Jon Hopkins");
    bind.extend_from_slice(&4i32.to_be_bytes());
    bind.extend_from_slice(b"2001");
    bind.extend_from_slice(&0i16.to_be_bytes()); // text results
    client.send(b'B', &bind).await;
    client.send(b'D', &[b'P', 0]).await;
    client.send(b'E', &[0, 0, 0, 0, 0]).await;
    client.send(b'S', &[]).await;

    let response = client.read_until_ready().await;
    assert_eq!(
        data_row_values(
            &response
                .iter()
                .find(|message| message.tag == b'D')
                .expect("returning row")
                .body
        ),
        vec![Some("Jon Hopkins".to_owned())]
    );
    assert_eq!(
        cstrings(
            &response
                .iter()
                .find(|message| message.tag == b'C')
                .expect("command tag")
                .body
        ),
        vec!["INSERT 0 1".to_owned()]
    );
}

#[tokio::test]
async fn show_databases_lists_the_catalog() {
    let address = start_server(PgWireConfig::default()).await;
    let mut client = Client::connect(address).await;
    client
        .startup(&[("user", "postgres"), ("database", "corium")])
        .await;
    client.read_until_ready().await;

    client.query("SHOW DATABASES").await;
    let response = client.read_until_ready().await;

    let description = response
        .iter()
        .find(|message| message.tag == b'T')
        .expect("row description");
    assert_eq!(
        row_description_names(&description.body),
        vec!["database".to_owned()]
    );
    let names: Vec<Option<String>> = response
        .iter()
        .filter(|message| message.tag == b'D')
        .flat_map(|message| data_row_values(&message.body))
        .collect();
    assert_eq!(
        names,
        vec![Some("corium".to_owned()), Some("people".to_owned())]
    );
    let complete = response
        .iter()
        .find(|message| message.tag == b'C')
        .expect("command complete");
    assert_eq!(cstrings(&complete.body), vec!["SHOW".to_owned()]);
}

#[tokio::test]
async fn use_switches_the_active_database() {
    let address = start_server(PgWireConfig::default()).await;
    let mut client = Client::connect(address).await;
    // Connect without a database; queries fail until one is selected.
    client.startup(&[("user", "postgres")]).await;
    client.read_until_ready().await;

    client.query("SELECT 1").await;
    let response = client.read_until_ready().await;
    assert_eq!(response.first().unwrap().tag, b'E');

    // Switching to a known database makes queries work.
    client.query("USE people").await;
    let response = client.read_until_ready().await;
    let complete = response
        .iter()
        .find(|message| message.tag == b'C')
        .expect("command complete");
    assert_eq!(cstrings(&complete.body), vec!["USE".to_owned()]);

    client
        .query("SELECT name FROM corium.artist ORDER BY name")
        .await;
    let response = client.read_until_ready().await;
    assert!(response.iter().any(|message| message.tag == b'D'));

    // Switching to an unknown database is an error.
    client.query("USE nope").await;
    let response = client.read_until_ready().await;
    assert_eq!(response.first().unwrap().tag, b'E');
}

// ---------------------------------------------------------------------------
// Identity and authorization: the password field carries a bearer token, and
// every statement is authorized as the principal it names.
// ---------------------------------------------------------------------------

/// A guard where `alice-token` reads everything, `bob-token` reads the artist
/// table without `release-year`, and any other caller holds no role at all.
fn scenario_guard() -> Guard {
    let tokens = StaticTokens::new()
        .with(
            "alice-token",
            Principal::new("static-token", "alice").with_role("full"),
        )
        .with(
            "bob-token",
            Principal::new("static-token", "bob").with_role("redacted"),
        )
        .with("carol-token", Principal::new("static-token", "carol"));
    let redacted: Arc<dyn ViewFilter> = Arc::new(AttributeAllowlist::new([":artist/name"]));
    let policy = PolicyAuthorizer::new()
        .grant(
            "full",
            Grant::new(
                [ActionClass::Read, ActionClass::Write],
                ["corium", "people"],
            ),
        )
        .grant(
            "redacted",
            Grant::new([ActionClass::Read, ActionClass::Write], ["corium"])
                .with_view(Arc::clone(&redacted)),
        );
    Guard::new(Arc::new(tokens), Arc::new(policy))
}

async fn start_guarded_server() -> std::net::SocketAddr {
    start_server(PgWireConfig {
        guard: scenario_guard(),
        ..PgWireConfig::default()
    })
    .await
}

/// Connects, authenticates with `token` in the password field, and selects
/// `database`.
async fn connect_as(address: std::net::SocketAddr, token: &str, database: &str) -> Client {
    let mut client = Client::connect(address).await;
    client
        .startup(&[("user", "irrelevant"), ("database", database)])
        .await;
    let request = client.read_message().await;
    assert_eq!(request.tag, b'R', "server asks for a password");
    client.password(token).await;
    client
}

#[tokio::test]
async fn a_view_redacts_one_principal_and_not_another() {
    let address = start_guarded_server().await;

    let mut alice = connect_as(address, "alice-token", "corium").await;
    alice.read_until_ready().await;
    alice
        .query("SELECT name, \"release-year\" FROM corium.artist ORDER BY name")
        .await;
    let response = alice.read_until_ready().await;
    assert_eq!(
        data_row_values(&response[1].body),
        vec![Some("Boards of Canada".to_owned()), Some("1998".to_owned())]
    );

    // Bob runs the identical statement against the identical database value.
    let mut bob = connect_as(address, "bob-token", "corium").await;
    bob.read_until_ready().await;
    bob.query("SELECT name, \"release-year\" FROM corium.artist ORDER BY name")
        .await;
    let response = bob.read_until_ready().await;
    assert_eq!(
        row_description_names(&response[0].body),
        vec!["name".to_owned(), "release-year".to_owned()],
        "the column keeps its declared shape"
    );
    assert_eq!(
        data_row_values(&response[1].body),
        vec![Some("Boards of Canada".to_owned()), None],
        "a column the view hides reports NULL"
    );
}

#[tokio::test]
async fn a_hidden_column_cannot_be_probed_by_a_predicate() {
    let address = start_guarded_server().await;
    let mut bob = connect_as(address, "bob-token", "corium").await;
    bob.read_until_ready().await;
    // If the predicate were pushed into the index, this would report which
    // artist holds the hidden value even though the projection is NULL.
    bob.query("SELECT name FROM corium.artist WHERE \"release-year\" = 1998")
        .await;
    let response = bob.read_until_ready().await;
    let rows: Vec<&Message> = response
        .iter()
        .filter(|message| message.tag == b'D')
        .collect();
    assert!(rows.is_empty(), "a hidden column matches nothing");
}

#[tokio::test]
async fn a_principal_with_no_grant_is_refused() {
    let address = start_guarded_server().await;
    let mut carol = connect_as(address, "carol-token", "corium").await;
    carol.read_until_ready().await;
    carol.query("SELECT name FROM corium.artist").await;
    let response = carol.read_until_ready().await;
    let error = response
        .iter()
        .find(|message| message.tag == b'E')
        .expect("an unauthorized read is refused");
    let fields = cstrings(&error.body);
    assert!(
        fields.iter().any(|field| field == "C42501"),
        "expected insufficient_privilege, got {fields:?}"
    );
}

#[tokio::test]
async fn a_restricted_session_may_not_write() {
    let address = start_guarded_server().await;
    let mut bob = connect_as(address, "bob-token", "corium").await;
    bob.read_until_ready().await;
    bob.query("UPDATE corium.artist SET name = 'x' WHERE name = 'Tycho'")
        .await;
    let response = bob.read_until_ready().await;
    let error = response
        .iter()
        .find(|message| message.tag == b'E')
        .expect("a restricted principal may not write");
    assert!(
        cstrings(&error.body)
            .iter()
            .any(|field| field.contains("restricted view")),
        "{:?}",
        cstrings(&error.body)
    );
}

#[tokio::test]
async fn show_databases_lists_only_what_the_principal_may_inspect() {
    let address = start_guarded_server().await;

    let mut alice = connect_as(address, "alice-token", "corium").await;
    alice.read_until_ready().await;
    alice.query("SHOW DATABASES").await;
    let response = alice.read_until_ready().await;
    let names: Vec<Option<String>> = response
        .iter()
        .filter(|message| message.tag == b'D')
        .flat_map(|message| data_row_values(&message.body))
        .collect();
    assert_eq!(
        names,
        vec![Some("corium".to_owned()), Some("people".to_owned())]
    );

    // Bob's grant covers only `corium`, so the catalog listing says so too.
    let mut bob = connect_as(address, "bob-token", "corium").await;
    bob.read_until_ready().await;
    bob.query("SHOW DATABASES").await;
    let response = bob.read_until_ready().await;
    let names: Vec<Option<String>> = response
        .iter()
        .filter(|message| message.tag == b'D')
        .flat_map(|message| data_row_values(&message.body))
        .collect();
    assert_eq!(names, vec![Some("corium".to_owned())]);
}

#[tokio::test]
async fn an_unknown_token_cannot_connect() {
    let address = start_guarded_server().await;
    let mut client = Client::connect(address).await;
    client.startup(&[("user", "mallory")]).await;
    assert_eq!(client.read_message().await.tag, b'R');
    client.password("not-a-real-token").await;
    let response = client.read_message().await;
    assert_eq!(response.tag, b'E', "authentication is required");
    assert!(
        cstrings(&response.body)
            .iter()
            .any(|field| field == "C28000"),
        "{:?}",
        cstrings(&response.body)
    );
}
