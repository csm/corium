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
        let prepared = prepare(&db, items, EntityId::new(Partition::Tx as u32, t), 2_000)
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
    let name = EntityId::from_raw(10);
    let tags = EntityId::from_raw(11);
    let release_year = EntityId::from_raw(12);
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
async fn writes_in_explicit_transaction_blocks_are_rejected_without_committing() {
    let address = start_server(PgWireConfig::default()).await;
    let mut client = Client::connect(address).await;
    client
        .startup(&[("user", "postgres"), ("database", "corium")])
        .await;
    client.read_until_ready().await;

    client
        .query("BEGIN; INSERT INTO corium.artist (name) VALUES ('Must Not Commit')")
        .await;
    let response = client.read_until_ready().await;
    assert!(response.iter().any(|message| message.tag == b'E'));
    assert_eq!(response.last().unwrap().body, vec![b'T']);

    client.query("ROLLBACK").await;
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
    parse.extend_from_slice(&25i32.to_be_bytes()); // text
    parse.extend_from_slice(&20i32.to_be_bytes()); // int8
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
