//! Attribute protection end to end: a key-holding peer writes and reads
//! plaintext, a keyless peer gets exactly what the class policy says, and the
//! transactor commits, indexes, and publishes without ever holding a key.

use std::net::TcpListener;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use corium_client::query::{Query, attr, data, var};
use corium_client::tx::{EntityMap, TxBuilder, tempid};
use corium_client::{Args, LocalPeer, Peer};
use corium_crypt::testing::InMemoryKms;
use corium_crypt::{KeyId, KmsKeyring, StaticKeyring};
use corium_peer::{Admin, ConnectConfig};
use corium_query::edn::read_all;
use corium_transactor::node::{NodeConfig, TransactorNode};

/// A value distinctive enough that finding it anywhere under the data
/// directory means something wrote it in the clear.
const SENTINEL: &str = "kaleidoscope-pangolin";

/// One class per policy, all naming the same key so one keyring opens them
/// all — classes are cheap, and each is independently shreddable.
fn schema(key: &KeyId) -> String {
    format!(
        r#"
{{:db.protect/ident :protect/redacting
  :db.protect/key "{key}"}}
{{:db.protect/ident :protect/hiding
  :db.protect/key "{key}"
  :db.protect/on-missing-key :db.protect.missing/hide}}
{{:db.protect/ident :protect/failing
  :db.protect/key "{key}"
  :db.protect/on-missing-key :db.protect.missing/error}}
{{:db/ident :person/name
  :db/valueType :db.type/string
  :db/cardinality :db.cardinality/one
  :db/unique :db.unique/identity}}
{{:db/ident :person/ssn
  :db/valueType :db.type/string
  :db/cardinality :db.cardinality/one
  :db/protection :protect/redacting}}
{{:db/ident :person/mood
  :db/valueType :db.type/keyword
  :db/cardinality :db.cardinality/one
  :db/protection :protect/hiding}}
{{:db/ident :person/salary
  :db/valueType :db.type/long
  :db/cardinality :db.cardinality/one
  :db/protection :protect/failing}}
"#
    )
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port()
}

fn key_file(dir: &Path, name: &str, byte: u8) -> KeyId {
    let path = dir.join(name);
    std::fs::write(&path, [byte; 32]).expect("write key");
    KeyId::new(format!("file:{}", path.display())).expect("key id")
}

async fn start_transactor(dir: &Path) -> (String, tokio::sync::oneshot::Sender<()>) {
    let mut config = NodeConfig::new(dir.join("data"));
    config.owner = "protection".into();
    config.lease_ttl_ms = 600_000;
    config.index_interval = Duration::from_secs(600);
    config.heartbeat_interval = Duration::from_secs(600);
    let node = TransactorNode::open(config).await.expect("open node");
    let addr: std::net::SocketAddr = format!("127.0.0.1:{}", free_port()).parse().expect("addr");
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(corium_transactor::server::serve(
        node,
        addr,
        corium_protocol::authz::Guard::disabled(),
        None,
        async move {
            let _ = stop_rx.await;
        },
    ));
    let endpoint = format!("http://{addr}");
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        if let Ok(mut admin) = Admin::connect(&endpoint, None, None).await
            && admin.list_databases().await.is_ok()
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "transactor never ready"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    (endpoint, stop_tx)
}

/// Every byte the node made durable, so a plaintext search covers blobs,
/// roots, and log files alike.
fn durable_bytes(dir: &Path) -> Vec<u8> {
    let mut bytes = Vec::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(path) = pending.pop() {
        for entry in std::fs::read_dir(&path).expect("read dir") {
            let entry = entry.expect("dir entry");
            if entry.file_type().expect("file type").is_dir() {
                pending.push(entry.path());
            } else {
                bytes.extend(std::fs::read(entry.path()).expect("read file"));
            }
        }
    }
    bytes
}

fn names() -> Query {
    Query::find_coll(var("name")).where_(data(var("e"), attr("person/name"), var("name")))
}

#[tokio::test]
#[allow(clippy::too_many_lines)]
async fn a_keyless_peer_sees_structure_but_not_protected_values() {
    let dir = tempfile::tempdir().expect("tempdir");
    let key = key_file(dir.path(), "pii.key", 7);
    let (endpoint, _stop) = start_transactor(dir.path()).await;
    Admin::connect(&endpoint, None, None)
        .await
        .expect("admin")
        .create_database("people", &read_all(&schema(&key)).expect("schema parses"))
        .await
        .expect("create db");

    let keyring = Arc::new(StaticKeyring::resolve([key.clone()]).expect("resolve key"));
    let writer =
        LocalPeer::connect(ConnectConfig::new(&endpoint, "people").with_keyring(keyring.clone()))
            .await
            .expect("key-holding peer connects");

    writer
        .transact(
            TxBuilder::new()
                .entity(
                    EntityMap::with_id(tempid("alice"))
                        .set("person/name", "Alice")
                        .set("person/ssn", SENTINEL)
                        .set("person/salary", 100_000_i64),
                )
                .build(),
        )
        .await
        .expect("write through the key holder");
    // A keyword value seals its text, so the protected vocabulary never
    // reaches the naming table either.
    writer
        .transact(
            TxBuilder::new()
                .entity(
                    EntityMap::with_id(tempid("bob"))
                        .set("person/name", "Bob")
                        .set("person/ssn", "999-99-9999")
                        .set("person/mood", corium_client::query::kw("mood/cheerful"))
                        .set("person/salary", 90_000_i64),
                )
                .build(),
        )
        .await
        .expect("write a keyword-valued protected attribute");

    // Nothing the transactor made durable holds the plaintext, and it
    // committed, indexed, and published all the same.
    let durable = durable_bytes(&dir.path().join("data"));
    assert!(
        !durable
            .windows(SENTINEL.len())
            .any(|window| window == SENTINEL.as_bytes()),
        "the protected value reached storage in the clear"
    );
    assert!(
        !durable.windows(8).any(|window| window == b"cheerful"),
        "the protected keyword vocabulary reached storage in the clear"
    );

    let writer_db = writer.sync().await.expect("writer sync");
    let ssn = Query::find_coll(var("ssn"))
        .in_scalar(var("name"))
        .where_(data(var("e"), attr("person/name"), var("name")))
        .and(data(var("e"), attr("person/ssn"), var("ssn")));
    let held = writer_db
        .query(&ssn, Args::new().scalar("Alice"))
        .await
        .expect("key holder query")
        .values_as::<String>()
        .expect("strings");
    assert_eq!(held, vec![SENTINEL.to_string()]);

    // --- A peer with no keys at all. ---
    let reader = LocalPeer::connect(ConnectConfig::new(&endpoint, "people"))
        .await
        .expect("keyless peer connects");
    let reader_db = reader.sync().await.expect("reader sync");

    // Everything that does not depend on a protected value is identical.
    let expected = writer_db
        .query(&names(), Args::new())
        .await
        .expect("writer names")
        .values_as::<String>()
        .expect("strings");
    let actual = reader_db
        .query(&names(), Args::new())
        .await
        .expect("reader names")
        .values_as::<String>()
        .expect("strings");
    assert_eq!(actual, expected);

    // `redact`: the value binds and renders redacted, so the entity, the
    // attribute, and the transaction are all still there.
    let keyless_ssn = reader_db
        .query(&ssn, Args::new().scalar("Alice"))
        .await
        .expect("keyless query still runs");
    let redacted = keyless_ssn.values();
    assert_eq!(redacted.len(), 1);
    let rendered = redacted[0].to_string();
    assert!(
        rendered.starts_with("#corium/redacted"),
        "expected a redacted rendering, got {rendered}"
    );
    assert!(
        rendered.contains(":protect/redacting"),
        "the redacted form names its class: {rendered}"
    );
    assert!(
        rendered.contains(":db.type/string"),
        "the redacted form names the declared type: {rendered}"
    );

    // A redacted value never satisfies a constant: the pattern binds nothing.
    let by_value = Query::find_coll(var("e")).where_(data(
        var("e"),
        attr("person/ssn"),
        corium_client::query::lit(SENTINEL),
    ));
    assert!(
        reader_db
            .query(&by_value, Args::new())
            .await
            .expect("query runs")
            .values()
            .is_empty(),
        "a keyless reader must not match a protected value by its plaintext"
    );

    // `hide`: the datom is gone from the scan, so the entity drops out of the
    // join entirely.
    let mood =
        Query::find_coll(var("mood")).where_(data(var("e"), attr("person/mood"), var("mood")));
    assert!(
        !writer_db
            .query(&mood, Args::new())
            .await
            .expect("key holder mood")
            .values()
            .is_empty(),
        "the key holder sees the moods"
    );
    assert!(
        reader_db
            .query(&mood, Args::new())
            .await
            .expect("keyless mood query still runs")
            .values()
            .is_empty(),
        "a hidden datom binds nothing"
    );

    // `error`: the read fails loudly rather than leaving a quiet hole.
    let salary = Query::find_coll(var("salary")).where_(data(
        var("e"),
        attr("person/salary"),
        var("salary"),
    ));
    let error = reader_db
        .query(&salary, Args::new())
        .await
        .expect_err("an error-policy class fails the read");
    assert!(
        error.to_string().contains("not readable without its key"),
        "unexpected error: {error}"
    );
}

/// The same schema with every `:db/protection` removed, so the two databases
/// differ in exactly one thing.
fn unprotected_schema() -> String {
    r"
{:db/ident :person/name
 :db/valueType :db.type/string
 :db/cardinality :db.cardinality/one
 :db/unique :db.unique/identity}
{:db/ident :person/ssn
 :db/valueType :db.type/string
 :db/cardinality :db.cardinality/one}
{:db/ident :person/mood
 :db/valueType :db.type/keyword
 :db/cardinality :db.cardinality/one}
{:db/ident :person/salary
 :db/valueType :db.type/long
 :db/cardinality :db.cardinality/one}
"
    .to_owned()
}

/// The workload both databases answer, covering every shape protection could
/// have broken: constants, joins, predicates, aggregates, pull, and history.
async fn workload(db: &corium_client::Db) -> Vec<String> {
    use corium_client::pull::Pull;
    use corium_client::query::{gt, sum};

    let mut out = Vec::new();
    let all = Query::find_rel(vec![
        var("name").into(),
        var("ssn").into(),
        var("mood").into(),
        var("salary").into(),
    ])
    .where_(data(var("e"), attr("person/name"), var("name")))
    .and(data(var("e"), attr("person/ssn"), var("ssn")))
    .and(data(var("e"), attr("person/mood"), var("mood")))
    .and(data(var("e"), attr("person/salary"), var("salary")));
    out.push(format!(
        "{:?}",
        db.query(&all, Args::new()).await.expect("relation").rows()
    ));

    // A constant in value position on a protected attribute: the key holder
    // seals it to compare, and finds the row.
    let by_ssn = Query::find_coll(var("name"))
        .where_(data(
            var("e"),
            attr("person/ssn"),
            corium_client::query::lit(SENTINEL),
        ))
        .and(data(var("e"), attr("person/name"), var("name")));
    out.push(format!(
        "{:?}",
        db.query(&by_ssn, Args::new())
            .await
            .expect("constant match")
            .values()
    ));

    // A predicate over a hydrated value.
    let rich = Query::find_coll(var("name"))
        .where_(data(var("e"), attr("person/name"), var("name")))
        .and(data(var("e"), attr("person/salary"), var("salary")))
        .and(gt(var("salary"), corium_client::query::lit(95_000_i64)));
    out.push(format!(
        "{:?}",
        db.query(&rich, Args::new())
            .await
            .expect("predicate")
            .values()
    ));

    // A value-ordered aggregate over a hydrated value.
    let total = Query::find_scalar(sum(var("salary"))).where_(data(
        var("e"),
        attr("person/salary"),
        var("salary"),
    ));
    out.push(format!(
        "{:?}",
        db.query(&total, Args::new())
            .await
            .expect("aggregate")
            .scalar()
    ));

    // Pull, which reads through the same scan helper.
    let alice = Query::find_coll(var("e")).where_(data(
        var("e"),
        attr("person/name"),
        corium_client::query::lit("Alice"),
    ));
    let alice_result = db.query(&alice, Args::new()).await.expect("alice");
    let alice = alice_result
        .values()
        .first()
        .copied()
        .expect("alice exists");
    out.push(
        db.pull(
            &Pull::new()
                .attr("person/name")
                .attr("person/ssn")
                .attr("person/salary"),
            alice,
        )
        .await
        .expect("pull")
        .to_string(),
    );
    out
}

async fn load(peer: &impl Peer) {
    peer.transact(
        TxBuilder::new()
            .entity(
                EntityMap::with_id(tempid("alice"))
                    .set("person/name", "Alice")
                    .set("person/ssn", SENTINEL)
                    .set("person/mood", corium_client::query::kw("mood/wry"))
                    .set("person/salary", 100_000_i64),
            )
            .entity(
                EntityMap::with_id(tempid("bob"))
                    .set("person/name", "Bob")
                    .set("person/ssn", "999-99-9999")
                    .set("person/mood", corium_client::query::kw("mood/cheerful"))
                    .set("person/salary", 90_000_i64),
            )
            .build(),
    )
    .await
    .expect("load");
}

/// A peer holding every class key gets the same answers it would get if the
/// attributes had never been protected. Protection is invisible to a reader
/// entitled to the data — that is the whole point of putting hydration at
/// scan output.
#[tokio::test]
async fn a_key_holder_reads_exactly_what_an_unprotected_database_returns() {
    let dir = tempfile::tempdir().expect("tempdir");
    let key = key_file(dir.path(), "pii.key", 11);
    let (endpoint, _stop) = start_transactor(dir.path()).await;
    let mut admin = Admin::connect(&endpoint, None, None).await.expect("admin");
    admin
        .create_database(
            "protected",
            &read_all(&schema(&key)).expect("schema parses"),
        )
        .await
        .expect("create protected db");
    admin
        .create_database(
            "plain",
            &read_all(&unprotected_schema()).expect("schema parses"),
        )
        .await
        .expect("create plain db");

    let keyring = Arc::new(StaticKeyring::resolve([key]).expect("resolve key"));
    let protected =
        LocalPeer::connect(ConnectConfig::new(&endpoint, "protected").with_keyring(keyring))
            .await
            .expect("key-holding peer");
    let plain = LocalPeer::connect(ConnectConfig::new(&endpoint, "plain"))
        .await
        .expect("plain peer");
    load(&protected).await;
    load(&plain).await;

    let protected_db = protected.sync().await.expect("sync");
    let plain_db = plain.sync().await.expect("sync");
    let observed = workload(&protected_db).await;
    assert_eq!(observed, workload(&plain_db).await);
    // Non-vacuity: the workload really did read the protected values, rather
    // than agreeing because both sides returned nothing.
    assert!(
        observed[1].contains("Alice"),
        "a constant in value position must match a protected value for a key \
         holder: {observed:?}"
    );
    assert!(
        observed[3].contains("190000"),
        "an aggregate must sum the hydrated salaries: {observed:?}"
    );
    assert!(
        observed[4].contains(SENTINEL),
        "pull must return the hydrated value: {observed:?}"
    );
}

/// A keyring whose class key is derived inside a key service.
fn kms_keyring(service: &Arc<InMemoryKms>, key: &KeyId) -> Arc<KmsKeyring> {
    Arc::new(KmsKeyring::new(
        Arc::clone(service) as Arc<dyn corium_crypt::KmsClient>,
        [key.clone()],
    ))
}

#[tokio::test]
async fn a_class_key_from_a_key_service_seals_on_one_peer_and_opens_on_another() {
    let dir = tempfile::tempdir().expect("tempdir");
    // The class names a KMS identity, so no process ever reads the material:
    // each derives it by asking the service to MAC the class context.
    let key = KeyId::new("awskms:alias/corium-pii").expect("key id");
    let (endpoint, _stop) = start_transactor(dir.path()).await;
    Admin::connect(&endpoint, None, None)
        .await
        .expect("admin")
        .create_database("people", &read_all(&schema(&key)).expect("schema parses"))
        .await
        .expect("create db");

    let writing_service = Arc::new(InMemoryKms::new(41));
    let writer = LocalPeer::connect(
        ConnectConfig::new(&endpoint, "people").with_keyring(kms_keyring(&writing_service, &key)),
    )
    .await
    .expect("key-holding peer connects");
    writer
        .transact(
            TxBuilder::new()
                .entity(
                    EntityMap::with_id(tempid("alice"))
                        .set("person/name", "Alice")
                        .set("person/ssn", SENTINEL),
                )
                .build(),
        )
        .await
        .expect("write through the key holder");
    assert!(
        writing_service.calls() > 0,
        "the class key was never derived remotely"
    );

    // Neither the sealed value nor the service's key reached storage.
    let durable = durable_bytes(&dir.path().join("data"));
    assert!(
        !durable
            .windows(SENTINEL.len())
            .any(|window| window == SENTINEL.as_bytes()),
        "the protected value reached storage in the clear"
    );
    assert!(
        !durable
            .windows(32)
            .any(|window| window == [41_u8; 32].as_slice()),
        "the key service's key reached storage"
    );

    let ssn = Query::find_coll(var("ssn"))
        .in_scalar(var("name"))
        .where_(data(var("e"), attr("person/name"), var("name")))
        .and(data(var("e"), attr("person/ssn"), var("ssn")));

    // A second peer, a separate client over the same service key — the
    // deployment where two processes hold one grant — opens what the first
    // sealed. That only works because derivation is deterministic.
    let granted = LocalPeer::connect(
        ConnectConfig::new(&endpoint, "people")
            .with_keyring(kms_keyring(&Arc::new(InMemoryKms::new(41)), &key)),
    )
    .await
    .expect("second grant holder connects");
    assert_eq!(
        granted
            .sync()
            .await
            .expect("sync")
            .query(&ssn, Args::new().scalar("Alice"))
            .await
            .expect("granted query")
            .values_as::<String>()
            .expect("strings"),
        vec![SENTINEL.to_string()]
    );

    // A peer whose keyring resolves other identities but not this class is
    // simply not granted it: an ordinary keyless reader for this attribute,
    // and the class policy redacts.
    let ungranted = LocalPeer::connect(ConnectConfig::new(&endpoint, "people").with_keyring(
        kms_keyring(
            &Arc::new(InMemoryKms::new(41)),
            &KeyId::new("awskms:alias/corium-payroll").expect("key id"),
        ),
    ))
    .await
    .expect("other peer connects");
    let results = ungranted
        .sync()
        .await
        .expect("sync")
        .query(&ssn, Args::new().scalar("Alice"))
        .await
        .expect("query still runs");
    let values = results.values();
    assert_eq!(values.len(), 1);
    assert!(
        values[0].to_string().starts_with("#corium/redacted"),
        "expected a redacted rendering, got {}",
        values[0]
    );
}
