//! Release scenarios for the security boundaries users actually deploy.
//!
//! These tests deliberately use signed RS256 JWTs, the self-hosted `ReBAC`
//! authorizer, real gRPC servers, real peer hydration, and a `PostgreSQL` wire
//! client. The peer-server and pgwire tests also pin current limitations so
//! the non-blocking scenario report can surface them without mistaking a
//! known, fail-safe boundary for a harness failure.

#![cfg(feature = "oidc")]

use std::net::TcpListener;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use corium_authz::{AuthzConfig, SystemDbAuthorizer, bootstrap, schema};
use corium_client::query::{Query, attr, data, lit, var};
use corium_client::tx::{EntityMap, TxBuilder, tempid};
use corium_client::{Args, LocalPeer, Peer, RemotePeer};
use corium_crypt::{KeyId, StaticKeyring};
use corium_db::Db;
use corium_peer::server::{PeerServerConfig, PeerServerSvc};
use corium_peer::{Admin, ConnectConfig, Connection, PeerError};
use corium_pgwire::{CatalogError, CatalogTxResult, DbCatalog, PgWireConfig};
use corium_protocol::authz::{ExternalTokens, Guard, IdentityProvider};
use corium_protocol::oidc::{OidcConfig, OidcVerifier};
use corium_query::edn::{Edn, read_all};
use corium_transactor::node::{NodeConfig, TransactorNode};
use ring::signature;

const ISSUER: &str = "https://scenario-issuer.example";
const AUDIENCE: &str = "corium-scenarios";
const SENTINEL: &str = "scenario-secret-8675309";

// A test-only 2048-bit RSA key. The private half signs short-lived scenario
// tokens; only its public modulus/exponent are placed in the JWKS.
const TEST_KEY_PKCS8_B64: &str = "MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDCjNlYMqERhTY1svp3UnxBTrUEisZnomkHMbWlkMV/8QErAHvknLuar8Gh/3rbHzuGSsW0qTj+QOvr7ZmeTQ1lUP031ssEkjDxJr2DFVC9zhtu0O5VN5OIy3s1DwfeWZfhaTZaEC4Oc0MSFOkxjsTtwvEy5jqgbMZo4xsQ1ug5q7zIxBeQ3WVR3sU4MG3Fw3JJJ/MZW+V8C9tq0bRPSolOdkZg6gOWA+Y9zJVUxCoWLcz9aLaHkIrGNxmY9azWCk55zfCwiZy3USxZC1zKknqV0EYL2OFfOvVfrD4l/vTK1iLpAgSu1dio2+4uxWWvv328rbEFr1x7GuBqtX7fKa67AgMBAAECggEAQI5BLpF6PdiQoOf3UXHG9lq6GTw9UrUjGbaGel5cErSzeQPrmHPjkpQgcfNW3m/yLgEQsn52gXOkdUB9uXgC6mwh4g39htJFuDdtKhqAFMNX+gENHKzY4Ur34qbOqxramXryhJca2UOo7U6QBJhFw0ltBME9ke8WNUaqu/87xqqgUFGs4lcvbDvh5pIYjfQ6WTssE8qnWS5JfHVIvx+acnQS7RLioaWrqtuT5ShGERrL2hagXzDWpEGVGrHujR86RaopL3Fo63MppK6ZXhYdWeLZMoXK3jae/nU6mTyz8KbpvBh1F5AOgCmMoYyKVSip4MjTzlFM5Ec+YhEQNfXIHQKBgQDkU+nUByFFD2+3GdVHgyoadojjt19o/TnD9EHamTUJjByDBJOvRYrlh0MlHEU5QUEIiZUKbQPZLBHbxIRPR3dyIpGAqA3AqlIx2fGtjYFlOwBQiI6S8iC8HVFlKjnkxJW3aIL7lLJyB9M4sKsxhawZHP7McpT9R90Ec6TMlDPddQKBgQDaIPQf4WNwQFKx3ibMQDM3ofYj0EICmSKpx8qeqfjr8vyHvVc+wmkPTdn0t7/RzVnn/NRqFSHJyV9EdEhiWnesq9ki+OuMUhK8meD459Tq25cCzhJzD4Pc/UAkYjNLKa5lNzCOJKqmFV7M0tRSFKCQBBiTNGzlpzrKbVJSd+flbwKBgAURs9xIOD3fRNyszyZiTBoATbO4i366OIEYOCoRQrMukCd8f4bhpV7JLP1y7jqCL15wJ4Xuu6ojp1XYvBNCg+1dxRs1H/EKFv8SVqJCxP+pWq1vCrNKet2STQ9Q664fiy9iO544Q+nyMIdOrM5RqGt6UFHbrWEeKlMB+kOseqZNAoGAaxDRwvQ2gtqPvI52LLs2aJAu6NVIEU5pHTzbz5VOgUH7ggUF1eBHASQNX3jxxmEtSBlpichllU4qXMdW4C/XngGbyvazZ2TBnaFKM+JXOBAgx1eu5psu9kG4QiORWctTtoqoYpzMxkinB5JUdRV62jWoeli5OuAik0mlpqUERjECgYB/7rS+Arvnzj+/YfJaGPiYnysLI8sdTJ9Tzijns6k41CDyLedPA+5l3cGQv6xxRqdDUq6JjnZQceNdnHEZgDMMes+tslaN5tmODoBKGGts+tppplc1SjMg5AkfDkuINkZhd65BIIHh5JDQJGIsvD1bgnPP3+fJIWofMQ1cDolO+w==";
const TEST_KEY_N_B64URL: &str = "wozZWDKhEYU2NbL6d1J8QU61BIrGZ6JpBzG1pZDFf_EBKwB75Jy7mq_Bof962x87hkrFtKk4_kDr6-2Znk0NZVD9N9bLBJIw8Sa9gxVQvc4bbtDuVTeTiMt7NQ8H3lmX4Wk2WhAuDnNDEhTpMY7E7cLxMuY6oGzGaOMbENboOau8yMQXkN1lUd7FODBtxcNySSfzGVvlfAvbatG0T0qJTnZGYOoDlgPmPcyVVMQqFi3M_Wi2h5CKxjcZmPWs1gpOec3wsImct1EsWQtcypJ6ldBGC9jhXzr1X6w-Jf70ytYi6QIErtXYqNvuLsVlr799vK2xBa9cexrgarV-3ymuuw";
const TEST_KEY_E_B64URL: &str = "AQAB";

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind an ephemeral port")
        .local_addr()
        .expect("read bound address")
        .port()
}

fn forms(text: &str) -> Vec<Edn> {
    match read_all(text).expect("EDN parses").pop() {
        Some(Edn::Vector(items)) => items,
        other => panic!("expected one vector, got {other:?}"),
    }
}

fn jwks_json() -> String {
    format!(
        r#"{{"keys":[{{"kty":"RSA","kid":"scenario-key","alg":"RS256","use":"sig","n":"{TEST_KEY_N_B64URL}","e":"{TEST_KEY_E_B64URL}"}}]}}"#,
    )
}

fn jwt(subject: &str) -> String {
    jwt_for(subject, ISSUER, AUDIENCE)
}

fn jwt_for(subject: &str, issuer: &str, audience: &str) -> String {
    let header = serde_json::json!({"alg": "RS256", "typ": "JWT", "kid": "scenario-key"});
    let expires = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time after epoch")
        .as_secs()
        + 3_600;
    let claims = serde_json::json!({
        "iss": issuer,
        "sub": subject,
        "aud": audience,
        "exp": expires,
    });
    let header = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&header).expect("header JSON"));
    let claims = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).expect("claims JSON"));
    let signing_input = format!("{header}.{claims}");
    let der = STANDARD
        .decode(TEST_KEY_PKCS8_B64)
        .expect("test key is base64");
    let key = signature::RsaKeyPair::from_pkcs8(&der).expect("test key is PKCS#8");
    let mut signed = vec![0_u8; key.public().modulus_len()];
    key.sign(
        &signature::RSA_PKCS1_SHA256,
        &ring::rand::SystemRandom::new(),
        signing_input.as_bytes(),
        &mut signed,
    )
    .expect("sign scenario JWT");
    format!("{signing_input}.{}", URL_SAFE_NO_PAD.encode(signed))
}

fn guard(authorizer: Arc<SystemDbAuthorizer>) -> Guard {
    let config = OidcConfig::new(ISSUER, [AUDIENCE]);
    let verifier = OidcVerifier::from_jwks_json(config, &jwks_json()).expect("load scenario JWKS");
    let provider: Arc<dyn IdentityProvider> = Arc::new(ExternalTokens::new("oidc", verifier));
    Guard::new(provider, authorizer)
}

fn schema_edn(key: &KeyId) -> String {
    format!(
        r#"[{{:db.protect/ident :protect/pii
              :db.protect/key "{key}"
              :db.protect/on-missing-key :db.protect.missing/redact}}
            {{:db/ident :person/name :db/valueType :db.type/string
              :db/unique :db.unique/identity}}
            {{:db/ident :person/age :db/valueType :db.type/long}}
            {{:db/ident :person/ssn :db/valueType :db.type/string
              :db/protection :protect/pii}}]"#,
    )
}

struct Cluster {
    _node: Arc<TransactorNode>,
    open_endpoint: String,
    guarded_endpoint: String,
    keyring: Arc<StaticKeyring>,
    _dir: tempfile::TempDir,
}

impl Cluster {
    async fn start() -> Self {
        let dir = tempfile::tempdir().expect("scenario tempdir");
        let key_path = dir.path().join("pii.key");
        std::fs::write(&key_path, [37_u8; 32]).expect("write scenario class key");
        let key = KeyId::new(format!("file:{}", key_path.display())).expect("class key id");
        let keyring = Arc::new(StaticKeyring::resolve([key.clone()]).expect("resolve class key"));

        let mut config = NodeConfig::new(dir.path().join("data"));
        config.owner = "security-scenario".into();
        config.lease_ttl_ms = 600_000;
        config.index_interval = Duration::from_secs(600);
        config.heartbeat_interval = Duration::from_secs(600);
        let node = TransactorNode::open(config)
            .await
            .expect("open transactor node");

        let open_port = free_port();
        let open_address = format!("127.0.0.1:{open_port}")
            .parse()
            .expect("open address");
        tokio::spawn(corium_transactor::server::serve(
            Arc::clone(&node),
            open_address,
            Guard::disabled(),
            None,
            std::future::pending(),
        ));
        let open_endpoint = format!("http://127.0.0.1:{open_port}");
        wait_for_admin(&open_endpoint).await;

        let mut admin = Admin::connect(&open_endpoint, None, None)
            .await
            .expect("open admin client");
        assert!(
            admin
                .create_database("people", &forms(&schema_edn(&key)))
                .await
                .expect("create people")
        );
        assert!(
            admin
                .create_database(schema::DEFAULT_AUTHZ_DB, &schema::schema_forms())
                .await
                .expect("create authz db")
        );

        let mut writer_config = ConnectConfig::new(&open_endpoint, "people");
        writer_config = writer_config.with_keyring(keyring.clone());
        let writer = LocalPeer::connect(writer_config)
            .await
            .expect("key-holding setup peer");
        writer
            .transact(
                TxBuilder::new()
                    .entity(
                        EntityMap::with_id(tempid("alice"))
                            .set("person/name", "Alice")
                            .set("person/age", 40_i64)
                            .set("person/ssn", SENTINEL),
                    )
                    .build(),
            )
            .await
            .expect("seed protected data");

        let authz =
            Connection::connect(ConnectConfig::new(&open_endpoint, schema::DEFAULT_AUTHZ_DB))
                .await
                .expect("connect to authz db");
        let mut policy = schema::default_permission_forms();
        policy.extend([
            bootstrap::principal_form("alice", Some("oidc"), &[]),
            bootstrap::principal_form("bob", Some("oidc"), &[]),
            bootstrap::principal_form("mallory", Some("oidc"), &[]),
            bootstrap::tuple_form("alice", "writer", "database:people"),
            bootstrap::tuple_form("bob", "viewer", "database:people"),
        ]);
        // The policy names the PII class key, and binds it to `writer`. Bob
        // is a `viewer`: he reads the same database from the same key-holding
        // server and never gets that key.
        policy.extend(forms(&format!(
            r#"[{{:db/id "v-pii" :authz.view/name "pii-reader" :authz.view/key ["{key}"]}}
                {{:db/id "b-pii" :authz.binding/relation "writer"
                  :authz.binding/object "database:people"
                  :authz.binding/view "pii-reader"}}]"#,
        )));
        authz.transact(policy).await.expect("install ReBAC policy");

        let authorizer = Arc::new(SystemDbAuthorizer::with_config(
            Arc::new(corium_transactor::authz::NodePolicySource::new(
                Arc::clone(&node),
                schema::DEFAULT_AUTHZ_DB,
            )),
            AuthzConfig::default(),
        ));
        authorizer.refresh().await.expect("compile ReBAC policy");

        let guarded_port = free_port();
        let guarded_address = format!("127.0.0.1:{guarded_port}")
            .parse()
            .expect("guarded address");
        tokio::spawn(corium_transactor::server::serve(
            Arc::clone(&node),
            guarded_address,
            guard(authorizer),
            None,
            std::future::pending(),
        ));
        let guarded_endpoint = format!("http://127.0.0.1:{guarded_port}");
        wait_for_port(guarded_port).await;

        Self {
            _node: node,
            open_endpoint,
            guarded_endpoint,
            keyring,
            _dir: dir,
        }
    }

    fn direct_config(&self, subject: &str, with_key: bool) -> ConnectConfig {
        let mut config = ConnectConfig::new(&self.guarded_endpoint, "people");
        config.token = Some(jwt(subject));
        config.failover_timeout = Duration::from_secs(2);
        if with_key {
            config = config.with_keyring(self.keyring.clone());
        }
        config
    }

    async fn peer_authorizer(&self) -> Arc<SystemDbAuthorizer> {
        let policy_connection = Arc::new(
            Connection::connect(ConnectConfig::new(
                &self.open_endpoint,
                schema::DEFAULT_AUTHZ_DB,
            ))
            .await
            .expect("peer policy connection"),
        );
        let authorizer = Arc::new(SystemDbAuthorizer::new(Arc::new(
            corium_peer::authz::ConnectionPolicySource::new(policy_connection),
        )));
        authorizer.refresh().await.expect("compile peer policy");
        authorizer
    }
}

async fn wait_for_admin(endpoint: &str) {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        if let Ok(mut admin) = Admin::connect(endpoint, None, None).await
            && admin.list_databases().await.is_ok()
        {
            return;
        }
        assert!(std::time::Instant::now() < deadline, "server never ready");
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
}

async fn wait_for_port(port: u16) {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        if tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .is_ok()
        {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "server never listened"
        );
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
}

fn ssns() -> Query {
    Query::find_coll(var("ssn"))
        .where_(data(var("e"), attr("person/name"), lit("Alice")))
        .and(data(var("e"), attr("person/ssn"), var("ssn")))
}

async fn query_ssn(peer: &impl Peer) -> corium_client::QueryResult {
    peer.db()
        .await
        .expect("database view")
        .query(&ssns(), Args::new())
        .await
        .expect("query protected attribute")
}

#[tokio::test(flavor = "multi_thread")]
async fn direct_peer_real_jwt_authz_and_protected_access() {
    let cluster = Cluster::start().await;

    let alice = LocalPeer::connect(cluster.direct_config("alice", true))
        .await
        .expect("Alice's signed JWT authenticates");
    assert_eq!(
        query_ssn(&alice)
            .await
            .values_as::<String>()
            .expect("Alice sees plaintext"),
        vec![SENTINEL.to_owned()]
    );

    let bob = LocalPeer::connect(cluster.direct_config("bob", false))
        .await
        .expect("Bob's signed JWT authenticates and ReBAC permits reads");
    let redacted = query_ssn(&bob).await;
    assert!(
        redacted.values()[0]
            .to_string()
            .starts_with("#corium/redacted"),
        "a keyless authorized direct peer must see a redacted value: {redacted:?}"
    );
    let denied = bob
        .transact(
            TxBuilder::new()
                .entity(EntityMap::with_id(tempid("bob-write")).set("person/name", "Bob"))
                .build(),
        )
        .await
        .expect_err("Bob is a viewer, not a writer");
    assert!(denied.to_string().contains("permission"), "{denied}");

    let mallory = LocalPeer::connect(cluster.direct_config("mallory", false)).await;
    assert!(
        mallory.is_err(),
        "a valid JWT with no ReBAC tuple must be denied"
    );

    let mut forged = cluster.direct_config("alice", false);
    forged.token = forged.token.map(|token| format!("{token}x"));
    let rejected = match LocalPeer::connect(forged).await {
        Err(error) => error,
        Ok(_) => panic!("a modified JWT signature must be rejected"),
    };
    assert!(
        rejected.to_string().contains("bad JWT signature"),
        "{rejected}"
    );

    let mut wrong_issuer = cluster.direct_config("alice", false);
    wrong_issuer.token = Some(jwt_for("alice", "https://wrong-issuer.example", AUDIENCE));
    let rejected = match LocalPeer::connect(wrong_issuer).await {
        Err(error) => error,
        Ok(_) => panic!("a JWT from the wrong issuer must be rejected"),
    };
    assert!(rejected.to_string().contains("token issuer"), "{rejected}");

    let mut wrong_audience = cluster.direct_config("alice", false);
    wrong_audience.token = Some(jwt_for("alice", ISSUER, "wrong-audience"));
    let rejected = match LocalPeer::connect(wrong_audience).await {
        Err(error) => error,
        Ok(_) => panic!("a JWT for the wrong audience must be rejected"),
    };
    assert!(
        rejected.to_string().contains("token audience"),
        "{rejected}"
    );

    println!(
        "verified: RS256 signature/issuer/audience checks; ReBAC read/write denial; \
         key-holding plaintext and keyless redaction through a direct peer"
    );
}

async fn start_peer_server(cluster: &Cluster, with_key: bool) -> String {
    let mut hosted = ConnectConfig::new(&cluster.open_endpoint, "people");
    if with_key {
        hosted = hosted.with_keyring(cluster.keyring.clone());
    }
    let connection = Arc::new(Connection::connect(hosted).await.expect("hosted peer"));
    let service = PeerServerSvc::new(connection, PeerServerConfig::default())
        .with_guard(guard(cluster.peer_authorizer().await));
    let port = free_port();
    let address = format!("127.0.0.1:{port}").parse().expect("peer address");
    tokio::spawn(corium_peer::server::serve_service(
        service,
        address,
        None,
        std::future::pending(),
    ));
    wait_for_port(port).await;
    format!("http://127.0.0.1:{port}")
}

#[tokio::test(flavor = "multi_thread")]
async fn peer_server_real_jwt_authz_and_per_principal_class_keys() {
    let cluster = Cluster::start().await;
    let keyed_endpoint = start_peer_server(&cluster, true).await;

    let alice = RemotePeer::connect(&keyed_endpoint, "people", Some(jwt("alice")), None)
        .await
        .expect("Alice remote peer");
    assert_eq!(
        query_ssn(&alice)
            .await
            .values_as::<String>()
            .expect("Alice sees plaintext"),
        vec![SENTINEL.to_owned()]
    );

    // Bob queries the same database value on the same key-holding server.
    // The policy grants the PII key to `writer`, and Bob is a `viewer`.
    let bob = RemotePeer::connect(&keyed_endpoint, "people", Some(jwt("bob")), None)
        .await
        .expect("Bob remote peer");
    let bob_ssn = query_ssn(&bob).await;
    let bob_ssn = bob_ssn.values();
    assert!(
        bob_ssn[0].to_string().starts_with("#corium/redacted"),
        "a server key Bob was not granted must not hydrate for him: {}",
        bob_ssn[0]
    );
    let denied = bob
        .transact(
            TxBuilder::new()
                .entity(EntityMap::with_id(tempid("remote-bob")).set("person/name", "Bob"))
                .build(),
        )
        .await
        .expect_err("ReBAC still denies Bob's write");
    assert!(denied.to_string().contains("permission"), "{denied}");

    let forged = RemotePeer::connect(
        &keyed_endpoint,
        "people",
        Some(format!("{}x", jwt("alice"))),
        None,
    )
    .await
    .expect("transport connects before the first authenticated RPC");
    let rejected = forged.db().await.expect("lazy db handle").stats().await;
    let rejected = rejected.expect_err("modified JWT must be rejected");
    assert!(
        rejected.to_string().contains("bad JWT signature"),
        "{rejected}"
    );

    let keyless_endpoint = start_peer_server(&cluster, false).await;
    let keyless_bob = RemotePeer::connect(keyless_endpoint, "people", Some(jwt("bob")), None)
        .await
        .expect("keyless Bob remote peer");
    assert!(
        query_ssn(&keyless_bob).await.values()[0]
            .to_string()
            .starts_with("#corium/redacted")
    );

    println!(
        "verified: RS256 signature/issuer/audience checks; ReBAC read/write denial; \
         per-principal class keys through one key-holding peer server (Alice hydrates, \
         Bob is redacted); a keyless peer server redacts for everyone"
    );
}

struct ConnectionCatalog {
    connection: Arc<Connection>,
}

#[async_trait]
impl DbCatalog for ConnectionCatalog {
    async fn list(&self) -> Result<Vec<String>, CatalogError> {
        Ok(vec!["people".to_owned()])
    }

    async fn db(&self, name: &str) -> Result<Db, CatalogError> {
        if name == "people" {
            Ok(self.connection.db())
        } else {
            Err(CatalogError::NotFound(name.to_owned()))
        }
    }

    /// Every class key this process can resolve. Which of them a given SQL
    /// principal may use is the pgwire front end's decision, not ours.
    async fn hydrator(
        &self,
        name: &str,
    ) -> Result<Arc<corium_db::protect::Hydrator>, CatalogError> {
        if name != "people" {
            return Err(CatalogError::NotFound(name.to_owned()));
        }
        self.connection
            .hydrator()
            .await
            .map_err(|error| CatalogError::Unavailable(error.to_string()))
    }

    async fn transact(
        &self,
        name: &str,
        expected_basis_t: u64,
        forms: Vec<Edn>,
    ) -> Result<CatalogTxResult, CatalogError> {
        if name != "people" {
            return Err(CatalogError::NotFound(name.to_owned()));
        }
        self.connection
            .transact_at(forms, Some(expected_basis_t))
            .await
            .map(|result| CatalogTxResult {
                db_after: result.db_after,
                tempids: result.tempids,
            })
            .map_err(|error| match error {
                PeerError::Rpc(status)
                    if matches!(
                        status.code(),
                        tonic::Code::PermissionDenied | tonic::Code::Unauthenticated
                    ) =>
                {
                    CatalogError::Denied(status.message().to_owned())
                }
                other => CatalogError::Unavailable(other.to_string()),
            })
    }
}

/// Connects a `PostgreSQL` client whose password field carries `token`.
async fn pg_connect(port: u16, user: &str, token: &str) -> tokio_postgres::Client {
    let config = format!("host=127.0.0.1 port={port} user={user} password={token} dbname=people");
    let (client, connection) = tokio_postgres::connect(&config, tokio_postgres::NoTls)
        .await
        .expect("pgwire authenticates the presented token");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

#[tokio::test(flavor = "multi_thread")]
async fn pgwire_maps_sql_clients_onto_corium_principals() {
    let cluster = Cluster::start().await;
    // The catalog's own peer connection holds the class key, exactly the
    // configuration that used to disclose PII to every SQL client.
    let hosted = Arc::new(
        Connection::connect(cluster.direct_config("alice", true))
            .await
            .expect("key-holding catalog connection"),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind pgwire");
    let address = listener.local_addr().expect("pgwire address");
    tokio::spawn(corium_pgwire::serve(
        listener,
        Arc::new(ConnectionCatalog { connection: hosted }),
        PgWireConfig {
            guard: guard(cluster.peer_authorizer().await),
            ..PgWireConfig::default()
        },
        std::future::pending(),
    ));
    wait_for_port(address.port()).await;

    // Alice's own JWT, presented in the password field, makes her a Corium
    // principal — and the policy grants her role the PII class key.
    let alice = pg_connect(address.port(), "alice", &jwt("alice")).await;
    let row = alice
        .query_one(
            "SELECT name, ssn FROM corium.person WHERE name = 'Alice'",
            &[],
        )
        .await
        .expect("Alice reads through pgwire");
    assert_eq!(row.get::<_, String>(0), "Alice");
    assert_eq!(
        row.get::<_, Option<String>>(1),
        Some(SENTINEL.to_owned()),
        "a granted key hydrates a protected column"
    );

    // Bob queries the same server, the same database value, the same column.
    let bob = pg_connect(address.port(), "bob", &jwt("bob")).await;
    let row = bob
        .query_one(
            "SELECT name, ssn FROM corium.person WHERE name = 'Alice'",
            &[],
        )
        .await
        .expect("Bob reads through pgwire");
    assert_eq!(row.get::<_, String>(0), "Alice");
    assert_eq!(
        row.get::<_, Option<String>>(1),
        None,
        "the server holds the key; Bob was not granted it, so the column is NULL"
    );

    // Bob is a viewer, so the pgwire guard refuses his write before it ever
    // reaches the transactor.
    let denied = bob
        .execute(
            "UPDATE corium.person SET age = 41 WHERE name = 'Alice'",
            &[],
        )
        .await
        .expect_err("Bob is a viewer, not a writer");
    let denied_message = denied
        .as_db_error()
        .map(|error| error.message().to_owned())
        .unwrap_or_else(|| denied.to_string());
    assert!(
        denied_message.contains("permission")
            || denied_message.contains("denied")
            || denied_message.contains("no relationship path grants"),
        "{denied_message}"
    );

    // A forged token is rejected at connect time by the same OIDC verifier
    // the gRPC surfaces use.
    let forged = format!(
        "host=127.0.0.1 port={} user=mallory password={}x dbname=people",
        address.port(),
        jwt("mallory")
    );
    assert!(
        tokio_postgres::connect(&forged, tokio_postgres::NoTls)
            .await
            .is_err(),
        "pgwire verifies the token it is handed"
    );

    println!(
        "verified: a SQL client's bearer token becomes a Corium principal; ReBAC decides \
         its reads and writes; per-principal class keys hydrate for Alice and report NULL \
         for Bob through one key-holding pgwire server"
    );
}
