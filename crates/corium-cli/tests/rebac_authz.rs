//! End-to-end enforcement of the self-hosted `ReBAC` policy on both network
//! surfaces: a real transactor over gRPC, and a peer server hosting one
//! database, each authorizing from the same authorization database.
//!
//! The policy is written the way an operator writes it — `corium authz init`'s
//! schema and default permissions, then relationship tuples — and the
//! assertions are what a caller observes: `PERMISSION_DENIED` from the wire,
//! and a policy change taking effect on a running server without a restart.

use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use corium_authz::{AuthzConfig, SystemDbAuthorizer, bootstrap, schema};
use corium_peer::server::{PeerServerConfig, PeerServerSvc};
use corium_peer::{Admin, ConnectConfig, Connection};
use corium_protocol::authz::{Guard, Principal, StaticTokens};
use corium_protocol::pb;
use corium_protocol::pb::peer_server_server::PeerServer;
use corium_protocol::{authz as protocol_authz, codec};
use corium_query::edn::Edn;
use corium_transactor::node::{NodeConfig, TransactorNode};
use tonic::Request;

const SCHEMA: &str = r"[{:db/ident :person/name :db/valueType :db.type/string
                        :db/unique :db.unique/identity}]";

/// Tokens for the three callers the tests use.
fn tokens() -> StaticTokens {
    StaticTokens::new()
        .with("ops-token", Principal::new("static-token", "operator"))
        .with("alice-token", Principal::new("static-token", "alice"))
        .with("bob-token", Principal::new("static-token", "bob"))
        .with("carol-token", Principal::new("static-token", "carol"))
        .with("mallory-token", Principal::new("static-token", "mallory"))
}

/// Reads EDN written as one vector of forms.
fn forms(text: &str) -> Vec<Edn> {
    match corium_query::edn::read_all(text).expect("EDN parses").pop() {
        Some(Edn::Vector(items)) => items,
        other => panic!("expected one vector, got {other:?}"),
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port()
}

/// A transactor node with an open (unauthorized) endpoint for setup and a
/// guarded endpoint that enforces the authorization database.
struct Cluster {
    node: Arc<TransactorNode>,
    open_endpoint: String,
    guarded_endpoint: String,
    _dir: tempfile::TempDir,
}

impl Cluster {
    async fn start() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut config = NodeConfig::new(dir.path().join("data"));
        config.owner = "rebac-test".into();
        config.lease_ttl_ms = 600_000;
        config.index_interval = Duration::from_secs(600);
        config.heartbeat_interval = Duration::from_secs(600);
        let node = TransactorNode::open(config).await.expect("open node");

        // The open endpoint is how the policy is installed in the first place:
        // the same bootstrap order an operator follows, `authz init` before
        // `--authz-db`.
        let open_port = free_port();
        let open_addr = format!("127.0.0.1:{open_port}").parse().expect("addr");
        tokio::spawn(corium_transactor::server::serve(
            Arc::clone(&node),
            open_addr,
            Guard::disabled(),
            None,
            std::future::pending(),
        ));

        let cluster = Self {
            node,
            open_endpoint: format!("http://127.0.0.1:{open_port}"),
            guarded_endpoint: String::new(),
            _dir: dir,
        };
        cluster.wait_ready(&cluster.open_endpoint).await;
        cluster
    }

    async fn wait_ready(&self, endpoint: &str) {
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        loop {
            if let Ok(mut admin) =
                Admin::connect(endpoint, Some("ops-token".to_owned()), None).await
                && admin.list_databases().await.is_ok()
            {
                return;
            }
            assert!(std::time::Instant::now() < deadline, "server never ready");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Serves the same node behind a guard that authenticates the static
    /// tokens and authorizes from the authz database.
    async fn serve_guarded(&mut self) -> Arc<SystemDbAuthorizer> {
        let authorizer = Arc::new(SystemDbAuthorizer::with_config(
            Arc::new(corium_transactor::authz::NodePolicySource::new(
                Arc::clone(&self.node),
                schema::DEFAULT_AUTHZ_DB,
            )),
            AuthzConfig::default(),
        ));
        authorizer.refresh().await.expect("policy compiles");
        authorizer.spawn_refresh();
        let guard = Guard::new(Arc::new(tokens()), Arc::clone(&authorizer) as Arc<_>);

        let port = free_port();
        let addr = format!("127.0.0.1:{port}").parse().expect("addr");
        tokio::spawn(corium_transactor::server::serve(
            Arc::clone(&self.node),
            addr,
            guard,
            None,
            std::future::pending(),
        ));
        self.guarded_endpoint = format!("http://127.0.0.1:{port}");
        self.wait_ready(&self.open_endpoint).await;
        authorizer
    }

    async fn connect(&self, db: &str) -> Connection {
        Connection::connect(ConnectConfig::new(self.open_endpoint.clone(), db))
            .await
            .expect("peer connects")
    }

    async fn admin(&self, endpoint: &str, token: &str) -> Admin {
        Admin::connect(endpoint, Some(token.to_owned()), None)
            .await
            .expect("admin connects")
    }
}

/// Installs the reserved schema, the default permissions, and the policy the
/// assertions below exercise.
async fn install_policy(cluster: &Cluster) -> Connection {
    let mut admin = cluster.admin(&cluster.open_endpoint, "ops-token").await;
    assert!(
        admin
            .create_database(schema::DEFAULT_AUTHZ_DB, &schema::schema_forms())
            .await
            .expect("create authz db")
    );
    assert!(
        admin
            .create_database("music", &forms(SCHEMA))
            .await
            .expect("create music")
    );

    let authz = cluster.connect(schema::DEFAULT_AUTHZ_DB).await;
    let mut policy: Vec<Edn> = schema::default_permission_forms();
    policy.extend([
        // operator administers everything, as `authz init` grants it.
        bootstrap::tuple_form("operator", "owner", "catalog:*"),
        bootstrap::tuple_form("operator", "owner", "database:*"),
        // alice reads music; bob writes it through the engineering group.
        bootstrap::tuple_form("alice", "viewer", "database:music"),
        bootstrap::tuple_form("group:eng#member", "writer", "database:music"),
        bootstrap::tuple_form("bob", "member", "group:eng"),
        // carol reads through a relation the policy binds a view to.
        bootstrap::tuple_form("carol", "redacted-viewer", "database:music"),
    ]);
    policy.extend(forms(
        r#"[{:db/id "p-redacted" :authz.permission/object-type "database"
             :authz.permission/action "read" :authz.permission/relation ["redacted-viewer"]}
            {:db/id "v-redacted" :authz.view/name "redacted"
             :authz.view/filter-type "attribute-allowlist"
             :authz.view/attribute ["person/name"]}
            {:db/id "b-redacted" :authz.binding/relation "redacted-viewer"
             :authz.binding/object "database:*" :authz.binding/view "redacted"}]"#,
    ));
    authz.transact(policy).await.expect("policy transacts");
    authz
}

#[tokio::test(flavor = "multi_thread")]
async fn transactor_enforces_relationship_policy() {
    let mut cluster = Cluster::start().await;
    let authz = install_policy(&cluster).await;
    cluster.serve_guarded().await;
    let guarded = cluster.guarded_endpoint.clone();

    // The catalog: operator owns it, nobody else has a relation to it.
    let mut ops = cluster.admin(&guarded, "ops-token").await;
    let databases = ops
        .list_databases()
        .await
        .expect("operator lists databases");
    assert!(databases.contains(&"music".to_owned()));
    let mut mallory = cluster.admin(&guarded, "mallory-token").await;
    let denied = mallory
        .list_databases()
        .await
        .expect_err("mallory has no catalog relation");
    assert!(denied.to_string().contains("permission"), "{denied}");

    // Creating a database is an admin action: operator's catalog ownership
    // covers a database that does not exist yet.
    assert!(
        ops.create_database("scratch", &[])
            .await
            .expect("ops creates")
    );
    assert!(
        cluster
            .admin(&guarded, "alice-token")
            .await
            .create_database("alice-db", &[])
            .await
            .is_err(),
        "alice is only a viewer of music"
    );

    // Writes: bob reaches `writer` through group membership, alice does not.
    let bob = Connection::connect({
        let mut config = ConnectConfig::new(guarded.clone(), "music");
        config.token = Some("bob-token".to_owned());
        config
    })
    .await
    .expect("bob connects");
    bob.transact(forms(r#"[{:db/id "p" :person/name "Ada"}]"#))
        .await
        .expect("bob writes music");

    let alice = Connection::connect({
        let mut config = ConnectConfig::new(guarded.clone(), "music");
        config.token = Some("alice-token".to_owned());
        config
    })
    .await
    .expect("alice connects");
    let refused = alice
        .transact(forms(r#"[{:db/id "p" :person/name "Grace"}]"#))
        .await
        .expect_err("alice may not write music");
    assert!(refused.to_string().contains("permission"), "{refused}");

    // A policy change reaches the running server without a restart: the
    // transactor's basis watch wakes the refresh task, which recompiles and
    // swaps the snapshot in.
    authz
        .transact(vec![bootstrap::tuple_form(
            "alice",
            "writer",
            "database:music",
        )])
        .await
        .expect("grant applies");
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let attempt = alice
            .transact(forms(r#"[{:db/id "p" :person/name "Grace"}]"#))
            .await;
        if attempt.is_ok() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the new grant never took effect: {attempt:?}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn peer_server_enforces_relationship_policy() {
    let cluster = Cluster::start().await;
    install_policy(&cluster).await;

    // The peer server reads policy over its own connection to the authz
    // database, exactly as `corium peer-server --authz-db` wires it.
    let authorizer = Arc::new(SystemDbAuthorizer::new(Arc::new(
        corium_peer::authz::ConnectionPolicySource::new(Arc::new(
            cluster.connect(schema::DEFAULT_AUTHZ_DB).await,
        )),
    )));
    authorizer.refresh().await.expect("policy compiles");
    let guard = Guard::new(Arc::new(tokens()), authorizer);
    let service = PeerServerSvc::new(
        Arc::new(cluster.connect("music").await),
        PeerServerConfig::default(),
    )
    .with_guard(guard);

    let query = |principal: Principal| {
        // The interceptor attaches the principal; calling the handler directly
        // exercises the same handler-side authorization it feeds.
        let mut request = Request::new(pb::QueryRequest {
            dbs: vec![pb::DbViewSpec {
                db: "music".to_owned(),
                view: None,
            }],
            query: codec::encode_edn(
                &corium_query::edn::read_one("[:find ?e :where [?e :person/name]]")
                    .expect("query parses"),
            ),
            args: codec::encode_edn(&Edn::Vector(Vec::new())),
            fuel: 0,
        });
        request.extensions_mut().insert(principal);
        request
    };

    assert!(
        service
            .query(query(Principal::new("static-token", "alice")))
            .await
            .is_ok(),
        "alice is a viewer of music"
    );
    let denied = service
        .query(query(Principal::new("static-token", "mallory")))
        .await
        .err()
        .expect("mallory has no relation to music");
    assert_eq!(denied.code(), tonic::Code::PermissionDenied);
    // An anonymous caller — what an unauthenticated request becomes — is
    // denied by policy rather than by the absence of a credential.
    let anonymous = service
        .query(query(protocol_authz::Principal::anonymous()))
        .await
        .err()
        .expect("anonymous has no relation to music");
    assert_eq!(anonymous.code(), tonic::Code::PermissionDenied);

    // carol's read is allowed *through a view*, and no read path applies one
    // yet. The surface refuses rather than returning unfiltered data — the
    // fail-safe that keeps a bound view from silently doing nothing.
    let filtered = service
        .query(query(Principal::new("static-token", "carol")))
        .await
        .err()
        .expect("a filtered decision is not servable yet");
    assert_eq!(filtered.code(), tonic::Code::Unimplemented);
}
