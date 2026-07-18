//! `corium` — launchers and admin commands for the distributed topology:
//! `transactor`, `peer-server`, `db *`, `gc`, and `log`.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use clap::{Args, Parser, Subcommand};
use corium_core::KeywordInterner;
use corium_peer::server::PeerServerConfig;
use corium_peer::{Admin, ConnectConfig, Connection};
use corium_protocol::auth::{StaticToken, client_tls, server_tls};
use corium_protocol::codec;
use corium_query::edn::{Edn, read_all};
use corium_store::{DbRoot, FsStore, RootStore, mark_and_sweep};
use corium_transactor::node::{NodeConfig, TransactorNode};

/// Corium database system command line.
#[derive(Parser)]
#[command(name = "corium", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

/// Client-side connection flags (endpoint, auth, TLS).
#[derive(Args, Clone)]
struct ClientFlags {
    /// Transactor endpoint, e.g. `http://127.0.0.1:4334`.
    #[arg(long, default_value = "http://127.0.0.1:4334")]
    transactor: String,
    /// Bearer token for the transactor.
    #[arg(long)]
    token: Option<String>,
    /// PEM file with a CA certificate to trust (enables TLS).
    #[arg(long)]
    ca: Option<PathBuf>,
    /// Domain name expected on the server certificate.
    #[arg(long)]
    tls_domain: Option<String>,
}

impl ClientFlags {
    fn tls(&self) -> Result<Option<tonic::transport::ClientTlsConfig>, String> {
        if self.ca.is_none() && self.tls_domain.is_none() {
            return Ok(None);
        }
        client_tls(self.ca.as_deref(), self.tls_domain.as_deref())
            .map(Some)
            .map_err(|error| format!("cannot load CA certificate: {error}"))
    }
}

/// Server-side TLS/auth flags.
#[derive(Args, Clone)]
struct ServeFlags {
    /// Require this bearer token from clients.
    #[arg(long)]
    serve_token: Option<String>,
    /// PEM certificate chain for TLS.
    #[arg(long, requires = "tls_key")]
    tls_cert: Option<PathBuf>,
    /// PEM private key for TLS.
    #[arg(long, requires = "tls_cert")]
    tls_key: Option<PathBuf>,
}

impl ServeFlags {
    fn tls(&self) -> Result<Option<tonic::transport::ServerTlsConfig>, String> {
        match (&self.tls_cert, &self.tls_key) {
            (Some(cert), Some(key)) => server_tls(cert, key)
                .map(Some)
                .map_err(|error| format!("cannot load TLS identity: {error}")),
            _ => Ok(None),
        }
    }

    fn authenticator(&self) -> Arc<StaticToken> {
        Arc::new(StaticToken::new(self.serve_token.clone()))
    }
}

#[derive(Subcommand)]
enum Command {
    /// Run a transactor process over a data directory.
    Transactor {
        /// Data directory (store, logs).
        #[arg(long)]
        data_dir: PathBuf,
        /// Listen address.
        #[arg(long, default_value = "127.0.0.1:4334")]
        listen: SocketAddr,
        /// Stable owner identity for lease records.
        #[arg(long)]
        owner: Option<String>,
        /// Lease time-to-live in milliseconds.
        #[arg(long, default_value_t = 5_000)]
        lease_ttl_ms: i64,
        /// How long to wait for a held lease before giving up (ms).
        #[arg(long, default_value_t = 15_000)]
        lease_wait_ms: i64,
        /// Interval between background index publications (ms).
        #[arg(long, default_value_t = 5_000)]
        index_interval_ms: u64,
        /// Interval between subscription heartbeats (ms).
        #[arg(long, default_value_t = 10_000)]
        heartbeat_ms: u64,
        /// Fuel budget per database-function invocation (function applications).
        #[arg(long, default_value_t = 1_000_000)]
        db_fn_fuel: u64,
        /// Wall-clock deadline per database-function invocation (ms).
        #[arg(long, default_value_t = 5_000)]
        db_fn_deadline_ms: u64,
        #[command(flatten)]
        serve: ServeFlags,
    },
    /// Run a peer server hosting one database for thin clients.
    PeerServer {
        /// Database to host.
        #[arg(long)]
        db: String,
        /// Listen address.
        #[arg(long, default_value = "127.0.0.1:4336")]
        listen: SocketAddr,
        /// Fuel ceiling per query (datoms touched).
        #[arg(long, default_value_t = 10_000_000)]
        max_fuel: u64,
        #[command(flatten)]
        client: ClientFlags,
        #[command(flatten)]
        serve: ServeFlags,
    },
    /// Database catalog operations.
    #[command(subcommand)]
    Db(DbCommand),
    /// Sweep blobs unreachable from any live database root.
    Gc {
        /// Operate offline over a data directory (transactor must be stopped).
        #[arg(long, conflicts_with = "transactor")]
        data_dir: Option<PathBuf>,
        /// Or ask a running transactor to collect.
        #[arg(long)]
        transactor: Option<String>,
        /// Bearer token for the transactor.
        #[arg(long)]
        token: Option<String>,
        /// PEM file with a CA certificate to trust (enables TLS).
        #[arg(long)]
        ca: Option<PathBuf>,
        /// Domain name expected on the server certificate.
        #[arg(long)]
        tls_domain: Option<String>,
    },
    /// Print committed transactions from a data directory's log.
    Log {
        /// Data directory (store, logs).
        #[arg(long)]
        data_dir: PathBuf,
        /// Database name.
        #[arg(long)]
        db: String,
        /// First transaction to print (inclusive).
        #[arg(long, default_value_t = 0)]
        from: u64,
        /// Last transaction to print (exclusive; 0 = open-ended).
        #[arg(long, default_value_t = 0)]
        to: u64,
    },
}

#[derive(Subcommand)]
enum DbCommand {
    /// Create a database (optionally with an EDN schema file).
    Create {
        /// Database name.
        name: String,
        /// EDN file containing a vector of attribute maps.
        #[arg(long)]
        schema: Option<PathBuf>,
        #[command(flatten)]
        client: ClientFlags,
    },
    /// Delete a database.
    Delete {
        /// Database name.
        name: String,
        #[command(flatten)]
        client: ClientFlags,
    },
    /// List databases.
    List {
        #[command(flatten)]
        client: ClientFlags,
    },
    /// Connect a peer and print database statistics.
    Stats {
        /// Database name.
        name: String,
        #[command(flatten)]
        client: ClientFlags,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    // The binary links two rustls crypto backends (`ring` via tonic, and
    // `aws-lc-rs` transitively through the cljrs runtime), so rustls cannot
    // auto-select a process-level provider; pin `ring` explicitly before any
    // TLS setup.
    let _ = rustls::crypto::ring::default_provider().install_default();
    match run(Cli::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("corium: {message}");
            ExitCode::FAILURE
        }
    }
}

#[allow(clippy::too_many_lines)]
async fn run(cli: Cli) -> Result<(), String> {
    match cli.command {
        Command::Transactor {
            data_dir,
            listen,
            owner,
            lease_ttl_ms,
            lease_wait_ms,
            index_interval_ms,
            heartbeat_ms,
            db_fn_fuel,
            db_fn_deadline_ms,
            serve,
        } => {
            let mut config = NodeConfig::new(data_dir);
            if let Some(owner) = owner {
                config.owner = owner;
            }
            config.lease_ttl_ms = lease_ttl_ms;
            config.lease_wait_ms = lease_wait_ms;
            config.index_interval = Duration::from_millis(index_interval_ms);
            config.heartbeat_interval = Duration::from_millis(heartbeat_ms);
            config.tx_fn_expander = Some(Arc::new(corium_cljrs::dbfn::DbFnExpander::new(
                corium_cljrs::sandbox::SandboxBudget {
                    fuel: db_fn_fuel,
                    deadline: Duration::from_millis(db_fn_deadline_ms),
                    ..corium_cljrs::sandbox::SandboxBudget::default()
                },
            )));
            let tls = serve.tls()?;
            let authenticator = serve.authenticator();
            let node = TransactorNode::open(config)
                .map_err(|error| format!("cannot open node: {error}"))?;
            let mut shutdown = node.shutdown_watch();
            eprintln!(
                "corium transactor: serving {:?} on {listen}",
                node.list_dbs()
            );
            let server = corium_transactor::server::serve(
                Arc::clone(&node),
                listen,
                authenticator,
                tls,
                async move {
                    tokio::select! {
                        _ = tokio::signal::ctrl_c() => {}
                        _ = shutdown.changed() => {}
                    }
                },
            );
            server.await.map_err(|error| error.to_string())?;
            if let Some(reason) = node.shutdown_watch().borrow().clone() {
                return Err(format!("shut down: {reason}"));
            }
            Ok(())
        }
        Command::PeerServer {
            db,
            listen,
            max_fuel,
            client,
            serve,
        } => {
            let tls = serve.tls()?;
            let authenticator = serve.authenticator();
            let mut config = ConnectConfig::new(client.transactor.clone(), db);
            config.token = client.token.clone();
            config.tls = client.tls()?;
            let connection = Arc::new(
                Connection::connect(config)
                    .await
                    .map_err(|error| format!("cannot connect to transactor: {error}"))?,
            );
            eprintln!(
                "corium peer-server: hosting {:?} on {listen}",
                connection.db_name()
            );
            corium_peer::server::serve(
                connection,
                listen,
                authenticator,
                tls,
                PeerServerConfig {
                    max_fuel,
                    ..PeerServerConfig::default()
                },
                async {
                    let _ = tokio::signal::ctrl_c().await;
                },
            )
            .await
            .map_err(|error| error.to_string())
        }
        Command::Db(command) => run_db(command).await,
        Command::Gc {
            data_dir,
            transactor,
            token,
            ca,
            tls_domain,
        } => match (data_dir, transactor) {
            (Some(data_dir), None) => {
                let store = FsStore::open(data_dir.join("store"))
                    .map_err(|error| format!("cannot open store: {error}"))?;
                let mut live = Vec::new();
                for root_name in store.list_roots("db:").map_err(|error| error.to_string())? {
                    if let Some(root) = store
                        .get_root(&root_name)
                        .map_err(|error| error.to_string())?
                        .as_deref()
                        .and_then(DbRoot::decode)
                    {
                        live.extend(root.roots.into_iter().flatten());
                    }
                }
                let report = mark_and_sweep(&store, live, |_, _| Ok(Vec::new()))
                    .map_err(|error| error.to_string())?;
                println!("{{:marked {} :swept {}}}", report.marked, report.swept);
                Ok(())
            }
            (None, Some(endpoint)) => {
                let flags = ClientFlags {
                    transactor: endpoint,
                    token,
                    ca,
                    tls_domain,
                };
                let mut admin =
                    Admin::connect(&flags.transactor, flags.token.clone(), flags.tls()?)
                        .await
                        .map_err(|error| error.to_string())?;
                let swept = admin
                    .gc_deleted_databases()
                    .await
                    .map_err(|error| error.to_string())?;
                println!("{{:swept {swept}}}");
                Ok(())
            }
            _ => Err("pass exactly one of --data-dir (offline) or --transactor".into()),
        },
        Command::Log {
            data_dir,
            db,
            from,
            to,
        } => run_log(&data_dir, &db, from, to),
    }
}

async fn run_db(command: DbCommand) -> Result<(), String> {
    match command {
        DbCommand::Create {
            name,
            schema,
            client,
        } => {
            let forms = match schema {
                Some(path) => {
                    let text = std::fs::read_to_string(&path)
                        .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
                    let mut forms =
                        read_all(&text).map_err(|error| format!("bad schema EDN: {error}"))?;
                    // Accept either one vector of maps or bare maps.
                    if forms.len() == 1 && matches!(forms[0], Edn::Vector(_)) {
                        let Edn::Vector(items) = forms.remove(0) else {
                            unreachable!()
                        };
                        items
                    } else {
                        forms
                    }
                }
                None => Vec::new(),
            };
            let mut admin = Admin::connect(&client.transactor, client.token.clone(), client.tls()?)
                .await
                .map_err(|error| error.to_string())?;
            let created = admin
                .create_database(&name, &forms)
                .await
                .map_err(|error| error.to_string())?;
            println!("{{:db {name:?} :created {created}}}");
            Ok(())
        }
        DbCommand::Delete { name, client } => {
            let mut admin = Admin::connect(&client.transactor, client.token.clone(), client.tls()?)
                .await
                .map_err(|error| error.to_string())?;
            let deleted = admin
                .delete_database(&name)
                .await
                .map_err(|error| error.to_string())?;
            println!("{{:db {name:?} :deleted {deleted}}}");
            Ok(())
        }
        DbCommand::List { client } => {
            let mut admin = Admin::connect(&client.transactor, client.token.clone(), client.tls()?)
                .await
                .map_err(|error| error.to_string())?;
            for db in admin
                .list_databases()
                .await
                .map_err(|error| error.to_string())?
            {
                println!("{db}");
            }
            Ok(())
        }
        DbCommand::Stats { name, client } => {
            let mut config = ConnectConfig::new(client.transactor.clone(), name);
            config.token = client.token.clone();
            config.tls = client.tls()?;
            let connection = Connection::connect(config)
                .await
                .map_err(|error| error.to_string())?;
            let db = connection.sync().await.map_err(|error| error.to_string())?;
            let stats = db.stats();
            println!(
                "{{:basis-t {} :index-basis-t {} :datoms {} :entities {} :attributes {}}}",
                db.basis_t(),
                connection.index_basis_t(),
                stats.datoms,
                stats.entities,
                stats.attributes
            );
            Ok(())
        }
    }
}

fn run_log(data_dir: &std::path::Path, db: &str, from: u64, to: u64) -> Result<(), String> {
    use corium_log::TransactionLog;
    let log = corium_log::FileLog::open(data_dir.join("logs").join(format!("{db}.log")))
        .map_err(|error| format!("cannot open log: {error}"))?;
    // Naming from the meta root makes keyword values readable.
    let interner = FsStore::open(data_dir.join("store"))
        .ok()
        .and_then(|store| store.get_root(&format!("meta:{db}")).ok().flatten())
        .and_then(|meta| decode_meta_interner(&meta))
        .unwrap_or_default();
    let end = if to == 0 { None } else { Some(to) };
    for record in log
        .tx_range(from, end)
        .map_err(|error| format!("cannot read log: {error}"))?
    {
        println!(
            "{{:t {} :tx-instant {} :datoms [",
            record.t, record.tx_instant
        );
        for datom in &record.datoms {
            let value = format_value(&datom.v, &interner);
            println!(
                "  [{} {} {value} {} {}]",
                datom.e.raw(),
                datom.a.raw(),
                datom.tx.sequence(),
                datom.added
            );
        }
        println!("]}}");
    }
    Ok(())
}

fn decode_meta_interner(meta: &[u8]) -> Option<KeywordInterner> {
    let schema_len = usize::try_from(u32::from_be_bytes(meta.get(..4)?.try_into().ok()?)).ok()?;
    let rest = meta.get(4 + schema_len..)?;
    let naming_len = usize::try_from(u32::from_be_bytes(rest.get(..4)?.try_into().ok()?)).ok()?;
    codec::decode_naming(rest.get(4..4 + naming_len)?).ok()
}

fn format_value(value: &corium_core::Value, interner: &KeywordInterner) -> String {
    use corium_core::Value;
    match value {
        Value::Bool(v) => v.to_string(),
        Value::Long(v) => v.to_string(),
        Value::Double(v) => format!("{}", v.0),
        Value::Instant(ms) => format!("#inst {ms}"),
        Value::Uuid(v) => format!("#uuid \"{v:032x}\""),
        Value::Keyword(id) => interner
            .resolve(*id)
            .map_or_else(|| format!("#kw {id}"), ToString::to_string),
        Value::Str(v) => format!("{v:?}"),
        Value::Bytes(bytes) => format!("#bytes[{}]", bytes.len()),
        Value::Ref(e) => format!("#eid {}", e.raw()),
    }
}
