//! `corium` — launchers and admin commands for the distributed topology:
//! `transactor`, `peer-server`, `db *`, `console`, backup/restore, `gc`, and `log`.

mod console;
mod metrics_http;
mod pg_catalog;
mod sql;
mod tui;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use clap::{Args, Parser, Subcommand, ValueEnum};
use corium_core::KeywordInterner;
use corium_peer::server::PeerServerConfig;
use corium_peer::{Admin, ConnectConfig, Connection, IndexPolicySettings};
use corium_protocol::auth::{StaticToken, client_tls, server_tls};
use corium_protocol::codec;
use corium_query::edn::{Edn, read_all};
use corium_store::{DbRoot, FsStore, RootStore};
use corium_transactor::StoreSpec;
use corium_transactor::node::{NodeConfig, TransactorNode};

/// Corium database system command line.
#[derive(Parser)]
#[command(name = "corium", version, about)]
struct Cli {
    /// Log rendering for tracing events.
    #[arg(long, global = true, value_enum, default_value_t = LogFormat::Human)]
    log_format: LogFormat,
    #[command(subcommand)]
    command: Command,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum LogFormat {
    Human,
    Json,
}

/// Storage-service backend for a transactor's blobs and roots.
#[derive(Clone, Copy, Debug, Default, ValueEnum)]
enum StoreKind {
    /// In-memory, ephemeral (single process); the whole database is lost on
    /// exit. Useful for demos and tests.
    Mem,
    /// Filesystem under `--data-dir` (blobs, roots, and logs).
    #[default]
    Fs,
    /// `PostgreSQL` for blobs and roots; the log stays on the local filesystem
    /// under `--data-dir`. Requires the `postgres` feature.
    Postgres,
    /// Turso (embeddable `SQLite`) for blobs and roots; the log stays on the
    /// local filesystem under `--data-dir`. Requires the `turso` feature.
    Turso,
    /// S3 (or an S3-compatible service) for blobs and roots; the log stays on
    /// the local filesystem under `--data-dir`. Requires the `s3` feature.
    /// Credentials, region, and endpoint come from the standard AWS
    /// environment (`AWS_ACCESS_KEY_ID`, `AWS_REGION`, `AWS_ENDPOINT_URL`,
    /// etc.).
    S3,
}

/// Client-side connection flags (endpoint, auth, TLS).
#[derive(Args, Clone)]
struct ClientFlags {
    /// Transactor endpoint, e.g. `http://127.0.0.1:4334`. With an HA pair,
    /// pass a comma-separated preference list (active first); connections
    /// fail over across it. Admin commands use the first endpoint.
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
    /// Direct storage backend for snapshot bootstrap. Omit to replay the
    /// transaction log from basis zero.
    #[arg(long, value_enum)]
    peer_store: Option<StoreKind>,
    /// Transactor data directory for `--peer-store fs`, and the default
    /// Turso database location.
    #[arg(long, default_value = "./corium-data")]
    peer_data_dir: PathBuf,
    /// Turso database path for `--peer-store turso`.
    #[arg(long)]
    peer_turso_path: Option<PathBuf>,
    /// `PostgreSQL` connection string for `--peer-store postgres`.
    #[arg(long)]
    peer_postgres_url: Option<String>,
    /// S3 bucket for `--peer-store s3`.
    #[arg(long)]
    peer_s3_bucket: Option<String>,
    /// S3 key prefix for `--peer-store s3` (defaults to the bucket root).
    #[arg(long, default_value = "")]
    peer_s3_prefix: String,
}

impl ClientFlags {
    /// Endpoint preference list parsed from the comma-separated flag.
    fn endpoints(&self) -> Vec<String> {
        self.transactor
            .split(',')
            .map(|endpoint| endpoint.trim().to_owned())
            .filter(|endpoint| !endpoint.is_empty())
            .collect()
    }

    /// First endpoint (admin commands talk to one transactor).
    fn primary(&self) -> String {
        self.endpoints()
            .into_iter()
            .next()
            .unwrap_or_else(|| self.transactor.clone())
    }

    fn tls(&self) -> Result<Option<tonic::transport::ClientTlsConfig>, String> {
        if self.ca.is_none() && self.tls_domain.is_none() {
            return Ok(None);
        }
        client_tls(self.ca.as_deref(), self.tls_domain.as_deref())
            .map(Some)
            .map_err(|error| format!("cannot load CA certificate: {error}"))
    }

    async fn connect_config(&self, db: impl Into<String>) -> Result<ConnectConfig, String> {
        let mut config = ConnectConfig::with_failover(self.endpoints(), db);
        config.token = self.token.clone();
        config.tls = self.tls()?;
        let Some(kind) = self.peer_store else {
            return Ok(config);
        };
        if matches!(kind, StoreKind::Mem) {
            return Err("--peer-store mem cannot be shared across processes".into());
        }
        let spec = store_spec(
            kind,
            &self.peer_data_dir,
            self.peer_turso_path.clone(),
            self.peer_postgres_url.clone(),
            self.peer_s3_bucket.clone(),
            self.peer_s3_prefix.clone(),
        )?;
        let storage = corium_transactor::NodeStore::open_existing(&spec, &self.peer_data_dir)
            .await
            .map_err(|error| format!("cannot open peer storage: {error}"))?;
        Ok(config.with_storage(Arc::new(storage)))
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
        /// Storage-service backend for blobs and roots.
        #[arg(long, value_enum, default_value_t = StoreKind::Fs)]
        store: StoreKind,
        /// Turso database path for `--store turso` (defaults to
        /// `{data_dir}/store.db`).
        #[arg(long)]
        turso_path: Option<PathBuf>,
        /// `PostgreSQL` connection string for `--store postgres`.
        #[arg(long)]
        postgres_url: Option<String>,
        /// S3 bucket for `--store s3`.
        #[arg(long)]
        s3_bucket: Option<String>,
        /// S3 key prefix for `--store s3` (defaults to the bucket root).
        #[arg(long, default_value = "")]
        s3_prefix: String,
        /// Data directory (filesystem store, logs). Ignored by `--store mem`.
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
        /// High-availability mode: stand by (and take over on lease expiry)
        /// when another transactor holds a database's lease, instead of
        /// failing startup; on depose, return to standby instead of exiting.
        #[arg(long)]
        ha: bool,
        /// Client endpoint advertised to peers for lease-holder discovery,
        /// e.g. `http://transactor-a:4334`.
        #[arg(long)]
        advertise: Option<String>,
        /// Interval between background index publications (ms).
        #[arg(long, default_value_t = 5_000)]
        index_interval_ms: u64,
        /// Minimum wait before the next index publication, as a multiple of
        /// the previous publication's duration (0 disables the backoff).
        #[arg(long, default_value_t = 4)]
        index_backoff: u32,
        /// Defer index publication while fewer than this many new datoms are
        /// pending (0 publishes any pending work).
        #[arg(long, default_value_t = 0)]
        index_tail_threshold: u64,
        /// Longest a below-threshold tail defers index publication (ms).
        #[arg(long, default_value_t = 60_000)]
        index_tail_deadline_ms: u64,
        /// Interval between subscription heartbeats (ms).
        #[arg(long, default_value_t = 10_000)]
        heartbeat_ms: u64,
        /// Prometheus HTTP listen address (`/metrics`); disabled when omitted.
        #[arg(long)]
        metrics_listen: Option<SocketAddr>,
        /// Scheduled GC interval (for example `1h`); `off` disables it.
        #[arg(long, default_value = "1h")]
        gc_interval: String,
        /// Retain unreachable blobs for at least this long.
        #[arg(long, default_value = "72h")]
        gc_window: String,
        /// Fuel budget per database-function invocation (execution credits).
        #[arg(long, default_value_t = 1_000_000)]
        db_fn_fuel: u64,
        /// Managed-memory budget per database-function invocation (bytes).
        #[arg(long, default_value_t = 16 * 1024 * 1024)]
        db_fn_memory_bytes: usize,
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
        /// Prometheus HTTP listen address (`/metrics`); disabled when omitted.
        #[arg(long)]
        metrics_listen: Option<SocketAddr>,
        #[command(flatten)]
        client: ClientFlags,
        #[command(flatten)]
        serve: ServeFlags,
    },
    /// Serve the database catalog over the `PostgreSQL` wire protocol
    /// (read-only). Clients pick a database with the startup `database`
    /// parameter or `USE <db>`, and list them with `SHOW DATABASES`.
    PostgresServer {
        /// Restrict the databases clients may reach (repeatable). When
        /// omitted, every database in the transactor's catalog is exposed.
        #[arg(long = "database")]
        databases: Vec<String>,
        /// Listen address.
        #[arg(long, default_value = "127.0.0.1:5432")]
        listen: SocketAddr,
        /// Require this cleartext password from clients (trust when omitted).
        #[arg(long)]
        password: Option<String>,
        #[command(flatten)]
        client: ClientFlags,
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
        /// Retain unreachable blobs newer than this window (offline and online).
        #[arg(long, default_value = "72h")]
        window: String,
    },
    /// Create or incrementally refresh an offline database backup.
    Backup {
        /// Source transactor data directory (transactor must be stopped).
        #[arg(long)]
        data_dir: PathBuf,
        /// Database name.
        db: String,
        /// Backup directory; reusing it performs an incremental backup.
        destination: PathBuf,
    },
    /// Restore a backup, optionally under a new database name (clone).
    Restore {
        /// Backup directory.
        source: PathBuf,
        /// Target transactor data directory (transactor must be stopped).
        #[arg(long)]
        data_dir: PathBuf,
        /// Target database name; may differ from the backed-up source name.
        #[arg(long)]
        as_db: String,
    },
    /// Open an interactive peer-local Datalog console.
    Console {
        /// Database name.
        db: String,
        #[command(flatten)]
        client: ClientFlags,
    },
    /// Open a full-screen terminal dashboard: query workbench, store
    /// metrics, live transaction feed, and schema browser.
    Tui {
        /// Database name.
        db: String,
        /// Metrics refresh interval in milliseconds (minimum 250).
        #[arg(long, default_value_t = 2_000)]
        refresh_ms: u64,
        #[command(flatten)]
        client: ClientFlags,
    },
    /// Open a read-only interactive SQL shell.
    Sql {
        /// Database name.
        db: String,
        /// Execute SQL and exit.
        #[arg(short = 'c', long, conflicts_with = "file")]
        command: Option<String>,
        /// Execute SQL from a file and exit.
        #[arg(short = 'f', long, conflicts_with = "command")]
        file: Option<PathBuf>,
        #[command(flatten)]
        client: ClientFlags,
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
    /// Fork a database: create a new database duplicating an existing one
    /// at a transaction basis (e.g. as a sandbox wound back to a point).
    Fork {
        /// Source database name.
        name: String,
        /// Name for the new fork.
        target: String,
        /// Transaction basis to fork at (defaults to the current basis).
        #[arg(long)]
        as_of: Option<u64>,
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
    /// Ask the transactor to publish the database's indexes now.
    RequestIndex {
        /// Database name.
        name: String,
        #[command(flatten)]
        client: ClientFlags,
    },
    /// Read or override the database's index-publication pacing at runtime.
    ///
    /// Omitted flags are left unchanged; with no flags the current policy
    /// is printed. Overrides last until the transactor restarts.
    IndexPolicy {
        /// Database name.
        name: String,
        /// Base interval between index publications (ms).
        #[arg(long)]
        interval_ms: Option<u64>,
        /// Minimum wait before the next publication, as a multiple of the
        /// previous publication's duration (0 disables the backoff).
        #[arg(long)]
        backoff: Option<u32>,
        /// Defer publication while fewer than this many new datoms are
        /// pending (0 publishes any pending work).
        #[arg(long)]
        tail_threshold: Option<u64>,
        /// Longest a below-threshold tail defers publication (ms).
        #[arg(long)]
        tail_deadline_ms: Option<u64>,
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
    let cli = Cli::parse();
    // The TUI owns the terminal; stray tracing output would corrupt it.
    if !matches!(cli.command, Command::Tui { .. }) {
        init_logging(cli.log_format);
    }
    match run(cli).await {
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
            store,
            turso_path,
            postgres_url,
            s3_bucket,
            s3_prefix,
            data_dir,
            listen,
            owner,
            lease_ttl_ms,
            lease_wait_ms,
            ha,
            advertise,
            index_interval_ms,
            index_backoff,
            index_tail_threshold,
            index_tail_deadline_ms,
            heartbeat_ms,
            metrics_listen,
            gc_interval,
            gc_window,
            db_fn_fuel,
            db_fn_memory_bytes,
            serve,
        } => {
            let store_spec = store_spec(
                store,
                &data_dir,
                turso_path,
                postgres_url,
                s3_bucket,
                s3_prefix,
            )?;
            let mut config = NodeConfig::new(data_dir);
            config.store = store_spec;
            if let Some(owner) = owner {
                config.owner = owner;
            }
            config.lease_ttl_ms = lease_ttl_ms;
            config.lease_wait_ms = lease_wait_ms;
            config.ha = ha;
            config.advertise = advertise;
            config.index_interval = Duration::from_millis(index_interval_ms);
            config.index_backoff = index_backoff;
            config.index_tail_threshold = index_tail_threshold;
            config.index_tail_deadline = Duration::from_millis(index_tail_deadline_ms);
            config.heartbeat_interval = Duration::from_millis(heartbeat_ms);
            config.gc_interval = if gc_interval == "off" {
                None
            } else {
                Some(parse_duration(&gc_interval)?)
            };
            config.gc_retention = parse_duration(&gc_window)?;
            // The built-in `cljrs-tx` runtime is wired by `NodeConfig::new`
            // when the `cljrs` feature is on; apply the flag budgets here.
            #[cfg(feature = "cljrs")]
            {
                config.tx_fn_expander = Some(Arc::new(corium_transactor::txfn::DbFnExpander::new(
                    corium_transactor::txfn::DbFnBudget {
                        fuel: db_fn_fuel,
                        memory_bytes: db_fn_memory_bytes,
                        ..corium_transactor::txfn::DbFnBudget::default()
                    },
                )));
            }
            #[cfg(not(feature = "cljrs"))]
            let _ = (db_fn_fuel, db_fn_memory_bytes);
            let tls = serve.tls()?;
            let authenticator = serve.authenticator();
            let node = TransactorNode::open(config)
                .await
                .map_err(|error| format!("cannot open node: {error}"))?;
            let _metrics = if let Some(address) = metrics_listen {
                let metrics_node = Arc::clone(&node);
                Some(
                    metrics_http::spawn(
                        address,
                        Arc::new(move || metrics_node.metrics().prometheus()),
                    )
                    .await?,
                )
            } else {
                None
            };
            let mut shutdown = node.shutdown_watch();
            tracing::info!(
                %listen,
                databases = ?node.list_dbs(),
                standby = ?node.standby_dbs(),
                "transactor serving"
            );
            eprintln!(
                "corium transactor: serving {:?} (standby for {:?}) on {listen}",
                node.list_dbs(),
                node.standby_dbs()
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
            // Graceful stop: expire held leases so a standby takes over
            // immediately instead of waiting out the TTL.
            node.release_leases().await;
            if let Some(reason) = node.shutdown_watch().borrow().clone() {
                return Err(format!("shut down: {reason}"));
            }
            Ok(())
        }
        Command::PeerServer {
            db,
            listen,
            max_fuel,
            metrics_listen,
            client,
            serve,
        } => {
            let tls = serve.tls()?;
            let authenticator = serve.authenticator();
            let config = client.connect_config(db).await?;
            let connection = Arc::new(
                Connection::connect(config)
                    .await
                    .map_err(|error| format!("cannot connect to transactor: {error}"))?,
            );
            eprintln!(
                "corium peer-server: hosting {:?} on {listen}",
                connection.db_name()
            );
            let service = corium_peer::server::PeerServerSvc::new(
                connection,
                PeerServerConfig {
                    max_fuel,
                    ..PeerServerConfig::default()
                },
            );
            let metrics = service.metrics();
            let _metrics = if let Some(address) = metrics_listen {
                Some(metrics_http::spawn(address, Arc::new(move || metrics.prometheus())).await?)
            } else {
                None
            };
            tracing::info!(%listen, "peer server serving");
            corium_peer::server::serve_service(service, listen, authenticator, tls, async {
                let _ = tokio::signal::ctrl_c().await;
            })
            .await
            .map_err(|error| error.to_string())
        }
        Command::PostgresServer {
            databases,
            listen,
            password,
            client,
        } => {
            let listener = tokio::net::TcpListener::bind(listen)
                .await
                .map_err(|error| format!("cannot bind {listen}: {error}"))?;
            let catalog = Arc::new(pg_catalog::PeerCatalog::new(client, databases));
            let pg_config = corium_pgwire::PgWireConfig {
                password,
                ..corium_pgwire::PgWireConfig::default()
            };
            tracing::info!(%listen, "postgres server serving");
            eprintln!("corium postgres-server: serving the database catalog on {listen}");
            corium_pgwire::serve(listener, catalog, pg_config, async {
                let _ = tokio::signal::ctrl_c().await;
            })
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
            window,
        } => match (data_dir, transactor) {
            (Some(data_dir), None) => {
                let store = FsStore::open(data_dir.join("store"))
                    .map_err(|error| format!("cannot open store: {error}"))?;
                let mut live = Vec::new();
                for root_name in store
                    .list_roots("db:")
                    .await
                    .map_err(|error| error.to_string())?
                {
                    if let Some(root) = store
                        .get_root(&root_name)
                        .await
                        .map_err(|error| error.to_string())?
                        .as_deref()
                        .and_then(DbRoot::decode)
                    {
                        live.extend(root.roots.into_iter().flatten());
                    }
                }
                let report = corium_store::mark_and_sweep_retained(
                    &store,
                    live,
                    |_, bytes| corium_store::index_blob_children(bytes),
                    parse_duration(&window)?,
                    std::time::SystemTime::now(),
                )
                .await
                .map_err(|error| error.to_string())?;
                println!(
                    "{{:marked {} :swept {} :retained {}}}",
                    report.marked, report.swept, report.retained
                );
                Ok(())
            }
            (None, Some(endpoint)) => {
                let flags = ClientFlags {
                    transactor: endpoint,
                    token,
                    ca,
                    tls_domain,
                    peer_store: None,
                    peer_data_dir: PathBuf::from("./corium-data"),
                    peer_turso_path: None,
                    peer_postgres_url: None,
                    peer_s3_bucket: None,
                    peer_s3_prefix: String::new(),
                };
                let mut admin = Admin::connect(&flags.primary(), flags.token.clone(), flags.tls()?)
                    .await
                    .map_err(|error| error.to_string())?;
                let swept = admin
                    .gc_deleted_databases_with_retention(Some(parse_duration(&window)?))
                    .await
                    .map_err(|error| error.to_string())?;
                println!("{{:swept {swept}}}");
                Ok(())
            }
            _ => Err("pass exactly one of --data-dir (offline) or --transactor".into()),
        },
        Command::Backup {
            data_dir,
            db,
            destination,
        } => {
            let report = corium_transactor::backup::backup(data_dir, &db, destination)
                .await
                .map_err(|error| error.to_string())?;
            println!(
                "{{:db {db:?} :basis-t {} :index-basis-t {} :copied-blobs {} :reused-blobs {}}}",
                report.basis_t, report.index_basis_t, report.copied_blobs, report.reused_blobs
            );
            Ok(())
        }
        Command::Restore {
            source,
            data_dir,
            as_db,
        } => {
            let report = corium_transactor::backup::restore(source, data_dir, &as_db)
                .await
                .map_err(|error| error.to_string())?;
            println!(
                "{{:source-db {:?} :db {:?} :basis-t {} :copied-blobs {} :reused-blobs {}}}",
                report.source_db,
                report.target_db,
                report.basis_t,
                report.copied_blobs,
                report.reused_blobs
            );
            Ok(())
        }
        Command::Console { db, client } => {
            let config = client.connect_config(db).await?;
            let connection = Connection::connect(config)
                .await
                .map_err(|error| format!("cannot connect to transactor: {error}"))?;
            console::run(&connection).await
        }
        Command::Tui {
            db,
            refresh_ms,
            client,
        } => {
            let config = client.connect_config(db).await?;
            let connection = Connection::connect(config)
                .await
                .map_err(|error| format!("cannot connect to transactor: {error}"))?;
            tui::run(
                Arc::new(connection),
                Duration::from_millis(refresh_ms.max(250)),
            )
            .await
        }
        Command::Sql {
            db,
            command,
            file,
            client,
        } => {
            let config = client.connect_config(db).await?;
            let connection = Connection::connect(config)
                .await
                .map_err(|error| format!("cannot connect to transactor: {error}"))?;
            sql::run(&connection, command.as_deref(), file.as_deref()).await
        }
        Command::Log {
            data_dir,
            db,
            from,
            to,
        } => run_log(&data_dir, &db, from, to).await,
    }
}

fn init_logging(format: LogFormat) {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    match format {
        LogFormat::Human => {
            let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();
        }
        LogFormat::Json => {
            let _ = tracing_subscriber::fmt()
                .json()
                .with_env_filter(filter)
                .try_init();
        }
    }
}

/// Resolves the `--store` flag and backend connection options into a [`StoreSpec`].
fn store_spec(
    store: StoreKind,
    data_dir: &std::path::Path,
    turso_path: Option<PathBuf>,
    postgres_url: Option<String>,
    s3_bucket: Option<String>,
    s3_prefix: String,
) -> Result<StoreSpec, String> {
    match store {
        StoreKind::Mem => Ok(StoreSpec::Memory),
        StoreKind::Fs => Ok(StoreSpec::Fs),
        StoreKind::Postgres => postgres_spec(postgres_url),
        StoreKind::Turso => turso_spec(data_dir, turso_path),
        StoreKind::S3 => s3_spec(s3_bucket, s3_prefix),
    }
}

#[cfg(feature = "postgres")]
fn postgres_spec(postgres_url: Option<String>) -> Result<StoreSpec, String> {
    let connection_string = postgres_url
        .ok_or_else(|| "--postgres-url is required with --store postgres".to_owned())?;
    Ok(StoreSpec::Postgres { connection_string })
}

#[cfg(not(feature = "postgres"))]
fn postgres_spec(_postgres_url: Option<String>) -> Result<StoreSpec, String> {
    Err(
        "this build lacks the PostgreSQL backend; rebuild corium-cli with --features postgres"
            .into(),
    )
}

#[cfg(feature = "turso")]
fn turso_spec(
    data_dir: &std::path::Path,
    turso_path: Option<PathBuf>,
) -> Result<StoreSpec, String> {
    let path = turso_path.unwrap_or_else(|| data_dir.join("store.db"));
    let path = path
        .to_str()
        .ok_or_else(|| format!("turso path is not valid UTF-8: {}", path.display()))?
        .to_owned();
    Ok(StoreSpec::Turso { path })
}

#[cfg(not(feature = "turso"))]
fn turso_spec(
    _data_dir: &std::path::Path,
    _turso_path: Option<PathBuf>,
) -> Result<StoreSpec, String> {
    Err("this build lacks the Turso backend; rebuild corium-cli with --features turso".into())
}

#[cfg(feature = "s3")]
fn s3_spec(s3_bucket: Option<String>, s3_prefix: String) -> Result<StoreSpec, String> {
    let bucket = s3_bucket.ok_or_else(|| "--s3-bucket is required with --store s3".to_owned())?;
    Ok(StoreSpec::S3 {
        bucket,
        prefix: s3_prefix,
    })
}

#[cfg(not(feature = "s3"))]
fn s3_spec(_s3_bucket: Option<String>, _s3_prefix: String) -> Result<StoreSpec, String> {
    Err("this build lacks the S3 backend; rebuild corium-cli with --features s3".into())
}

fn parse_duration(text: &str) -> Result<Duration, String> {
    let split = text
        .find(|character: char| !character.is_ascii_digit())
        .unwrap_or(text.len());
    let amount: u64 = text[..split]
        .parse()
        .map_err(|_| format!("invalid duration {text:?}"))?;
    let unit = &text[split..];
    let seconds = match unit {
        "ms" => return Ok(Duration::from_millis(amount)),
        "s" | "" => amount,
        "m" => amount.saturating_mul(60),
        "h" => amount.saturating_mul(60 * 60),
        "d" => amount.saturating_mul(24 * 60 * 60),
        _ => {
            return Err(format!(
                "invalid duration unit in {text:?}; use ms, s, m, h, or d"
            ));
        }
    };
    Ok(Duration::from_secs(seconds))
}

async fn admin_client(client: &ClientFlags) -> Result<Admin, String> {
    Admin::connect(&client.primary(), client.token.clone(), client.tls()?)
        .await
        .map_err(|error| error.to_string())
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
            let mut admin = admin_client(&client).await?;
            let created = admin
                .create_database(&name, &forms)
                .await
                .map_err(|error| error.to_string())?;
            println!("{{:db {name:?} :created {created}}}");
            Ok(())
        }
        DbCommand::Delete { name, client } => {
            let mut admin = admin_client(&client).await?;
            let deleted = admin
                .delete_database(&name)
                .await
                .map_err(|error| error.to_string())?;
            println!("{{:db {name:?} :deleted {deleted}}}");
            Ok(())
        }
        DbCommand::Fork {
            name,
            target,
            as_of,
            client,
        } => {
            let mut admin = admin_client(&client).await?;
            let forked = admin
                .fork_database(&name, &target, as_of)
                .await
                .map_err(|error| error.to_string())?;
            match forked {
                Some(basis_t) => println!(
                    "{{:db {target:?} :forked-from {name:?} :basis-t {basis_t} :created true}}"
                ),
                None => println!("{{:db {target:?} :created false}}"),
            }
            Ok(())
        }
        DbCommand::List { client } => {
            let mut admin = admin_client(&client).await?;
            for db in admin
                .list_databases()
                .await
                .map_err(|error| error.to_string())?
            {
                println!("{db}");
            }
            Ok(())
        }
        DbCommand::Stats { name, client } => run_db_stats(name, &client).await,
        DbCommand::RequestIndex { name, client } => {
            let mut admin = admin_client(&client).await?;
            let index_basis_t = admin
                .request_index(&name)
                .await
                .map_err(|error| error.to_string())?;
            println!("{{:db {name:?} :index-basis-t {index_basis_t}}}");
            Ok(())
        }
        DbCommand::IndexPolicy {
            name,
            interval_ms,
            backoff,
            tail_threshold,
            tail_deadline_ms,
            client,
        } => {
            let update = IndexPolicySettings {
                interval_ms,
                backoff,
                tail_threshold,
                tail_deadline_ms,
            };
            run_db_index_policy(&name, update, &client).await
        }
    }
}

async fn run_db_stats(name: String, client: &ClientFlags) -> Result<(), String> {
    let config = client.connect_config(name).await?;
    let connection = Connection::connect(config)
        .await
        .map_err(|error| error.to_string())?;
    let db = connection.sync().await.map_err(|error| error.to_string())?;
    let stats = db.stats();
    let status_response = connection
        .status()
        .await
        .map_err(|error| error.to_string())?;
    println!(
        "{{:basis-t {} :index-basis-t {} :datoms {} :entities {} :attributes {} :index-lag {} :tx-count {} :tx-failures {} :tx-queue-depth {} :gc-runs {} :gc-swept-blobs {}}}",
        db.basis_t(),
        connection.index_basis_t(),
        stats.datoms,
        stats.entities,
        stats.attributes,
        status_response.index_lag,
        status_response.transaction_count,
        status_response.transaction_failure_count,
        status_response.transaction_queue_depth,
        status_response.gc_runs,
        status_response.gc_swept_blobs,
    );
    Ok(())
}

async fn run_db_index_policy(
    name: &str,
    update: IndexPolicySettings,
    client: &ClientFlags,
) -> Result<(), String> {
    let mut admin = admin_client(client).await?;
    let policy = admin
        .set_index_policy(name, update)
        .await
        .map_err(|error| error.to_string())?;
    println!(
        "{{:db {name:?} :interval-ms {} :backoff {} :tail-threshold {} :tail-deadline-ms {}}}",
        policy.interval_ms.unwrap_or_default(),
        policy.backoff.unwrap_or_default(),
        policy.tail_threshold.unwrap_or_default(),
        policy.tail_deadline_ms.unwrap_or_default(),
    );
    Ok(())
}

async fn run_log(data_dir: &std::path::Path, db: &str, from: u64, to: u64) -> Result<(), String> {
    use corium_log::TransactionLog;
    let log = corium_log::VersionedLog::open_read_only(data_dir.join("logs"), db)
        .map_err(|error| format!("cannot open log: {error}"))?;
    // Naming from the meta root makes keyword values readable.
    let interner = match FsStore::open(data_dir.join("store")) {
        Ok(store) => store
            .get_root(&format!("meta:{db}"))
            .await
            .ok()
            .flatten()
            .and_then(|meta| decode_meta_interner(&meta))
            .unwrap_or_default(),
        Err(_) => KeywordInterner::default(),
    };
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
