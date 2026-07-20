//! `mbrainz-load` — a pure-Rust loader that creates the `MusicBrainz` database
//! and schema on a running transactor and streams the example dataset into
//! it. It talks the ordinary peer API, so it works against a transactor
//! backed by any store (`--store mem|fs|turso`).
//!
//! The dataset is a stream of top-level EDN forms (see the crate's
//! `FormReader`): an entity **map** is batched with its neighbours into a
//! transaction, while a **vector** is applied verbatim as one transaction.
//! Large files stream form by form and never load whole into memory.

use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::Parser;
use corium_mbrainz::{FormReader, endpoints};
use corium_peer::{Admin, ConnectConfig, Connection};
use corium_query::edn::{Edn, read_all, read_one};

/// Load the `MusicBrainz` schema and example data into a corium database.
#[derive(Parser)]
#[command(name = "mbrainz-load", about)]
struct Args {
    /// Transactor endpoint (`http://host:port` or `host:port`).
    #[arg(long, default_value = "http://127.0.0.1:4334")]
    transactor: String,
    /// Database name to create and load.
    #[arg(long, default_value = "mbrainz")]
    db: String,
    /// Bearer token, if the transactor requires one.
    #[arg(long)]
    token: Option<String>,
    /// Schema file (a vector of attribute maps).
    #[arg(long, default_value = "examples/musicbrainz/schema.edn")]
    schema: PathBuf,
    /// Optional file of entities to install as one transaction before the
    /// data (e.g. reference/lookup entities the dataset points at).
    #[arg(long)]
    enums: Option<PathBuf>,
    /// Dataset file of EDN transactions/entities to stream in.
    #[arg(long, default_value = "examples/musicbrainz/data/sample.edn")]
    data: PathBuf,
    /// Entity maps grouped into a single transaction.
    #[arg(long, default_value_t = 1000)]
    batch: usize,
    /// Skip schema creation (the database already exists).
    #[arg(long)]
    skip_create: bool,
}

#[tokio::main]
async fn main() -> ExitCode {
    let _ = rustls::crypto::ring::default_provider().install_default();
    match run(Args::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("mbrainz-load: {message}");
            ExitCode::FAILURE
        }
    }
}

async fn run(args: Args) -> Result<(), String> {
    let (endpoint, url) = endpoints(&args.transactor, &args.db)?;

    if args.skip_create {
        println!("skipping schema creation for {:?}", args.db);
    } else {
        let schema_text = std::fs::read_to_string(&args.schema)
            .map_err(|error| format!("cannot read schema {}: {error}", args.schema.display()))?;
        let schema = read_all(&schema_text)
            .map_err(|error| format!("bad schema EDN: {error}"))?
            .into_iter()
            .flat_map(flatten_form)
            .collect::<Vec<_>>();
        let mut admin = Admin::connect(&endpoint, args.token.clone(), None)
            .await
            .map_err(|error| format!("cannot reach transactor at {endpoint}: {error}"))?;
        let created = admin
            .create_database(&args.db, &schema)
            .await
            .map_err(|error| format!("create database failed: {error}"))?;
        println!(
            "database {:?}: {} ({} schema attributes)",
            args.db,
            if created {
                "created"
            } else {
                "already existed"
            },
            schema.len()
        );
    }

    let mut config = ConnectConfig::new(&endpoint, &args.db);
    config.token = args.token.clone();
    let connection = Connection::connect(config)
        .await
        .map_err(|error| format!("cannot connect to {url}: {error}"))?;

    if let Some(enums) = &args.enums {
        let entities = read_forms(enums)?;
        if !entities.is_empty() {
            let count = entities.len();
            connection
                .transact(entities)
                .await
                .map_err(|error| format!("installing reference entities failed: {error}"))?;
            println!("installed {count} reference entities");
        }
    }

    load_data(&connection, &args.data, args.batch).await
}

/// Flattens a top-level form into transactable items: a vector/list spreads
/// into its elements, anything else passes through unchanged.
fn flatten_form(form: Edn) -> Vec<Edn> {
    match form {
        Edn::Vector(items) | Edn::List(items) => items,
        other => vec![other],
    }
}

/// Reads a whole EDN file and flattens it into a single list of items.
fn read_forms(path: &PathBuf) -> Result<Vec<Edn>, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
    Ok(read_all(&text)
        .map_err(|error| format!("bad EDN in {}: {error}", path.display()))?
        .into_iter()
        .flat_map(flatten_form)
        .collect())
}

/// Streams `path` form by form: entity maps are batched, transaction vectors
/// are applied verbatim.
async fn load_data(
    connection: &Connection,
    path: &PathBuf,
    batch_size: usize,
) -> Result<(), String> {
    let file = File::open(path)
        .map_err(|error| format!("cannot open dataset {}: {error}", path.display()))?;
    let reader = FormReader::new(BufReader::with_capacity(1 << 20, file));

    let batch_size = batch_size.max(1);
    let mut batch: Vec<Edn> = Vec::with_capacity(batch_size);
    let mut transactions = 0_u64;
    let mut entities = 0_u64;

    for form in reader {
        let text = form.map_err(|error| format!("reading dataset: {error}"))?;
        let edn = read_one(&text).map_err(|error| format!("bad dataset form: {error}"))?;
        match edn {
            Edn::Vector(items) | Edn::List(items) => {
                // An explicit transaction flushes any pending entity batch
                // first so ordering is preserved, then applies atomically.
                if !batch.is_empty() {
                    let forms = std::mem::take(&mut batch);
                    entities += forms.len() as u64;
                    connection
                        .transact(forms)
                        .await
                        .map_err(|error| tx_error(&error))?;
                    transactions += 1;
                }
                entities += items.len() as u64;
                connection
                    .transact(items)
                    .await
                    .map_err(|error| tx_error(&error))?;
                transactions += 1;
            }
            map @ Edn::Map(_) => {
                batch.push(map);
                if batch.len() >= batch_size {
                    let forms = std::mem::take(&mut batch);
                    entities += forms.len() as u64;
                    connection
                        .transact(forms)
                        .await
                        .map_err(|error| tx_error(&error))?;
                    transactions += 1;
                    if transactions.is_multiple_of(20) {
                        println!("… {transactions} transactions, {entities} entities");
                    }
                }
            }
            other => return Err(format!("dataset form must be a map or vector, got {other}")),
        }
    }
    if !batch.is_empty() {
        entities += batch.len() as u64;
        connection
            .transact(batch)
            .await
            .map_err(|error| tx_error(&error))?;
        transactions += 1;
    }

    let db = connection
        .sync()
        .await
        .map_err(|error| format!("final sync failed: {error}"))?;
    println!(
        "loaded {entities} entities in {transactions} transactions; database basis-t is {}",
        db.basis_t()
    );
    Ok(())
}

fn tx_error(error: &corium_peer::PeerError) -> String {
    format!("transaction failed: {error}")
}
