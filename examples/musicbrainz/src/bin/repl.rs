//! `mbrainz-repl` — an interactive Clojurust REPL wired to a running
//! transactor with the `corium.api` client namespace preloaded (aliased `d`)
//! and `conn`/`db` bound to the `MusicBrainz` database, ready for example
//! queries.
//!
//! Each entered line is read and evaluated with the full cljrs client
//! environment; the value of the last form is printed. Because the peer API
//! drives async calls by blocking on the shared runtime, the read/eval loop
//! runs on the main thread (never a runtime worker), matching the pattern the
//! `corium.api` bindings expect.

use std::process::ExitCode;
use std::sync::Arc;

use clap::Parser;
use cljrs_env::env::Env;
use cljrs_value::Value;
use corium_mbrainz::endpoints;
use rustyline::DefaultEditor;
use rustyline::error::ReadlineError;

/// Open a Clojurust query REPL against a corium database.
#[derive(Parser)]
#[command(name = "mbrainz-repl", about)]
struct Args {
    /// Transactor endpoint (`http://host:port` or `host:port`).
    #[arg(long, default_value = "http://127.0.0.1:4334")]
    transactor: String,
    /// Database to connect to.
    #[arg(long, default_value = "mbrainz")]
    db: String,
}

fn main() -> ExitCode {
    // The process links two rustls crypto backends (ring via tonic, aws-lc
    // via the cljrs runtime), so pin ring before any TLS setup — mirroring
    // the `corium` CLI.
    let _ = rustls::crypto::ring::default_provider().install_default();
    match run(&Args::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("mbrainz-repl: {message}");
            ExitCode::FAILURE
        }
    }
}

fn run(args: &Args) -> Result<(), String> {
    let (_endpoint, url) = endpoints(&args.transactor, &args.db)?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .map_err(|error| format!("cannot start async runtime: {error}"))?;

    // cljrs isolates own a per-thread GC heap; register this thread as a
    // mutator and keep the read/eval loop here (block_on is legal off a
    // runtime worker, which is what the `corium.api` bindings need).
    let _mutator = cljrs_gc::register_mutator();
    let globals = corium_cljrs::api::client_env(runtime.handle());

    let mut session = Session { globals };
    session
        .eval(&format!("(def conn (d/connect \"{url}\"))"))
        .map_err(|error| format!("cannot connect to {url}: {error}"))?;
    session
        .eval("(def db (d/db conn))")
        .map_err(|error| format!("cannot read database value: {error}"))?;

    banner(&url);
    repl(&mut session)
}

struct Session {
    globals: Arc<cljrs_env::env::GlobalEnv>,
}

impl Session {
    /// Reads and evaluates every form on the line, returning the last value.
    fn eval(&mut self, source: &str) -> Result<Value, String> {
        let mut parser = cljrs_reader::Parser::new(source.to_owned(), "<mbrainz-repl>".to_owned());
        let forms = parser.parse_all().map_err(|error| format!("{error:?}"))?;
        let mut env = Env::new(Arc::clone(&self.globals), "user");
        let mut last = Value::Nil;
        for form in &forms {
            let _frame = cljrs_gc::push_alloc_frame();
            last =
                cljrs_interp::eval::eval(form, &mut env).map_err(|error| format!("{error:?}"))?;
        }
        Ok(last)
    }
}

fn repl(session: &mut Session) -> Result<(), String> {
    let mut editor = DefaultEditor::new().map_err(|error| error.to_string())?;
    loop {
        match editor.readline("mbrainz=> ") {
            Ok(line) => {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let _ = editor.add_history_entry(line.as_str());
                if matches!(trimmed, ":quit" | ":exit") {
                    return Ok(());
                }
                if trimmed == ":help" {
                    help();
                    continue;
                }
                match session.eval(trimmed) {
                    Ok(value) => println!("{value}"),
                    Err(error) => eprintln!("error: {error}"),
                }
            }
            Err(ReadlineError::Interrupted) => println!("^C (type :quit to exit)"),
            Err(ReadlineError::Eof) => return Ok(()),
            Err(error) => return Err(error.to_string()),
        }
    }
}

fn banner(url: &str) {
    println!("Corium MusicBrainz REPL — Clojurust with corium.api (aliased d).");
    println!("Connected to {url}.  conn = connection, db = current database value.");
    println!("Type :help for example queries, :quit to exit.");
}

fn help() {
    const HELP: &str = r#";; Preloaded: (def conn ...) (def db (d/db conn))
;; Refresh the database value after loads:  (def db (d/sync conn))
;;
;; Count artists:
(d/q '[:find (count ?a) . :where [?a :artist/name]] db)
;;
;; Artists named "Bob Dylan" with their start year:
(d/q '[:find ?name ?year
       :where [?a :artist/name ?name]
              [(clojure.string/starts-with? ?name "Bob")]
              [?a :artist/startYear ?year]] db)
;;
;; Pull a release and its media/tracks by gid lookup ref:
(d/pull db [:release/name {:release/media [:medium/position :medium/trackCount]}]
        [:release/gid #uuid "00000000000000000000000000004001"])
;;
;; Entity map for an artist:
(d/entity db [:artist/gid #uuid "00000000000000000000000000000001"])
;;
;; Time travel — the database as of transaction t:
(d/q '[:find (count ?a) . :where [?a :artist/name]] (d/as-of db 3))"#;
    println!("{HELP}");
}
