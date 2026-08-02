//! End-to-end coverage of `corium schema update` against a live transactor.
//!
//! Everything here runs the real binary against a real database, so the exit
//! codes, the human report, and the JSON contract are checked as an operator
//! and a script would see them.

use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use corium_peer::{Admin, ConnectConfig, Connection};
use corium_query::edn::{Edn, read_all};

/// The schema the database is created with.
const INSTALLED: &str = r#"
schema-version = 1

[[entity]]
name = "person"

[entity.attributes]
email = "string"
tags = "keyword"
address = "ref"
age = "long"

[[attribute]]
group = "legacy"
name = "import-id"
type = "string"
"#;

/// A desired schema exercising every class the command can report.
const DESIRED: &str = r#"
schema-version = 1

[[entity]]
name = "person"

[entity.attributes]
email = { type = "string", unique = "identity" }
tags = { type = "keyword", many = true }
address = { type = "ref", component = true }
age = { type = "string" }
nickname = "string"
"#;

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind")
        .local_addr()
        .expect("addr")
        .port()
}

/// A transactor child process, killed on drop.
struct TransactorProc {
    child: Child,
    port: u16,
}

impl TransactorProc {
    fn spawn(data_dir: &Path) -> Self {
        let port = free_port();
        let child = Command::new(env!("CARGO_BIN_EXE_corium"))
            .arg("transactor")
            .arg("--data-dir")
            .arg(data_dir)
            .arg("--listen")
            .arg(format!("127.0.0.1:{port}"))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn transactor");
        Self { child, port }
    }

    fn endpoint(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    async fn wait_ready(&self) -> Admin {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if let Ok(mut admin) = Admin::connect(&self.endpoint(), None, None).await
                && admin.list_databases().await.is_ok()
            {
                return admin;
            }
            assert!(Instant::now() < deadline, "transactor never became ready");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

impl Drop for TransactorProc {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// The result of running the CLI.
struct Run {
    code: i32,
    stdout: String,
    stderr: String,
}

fn schema_update(endpoint: &str, schema: &Path, args: &[&str]) -> Run {
    let output = Command::new(env!("CARGO_BIN_EXE_corium"))
        .arg("schema")
        .arg("update")
        .arg("people")
        .arg("--schema")
        .arg(schema)
        .arg("--transactor")
        .arg(endpoint)
        .args(args)
        .output()
        .expect("run corium schema update");
    Run {
        code: output.status.code().expect("exit code"),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    }
}

fn write(dir: &Path, name: &str, contents: &str) -> std::path::PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, contents).expect("write schema file");
    path
}

fn tx(forms: &str) -> Vec<Edn> {
    read_all(forms).expect("transaction EDN")
}

/// Extracts a JSON string field without a JSON parser: the contract is stable
/// enough to read positionally, and this keeps the test dependency-free.
fn json_field<'a>(json: &'a str, name: &str) -> &'a str {
    let key = format!("\"{name}\":\"");
    let start = json.find(&key).unwrap_or_else(|| panic!("{name} in {json}")) + key.len();
    let rest = &json[start..];
    &rest[..rest.find('"').expect("terminated string")]
}

// One sequential scenario against one live database: splitting it would pay
// for several transactors to assert on the same plan.
#[allow(clippy::too_many_lines)]
#[tokio::test(flavor = "multi_thread")]
async fn schema_update_plans_reports_and_refuses_to_apply() {
    let dir = tempfile::tempdir().expect("temp dir");
    let transactor = TransactorProc::spawn(dir.path());
    let mut admin = transactor.wait_ready().await;

    let installed_path = write(dir.path(), "installed.toml", INSTALLED);
    let desired_path = write(dir.path(), "desired.toml", DESIRED);
    let forms = corium_forms::toml_schema::parse_edn(INSTALLED).expect("installed schema parses");
    assert!(
        admin
            .create_database("people", &forms)
            .await
            .expect("create database")
    );

    // Two people share an email, one has several tags, and one is a component
    // reference — so the plan has real counts and a real violation to report.
    let connection = Connection::connect(ConnectConfig::new(transactor.endpoint(), "people"))
        .await
        .expect("connect");
    let people = connection
        .transact(tx(r#"
            {:db/id "a" :person/email "shared@example.com" :person/age 41 :legacy/import-id "x1"}
            {:db/id "b" :person/email "shared@example.com" :person/age 32}
            {:db/id "c" :person/tags :one}
        "#))
        .await
        .expect("transact");
    // Value-position tempids are resolved by the client, so the component
    // reference is asserted once both entities exist.
    let home = people.tempids.get("a").expect("tempid a").raw();
    let resident = people.tempids.get("b").expect("tempid b").raw();
    connection
        .transact(tx(&format!(
            "[:db/add {resident} :person/address {home}]"
        )))
        .await
        .expect("transact reference");

    // 1. The installed schema is its own plan: no changes, exit 0.
    let unchanged = schema_update(&transactor.endpoint(), &installed_path, &[]);
    assert_eq!(unchanged.code, 0, "{unchanged:?}", );
    assert!(
        unchanged.stdout.contains("No changes. Nothing was written."),
        "{}",
        unchanged.stdout
    );

    // `--detailed-exit-code` still reports 0 when nothing would change, so an
    // `&&` chain keeps working.
    let unchanged = schema_update(
        &transactor.endpoint(),
        &installed_path,
        &["--detailed-exit-code"],
    );
    assert_eq!(unchanged.code, 0);

    // 2. The desired schema plans every class.
    let planned = schema_update(&transactor.endpoint(), &desired_path, &[]);
    assert_eq!(planned.code, 0, "{}", planned.stderr);
    let report = &planned.stdout;
    assert!(report.contains("database: people"), "{report}");
    assert!(report.contains("ADDITIVE"), "{report}");
    assert!(
        report.contains("+ :person/nickname string cardinality-one"),
        "{report}"
    );
    assert!(
        report.contains("~ :person/tags cardinality one -> many"),
        "{report}"
    );
    assert!(report.contains("VALIDATE-REINDEX"), "{report}");
    assert!(
        report.contains("~ :person/email unique none -> identity"),
        "{report}"
    );
    // The duplicate email is found, and blocks uniqueness.
    assert!(report.contains("duplicate values: 1"), "{report}");
    assert!(
        report.contains("blocked: duplicate values must be resolved before uniqueness"),
        "{report}"
    );
    assert!(report.contains("[ack: component-enable]"), "{report}");
    assert!(report.contains("live refs: 1"), "{report}");
    assert!(report.contains("DESTRUCTIVE (blocked)"), "{report}");
    assert!(
        report.contains("~ :person/age long -> string"),
        "{report}"
    );
    assert!(
        report.contains("recipe: add :person/age-string"),
        "{report}"
    );
    // The file is partial, so the attribute it omits is reported, not retired.
    assert!(
        report.contains("UNMANAGED") && report.contains(":legacy/import-id"),
        "{report}"
    );
    assert!(report.contains("Nothing was written."), "{report}");

    // 3. `--detailed-exit-code` reports 2 when changes are planned.
    let detailed = schema_update(
        &transactor.endpoint(),
        &desired_path,
        &["--detailed-exit-code"],
    );
    assert_eq!(detailed.code, 2, "{}", detailed.stderr);

    // 4. The JSON contract carries the same plan with stable codes.
    let json_run = schema_update(&transactor.endpoint(), &desired_path, &["--json"]);
    assert_eq!(json_run.code, 0, "{}", json_run.stderr);
    let json = json_run.stdout.trim();
    assert!(json.starts_with("{\"version\":1,"), "{json}");
    assert!(json.contains("\"has_changes\":true"), "{json}");
    assert!(json.contains("\"key\":\":person/nickname#add\""), "{json}");
    assert!(
        json.contains("\"execution_class\":\"destructive\""),
        "{json}"
    );
    assert!(json.contains("\"blocked\":\"unique-duplicates\""), "{json}");
    assert!(json.contains("\"acks\":[\"component-enable\"]"), "{json}");
    assert!(
        json.contains("\"unmanaged\":[\":legacy/import-id\"]"),
        "{json}"
    );
    assert!(json.contains("\"apply\":{\"supported\":false"), "{json}");
    let plan_digest = json_field(json, "plan_digest");
    assert!(plan_digest.starts_with("sha256:"), "{plan_digest}");
    // The human report prints the same digest the JSON does.
    assert!(report.contains(plan_digest), "{report}");

    // 5. `--prune` turns the unmanaged attribute into a retirement, and that
    //    is a different plan.
    let pruned = schema_update(&transactor.endpoint(), &desired_path, &["--json", "--prune"]);
    assert_eq!(pruned.code, 0, "{}", pruned.stderr);
    let pruned_json = pruned.stdout.trim();
    assert!(
        pruned_json.contains("\"key\":\":legacy/import-id#retire\""),
        "{pruned_json}"
    );
    assert!(
        pruned_json.contains("\"acks\":[\"retire-live-attribute\"]"),
        "{pruned_json}"
    );
    assert!(pruned_json.contains("\"unmanaged\":[]"), "{pruned_json}");
    assert_ne!(json_field(pruned_json, "plan_digest"), plan_digest);

    // 6. Applying a stale digest is refused before anything else.
    let stale = schema_update(
        &transactor.endpoint(),
        &desired_path,
        &["--apply", "--plan", "sha256:0", "--json"],
    );
    assert_eq!(stale.code, 1);
    assert!(
        stale.stdout.contains("\"code\":\"plan-mismatch\""),
        "{}",
        stale.stdout
    );

    // 7. Applying the current digest reaches the blocked change, and no
    //    allowance can get past it.
    let blocked = schema_update(
        &transactor.endpoint(),
        &desired_path,
        &[
            "--apply",
            "--plan",
            plan_digest,
            "--allow",
            "validate-reindex",
            "--allow",
            "rewrite",
            "--ack",
            "component-enable",
            "--json",
        ],
    );
    assert_eq!(blocked.code, 1);
    assert!(
        blocked.stdout.contains("\"code\":\"blocked-change\""),
        "{}",
        blocked.stdout
    );

    // 8. `destructive` is not an allowance at all.
    let refused = schema_update(
        &transactor.endpoint(),
        &desired_path,
        &["--allow", "destructive"],
    );
    assert_eq!(refused.code, 1);
    assert!(
        refused
            .stderr
            .contains("destructive changes cannot be allowed"),
        "{}",
        refused.stderr
    );

    // 9. A plan with nothing blocked still cannot be applied in this build,
    //    and says exactly why.
    let additive = write(
        dir.path(),
        "additive.toml",
        &format!("{INSTALLED}\n[[attribute]]\ngroup = \"person\"\nname = \"nickname\"\ntype = \"string\"\n"),
    );
    let additive_json = schema_update(&transactor.endpoint(), &additive, &["--json"]);
    let digest = json_field(additive_json.stdout.trim(), "plan_digest").to_owned();
    let unsupported = schema_update(
        &transactor.endpoint(),
        &additive,
        &["--apply", "--plan", &digest, "--json"],
    );
    assert_eq!(unsupported.code, 1);
    assert!(
        unsupported
            .stdout
            .contains("\"code\":\"apply-unsupported\""),
        "{}",
        unsupported.stdout
    );

    // 10. A malformed file fails with a stable parse code and writes nothing.
    let broken = write(dir.path(), "broken.toml", "schema-version = 99");
    let parse_error = schema_update(&transactor.endpoint(), &broken, &["--json"]);
    assert_eq!(parse_error.code, 1);
    assert!(
        parse_error.stdout.contains("\"code\":\"parse-error\""),
        "{}",
        parse_error.stdout
    );

    // 11. Naming an engine attribute is refused.
    let engine = write(
        dir.path(),
        "engine.edn",
        "{:db/ident :db/txInstant :db/valueType :db.type/string}",
    );
    let engine_error = schema_update(&transactor.endpoint(), &engine, &["--json"]);
    assert_eq!(engine_error.code, 1);
    assert!(
        engine_error.stdout.contains("\"code\":\"plan-error\"")
            && engine_error.stdout.contains(":db/txInstant"),
        "{}",
        engine_error.stdout
    );

    // The database is untouched by every one of the runs above.
    let after = connection.sync().await.expect("sync");
    assert_eq!(after.schema().iter().count(), forms.len() + 1);
}

impl std::fmt::Debug for Run {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "exit {}\nstdout:\n{}\nstderr:\n{}",
            self.code, self.stdout, self.stderr
        )
    }
}
