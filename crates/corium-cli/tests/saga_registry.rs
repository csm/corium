//! End-to-end coverage of `corium saga` against a live transactor.
//!
//! The whole registry surface an operator touches is here — open, list,
//! status, extend, abort — run as the real binary against a real database, so
//! the output an operator reads and a script parses is what is asserted.

use std::net::TcpListener;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use corium_peer::Admin;

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

impl Run {
    fn succeeded(self) -> String {
        assert_eq!(self.code, 0, "stderr: {}", self.stderr);
        self.stdout
    }
}

fn saga(endpoint: &str, args: &[&str]) -> Run {
    let output = Command::new(env!("CARGO_BIN_EXE_corium"))
        .arg("saga")
        .args(args)
        .arg("--transactor")
        .arg(endpoint)
        .output()
        .expect("run corium saga");
    Run {
        code: output.status.code().expect("exit code"),
        stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
    }
}

/// Reads `:saga "…"` out of a rendered registry entry.
fn saga_id(rendered: &str) -> String {
    let start = rendered.find(":saga \"").expect("an id in the output") + ":saga \"".len();
    let rest = &rendered[start..];
    rest[..rest.find('"').expect("terminated id")].to_owned()
}

// One sequential scenario against one live database: each step reads the
// registry the previous one wrote, which is the thing worth testing.
#[tokio::test(flavor = "multi_thread")]
async fn saga_opens_lists_extends_and_aborts() {
    let dir = tempfile::tempdir().expect("temp dir");
    let transactor = TransactorProc::spawn(dir.path());
    let mut admin = transactor.wait_ready().await;
    admin
        .create_database("orders", &[])
        .await
        .expect("create database");
    let endpoint = transactor.endpoint();

    let opened = saga(
        &endpoint,
        &[
            "open",
            "orders",
            "--description",
            "quarterly reconciliation",
            "--owner",
            "alice",
            "--ttl",
            "7d",
            "--reserve",
            ":db/doc",
            "--footprint",
            "1005",
        ],
    )
    .succeeded();
    assert!(opened.contains(":status :db.saga.status/open"), "{opened}");
    assert!(opened.contains(":owner \"alice\""), "{opened}");
    assert!(opened.contains(":reserves 1"), "{opened}");
    assert!(opened.contains(":footprint 1"), "{opened}");
    let id = saga_id(&opened);

    // `list` sees it, and the status filter is a filter.
    let listed = saga(&endpoint, &["list", "orders"]).succeeded();
    assert!(listed.contains(&id), "{listed}");
    assert_eq!(
        saga(&endpoint, &["list", "orders", "--status", "committed"]).succeeded(),
        ""
    );
    let refused = saga(&endpoint, &["list", "orders", "--status", "paused"]);
    assert_ne!(refused.code, 0);
    assert!(
        refused.stderr.contains("unknown saga status"),
        "{refused:?}",
        refused = refused.stderr
    );

    // `status` renders the whole entry, ledger included (it is empty here).
    let status = saga(&endpoint, &["status", "orders", &id]).succeeded();
    assert!(
        status.contains(":description \"quarterly reconciliation\""),
        "{status}"
    );
    assert!(status.contains(":merged-tx nil"), "{status}");

    let extended = saga(&endpoint, &["extend", "orders", &id, "--ttl", "30d"]).succeeded();
    let expiry = |rendered: &str| {
        let start = rendered.find(":expires-at \"").expect("a deadline") + ":expires-at \"".len();
        rendered[start..][..23].to_owned()
    };
    assert!(expiry(&extended) > expiry(&opened), "{extended}");

    let aborted = saga(&endpoint, &["abort", "orders", &id]).succeeded();
    assert!(
        aborted.contains(":status :db.saga.status/aborted"),
        "{aborted}"
    );

    // A second abort reports what the registry says, and fails.
    let again = saga(&endpoint, &["abort", "orders", &id]);
    assert_ne!(again.code, 0);
    assert!(again.stderr.contains("aborted"), "{}", again.stderr);

    // An id that is not in the registry is an error with the id in it.
    let missing = saga(
        &endpoint,
        &["status", "orders", "ffffffffffffffffffffffffffffffff"],
    );
    assert_ne!(missing.code, 0);
    assert!(missing.stderr.contains("no saga"), "{}", missing.stderr);
}
