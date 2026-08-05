//! Live embedded-peer scenarios used by the release integration matrix.

use std::sync::Arc;

use corium_client::query::{Query, attr, data, kw, var};
use corium_client::tx::{EntityMap, TxBuilder, tempid};
use corium_client::{Args, LocalPeer, Peer};
use corium_crypt::{KeyId, StaticKeyring};
use corium_peer::ConnectConfig;

const SENTINEL: &str = "scenario-kaleidoscope-pangolin";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args().skip(1);
    let mode = arguments.next().ok_or(
        "usage: local_scenario (peer ENDPOINT DATABASE | protection ENDPOINT DATABASE KEY_URI)",
    )?;
    let endpoint = arguments.next().ok_or("missing transactor endpoint")?;
    let database = arguments.next().ok_or("missing database")?;

    match mode.as_str() {
        "peer" => {
            if arguments.next().is_some() {
                return Err("peer mode accepts exactly ENDPOINT DATABASE".into());
            }
            exercise_peer(&endpoint, &database).await?;
        }
        "protection" => {
            let key_uri = arguments.next().ok_or("protection mode needs KEY_URI")?;
            if arguments.next().is_some() {
                return Err("protection mode accepts exactly ENDPOINT DATABASE KEY_URI".into());
            }
            exercise_protection(&endpoint, &database, key_uri).await?;
        }
        _ => return Err(format!("unknown scenario mode {mode:?}").into()),
    }
    Ok(())
}

async fn exercise_peer(endpoint: &str, database: &str) -> Result<(), Box<dyn std::error::Error>> {
    let peer = LocalPeer::connect(ConnectConfig::new(endpoint, database)).await?;
    let before = peer.db().await?.stats().await?;
    let report = peer
        .transact(
            TxBuilder::new()
                .entity(
                    EntityMap::with_id(tempid("scenario-person"))
                        .set("person/name", "Scenario Person")
                        .set("person/age", 42_i64),
                )
                .build(),
        )
        .await?;
    let after = report.db_after.stats().await?;
    if report.basis_t <= before.basis_t || after.basis_t != report.basis_t {
        return Err(format!(
            "unexpected basis progression: before={}, report={}, after={}",
            before.basis_t, report.basis_t, after.basis_t
        )
        .into());
    }
    println!(
        "embedded peer reached {database}: basis {} -> {}",
        before.basis_t, after.basis_t
    );
    Ok(())
}

#[allow(clippy::too_many_lines)]
async fn exercise_protection(
    endpoint: &str,
    database: &str,
    key_uri: String,
) -> Result<(), Box<dyn std::error::Error>> {
    let key = KeyId::new(key_uri)?;
    let keyring = Arc::new(StaticKeyring::resolve([key])?);
    let writer =
        LocalPeer::connect(ConnectConfig::new(endpoint, database).with_keyring(keyring)).await?;

    writer
        .transact(
            TxBuilder::new()
                .entity(
                    EntityMap::with_id(tempid("alice"))
                        .set("person/name", "Alice")
                        .set("person/ssn", SENTINEL)
                        .set("person/mood", kw("mood/cheerful"))
                        .set("person/salary", 100_000_i64),
                )
                .build(),
        )
        .await?;

    let writer_db = writer.sync().await?;
    let ssn = Query::find_coll(var("ssn"))
        .in_scalar(var("name"))
        .where_(data(var("e"), attr("person/name"), var("name")))
        .and(data(var("e"), attr("person/ssn"), var("ssn")));
    let plaintext = writer_db
        .query(&ssn, Args::new().scalar("Alice"))
        .await?
        .values_as::<String>()?;
    if plaintext != [SENTINEL] {
        return Err(format!("key holder did not recover plaintext: {plaintext:?}").into());
    }

    let reader = LocalPeer::connect(ConnectConfig::new(endpoint, database)).await?;
    let reader_db = reader.sync().await?;

    let redacted = reader_db.query(&ssn, Args::new().scalar("Alice")).await?;
    let redacted_values = redacted.values();
    let Some(value) = redacted_values.first() else {
        return Err("redact policy removed the protected value".into());
    };
    let rendered = value.to_string();
    if !rendered.starts_with("#corium/redacted") || !rendered.contains(":protect/redacting") {
        return Err(format!("redact policy returned {rendered}").into());
    }

    let mood =
        Query::find_coll(var("mood")).where_(data(var("e"), attr("person/mood"), var("mood")));
    if !reader_db
        .query(&mood, Args::new())
        .await?
        .values()
        .is_empty()
    {
        return Err("hide policy exposed a protected datom".into());
    }

    let salary = Query::find_coll(var("salary")).where_(data(
        var("e"),
        attr("person/salary"),
        var("salary"),
    ));
    let error = reader_db
        .query(&salary, Args::new())
        .await
        .expect_err("error policy must reject a keyless read");
    if !error.to_string().contains("not readable without its key") {
        return Err(format!("error policy returned an unexpected error: {error}").into());
    }

    println!("protection policies passed for {database}: keyed read, redact, hide, error");
    Ok(())
}
