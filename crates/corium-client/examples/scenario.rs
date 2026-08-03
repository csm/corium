//! Minimal live peer-server scenario for the Rust client.

use corium_client::{Peer, RemotePeer, TxBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = std::env::args().skip(1);
    let endpoint = arguments
        .next()
        .ok_or("usage: scenario PEER_ENDPOINT DATABASE")?;
    let database = arguments
        .next()
        .ok_or("usage: scenario PEER_ENDPOINT DATABASE")?;
    if arguments.next().is_some() {
        return Err("usage: scenario PEER_ENDPOINT DATABASE".into());
    }

    let peer = RemotePeer::connect(&endpoint, &database, None, None).await?;
    let before = peer.db().await?.stats().await?;
    let report = peer.transact(TxBuilder::new().build()).await?;
    let after = report.db_after.stats().await?;
    if report.basis_t <= before.basis_t || after.basis_t != report.basis_t {
        return Err(format!(
            "unexpected basis progression: before={}, report={}, after={}",
            before.basis_t, report.basis_t, after.basis_t
        )
        .into());
    }
    println!(
        "rust client reached {database}: basis {} -> {}",
        before.basis_t, after.basis_t
    );
    Ok(())
}
