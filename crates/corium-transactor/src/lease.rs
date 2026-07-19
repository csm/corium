//! Write-lease records with CAS fencing (see `docs/design/log-and-transactor.md`).
//!
//! The lease is a record in the root store: `{owner, version, expiry}`,
//! renewed by CAS. The fencing rule pairs it with the database root: every
//! db-root publication carries the lease version the writer believes it
//! holds, and acquiring a lease with a new version immediately re-publishes
//! the db root under that version (the *fence bump*). A deposed transactor
//! that wakes later always fails its root CAS — its expected bytes are
//! stale — and observes the newer lease version instead of publishing.

use corium_store::{RootStore, StoreError};
use thiserror::Error;

/// Root-store key for a database's lease record.
#[must_use]
pub fn lease_root(db: &str) -> String {
    format!("lease:{db}")
}

/// A write-lease record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Lease {
    /// Owning transactor id.
    pub owner: String,
    /// Fencing version; increments on every change of ownership.
    pub version: u64,
    /// Expiry as Unix milliseconds.
    pub expires_unix_ms: i64,
}

impl Lease {
    /// Encodes the record for the root store.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        format!(
            "{}\n{}\n{}\n",
            self.owner, self.version, self.expires_unix_ms
        )
        .into_bytes()
    }

    /// Decodes a stored record.
    #[must_use]
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let text = std::str::from_utf8(bytes).ok()?;
        let mut lines = text.lines();
        Some(Self {
            owner: lines.next()?.to_owned(),
            version: lines.next()?.parse().ok()?,
            expires_unix_ms: lines.next()?.parse().ok()?,
        })
    }
}

/// Lease acquisition/renewal failure.
#[derive(Debug, Error)]
pub enum LeaseError {
    /// Another owner holds an unexpired lease.
    #[error("lease held by {owner} until {expires_unix_ms}")]
    Held {
        /// Current owner id.
        owner: String,
        /// Expiry of the conflicting lease.
        expires_unix_ms: i64,
    },
    /// This owner no longer holds the lease.
    #[error("lease lost to another owner")]
    Lost,
    /// Root store failure.
    #[error(transparent)]
    Store(#[from] StoreError),
}

/// Acquires (or re-acquires) the write lease for `db`.
///
/// Returns the held lease. The version increments only when ownership
/// changes hands; a same-owner acquisition keeps its version.
///
/// # Errors
/// Returns [`LeaseError::Held`] while another owner's lease is unexpired.
pub async fn acquire(
    store: &dyn RootStore,
    db: &str,
    owner: &str,
    ttl_ms: i64,
    now_unix_ms: i64,
) -> Result<Lease, LeaseError> {
    let name = lease_root(db);
    loop {
        let current = store.get_root(&name).await?;
        let decoded = current.as_deref().and_then(Lease::decode);
        let version = match &decoded {
            Some(lease) if lease.owner == owner => lease.version,
            Some(lease) if lease.expires_unix_ms > now_unix_ms => {
                return Err(LeaseError::Held {
                    owner: lease.owner.clone(),
                    expires_unix_ms: lease.expires_unix_ms,
                });
            }
            Some(lease) => lease.version + 1,
            None => 1,
        };
        let next = Lease {
            owner: owner.to_owned(),
            version,
            expires_unix_ms: now_unix_ms + ttl_ms,
        };
        match store
            .cas_root(&name, current.as_deref(), &next.encode())
            .await
        {
            Ok(()) => return Ok(next),
            Err(StoreError::CasFailed { .. }) => {}
            Err(error) => return Err(error.into()),
        }
    }
}

/// Renews a held lease; fails with [`LeaseError::Lost`] when another owner
/// has taken it (or the stored record no longer matches).
///
/// # Errors
/// Returns [`LeaseError::Lost`] on any ownership change.
pub async fn renew(
    store: &dyn RootStore,
    db: &str,
    held: &Lease,
    ttl_ms: i64,
    now_unix_ms: i64,
) -> Result<Lease, LeaseError> {
    let name = lease_root(db);
    let current = store.get_root(&name).await?;
    if current.as_deref() != Some(held.encode().as_slice()) {
        return Err(LeaseError::Lost);
    }
    let next = Lease {
        owner: held.owner.clone(),
        version: held.version,
        expires_unix_ms: now_unix_ms + ttl_ms,
    };
    match store
        .cas_root(&name, current.as_deref(), &next.encode())
        .await
    {
        Ok(()) => Ok(next),
        Err(StoreError::CasFailed { .. }) => Err(LeaseError::Lost),
        Err(error) => Err(error.into()),
    }
}

/// Releases a held lease by expiring it immediately. Ownership changes
/// after release still increment the version (the record remains).
///
/// # Errors
/// Returns [`LeaseError::Lost`] when the lease is no longer held.
pub async fn release(store: &dyn RootStore, db: &str, held: &Lease) -> Result<(), LeaseError> {
    let name = lease_root(db);
    let current = store.get_root(&name).await?;
    if current.as_deref() != Some(held.encode().as_slice()) {
        return Err(LeaseError::Lost);
    }
    let expired = Lease {
        expires_unix_ms: 0,
        ..held.clone()
    };
    match store
        .cas_root(&name, current.as_deref(), &expired.encode())
        .await
    {
        Ok(()) => Ok(()),
        Err(StoreError::CasFailed { .. }) => Err(LeaseError::Lost),
        Err(error) => Err(error.into()),
    }
}
