//! Write leases carried on the database root record
//! (see `docs/design/log-and-transactor.md`).
//!
//! Since format 2, the lease *is* part of the [`DbRoot`] record:
//! `{owner, version, expiry, endpoint}` live next to the index roots, and
//! every mutation — acquisition, renewal, release, index publication — is a
//! CAS on that one record. Acquiring the lease with a new version therefore
//! rewrites the record a deposed transactor would have to CAS against, so a
//! writer that lost ownership always fails its next root CAS: the fencing
//! check and the publication are the same atomic operation, and no separate
//! fence-bump step exists.
//!
//! Pre-M7 deployments kept the lease in a separate `lease:{db}` record;
//! [`acquire`] folds any such record into the root once (respecting its
//! version so fencing never regresses) and deletes it.

use corium_store::{DbRoot, FORMAT_VERSION, RootStore, StoreError, db_root_name};
use thiserror::Error;

/// Root-store key of the pre-format-2 standalone lease record.
#[must_use]
pub fn lease_root(db: &str) -> String {
    format!("lease:{db}")
}

/// A held write lease (the lease fields of the database root).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Lease {
    /// Owning transactor id.
    pub owner: String,
    /// Fencing version; increments on every change of ownership.
    pub version: u64,
    /// Expiry as Unix milliseconds.
    pub expires_unix_ms: i64,
    /// Client endpoint advertised to peers; empty when unadvertised.
    pub endpoint: String,
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

/// Pre-format-2 standalone lease record: `owner\nversion\nexpiry\n`.
fn decode_legacy(bytes: &[u8]) -> Option<(String, u64, i64)> {
    let text = std::str::from_utf8(bytes).ok()?;
    let mut lines = text.lines();
    Some((
        lines.next()?.to_owned(),
        lines.next()?.parse().ok()?,
        lines.next()?.parse().ok()?,
    ))
}

/// The lease view of a stored root: owner, version, expiry.
fn holder(root: Option<&DbRoot>, legacy: Option<&(String, u64, i64)>) -> (String, u64, i64) {
    match root {
        Some(root) if !root.owner.is_empty() => (
            root.owner.clone(),
            root.lease_version,
            root.lease_expires_unix_ms,
        ),
        Some(root) => match legacy {
            Some((owner, version, expiry)) => {
                (owner.clone(), (*version).max(root.lease_version), *expiry)
            }
            None => (String::new(), root.lease_version, 0),
        },
        None => match legacy {
            Some((owner, version, expiry)) => (owner.clone(), *version, *expiry),
            None => (String::new(), 0, 0),
        },
    }
}

/// Acquires (or re-acquires) the write lease for `db` with a CAS on the
/// lease fields of the database root. Acquisition under a new version is
/// itself the fence: any deposed writer's pending root CAS now has stale
/// expected bytes and fails.
///
/// The version increments only when ownership changes hands; a same-owner
/// acquisition keeps its version.
///
/// # Errors
/// Returns [`LeaseError::Held`] while another owner's lease is unexpired.
pub fn acquire(
    store: &dyn RootStore,
    db: &str,
    owner: &str,
    endpoint: &str,
    ttl_ms: i64,
    now_unix_ms: i64,
) -> Result<Lease, LeaseError> {
    let name = db_root_name(db);
    let legacy_name = lease_root(db);
    loop {
        let current_bytes = store.get_root(&name)?;
        let current = current_bytes.as_deref().and_then(DbRoot::decode);
        let legacy = store
            .get_root(&legacy_name)?
            .as_deref()
            .and_then(decode_legacy);
        let (cur_owner, cur_version, cur_expiry) = holder(current.as_ref(), legacy.as_ref());
        let version = if !cur_owner.is_empty() && cur_owner == owner {
            cur_version
        } else if cur_expiry > now_unix_ms {
            return Err(LeaseError::Held {
                owner: cur_owner,
                expires_unix_ms: cur_expiry,
            });
        } else {
            cur_version + 1
        };
        let next = DbRoot {
            format_version: FORMAT_VERSION,
            lease_version: version,
            owner: owner.to_owned(),
            lease_expires_unix_ms: now_unix_ms + ttl_ms,
            owner_endpoint: endpoint.to_owned(),
            index_basis_t: current.as_ref().map_or(0, |root| root.index_basis_t),
            roots: current.as_ref().and_then(|root| root.roots.clone()),
        };
        match store.cas_root(&name, current_bytes.as_deref(), &next.encode()) {
            Ok(()) => {
                if legacy.is_some() {
                    // Folded into the root; fencing now lives there alone.
                    let _ = store.delete_root(&legacy_name);
                }
                return Ok(Lease {
                    owner: owner.to_owned(),
                    version,
                    expires_unix_ms: next.lease_expires_unix_ms,
                    endpoint: endpoint.to_owned(),
                });
            }
            Err(StoreError::CasFailed { .. }) => {}
            Err(error) => return Err(error.into()),
        }
    }
}

/// Renews a held lease; fails with [`LeaseError::Lost`] when ownership has
/// changed hands. Index fields published concurrently under the same lease
/// are preserved (the CAS retries around them).
///
/// # Errors
/// Returns [`LeaseError::Lost`] on any ownership change.
pub fn renew(
    store: &dyn RootStore,
    db: &str,
    held: &Lease,
    ttl_ms: i64,
    now_unix_ms: i64,
) -> Result<Lease, LeaseError> {
    let name = db_root_name(db);
    loop {
        let current_bytes = store.get_root(&name)?;
        let Some(root) = current_bytes.as_deref().and_then(DbRoot::decode) else {
            return Err(LeaseError::Lost);
        };
        if root.owner != held.owner || root.lease_version != held.version {
            return Err(LeaseError::Lost);
        }
        let next = DbRoot {
            lease_expires_unix_ms: now_unix_ms + ttl_ms,
            owner_endpoint: held.endpoint.clone(),
            ..root
        };
        match store.cas_root(&name, current_bytes.as_deref(), &next.encode()) {
            Ok(()) => {
                return Ok(Lease {
                    expires_unix_ms: next.lease_expires_unix_ms,
                    ..held.clone()
                });
            }
            // A concurrent index publication under our own lease raced the
            // CAS; re-read and retry. Genuine loss is caught by the
            // ownership check above.
            Err(StoreError::CasFailed { .. }) => {}
            Err(error) => return Err(error.into()),
        }
    }
}

/// Verifies this writer still owns the lease. Used before *and after* the
/// durable log append: the post-append check guarantees no transaction is
/// acknowledged unless ownership was intact after its record became
/// durable, which is what makes takeover replay complete (see
/// `docs/design/log-and-transactor.md`).
///
/// # Errors
/// Returns [`LeaseError::Lost`] when ownership has changed hands.
pub fn verify(store: &dyn RootStore, db: &str, held: &Lease) -> Result<(), LeaseError> {
    let root = store
        .get_root(&db_root_name(db))?
        .as_deref()
        .and_then(DbRoot::decode);
    match root {
        Some(root) if root.owner == held.owner && root.lease_version == held.version => Ok(()),
        _ => Err(LeaseError::Lost),
    }
}

/// Releases a held lease by expiring it immediately. Ownership changes
/// after release still increment the version (the record remains).
///
/// # Errors
/// Returns [`LeaseError::Lost`] when the lease is no longer held.
pub fn release(store: &dyn RootStore, db: &str, held: &Lease) -> Result<(), LeaseError> {
    let name = db_root_name(db);
    loop {
        let current_bytes = store.get_root(&name)?;
        let Some(root) = current_bytes.as_deref().and_then(DbRoot::decode) else {
            return Err(LeaseError::Lost);
        };
        if root.owner != held.owner || root.lease_version != held.version {
            return Err(LeaseError::Lost);
        }
        let next = DbRoot {
            lease_expires_unix_ms: 0,
            ..root
        };
        match store.cas_root(&name, current_bytes.as_deref(), &next.encode()) {
            Ok(()) => return Ok(()),
            Err(StoreError::CasFailed { .. }) => {}
            Err(error) => return Err(error.into()),
        }
    }
}
