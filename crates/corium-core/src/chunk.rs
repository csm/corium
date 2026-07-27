//! Content-defined boundaries for covering-index leaf chunks.
//!
//! One rule decides where a sorted covering-index key stream is cut, and both
//! the in-memory segment (`corium-index`) and the published snapshot format
//! (`corium-store`) obey it. That is what makes a segment's leaf *the same
//! run of keys* as a published chunk: an indexing pass re-encodes and
//! re-uploads only the leaves a transaction actually touched, and every other
//! leaf keeps the blob id the previous pass published for it.
//!
//! A boundary is a function of the boundary key alone (plus the clamps), so
//! an unchanged run of keys is cut identically no matter what precedes it.
//! Two consequences the storage design leans on: inserting or removing keys
//! re-chunks only the runs they touch, and a transactor rebuilding a segment
//! from scratch after a restart reproduces the chunks the previous process
//! published instead of re-uploading the whole index.

/// Average number of keys per chunk (must be a power of two: boundaries are
/// taken where a key's hash has this many trailing zero bits).
pub const CHUNK_TARGET_ENTRIES: u64 = 2_048;
/// Minimum keys in a chunk before a content boundary may cut it.
pub const CHUNK_MIN_ENTRIES: usize = 512;
/// Maximum keys in a chunk before an unconditional cut.
pub const CHUNK_MAX_ENTRIES: usize = 8_192;

/// Whether a chunk that ends at `key` and holds `entries` keys — `key`
/// included — must be cut here.
///
/// Callers feed keys in sorted order, counting from the last cut. The result
/// depends only on `key` and the running count, never on the keys before it,
/// which is what keeps chunking incremental.
#[must_use]
pub fn cut_after(key: &[u8], entries: usize) -> bool {
    if entries >= CHUNK_MAX_ENTRIES {
        return true;
    }
    entries >= CHUNK_MIN_ENTRIES
        && boundary_hash(key) % CHUNK_TARGET_ENTRIES == CHUNK_TARGET_ENTRIES - 1
}

fn boundary_hash(key: &[u8]) -> u64 {
    // FNV-1a; any stable hash works, but it must never change: chunk
    // boundaries are part of the published format's sharing behavior.
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in key {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clamps_bound_every_chunk() {
        // Below the minimum nothing is cut, at the maximum everything is.
        assert!(!cut_after(b"any", CHUNK_MIN_ENTRIES - 1));
        assert!(cut_after(b"any", CHUNK_MAX_ENTRIES));
    }

    #[test]
    fn boundaries_depend_only_on_the_key() {
        let selected = (0u64..100_000)
            .map(|n| n.to_be_bytes().to_vec())
            .find(|key| cut_after(key, CHUNK_MIN_ENTRIES))
            .expect("a boundary key exists within the search space");
        // The same key cuts at any count in the unclamped range, so a run of
        // unchanged keys is cut identically wherever it appears.
        for entries in [
            CHUNK_MIN_ENTRIES,
            CHUNK_MIN_ENTRIES + 1,
            CHUNK_MAX_ENTRIES - 1,
        ] {
            assert!(cut_after(&selected, entries));
        }
    }
}
