//! Chunked index-snapshot encoding.
//!
//! A published covering index is a **manifest** blob naming a sequence of
//! **leaf chunk** blobs; concatenating the chunks in manifest order yields
//! the index's sorted, length-prefixed key stream. Chunk boundaries are
//! content-defined (a function of each boundary key alone), so an unchanged
//! run of keys always produces byte-identical chunks: consecutive
//! publications share every untouched chunk by content id, and only the
//! chunks a change lands in are re-uploaded. Snapshots published before
//! format 3 stored the whole key stream as one flat blob; readers accept
//! both by sniffing the manifest magic.

use crate::{BlobId, StoreError};

/// First line of every index-manifest blob. A flat key-stream blob can
/// never start with these bytes: its first eight bytes are a big-endian key
/// length, and this text decodes to an impossible length.
pub const INDEX_MANIFEST_MAGIC: &str = "corium-index-manifest-v1";

/// Average number of keys per chunk (must be a power of two: boundaries are
/// taken where a key's hash has this many trailing zero bits).
const CHUNK_TARGET_ENTRIES: u64 = 2_048;
/// Minimum keys in a chunk before a content boundary may cut it.
const CHUNK_MIN_ENTRIES: usize = 512;
/// Maximum keys in a chunk before an unconditional cut.
const CHUNK_MAX_ENTRIES: usize = 8_192;

/// Reports whether blob bytes are an index manifest (vs a flat key stream).
#[must_use]
pub fn is_index_manifest(bytes: &[u8]) -> bool {
    bytes.starts_with(INDEX_MANIFEST_MAGIC.as_bytes())
}

/// Encodes a manifest naming `children` leaf chunks in key order.
#[must_use]
pub fn encode_index_manifest(children: &[BlobId]) -> Vec<u8> {
    let mut out = String::from(INDEX_MANIFEST_MAGIC);
    out.push('\n');
    for child in children {
        out.push_str(child.as_str());
        out.push('\n');
    }
    out.into_bytes()
}

/// Decodes a manifest's leaf-chunk ids in key order.
///
/// # Errors
/// Returns an error when the bytes are not a manifest or a child id line is
/// malformed.
pub fn decode_index_manifest(bytes: &[u8]) -> Result<Vec<BlobId>, StoreError> {
    let corrupt = |detail: &str| {
        StoreError::Io(std::io::Error::other(format!(
            "malformed index manifest: {detail}"
        )))
    };
    let text = std::str::from_utf8(bytes).map_err(|_| corrupt("not UTF-8"))?;
    let mut lines = text.lines();
    if lines.next() != Some(INDEX_MANIFEST_MAGIC) {
        return Err(corrupt("missing magic"));
    }
    lines
        .map(|line| BlobId::from_hex(line).ok_or_else(|| corrupt("invalid child id")))
        .collect()
}

/// Returns the blob ids referenced by an index blob: a manifest's children,
/// or nothing for a flat (pre-format-3) key stream. Garbage collection and
/// backup share this walk so reachability can never diverge between them.
///
/// # Errors
/// Returns an error for a malformed manifest.
pub fn index_blob_children(bytes: &[u8]) -> Result<Vec<BlobId>, StoreError> {
    if is_index_manifest(bytes) {
        decode_index_manifest(bytes)
    } else {
        Ok(Vec::new())
    }
}

/// Decodes one `(u64 big-endian length, key)*` chunk or flat segment blob
/// back into its sorted key stream. Inverse of [`chunk_segment_keys`]'
/// per-chunk framing; a pre-format-3 flat snapshot is one such run.
///
/// # Errors
/// Returns an error when the framing is truncated or a length is unrepresentable.
pub fn decode_segment_keys(bytes: &[u8]) -> Result<Vec<Vec<u8>>, StoreError> {
    let truncated = || StoreError::Io(std::io::Error::other("truncated segment"));
    let mut keys = Vec::new();
    let mut input = bytes;
    while !input.is_empty() {
        let (len_bytes, rest) = input.split_at_checked(8).ok_or_else(truncated)?;
        let len = usize::try_from(u64::from_be_bytes(len_bytes.try_into().unwrap_or_default()))
            .map_err(|_| StoreError::Io(std::io::Error::other("segment key too large")))?;
        let (key, rest) = rest.split_at_checked(len).ok_or_else(truncated)?;
        keys.push(key.to_vec());
        input = rest;
    }
    Ok(keys)
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

/// Splits a sorted key stream into encoded leaf chunks.
///
/// Each chunk is a run of `(u64 big-endian length, key)` records — the same
/// framing as a whole pre-format-3 segment, so one decoder reads both.
/// Boundaries fall after keys whose hash selects them (about one in
/// [`CHUNK_TARGET_ENTRIES`]), clamped to at least [`CHUNK_MIN_ENTRIES`] and
/// at most [`CHUNK_MAX_ENTRIES`] keys, and depend only on the boundary key
/// itself: inserting or removing keys re-chunks only the runs they touch.
#[must_use]
pub fn chunk_segment_keys<'a>(keys: impl IntoIterator<Item = &'a [u8]>) -> Vec<Vec<u8>> {
    let mut chunks = Vec::new();
    let mut chunk = Vec::new();
    let mut entries = 0usize;
    for key in keys {
        chunk.extend_from_slice(&(key.len() as u64).to_be_bytes());
        chunk.extend_from_slice(key);
        entries += 1;
        let content_cut = entries >= CHUNK_MIN_ENTRIES
            && boundary_hash(key) % CHUNK_TARGET_ENTRIES == CHUNK_TARGET_ENTRIES - 1;
        if content_cut || entries >= CHUNK_MAX_ENTRIES {
            chunks.push(std::mem::take(&mut chunk));
            entries = 0;
        }
    }
    if !chunk.is_empty() {
        chunks.push(chunk);
    }
    chunks
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::digest;

    fn keys(count: u64) -> Vec<Vec<u8>> {
        (0..count).map(|n| n.to_be_bytes().to_vec()).collect()
    }

    #[test]
    fn manifest_round_trips() {
        let children = vec![digest(b"a"), digest(b"b")];
        let encoded = encode_index_manifest(&children);
        assert!(is_index_manifest(&encoded));
        assert_eq!(decode_index_manifest(&encoded).unwrap(), children);
        assert_eq!(index_blob_children(&encoded).unwrap(), children);
    }

    #[test]
    fn empty_manifest_round_trips() {
        let encoded = encode_index_manifest(&[]);
        assert!(decode_index_manifest(&encoded).unwrap().is_empty());
    }

    #[test]
    fn flat_key_stream_is_not_a_manifest_and_has_no_children() {
        let mut flat = Vec::new();
        flat.extend_from_slice(&4u64.to_be_bytes());
        flat.extend_from_slice(b"key0");
        assert!(!is_index_manifest(&flat));
        assert!(index_blob_children(&flat).unwrap().is_empty());
        assert!(index_blob_children(&[]).unwrap().is_empty());
    }

    #[test]
    fn corrupt_manifest_is_an_error_not_a_flat_blob() {
        let mut encoded = encode_index_manifest(&[digest(b"a")]);
        encoded.extend_from_slice(b"not-a-blob-id\n");
        assert!(decode_index_manifest(&encoded).is_err());
        assert!(index_blob_children(&encoded).is_err());
    }

    #[test]
    fn chunks_concatenate_back_to_the_input() {
        let keys = keys(10_000);
        let chunks = chunk_segment_keys(keys.iter().map(Vec::as_slice));
        assert!(chunks.len() > 1, "10k keys should span several chunks");
        let mut decoded = Vec::new();
        for chunk in &chunks {
            let mut input = chunk.as_slice();
            while !input.is_empty() {
                let (len, rest) = input.split_at(8);
                let len = usize::try_from(u64::from_be_bytes(len.try_into().unwrap())).unwrap();
                let (key, rest) = rest.split_at(len);
                decoded.push(key.to_vec());
                input = rest;
            }
        }
        assert_eq!(decoded, keys);
    }

    #[test]
    fn chunking_is_deterministic() {
        let keys = keys(5_000);
        let first = chunk_segment_keys(keys.iter().map(Vec::as_slice));
        let second = chunk_segment_keys(keys.iter().map(Vec::as_slice));
        assert_eq!(first, second);
    }

    #[test]
    fn empty_input_yields_no_chunks() {
        assert!(chunk_segment_keys(std::iter::empty()).is_empty());
    }

    #[test]
    fn appending_keys_reuses_every_settled_chunk() {
        let original = keys(10_000);
        let extended = keys(10_100);
        let before: Vec<BlobId> = chunk_segment_keys(original.iter().map(Vec::as_slice))
            .iter()
            .map(|chunk| digest(chunk))
            .collect();
        let after: Vec<BlobId> = chunk_segment_keys(extended.iter().map(Vec::as_slice))
            .iter()
            .map(|chunk| digest(chunk))
            .collect();
        // Every chunk except the tail the append landed in is shared.
        let shared = after.iter().filter(|id| before.contains(id)).count();
        assert!(
            shared >= before.len() - 1,
            "expected at least {} shared chunks, found {shared}",
            before.len() - 1
        );
    }

    #[test]
    fn inserting_a_key_rechunks_only_its_neighborhood() {
        let original = keys(10_000);
        let mut modified = original.clone();
        modified.insert(4_000, b"inserted-key".to_vec());
        let before: Vec<BlobId> = chunk_segment_keys(original.iter().map(Vec::as_slice))
            .iter()
            .map(|chunk| digest(chunk))
            .collect();
        let after: Vec<BlobId> = chunk_segment_keys(modified.iter().map(Vec::as_slice))
            .iter()
            .map(|chunk| digest(chunk))
            .collect();
        let fresh = after.iter().filter(|id| !before.contains(id)).count();
        assert!(
            fresh <= 2,
            "one insertion should rewrite at most two chunks, rewrote {fresh} of {}",
            after.len()
        );
    }
}
