//! SHA-256 hash chain computation for append-only collections with HASH_CHAIN.
//!
//! Each INSERT computes `SHA-256(previous_hash || row_id || row_contents)`.
//! The resulting hash is stored alongside the document and the `last_chain_hash`
//! on the collection config is updated atomically.

use sha2::{Digest, Sha256};

/// Encode bytes as lowercase hex string (avoids external `hex` crate dependency).
fn encode_hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// The zero hash used as the initial `previous_hash` for the first entry in a chain.
pub const GENESIS_HASH: &str = "0000000000000000000000000000000000000000000000000000000000000000";

/// Compute the next hash in the chain.
///
/// `hash = SHA-256(previous_hash || row_id || row_contents)`
///
/// All inputs are concatenated as raw bytes with length-prefix framing to
/// prevent ambiguity (e.g. id="ab" + content="cd" vs id="abc" + content="d").
pub fn compute_chain_hash(previous_hash: &str, row_id: &str, row_contents: &[u8]) -> String {
    let mut hasher = Sha256::new();

    // Length-prefix framing: prevents collision between different field boundaries.
    let prev_bytes = previous_hash.as_bytes();
    hasher.update((prev_bytes.len() as u32).to_le_bytes());
    hasher.update(prev_bytes);

    let id_bytes = row_id.as_bytes();
    hasher.update((id_bytes.len() as u32).to_le_bytes());
    hasher.update(id_bytes);

    hasher.update((row_contents.len() as u32).to_le_bytes());
    hasher.update(row_contents);

    encode_hex(&hasher.finalize())
}

/// Verify a segment of the hash chain.
///
/// Takes an iterator of `(row_id, row_contents, stored_hash)` tuples in
/// insertion order. Returns `Ok(last_hash)` if the chain is valid, or
/// `Err((index, expected, actual))` on the first broken link.
///
/// `initial_hash` is the hash of the entry immediately before the range.
/// Use [`GENESIS_HASH`] if verifying from the beginning.
pub fn verify_chain<'a>(
    initial_hash: &str,
    entries: impl Iterator<Item = (&'a str, &'a [u8], &'a str)>,
) -> Result<String, (usize, String, String)> {
    let mut prev = initial_hash.to_string();

    for (i, (row_id, contents, stored_hash)) in entries.enumerate() {
        let expected = compute_chain_hash(&prev, row_id, contents);
        if expected != stored_hash {
            return Err((i, expected, stored_hash.to_string()));
        }
        prev = expected;
    }

    Ok(prev)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn genesis_chain() {
        let h1 = compute_chain_hash(GENESIS_HASH, "doc-001", b"hello");
        assert_eq!(h1.len(), 64); // SHA-256 hex = 64 chars
        assert_ne!(h1, GENESIS_HASH);
    }

    #[test]
    fn chain_is_deterministic() {
        let h1 = compute_chain_hash(GENESIS_HASH, "doc-001", b"hello");
        let h2 = compute_chain_hash(GENESIS_HASH, "doc-001", b"hello");
        assert_eq!(h1, h2);
    }

    #[test]
    fn different_content_different_hash() {
        let h1 = compute_chain_hash(GENESIS_HASH, "doc-001", b"hello");
        let h2 = compute_chain_hash(GENESIS_HASH, "doc-001", b"world");
        assert_ne!(h1, h2);
    }

    #[test]
    fn different_id_different_hash() {
        let h1 = compute_chain_hash(GENESIS_HASH, "doc-001", b"hello");
        let h2 = compute_chain_hash(GENESIS_HASH, "doc-002", b"hello");
        assert_ne!(h1, h2);
    }

    #[test]
    fn chain_links() {
        let h1 = compute_chain_hash(GENESIS_HASH, "doc-001", b"first");
        let h2 = compute_chain_hash(&h1, "doc-002", b"second");
        let h3 = compute_chain_hash(&h2, "doc-003", b"third");

        // Verify the full chain.
        let entries = vec![
            ("doc-001", b"first".as_slice(), h1.as_str()),
            ("doc-002", b"second".as_slice(), h2.as_str()),
            ("doc-003", b"third".as_slice(), h3.as_str()),
        ];
        let result = verify_chain(GENESIS_HASH, entries.into_iter());
        assert_eq!(result, Ok(h3));
    }

    #[test]
    fn broken_chain_detected() {
        let h1 = compute_chain_hash(GENESIS_HASH, "doc-001", b"first");
        let h2 = compute_chain_hash(&h1, "doc-002", b"second");

        // Tamper with h2.
        let entries = vec![
            ("doc-001", b"first".as_slice(), h1.as_str()),
            ("doc-002", b"second".as_slice(), "tampered_hash"),
        ];
        let result = verify_chain(GENESIS_HASH, entries.into_iter());
        assert!(result.is_err());
        let (idx, expected, actual) = result.unwrap_err();
        assert_eq!(idx, 1);
        assert_eq!(expected, h2);
        assert_eq!(actual, "tampered_hash");
    }

    #[test]
    fn length_prefix_prevents_collision() {
        // id="ab" + content="cd" should differ from id="abc" + content="d"
        let h1 = compute_chain_hash(GENESIS_HASH, "ab", b"cd");
        let h2 = compute_chain_hash(GENESIS_HASH, "abc", b"d");
        assert_ne!(h1, h2);
    }
}
