// SPDX-License-Identifier: BUSL-1.1

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

/// Apply hash chain enforcement to an INSERT.
///
/// Computes the chain hash, injects `_chain_hash` into the document JSON,
/// and returns the re-encoded document. Updates `chain_hashes` with the new hash.
///
/// `Ok(None)` means the hash chain is not enabled for this collection config.
///
/// A body that will not decode is an error, never an empty document: the chain
/// exists to make tampering detectable, and hashing a substituted empty object
/// would write a link whose hash covers nothing the row actually contains —
/// silently ending the tamper-evidence the feature is the whole point of.
pub fn apply_chain_on_insert(
    chain_hashes: &mut std::collections::HashMap<
        (nodedb_types::DatabaseId, nodedb_types::TenantId, String),
        String,
    >,
    database_id: u64,
    tid: u64,
    collection: &str,
    document_id: &str,
    value: &[u8],
    hash_chain_enabled: bool,
) -> crate::Result<Option<Vec<u8>>> {
    if !hash_chain_enabled {
        return Ok(None);
    }

    let key = (
        nodedb_types::DatabaseId::new(database_id),
        nodedb_types::TenantId::new(tid),
        collection.to_string(),
    );
    let prev_hash = chain_hashes
        .get(&key)
        .map(|s| s.as_str())
        .unwrap_or(GENESIS_HASH);
    let chain_hash = compute_chain_hash(prev_hash, document_id, value);

    let mut doc_json = super::super::doc_format::decode_document(value)?;
    if let Some(obj) = doc_json.as_object_mut() {
        obj.insert(
            "_chain_hash".to_string(),
            serde_json::Value::String(chain_hash.clone()),
        );
    }

    chain_hashes.insert(key, chain_hash);
    Ok(Some(super::super::doc_format::encode_to_msgpack(&doc_json)))
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

    fn chain_map()
    -> std::collections::HashMap<(nodedb_types::DatabaseId, nodedb_types::TenantId, String), String>
    {
        std::collections::HashMap::new()
    }

    /// An unreadable body must fail the insert, never be hashed as an empty
    /// document.
    ///
    /// Substituting `{}` produces a link whose hash covers nothing the row
    /// actually contains, so `verify_chain` keeps passing over a row whose
    /// content is not in the chain at all — the tamper-evidence this module
    /// exists to provide, silently switched off.
    #[test]
    fn an_undecodable_body_fails_the_insert_instead_of_hashing_an_empty_document() {
        let mut chain_hashes = chain_map();
        let mut body =
            nodedb_types::json_to_msgpack(&serde_json::json!({"amount": 10})).expect("encode");
        assert!(
            super::super::super::doc_format::decode_document(&body).is_ok(),
            "baseline body must decode"
        );
        body.push(0xC0);

        let result =
            apply_chain_on_insert(&mut chain_hashes, 1, 1, "ledger", "doc-001", &body, true);
        assert!(result.is_err(), "a body with trailing bytes must fail");
        assert!(
            chain_hashes.is_empty(),
            "a failed insert must not advance the chain head"
        );
    }

    /// The chain head is durable state: a restart must resume the chain where
    /// the previous process left it.
    ///
    /// Before the head was persisted, `chain_hashes` was rebuilt as an empty
    /// map at every boot, so the first row inserted after a restart chained
    /// from `GENESIS_HASH` and `VERIFY_HASH_CHAIN` reported that untampered row
    /// as the broken link. The assertions below pin both halves: the reopened
    /// core carries the pre-restart head (not genesis), and `verify_chain`
    /// walks the whole sequence from genesis straight across the restart
    /// boundary.
    #[test]
    fn the_chain_head_survives_a_restart() {
        use crate::bridge::envelope::Status;
        use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
        use crate::engine::document::store::CollectionConfig;
        use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};
        use nodedb_types::{DatabaseId, Surrogate, TenantId};

        const TID: u64 = 1;
        const COLL: &str = "ledger";

        let db = DatabaseId::DEFAULT;
        let config_key = (db, TenantId::new(TID), COLL.to_string());

        let put_plan = |surrogate: u32, doc_id: &str, body: &[u8]| {
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: nodedb_types::QualifiedCollection::new(db, COLL),
                document_id: doc_id.to_string(),
                value: body.to_vec(),
                surrogate: Surrogate::new(surrogate),
                pk_bytes: doc_id.as_bytes().to_vec(),
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
            })
        };
        let body = |amount: i64| {
            nodedb_types::json_to_msgpack(&serde_json::json!({"amount": amount})).expect("encode")
        };

        // `(surrogate, document_id, body)` in the order they are inserted —
        // rows 1 and 2 before the restart, row 3 after it.
        let rows: Vec<(u32, String, Vec<u8>)> = vec![
            (1, "doc-001".to_string(), body(10)),
            (2, "doc-002".to_string(), body(20)),
            (3, "doc-003".to_string(), body(30)),
        ];

        let dir = tempfile::tempdir().expect("tempdir");
        let task = make_default_task();

        let head_before_restart = {
            let (mut core, _req, _resp) = make_core_with_dir(dir.path());
            let mut config = CollectionConfig::new(COLL);
            config.enforcement.hash_chain = true;
            core.doc_configs.insert(config_key.clone(), config);

            for (surrogate, doc_id, value) in rows.iter().take(2) {
                let resp = core.execute_transaction_batch(
                    &task,
                    TID,
                    &[put_plan(*surrogate, doc_id, value)],
                    &[],
                    None,
                );
                assert_eq!(resp.status, Status::Ok, "pre-restart insert must succeed");
            }
            core.chain_hashes
                .get(&config_key)
                .cloned()
                .expect("two chained inserts must leave a head")
        };
        assert_ne!(
            head_before_restart, GENESIS_HASH,
            "two inserts must have advanced the chain past genesis"
        );

        // Reopen the same directory: a boot on an existing directory must
        // rehydrate every chain head, not reset it to genesis.
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        assert_eq!(
            core.chain_hashes.get(&config_key),
            Some(&head_before_restart),
            "the reopened core must rehydrate the persisted head, not restart at genesis"
        );

        let mut config = CollectionConfig::new(COLL);
        config.enforcement.hash_chain = true;
        core.doc_configs.insert(config_key.clone(), config);

        let (surrogate, doc_id, value) = &rows[2];
        let resp = core.execute_transaction_batch(
            &task,
            TID,
            &[put_plan(*surrogate, doc_id, value)],
            &[],
            None,
        );
        assert_eq!(resp.status, Status::Ok, "post-restart insert must succeed");

        // Every stored row carries the `_chain_hash` its INSERT computed. Feed
        // them to `verify_chain` in insertion order together with the exact
        // bodies that were hashed.
        let stored_hashes: Vec<String> = rows
            .iter()
            .map(|(surrogate, _, _)| {
                let row_key = nodedb_types::StorageKey::for_surrogate(Surrogate::new(*surrogate));
                let stored = core
                    .sparse
                    .get(db.as_u64(), TID, COLL, &row_key)
                    .expect("read back")
                    .expect("row must exist");
                let doc =
                    super::super::super::doc_format::decode_document(&stored).expect("decode");
                doc.get("_chain_hash")
                    .and_then(|v| v.as_str())
                    .expect("every chained row stores its link")
                    .to_string()
            })
            .collect();

        let entries = rows
            .iter()
            .zip(stored_hashes.iter())
            .map(|((_, doc_id, value), hash)| (doc_id.as_str(), value.as_slice(), hash.as_str()));
        let verified = verify_chain(GENESIS_HASH, entries);
        assert_eq!(
            verified,
            Ok(stored_hashes[2].clone()),
            "the chain must verify from genesis across the restart boundary"
        );
    }

    #[test]
    fn a_disabled_chain_is_not_an_error() {
        let mut chain_hashes = chain_map();
        let result = apply_chain_on_insert(
            &mut chain_hashes,
            1,
            1,
            "ledger",
            "doc-001",
            b"anything at all",
            false,
        );
        assert!(matches!(result, Ok(None)));
    }
}
