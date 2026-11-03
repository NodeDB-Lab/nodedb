// SPDX-License-Identifier: Apache-2.0

//! Integration tests for vector checkpoint at-rest encryption.
//!
//! Tests verify:
//! 1. Encrypted checkpoint framing (`SEGV` magic, ciphertext opacity).
//! 2. Full roundtrip: encrypt → persist → decrypt → verify vectors.
//! 3. Policy enforcement: refuse plaintext when KEK is configured.

use nodedb_vector::{VectorCollection, distance::DistanceMetric, hnsw::HnswParams};
use nodedb_wal::crypto::WalEncryptionKey;

use crate::support::test_memory;

const KEK_BYTES: [u8; 32] = [0x42u8; 32];
const DIM: usize = 4;

fn params() -> HnswParams {
    HnswParams {
        metric: DistanceMetric::L2,
        ..HnswParams::default()
    }
}

fn make_key() -> WalEncryptionKey {
    WalEncryptionKey::from_bytes(&KEK_BYTES).expect("test key creation must succeed")
}

/// Build a small collection with a few vectors.
fn make_collection() -> VectorCollection {
    let mut coll = VectorCollection::new(DIM, params());
    for i in 0u32..10 {
        coll.insert(vec![i as f32, 0.0, 0.0, 0.0]).unwrap();
    }
    coll
}

/// Encrypted checkpoint starts with `SEGV`; raw vector floats not exposed.
#[test]
fn hnsw_checkpoint_encrypted_at_rest() {
    let coll = make_collection();
    let key = make_key();

    let blob = coll.checkpoint_to_bytes(Some(&key)).unwrap();
    assert!(!blob.is_empty(), "encrypted checkpoint must not be empty");

    // File must start with SEGV magic.
    assert_eq!(
        &blob[0..4],
        b"SEGV",
        "encrypted checkpoint must begin with SEGV magic"
    );

    // Plaintext of a known float (vector[5] = [5.0, 0.0, 0.0, 0.0]) must
    // not appear as a raw IEEE 754 LE bytes sequence in the ciphertext.
    let five_f32: [u8; 4] = 5.0f32.to_le_bytes();
    let ciphertext = &blob[nodedb_wal::crypto::SEGMENT_ENVELOPE_PREAMBLE_SIZE..];
    assert!(
        !ciphertext.windows(4).any(|w| w == five_f32),
        "plaintext float bytes (5.0f32) must not appear verbatim in ciphertext"
    );

    // Restore must recover the same vectors.
    let restored = VectorCollection::from_checkpoint(&blob, Some(&key), test_memory())
        .expect("decryption must succeed with correct key");

    assert_eq!(
        restored.len(),
        10,
        "all 10 vectors must be present after decrypt+restore"
    );
    assert_eq!(restored.dim(), DIM);

    // Nearest neighbour to [5.0, 0, 0, 0] must be vector id=5.
    let results = restored.search(&[5.0, 0.0, 0.0, 0.0], 1, 64).unwrap();
    assert!(!results.is_empty(), "search must return a result");
    assert_eq!(
        results[0].id, 5,
        "NN search must find id=5 after encrypted checkpoint roundtrip"
    );
}

/// Loading a plaintext checkpoint when a KEK is configured is rejected.
#[test]
fn hnsw_refuses_plaintext_when_kek_required() {
    let coll = make_collection();

    // Write a plaintext checkpoint (no KEK).
    let plaintext_blob = coll.checkpoint_to_bytes(None).unwrap();
    assert!(
        plaintext_blob.len() >= 4,
        "plaintext checkpoint must have content"
    );
    // Sanity: plaintext does NOT start with SEGV.
    assert_ne!(
        &plaintext_blob[0..4],
        b"SEGV",
        "plaintext checkpoint must not carry SEGV magic"
    );

    // Attempt to load with a KEK — must return a typed error, not succeed.
    let key = make_key();
    let result = VectorCollection::from_checkpoint(&plaintext_blob, Some(&key), test_memory());
    assert!(
        result.is_err(),
        "loading a plaintext checkpoint with a KEK configured must return Err"
    );

    let err = match result {
        Err(e) => e,
        Ok(_) => panic!("expected Err, got Ok"),
    };
    let detail = format!("{err}");
    assert!(
        detail.contains("plaintext") || detail.contains("unencrypted"),
        "error must mention 'plaintext' or 'unencrypted'; got: {detail}"
    );
}
