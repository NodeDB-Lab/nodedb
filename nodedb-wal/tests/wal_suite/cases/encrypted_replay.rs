// SPDX-License-Identifier: Apache-2.0

//! Replay of an encrypted WAL must hand back plaintext.
//!
//! Every record type is encrypted when a key is configured, so a replay path
//! that returns ciphertext does not fail loudly — it feeds undecodable bytes to
//! each engine's decoder and loses the committed suffix on every restart. These
//! tests drive the same entry points production boot uses (`SegmentedWal::replay`
//! and the free `replay_*` drivers) rather than decrypting by hand, so they pin
//! the behaviour where consumers actually observe it.

use nodedb_wal::crypto::{KeyRing, WalEncryptionKey};
use nodedb_wal::mmap_reader::replay_segments_mmap;
use nodedb_wal::record::RecordType;
use nodedb_wal::segmented::{SegmentedWal, SegmentedWalConfig, replay_all_segments};
use nodedb_wal::{CollectionTombstonePayload, WalError, extract_tombstones};

const KEY_BYTES: [u8; 32] = [0x3Cu8; 32];

fn ring() -> KeyRing {
    KeyRing::new(WalEncryptionKey::from_bytes(&KEY_BYTES).expect("key"))
}

fn config(wal_dir: &std::path::Path) -> SegmentedWalConfig {
    SegmentedWalConfig::for_testing(wal_dir.to_path_buf())
}

/// Write `payloads` to a fresh encrypted WAL in `wal_dir` and close it.
fn write_encrypted(wal_dir: &std::path::Path, record_type: u32, payloads: &[Vec<u8>]) {
    let mut wal = SegmentedWal::open(config(wal_dir)).expect("open");
    wal.set_encryption_ring(ring()).expect("enable encryption");
    for payload in payloads {
        wal.append(record_type, 1, 0, 0, payload).expect("append");
    }
    wal.sync().expect("sync");
}

/// Reopen the WAL the way boot does — same key material, a fresh in-memory
/// epoch — so the epoch must come from the on-disk preamble.
fn reopen_with_key(wal_dir: &std::path::Path) -> SegmentedWal {
    let mut wal = SegmentedWal::open(config(wal_dir)).expect("reopen");
    wal.configure_encryption_ring(ring())
        .expect("configure encryption on reopen");
    wal
}

#[test]
fn replay_of_encrypted_wal_returns_plaintext() {
    let dir = crate::tempdir().expect("tempdir");
    let wal_dir = dir.path().join("wal");

    let payloads: Vec<Vec<u8>> = (0..8)
        .map(|i| format!("committed-payload-{i}").into_bytes())
        .collect();
    write_encrypted(&wal_dir, RecordType::Put as u32, &payloads);

    let wal = reopen_with_key(&wal_dir);
    let records = wal.replay().expect("replay must decrypt");

    assert_eq!(records.len(), payloads.len());
    for (record, expected) in records.iter().zip(&payloads) {
        assert_eq!(
            &record.payload, expected,
            "replay must return the plaintext that was written"
        );
        assert!(
            !record.is_encrypted(),
            "the encrypted flag must be cleared so downstream dispatch on \
             logical_record_type stays correct"
        );
        assert_eq!(record.header.record_type, RecordType::Put as u32);
        record
            .verify_checksum()
            .expect("the returned record must still verify its own checksum");
    }
}

#[test]
fn mmap_replay_of_encrypted_wal_returns_plaintext() {
    let dir = crate::tempdir().expect("tempdir");
    let wal_dir = dir.path().join("wal");

    let payloads: Vec<Vec<u8>> = (0..4).map(|i| format!("mmap-{i}").into_bytes()).collect();
    write_encrypted(&wal_dir, RecordType::Put as u32, &payloads);

    let records = replay_segments_mmap(&wal_dir, 0, Some(&ring())).expect("mmap replay");
    assert_eq!(records.len(), payloads.len());
    for (record, expected) in records.iter().zip(&payloads) {
        assert_eq!(&record.payload, expected);
        assert!(!record.is_encrypted());
    }
}

#[test]
fn tombstones_are_extracted_from_an_encrypted_wal() {
    let dir = crate::tempdir().expect("tempdir");
    let wal_dir = dir.path().join("wal");

    let tombstone = CollectionTombstonePayload::new("users", 42)
        .to_bytes()
        .expect("encode tombstone");
    write_encrypted(
        &wal_dir,
        RecordType::CollectionTombstoned as u32,
        std::slice::from_ref(&tombstone),
    );

    let wal = reopen_with_key(&wal_dir);
    let records = wal.replay().expect("replay must decrypt");

    let set = extract_tombstones(&records)
        .expect("a tombstone written to an encrypted WAL must still be extractable");
    assert_eq!(set.purge_lsn(0, 1, "users"), Some(42));
}

#[test]
fn encrypted_replay_without_a_key_is_a_typed_error() {
    let dir = crate::tempdir().expect("tempdir");
    let wal_dir = dir.path().join("wal");

    write_encrypted(&wal_dir, RecordType::Put as u32, &[b"secret".to_vec()]);

    match replay_all_segments(&wal_dir, None) {
        Err(WalError::EncryptedRecordWithoutKey { lsn, .. }) => assert_eq!(lsn, 1),
        Err(other) => panic!("expected EncryptedRecordWithoutKey, got {other}"),
        Ok(records) => panic!(
            "replay without a key returned {} record(s) instead of failing — \
             ciphertext must never be passed off as plaintext",
            records.len()
        ),
    }
}

#[test]
fn unencrypted_replay_is_unchanged() {
    let dir = crate::tempdir().expect("tempdir");
    let wal_dir = dir.path().join("wal");

    let payloads: Vec<Vec<u8>> = (0..5).map(|i| format!("plain-{i}").into_bytes()).collect();
    {
        let mut wal = SegmentedWal::open(config(&wal_dir)).expect("open");
        for payload in &payloads {
            wal.append(RecordType::Put as u32, 1, 0, 0, payload)
                .expect("append");
        }
        wal.sync().expect("sync");
    }

    // Both with no ring at all and with a ring present: an unencrypted record
    // must never be run through decryption.
    for keys in [None, Some(ring())] {
        let records = replay_all_segments(&wal_dir, keys.as_ref()).expect("replay");
        assert_eq!(records.len(), payloads.len());
        for (record, expected) in records.iter().zip(&payloads) {
            assert_eq!(&record.payload, expected);
            assert!(!record.is_encrypted());
            record.verify_checksum().expect("checksum");
        }
    }

    let wal = SegmentedWal::open(config(&wal_dir)).expect("reopen");
    let records = wal.replay().expect("replay via the method");
    assert_eq!(records.len(), payloads.len());
    assert_eq!(records[0].payload, payloads[0]);
}
