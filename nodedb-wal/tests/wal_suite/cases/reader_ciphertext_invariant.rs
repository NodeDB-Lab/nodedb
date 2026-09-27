// SPDX-License-Identifier: Apache-2.0

//! No reader hands out ciphertext by accident.
//!
//! `WalReader` and `MmapWalReader` each have two constructors: `open`, which
//! takes the key ring and decrypts every record on the way out, and `open_raw`,
//! which is the opt-in structural scan. These tests pin both halves of that
//! split, plus the constraint that forces it to exist: `recovery::recover`
//! walks an encrypted segment with no key at all, because `WalWriter::open`
//! resumes a segment for appending and has no key ring to give it.

// `MmapWalReader` drives `nodedb_wal::mmap_reader`, which `src/lib.rs` gates
// off on wasm32, and every case here compares the mmap reader against the file
// reader. Gating the whole case keeps the wasm build from resolving the import
// at all — the equivalent of the `mod` gate in cases/mod.rs, which this file's
// target cannot use because that file is held elsewhere.
#![cfg(not(target_arch = "wasm32"))]

use std::path::{Path, PathBuf};

use nodedb_wal::{
    RecordType, WalError, WalReader,
    crypto::{KeyRing, WalEncryptionKey},
    mmap_reader::MmapWalReader,
    recovery::recover,
    segment::discover_segments,
    segmented::{SegmentedWal, SegmentedWalConfig},
    writer::WalWriter,
};

const KEK: [u8; 32] = [0x7bu8; 32];

const PAYLOADS: [&[u8]; 3] = [b"cipher-0", b"cipher-1", b"cipher-2"];

fn test_ring() -> KeyRing {
    KeyRing::new(WalEncryptionKey::from_bytes(&KEK).expect("build key from KEK"))
}

/// Write one segment holding `PAYLOADS`, encrypted iff `ring` is given, and
/// return its path.
fn write_segment(wal_dir: &Path, ring: Option<KeyRing>) -> PathBuf {
    {
        let mut wal = SegmentedWal::open(SegmentedWalConfig::for_testing(wal_dir.to_path_buf()))
            .expect("open wal");
        if let Some(ring) = ring {
            wal.set_encryption_ring(ring).expect("set key ring");
        }
        for payload in PAYLOADS {
            wal.append(RecordType::Put as u32, 1, 0, 0, payload)
                .expect("append");
        }
        wal.sync().expect("sync");
    }
    let segments = discover_segments(wal_dir).expect("discover");
    assert_eq!(segments.len(), 1, "expected exactly one segment");
    segments[0].path.clone()
}

#[test]
fn keyed_wal_reader_yields_plaintext_from_an_encrypted_segment() {
    let dir = crate::tempdir().unwrap();
    let path = write_segment(&dir.path().join("wal"), Some(test_ring()));

    let ring = test_ring();
    let reader = WalReader::open(&path, Some(&ring)).unwrap();
    let records: Vec<_> = reader.records().collect::<Result<Vec<_>, _>>().unwrap();

    assert_eq!(records.len(), PAYLOADS.len());
    for (record, expected) in records.iter().zip(PAYLOADS.iter()) {
        assert!(
            !record.is_encrypted(),
            "keyed reader must clear ENCRYPTED_FLAG"
        );
        assert!(
            record.verify_checksum().is_ok(),
            "keyed reader must recompute the CRC over the plaintext"
        );
        assert_eq!(record.payload.as_slice(), *expected);
    }
}

#[test]
fn keyed_wal_reader_without_a_key_refuses_an_encrypted_segment() {
    let dir = crate::tempdir().unwrap();
    let path = write_segment(&dir.path().join("wal"), Some(test_ring()));

    let mut reader = WalReader::open(&path, None).unwrap();
    // Not a ciphertext record, and not a silent stop: the read fails outright.
    assert!(matches!(
        reader.next_record(),
        Err(WalError::EncryptedRecordWithoutKey { .. })
    ));
}

#[test]
fn keyed_mmap_reader_yields_plaintext_from_an_encrypted_segment() {
    let dir = crate::tempdir().unwrap();
    let path = write_segment(&dir.path().join("wal"), Some(test_ring()));

    let ring = test_ring();
    let reader = MmapWalReader::open(&path, Some(&ring)).unwrap();
    let records: Vec<_> = reader.records().collect::<Result<Vec<_>, _>>().unwrap();

    assert_eq!(records.len(), PAYLOADS.len());
    for (record, expected) in records.iter().zip(PAYLOADS.iter()) {
        assert!(
            !record.is_encrypted(),
            "keyed mmap reader must clear ENCRYPTED_FLAG"
        );
        assert!(
            record.verify_checksum().is_ok(),
            "keyed mmap reader must recompute the CRC over the plaintext"
        );
        assert_eq!(record.payload.as_slice(), *expected);
    }
}

#[test]
fn keyed_mmap_reader_without_a_key_refuses_an_encrypted_segment() {
    let dir = crate::tempdir().unwrap();
    let path = write_segment(&dir.path().join("wal"), Some(test_ring()));

    let mut reader = MmapWalReader::open(&path, None).unwrap();
    assert!(matches!(
        reader.next_record(),
        Err(WalError::EncryptedRecordWithoutKey { .. })
    ));
}

#[test]
fn recovery_scans_an_encrypted_segment_with_no_key_available() {
    let dir = crate::tempdir().unwrap();
    let encrypted = write_segment(&dir.path().join("enc"), Some(test_ring()));
    let plain = write_segment(&dir.path().join("plain"), None);

    // No key ring anywhere in this call — recovery is a structural scan, and
    // `WalWriter::open` performs it with nothing but a path.
    let encrypted_info = recover(&encrypted).expect("recovery must not need a key");
    let plain_info = recover(&plain).expect("recover plaintext");

    assert_eq!(encrypted_info.record_count, plain_info.record_count);
    assert_eq!(encrypted_info.last_lsn, plain_info.last_lsn);
    assert_eq!(encrypted_info.next_lsn(), PAYLOADS.len() as u64 + 1);
    // The two segments cannot have equal byte lengths — an encrypted segment
    // carries a preamble and a 16-byte auth tag per record — so the offset is
    // pinned against the file it was scanned from instead.
    assert_eq!(
        encrypted_info.end_offset,
        std::fs::metadata(&encrypted).unwrap().len(),
        "recovery must reach the end of the encrypted segment"
    );
}

#[test]
fn an_encrypted_wal_can_still_be_reopened_for_writing() {
    let dir = crate::tempdir().unwrap();
    let wal_dir = dir.path().join("wal");
    let path = write_segment(&wal_dir, Some(test_ring()));

    // The low-level writer resumes the encrypted segment with no key ring.
    let writer = WalWriter::open_without_direct_io(&path).expect("reopen encrypted segment");
    assert_eq!(writer.next_lsn(), PAYLOADS.len() as u64 + 1);
    assert_eq!(
        writer.file_offset(),
        std::fs::metadata(&path).unwrap().len()
    );
    drop(writer);

    // And so does the segmented WAL, which then continues the sequence under
    // the same ring.
    let mut wal =
        SegmentedWal::open(SegmentedWalConfig::for_testing(wal_dir.clone())).expect("reopen wal");
    assert_eq!(wal.next_lsn(), PAYLOADS.len() as u64 + 1);
    wal.configure_encryption_ring(test_ring())
        .expect("reattach key ring");
    let lsn = wal
        .append(RecordType::Put as u32, 1, 0, 0, b"cipher-3")
        .expect("append after reopen");
    assert_eq!(lsn, PAYLOADS.len() as u64 + 1);
    wal.sync().expect("sync");
    drop(wal);

    let ring = test_ring();
    let records = nodedb_wal::segmented::replay_all_segments(&wal_dir, Some(&ring)).unwrap();
    let payloads: Vec<&[u8]> = records.iter().map(|r| r.payload.as_slice()).collect();
    assert_eq!(
        payloads,
        [PAYLOADS[0], PAYLOADS[1], PAYLOADS[2], &b"cipher-3"[..]]
    );
}

#[test]
fn unencrypted_segments_read_the_same_through_both_constructors() {
    let dir = crate::tempdir().unwrap();
    let path = write_segment(&dir.path().join("wal"), None);

    let keyed: Vec<_> = WalReader::open(&path, None)
        .unwrap()
        .records()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let raw: Vec<_> = WalReader::open_raw(&path)
        .unwrap()
        .records()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let keyed_mmap: Vec<_> = MmapWalReader::open(&path, None)
        .unwrap()
        .records()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let raw_mmap: Vec<_> = MmapWalReader::open_raw(&path)
        .unwrap()
        .records()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    for records in [&keyed, &raw, &keyed_mmap, &raw_mmap] {
        let payloads: Vec<&[u8]> = records.iter().map(|r| r.payload.as_slice()).collect();
        assert_eq!(payloads, PAYLOADS);
        assert!(records.iter().all(|r| !r.is_encrypted()));
    }
    // A ring the segment was never written under changes nothing either: the
    // keyed path only decrypts records that are actually flagged.
    let ring = test_ring();
    let with_unused_ring: Vec<_> = WalReader::open(&path, Some(&ring))
        .unwrap()
        .records()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let payloads: Vec<&[u8]> = with_unused_ring
        .iter()
        .map(|r| r.payload.as_slice())
        .collect();
    assert_eq!(payloads, PAYLOADS);
}
