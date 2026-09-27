// SPDX-License-Identifier: Apache-2.0

//! Preamble handling and multi-segment integrity for the mmap and lazy readers.
//!
//! Both readers are used for replay paths that must never silently observe a
//! shortened log: an encrypted segment whose preamble is parsed as a record
//! header yields zero records, and a segment that stops on damage (or has gone
//! missing from the middle of the log) must be an error rather than a quiet
//! truncation.

use std::path::{Path, PathBuf};

use nodedb_wal::{
    RecordType, WalError,
    crypto::{KeyRing, WalEncryptionKey},
    lazy_reader::{LazyWalReader, replay_segment_lazy},
    mmap_reader::{MmapWalReader, replay_segments_mmap},
    record::{HEADER_SIZE, WAL_MAGIC},
    segment::discover_segments,
    segmented::{SegmentedWal, SegmentedWalConfig},
};

const KEK: [u8; 32] = [0x42u8; 32];

fn test_ring() -> KeyRing {
    KeyRing::new(WalEncryptionKey::from_bytes(&KEK).unwrap())
}

/// Write one encrypted segment holding `payloads`, then close the WAL.
fn write_encrypted_segment(wal_dir: &Path, payloads: &[&[u8]]) -> PathBuf {
    {
        let mut wal = SegmentedWal::open(SegmentedWalConfig::for_testing(wal_dir.to_path_buf()))
            .expect("open wal");
        wal.set_encryption_ring(test_ring()).expect("set key ring");
        for payload in payloads {
            wal.append(RecordType::Put as u32, 1, 0, 0, payload)
                .expect("append");
        }
        wal.sync().expect("sync");
    }
    let segments = discover_segments(wal_dir).expect("discover");
    assert_eq!(segments.len(), 1, "expected exactly one segment");
    segments[0].path.clone()
}

/// Write one unencrypted segment holding `payloads`, then close the WAL.
fn write_plain_segment(wal_dir: &Path, payloads: &[&[u8]]) -> PathBuf {
    {
        let mut wal = SegmentedWal::open(SegmentedWalConfig::for_testing(wal_dir.to_path_buf()))
            .expect("open wal");
        for payload in payloads {
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
fn mmap_reader_replays_every_record_of_an_encrypted_segment() {
    let dir = crate::tempdir().unwrap();
    let payloads: Vec<&[u8]> = vec![b"enc-mmap-0", b"enc-mmap-1", b"enc-mmap-2"];
    let path = write_encrypted_segment(&dir.path().join("wal"), &payloads);

    // Raw mode on purpose: this test asserts the on-disk records are ciphertext
    // and then decrypts them by hand.
    let reader = MmapWalReader::open_raw(&path).unwrap();
    let preamble = *reader
        .segment_preamble()
        .expect("encrypted segment must expose its preamble");
    let preamble_bytes = preamble.to_bytes();
    let records: Vec<_> = reader.records().collect::<Result<Vec<_>, _>>().unwrap();

    assert_eq!(
        records.len(),
        payloads.len(),
        "mmap reader must skip the preamble and read every record"
    );

    let ring = test_ring();
    for (record, expected) in records.iter().zip(payloads.iter()) {
        assert!(record.is_encrypted());
        let plaintext = record
            .decrypt_payload_ring(preamble.epoch(), Some(&preamble_bytes), Some(&ring))
            .unwrap();
        assert_eq!(plaintext.as_slice(), *expected);
    }
}

#[test]
fn lazy_reader_replays_every_record_of_an_encrypted_segment() {
    let dir = crate::tempdir().unwrap();
    let payloads: Vec<&[u8]> = vec![b"enc-lazy-0", b"enc-lazy-1", b"enc-lazy-2"];
    let path = write_encrypted_segment(&dir.path().join("wal"), &payloads);

    let ring = test_ring();
    let mut recovered: Vec<Vec<u8>> = Vec::new();
    // The driver owns decryption now: a consumer that is handed the key ring
    // receives plaintext and cannot accidentally act on ciphertext.
    replay_segment_lazy(&path, Some(&ring), |reader, header| {
        assert!(
            reader.segment_preamble().is_some(),
            "encrypted segment must expose its preamble"
        );
        let record = reader.read_record(header)?;
        assert!(
            !record.is_encrypted(),
            "replay must hand the consumer a decrypted record"
        );
        recovered.push(record.payload);
        Ok(())
    })
    .unwrap();

    assert_eq!(
        recovered.len(),
        payloads.len(),
        "lazy reader must skip the preamble and read every record"
    );
    for (got, expected) in recovered.iter().zip(payloads.iter()) {
        assert_eq!(got.as_slice(), *expected);
    }
}

#[test]
fn both_readers_still_replay_an_unencrypted_segment() {
    let dir = crate::tempdir().unwrap();
    let payloads: Vec<&[u8]> = vec![b"plain-0", b"plain-1", b"plain-2"];
    let path = write_plain_segment(&dir.path().join("wal"), &payloads);

    let reader = MmapWalReader::open(&path, None).unwrap();
    assert!(
        reader.segment_preamble().is_none(),
        "unencrypted segments carry no preamble"
    );
    let records: Vec<_> = reader.records().collect::<Result<Vec<_>, _>>().unwrap();
    assert_eq!(records.len(), payloads.len());
    for (record, expected) in records.iter().zip(payloads.iter()) {
        assert_eq!(record.payload.as_slice(), *expected);
    }

    let mut lazy = LazyWalReader::open(&path, None).unwrap();
    assert!(lazy.segment_preamble().is_none());
    assert_eq!(lazy.offset(), 0, "no preamble means no bytes to skip");
    let mut lazy_payloads = Vec::new();
    while let Some(header) = lazy.next_header().unwrap() {
        lazy_payloads.push(lazy.read_payload(&header).unwrap());
    }
    assert_eq!(lazy_payloads.len(), payloads.len());
    for (got, expected) in lazy_payloads.iter().zip(payloads.iter()) {
        assert_eq!(got.as_slice(), *expected);
    }
}

/// Overwrite `len` bytes at `offset` with a pattern that is not the WAL magic.
fn smash(path: &Path, offset: u64, len: usize) {
    use std::io::{Seek, SeekFrom, Write};
    let mut file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(&vec![0xA5u8; len]).unwrap();
    file.sync_all().unwrap();
}

/// Offset of the `n`th record header in a segment, located by its magic.
fn record_header_offset(path: &Path, n: usize) -> u64 {
    let bytes = std::fs::read(path).unwrap();
    let magic = WAL_MAGIC.to_le_bytes();
    let mut found = 0usize;
    for (i, window) in bytes.windows(magic.len()).enumerate() {
        if window == magic {
            if found == n {
                return i as u64;
            }
            found += 1;
        }
    }
    panic!("segment {path:?} has fewer than {} records", n + 1);
}

#[test]
fn mmap_replay_rejects_mid_file_corruption_instead_of_truncating() {
    let dir = crate::tempdir().unwrap();
    let wal_dir = dir.path().join("wal");
    let payloads: Vec<&[u8]> = vec![b"row-0", b"row-1", b"row-2", b"row-3", b"row-4", b"row-5"];
    let path = write_plain_segment(&wal_dir, &payloads);

    // Smash the second record's header; the records behind it stay intact and
    // committed, so stopping there would silently discard them.
    let hole = record_header_offset(&path, 1);
    smash(&path, hole, HEADER_SIZE);

    match replay_segments_mmap(&wal_dir, 0, None) {
        Err(WalError::MidFileCorruption { .. }) => {}
        Ok(records) => panic!(
            "mid-file corruption was silently truncated to {} records",
            records.len()
        ),
        Err(other) => panic!("expected mid-file corruption, got {other:?}"),
    }
}

/// Write `count` records into a WAL forced to roll over frequently.
fn write_many_segments(wal_dir: &Path, count: u32) {
    let config = SegmentedWalConfig {
        wal_dir: wal_dir.to_path_buf(),
        segment_target_size: 100,
        ..SegmentedWalConfig::for_testing(wal_dir.to_path_buf())
    };
    let mut wal = SegmentedWal::open(config).unwrap();
    for i in 0..count {
        wal.append(
            RecordType::Put as u32,
            1,
            0,
            0,
            format!("record-{i:04}").as_bytes(),
        )
        .unwrap();
        wal.sync().unwrap();
    }
}

#[test]
fn mmap_replay_rejects_a_missing_middle_segment() {
    let dir = crate::tempdir().unwrap();
    let wal_dir = dir.path().join("wal");
    write_many_segments(&wal_dir, 20);

    let segments = discover_segments(&wal_dir).unwrap();
    assert!(
        segments.len() >= 4,
        "test needs several segments, got {}",
        segments.len()
    );
    std::fs::remove_file(&segments[segments.len() / 2].path).unwrap();

    match replay_segments_mmap(&wal_dir, 0, None) {
        Err(WalError::SegmentLsnGap { .. }) => {}
        Ok(records) => panic!(
            "a missing middle segment produced {} records and no error",
            records.len()
        ),
        Err(other) => panic!("expected a segment LSN gap, got {other:?}"),
    }
}

#[test]
fn mmap_replay_accepts_a_truncated_prefix() {
    let dir = crate::tempdir().unwrap();
    let wal_dir = dir.path().join("wal");
    write_many_segments(&wal_dir, 20);

    let segments = discover_segments(&wal_dir).unwrap();
    assert!(segments.len() >= 4);
    // Checkpoint truncation deletes whole segments from the bottom of the log
    // by design — that is a shorter log, not a hole.
    for seg in &segments[..2] {
        std::fs::remove_file(&seg.path).unwrap();
    }

    let records = replay_segments_mmap(&wal_dir, 0, None).expect("a truncated prefix is legal");
    let surviving_first_lsn = segments[2].first_lsn;
    assert!(
        records.iter().all(|r| r.header.lsn >= surviving_first_lsn),
        "replay must return only records from the surviving segments"
    );
    assert!(!records.is_empty());
}
