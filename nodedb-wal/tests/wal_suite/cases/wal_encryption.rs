// SPDX-License-Identifier: Apache-2.0

/// WAL encryption restart roundtrip integration test.
///
/// Writes encrypted records, verifies ciphertext on disk, simulates a process
/// restart with a fresh in-memory epoch derived from the same KEK bytes, replays
/// all records, and asserts correct payload recovery in order.
use nodedb_wal::{
    RecordType,
    crypto::{KeyRing, WalEncryptionKey},
    reader::WalReader,
    segment::discover_segments,
    segmented::{SegmentedWal, SegmentedWalConfig},
};

#[test]
fn wal_encryption_restart_roundtrip() {
    let dir = crate::tempdir().unwrap();
    let wal_dir = dir.path().join("wal");
    let kek = [0x42u8; 32];

    let payloads: Vec<&[u8]> = vec![
        b"g12-record-0",
        b"g12-record-1",
        b"g12-record-2",
        b"g12-record-3",
        b"g12-record-4",
    ];

    let config = SegmentedWalConfig::for_testing(wal_dir.clone());

    // Step 1 + 2: write 5 encrypted records and drop the WAL (clean shutdown).
    {
        let key = WalEncryptionKey::from_bytes(&kek).unwrap();
        let ring = KeyRing::new(key);
        let mut wal = SegmentedWal::open(config.clone()).unwrap();
        wal.set_encryption_ring(ring).unwrap();

        for payload in &payloads {
            wal.append(RecordType::Put as u32, 1, 0, 0, payload)
                .unwrap();
        }
        wal.sync().unwrap();
        // WAL drops here — simulates process exit.
    }

    // Step 3 + 4: read raw segment bytes and assert no plaintext payload appears.
    let segments = discover_segments(&wal_dir).unwrap();
    assert!(
        !segments.is_empty(),
        "expected at least one segment after write"
    );

    for seg in &segments {
        let raw = std::fs::read(&seg.path).unwrap();
        for payload in &payloads {
            assert!(
                !raw.windows(payload.len()).any(|w| w == *payload),
                "plaintext payload {:?} found verbatim in segment {:?} — not encrypted",
                std::str::from_utf8(payload).unwrap_or("<binary>"),
                seg.path,
            );
        }
    }

    // Step 5: reopen with same KEK bytes — fresh random in-memory epoch from preamble.
    let key_restart = WalEncryptionKey::from_bytes(&kek).unwrap();
    let ring_restart = KeyRing::new(key_restart);

    let mut recovered: Vec<Vec<u8>> = Vec::new();

    for seg in &segments {
        // Raw mode on purpose: this step asserts the records really are
        // ciphertext on disk and then decrypts them by hand.
        let reader = WalReader::open_raw(&seg.path).unwrap();
        let epoch = *reader
            .segment_preamble()
            .expect("encrypted segment must have a preamble")
            .epoch();
        let preamble_bytes = reader.segment_preamble().unwrap().to_bytes();

        for record_result in reader.records() {
            let record = record_result.unwrap();
            assert!(
                record.is_encrypted(),
                "all written records must be encrypted"
            );
            let plaintext = record
                .decrypt_payload_ring(&epoch, Some(&preamble_bytes), Some(&ring_restart))
                .unwrap();
            recovered.push(plaintext);
        }
    }

    // Step 6: assert all 5 payloads recovered correctly and in order.
    assert_eq!(
        recovered.len(),
        payloads.len(),
        "record count mismatch after replay"
    );
    for (i, (got, expected)) in recovered.iter().zip(payloads.iter()).enumerate() {
        assert_eq!(
            got.as_slice(),
            *expected,
            "payload mismatch at index {i}: got {:?}, expected {:?}",
            std::str::from_utf8(got).unwrap_or("<binary>"),
            std::str::from_utf8(expected).unwrap_or("<binary>"),
        );
    }
}
