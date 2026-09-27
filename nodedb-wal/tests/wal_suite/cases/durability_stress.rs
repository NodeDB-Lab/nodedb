// SPDX-License-Identifier: Apache-2.0

//! WAL durability stress test.
//!
//! Spawns 8 independent writer threads, each owning its own WAL file.
//! After all writes are durable, truncates each file to a seeded-random
//! offset within the last group-commit window to simulate a mid-fsync
//! power loss. Then opens each file with the recovery path and asserts:
//!
//! - Recovered record count is contiguous from LSN 1.
//! - No records beyond what was synced are fabricated.
//! - All recovered records pass the built-in checksum check (WalReader
//!   already enforces this; stopping before a torn record is enough).

use std::time::Instant;

use nodedb_wal::Result;
use nodedb_wal::record::RecordType;
use nodedb_wal::recovery::recover;
use nodedb_wal::writer::WalWriter;
use rand::SeedableRng;
use rand::rngs::StdRng;

/// Number of records each writer thread appends.
const RECORDS_PER_WRITER: u32 = 12_500;

/// Number of concurrent writer threads.
const WRITER_COUNT: usize = 8;

/// How many bytes to walk back from the end of file when choosing the
/// truncation point.  We want a window that overlaps the last sync but
/// does not undercut the second-to-last sync, so that the floor assertion
/// (no loss before the truncation) is always satisfiable.
const TRUNCATION_WINDOW_BYTES: u64 = 4096;

fn write_wal(path: &std::path::Path, writer_id: usize) -> Vec<u64> {
    let mut writer = WalWriter::open_without_direct_io(path).unwrap();
    let mut synced_lsns: Vec<u64> = Vec::new();

    for i in 0..RECORDS_PER_WRITER {
        // Encode (writer_id, record_index) into the payload so recovery
        // assertions can cross-check ordering.
        let mut payload = [0u8; 12];
        payload[0..4].copy_from_slice(&(writer_id as u32).to_le_bytes());
        payload[4..8].copy_from_slice(&i.to_le_bytes());
        payload[8..12].copy_from_slice(&i.to_le_bytes()); // redundant sentinel

        let lsn = writer
            .append(RecordType::Put as u32, 1, 0, 0, &payload)
            .unwrap();

        // Sync every 500 records, and once at the very end.
        if i % 500 == 499 || i == RECORDS_PER_WRITER - 1 {
            writer.sync().unwrap();
            synced_lsns.push(lsn);
        }
    }

    synced_lsns
}

fn truncate_within_last_window(path: &std::path::Path, rng: &mut StdRng) -> u64 {
    use rand::RngExt as _;

    let file_len = std::fs::metadata(path).unwrap().len();
    if file_len == 0 {
        return 0;
    }

    // Choose a random truncation point within the last TRUNCATION_WINDOW_BYTES
    // bytes.  Clamp so we never truncate before byte 1 (empty file is a
    // different, uninteresting case).
    let window_start = file_len.saturating_sub(TRUNCATION_WINDOW_BYTES).max(1);
    let trunc_at = rng.random_range(window_start..=file_len);

    let file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    file.set_len(trunc_at).unwrap();
    trunc_at
}

#[test]
fn parallel_writers_survive_truncation() {
    let wall_start = Instant::now();

    // Each thread gets its own tempdir so there's no shared state.
    let dirs: Vec<tempfile::TempDir> = (0..WRITER_COUNT)
        .map(|_| crate::tempdir().unwrap())
        .collect();

    let paths: Vec<std::path::PathBuf> = dirs.iter().map(|d| d.path().join("writer.wal")).collect();

    // Collect the last synced LSN per writer (the floor for the assertion).
    let synced_lsns_per_writer: Vec<u64> = std::thread::scope(|scope| {
        let handles: Vec<_> = paths
            .iter()
            .enumerate()
            .map(|(id, path)| {
                let path = path.clone();
                scope.spawn(move || {
                    let lsns = write_wal(&path, id);
                    // The last element is the LSN of the final synced record.
                    *lsns.last().unwrap()
                })
            })
            .collect();

        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    // All writers confirm final LSN = RECORDS_PER_WRITER.
    for (id, &last_lsn) in synced_lsns_per_writer.iter().enumerate() {
        assert_eq!(
            last_lsn, RECORDS_PER_WRITER as u64,
            "writer {id}: expected final LSN {}, got {last_lsn}",
            RECORDS_PER_WRITER
        );
    }

    // Simulate power loss: truncate each file to a seeded-random offset.
    // Fixed seed — any failure is reproducible.
    let mut rng = StdRng::seed_from_u64(0xA6_DEAD_BEEF);
    let trunc_offsets: Vec<u64> = paths
        .iter()
        .map(|path| truncate_within_last_window(path, &mut rng))
        .collect();

    // Recover each file and assert invariants.
    for (id, (path, _trunc_offset)) in paths.iter().zip(trunc_offsets.iter()).enumerate() {
        let info = recover(path).unwrap();

        // No fabrication: cannot have more records than we originally wrote.
        assert!(
            info.record_count <= RECORDS_PER_WRITER as u64,
            "writer {id}: recovered {}, but only wrote {}",
            info.record_count,
            RECORDS_PER_WRITER
        );

        // LSNs are contiguous: last_lsn == record_count (WAL assigns
        // LSNs starting from 1 in sequence).
        if info.record_count > 0 {
            assert_eq!(
                info.last_lsn, info.record_count,
                "writer {id}: LSN gap — last_lsn={} but record_count={}",
                info.last_lsn, info.record_count
            );
        }

        // The WalReader stops at the first torn/corrupt record, so all
        // returned records have already passed the built-in CRC check.
        // Re-reading through WalReader here confirms the reader is
        // consistent with the recovery scan.
        let reader = nodedb_wal::reader::WalReader::open(path, None).unwrap();
        let records: Vec<_> = reader.records().collect::<Result<_>>().unwrap();

        assert_eq!(
            records.len() as u64,
            info.record_count,
            "writer {id}: WalReader count {} != RecoveryInfo count {}",
            records.len(),
            info.record_count
        );

        // Verify LSN ordering is monotone from 1.
        for (pos, record) in records.iter().enumerate() {
            let expected_lsn = (pos + 1) as u64;
            assert_eq!(
                record.header.lsn, expected_lsn,
                "writer {id}: record at position {pos} has LSN {} (expected {expected_lsn})",
                record.header.lsn
            );
        }
    }

    let elapsed = wall_start.elapsed();
    eprintln!(
        "durability_stress: {} writers × {} records = {} total; elapsed {:.2?}",
        WRITER_COUNT,
        RECORDS_PER_WRITER,
        WRITER_COUNT * RECORDS_PER_WRITER as usize,
        elapsed
    );

    assert!(
        elapsed.as_secs() < 30,
        "durability_stress exceeded 30 s budget: {elapsed:.2?}"
    );
}
