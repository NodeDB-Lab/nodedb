// SPDX-License-Identifier: Apache-2.0

//! With the recorder compiled out — the default — the WAL behaves exactly as it
//! did before it existed.
//!
//! The report sites sit on error paths that must keep working for embedders who
//! never enable `diagnostics`, including wasm32 builds where the feature is
//! inert even when it is on. A recorder that changed an error, swallowed one, or
//! panicked at a detection site would be worse than no recorder at all.

use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

use nodedb_wal::record::{HEADER_SIZE, RecordType, WAL_MAGIC};
use nodedb_wal::writer::WalWriter;
use nodedb_wal::{WalError, recover};

fn write_segment(path: &Path, count: u64) {
    let mut writer = WalWriter::open_without_direct_io(path).expect("open writer");
    for i in 0..count {
        writer
            .append(
                RecordType::Put as u32,
                1,
                0,
                0,
                format!("row-{i}").as_bytes(),
            )
            .expect("append");
        writer.sync().expect("sync");
    }
}

fn second_record_offset(path: &Path) -> u64 {
    let bytes = std::fs::read(path).expect("read segment");
    let magic = WAL_MAGIC.to_le_bytes();
    bytes
        .windows(magic.len())
        .enumerate()
        .filter(|(_, window)| *window == magic)
        .map(|(i, _)| i as u64)
        .nth(1)
        .expect("the segment holds at least two records")
}

#[test]
fn a_hole_still_fails_recovery_with_the_same_error() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("hole.wal");
    write_segment(&path, 6);

    let hole = second_record_offset(&path);
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .expect("open segment");
    file.seek(SeekFrom::Start(hole)).expect("seek");
    file.write_all(&[0xA5u8; HEADER_SIZE]).expect("write");
    file.sync_all().expect("sync");
    drop(file);

    match recover(&path) {
        Err(WalError::MidFileCorruption {
            offset,
            resync_offset,
            resync_lsn,
            ..
        }) => {
            assert!(resync_offset > offset);
            assert!(resync_lsn > 1);
        }
        Err(other) => panic!("expected mid-file corruption, got {other}"),
        Ok(info) => panic!("a hole was accepted as the end of the log: {info:?}"),
    }
}

#[test]
fn a_healthy_segment_recovers_untouched() {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("clean.wal");
    write_segment(&path, 4);

    let info = recover(&path).expect("a healthy segment recovers");
    assert_eq!(info.last_lsn, 4);
    assert_eq!(info.record_count, 4);
    assert_eq!(info.next_lsn(), 5);
}
