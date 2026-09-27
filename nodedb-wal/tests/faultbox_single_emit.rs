// SPDX-License-Identifier: Apache-2.0

//! One root cause files one report, however many layers the error passes
//! through on the way out.
//!
//! The failure below is detected deep inside the segment reader and surfaces at
//! the public replay API, crossing the reader, the per-segment replay loop, and
//! the directory-wide driver. If any of those also captured, triage would see
//! three unrelated-looking reports — each with its own domain kind and
//! fingerprint — for a single damaged segment.
//!
//! This test owns its own binary on purpose: the recorder is process-wide, so a
//! second test emitting anything at all would make the counts below meaningless.

#![cfg(feature = "diagnostics")]

use std::io::{Seek, SeekFrom, Write};
use std::path::Path;

use nodedb_wal::record::{HEADER_SIZE, RecordType, WAL_MAGIC};
use nodedb_wal::segmented::replay_all_segments;
use nodedb_wal::{SegmentedWal, SegmentedWalConfig, WalError};

/// Create a temporary directory that also works under `wasm32-wasip1`, where
/// `std::env::temp_dir()` is `unimplemented!()` (see the same helper in
/// `tests/wal_suite/main.rs`). Native keeps `tempfile::tempdir()`.
fn tempdir() -> std::io::Result<tempfile::TempDir> {
    #[cfg(target_arch = "wasm32")]
    {
        tempfile::Builder::new().tempdir_in(".")
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        tempfile::tempdir()
    }
}

fn init_recorder(reports_dir: &Path) {
    assert!(
        faultbox::init(
            faultbox::Config::new("nodedb-wal-test", "0.0.0", reports_dir)
                .install_panic_hook(false),
        ),
        "this test binary must be the first and only initializer of the recorder"
    );
}

/// Byte offset of the second record header, found by scanning for the record
/// magic rather than assuming a fixed on-disk layout.
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
fn one_damaged_segment_files_one_report() {
    let reports = tempdir().expect("reports temp dir");
    init_recorder(reports.path());

    let dir = tempdir().expect("wal temp dir");
    let wal_dir = dir.path().join("wal");

    {
        let mut wal = SegmentedWal::open(SegmentedWalConfig::for_testing(wal_dir.clone()))
            .expect("open segmented wal");
        for i in 0..6 {
            wal.append(
                RecordType::Put as u32,
                1,
                0,
                0,
                format!("row-{i}").as_bytes(),
            )
            .expect("append");
            wal.sync().expect("sync");
        }
    }

    let segment = nodedb_wal::segment::discover_segments(&wal_dir)
        .expect("discover segments")
        .into_iter()
        .next()
        .expect("one segment was written");

    // Punch a hole through the second record's header; the records behind it
    // stay intact and carry higher LSNs.
    let hole = second_record_offset(&segment.path);
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .open(&segment.path)
        .expect("open segment");
    file.seek(SeekFrom::Start(hole)).expect("seek");
    file.write_all(&[0xA5u8; HEADER_SIZE]).expect("write");
    file.sync_all().expect("sync");
    drop(file);

    let err = replay_all_segments(&wal_dir, None).expect_err("a hole must fail replay");
    assert!(
        matches!(err, WalError::MidFileCorruption { .. }),
        "expected mid-file corruption, got {err}"
    );

    let groups = faultbox::reader::list(reports.path()).expect("reports are listable");
    assert_eq!(
        groups.len(),
        1,
        "one root cause must file one report, got: {:?}",
        groups.iter().map(|g| g.summary()).collect::<Vec<_>>()
    );
    assert_eq!(
        groups[0].first.domain_kind.as_deref(),
        Some("nodedb_wal.mid_file_corruption")
    );
    assert_eq!(
        faultbox::reader::total_occurrences(&groups),
        1,
        "the failure was detected once; every layer above it only propagated"
    );
}
