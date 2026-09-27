// SPDX-License-Identifier: Apache-2.0

//! Cross-segment LSN integrity: the resumed segment never re-issues LSNs the
//! log already handed out, and replay refuses to concatenate segments with a
//! whole segment missing from between them.

use std::path::{Path, PathBuf};

use nodedb_wal::record::RecordType;
use nodedb_wal::segment::{discover_segments, segment_filename};
use nodedb_wal::segmented::{replay_all_segments, replay_from_limit_dir};
use nodedb_wal::writer::WalWriterConfig;
use nodedb_wal::{SegmentedWal, SegmentedWalConfig, WalError};

/// Config that rolls after a couple of records so tests get many segments.
fn rolling_config(wal_dir: &Path) -> SegmentedWalConfig {
    SegmentedWalConfig {
        wal_dir: wal_dir.to_path_buf(),
        segment_target_size: 100,
        writer_config: WalWriterConfig {
            use_direct_io: false,
            ..Default::default()
        },
    }
}

fn single_segment_config(wal_dir: &Path) -> SegmentedWalConfig {
    SegmentedWalConfig::for_testing(wal_dir.to_path_buf())
}

/// Append `count` records, syncing each one, and return the assigned LSNs.
///
/// The per-record sync is load-bearing, not incidental: rollover triggers on
/// `writer.file_offset()`, which only advances when the buffer is flushed. A
/// batch of appends with a single trailing sync stays in one segment no matter
/// how small `segment_target_size` is, and every multi-segment test below would
/// silently degenerate into a single-segment one.
fn append_records(wal: &mut SegmentedWal, count: usize) -> Vec<u64> {
    let mut lsns = Vec::with_capacity(count);
    for i in 0..count {
        let payload = format!("row-{i:04}");
        let lsn = wal
            .append(RecordType::Put as u32, 1, 0, 0, payload.as_bytes())
            .expect("append must succeed");
        wal.sync().expect("sync must succeed");
        lsns.push(lsn);
    }
    lsns
}

fn replayed_lsns(wal_dir: &Path) -> Vec<u64> {
    replay_all_segments(wal_dir, None)
        .expect("replay must succeed")
        .iter()
        .map(|r| r.header.lsn)
        .collect()
}

fn assert_strictly_increasing(lsns: &[u64]) {
    for pair in lsns.windows(2) {
        assert!(
            pair[1] > pair[0],
            "replayed LSNs must be strictly increasing, got {lsns:?}"
        );
    }
}

/// Build a WAL spanning several segments and return its segment paths.
fn build_multi_segment_wal(wal_dir: &Path, records: usize) -> Vec<PathBuf> {
    {
        let mut wal = SegmentedWal::open(rolling_config(wal_dir)).expect("open must succeed");
        append_records(&mut wal, records);
    }
    let segments = discover_segments(wal_dir).expect("discovery must succeed");
    assert!(
        segments.len() >= 3,
        "test needs at least three segments, got {}",
        segments.len()
    );
    segments.into_iter().map(|s| s.path).collect()
}

#[test]
fn empty_rolled_segment_does_not_reuse_lsns() {
    let dir = crate::tempdir().unwrap();
    let wal_dir = dir.path().join("wal");

    let written = {
        let mut wal = SegmentedWal::open(single_segment_config(&wal_dir)).unwrap();
        append_records(&mut wal, 5)
    };
    let highest = *written.last().unwrap();

    // A rollover that installed a new segment but never flushed a record into
    // it leaves exactly this on disk: a zero-length segment file whose name
    // declares the LSN range it owns.
    let rolled = wal_dir.join(segment_filename(highest + 1));
    std::fs::write(&rolled, b"").unwrap();

    let mut wal = SegmentedWal::open(single_segment_config(&wal_dir)).unwrap();
    assert!(
        wal.next_lsn() > highest,
        "resumed next LSN {} must be above every written LSN {highest}",
        wal.next_lsn()
    );

    let fresh = wal
        .append(RecordType::Put as u32, 1, 0, 0, b"after-restart")
        .unwrap();
    wal.sync().unwrap();
    assert!(
        fresh > highest,
        "new record reused LSN {fresh} at or below the existing high-water mark {highest}"
    );

    let replayed = replayed_lsns(&wal_dir);
    assert_eq!(replayed.len(), written.len() + 1);
    assert_strictly_increasing(&replayed);
}

#[test]
fn torn_last_segment_does_not_reuse_lsns() {
    let dir = crate::tempdir().unwrap();
    let wal_dir = dir.path().join("wal");

    {
        let mut wal = SegmentedWal::open(rolling_config(&wal_dir)).unwrap();
        append_records(&mut wal, 12);
    }

    // Tear away every record of the last segment, leaving an unparseable stub.
    let segments = discover_segments(&wal_dir).unwrap();
    assert!(segments.len() >= 2);
    let last = segments.last().unwrap();
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&last.path)
        .unwrap();
    file.set_len(8).unwrap();
    file.sync_all().unwrap();

    let surviving_high = last.first_lsn - 1;
    let mut wal = SegmentedWal::open(rolling_config(&wal_dir)).unwrap();
    assert!(
        wal.next_lsn() > surviving_high,
        "resumed next LSN {} must be above the surviving high-water mark {surviving_high}",
        wal.next_lsn()
    );

    let fresh = wal
        .append(RecordType::Put as u32, 1, 0, 0, b"after-tear")
        .unwrap();
    wal.sync().unwrap();
    assert!(
        fresh > surviving_high,
        "new record reused LSN {fresh} already covered by surviving segments"
    );

    let replayed = replayed_lsns(&wal_dir);
    assert_strictly_increasing(&replayed);
}

#[test]
fn missing_middle_segment_fails_replay() {
    let dir = crate::tempdir().unwrap();
    let wal_dir = dir.path().join("wal");
    let paths = build_multi_segment_wal(&wal_dir, 20);

    std::fs::remove_file(&paths[1]).unwrap();

    match replay_all_segments(&wal_dir, None) {
        Err(WalError::SegmentLsnGap {
            expected_lsn,
            found_lsn,
            ..
        }) => {
            assert!(
                found_lsn > expected_lsn,
                "gap boundaries are inverted: expected {expected_lsn}, found {found_lsn}"
            );
        }
        Err(other) => panic!("expected a segment LSN gap, got {other}"),
        Ok(records) => panic!(
            "replay silently returned {} records across a missing segment",
            records.len()
        ),
    }
}

#[test]
fn missing_oldest_segments_replay_cleanly() {
    let dir = crate::tempdir().unwrap();
    let wal_dir = dir.path().join("wal");
    let paths = build_multi_segment_wal(&wal_dir, 20);

    // Checkpoint truncation deletes whole segments off the front of the log;
    // the surviving prefix boundary is not a hole.
    std::fs::remove_file(&paths[0]).unwrap();
    std::fs::remove_file(&paths[1]).unwrap();

    let replayed = replayed_lsns(&wal_dir);
    assert!(
        !replayed.is_empty(),
        "surviving segments must still replay after front truncation"
    );
    assert_strictly_increasing(&replayed);
}

#[test]
fn limited_replay_stopping_early_reports_no_gap() {
    let dir = crate::tempdir().unwrap();
    let wal_dir = dir.path().join("wal");
    let paths = build_multi_segment_wal(&wal_dir, 20);

    std::fs::remove_file(&paths[1]).unwrap();

    // The limit is reached inside the first segment, so the replay never
    // reaches the boundary it could not have judged anyway.
    let (records, has_more) = replay_from_limit_dir(&wal_dir, 1, 2, None)
        .expect("early-exit replay must not report a gap");
    assert_eq!(records.len(), 2);
    assert!(has_more);
}
