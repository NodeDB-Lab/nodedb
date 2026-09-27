// SPDX-License-Identifier: Apache-2.0

//! Crash injection at the durability-critical boundaries of the WAL.
//!
//! Each test arms a fail point, drives the writer into the injected failure,
//! and asserts the two invariants a crash must never break:
//!
//! 1. No acknowledged write disappears.
//! 2. What survives is a replayable prefix — never a hole, never a torn record
//!    presented as data, never a gap in the LSN sequence.
//!
//! Requires `--features failpoints`; without it the injections compile away
//! and there is nothing to drive.

use std::path::Path;

use nodedb_types::fail_point::FailGuard;
use nodedb_wal::record::RecordType;
use nodedb_wal::recovery::recover;
use nodedb_wal::segmented::{SegmentedWal, SegmentedWalConfig};
use nodedb_wal::writer::{WalWriter, WalWriterConfig};
use nodedb_wal::{WalError, WalReader};

fn read_lsns(path: &Path) -> Vec<u64> {
    let mut reader = WalReader::open(path, None).unwrap();
    let mut lsns = Vec::new();
    while let Some(record) = reader.next_record().unwrap() {
        lsns.push(record.header.lsn);
    }
    lsns
}

#[test]
fn out_of_space_on_sync_retains_the_batch_for_retry() {
    let dir = crate::tempdir().unwrap();
    let path = dir.path().join("enospc_sync.wal");

    let mut writer = WalWriter::open_without_direct_io(&path).unwrap();
    for i in 0..3u32 {
        writer
            .append(
                RecordType::Put as u32,
                1,
                0,
                0,
                format!("row-{i}").as_bytes(),
            )
            .unwrap();
    }

    let next_lsn_before = writer.next_lsn();
    {
        let _g = FailGuard::fail("wal::flush_out_of_space", "device full");
        match writer.sync() {
            Err(WalError::OutOfSpace { .. }) => {}
            Err(other) => panic!("expected OutOfSpace, got {other:?}"),
            Ok(()) => panic!("sync succeeded while the device was full"),
        }
    }

    // The failed flush must not have consumed LSNs or dropped the batch.
    assert_eq!(writer.next_lsn(), next_lsn_before);
    assert_eq!(
        std::fs::metadata(&path).unwrap().len(),
        0,
        "a failed flush must not leave a partial batch on disk"
    );

    // Space freed: the same batch retries and lands intact.
    writer.sync().unwrap();
    writer
        .append(RecordType::Put as u32, 1, 0, 0, b"after-recovery")
        .unwrap();
    writer.sync().unwrap();
    drop(writer);

    assert_eq!(read_lsns(&path), vec![1, 2, 3, 4]);
    let info = recover(&path).unwrap();
    assert_eq!(info.record_count, 4);
    assert_eq!(info.last_lsn, 4);
}

#[test]
fn out_of_space_on_append_does_not_burn_an_lsn() {
    let dir = crate::tempdir().unwrap();
    let path = dir.path().join("enospc_append.wal");

    // A small buffer makes `append` itself flush once it fills up, which is
    // where a full device is first observed on the write path.
    let mut writer = WalWriter::open(
        &path,
        WalWriterConfig {
            write_buffer_size: 4096,
            use_direct_io: false,
            ..Default::default()
        },
    )
    .unwrap();

    let payload = vec![b'x'; 512];
    let mut accepted = 0u64;
    let failed_lsn = {
        let _g = FailGuard::fail("wal::flush_out_of_space", "device full");
        loop {
            let before = writer.next_lsn();
            match writer.append(RecordType::Put as u32, 1, 0, 0, &payload) {
                Ok(_) => accepted += 1,
                Err(WalError::OutOfSpace { .. }) => {
                    // The rejected append must leave the sequence untouched.
                    assert_eq!(
                        writer.next_lsn(),
                        before,
                        "a rejected append consumed an LSN"
                    );
                    break before;
                }
                Err(other) => panic!("expected OutOfSpace, got {other:?}"),
            }
            assert!(accepted < 1000, "buffer never filled");
        }
    };

    // Once space is available the rejected record is re-appended under the
    // very LSN that was held back — no hole in the log.
    let reused = writer
        .append(RecordType::Put as u32, 1, 0, 0, &payload)
        .unwrap();
    assert_eq!(reused, failed_lsn);
    writer.sync().unwrap();
    drop(writer);

    let lsns = read_lsns(&path);
    assert_eq!(lsns.len() as u64, accepted + 1);
    assert_eq!(
        lsns,
        (1..=accepted + 1).collect::<Vec<_>>(),
        "LSN sequence has a hole"
    );
}

#[test]
fn crash_before_dwb_flush_keeps_the_acknowledged_prefix() {
    let dir = crate::tempdir().unwrap();
    let path = dir.path().join("crash_dwb.wal");

    let mut writer = WalWriter::open_without_direct_io(&path).unwrap();
    for i in 0..3u32 {
        writer
            .append(
                RecordType::Put as u32,
                1,
                0,
                0,
                format!("acked-{i}").as_bytes(),
            )
            .unwrap();
    }
    writer.sync().unwrap(); // Acknowledged.

    writer
        .append(RecordType::Put as u32, 1, 0, 0, b"never-acked")
        .unwrap();
    {
        let _g = FailGuard::fail("wal::before_dwb_flush", "crash");
        assert!(writer.sync().is_err());
    }
    drop(writer); // Simulated crash: no further fsync.

    // The three acknowledged records must be intact and replayable.
    let info = recover(&path).unwrap();
    assert!(
        info.record_count >= 3,
        "acknowledged records were lost: only {} survived",
        info.record_count
    );
    let lsns = read_lsns(&path);
    assert_eq!(
        lsns,
        (1..=lsns.len() as u64).collect::<Vec<_>>(),
        "surviving records are not a contiguous prefix"
    );

    // Reopening continues cleanly from whatever survived.
    let mut writer = WalWriter::open_without_direct_io(&path).unwrap();
    let next = writer.next_lsn();
    assert_eq!(next, info.last_lsn + 1);
    writer
        .append(RecordType::Put as u32, 1, 0, 0, b"post-crash")
        .unwrap();
    writer.sync().unwrap();
    drop(writer);

    assert!(recover(&path).unwrap().record_count >= 4);
}

#[test]
fn crash_before_wal_fsync_never_resurrects_records_from_the_dwb() {
    let dir = crate::tempdir().unwrap();
    let path = dir.path().join("crash_fsync.wal");

    let mut writer = WalWriter::open_without_direct_io(&path).unwrap();
    writer
        .append(RecordType::Put as u32, 1, 0, 0, b"acked")
        .unwrap();
    writer.sync().unwrap();

    writer
        .append(RecordType::Put as u32, 1, 0, 0, b"in-flight")
        .unwrap();
    {
        let _g = FailGuard::fail("wal::before_wal_fsync", "crash");
        assert!(writer.sync().is_err());
    }
    drop(writer);

    // Whatever survives must be a contiguous prefix. The DWB is a torn-write
    // side channel: it must never inject a record the WAL never received, and
    // it must never make replay stop early.
    let info = recover(&path).unwrap();
    assert!(info.record_count >= 1);
    let lsns = read_lsns(&path);
    assert_eq!(lsns, (1..=lsns.len() as u64).collect::<Vec<_>>());
    assert_eq!(info.last_lsn, lsns.len() as u64);
}

#[test]
fn crash_mid_truncate_leaves_a_replayable_log() {
    let dir = crate::tempdir().unwrap();
    let wal_dir = dir.path().join("wal");

    let mut wal = SegmentedWal::open(SegmentedWalConfig {
        segment_target_size: 512,
        ..SegmentedWalConfig::for_testing(wal_dir.clone())
    })
    .unwrap();

    // Enough records to roll several segments.
    let total = 60u32;
    for i in 0..total {
        wal.append(
            RecordType::Put as u32,
            1,
            0,
            0,
            format!("row-{i:03}").as_bytes(),
        )
        .unwrap();
        wal.sync().unwrap();
    }
    let segments_before = wal.list_segments().unwrap().len();
    assert!(
        segments_before >= 3,
        "test needs several segments, got {segments_before}"
    );

    let checkpoint_lsn = u64::from(total) - 5;
    {
        let _g = FailGuard::fail("wal::mid_truncate_segments", "crash");
        assert!(wal.truncate_before(checkpoint_lsn).is_err());
    }

    // Deletion runs oldest-first, so an interrupted truncate can only leave a
    // suffix — never a hole. Replay must succeed and stay ordered.
    let replayed = wal.replay().unwrap();
    assert!(!replayed.is_empty());
    let lsns: Vec<u64> = replayed.iter().map(|r| r.header.lsn).collect();
    assert!(
        lsns.windows(2).all(|w| w[1] == w[0] + 1),
        "surviving records are not contiguous: {lsns:?}"
    );
    assert_eq!(
        *lsns.last().unwrap(),
        u64::from(total),
        "the newest records must always survive truncation"
    );

    // Retrying after the crash completes the truncation.
    wal.truncate_before(checkpoint_lsn).unwrap();
    let after = wal.replay().unwrap();
    assert!(after.iter().any(|r| r.header.lsn == u64::from(total)));
}
