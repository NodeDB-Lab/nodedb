// SPDX-License-Identifier: Apache-2.0

//! Durability of the two syscalls that stand between an acknowledged write and
//! stable storage: the fsync of the segment file, and the fsync of the
//! directory holding that segment's name.
//!
//! Both have a failure mode that looks like success from the outside — a retry
//! over an already-emptied buffer, and a discarded directory-fsync result —
//! and both lose records that a client was told were committed.
//!
//! Requires `--features failpoints`; without it the injections compile away
//! and there is nothing to drive.

use nodedb_types::fail_point::FailGuard;
use nodedb_wal::record::RecordType;
use nodedb_wal::segmented::{SegmentedWal, SegmentedWalConfig};
use nodedb_wal::writer::{WalWriter, WalWriterConfig};
use nodedb_wal::{Result as WalResult, WalError};

/// One record per segment, so a rollover is reached deterministically on the
/// second append rather than after an unspecified amount of payload.
fn rolling_config(wal_dir: std::path::PathBuf) -> SegmentedWalConfig {
    SegmentedWalConfig {
        wal_dir,
        segment_target_size: 1,
        writer_config: WalWriterConfig {
            use_direct_io: false,
            ..Default::default()
        },
    }
}

fn append(wal: &mut SegmentedWal, payload: &[u8]) -> WalResult<u64> {
    wal.append(RecordType::Put as u32, 1, 0, 0, payload)
}

/// A batch is flushed into the page cache and its fsync then fails. The buffer
/// is already empty at that point, so a second waiter retrying `sync()` must
/// not mistake emptiness for durability.
#[test]
fn retry_after_a_failed_fsync_never_reports_durability() {
    let dir = crate::tempdir().unwrap();
    let path = dir.path().join("failed_fsync.wal");

    let mut writer = WalWriter::open_without_direct_io(&path).unwrap();
    writer
        .append(RecordType::Put as u32, 1, 0, 0, b"acked-5")
        .unwrap();
    writer
        .append(RecordType::Put as u32, 1, 0, 0, b"acked-6")
        .unwrap();

    {
        let _g = FailGuard::fail("wal::fsync_failure", "EIO");
        match writer.sync() {
            Err(WalError::DurabilityLost { .. }) => {}
            Err(other) => panic!("expected DurabilityLost, got {other:?}"),
            Ok(()) => panic!("sync reported success while the fsync failed"),
        }
    }

    // The flush emptied the buffer before the fsync ran. A retry — the second
    // waiter in the same group commit — must not return Ok.
    match writer.sync() {
        Err(WalError::DurabilityLost { .. }) => {}
        Err(other) => panic!("expected DurabilityLost on retry, got {other:?}"),
        Ok(()) => panic!("retry reported durability over an unsynced flush"),
    }

    // The page-cache contents are gone with the reported error, so the writer
    // must not accept anything further either.
    match writer.append(RecordType::Put as u32, 1, 0, 0, b"after") {
        Err(WalError::DurabilityLost { .. }) => {}
        other => panic!("poisoned writer accepted an append: {other:?}"),
    }

    // Sealing is a durability boundary and must not paper over the failure.
    match writer.seal() {
        Err(WalError::DurabilityLost { .. }) => {}
        other => panic!("poisoned writer sealed successfully: {other:?}"),
    }
}

/// The no-op case stays a no-op: with nothing flushed-and-unsynced, `sync()`
/// must not reach the fsync at all. The armed fail point is the assertion —
/// if the fsync were issued, this would return an error.
#[test]
fn sync_with_nothing_outstanding_skips_the_fsync() {
    let dir = crate::tempdir().unwrap();
    let path = dir.path().join("noop_sync.wal");

    let mut writer = WalWriter::open_without_direct_io(&path).unwrap();
    writer
        .append(RecordType::Put as u32, 1, 0, 0, b"durable")
        .unwrap();
    writer.sync().unwrap();

    let _g = FailGuard::fail("wal::fsync_failure", "must not be reached");
    writer.sync().unwrap();
    writer.sync().unwrap();
}

/// A rollover whose directory fsync fails must fail the append. Otherwise the
/// record is acknowledged into an inode whose name may never reach disk.
#[test]
fn rollover_propagates_a_failed_directory_fsync() {
    let dir = crate::tempdir().unwrap();
    let mut wal = SegmentedWal::open(rolling_config(dir.path().join("wal"))).unwrap();

    append(&mut wal, b"first").unwrap();
    wal.sync().unwrap();

    let _g = FailGuard::fail("wal::fsync_directory", "dirent not durable");
    match append(&mut wal, b"second") {
        Err(WalError::Io(_)) => {}
        Err(other) => panic!("expected the directory fsync error, got {other:?}"),
        Ok(lsn) => {
            panic!("append at LSN {lsn} was acknowledged into a segment with no durable dirent")
        }
    }
}

/// A rollover that fails must not brick the WAL. The old writer stays
/// installed and unsealed, so the next append retries the roll and lands.
#[test]
fn a_failed_rollover_leaves_the_wal_writable() {
    let dir = crate::tempdir().unwrap();
    let mut wal = SegmentedWal::open(rolling_config(dir.path().join("wal"))).unwrap();

    append(&mut wal, b"first").unwrap();
    wal.sync().unwrap();

    {
        let _g = FailGuard::fail("wal::fsync_directory", "dirent not durable");
        match append(&mut wal, b"rejected") {
            Err(WalError::Io(_)) => {}
            Err(other) => panic!("expected the directory fsync error, got {other:?}"),
            Ok(lsn) => panic!("append at LSN {lsn} succeeded despite a failed roll"),
        }
    }

    // The fail point is disarmed; the WAL must still be usable.
    let lsn = match append(&mut wal, b"after-failed-roll") {
        Ok(lsn) => lsn,
        Err(WalError::Sealed) => panic!("the failed roll left a sealed writer installed"),
        Err(other) => panic!("append after a failed roll returned {other:?}"),
    };
    wal.sync().unwrap();

    let records = wal.replay().unwrap();
    assert!(
        records
            .iter()
            .any(|r| r.header.lsn == lsn && r.payload == b"after-failed-roll"),
        "the record written after the failed roll must replay"
    );
    assert!(
        records.iter().any(|r| r.payload == b"first"),
        "the record written before the failed roll must still replay"
    );
    assert!(
        records.iter().all(|r| r.payload != b"rejected"),
        "the rejected append must not have reached the log"
    );
}

/// The same for truncation: a discarded failure lets deleted segments come
/// back after a crash and replay below the checkpoint.
#[test]
fn truncation_propagates_a_failed_directory_fsync() {
    let dir = crate::tempdir().unwrap();
    let mut wal = SegmentedWal::open(rolling_config(dir.path().join("wal"))).unwrap();

    for i in 0..5u32 {
        append(&mut wal, format!("row-{i}").as_bytes()).unwrap();
        wal.sync().unwrap();
    }
    assert!(wal.list_segments().unwrap().len() >= 4);

    let _g = FailGuard::fail("wal::fsync_directory", "dirent not durable");
    match wal.truncate_before(4) {
        Err(WalError::Io(_)) => {}
        Err(other) => panic!("expected the directory fsync error, got {other:?}"),
        Ok(result) => panic!("truncation reported {result:?} without a durable directory"),
    }
}

/// Guards the fixes against over-strictness: with the filesystem behaving,
/// segment creation, rollover, and truncation all still work end to end.
#[test]
fn rollover_and_truncation_succeed_when_the_directory_fsync_works() {
    let dir = crate::tempdir().unwrap();
    let mut wal = SegmentedWal::open(rolling_config(dir.path().join("wal"))).unwrap();

    for i in 0..5u32 {
        append(&mut wal, format!("row-{i}").as_bytes()).unwrap();
        wal.sync().unwrap();
    }

    let before = wal.list_segments().unwrap().len();
    assert!(before >= 4, "expected several segments, got {before}");

    let result = wal.truncate_before(4).unwrap();
    assert!(result.segments_deleted > 0);
    assert!(result.bytes_reclaimed > 0);

    let after = wal.list_segments().unwrap().len();
    assert!(after < before);

    // Whatever survived is still a replayable suffix, and the WAL keeps going.
    let records = wal.replay().unwrap();
    assert!(records.iter().all(|r| r.header.lsn >= 4));
    append(&mut wal, b"after-truncate").unwrap();
    wal.sync().unwrap();
}
