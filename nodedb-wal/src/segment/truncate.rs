// SPDX-License-Identifier: Apache-2.0

//! Sealed segment truncation after checkpoint LSN advances.

use std::fs;
use std::path::Path;

use crate::error::{Result, WalError};

use super::atomic_io::fsync_directory;
use super::discovery::discover_segments;

/// Result of a WAL truncation operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TruncateResult {
    /// Number of segment files deleted.
    pub segments_deleted: u64,

    /// Total bytes reclaimed from disk.
    pub bytes_reclaimed: u64,
}

/// Delete all sealed segments whose maximum LSN is strictly less than `checkpoint_lsn`.
///
/// The `active_segment_first_lsn` identifies the segment currently being written
/// to — it is NEVER deleted, even if all its records are below the checkpoint LSN.
///
/// Returns the number of segments deleted and total bytes reclaimed.
pub fn truncate_segments(
    wal_dir: &Path,
    checkpoint_lsn: u64,
    active_segment_first_lsn: u64,
) -> Result<TruncateResult> {
    let segments = discover_segments(wal_dir)?;
    let mut deleted_count = 0u64;
    let mut bytes_reclaimed = 0u64;

    for seg in &segments {
        if seg.first_lsn == active_segment_first_lsn {
            continue;
        }

        // A segment's max_lsn < next_segment.first_lsn. Safe to delete if the
        // next segment's first_lsn <= checkpoint_lsn.
        let next_first_lsn = segments
            .iter()
            .find(|s| s.first_lsn > seg.first_lsn)
            .map(|s| s.first_lsn)
            .unwrap_or(u64::MAX);

        if next_first_lsn <= checkpoint_lsn {
            bytes_reclaimed += seg.file_size;
            fs::remove_file(&seg.path).map_err(WalError::Io)?;

            let dwb_path = seg.dwb_path();
            // A segment with no DWB is the normal case, so a missing file is
            // not a failure — anything else is, and leaving it unreported
            // would orphan a torn-write side channel for a segment that no
            // longer exists.
            if let Err(e) = fs::remove_file(&dwb_path)
                && e.kind() != std::io::ErrorKind::NotFound
            {
                return Err(WalError::Io(e));
            }

            tracing::info!(
                segment = %seg.path.display(),
                first_lsn = seg.first_lsn,
                "deleted WAL segment (checkpoint_lsn={})",
                checkpoint_lsn
            );

            deleted_count += 1;

            // Crash injection: die partway through a multi-segment deletion.
            // The surviving segments must still form a replayable log — the
            // deletion order is oldest-first, so an interrupted truncate can
            // only ever leave a suffix, never a hole.
            nodedb_types::fail_point_err!("wal::mid_truncate_segments", |detail: String| {
                WalError::Io(std::io::Error::other(format!(
                    "failpoint wal::mid_truncate_segments: {detail}"
                )))
            });
        }
    }

    if deleted_count > 0 {
        // Until the directory entries are durable the unlinks can be undone by
        // a crash, resurrecting segments whose records sit below the
        // checkpoint — replay would then apply them a second time. A failure
        // here means the truncation did not happen, so the caller must not
        // advance anything on the strength of it.
        fsync_directory(wal_dir)?;
    }

    Ok(TruncateResult {
        segments_deleted: deleted_count,
        bytes_reclaimed,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_deletes_old_segments() {
        let dir = crate::test_tempdir().unwrap();

        fs::write(dir.path().join("wal-00000000000000000001.seg"), b"data1").unwrap();
        fs::write(dir.path().join("wal-00000000000000000001.dwb"), b"dwb1").unwrap();
        fs::write(dir.path().join("wal-00000000000000000050.seg"), b"data2").unwrap();
        fs::write(dir.path().join("wal-00000000000000000100.seg"), b"data3").unwrap();

        let result = truncate_segments(dir.path(), 100, 100).unwrap();
        assert_eq!(result.segments_deleted, 2);

        let remaining = discover_segments(dir.path()).unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].first_lsn, 100);

        assert!(!dir.path().join("wal-00000000000000000001.dwb").exists());
    }

    #[test]
    fn truncate_never_deletes_active_segment() {
        let dir = crate::test_tempdir().unwrap();

        fs::write(dir.path().join("wal-00000000000000000001.seg"), b"data").unwrap();

        let result = truncate_segments(dir.path(), 999, 1).unwrap();
        assert_eq!(result.segments_deleted, 0);

        let remaining = discover_segments(dir.path()).unwrap();
        assert_eq!(remaining.len(), 1);
    }

    #[test]
    fn truncate_no_segments_below_checkpoint() {
        let dir = crate::test_tempdir().unwrap();

        fs::write(dir.path().join("wal-00000000000000000100.seg"), b"data").unwrap();
        fs::write(dir.path().join("wal-00000000000000000200.seg"), b"data").unwrap();

        let result = truncate_segments(dir.path(), 50, 200).unwrap();
        assert_eq!(result.segments_deleted, 0);
    }
}
