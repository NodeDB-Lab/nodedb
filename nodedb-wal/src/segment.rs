//! WAL segment management.
//!
//! The WAL is split into fixed-size segment files for efficient truncation.
//! Each segment is a standalone WAL file containing records within an LSN range.
//!
//! ## Naming convention
//!
//! Segments are named `wal-{first_lsn:020}.seg` — zero-padded for lexicographic
//! ordering. This guarantees `ls` and `readdir` return segments in LSN order.
//!
//! ## Lifecycle
//!
//! 1. Writer creates a new segment when the current segment exceeds `target_size`.
//! 2. The active segment is the one being appended to.
//! 3. `truncate_before(lsn)` deletes all sealed segments whose `max_lsn < lsn`.
//! 4. The active segment is NEVER deleted — only sealed (closed) segments are eligible.
//!
//! ## Recovery
//!
//! On startup, all segment files in the WAL directory are discovered via `readdir`,
//! sorted by first_lsn, and replayed in order. The last segment is the active one.

use std::fs;
use std::path::{Path, PathBuf};

use crate::error::{Result, WalError};

/// Default segment target size: 64 MiB.
///
/// This is a soft limit — the writer finishes the current record before rolling.
/// Actual segments may be slightly larger than this value.
pub const DEFAULT_SEGMENT_TARGET_SIZE: u64 = 64 * 1024 * 1024;

/// Segment file extension.
const SEGMENT_EXTENSION: &str = "seg";

/// Segment file prefix.
const SEGMENT_PREFIX: &str = "wal-";

/// Metadata about a WAL segment file on disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentMeta {
    /// Path to the segment file on disk.
    pub path: PathBuf,

    /// First LSN stored in this segment (from the filename).
    pub first_lsn: u64,

    /// File size in bytes.
    pub file_size: u64,
}

impl SegmentMeta {
    /// Path to the companion double-write buffer file.
    pub fn dwb_path(&self) -> PathBuf {
        self.path.with_extension("dwb")
    }
}

impl Ord for SegmentMeta {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.first_lsn.cmp(&other.first_lsn)
    }
}

impl PartialOrd for SegmentMeta {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Build a segment filename from a starting LSN.
pub fn segment_filename(first_lsn: u64) -> String {
    format!("{SEGMENT_PREFIX}{first_lsn:020}.{SEGMENT_EXTENSION}")
}

/// Build a full segment path within a WAL directory.
pub fn segment_path(wal_dir: &Path, first_lsn: u64) -> PathBuf {
    wal_dir.join(segment_filename(first_lsn))
}

/// Parse the first_lsn from a segment filename.
///
/// Returns `None` if the filename doesn't match the expected pattern.
fn parse_segment_filename(filename: &str) -> Option<u64> {
    let stem = filename.strip_prefix(SEGMENT_PREFIX)?;
    let lsn_str = stem.strip_suffix(&format!(".{SEGMENT_EXTENSION}"))?;
    lsn_str.parse::<u64>().ok()
}

/// Discover all WAL segments in a directory, sorted by first_lsn.
///
/// Ignores non-segment files (DWB files, other metadata).
pub fn discover_segments(wal_dir: &Path) -> Result<Vec<SegmentMeta>> {
    if !wal_dir.exists() {
        return Ok(Vec::new());
    }

    let entries = fs::read_dir(wal_dir).map_err(WalError::Io)?;
    let mut segments = Vec::new();

    for entry in entries {
        let entry = entry.map_err(WalError::Io)?;
        let file_name = entry.file_name();
        let name = file_name.to_string_lossy();

        if let Some(first_lsn) = parse_segment_filename(&name) {
            let metadata = entry.metadata().map_err(WalError::Io)?;
            segments.push(SegmentMeta {
                path: entry.path(),
                first_lsn,
                file_size: metadata.len(),
            });
        }
    }

    segments.sort();
    Ok(segments)
}

/// Migrate a legacy single-file WAL to the segmented format.
///
/// If a file named `wal_path` (e.g., `data/wal`) exists and is not a directory,
/// it is renamed to `{wal_dir}/wal-00000000000000000001.seg`. The WAL directory
/// is created if needed.
///
/// This is a one-time migration that runs transparently on startup. After
/// migration, the old path no longer exists as a file.
///
/// Returns `true` if migration occurred, `false` if no legacy file found.
pub fn migrate_legacy_wal(legacy_path: &Path, wal_dir: &Path) -> Result<bool> {
    // Only migrate if legacy_path is a file (not a directory or missing).
    if !legacy_path.is_file() {
        return Ok(false);
    }

    // Don't migrate if it's zero-length (empty WAL).
    let metadata = fs::metadata(legacy_path).map_err(WalError::Io)?;
    if metadata.len() == 0 {
        // Remove the empty file and let the new system create a fresh directory.
        let _ = fs::remove_file(legacy_path);
        return Ok(false);
    }

    // Scan the legacy WAL to find its first LSN.
    let info = crate::recovery::recover(legacy_path)?;
    let first_lsn = if info.record_count == 0 {
        1 // Empty but valid — start at 1.
    } else {
        // Scan to find the actual first LSN (recovery only gives us last_lsn).
        // Read the first record to get its LSN.
        let mut reader = crate::reader::WalReader::open(legacy_path)?;
        match reader.next_record()? {
            Some(record) => record.header.lsn,
            None => 1,
        }
    };

    // Create the WAL directory.
    fs::create_dir_all(wal_dir).map_err(WalError::Io)?;

    // Move the legacy file to the segmented location.
    let new_path = segment_path(wal_dir, first_lsn);
    fs::rename(legacy_path, &new_path).map_err(WalError::Io)?;

    // Move the companion DWB file if it exists.
    let legacy_dwb = legacy_path.with_extension("dwb");
    if legacy_dwb.exists() {
        let new_dwb = new_path.with_extension("dwb");
        fs::rename(&legacy_dwb, &new_dwb).map_err(WalError::Io)?;
    }

    tracing::info!(
        legacy = %legacy_path.display(),
        segment = %new_path.display(),
        first_lsn,
        "migrated legacy WAL to segmented format"
    );

    Ok(true)
}

/// Delete all sealed segments whose maximum LSN is strictly less than `checkpoint_lsn`.
///
/// The `active_segment_path` is the segment currently being written to — it is
/// NEVER deleted, even if all its records are below the checkpoint LSN.
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
        // Never delete the active segment.
        if seg.first_lsn == active_segment_first_lsn {
            continue;
        }

        // A sealed segment's records are all below checkpoint_lsn if the NEXT
        // segment's first_lsn <= checkpoint_lsn. We find the next segment by
        // looking at segments with higher first_lsn.
        //
        // However, we need to know the max_lsn of each segment. Since segments
        // are sequential, a segment's max_lsn < next_segment.first_lsn.
        // Therefore: if next_segment.first_lsn <= checkpoint_lsn, then this
        // segment's max_lsn < checkpoint_lsn, so it's safe to delete.
        //
        // For the last sealed segment (before active), we use the active
        // segment's first_lsn as the upper bound.
        let next_first_lsn = segments
            .iter()
            .find(|s| s.first_lsn > seg.first_lsn)
            .map(|s| s.first_lsn)
            .unwrap_or(u64::MAX);

        // This segment's max_lsn < next_first_lsn.
        // Safe to delete if next_first_lsn <= checkpoint_lsn.
        if next_first_lsn <= checkpoint_lsn {
            bytes_reclaimed += seg.file_size;

            // Delete the segment file.
            fs::remove_file(&seg.path).map_err(WalError::Io)?;

            // Delete the companion DWB file if it exists.
            let dwb_path = seg.dwb_path();
            if dwb_path.exists() {
                let _ = fs::remove_file(&dwb_path);
            }

            tracing::info!(
                segment = %seg.path.display(),
                first_lsn = seg.first_lsn,
                "deleted WAL segment (checkpoint_lsn={})",
                checkpoint_lsn
            );

            deleted_count += 1;
        }
    }

    Ok(TruncateResult {
        segments_deleted: deleted_count,
        bytes_reclaimed,
    })
}

/// Result of a WAL truncation operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TruncateResult {
    /// Number of segment files deleted.
    pub segments_deleted: u64,

    /// Total bytes reclaimed from disk.
    pub bytes_reclaimed: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_filename_format() {
        assert_eq!(
            segment_filename(1),
            "wal-00000000000000000001.seg"
        );
        assert_eq!(
            segment_filename(999),
            "wal-00000000000000000999.seg"
        );
        assert_eq!(
            segment_filename(u64::MAX),
            "wal-18446744073709551615.seg"
        );
    }

    #[test]
    fn parse_segment_filename_valid() {
        assert_eq!(
            parse_segment_filename("wal-00000000000000000001.seg"),
            Some(1)
        );
        assert_eq!(
            parse_segment_filename("wal-00000000000000000999.seg"),
            Some(999)
        );
    }

    #[test]
    fn parse_segment_filename_invalid() {
        assert_eq!(parse_segment_filename("wal.log"), None);
        assert_eq!(parse_segment_filename("wal-abc.seg"), None);
        assert_eq!(parse_segment_filename("other-00001.seg"), None);
        assert_eq!(parse_segment_filename("wal-00001.dwb"), None);
    }

    #[test]
    fn discover_empty_dir() {
        let dir = tempfile::tempdir().unwrap();
        let segments = discover_segments(dir.path()).unwrap();
        assert!(segments.is_empty());
    }

    #[test]
    fn discover_nonexistent_dir() {
        let segments = discover_segments(Path::new("/nonexistent/wal/dir")).unwrap();
        assert!(segments.is_empty());
    }

    #[test]
    fn discover_segments_sorted() {
        let dir = tempfile::tempdir().unwrap();

        // Create segment files out of order.
        fs::write(dir.path().join("wal-00000000000000000050.seg"), b"seg3").unwrap();
        fs::write(dir.path().join("wal-00000000000000000001.seg"), b"seg1").unwrap();
        fs::write(dir.path().join("wal-00000000000000000025.seg"), b"seg2").unwrap();
        // Non-segment file should be ignored.
        fs::write(dir.path().join("wal-00000000000000000001.dwb"), b"dwb").unwrap();
        fs::write(dir.path().join("metadata.json"), b"{}").unwrap();

        let segments = discover_segments(dir.path()).unwrap();
        assert_eq!(segments.len(), 3);
        assert_eq!(segments[0].first_lsn, 1);
        assert_eq!(segments[1].first_lsn, 25);
        assert_eq!(segments[2].first_lsn, 50);
    }

    #[test]
    fn truncate_deletes_old_segments() {
        let dir = tempfile::tempdir().unwrap();

        // Create 3 segment files.
        fs::write(dir.path().join("wal-00000000000000000001.seg"), b"data1").unwrap();
        fs::write(dir.path().join("wal-00000000000000000001.dwb"), b"dwb1").unwrap();
        fs::write(dir.path().join("wal-00000000000000000050.seg"), b"data2").unwrap();
        fs::write(dir.path().join("wal-00000000000000000100.seg"), b"data3").unwrap();

        // Truncate: checkpoint_lsn=100, active segment starts at 100.
        // Segment 1 (max_lsn < 50) and segment 50 (max_lsn < 100) should be deleted.
        let result = truncate_segments(dir.path(), 100, 100).unwrap();
        assert_eq!(result.segments_deleted, 2);

        // Only the active segment should remain.
        let remaining = discover_segments(dir.path()).unwrap();
        assert_eq!(remaining.len(), 1);
        assert_eq!(remaining[0].first_lsn, 100);

        // DWB for segment 1 should also be deleted.
        assert!(!dir.path().join("wal-00000000000000000001.dwb").exists());
    }

    #[test]
    fn truncate_never_deletes_active_segment() {
        let dir = tempfile::tempdir().unwrap();

        // Single active segment.
        fs::write(dir.path().join("wal-00000000000000000001.seg"), b"data").unwrap();

        let result = truncate_segments(dir.path(), 999, 1).unwrap();
        assert_eq!(result.segments_deleted, 0);

        let remaining = discover_segments(dir.path()).unwrap();
        assert_eq!(remaining.len(), 1);
    }

    #[test]
    fn truncate_no_segments_below_checkpoint() {
        let dir = tempfile::tempdir().unwrap();

        fs::write(dir.path().join("wal-00000000000000000100.seg"), b"data").unwrap();
        fs::write(dir.path().join("wal-00000000000000000200.seg"), b"data").unwrap();

        // Checkpoint at 50 — neither segment should be deleted because
        // segment 100's next is 200, and 200 > 50.
        let result = truncate_segments(dir.path(), 50, 200).unwrap();
        assert_eq!(result.segments_deleted, 0);
    }

    #[test]
    fn migrate_legacy_wal() {
        let dir = tempfile::tempdir().unwrap();
        let legacy_path = dir.path().join("test.wal");
        let wal_dir = dir.path().join("wal_segments");

        // Write a legacy WAL with some records.
        {
            let mut writer = crate::writer::WalWriter::open_without_direct_io(&legacy_path).unwrap();
            writer.append(crate::record::RecordType::Put as u16, 1, 0, b"hello").unwrap();
            writer.append(crate::record::RecordType::Put as u16, 1, 0, b"world").unwrap();
            writer.sync().unwrap();
        }

        // Migrate.
        let migrated = super::migrate_legacy_wal(&legacy_path, &wal_dir).unwrap();
        assert!(migrated);

        // Legacy file should be gone.
        assert!(!legacy_path.exists());

        // Segment should exist in the new directory.
        let segments = discover_segments(&wal_dir).unwrap();
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].first_lsn, 1);

        // Verify records are still readable.
        let reader = crate::reader::WalReader::open(&segments[0].path).unwrap();
        let records: Vec<_> = reader.records().collect::<crate::Result<_>>().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].payload, b"hello");
    }

    #[test]
    fn migrate_nonexistent_legacy_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let legacy_path = dir.path().join("nonexistent.wal");
        let wal_dir = dir.path().join("wal_segments");

        let migrated = super::migrate_legacy_wal(&legacy_path, &wal_dir).unwrap();
        assert!(!migrated);
    }

    #[test]
    fn migrate_empty_legacy_is_noop() {
        let dir = tempfile::tempdir().unwrap();
        let legacy_path = dir.path().join("empty.wal");
        let wal_dir = dir.path().join("wal_segments");

        // Create empty file.
        fs::write(&legacy_path, b"").unwrap();

        let migrated = super::migrate_legacy_wal(&legacy_path, &wal_dir).unwrap();
        assert!(!migrated);
        assert!(!legacy_path.exists()); // Empty file cleaned up.
    }
}
