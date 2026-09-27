// SPDX-License-Identifier: Apache-2.0

//! Discovery of WAL segment files in a directory.

use std::fs;
use std::path::Path;

use crate::error::{Result, WalError};

use super::meta::{SegmentMeta, parse_segment_filename};

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discover_empty_dir() {
        let dir = crate::test_tempdir().unwrap();
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
        let dir = crate::test_tempdir().unwrap();

        fs::write(dir.path().join("wal-00000000000000000050.seg"), b"seg3").unwrap();
        fs::write(dir.path().join("wal-00000000000000000001.seg"), b"seg1").unwrap();
        fs::write(dir.path().join("wal-00000000000000000025.seg"), b"seg2").unwrap();
        fs::write(dir.path().join("wal-00000000000000000001.dwb"), b"dwb").unwrap();
        fs::write(dir.path().join("metadata.json"), b"{}").unwrap();

        let segments = discover_segments(dir.path()).unwrap();
        assert_eq!(segments.len(), 3);
        assert_eq!(segments[0].first_lsn, 1);
        assert_eq!(segments[1].first_lsn, 25);
        assert_eq!(segments[2].first_lsn, 50);
    }
}
