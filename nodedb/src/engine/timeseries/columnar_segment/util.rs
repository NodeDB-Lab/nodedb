use std::path::Path;

use super::error::SegmentError;

pub(super) fn dir_size(dir: &Path) -> Result<u64, SegmentError> {
    let mut size = 0u64;
    let entries = std::fs::read_dir(dir)
        .map_err(|e| SegmentError::Io(format!("read dir {}: {e}", dir.display())))?;
    for entry in entries {
        let entry = entry.map_err(|e| SegmentError::Io(format!("dir entry: {e}")))?;
        let meta = entry
            .metadata()
            .map_err(|e| SegmentError::Io(format!("metadata {}: {e}", entry.path().display())))?;
        size += meta.len();
    }
    Ok(size)
}
