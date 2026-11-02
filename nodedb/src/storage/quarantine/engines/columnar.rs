// SPDX-License-Identifier: BUSL-1.1

//! Quarantine wrapper for columnar segment reads.
//!
//! All production `nodedb_columnar::SegmentReader::open` call sites that can
//! serve user queries go through `open_segment_with_quarantine`. Sites that
//! only run during compaction or testing bypass this wrapper intentionally —
//! they either tolerate open failures or run in controlled conditions.

use std::sync::Arc;

use nodedb_columnar::ColumnarError;

use crate::storage::quarantine::error::QuarantineError;
use crate::storage::quarantine::key::{CollectionScope, SegmentKey};
use crate::storage::quarantine::registry::QuarantineRegistry;

/// Attempt to open flushed segment `segment_id` of `scope`, routing
/// CRC-class errors through the quarantine registry.
///
/// Returns `Ok(reader)` on success, `Err(Quarantined)` on the second
/// consecutive CRC failure, and `Err(Columnar)` for every other failure.
pub fn open_segment_with_quarantine<'a>(
    registry: &Arc<QuarantineRegistry>,
    seg_bytes: &'a [u8],
    scope: &CollectionScope,
    segment_id: u64,
) -> Result<nodedb_columnar::reader::SegmentReader<'a>, ColumnarOrQuarantine> {
    let key = || SegmentKey::Columnar {
        scope: scope.clone(),
        segment_id,
    };
    match nodedb_columnar::SegmentReader::open(seg_bytes) {
        Ok(reader) => {
            registry.record_success(&key());
            Ok(reader)
        }
        Err(e) if is_crc_class(&e) => {
            // The first strike returns `Ok(())` and the caller surfaces `e`.
            registry
                .record_failure(key(), &e.to_string())
                .map_err(ColumnarOrQuarantine::Quarantined)?;
            Err(ColumnarOrQuarantine::Columnar(e))
        }
        Err(e) => Err(ColumnarOrQuarantine::Columnar(e)),
    }
}

/// Error type returned by `open_segment_with_quarantine`.
#[derive(Debug, thiserror::Error)]
pub enum ColumnarOrQuarantine {
    #[error(transparent)]
    Columnar(#[from] ColumnarError),
    #[error(transparent)]
    Quarantined(#[from] QuarantineError),
}

/// Returns `true` for the error variants that indicate data corruption
/// (CRC failure, truncation, invalid magic) as opposed to transient I/O or
/// schema mismatches.
fn is_crc_class(e: &ColumnarError) -> bool {
    matches!(
        e,
        ColumnarError::FooterCrcMismatch { .. }
            | ColumnarError::Corruption { .. }
            | ColumnarError::TruncatedSegment { .. }
            | ColumnarError::InvalidMagic(_)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_bytes_triggers_quarantine_path() {
        let reg = Arc::new(QuarantineRegistry::new());
        let scope = CollectionScope::new(1, 1, "coll");
        // Empty bytes → TruncatedSegment (a CRC-class error). First call
        // returns a ColumnarError (first strike); second returns Quarantined.
        let r1 = open_segment_with_quarantine(&reg, &[], &scope, 1);
        assert!(matches!(r1, Err(ColumnarOrQuarantine::Columnar(_))));

        let r2 = open_segment_with_quarantine(&reg, &[], &scope, 1);
        assert!(matches!(r2, Err(ColumnarOrQuarantine::Quarantined(_))));
    }
}
