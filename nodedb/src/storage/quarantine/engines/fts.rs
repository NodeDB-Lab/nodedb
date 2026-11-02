// SPDX-License-Identifier: BUSL-1.1

//! Quarantine wrapper for FTS LSM segment reads.

use std::sync::Arc;

use nodedb_fts::lsm::segment::error::SegmentError;

use crate::storage::quarantine::error::QuarantineError;
use crate::storage::quarantine::key::{CollectionScope, SegmentKey};
use crate::storage::quarantine::registry::QuarantineRegistry;

/// The full storage identity of one FTS LSM segment.
pub struct FtsSegmentRef<'a> {
    pub scope: &'a CollectionScope,
    /// `None` for the whole-document index.
    pub field: Option<&'a str>,
    pub segment_id: &'a str,
}

impl FtsSegmentRef<'_> {
    fn key(&self) -> SegmentKey {
        SegmentKey::Fts {
            scope: self.scope.clone(),
            field: self.field.map(str::to_string),
            segment_id: self.segment_id.to_string(),
        }
    }
}

/// Validate FTS segment bytes. On CRC-class corruption, route through the
/// quarantine registry. Returns the bytes unchanged on success.
///
/// The first CRC-class failure returns `Err(Segment)`. The second consecutive
/// failure returns `Err(Quarantined)`.
pub fn validate_fts_segment_bytes(
    registry: &Arc<QuarantineRegistry>,
    bytes: Vec<u8>,
    segment: &FtsSegmentRef<'_>,
) -> Result<Vec<u8>, FtsOrQuarantine> {
    match nodedb_fts::lsm::segment::reader::SegmentReader::open(bytes.clone()) {
        Ok(_reader) => {
            registry.record_success(&segment.key());
            Ok(bytes)
        }
        Err(e) if is_crc_class(&e) => {
            registry
                .record_failure(segment.key(), &e.to_string())
                .map_err(FtsOrQuarantine::Quarantined)?;
            Err(FtsOrQuarantine::Segment(e))
        }
        Err(e) => Err(FtsOrQuarantine::Segment(e)),
    }
}

/// Error type returned by [`validate_fts_segment_bytes`].
#[derive(Debug, thiserror::Error)]
pub enum FtsOrQuarantine {
    #[error(transparent)]
    Segment(#[from] SegmentError),
    #[error(transparent)]
    Quarantined(#[from] QuarantineError),
}

fn is_crc_class(e: &SegmentError) -> bool {
    matches!(
        e,
        SegmentError::ChecksumMismatch { .. } | SegmentError::Truncated | SegmentError::BadMagic
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seg<'a>(scope: &'a CollectionScope, id: &'a str) -> FtsSegmentRef<'a> {
        FtsSegmentRef {
            scope,
            field: None,
            segment_id: id,
        }
    }

    #[test]
    fn empty_bytes_first_strike_returns_segment_error() {
        let reg = Arc::new(QuarantineRegistry::new());
        let scope = CollectionScope::new(1, 1, "coll");
        // Empty bytes → Truncated (CRC-class). First strike returns SegmentError.
        let r1 = validate_fts_segment_bytes(&reg, vec![], &seg(&scope, "seg1"));
        assert!(matches!(r1, Err(FtsOrQuarantine::Segment(_))));
    }

    #[test]
    fn empty_bytes_second_strike_returns_quarantined() {
        let reg = Arc::new(QuarantineRegistry::new());
        let scope = CollectionScope::new(1, 1, "coll");
        let _ = validate_fts_segment_bytes(&reg, vec![], &seg(&scope, "seg2"));
        let r2 = validate_fts_segment_bytes(&reg, vec![], &seg(&scope, "seg2"));
        assert!(matches!(r2, Err(FtsOrQuarantine::Quarantined(_))));
    }

    #[test]
    fn success_clears_strike() {
        let reg = Arc::new(QuarantineRegistry::new());
        let scope = CollectionScope::new(1, 1, "coll");
        let s = seg(&scope, "seg3");
        let _ = validate_fts_segment_bytes(&reg, vec![], &s);
        reg.record_success(&s.key());
        // The strike is cleared, so the next failure is a first strike again.
        let r = validate_fts_segment_bytes(&reg, vec![], &s);
        assert!(matches!(r, Err(FtsOrQuarantine::Segment(_))));
    }

    #[test]
    fn two_fields_of_one_collection_are_separate() {
        let reg = Arc::new(QuarantineRegistry::new());
        let scope = CollectionScope::new(1, 1, "coll");
        let title = FtsSegmentRef {
            scope: &scope,
            field: Some("title"),
            segment_id: "s1",
        };
        let body = FtsSegmentRef {
            scope: &scope,
            field: Some("body"),
            segment_id: "s1",
        };
        let _ = validate_fts_segment_bytes(&reg, vec![], &title);
        let _ = validate_fts_segment_bytes(&reg, vec![], &title);
        assert!(reg.is_quarantined(&title.key()));
        assert!(!reg.is_quarantined(&body.key()));
    }
}
