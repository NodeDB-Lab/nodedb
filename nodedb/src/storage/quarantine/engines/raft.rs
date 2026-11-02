// SPDX-License-Identifier: BUSL-1.1

//! Quarantine wrapper for Raft snapshot chunk decoding.
//!
//! Snapshot chunks live in memory and have no file. The quarantine record
//! lets operators see through `/v1/cluster/debug/quarantined-segments` that
//! a Raft peer keeps sending corrupt snapshot data. A chunk is keyed by its
//! group id and its `last_included_index`.

use std::sync::Arc;

use nodedb_raft::snapshot_framing::{SnapshotFramingError, decode_snapshot_chunk};

use crate::storage::quarantine::error::QuarantineError;
use crate::storage::quarantine::key::SegmentKey;
use crate::storage::quarantine::registry::QuarantineRegistry;

/// Attempt to decode a snapshot chunk of `group_id` at `last_included_index`,
/// routing CRC-class errors through the quarantine registry.
pub fn decode_snapshot_chunk_with_quarantine<'a>(
    registry: &Arc<QuarantineRegistry>,
    data: &'a [u8],
    group_id: u64,
    last_included_index: u64,
) -> Result<(nodedb_raft::snapshot_framing::SnapshotEngineId, &'a [u8]), RaftOrQuarantine> {
    let key = SegmentKey::Raft {
        group_id,
        last_included_index,
    };
    match decode_snapshot_chunk(data) {
        Ok(result) => {
            registry.record_success(&key);
            Ok(result)
        }
        Err(e) if is_crc_class(&e) => {
            registry
                .record_failure(key, &e.to_string())
                .map_err(RaftOrQuarantine::Quarantined)?;
            Err(RaftOrQuarantine::Framing(e))
        }
        Err(e) => Err(RaftOrQuarantine::Framing(e)),
    }
}

/// Error type returned by `decode_snapshot_chunk_with_quarantine`.
#[derive(Debug, thiserror::Error)]
pub enum RaftOrQuarantine {
    #[error(transparent)]
    Framing(#[from] SnapshotFramingError),
    #[error(transparent)]
    Quarantined(#[from] QuarantineError),
}

fn is_crc_class(e: &SnapshotFramingError) -> bool {
    matches!(
        e,
        SnapshotFramingError::CrcMismatch { .. } | SnapshotFramingError::Truncated(_)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_data_triggers_quarantine_path() {
        let reg = Arc::new(QuarantineRegistry::new());
        // Empty → Truncated (CRC-class).
        let r1 = decode_snapshot_chunk_with_quarantine(&reg, &[], 1, 0);
        assert!(matches!(r1, Err(RaftOrQuarantine::Framing(_))));

        let r2 = decode_snapshot_chunk_with_quarantine(&reg, &[], 1, 0);
        assert!(matches!(r2, Err(RaftOrQuarantine::Quarantined(_))));
    }
}
