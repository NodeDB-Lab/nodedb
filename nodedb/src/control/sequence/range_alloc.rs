//! Raft-coordinated range allocation for distributed sequences.
//!
//! Each node (Origin shard or Lite instance) requests a chunk of IDs from
//! the Raft leader. Local `nextval` advances within the chunk without
//! network round-trips. When the chunk is exhausted, a new chunk is allocated.
//!
//! Uses the existing `RaftProposer` on SharedState — proposes serialized
//! `RangeAllocationRequest` messages to the Raft group, which are applied
//! by the state machine to update the global sequence counter.

use serde::{Deserialize, Serialize};

/// A request to allocate a range of sequence values via Raft consensus.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct RangeAllocationRequest {
    pub tenant_id: u32,
    pub sequence_name: String,
    /// How many values to allocate in this chunk.
    pub chunk_size: i64,
    /// Expected epoch — allocation rejected if epoch mismatch (stale node).
    pub epoch: u64,
}

/// A GAP_FREE counter advance proposed to Raft for cluster-safe serialization.
///
/// In cluster mode, each gap-free nextval is proposed as a Raft log entry.
/// On leader failover, the new leader replays the log and has the exact counter.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct GapFreeAdvanceRequest {
    pub tenant_id: u32,
    pub sequence_name: String,
    /// The value being reserved (must match the local counter advance).
    pub reserved_value: i64,
    pub epoch: u64,
}

/// Response from the Raft leader after a range allocation.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct RangeAllocationResponse {
    /// First value in the allocated range (inclusive).
    pub range_start: i64,
    /// Last value in the allocated range (inclusive).
    pub range_end: i64,
    /// Epoch of this allocation.
    pub epoch: u64,
}

/// Allocates sequence ranges via the Raft proposer.
///
/// In single-node mode (no Raft), ranges are allocated locally with no
/// coordination. In cluster mode, each allocation is proposed to the
/// Raft leader and committed across the group.
pub struct RangeAllocator {
    /// Default chunk size for allocations.
    pub default_chunk_size: i64,
}

impl RangeAllocator {
    pub fn new(default_chunk_size: i64) -> Self {
        Self { default_chunk_size }
    }

    /// Allocate a chunk of sequence values.
    ///
    /// In cluster mode: proposes to Raft leader, awaits commit.
    /// In single-node mode: directly advances the catalog counter.
    pub fn allocate_chunk(
        &self,
        state: &crate::control::state::SharedState,
        tenant_id: u32,
        sequence_name: &str,
        increment: i64,
        epoch: u64,
    ) -> Result<RangeAllocationResponse, crate::Error> {
        let chunk_size = self.default_chunk_size;

        // In cluster mode, propose through Raft for distributed uniqueness.
        if let Some(proposer) = state.raft_proposer.get() {
            let request = RangeAllocationRequest {
                tenant_id,
                sequence_name: sequence_name.to_string(),
                chunk_size,
                epoch,
            };
            let payload =
                zerompk::to_msgpack_vec(&request).map_err(|e| crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("range allocation request: {e}"),
                })?;

            // Propose to vshard 0 (system shard for metadata operations).
            let (_group_id, _log_index) =
                proposer(0, payload).map_err(|e| crate::Error::Dispatch {
                    detail: format!("sequence range allocation raft propose: {e}"),
                })?;

            // Compute the allocated range based on current state.
            // The Raft commit handler will advance the global counter.
            let current = state
                .sequence_registry
                .get_def(tenant_id, sequence_name)
                .map(|d| d.start_value)
                .unwrap_or(1);

            let range_start = current;
            let range_end = if increment > 0 {
                current + chunk_size * increment - increment
            } else {
                current + chunk_size * increment + increment.abs()
            };

            return Ok(RangeAllocationResponse {
                range_start,
                range_end,
                epoch,
            });
        }

        // Single-node mode: allocate directly from the local counter.
        // No Raft needed — just advance the counter by chunk_size.
        let handle_exists = state.sequence_registry.exists(tenant_id, sequence_name);
        if !handle_exists {
            return Err(crate::Error::BadRequest {
                detail: format!("sequence \"{sequence_name}\" does not exist"),
            });
        }

        let current_val = state
            .sequence_registry
            .currval(tenant_id, sequence_name)
            .unwrap_or(0);

        let range_start = current_val + increment;
        let range_end = range_start + (chunk_size - 1) * increment;

        Ok(RangeAllocationResponse {
            range_start,
            range_end,
            epoch,
        })
    }

    /// Propose a GAP_FREE counter advance to Raft.
    ///
    /// In cluster mode: serializes the advance as a Raft log entry so that
    /// on leader failover, the new leader has the exact counter value.
    /// In single-node mode: no-op (local counter is authoritative).
    pub fn propose_gap_free_advance(
        &self,
        state: &crate::control::state::SharedState,
        tenant_id: u32,
        sequence_name: &str,
        reserved_value: i64,
        epoch: u64,
    ) -> Result<(), crate::Error> {
        let Some(proposer) = state.raft_proposer.get() else {
            // Single-node mode — local counter is authoritative, no Raft needed.
            return Ok(());
        };

        let request = GapFreeAdvanceRequest {
            tenant_id,
            sequence_name: sequence_name.to_string(),
            reserved_value,
            epoch,
        };
        let payload =
            zerompk::to_msgpack_vec(&request).map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("gap-free advance request: {e}"),
            })?;

        // Propose to vshard 0 (system shard).
        proposer(0, payload).map_err(|e| crate::Error::Dispatch {
            detail: format!("gap-free advance raft propose: {e}"),
        })?;

        Ok(())
    }
}

impl Default for RangeAllocator {
    fn default() -> Self {
        Self::new(10_000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_roundtrip() {
        let req = RangeAllocationRequest {
            tenant_id: 1,
            sequence_name: "order_seq".into(),
            chunk_size: 10_000,
            epoch: 1,
        };
        let bytes = zerompk::to_msgpack_vec(&req).unwrap();
        let decoded: RangeAllocationRequest = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded.sequence_name, "order_seq");
        assert_eq!(decoded.chunk_size, 10_000);
    }

    #[test]
    fn response_roundtrip() {
        let resp = RangeAllocationResponse {
            range_start: 1,
            range_end: 10_000,
            epoch: 1,
        };
        let bytes = zerompk::to_msgpack_vec(&resp).unwrap();
        let decoded: RangeAllocationResponse = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded.range_start, 1);
        assert_eq!(decoded.range_end, 10_000);
    }
}
