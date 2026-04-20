//! Re-sync request and throttle messages.

use serde::{Deserialize, Serialize};

/// Re-sync request message (bidirectional, 0x50).
///
/// Sent when a receiver detects:
/// - Sequence gap: missing `mutation_id`s in the delta stream
/// - Checksum failure: CRC32C mismatch on a delta payload
/// - State divergence: local state inconsistent with received deltas
///
/// On receiving a ResyncRequest, the sender should:
/// 1. Re-send all deltas from `from_mutation_id` onwards, OR
/// 2. Send a full snapshot if `from_mutation_id` is 0
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct ResyncRequestMsg {
    /// Reason for requesting re-sync.
    pub reason: ResyncReason,
    /// Resume from this mutation ID (0 = full re-sync).
    pub from_mutation_id: u64,
    /// Collection scope (empty = all collections).
    pub collection: String,
}

/// Reason for a re-sync request.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub enum ResyncReason {
    /// Detected missing mutation IDs in the delta stream.
    SequenceGap {
        /// The expected next mutation ID.
        expected: u64,
        /// The mutation ID that was actually received.
        received: u64,
    },
    /// CRC32C checksum mismatch on a delta payload.
    ChecksumMismatch {
        /// The mutation ID of the corrupted delta.
        mutation_id: u64,
    },
    /// Corruption detected on cold start, need full re-sync.
    CorruptedState,
}

/// Downstream throttle message (client → server, 0x52).
///
/// Sent by Lite when its incoming shape delta queue is overwhelmed.
/// Origin should reduce its push rate for this peer until a
/// `Throttle { throttle: false }` is received.
#[derive(
    Debug, Clone, Serialize, Deserialize, zerompk::ToMessagePack, zerompk::FromMessagePack,
)]
pub struct ThrottleMsg {
    /// `true` to enable throttling, `false` to release.
    pub throttle: bool,
    /// Current queue depth at Lite (informational).
    pub queue_depth: u64,
    /// Suggested max deltas per second (0 = use server default).
    pub suggested_rate: u64,
}
