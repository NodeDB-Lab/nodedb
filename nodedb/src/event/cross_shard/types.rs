//! Cross-shard event delivery types.
//!
//! Serialized as MessagePack inside `VShardEnvelope.payload` for
//! transport-agnostic cross-node delivery via QUIC.

use serde::{Deserialize, Serialize};

/// Request to execute a write on a remote shard.
///
/// Packaged by the source Event Plane, sent via `VShardEnvelope(CrossShardEvent)`,
/// received and executed by the target Event Plane's `CrossShardReceiver`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrossShardWriteRequest {
    /// SQL statement to execute on the target shard.
    pub sql: String,
    /// Tenant context for the execution.
    pub tenant_id: u32,
    /// Source vShard that generated this event (for HWM dedup).
    pub source_vshard: u16,
    /// Source LSN — used for high-water-mark dedup on the target.
    /// Events with `source_lsn <= hwm[source_vshard]` are duplicates.
    pub source_lsn: u64,
    /// Source sequence number — monotonic per (core, collection).
    pub source_sequence: u64,
    /// Cascade depth to prevent infinite trigger chains.
    pub cascade_depth: u32,
    /// Source collection that triggered this cross-shard write.
    pub source_collection: String,
    /// Target vShard ID for routing verification on the receiver.
    pub target_vshard: u16,
}

/// Response from the target shard after processing a cross-shard write.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrossShardWriteResponse {
    /// Whether the write was successfully executed.
    pub success: bool,
    /// If the write was a duplicate (HWM dedup), this is true.
    /// The sender should NOT retry duplicates.
    pub duplicate: bool,
    /// Error message if `success` is false and `duplicate` is false.
    pub error: String,
    /// The source_lsn echoed back for correlation.
    pub source_lsn: u64,
}

impl CrossShardWriteResponse {
    pub fn ok(source_lsn: u64) -> Self {
        Self {
            success: true,
            duplicate: false,
            error: String::new(),
            source_lsn,
        }
    }

    pub fn duplicate(source_lsn: u64) -> Self {
        Self {
            success: true,
            duplicate: true,
            error: String::new(),
            source_lsn,
        }
    }

    pub fn error(source_lsn: u64, error: String) -> Self {
        Self {
            success: false,
            duplicate: false,
            error,
            source_lsn,
        }
    }
}

/// A NOTIFY broadcast message sent to all peers for cluster-wide delivery.
///
/// When a node publishes a NOTIFY (via `ChangeStream.publish()`), it also
/// broadcasts this message to all peer nodes via `VShardEnvelope(NotifyBroadcast)`.
/// Each peer delivers the event to its local `ChangeStream` for LISTEN subscribers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotifyBroadcastMsg {
    /// The originating node ID (for dedup — don't re-broadcast our own events).
    pub source_node: u64,
    /// Monotonic sequence on the source node (for dedup on receiver).
    pub sequence: u64,
    /// Tenant that published the NOTIFY.
    pub tenant_id: u32,
    /// Collection affected.
    pub collection: String,
    /// Document ID affected.
    pub document_id: String,
    /// Operation type: "INSERT", "UPDATE", "DELETE".
    pub operation: String,
    /// Timestamp (epoch milliseconds).
    pub timestamp_ms: u64,
    /// LSN from the source node's WAL.
    pub lsn: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_roundtrip() {
        let req = CrossShardWriteRequest {
            sql: "INSERT INTO audit_log (event) VALUES ('created')".into(),
            tenant_id: 1,
            source_vshard: 3,
            source_lsn: 1500,
            source_sequence: 42,
            cascade_depth: 0,
            source_collection: "orders".into(),
            target_vshard: 7,
        };
        let bytes = rmp_serde::to_vec(&req).unwrap();
        let decoded: CrossShardWriteRequest = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(decoded.sql, req.sql);
        assert_eq!(decoded.source_lsn, 1500);
        assert_eq!(decoded.source_vshard, 3);
    }

    #[test]
    fn response_roundtrip() {
        let resp = CrossShardWriteResponse::ok(1500);
        let bytes = rmp_serde::to_vec(&resp).unwrap();
        let decoded: CrossShardWriteResponse = rmp_serde::from_slice(&bytes).unwrap();
        assert!(decoded.success);
        assert!(!decoded.duplicate);
        assert_eq!(decoded.source_lsn, 1500);
    }

    #[test]
    fn response_variants() {
        let dup = CrossShardWriteResponse::duplicate(100);
        assert!(dup.success);
        assert!(dup.duplicate);

        let err = CrossShardWriteResponse::error(100, "shard unavailable".into());
        assert!(!err.success);
        assert!(!err.duplicate);
        assert_eq!(err.error, "shard unavailable");
    }

    #[test]
    fn notify_broadcast_roundtrip() {
        let msg = NotifyBroadcastMsg {
            source_node: 1,
            sequence: 42,
            tenant_id: 5,
            collection: "orders".into(),
            document_id: "o-123".into(),
            operation: "INSERT".into(),
            timestamp_ms: 1700000000000,
            lsn: 500,
        };
        let bytes = rmp_serde::to_vec(&msg).unwrap();
        let decoded: NotifyBroadcastMsg = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(decoded.source_node, 1);
        assert_eq!(decoded.collection, "orders");
        assert_eq!(decoded.lsn, 500);
    }
}
