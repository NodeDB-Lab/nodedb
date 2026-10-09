// SPDX-License-Identifier: BUSL-1.1

//! Cross-shard event delivery types.
//!
//! Serialized as MessagePack inside `VShardEnvelope.payload` for
//! transport-agnostic cross-node delivery via QUIC.

/// Request to execute a write on a remote shard.
///
/// Packaged by the source Event Plane, sent via `VShardEnvelope(CrossShardEvent)`,
/// received and executed by the target Event Plane's `CrossShardReceiver`.
///
/// The current wire encoding is a versioned MessagePack map: receivers accept
/// unknown fields so later versions remain forward-compatible. The receiver
/// separately recognizes the exact legacy positional encoding; malformed
/// payloads that match neither representation are rejected.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map, allow_unknown_fields)]
pub struct CrossShardWriteRequest {
    /// SQL statement to execute on the target shard.
    pub sql: String,
    /// Tenant context for the execution.
    pub tenant_id: u64,
    /// Database context for the trigger body execution.
    #[msgpack(default)]
    pub database_id: u64,
    /// vShard that owns the source write.
    pub source_vshard: u32,
    /// LSN of the source write.
    pub source_lsn: u64,
    /// Sequence number of the source write — monotonic per (core, collection).
    pub source_sequence: u64,
    /// Body that emitted this request within the source write, such as
    /// `trigger/<database>/<name>`. The receiver deduplicates on
    /// `(source_vshard, source_lsn, source_sequence, origin)`.
    #[msgpack(default)]
    pub origin: String,
    /// Cascade depth to prevent infinite trigger chains.
    pub cascade_depth: u32,
    /// Source collection that triggered this cross-shard write.
    pub source_collection: String,
    /// Target vShard ID for routing verification on the receiver.
    pub target_vshard: u32,
}

/// Response from the target shard after processing a cross-shard write.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct CrossShardWriteResponse {
    /// Whether the write was successfully executed.
    pub success: bool,
    /// True when the request's dedup key already applied on the target.
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

/// A stretch of one Control-Plane change-stream partition, forwarded by the
/// node that leads the partition to every node that does not replicate it.
///
/// The sender holds every event of the partition in `(after, through]`, and
/// `changes` are all of them. The receiver appends the changes it does not
/// hold, and records a hole when `after` lies above what it saw.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct NotifyBroadcastMsg {
    /// The forwarding node.
    pub source_node: u64,
    /// [`NOTIFY_PARTITION_GROUP`]. A receiver drops a run of any other kind.
    pub partition_kind: u8,
    /// The data group.
    pub partition_id: u64,
    pub after: crate::event::cdc::CdcOffset,
    pub through: crate::event::cdc::CdcOffset,
    pub changes: Vec<NotifyChange>,
}

/// `NotifyBroadcastMsg::partition_kind` of a data group's feed.
pub const NOTIFY_PARTITION_GROUP: u8 = 0;

/// One forwarded change at its position.
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct NotifyChange {
    pub position: crate::event::cdc::CdcOffset,
    pub tenant_id: u64,
    pub database_id: u64,
    pub collection: String,
    pub document_id: String,
    /// `INSERT`, `UPDATE`, or `DELETE`.
    pub operation: String,
    /// Epoch milliseconds.
    pub timestamp_ms: u64,
    /// LSN from the applying node's WAL. Observability only.
    pub lsn: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn current_request_roundtrip() {
        let req = CrossShardWriteRequest {
            sql: "INSERT INTO audit_log (event) VALUES ('created')".into(),
            tenant_id: 1,
            database_id: 42,
            source_vshard: 3,
            source_lsn: 1500,
            source_sequence: 42,
            origin: "trigger/42/audit".into(),
            cascade_depth: 0,
            source_collection: "orders".into(),
            target_vshard: 7,
        };
        let bytes = zerompk::to_msgpack_vec(&req).unwrap();
        let decoded: CrossShardWriteRequest = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded.sql, req.sql);
        assert_eq!(decoded.source_lsn, 1500);
        assert_eq!(decoded.source_vshard, 3);
        assert_eq!(decoded.database_id, 42);
        assert_eq!(decoded.origin, "trigger/42/audit");
    }

    #[test]
    fn response_roundtrip() {
        let resp = CrossShardWriteResponse::ok(1500);
        let bytes = zerompk::to_msgpack_vec(&resp).unwrap();
        let decoded: CrossShardWriteResponse = zerompk::from_msgpack(&bytes).unwrap();
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
        use crate::event::cdc::CdcOffset;
        let msg = NotifyBroadcastMsg {
            source_node: 1,
            partition_kind: NOTIFY_PARTITION_GROUP,
            partition_id: 7,
            after: CdcOffset::whole_index(9),
            through: CdcOffset::whole_index(10),
            changes: vec![NotifyChange {
                position: CdcOffset::data_event(0, 10, 1),
                tenant_id: 5,
                database_id: 1024,
                collection: "orders".into(),
                document_id: "o-123".into(),
                operation: "INSERT".into(),
                timestamp_ms: 1700000000000,
                lsn: 500,
            }],
        };
        let bytes = zerompk::to_msgpack_vec(&msg).unwrap();
        let decoded: NotifyBroadcastMsg = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded.source_node, 1);
        assert_eq!(decoded.partition_id, 7);
        assert_eq!(decoded.after, CdcOffset::whole_index(9));
        assert_eq!(decoded.changes[0].position, CdcOffset::data_event(0, 10, 1));
        assert_eq!(decoded.changes[0].collection, "orders");
    }
}
