//! CRDT sync coordination types.
//!
//! Types for the Event Plane's role as intermediary between Origin writes
//! and Lite device delta delivery.

/// An outbound CRDT delta ready for delivery to connected Lite instances.
///
/// Packaged by the Event Plane's packager from a `WriteEvent`. Contains
/// the full row data serialized as the CRDT delta payload (MessagePack).
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct OutboundDelta {
    /// Collection the delta applies to.
    pub collection: String,
    /// Document/row ID.
    pub document_id: String,
    /// Serialized row data (MessagePack). For INSERT/UPDATE, this is
    /// the new_value. For DELETE, this is empty (tombstone).
    pub payload: Vec<u8>,
    /// Write operation type.
    pub op: DeltaOp,
    /// WAL LSN on the source node. Used for ordering and dedup.
    pub lsn: u64,
    /// Tenant context.
    pub tenant_id: u32,
    /// Origin node's peer ID for CRDT identity attribution.
    pub peer_id: u64,
    /// Monotonic sequence per (collection) for ordering enforcement.
    pub sequence: u64,
}

/// Operation type for outbound deltas.
#[derive(Debug, Clone, Copy, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[repr(u8)]
#[msgpack(c_enum)]
pub enum DeltaOp {
    Upsert = 0,
    Delete = 1,
}

/// Handle to a connected Lite session for delta delivery.
///
/// The Event Plane holds these to push deltas. The session writer
/// (in the sync listener) receives via the mpsc channel.
#[derive(Debug, Clone)]
pub struct LiteSessionHandle {
    /// Unique session identifier (e.g., "sync-192.168.1.5:34212-7").
    pub session_id: String,
    /// Lite device's CRDT peer ID.
    pub peer_id: u64,
    /// Tenant this session belongs to.
    pub tenant_id: u32,
    /// Collections this Lite device has subscribed to (via shape subscriptions).
    /// Empty = all collections for the tenant.
    pub subscribed_collections: Vec<String>,
    /// Channel for pushing deltas to the session writer task.
    pub sender: tokio::sync::mpsc::Sender<OutboundDelta>,
}

/// Configuration for CRDT sync delivery.
#[derive(Debug, Clone)]
pub struct DeliveryConfig {
    /// Maximum pending deltas per session before backpressure.
    pub max_queue_per_session: usize,
    /// How often the delivery task drains queues (milliseconds).
    pub drain_interval_ms: u64,
}

impl Default for DeliveryConfig {
    fn default() -> Self {
        Self {
            max_queue_per_session: 10_000,
            drain_interval_ms: 50,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outbound_delta_roundtrip() {
        let delta = OutboundDelta {
            collection: "orders".into(),
            document_id: "o-123".into(),
            payload: vec![1, 2, 3, 4],
            op: DeltaOp::Upsert,
            lsn: 1500,
            tenant_id: 1,
            peer_id: 42,
            sequence: 7,
        };
        let bytes = zerompk::to_msgpack_vec(&delta).unwrap();
        let decoded: OutboundDelta = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded.collection, "orders");
        assert_eq!(decoded.lsn, 1500);
        assert_eq!(decoded.op, DeltaOp::Upsert);
    }

    #[test]
    fn delta_op_variants() {
        let upsert = DeltaOp::Upsert;
        let delete = DeltaOp::Delete;
        assert_ne!(upsert, delete);
    }

    #[test]
    fn default_config() {
        let config = DeliveryConfig::default();
        assert_eq!(config.max_queue_per_session, 10_000);
        assert_eq!(config.drain_interval_ms, 50);
    }
}
