// SPDX-License-Identifier: BUSL-1.1

//! CRDT sync coordination types.
//!
//! Types for the Event Plane's role as intermediary between Origin writes
//! and Lite device delta delivery.

use crate::types::DatabaseId;

/// An outbound CRDT delta ready for delivery to connected Lite instances.
///
/// Packaged by the Event Plane's packager from a `WriteEvent`. Contains
/// the full row data serialized as the CRDT delta payload (MessagePack).
#[derive(Debug, Clone, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct OutboundDelta {
    /// Database containing the source collection. Used exclusively for Origin
    /// session routing and is not added to the RowPush wire payload.
    pub database_id: DatabaseId,
    /// Collection the delta applies to.
    pub collection: String,
    /// Document/row ID.
    pub document_id: String,
    /// Serialized row data (MessagePack). For INSERT/UPDATE, this is
    /// the new_value. For DELETE, this is empty (tombstone).
    pub payload: Vec<u8>,
    /// The row's pre-image (MessagePack) for an UPDATE or DELETE. `None` for
    /// an INSERT, and for a write whose event carried no pre-image. The
    /// push-time checks use it to decide whether the subscriber saw the row
    /// before the write. It never goes out on the wire.
    pub prior_payload: Option<Vec<u8>>,
    /// Which sessions the delta reaches.
    pub audience: DeltaAudience,
    /// Write operation type.
    pub op: DeltaOp,
    /// WAL LSN on the source node. Used for ordering and dedup.
    pub lsn: u64,
    /// Tenant context.
    pub tenant_id: u64,
    /// Origin node's peer ID for CRDT identity attribution.
    pub peer_id: u64,
    /// Monotonic sequence per collection for ordering enforcement.
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

/// Which sessions an outbound delta reaches.
#[derive(Debug, Clone, Copy, PartialEq, Eq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[repr(u8)]
#[msgpack(c_enum)]
pub enum DeltaAudience {
    /// A user-collection row. It reaches a session only when a shape of the
    /// session names the collection. The session's push-time read checks
    /// then judge the row.
    Subscribers = 0,
    /// A DDL-managed definition notice (`_alert_rules`, `_retention_policies`,
    /// `_schedules`). It reaches only a session of the same tenant and
    /// database whose principal is a superuser or tenant admin: the privilege
    /// the DDL that wrote it requires.
    TenantAdmins = 1,
}

/// The pseudo-collection a family of DDL-managed definitions syncs under.
/// A [`DeltaAudience::TenantAdmins`] notice names one of these.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DefinitionFamily {
    AlertRules,
    RetentionPolicies,
    Schedules,
}

impl DefinitionFamily {
    /// Every family.
    pub const ALL: [Self; 3] = [Self::AlertRules, Self::RetentionPolicies, Self::Schedules];

    /// The pseudo-collection name the family's notices carry.
    pub const fn collection(self) -> &'static str {
        match self {
            Self::AlertRules => "_alert_rules",
            Self::RetentionPolicies => "_retention_policies",
            Self::Schedules => "_schedules",
        }
    }

    /// The family a notice's collection names, if any.
    pub fn from_collection(collection: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|family| family.collection() == collection)
    }
}

/// A control frame for one session, with the scope and collection it
/// concerns. The session checks both again when it pushes the frame.
#[derive(Debug, Clone)]
pub struct ControlFrame {
    pub tenant_id: u64,
    pub database_id: DatabaseId,
    /// The collection the frame concerns.
    pub collection: String,
    pub frame: nodedb_types::sync::wire::SyncFrame,
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
    pub tenant_id: u64,
    /// Database this session is authorized to receive deltas from.
    pub database_id: DatabaseId,
    /// The session's authorized shapes and held collections. Empty means the
    /// session receives no row push and no purge notice.
    pub subscriptions: std::sync::Arc<super::subscriptions::SharedSubscriptions>,
    /// Collections whose deltas the session's full queue dropped. The session
    /// re-snapshots its shapes on them, so the client does not stay diverged.
    pub resync: std::sync::Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
    /// Definition families whose notices the session's full admin queue
    /// dropped. The session sends the family's full current set.
    pub admin_resync: std::sync::Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
    /// Channel for pushing deltas to the session writer task.
    pub sender: tokio::sync::mpsc::Sender<OutboundDelta>,
    /// Channel for pushing control frames (`CollectionPurged`,
    /// stream-termination, etc.) that aren't per-row deltas. Kept on
    /// a separate channel so a saturated delta queue never drops a
    /// purge notification.
    pub control_sender: tokio::sync::mpsc::Sender<ControlFrame>,
    /// Channel for [`DeltaAudience::TenantAdmins`] notices. Present only
    /// once the session registered it, which it does only while its
    /// principal is a superuser or tenant admin. A row delta never fills it,
    /// and a notice never fills the row queue.
    pub admin_sender: Option<tokio::sync::mpsc::Sender<OutboundDelta>>,
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
            database_id: DatabaseId::new(7),
            collection: "orders".into(),
            document_id: "o-123".into(),
            payload: vec![1, 2, 3, 4],
            prior_payload: Some(vec![5, 6]),
            audience: DeltaAudience::Subscribers,
            op: DeltaOp::Upsert,
            lsn: 1500,
            tenant_id: 1,
            peer_id: 42,
            sequence: 7,
        };
        let bytes = zerompk::to_msgpack_vec(&delta).unwrap();
        let decoded: OutboundDelta = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(decoded.database_id, DatabaseId::new(7));
        assert_eq!(decoded.collection, "orders");
        assert_eq!(decoded.lsn, 1500);
        assert_eq!(decoded.op, DeltaOp::Upsert);
        assert_eq!(decoded.prior_payload, Some(vec![5, 6]));
        assert_eq!(decoded.audience, DeltaAudience::Subscribers);
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
