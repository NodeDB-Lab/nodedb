//! Event types emitted by the Data Plane and consumed by the Event Plane.
//!
//! Events carry **full row data** (new_value + old_value) as `Arc<[u8]>` pointing
//! to the WAL payload buffer — zero-copy from WAL to event bus (refcount bump,
//! no memcpy). This is critical: because the Event Plane is asynchronous, a
//! lazy-fetch from live storage would read a stale or overwritten row if a
//! subsequent write landed between event emission and processing.

use std::sync::Arc;

use crate::types::{Lsn, TenantId, VShardId};

/// Identifies a row within a collection. Wraps the document/row ID string.
///
/// Separate from `DocumentId` (nodedb-types) because event row IDs are
/// ephemeral references into the event payload, not owned document handles.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RowId(pub Arc<str>);

impl RowId {
    pub fn new(id: impl Into<Arc<str>>) -> Self {
        Self(id.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for RowId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A write event emitted by the Data Plane after a successful write.
///
/// Contains the full row data at the time of the write, serialized in the
/// same format as the WAL payload (MessagePack or Binary Tuple).
#[derive(Debug, Clone)]
pub struct WriteEvent {
    /// Monotonic sequence number per (core, collection). Used for ordering
    /// and deduplication. The Event Plane detects gaps and triggers WAL replay.
    pub sequence: u64,

    /// Which collection was written.
    pub collection: Arc<str>,

    /// Operation type.
    pub op: WriteOp,

    /// Primary key or document ID of the affected row(s).
    pub row_id: RowId,

    /// WAL LSN for this write. Enables replay from WAL on Event Plane restart.
    pub lsn: Lsn,

    /// Tenant context.
    pub tenant_id: TenantId,

    /// vShard that owns this data.
    pub vshard_id: VShardId,

    /// Whether this write was from a trigger side effect (prevents re-triggering).
    pub source: EventSource,

    /// The new row data at the time of the write.
    /// Present for INSERT and UPDATE operations.
    ///
    /// Ownership: points to the serialized payload (MessagePack or Binary Tuple).
    /// In future batches, this will be an `Arc<[u8]>` sub-slice of a frozen WAL
    /// slab. For now, it is a copied payload — zero-copy slab integration comes
    /// when the WAL slab allocator is wired into the event bus.
    pub new_value: Option<Arc<[u8]>>,

    /// The old row data before the write.
    /// Present for UPDATE and DELETE operations.
    pub old_value: Option<Arc<[u8]>>,
}

/// The type of write operation that generated this event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteOp {
    /// Single row inserted.
    Insert,
    /// Single row updated (old_value + new_value both present).
    Update,
    /// Single row deleted (old_value present, new_value absent).
    Delete,
    /// Multiple rows inserted in a batch.
    BulkInsert { count: u32 },
    /// Multiple rows deleted in a batch.
    BulkDelete { count: u32 },
    /// Idle heartbeat: emitted by the Data Plane when no user writes occur
    /// for >1 second. Carries the current LSN and wall-clock timestamp.
    /// Advances partition watermarks without triggering CDC/triggers/MVs.
    Heartbeat,
}

impl WriteOp {
    /// Whether this operation should trigger CDC routing, triggers, and MVs.
    /// Heartbeats only advance watermarks — they are NOT data events.
    pub fn is_data_event(&self) -> bool {
        !matches!(self, Self::Heartbeat)
    }
}

impl std::fmt::Display for WriteOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Insert => write!(f, "INSERT"),
            Self::Update => write!(f, "UPDATE"),
            Self::Delete => write!(f, "DELETE"),
            Self::BulkInsert { count } => write!(f, "BULK_INSERT({count})"),
            Self::BulkDelete { count } => write!(f, "BULK_DELETE({count})"),
            Self::Heartbeat => write!(f, "HEARTBEAT"),
        }
    }
}

/// Source of a write event. The Event Plane uses this to decide whether
/// to fire AFTER triggers and other side effects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventSource {
    /// User-originated DML. AFTER triggers should fire.
    User,
    /// Trigger-generated side effect. Do NOT re-trigger (prevents loops).
    Trigger,
    /// Replicated via Raft (follower apply). Do NOT trigger.
    RaftFollower,
    /// Replicated via CRDT sync. Do NOT trigger.
    CrdtSync,
    /// Deferred trigger write. The Event Plane fires DEFERRED-mode triggers
    /// for these events (post-commit from transaction batch).
    Deferred,
}

impl std::fmt::Display for EventSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::User => write!(f, "user"),
            Self::Trigger => write!(f, "trigger"),
            Self::RaftFollower => write!(f, "raft_follower"),
            Self::CrdtSync => write!(f, "crdt_sync"),
            Self::Deferred => write!(f, "deferred"),
        }
    }
}

/// Deserialize a MessagePack or JSON payload into a [`serde_json::Map`].
///
/// WriteEvent payloads are stored in the same format as the WAL payload
/// (MessagePack for schemaless documents, Binary Tuple for strict).
/// Tries MessagePack first, then JSON fallback. Returns `None` if neither
/// succeeds (e.g. Binary Tuple payloads that need schema-aware decoding).
pub fn deserialize_event_payload(
    bytes: &[u8],
) -> Option<serde_json::Map<String, serde_json::Value>> {
    if let Ok(serde_json::Value::Object(map)) = rmp_serde::from_slice::<serde_json::Value>(bytes) {
        return Some(map);
    }
    if let Ok(serde_json::Value::Object(map)) = serde_json::from_slice::<serde_json::Value>(bytes) {
        return Some(map);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_id_display() {
        let id = RowId::new("doc-123");
        assert_eq!(id.as_str(), "doc-123");
        assert_eq!(id.to_string(), "doc-123");
    }

    #[test]
    fn write_op_display() {
        assert_eq!(WriteOp::Insert.to_string(), "INSERT");
        assert_eq!(
            WriteOp::BulkInsert { count: 42 }.to_string(),
            "BULK_INSERT(42)"
        );
    }

    #[test]
    fn event_source_display() {
        assert_eq!(EventSource::User.to_string(), "user");
        assert_eq!(EventSource::RaftFollower.to_string(), "raft_follower");
    }

    #[test]
    fn write_event_construction() {
        let event = WriteEvent {
            sequence: 1,
            collection: Arc::from("orders"),
            op: WriteOp::Insert,
            row_id: RowId::new("order-1"),
            lsn: Lsn::new(100),
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            source: EventSource::User,
            new_value: Some(Arc::from(b"payload".as_slice())),
            old_value: None,
        };
        assert_eq!(event.sequence, 1);
        assert_eq!(event.op, WriteOp::Insert);
        assert!(event.new_value.is_some());
        assert!(event.old_value.is_none());
    }
}
