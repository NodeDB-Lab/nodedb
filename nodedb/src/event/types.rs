// SPDX-License-Identifier: BUSL-1.1

//! Event types emitted by the Data Plane and consumed by the Event Plane.
//!
//! Events carry **full row data** (new_value + old_value) as `Arc<[u8]>` pointing
//! to the WAL payload buffer — zero-copy from WAL to event bus (refcount bump,
//! no memcpy). This is critical: because the Event Plane is asynchronous, a
//! lazy-fetch from live storage would read a stale or overwritten row if a
//! subsequent write landed between event emission and processing.

use std::sync::Arc;

use nodedb_types::RowIdentity;
use sonic_rs;

use crate::types::{DatabaseId, Lsn, TenantId, VShardId};

/// The row an event names, as the client sees it.
///
/// A `Row` carries the same [`RowIdentity`] INSERT minted for the row: the
/// declared `PRIMARY KEY` value when the collection declares one, else the
/// decimal surrogate. The live emit path and WAL replay both build it from
/// that identity, never from a storage key.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RowId {
    /// A document, KV, or graph-node row.
    Row(RowIdentity),
    /// A KV batch op that names no single row.
    Batch,
    /// A graph edge, named by its endpoints and label. Boxed so every
    /// event on the ring pays for one identity, not four strings.
    Edge(Box<EdgeRowId>),
    /// An idle heartbeat, which names no row.
    Heartbeat,
}

/// Text a [`RowId::Batch`] renders as.
const BATCH_ROW_ID: &str = "_batch";

/// A graph edge's `(src, label, dst)` identity with its composite text.
///
/// The text is what [`crate::event::graph_cdc::edge_row_id`] builds, rendered
/// once at construction so `as_str` allocates nothing.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EdgeRowId {
    src: String,
    label: String,
    dst: String,
    rendered: String,
}

impl EdgeRowId {
    pub fn src(&self) -> &str {
        &self.src
    }

    pub fn label(&self) -> &str {
        &self.label
    }

    pub fn dst(&self) -> &str {
        &self.dst
    }

    /// The composite `src\u{1}label\u{1}dst` text.
    pub fn as_str(&self) -> &str {
        &self.rendered
    }
}

impl RowId {
    /// Name a single row by its client identity.
    pub fn row(identity: RowIdentity) -> Self {
        Self::Row(identity)
    }

    /// Name a graph edge by its `(src, label, dst)` triple.
    pub fn edge(src: impl Into<String>, label: impl Into<String>, dst: impl Into<String>) -> Self {
        let src = src.into();
        let label = label.into();
        let dst = dst.into();
        let rendered = crate::event::graph_cdc::edge_row_id(&src, &label, &dst);
        Self::Edge(Box::new(EdgeRowId {
            src,
            label,
            dst,
            rendered,
        }))
    }

    /// The row id as text, without allocating.
    ///
    /// `Row` yields the identity text, `Batch` yields `"_batch"`, `Edge`
    /// yields the rendered composite, and `Heartbeat` yields `""`.
    pub fn as_str(&self) -> &str {
        match self {
            Self::Row(identity) => identity.as_str(),
            Self::Batch => BATCH_ROW_ID,
            Self::Edge(edge) => edge.as_str(),
            Self::Heartbeat => "",
        }
    }
}

impl std::fmt::Display for RowId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
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

    /// The WAL record this event reproduces, when the write has one. WAL
    /// catch-up rebuilds the events of such a record, so the Event Plane names
    /// an event by its record position and row to deliver it once. `None` for
    /// a write the WAL does not carry: only its ring copy ever arrives.
    pub record: Option<RecordPosition>,

    /// Database context. Producers will propagate the selected database in the
    /// next CDC scoping slice; existing construction sites use `DEFAULT`.
    pub database_id: DatabaseId,

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

    /// `_ts_system` extracted from the new (or old, on delete) row payload.
    /// `None` for non-bitemporal collections, heartbeats, and bulk-summary
    /// events whose payload is intentionally absent.
    pub system_time_ms: Option<i64>,

    /// `_ts_valid_from` extracted from the new (or old, on delete) row
    /// payload. `None` under the same conditions as `system_time_ms`.
    pub valid_time_ms: Option<i64>,

    /// The authenticated user ID that originated this write, if available.
    ///
    /// Populated from `Request.user_id` for user-originated DML. `None` for
    /// trigger, CRDT sync, Raft follower, and deferred writes.
    pub user_id: Option<Arc<str>>,

    /// The SQL statement digest (plan digest) that produced this write, if available.
    ///
    /// Populated from `Request.statement_digest` (which reuses the plan digest
    /// already computed by nodedb-sql). `None` for non-user writes.
    pub statement_digest: Option<Arc<str>>,
}

/// Where an event sits in the WAL record it reproduces.
///
/// A record can write one row more than once (a transaction's redo). The
/// ring and WAL catch-up both number those events per row, in record order,
/// so each names the same event the same way.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RecordPosition {
    /// LSN of the WAL record.
    pub lsn: Lsn,
    /// How many earlier events of the same record name the same row and the
    /// same kind of write (a delete, or an insert or update).
    pub occurrence: u32,
}

impl RecordPosition {
    /// The first event of record `lsn` on its row.
    pub fn first(lsn: Lsn) -> Self {
        Self { lsn, occurrence: 0 }
    }
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
///
/// Serde names match [`EventSource::as_str`]. CDC events carry the source
/// under those names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
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
    /// A row a RESTORE re-issued from a backup. AFTER triggers do not fire:
    /// they fired when the row was first written. CDC streams deliver the
    /// event tagged `restore`. Consumers that keep derived state in step
    /// with the base data process it.
    Restore,
}

impl EventSource {
    /// The source's stable name, as CDC events and logs show it.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Trigger => "trigger",
            Self::RaftFollower => "raft_follower",
            Self::CrdtSync => "crdt_sync",
            Self::Deferred => "deferred",
            Self::Restore => "restore",
        }
    }

    /// The source named `name`, as [`Self::as_str`] spells it.
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "user" => Some(Self::User),
            "trigger" => Some(Self::Trigger),
            "raft_follower" => Some(Self::RaftFollower),
            "crdt_sync" => Some(Self::CrdtSync),
            "deferred" => Some(Self::Deferred),
            "restore" => Some(Self::Restore),
            _ => None,
        }
    }
}

impl std::fmt::Display for EventSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
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
    if let Ok(serde_json::Value::Object(map)) = nodedb_types::json_from_msgpack(bytes) {
        return Some(map);
    }
    if let Ok(serde_json::Value::Object(map)) = sonic_rs::from_slice::<serde_json::Value>(bytes) {
        return Some(map);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_id_display() {
        let id = RowId::row(RowIdentity::from_user_key("doc-123"));
        assert_eq!(id.as_str(), "doc-123");
        assert_eq!(id.to_string(), "doc-123");
    }

    #[test]
    fn row_id_surrogate_identity_is_decimal() {
        let id = RowId::row(RowIdentity::for_surrogate(nodedb_types::Surrogate::new(9)));
        assert_eq!(id.as_str(), "9");
    }

    #[test]
    fn row_id_batch_and_heartbeat_text() {
        assert_eq!(RowId::Batch.as_str(), "_batch");
        assert_eq!(RowId::Heartbeat.as_str(), "");
    }

    #[test]
    fn row_id_edge_matches_graph_cdc_composition() {
        let id = RowId::edge("a", "KNOWS", "b");
        assert_eq!(
            id.as_str(),
            crate::event::graph_cdc::edge_row_id("a", "KNOWS", "b").as_str()
        );
        match id {
            RowId::Edge(edge) => {
                assert_eq!(edge.src(), "a");
                assert_eq!(edge.label(), "KNOWS");
                assert_eq!(edge.dst(), "b");
            }
            other => panic!("expected edge row id, got {other:?}"),
        }
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
        assert_eq!(EventSource::Restore.to_string(), "restore");
    }

    #[test]
    fn every_event_source_name_round_trips_through_serde_and_from_name() {
        for source in [
            EventSource::User,
            EventSource::Trigger,
            EventSource::RaftFollower,
            EventSource::CrdtSync,
            EventSource::Deferred,
            EventSource::Restore,
        ] {
            assert_eq!(EventSource::from_name(source.as_str()), Some(source));
            let json = sonic_rs::to_string(&source).expect("encode source");
            assert_eq!(json, format!("\"{}\"", source.as_str()));
            let decoded: EventSource = sonic_rs::from_str(&json).expect("decode source");
            assert_eq!(decoded, source);
        }
        assert_eq!(EventSource::from_name("unknown"), None);
    }

    #[test]
    fn write_event_construction() {
        let event = WriteEvent {
            sequence: 1,
            collection: Arc::from("orders"),
            op: WriteOp::Insert,
            row_id: RowId::row(RowIdentity::from_user_key("order-1")),
            lsn: Lsn::new(100),
            record: None,
            database_id: DatabaseId::DEFAULT,
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            source: EventSource::User,
            new_value: Some(Arc::from(b"payload".as_slice())),
            old_value: None,
            system_time_ms: None,
            valid_time_ms: None,
            user_id: None,
            statement_digest: None,
        };
        assert_eq!(event.sequence, 1);
        assert_eq!(event.op, WriteOp::Insert);
        assert!(event.new_value.is_some());
        assert!(event.old_value.is_none());
    }
}
