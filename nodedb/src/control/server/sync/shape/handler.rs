//! Shape subscribe/delta/unsubscribe handlers and compaction.
//!
//! - ShapeSubscribe (0x20): client registers a shape, receives initial snapshot
//! - ShapeDelta (0x22): server pushes incremental shape-matched mutations
//! - ShapeUnsubscribe (0x23): client deregisters a shape
//! - Compaction: periodically merges delta chains into snapshots
//! - Historical delta resolution: handles WAL-truncated history via Loro ops

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tracing::{debug, info};
use zerompk::{FromMessagePack, ToMessagePack};

use super::definition::{ShapeDefinition, ShapeId};
use super::registry::ShapeRegistry;
use crate::control::server::sync::wire::*;

/// ShapeSubscribe request (client → server, 0x20).
#[derive(Debug, Clone, Serialize, Deserialize, ToMessagePack, FromMessagePack)]
pub struct ShapeSubscribeMsg {
    /// Shape definition to subscribe to.
    pub shape: ShapeDefinition,
}

/// ShapeSnapshot response (server → client, 0x21).
/// Sent after ShapeSubscribe with the initial dataset.
#[derive(Debug, Clone, Serialize, Deserialize, ToMessagePack, FromMessagePack)]
pub struct ShapeSnapshotMsg {
    /// Shape ID this snapshot belongs to.
    pub shape_id: ShapeId,
    /// Initial dataset: serialized document rows matching the shape.
    /// Format: MessagePack `Vec<(doc_id, doc_bytes)>`.
    pub data: Vec<u8>,
    /// LSN at snapshot time — deltas after this LSN will follow.
    pub snapshot_lsn: u64,
    /// Number of documents in the snapshot.
    pub doc_count: usize,
}

/// ShapeDelta message (server → client, 0x22).
/// Pushed when a mutation matches a subscribed shape.
#[derive(Debug, Clone, Serialize, Deserialize, ToMessagePack, FromMessagePack)]
pub struct ShapeDeltaMsg {
    /// Shape ID this delta applies to.
    pub shape_id: ShapeId,
    /// Collection affected.
    pub collection: String,
    /// Document ID affected.
    pub document_id: String,
    /// Operation type: "INSERT", "UPDATE", "DELETE".
    pub operation: String,
    /// Delta payload (CRDT delta bytes or document value).
    pub delta: Vec<u8>,
    /// WAL LSN of this mutation.
    pub lsn: u64,
}

/// ShapeUnsubscribe request (client → server, 0x23).
#[derive(Debug, Clone, Serialize, Deserialize, ToMessagePack, FromMessagePack)]
pub struct ShapeUnsubscribeMsg {
    pub shape_id: ShapeId,
}

/// Initial snapshot data returned by the snapshot provider.
#[derive(Debug, Clone)]
pub struct ShapeSnapshotData {
    /// Serialized documents/edges/vectors matching the shape.
    pub data: Vec<u8>,
    /// Number of documents in the snapshot.
    pub doc_count: usize,
}

impl ShapeSnapshotData {
    /// Empty snapshot (no matching documents).
    pub fn empty() -> Self {
        Self {
            data: Vec::new(),
            doc_count: 0,
        }
    }
}

/// Handle a ShapeSubscribe message.
///
/// Registers the shape in the registry, evaluates the initial dataset
/// via the provided `snapshot_provider`, and returns a ShapeSnapshot frame.
///
/// The `snapshot_provider` callback bridges to the Data Plane: it receives
/// the shape definition and current LSN, dispatches the appropriate query
/// (DocumentScan for document shapes, GraphHop for graph shapes, collection
/// scan for vector shapes), and returns the serialized result set. The
/// callback is synchronous because the SPSC bridge response is collected
/// before this function is called.
pub fn handle_subscribe<F>(
    session_id: &str,
    tenant_id: u32,
    msg: &ShapeSubscribeMsg,
    registry: &ShapeRegistry,
    current_lsn: u64,
    snapshot_provider: F,
) -> Option<SyncFrame>
where
    F: FnOnce(&ShapeDefinition, u64) -> ShapeSnapshotData,
{
    let shape = msg.shape.clone();
    let shape_id = shape.shape_id.clone();

    registry.subscribe(session_id, tenant_id, shape.clone());

    // Query the Data Plane for the initial dataset matching this shape.
    let snapshot_data = snapshot_provider(&shape, current_lsn);

    let snapshot = ShapeSnapshotMsg {
        shape_id,
        data: snapshot_data.data,
        snapshot_lsn: current_lsn,
        doc_count: snapshot_data.doc_count,
    };

    info!(
        session = session_id,
        shape_id = %msg.shape.shape_id,
        lsn = current_lsn,
        doc_count = snapshot.doc_count,
        "shape subscribed, snapshot sent"
    );

    SyncFrame::try_encode(SyncMessageType::ShapeSnapshot, &snapshot)
}

/// Handle a ShapeUnsubscribe message.
pub fn handle_unsubscribe(session_id: &str, msg: &ShapeUnsubscribeMsg, registry: &ShapeRegistry) {
    registry.unsubscribe(session_id, &msg.shape_id);
    debug!(session = session_id, shape_id = %msg.shape_id, "shape unsubscribed");
}

/// Evaluate a mutation against all shapes and generate ShapeDelta frames.
///
/// Called by the WAL tail loop when a committed mutation is observed.
/// Returns `(session_id, SyncFrame)` pairs for each matching subscription.
pub fn evaluate_and_generate_deltas(
    tenant_id: u32,
    collection: &str,
    doc_id: &str,
    operation: &str,
    delta: &[u8],
    lsn: u64,
    registry: &ShapeRegistry,
) -> Vec<(String, SyncFrame)> {
    let matches = registry.evaluate_mutation(tenant_id, collection, doc_id);

    matches
        .into_iter()
        .filter_map(|(session_id, shape_id)| {
            let msg = ShapeDeltaMsg {
                shape_id,
                collection: collection.to_string(),
                document_id: doc_id.to_string(),
                operation: operation.to_string(),
                delta: delta.to_vec(),
                lsn,
            };
            let frame = SyncFrame::new_msgpack(SyncMessageType::ShapeDelta, &msg)?;
            Some((session_id, frame))
        })
        .collect()
}

/// Shape compaction state: tracks delta chains per shape per session.
pub struct ShapeCompactor {
    /// Per-shape delta count since last compaction: `(session_id, shape_id) → count`.
    delta_counts: HashMap<(String, ShapeId), usize>,
    /// Compaction threshold: compact when delta count exceeds this.
    compact_threshold: usize,
    /// Maximum age of deltas before forced compaction (seconds).
    max_delta_age_secs: u64,
}

impl ShapeCompactor {
    pub fn new(compact_threshold: usize, max_delta_age_secs: u64) -> Self {
        Self {
            delta_counts: HashMap::new(),
            compact_threshold,
            max_delta_age_secs,
        }
    }

    /// Record a delta sent to a session's shape subscription.
    /// Returns true if compaction should be triggered.
    pub fn record_delta(&mut self, session_id: &str, shape_id: &str) -> bool {
        let key = (session_id.to_string(), shape_id.to_string());
        let count = self.delta_counts.entry(key).or_insert(0);
        *count += 1;
        *count >= self.compact_threshold
    }

    /// Reset the delta count after compaction.
    pub fn compaction_done(&mut self, session_id: &str, shape_id: &str) {
        let key = (session_id.to_string(), shape_id.to_string());
        self.delta_counts.remove(&key);
    }

    /// Maximum delta age in seconds before forced compaction.
    pub fn max_delta_age_secs(&self) -> u64 {
        self.max_delta_age_secs
    }

    /// Get all shapes that need compaction.
    pub fn shapes_needing_compaction(&self) -> Vec<(String, ShapeId)> {
        self.delta_counts
            .iter()
            .filter(|(_, count)| **count >= self.compact_threshold)
            .map(|((s, sh), _)| (s.clone(), sh.clone()))
            .collect()
    }
}

/// Resolve historical deltas when client's vector clock predates WAL truncation.
///
/// Strategy:
/// 1. Try CRDT engine's Loro history (survives WAL truncation)
/// 2. If Loro history also compacted past client's clock → full snapshot re-send
///
/// `oldest_wal_lsn`: oldest LSN still in the WAL
/// `client_lsn`: client's last-seen LSN for this collection
/// `loro_history_lsn`: oldest LSN retained in Loro's operation history
pub fn resolve_historical_delta(
    client_lsn: u64,
    oldest_wal_lsn: u64,
    loro_history_lsn: u64,
) -> HistoricalResolution {
    if client_lsn >= oldest_wal_lsn {
        // Client is within WAL range — normal delta from WAL.
        HistoricalResolution::WalDelta
    } else if client_lsn >= loro_history_lsn {
        // Client predates WAL but Loro history has the ops.
        // Generate delta from Loro operation history.
        HistoricalResolution::LoroDelta
    } else {
        // Both WAL and Loro history are truncated past client's clock.
        // Fall back to full shape snapshot re-send.
        HistoricalResolution::FullSnapshot
    }
}

/// How to resolve a client that's behind the WAL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistoricalResolution {
    /// Client within WAL range — replay WAL entries.
    WalDelta,
    /// Client predates WAL but within Loro history — use Loro ops.
    LoroDelta,
    /// Both truncated — full shape snapshot re-send.
    FullSnapshot,
}

#[cfg(test)]
mod tests {
    use super::super::definition::ShapeType;
    use super::*;

    fn make_shape(id: &str) -> ShapeDefinition {
        ShapeDefinition {
            shape_id: id.into(),
            tenant_id: 1,
            shape_type: ShapeType::Document {
                collection: "orders".into(),
                predicate: Vec::new(),
            },
            description: "test shape".into(),
            field_filter: vec![],
        }
    }

    #[test]
    fn subscribe_sends_snapshot() {
        let registry = ShapeRegistry::new();
        let msg = ShapeSubscribeMsg {
            shape: make_shape("sh1"),
        };

        let frame = handle_subscribe("s1", 1, &msg, &registry, 100, |_shape, _lsn| {
            ShapeSnapshotData {
                data: zerompk::to_msgpack_vec(&vec!["doc1", "doc2"]).unwrap_or_default(),
                doc_count: 2,
            }
        })
        .expect("encode");
        assert_eq!(frame.msg_type, SyncMessageType::ShapeSnapshot);

        let snapshot: ShapeSnapshotMsg = frame.decode_body().unwrap();
        assert_eq!(snapshot.shape_id, "sh1");
        assert_eq!(snapshot.snapshot_lsn, 100);
        assert_eq!(snapshot.doc_count, 2);
        assert!(!snapshot.data.is_empty());
        assert_eq!(registry.total_shapes(), 1);
    }

    #[test]
    fn unsubscribe_removes() {
        let registry = ShapeRegistry::new();
        registry.subscribe("s1", 1, make_shape("sh1"));
        assert_eq!(registry.total_shapes(), 1);

        handle_unsubscribe(
            "s1",
            &ShapeUnsubscribeMsg {
                shape_id: "sh1".into(),
            },
            &registry,
        );
        assert_eq!(registry.total_shapes(), 0);
    }

    #[test]
    fn evaluate_generates_deltas() {
        let registry = ShapeRegistry::new();
        registry.subscribe("s1", 1, make_shape("sh1"));

        let deltas = evaluate_and_generate_deltas(
            1,
            "orders",
            "o42",
            "INSERT",
            b"delta_bytes",
            200,
            &registry,
        );
        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0].0, "s1");
        assert_eq!(deltas[0].1.msg_type, SyncMessageType::ShapeDelta);
    }

    #[test]
    fn compactor_triggers() {
        let mut compactor = ShapeCompactor::new(3, 3600);
        assert!(!compactor.record_delta("s1", "sh1"));
        assert!(!compactor.record_delta("s1", "sh1"));
        assert!(compactor.record_delta("s1", "sh1")); // 3rd = threshold.

        assert_eq!(compactor.shapes_needing_compaction().len(), 1);
        compactor.compaction_done("s1", "sh1");
        assert!(compactor.shapes_needing_compaction().is_empty());
    }

    #[test]
    fn historical_resolution() {
        // Client within WAL.
        assert_eq!(
            resolve_historical_delta(100, 50, 10),
            HistoricalResolution::WalDelta
        );

        // Client predates WAL but within Loro history.
        assert_eq!(
            resolve_historical_delta(30, 50, 10),
            HistoricalResolution::LoroDelta
        );

        // Both truncated.
        assert_eq!(
            resolve_historical_delta(5, 50, 10),
            HistoricalResolution::FullSnapshot
        );
    }
}
