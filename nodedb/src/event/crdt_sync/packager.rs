//! CRDT delta packager: converts WriteEvents into outbound deltas for Lite sync.
//!
//! The Event Plane consumer calls `package_for_lite()` for each WriteEvent
//! with `source: User`. If connected Lite devices have subscribed to the
//! event's collection, the write is packaged as an `OutboundDelta` and
//! enqueued for delivery.
//!
//! **Ordering:** Per-collection monotonic sequence ensures Lite devices
//! apply deltas in LSN order. Cross-collection ordering is best-effort.
//!
//! **Conflict resolution:** Outbound deltas carry the full row state
//! (last-writer-wins). The Lite device applies them via `CrdtState.import()`,
//! which merges using Loro's built-in conflict resolution (operation-based CRDT).
//! Constraint violations on the Lite side are surfaced via `CompensationHint`.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use tracing::trace;

use super::delivery::CrdtSyncDelivery;
use super::types::{DeltaOp, OutboundDelta};
use crate::event::types::{EventSource, WriteEvent, WriteOp};

/// Per-collection sequence counter for ordering enforcement.
struct SequenceTracker {
    /// collection_name → next sequence number.
    sequences: Mutex<HashMap<String, u64>>,
}

impl SequenceTracker {
    fn new() -> Self {
        Self {
            sequences: Mutex::new(HashMap::new()),
        }
    }

    fn next(&self, collection: &str) -> u64 {
        let mut seqs = self.sequences.lock().unwrap_or_else(|p| p.into_inner());
        let seq = seqs.entry(collection.to_string()).or_insert(0);
        *seq += 1;
        *seq
    }
}

/// Origin node's peer ID for CRDT attribution.
/// Set once during startup from the cluster node_id (or 0 for single-node).
static ORIGIN_PEER_ID: AtomicU64 = AtomicU64::new(0);

/// Set the origin peer ID (called once during EventPlane startup).
pub fn set_origin_peer_id(peer_id: u64) {
    ORIGIN_PEER_ID.store(peer_id, Ordering::Relaxed);
}

/// Shared packager state (owned by the Event Plane, called from consumer loop).
pub struct DeltaPackager {
    sequences: SequenceTracker,
    /// Total deltas packaged (monotonic counter).
    pub deltas_packaged: AtomicU64,
    /// Total deltas skipped (no subscribers).
    pub deltas_skipped: AtomicU64,
}

impl DeltaPackager {
    pub fn new() -> Self {
        Self {
            sequences: SequenceTracker::new(),
            deltas_packaged: AtomicU64::new(0),
            deltas_skipped: AtomicU64::new(0),
        }
    }

    /// Package a WriteEvent as an outbound delta and enqueue for delivery.
    ///
    /// Only packages events from `EventSource::User` (trigger side-effects
    /// are already captured by the triggering event's delta). Events from
    /// `CrdtSync` are skipped (prevent echo: Lite → Origin → Lite loop).
    ///
    /// Returns `true` if the delta was enqueued, `false` if skipped.
    pub fn package_and_enqueue(&self, event: &WriteEvent, delivery: &CrdtSyncDelivery) -> bool {
        // Only package User-originated writes.
        // CrdtSync events are inbound FROM Lite — don't echo back.
        // Trigger/RaftFollower events are derivative — the original User
        // event already covers the data change.
        if event.source != EventSource::User {
            return false;
        }

        // Check if any connected Lite session cares about this collection.
        if !delivery.has_subscribers(event.tenant_id.as_u32(), &event.collection) {
            self.deltas_skipped.fetch_add(1, Ordering::Relaxed);
            return false;
        }

        let (op, payload) = match event.op {
            WriteOp::Insert | WriteOp::Update => {
                let payload = event
                    .new_value
                    .as_ref()
                    .map(|v| v.to_vec())
                    .unwrap_or_default();
                (DeltaOp::Upsert, payload)
            }
            WriteOp::Delete => (DeltaOp::Delete, Vec::new()),
            // Bulk operations: each row in the batch was already emitted
            // as individual per-row events by the ring buffer path. The
            // bulk event carries aggregate metadata, not per-row payloads.
            WriteOp::BulkInsert { count } | WriteOp::BulkDelete { count } => {
                trace!(
                    collection = %event.collection,
                    count,
                    "skipping bulk event for CRDT packaging (per-row events handle delivery)"
                );
                return false;
            }
            WriteOp::Heartbeat => return false,
        };

        let sequence = self.sequences.next(&event.collection);

        let delta = OutboundDelta {
            collection: event.collection.to_string(),
            document_id: event.row_id.as_str().to_string(),
            payload,
            op,
            lsn: event.lsn.as_u64(),
            tenant_id: event.tenant_id.as_u32(),
            peer_id: ORIGIN_PEER_ID.load(Ordering::Relaxed),
            sequence,
        };

        delivery.enqueue(event.tenant_id.as_u32(), delta);
        self.deltas_packaged.fetch_add(1, Ordering::Relaxed);

        trace!(
            collection = %event.collection,
            doc_id = %event.row_id,
            seq = sequence,
            "packaged outbound CRDT delta for Lite delivery"
        );

        true
    }
}

impl Default for DeltaPackager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::types::RowId;
    use crate::types::{Lsn, TenantId, VShardId};
    use std::sync::Arc;

    fn make_event(source: EventSource, op: WriteOp) -> WriteEvent {
        WriteEvent {
            sequence: 1,
            collection: Arc::from("orders"),
            op,
            row_id: RowId::new("o-1"),
            lsn: Lsn::new(100),
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            source,
            new_value: Some(Arc::from(b"payload".as_slice())),
            old_value: None,
            system_time_ms: None,
            valid_time_ms: None,
        }
    }

    #[test]
    fn skips_non_user_sources() {
        let packager = DeltaPackager::new();
        let delivery = CrdtSyncDelivery::new();

        let crdt_event = make_event(EventSource::CrdtSync, WriteOp::Insert);
        assert!(!packager.package_and_enqueue(&crdt_event, &delivery));

        let trigger_event = make_event(EventSource::Trigger, WriteOp::Insert);
        assert!(!packager.package_and_enqueue(&trigger_event, &delivery));
    }

    #[test]
    fn skips_heartbeats() {
        let packager = DeltaPackager::new();
        let delivery = CrdtSyncDelivery::new();

        let hb = make_event(EventSource::User, WriteOp::Heartbeat);
        assert!(!packager.package_and_enqueue(&hb, &delivery));
    }

    #[test]
    fn skips_when_no_subscribers() {
        let packager = DeltaPackager::new();
        let delivery = CrdtSyncDelivery::new();
        // No sessions registered → no subscribers.
        let event = make_event(EventSource::User, WriteOp::Insert);
        assert!(!packager.package_and_enqueue(&event, &delivery));
        assert_eq!(packager.deltas_skipped.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn sequence_monotonic_per_collection() {
        let tracker = SequenceTracker::new();
        assert_eq!(tracker.next("orders"), 1);
        assert_eq!(tracker.next("orders"), 2);
        assert_eq!(tracker.next("users"), 1);
        assert_eq!(tracker.next("orders"), 3);
    }
}
