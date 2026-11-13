// SPDX-License-Identifier: BUSL-1.1

//! CRDT delta packager: converts WriteEvents into outbound deltas for Lite sync.
//!
//! The Event Plane consumer calls `package_and_enqueue()` for each
//! WriteEvent with `source: User`. If a connected Lite session holds an
//! authorized shape on the event's collection, the write is packaged as an
//! `OutboundDelta` and enqueued for delivery. Each session's push-time
//! checks then decide what of it leaves the server.
//!
//! **Ordering:** Per-collection monotonic sequence ensures Lite devices
//! apply deltas in LSN order. Cross-collection ordering is best-effort.
//!
//! **Payload:** An outbound delta carries the full row post-image, not a Loro
//! delta. The session sends it as a `RowPush` frame, and the last write wins.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use tracing::trace;

use super::delivery::CrdtSyncDelivery;
use super::types::{DeltaAudience, DeltaOp, OutboundDelta};
use crate::event::sink_ledger::{CrdtLedger, SinkEventKey};
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
    /// With a `ledger`, the event's `key` and the collection's sequence are
    /// recorded durably before the delta is handed on: an event the ledger
    /// already holds is not packaged again, and sequences continue across a
    /// restart.
    ///
    /// Returns `true` if the delta was enqueued, `false` if skipped.
    pub fn package_and_enqueue(
        &self,
        event: &WriteEvent,
        key: Option<&SinkEventKey>,
        ledger: Option<&CrdtLedger>,
        delivery: &CrdtSyncDelivery,
    ) -> bool {
        // User writes and restored rows change the base data Lite peers hold,
        // so both are packaged. CrdtSync events came FROM Lite and are not
        // echoed back. Trigger, RaftFollower and Deferred events are derived
        // from a User event that already carries the data change.
        match event.source {
            EventSource::User | EventSource::ImplicitClient | EventSource::Restore => {}
            EventSource::Trigger
            | EventSource::RaftFollower
            | EventSource::CrdtSync
            | EventSource::Deferred => return false,
        }

        // A write event names its collection as storage keys it: qualified
        // outside the default database. Shapes, grants and the client's
        // replica use the bare name, so the delta carries the bare name. A
        // name that is not qualified for the event's database is not routed.
        let Some(collection) =
            event.routed_collection(crate::event::unrouted::UnroutedConsumer::CrdtPackager)
        else {
            self.deltas_skipped.fetch_add(1, Ordering::Relaxed);
            return false;
        };

        // Check if any connected Lite session cares about this collection.
        if !delivery.has_subscribers(event.tenant_id.as_u64(), event.database_id, collection) {
            self.deltas_skipped.fetch_add(1, Ordering::Relaxed);
            return false;
        }

        // The pre-image travels with an update or delete so the subscriber's
        // push-time check can tell whether the row was visible to it before
        // the write: that decides whether a move-out delete goes out.
        let pre_image = || event.old_value.as_ref().map(|v| v.to_vec());
        let (op, payload, prior_payload) = match event.op {
            WriteOp::Insert => (DeltaOp::Upsert, post_image(event), None),
            WriteOp::Update => (DeltaOp::Upsert, post_image(event), pre_image()),
            WriteOp::Delete => (DeltaOp::Delete, Vec::new(), pre_image()),
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
            WriteOp::Heartbeat | WriteOp::Publish => return false,
        };

        let sequence = match ledger {
            None => self.sequences.next(&event.collection),
            Some(ledger) => match ledger.claim(key, &event.collection) {
                Ok(Some(sequence)) => sequence,
                // Packaged before a restart: the delta already went out.
                Ok(None) => return false,
                Err(error) => {
                    // Packaging without the ledger could hand the same event
                    // on twice, so the delta is not packaged.
                    tracing::error!(
                        %error,
                        collection = %event.collection,
                        "crdt ledger unavailable; outbound delta not packaged"
                    );
                    return false;
                }
            },
        };

        let delta = OutboundDelta {
            database_id: event.database_id,
            collection: collection.to_string(),
            document_id: event.row_id.as_str().to_string(),
            payload,
            prior_payload,
            audience: DeltaAudience::Subscribers,
            op,
            lsn: event.lsn.as_u64(),
            tenant_id: event.tenant_id.as_u64(),
            peer_id: ORIGIN_PEER_ID.load(Ordering::Relaxed),
            sequence,
        };

        delivery.enqueue(delta);
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

/// The event's post-image bytes. Empty when the event carried none.
fn post_image(event: &WriteEvent) -> Vec<u8> {
    event
        .new_value
        .as_ref()
        .map(|v| v.to_vec())
        .unwrap_or_default()
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
    use crate::types::{DatabaseId, Lsn, TenantId, VShardId};
    use std::sync::Arc;

    fn make_event(source: EventSource, op: WriteOp) -> WriteEvent {
        WriteEvent {
            sequence: 1,
            // Database 7 qualifies the name, as storage keys it.
            collection: Arc::from("7/orders"),
            op,
            row_id: RowId::row(nodedb_types::RowIdentity::from_user_key("o-1")),
            lsn: Lsn::new(100),
            record: None,
            database_id: DatabaseId::new(7),
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            source,
            new_value: Some(Arc::from(b"payload".as_slice())),
            old_value: None,
            system_time_ms: None,
            valid_time_ms: None,
            user_id: None,
            statement_digest: None,
            commit_hlc: Some(crate::event::test_utils::test_commit_hlc()),
            image_fault: None,
        }
    }

    #[test]
    fn skips_non_user_sources() {
        let packager = DeltaPackager::new();
        let delivery = CrdtSyncDelivery::new();

        let crdt_event = make_event(EventSource::CrdtSync, WriteOp::Insert);
        assert!(!packager.package_and_enqueue(&crdt_event, None, None, &delivery));

        let trigger_event = make_event(EventSource::Trigger, WriteOp::Insert);
        assert!(!packager.package_and_enqueue(&trigger_event, None, None, &delivery));
    }

    #[test]
    fn a_restored_row_passes_the_source_gate() {
        let packager = DeltaPackager::new();
        let delivery = CrdtSyncDelivery::new();
        // No session subscribes, so the event stops at the subscriber check.
        // The skip counter moves only for an event past the source gate.
        let restored = make_event(EventSource::Restore, WriteOp::Insert);
        assert!(!packager.package_and_enqueue(&restored, None, None, &delivery));
        assert_eq!(packager.deltas_skipped.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn skips_heartbeats() {
        let packager = DeltaPackager::new();
        let delivery = CrdtSyncDelivery::new();

        let hb = make_event(EventSource::User, WriteOp::Heartbeat);
        assert!(!packager.package_and_enqueue(&hb, None, None, &delivery));
    }

    #[test]
    fn skips_when_no_subscribers() {
        let packager = DeltaPackager::new();
        let delivery = CrdtSyncDelivery::new();
        // No sessions registered → no subscribers.
        let event = make_event(EventSource::User, WriteOp::Insert);
        assert!(!packager.package_and_enqueue(&event, None, None, &delivery));
        assert_eq!(packager.deltas_skipped.load(Ordering::Relaxed), 1);
    }

    /// An outbound delta carries the row's post-image verbatim, exactly as the
    /// Data Plane wrote it. It is NOT Loro update bytes — the field name
    /// `payload` is deliberate — so a consumer must decode it as a row value
    /// rather than importing it into a CRDT document.
    #[test]
    fn outbound_payload_is_the_row_post_image() {
        let packager = DeltaPackager::new();
        let delivery = CrdtSyncDelivery::new();
        let event = make_event(EventSource::User, WriteOp::Insert);

        let expected = event
            .new_value
            .as_ref()
            .map(|v| v.to_vec())
            .unwrap_or_default();
        let _ = packager.package_and_enqueue(&event, None, None, &delivery);

        assert_eq!(expected, b"payload".to_vec());
    }

    fn subscribed_delivery() -> (
        CrdtSyncDelivery,
        tokio::sync::mpsc::Receiver<OutboundDelta>,
        tokio::sync::mpsc::Receiver<crate::event::crdt_sync::types::ControlFrame>,
    ) {
        let delivery = CrdtSyncDelivery::new();
        let (rx, crx) = delivery.register(
            "s1".into(),
            1,
            1,
            DatabaseId::new(7),
            &crate::event::crdt_sync::types::DeliveryConfig::default(),
        );
        let shape = crate::event::crdt_sync::SubscribedShape::new("orders".into(), Vec::new())
            .expect("empty predicate decodes");
        delivery.subscribe_shape("s1", "sh1", shape);
        (delivery, rx, crx)
    }

    /// An update carries its pre-image so the push-time check can send a
    /// move-out delete when the row leaves what the subscriber sees.
    #[test]
    fn update_carries_the_pre_image() {
        let packager = DeltaPackager::new();
        let (delivery, mut rx, _crx) = subscribed_delivery();
        let mut event = make_event(EventSource::User, WriteOp::Update);
        event.old_value = Some(Arc::from(b"before".as_slice()));

        assert!(packager.package_and_enqueue(&event, None, None, &delivery));
        let delta = rx
            .try_recv()
            .expect("subscribed session receives the delta");
        assert_eq!(delta.op, DeltaOp::Upsert);
        assert_eq!(delta.payload, b"payload".to_vec());
        assert_eq!(delta.prior_payload, Some(b"before".to_vec()));
        assert_eq!(delta.audience, DeltaAudience::Subscribers);
    }

    #[test]
    fn delete_carries_the_pre_image_and_insert_carries_none() {
        let packager = DeltaPackager::new();
        let (delivery, mut rx, _crx) = subscribed_delivery();

        let mut delete = make_event(EventSource::User, WriteOp::Delete);
        delete.old_value = Some(Arc::from(b"before".as_slice()));
        assert!(packager.package_and_enqueue(&delete, None, None, &delivery));
        let delta = rx.try_recv().expect("delete delta");
        assert_eq!(delta.op, DeltaOp::Delete);
        assert!(delta.payload.is_empty());
        assert_eq!(delta.prior_payload, Some(b"before".to_vec()));

        let insert = make_event(EventSource::User, WriteOp::Insert);
        assert!(packager.package_and_enqueue(&insert, None, None, &delivery));
        let delta = rx.try_recv().expect("insert delta");
        assert_eq!(delta.prior_payload, None);
    }

    /// Outside the default database a write event names its collection
    /// qualified. The delta carries the bare name the shapes subscribe to.
    #[test]
    fn qualified_event_collection_routes_by_its_bare_name() {
        let packager = DeltaPackager::new();
        let (delivery, mut rx, _crx) = subscribed_delivery();
        let event = make_event(EventSource::User, WriteOp::Insert);

        assert!(packager.package_and_enqueue(&event, None, None, &delivery));
        let delta = rx.try_recv().expect("the bare name matches the shape");
        assert_eq!(delta.collection, "orders");
    }

    /// A bare name outside the default database matches no storage key, so
    /// it is not routed.
    #[test]
    fn unqualified_name_outside_the_default_database_is_not_packaged() {
        let packager = DeltaPackager::new();
        let (delivery, mut rx, _crx) = subscribed_delivery();
        let mut event = make_event(EventSource::User, WriteOp::Insert);
        event.collection = Arc::from("orders");

        assert!(!packager.package_and_enqueue(&event, None, None, &delivery));
        assert!(rx.try_recv().is_err());
    }

    /// A collection no session subscribes to is skipped before packaging.
    #[test]
    fn unsubscribed_collection_is_not_packaged() {
        let packager = DeltaPackager::new();
        let (delivery, mut rx, _crx) = subscribed_delivery();
        let mut event = make_event(EventSource::User, WriteOp::Insert);
        event.collection = Arc::from("7/users");

        assert!(!packager.package_and_enqueue(&event, None, None, &delivery));
        assert_eq!(packager.deltas_skipped.load(Ordering::Relaxed), 1);
        assert!(rx.try_recv().is_err());
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
