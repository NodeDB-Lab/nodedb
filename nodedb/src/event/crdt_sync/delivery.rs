//! CRDT sync delivery: session registry + per-session push queues.
//!
//! Manages connected Lite sessions and delivers outbound deltas to them.
//! Each session has a bounded mpsc channel. The background delivery task
//! monitors channel health and removes disconnected sessions.
//!
//! **Backpressure:** When a session's channel is full (Lite is slow/offline),
//! the oldest deltas are dropped with a warning. The Lite device recovers
//! via shape-based resync on reconnect (full snapshot + catch-up).

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tracing::{debug, info, trace, warn};

use super::types::{DeliveryConfig, LiteSessionHandle, OutboundDelta};

/// Registry of connected Lite sessions for delta delivery.
///
/// Thread-safe: `register()` / `unregister()` called from sync listener tasks,
/// `enqueue()` called from Event Plane consumer tasks.
pub struct CrdtSyncDelivery {
    /// session_id → session handle.
    sessions: RwLock<HashMap<String, LiteSessionHandle>>,
    /// Total deltas delivered (monotonic counter).
    pub deltas_delivered: std::sync::atomic::AtomicU64,
    /// Total deltas dropped due to backpressure.
    pub deltas_dropped: std::sync::atomic::AtomicU64,
    /// Total sessions registered since startup.
    pub sessions_registered: std::sync::atomic::AtomicU64,
}

impl CrdtSyncDelivery {
    pub fn new() -> Self {
        Self {
            sessions: RwLock::new(HashMap::new()),
            deltas_delivered: std::sync::atomic::AtomicU64::new(0),
            deltas_dropped: std::sync::atomic::AtomicU64::new(0),
            sessions_registered: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Register a connected Lite session for delta delivery.
    ///
    /// Returns an `mpsc::Receiver<OutboundDelta>` that the session writer
    /// task should drain and send to the Lite device via WebSocket.
    ///
    /// **Architecture note:** This mpsc channel bridges Event Plane (producer)
    /// to Control Plane (consumer/session writer). This is NOT the same as the
    /// SPSC bridge or Event Bus — those cross the `Send`/`!Send` boundary into
    /// the Data Plane. Both Event Plane and Control Plane are `Send + Sync`
    /// Tokio code on the same runtime, so a standard mpsc channel is correct.
    /// The SPSC/Event Bus rules apply to Data Plane boundaries only.
    pub fn register(
        &self,
        session_id: String,
        peer_id: u64,
        tenant_id: u32,
        subscribed_collections: Vec<String>,
        config: &DeliveryConfig,
    ) -> (
        mpsc::Receiver<OutboundDelta>,
        mpsc::Receiver<nodedb_types::sync::wire::SyncFrame>,
    ) {
        let (tx, rx) = mpsc::channel(config.max_queue_per_session);
        // Control-frame channel: small bounded queue. Purge
        // notifications are rare + fire-and-forget, so 32 slots is
        // plenty. If the listener can't drain fast enough the session
        // is already wedged for other reasons.
        let (control_tx, control_rx) = mpsc::channel(32);

        let handle = LiteSessionHandle {
            session_id: session_id.clone(),
            peer_id,
            tenant_id,
            subscribed_collections,
            sender: tx,
            control_sender: control_tx,
        };

        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        sessions.insert(session_id.clone(), handle);
        self.sessions_registered
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        info!(
            session = %session_id,
            peer_id,
            tenant_id,
            "registered Lite session for CRDT sync delivery"
        );

        (rx, control_rx)
    }

    /// Broadcast a `CollectionPurged` control frame to every session
    /// currently subscribed to `(tenant_id, collection)`. Runs when
    /// `purge_async` finishes the per-node reclaim, so every online
    /// Lite client learns about the hard-delete within one network
    /// round-trip and can drop local state. Offline clients pick it
    /// up on reconnect via the `last_seen_lsn` replay path.
    pub fn broadcast_collection_purged(&self, tenant_id: u32, collection: &str, purge_lsn: u64) {
        let msg = nodedb_types::sync::wire::CollectionPurgedMsg {
            tenant_id,
            name: collection.to_string(),
            purge_lsn,
        };
        let Some(frame) = nodedb_types::sync::wire::SyncFrame::new_msgpack(
            nodedb_types::sync::wire::SyncMessageType::CollectionPurged,
            &msg,
        ) else {
            warn!(
                tenant_id,
                collection, purge_lsn, "failed to encode CollectionPurgedMsg — skipping broadcast"
            );
            return;
        };

        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        let mut sent = 0usize;
        let mut dropped = 0usize;
        for session in sessions.values() {
            if session.tenant_id != tenant_id {
                continue;
            }
            // Collection filter: if the session declared a subscription
            // list, the collection must be in it. Empty list = "all
            // collections for this tenant" = always notify.
            if !session.subscribed_collections.is_empty()
                && !session
                    .subscribed_collections
                    .iter()
                    .any(|c| c == collection)
            {
                continue;
            }
            match session.control_sender.try_send(frame.clone()) {
                Ok(()) => sent += 1,
                Err(_) => dropped += 1,
            }
        }
        info!(
            tenant_id,
            collection, purge_lsn, sent, dropped, "broadcast CollectionPurged to sync sessions"
        );
    }

    /// Unregister a disconnected Lite session.
    ///
    /// The mpsc sender is dropped, which signals the receiver.
    pub fn unregister(&self, session_id: &str) {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        if sessions.remove(session_id).is_some() {
            debug!(session = %session_id, "unregistered Lite session from CRDT sync delivery");
        }
    }

    /// Check if any connected Lite session subscribes to this collection.
    pub fn has_subscribers(&self, tenant_id: u32, collection: &str) -> bool {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        sessions.values().any(|s| {
            s.tenant_id == tenant_id
                && (s.subscribed_collections.is_empty()
                    || s.subscribed_collections.iter().any(|c| c == collection))
        })
    }

    /// Enqueue an outbound delta for delivery to all matching sessions.
    ///
    /// Sends to each session whose tenant_id matches and that subscribes
    /// to the delta's collection. Uses `try_send` (non-blocking) — if the
    /// channel is full, the delta is dropped with a warning (backpressure).
    pub fn enqueue(&self, tenant_id: u32, delta: OutboundDelta) {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());

        for session in sessions.values() {
            if session.tenant_id != tenant_id {
                continue;
            }

            // Check collection subscription filter.
            if !session.subscribed_collections.is_empty()
                && !session
                    .subscribed_collections
                    .iter()
                    .any(|c| c == &delta.collection)
            {
                continue;
            }

            match session.sender.try_send(delta.clone()) {
                Ok(()) => {
                    self.deltas_delivered
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    self.deltas_dropped
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    warn!(
                        session = %session.session_id,
                        collection = %delta.collection,
                        "CRDT sync channel full — dropping delta (backpressure)"
                    );
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    // Session disconnected — will be cleaned up by prune task.
                    trace!(
                        session = %session.session_id,
                        "CRDT sync channel closed (session disconnected)"
                    );
                }
            }
        }
    }

    /// Number of currently registered sessions.
    pub fn session_count(&self) -> usize {
        let sessions = self.sessions.read().unwrap_or_else(|p| p.into_inner());
        sessions.len()
    }

    /// Remove sessions with closed channels (disconnected Lite devices).
    fn prune_closed(&self) {
        let mut sessions = self.sessions.write().unwrap_or_else(|p| p.into_inner());
        let before = sessions.len();
        sessions.retain(|_, handle| !handle.sender.is_closed());
        let pruned = before - sessions.len();
        if pruned > 0 {
            debug!(pruned, "pruned disconnected Lite sessions");
        }
    }
}

impl Default for CrdtSyncDelivery {
    fn default() -> Self {
        Self::new()
    }
}

/// Spawn the background delivery maintenance task.
///
/// Periodically prunes disconnected sessions. The actual delivery happens
/// via `enqueue()` which uses `try_send` — no separate drain loop needed
/// since mpsc channels deliver immediately.
pub fn spawn_delivery_task(
    delivery: Arc<CrdtSyncDelivery>,
    mut shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        debug!("CRDT sync delivery task started");
        let prune_interval = Duration::from_secs(10);

        loop {
            tokio::select! {
                _ = tokio::time::sleep(prune_interval) => {
                    delivery.prune_closed();
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        debug!("CRDT sync delivery task shutting down");
                        return;
                    }
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::crdt_sync::types::DeltaOp;

    fn make_delta(collection: &str, lsn: u64) -> OutboundDelta {
        OutboundDelta {
            collection: collection.into(),
            document_id: "doc-1".into(),
            payload: vec![1, 2, 3],
            op: DeltaOp::Upsert,
            lsn,
            tenant_id: 1,
            peer_id: 42,
            sequence: lsn,
        }
    }

    #[test]
    fn register_and_unregister() {
        let delivery = CrdtSyncDelivery::new();
        let config = DeliveryConfig::default();

        let (_rx, _crx) = delivery.register("s1".into(), 42, 1, vec!["orders".into()], &config);
        assert_eq!(delivery.session_count(), 1);
        assert!(delivery.has_subscribers(1, "orders"));
        assert!(!delivery.has_subscribers(1, "users"));
        assert!(!delivery.has_subscribers(2, "orders"));

        delivery.unregister("s1");
        assert_eq!(delivery.session_count(), 0);
    }

    #[test]
    fn empty_subscription_matches_all() {
        let delivery = CrdtSyncDelivery::new();
        let config = DeliveryConfig::default();

        // Empty subscribed_collections = all collections.
        let (_rx, _crx) = delivery.register("s1".into(), 42, 1, vec![], &config);
        assert!(delivery.has_subscribers(1, "orders"));
        assert!(delivery.has_subscribers(1, "users"));
        assert!(delivery.has_subscribers(1, "anything"));
    }

    #[tokio::test]
    async fn enqueue_delivers_to_matching_session() {
        let delivery = CrdtSyncDelivery::new();
        let config = DeliveryConfig::default();

        let (mut rx, _crx) = delivery.register("s1".into(), 42, 1, vec!["orders".into()], &config);

        delivery.enqueue(1, make_delta("orders", 100));
        delivery.enqueue(1, make_delta("users", 200)); // Should NOT deliver.

        let delta = rx.try_recv().unwrap();
        assert_eq!(delta.collection, "orders");
        assert_eq!(delta.lsn, 100);

        // No more — "users" was filtered out.
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn backpressure_drops_oldest() {
        let delivery = CrdtSyncDelivery::new();
        let config = DeliveryConfig {
            max_queue_per_session: 2,
            ..Default::default()
        };

        let (_rx, _crx) = delivery.register("s1".into(), 42, 1, vec![], &config);

        // Fill the channel (capacity 2).
        delivery.enqueue(1, make_delta("a", 1));
        delivery.enqueue(1, make_delta("b", 2));
        // Third should be dropped (backpressure).
        delivery.enqueue(1, make_delta("c", 3));

        assert_eq!(
            delivery
                .deltas_dropped
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
    }

    #[test]
    fn prune_removes_closed_sessions() {
        let delivery = CrdtSyncDelivery::new();
        let config = DeliveryConfig::default();

        let (rx, _crx) = delivery.register("s1".into(), 42, 1, vec![], &config);
        assert_eq!(delivery.session_count(), 1);

        // Drop receiver → channel closed.
        drop(rx);
        delivery.prune_closed();
        assert_eq!(delivery.session_count(), 0);
    }

    #[test]
    fn multiple_sessions_receive() {
        let delivery = CrdtSyncDelivery::new();
        let config = DeliveryConfig::default();

        let (mut rx1, _c1) = delivery.register("s1".into(), 1, 1, vec![], &config);
        let (mut rx2, _c2) = delivery.register("s2".into(), 2, 1, vec![], &config);

        delivery.enqueue(1, make_delta("orders", 100));

        assert!(rx1.try_recv().is_ok());
        assert!(rx2.try_recv().is_ok());
    }
}
