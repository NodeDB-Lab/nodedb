//! WAL change notification bus.
//!
//! Tails the WAL and broadcasts committed mutations to subscribers on
//! the Control Plane. This is the foundation for:
//!
//! - Live queries: `SELECT * FROM orders WHERE status = 'pending'` pushes
//!   changes in real-time to connected WebSocket clients.
//! - Phase 5 Shape evaluator: evaluates CRDT mutations against client shapes
//!   and pushes matching deltas.
//! - LISTEN/NOTIFY: PostgreSQL-compatible async notifications.
//!
//! The bus is a broadcast channel: each subscriber gets all mutations.
//! Subscribers can filter by collection/tenant on their end.

use std::sync::Arc;

use tracing::{debug, trace, warn};

use crate::types::{Lsn, TenantId};

/// A single mutation event broadcast by the change stream.
///
/// **Ordering guarantee**: Events are published in WAL commit order (by LSN).
/// Within a single core, events are strictly ordered. Across cores, events
/// are ordered by the time `dispatch_to_data_plane` returns (response arrival).
/// The LSN field provides a total order for consumers that need it.
#[derive(Debug, Clone)]
pub struct ChangeEvent {
    /// WAL LSN of this mutation. Provides total ordering across all events.
    pub lsn: Lsn,
    /// Tenant that performed the mutation.
    pub tenant_id: TenantId,
    /// Collection affected.
    pub collection: String,
    /// Document ID affected.
    pub document_id: String,
    /// Type of mutation.
    pub operation: ChangeOperation,
    /// Timestamp (epoch milliseconds).
    pub timestamp_ms: u64,
    /// Optional: document state after the change (for DIFF mode).
    /// Only populated when at least one DIFF subscription exists.
    pub after: Option<serde_json::Value>,
}

/// Type of mutation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeOperation {
    Insert,
    Update,
    Delete,
}

impl ChangeOperation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Insert => "INSERT",
            Self::Update => "UPDATE",
            Self::Delete => "DELETE",
        }
    }
}

/// A subscription handle. Receives change events via a bounded channel.
///
/// Automatically decrements the active subscription counter on drop,
/// ensuring the count stays accurate even if the caller forgets to
/// explicitly unsubscribe.
pub struct Subscription {
    /// Unique subscription ID.
    pub id: u64,
    /// Receives change events. Bounded buffer — drops oldest on overflow.
    pub receiver: tokio::sync::broadcast::Receiver<ChangeEvent>,
    /// Optional collection filter (None = all collections).
    pub collection_filter: Option<String>,
    /// Optional tenant filter (None = all tenants).
    pub tenant_filter: Option<TenantId>,
    /// Optional field filter: only include these fields in live query events.
    /// Empty = all fields.
    pub field_filter: Vec<String>,
    /// Shared counter for automatic cleanup on drop.
    active_counter: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.active_counter
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
}

impl Subscription {
    /// Receive the next event that matches this subscription's filters.
    ///
    /// Skips events that don't match the collection and tenant filters.
    /// This provides server-side CDC filtering — subscribers only see
    /// events for their subscribed collection/tenant.
    pub async fn recv_filtered(
        &mut self,
    ) -> Result<ChangeEvent, tokio::sync::broadcast::error::RecvError> {
        loop {
            let event = self.receiver.recv().await?;
            if self
                .collection_filter
                .as_ref()
                .is_some_and(|c| event.collection != *c)
            {
                continue;
            }
            if self
                .tenant_filter
                .as_ref()
                .is_some_and(|t| event.tenant_id != *t)
            {
                continue;
            }
            return Ok(event);
        }
    }
}

/// The change stream bus: broadcasts WAL mutations to subscribers.
///
/// Lives on the Control Plane (Send + Sync). The Data Plane writes
/// mutations to the WAL; the change stream tails the WAL and broadcasts
/// change events to all subscribers.
pub struct ChangeStream {
    /// Broadcast sender — all subscribers receive from this.
    sender: tokio::sync::broadcast::Sender<ChangeEvent>,
    /// Next subscription ID.
    next_sub_id: std::sync::atomic::AtomicU64,
    /// Active subscription count (shared with Subscription for Drop cleanup).
    active_subscriptions: std::sync::Arc<std::sync::atomic::AtomicU64>,
    /// Total events published.
    events_published: std::sync::atomic::AtomicU64,
    /// Last LSN processed from the WAL.
    last_lsn: std::sync::atomic::AtomicU64,
    /// Ring buffer of recent change events for SHOW CHANGES queries.
    /// RwLock allows concurrent reads (query_changes) with exclusive writes (publish).
    recent_changes: std::sync::RwLock<std::collections::VecDeque<ChangeEvent>>,
    /// Max recent changes to retain.
    recent_capacity: usize,
}

impl ChangeStream {
    /// Create a new change stream with the given buffer capacity.
    ///
    /// `capacity` is the number of events buffered before slow
    /// subscribers start losing events (backpressure policy: drop oldest).
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = tokio::sync::broadcast::channel(capacity);
        Self {
            sender,
            next_sub_id: std::sync::atomic::AtomicU64::new(1),
            active_subscriptions: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0)),
            events_published: std::sync::atomic::AtomicU64::new(0),
            last_lsn: std::sync::atomic::AtomicU64::new(0),
            recent_changes: std::sync::RwLock::new(std::collections::VecDeque::with_capacity(
                capacity,
            )),
            recent_capacity: capacity,
        }
    }

    /// Subscribe to the change stream.
    ///
    /// Returns a `Subscription` with a broadcast receiver. The subscriber
    /// receives all events and can filter locally by collection/tenant.
    pub fn subscribe(
        &self,
        collection_filter: Option<String>,
        tenant_filter: Option<TenantId>,
    ) -> Subscription {
        let id = self
            .next_sub_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let receiver = self.sender.subscribe();
        self.active_subscriptions
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        debug!(
            id,
            ?collection_filter,
            ?tenant_filter,
            "change stream: new subscription"
        );
        Subscription {
            id,
            receiver,
            collection_filter,
            tenant_filter,
            field_filter: Vec::new(),
            active_counter: std::sync::Arc::clone(&self.active_subscriptions),
        }
    }

    /// Publish a change event to all subscribers.
    ///
    /// Called by the WAL tail loop when a committed mutation is observed.
    /// If no subscribers are active, the event is dropped silently.
    pub fn publish(&self, event: ChangeEvent) {
        self.last_lsn
            .store(event.lsn.as_u64(), std::sync::atomic::Ordering::Relaxed);
        self.events_published
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        // Store in recent changes ring buffer.
        if let Ok(mut buf) = self.recent_changes.write() {
            if buf.len() >= self.recent_capacity {
                buf.pop_front();
            }
            buf.push_back(event.clone());
        }

        // broadcast::send returns Err if no receivers — that's fine.
        let _ = self.sender.send(event);
    }

    /// Publish a batch of events (e.g., from a Transaction WAL record).
    pub fn publish_batch(&self, events: &[ChangeEvent]) {
        for event in events {
            self.publish(event.clone());
        }
    }

    /// Number of active subscriptions.
    pub fn subscriber_count(&self) -> u64 {
        self.active_subscriptions
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Total events published since creation.
    pub fn events_published(&self) -> u64 {
        self.events_published
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Last WAL LSN processed.
    pub fn last_lsn(&self) -> u64 {
        self.last_lsn.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Query recent changes for a collection since a given timestamp.
    ///
    /// Returns change events matching the collection filter that occurred
    /// at or after `since_ms` (epoch milliseconds). Limited to the ring
    /// buffer capacity (most recent N events).
    pub fn query_changes(
        &self,
        collection: Option<&str>,
        since_ms: u64,
        limit: usize,
    ) -> Vec<ChangeEvent> {
        let buf = match self.recent_changes.read() {
            Ok(b) => b,
            Err(p) => p.into_inner(),
        };
        buf.iter()
            .filter(|e| e.timestamp_ms >= since_ms && collection.is_none_or(|c| e.collection == c))
            .take(limit)
            .cloned()
            .collect()
    }

    /// Manually decrement the active subscription counter.
    ///
    /// **Prefer dropping the `Subscription`** — its `Drop` impl handles
    /// cleanup automatically. Use this only for legacy callers that
    /// manage subscriptions outside the `Subscription` struct.
    pub fn unsubscribe(&self) {
        self.active_subscriptions
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }

    /// Deliver a remote NOTIFY broadcast to local subscribers.
    ///
    /// Called when a `NotifyBroadcast` message is received from another node.
    /// Converts the message into a `ChangeEvent` and publishes locally.
    pub fn deliver_remote_notify(
        &self,
        msg: &crate::event::cross_shard::types::NotifyBroadcastMsg,
    ) {
        let op = match msg.operation.as_str() {
            "INSERT" => ChangeOperation::Insert,
            "UPDATE" => ChangeOperation::Update,
            "DELETE" => ChangeOperation::Delete,
            _ => ChangeOperation::Insert,
        };

        let event = ChangeEvent {
            lsn: Lsn::new(msg.lsn),
            tenant_id: TenantId::new(msg.tenant_id),
            collection: msg.collection.clone(),
            document_id: msg.document_id.clone(),
            operation: op,
            timestamp_ms: msg.timestamp_ms,
            after: None,
        };

        // Publish locally — don't store in recent_changes (remote events).
        let _ = self.sender.send(event);
    }
}

/// Broadcast a `ChangeEvent` to all peer nodes in the cluster.
///
/// Fire-and-forget: spawns a Tokio task per peer. Failures are logged but
/// don't block the local publish path. Called from the Event Plane consumer
/// after processing each write event.
pub fn broadcast_notify_to_cluster(
    event: &ChangeEvent,
    node_id: u64,
    sequence: u64,
    transport: &Arc<nodedb_cluster::NexarTransport>,
    topology: &Arc<std::sync::RwLock<nodedb_cluster::ClusterTopology>>,
) {
    use crate::event::cross_shard::types::NotifyBroadcastMsg;
    use nodedb_cluster::RaftRpc;
    use nodedb_cluster::wire::{VShardEnvelope, VShardMessageType};

    let msg = NotifyBroadcastMsg {
        source_node: node_id,
        sequence,
        tenant_id: event.tenant_id.as_u32(),
        collection: event.collection.clone(),
        document_id: event.document_id.clone(),
        operation: event.operation.as_str().to_string(),
        timestamp_ms: event.timestamp_ms,
        lsn: event.lsn.as_u64(),
    };

    let payload = match rmp_serde::to_vec(&msg) {
        Ok(p) => p,
        Err(e) => {
            warn!(error = %e, "failed to serialize NotifyBroadcast");
            return;
        }
    };

    // Get all active peer node IDs (excluding self).
    let peer_ids: Vec<u64> = {
        let topo = topology.read().unwrap_or_else(|p| p.into_inner());
        topo.active_nodes()
            .iter()
            .map(|n| n.node_id)
            .filter(|&id| id != node_id)
            .collect()
    };

    if peer_ids.is_empty() {
        return;
    }

    trace!(
        peer_count = peer_ids.len(),
        collection = %event.collection,
        "broadcasting NOTIFY to cluster peers"
    );

    // Fire-and-forget: spawn one send per peer.
    let transport = Arc::clone(transport);
    for peer_id in peer_ids {
        let envelope = VShardEnvelope::new(
            VShardMessageType::NotifyBroadcast,
            node_id,
            peer_id,
            0, // No specific vShard for NOTIFY.
            payload.clone(),
        );

        let transport = Arc::clone(&transport);
        tokio::spawn(async move {
            let rpc = RaftRpc::VShardEnvelope(envelope.to_bytes());
            if let Err(e) = transport.send_rpc(peer_id, rpc).await {
                trace!(
                    peer = peer_id,
                    error = %e,
                    "NOTIFY broadcast to peer failed (best-effort)"
                );
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn publish_and_receive() {
        let stream = ChangeStream::new(64);
        let mut sub = stream.subscribe(None, None);

        let event = ChangeEvent {
            lsn: Lsn::new(1),
            tenant_id: TenantId::new(1),
            collection: "users".into(),
            document_id: "u1".into(),
            operation: ChangeOperation::Insert,
            timestamp_ms: 1000,
            after: None,
        };
        stream.publish(event);

        let received = sub.receiver.recv().await.unwrap();
        assert_eq!(received.document_id, "u1");
        assert_eq!(received.operation, ChangeOperation::Insert);
    }

    #[tokio::test]
    async fn multiple_subscribers() {
        let stream = ChangeStream::new(64);
        let mut sub1 = stream.subscribe(None, None);
        let mut sub2 = stream.subscribe(Some("orders".into()), None);

        let event = ChangeEvent {
            lsn: Lsn::new(2),
            tenant_id: TenantId::new(1),
            collection: "orders".into(),
            document_id: "o1".into(),
            operation: ChangeOperation::Update,
            timestamp_ms: 2000,
            after: None,
        };
        stream.publish(event);

        // Both subscribers receive the event.
        let r1 = sub1.receiver.recv().await.unwrap();
        let r2 = sub2.receiver.recv().await.unwrap();
        assert_eq!(r1.document_id, "o1");
        assert_eq!(r2.document_id, "o1");
    }

    #[test]
    fn metrics_tracking() {
        let stream = ChangeStream::new(64);
        let _sub = stream.subscribe(None, None);
        assert_eq!(stream.subscriber_count(), 1);

        for i in 0..10 {
            stream.publish(ChangeEvent {
                lsn: Lsn::new(i),
                tenant_id: TenantId::new(1),
                collection: "test".into(),
                document_id: format!("d{i}"),
                operation: ChangeOperation::Insert,
                timestamp_ms: 0,
                after: None,
            });
        }
        assert_eq!(stream.events_published(), 10);
        assert_eq!(stream.last_lsn(), 9);

        // Drop the subscription — Drop impl decrements the counter.
        drop(_sub);
        assert_eq!(stream.subscriber_count(), 0);
    }
}
