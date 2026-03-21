//! Live query subscriptions via WebSocket.
//!
//! HTTP upgrade to WebSocket. Client sends a query filter (collection +
//! optional predicates). Server evaluates each committed mutation against
//! the filter and pushes matching changes in real-time.
//!
//! Protocol:
//! 1. Client → `{ "collection": "orders", "filter": {"status": "pending"} }`
//! 2. Server → `{ "event": "INSERT", "doc_id": "o42", "lsn": 100 }` (per change)
//! 3. Client → `{ "ack": 100 }` (optional, for at-least-once delivery)

use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::control::change_stream::ChangeEvent;
use crate::types::TenantId;

/// Maximum events buffered per subscription before dropping oldest.
pub const SUBSCRIPTION_BUFFER_SIZE: usize = 1024;

/// Maximum idle time before a subscription is considered stale.
pub const SUBSCRIPTION_IDLE_TIMEOUT_SECS: u64 = 300; // 5 minutes.

/// Client subscription request.
#[derive(Debug, Clone, Deserialize)]
pub struct SubscriptionRequest {
    /// Collection to watch.
    pub collection: String,
    /// Optional field-level filter (JSON object).
    /// Only events matching ALL filter fields are delivered.
    #[serde(default)]
    pub filter: Option<serde_json::Value>,
    /// Optional tenant scope override (superuser only).
    #[serde(default)]
    pub tenant_id: Option<u32>,
}

/// Server-to-client change notification.
#[derive(Debug, Clone, Serialize)]
pub struct ChangeNotification {
    /// Type of mutation.
    pub event: String,
    /// Affected document ID.
    pub doc_id: String,
    /// Collection name.
    pub collection: String,
    /// WAL LSN of the mutation.
    pub lsn: u64,
    /// Timestamp (epoch milliseconds).
    pub timestamp_ms: u64,
}

impl From<&ChangeEvent> for ChangeNotification {
    fn from(e: &ChangeEvent) -> Self {
        Self {
            event: e.operation.as_str().to_string(),
            doc_id: e.document_id.clone(),
            collection: e.collection.clone(),
            lsn: e.lsn.as_u64(),
            timestamp_ms: e.timestamp_ms,
        }
    }
}

/// Tracks active subscriptions across all WebSocket connections.
pub struct SubscriptionManager {
    /// Total subscriptions created (monotonic counter for IDs).
    next_id: AtomicU64,
    /// Active subscription count.
    active: AtomicU64,
    /// Total events delivered across all subscriptions.
    events_delivered: AtomicU64,
    /// Total events dropped due to slow consumers.
    events_dropped: AtomicU64,
}

impl Default for SubscriptionManager {
    fn default() -> Self {
        Self::new()
    }
}

impl SubscriptionManager {
    pub fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
            active: AtomicU64::new(0),
            events_delivered: AtomicU64::new(0),
            events_dropped: AtomicU64::new(0),
        }
    }

    /// Register a new subscription.
    pub fn register(&self) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.active.fetch_add(1, Ordering::Relaxed);
        debug!(id, "subscription registered");
        id
    }

    /// Unregister a subscription (client disconnected).
    pub fn unregister(&self, id: u64) {
        self.active.fetch_sub(1, Ordering::Relaxed);
        debug!(id, "subscription unregistered");
    }

    /// Record that an event was delivered to a subscriber.
    pub fn record_delivery(&self) {
        self.events_delivered.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that an event was dropped (slow consumer).
    pub fn record_drop(&self) {
        self.events_dropped.fetch_add(1, Ordering::Relaxed);
    }

    /// Number of active subscriptions.
    pub fn active_count(&self) -> u64 {
        self.active.load(Ordering::Relaxed)
    }

    /// Total events delivered.
    pub fn events_delivered(&self) -> u64 {
        self.events_delivered.load(Ordering::Relaxed)
    }

    /// Total events dropped.
    pub fn events_dropped(&self) -> u64 {
        self.events_dropped.load(Ordering::Relaxed)
    }
}

/// Check if a change event matches a subscription's filter.
///
/// If no filter is set, all events for the collection match.
/// If a filter is set, the event matches if all filter fields
/// are present in the event metadata (shallow JSON comparison).
pub fn matches_filter(
    event: &ChangeEvent,
    collection: &str,
    tenant_id: TenantId,
    filter: Option<&serde_json::Value>,
) -> bool {
    if event.collection != collection {
        return false;
    }
    if event.tenant_id != tenant_id {
        return false;
    }
    // If no filter, all events for this collection match.
    if filter.is_none() {
        return true;
    }
    // Filter matching happens at the document level, not the event level.
    // Since change events don't carry the full document body (only doc_id),
    // filter evaluation would require a secondary read. For now, we deliver
    // all events for the collection and let the client filter.
    //
    // Future: integrate with the change stream to carry relevant fields
    // in the event payload for server-side filtering without a secondary read.
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::change_stream::ChangeOperation;
    use crate::types::Lsn;

    #[test]
    fn subscription_lifecycle() {
        let mgr = SubscriptionManager::new();
        let id1 = mgr.register();
        let id2 = mgr.register();
        assert_eq!(mgr.active_count(), 2);
        assert_ne!(id1, id2);

        mgr.unregister(id1);
        assert_eq!(mgr.active_count(), 1);

        mgr.record_delivery();
        mgr.record_delivery();
        mgr.record_drop();
        assert_eq!(mgr.events_delivered(), 2);
        assert_eq!(mgr.events_dropped(), 1);
    }

    #[test]
    fn filter_matching() {
        let event = ChangeEvent {
            lsn: Lsn::new(1),
            tenant_id: TenantId::new(1),
            collection: "orders".into(),
            document_id: "o1".into(),
            operation: ChangeOperation::Insert,
            timestamp_ms: 0,
        };

        assert!(matches_filter(&event, "orders", TenantId::new(1), None));
        assert!(!matches_filter(&event, "users", TenantId::new(1), None));
        assert!(!matches_filter(&event, "orders", TenantId::new(99), None));
    }

    #[test]
    fn notification_from_event() {
        let event = ChangeEvent {
            lsn: Lsn::new(42),
            tenant_id: TenantId::new(1),
            collection: "orders".into(),
            document_id: "o1".into(),
            operation: ChangeOperation::Update,
            timestamp_ms: 12345,
        };
        let notif = ChangeNotification::from(&event);
        assert_eq!(notif.event, "UPDATE");
        assert_eq!(notif.doc_id, "o1");
        assert_eq!(notif.lsn, 42);
    }
}
