//! LISTEN/NOTIFY: PostgreSQL-compatible async change notifications.
//!
//! ```sql
//! LISTEN orders;        -- subscribe to 'orders' collection changes
//! UNLISTEN orders;      -- unsubscribe
//! NOTIFY orders, 'custom payload';  -- manual notification
//! ```
//!
//! When a mutation commits on a listened collection, the server pushes
//! an async `NotificationResponse` to the client with the collection name,
//! changed doc_id, and operation type. Standard PostgreSQL drivers handle
//! this transparently.

use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::RwLock;

use tracing::debug;

use crate::control::change_stream::{ChangeEvent, ChangeStream, Subscription};
use crate::types::TenantId;

/// Per-connection LISTEN state.
struct ListenState {
    /// Collections this connection is listening to.
    channels: HashSet<String>,
    /// Change stream subscription (created on first LISTEN).
    subscription: Option<Subscription>,
    /// Tenant scope for this connection.
    tenant_id: TenantId,
}

/// Manages LISTEN/NOTIFY subscriptions across all pgwire connections.
pub struct ListenNotifyManager {
    /// Per-connection state: addr → ListenState.
    connections: RwLock<HashMap<SocketAddr, ListenState>>,
}

impl Default for ListenNotifyManager {
    fn default() -> Self {
        Self::new()
    }
}

impl ListenNotifyManager {
    pub fn new() -> Self {
        Self {
            connections: RwLock::new(HashMap::new()),
        }
    }

    /// Handle `LISTEN <channel>` command.
    ///
    /// Subscribes the connection to change notifications for the named
    /// collection. If this is the first LISTEN on this connection,
    /// creates a change stream subscription.
    pub fn listen(
        &self,
        addr: &SocketAddr,
        channel: &str,
        tenant_id: TenantId,
        change_stream: &ChangeStream,
    ) {
        let mut conns = self.connections.write().unwrap_or_else(|p| p.into_inner());
        let state = conns.entry(*addr).or_insert_with(|| ListenState {
            channels: HashSet::new(),
            subscription: None,
            tenant_id,
        });

        if state.channels.insert(channel.to_string()) {
            debug!(%addr, channel, "LISTEN registered");

            // Create subscription on first LISTEN.
            if state.subscription.is_none() {
                state.subscription = Some(change_stream.subscribe(None, Some(tenant_id)));
            }
        }
    }

    /// Handle `UNLISTEN <channel>` command.
    pub fn unlisten(&self, addr: &SocketAddr, channel: &str) {
        let mut conns = self.connections.write().unwrap_or_else(|p| p.into_inner());
        if let Some(state) = conns.get_mut(addr) {
            state.channels.remove(channel);
            debug!(%addr, channel, "UNLISTEN");

            // If no more channels, drop the subscription.
            if state.channels.is_empty() {
                state.subscription = None;
            }
        }
    }

    /// Handle `UNLISTEN *` — remove all subscriptions for this connection.
    pub fn unlisten_all(&self, addr: &SocketAddr, change_stream: &ChangeStream) {
        let mut conns = self.connections.write().unwrap_or_else(|p| p.into_inner());
        if let Some(state) = conns.remove(addr) {
            if state.subscription.is_some() {
                change_stream.unsubscribe();
            }
            debug!(%addr, channels = state.channels.len(), "UNLISTEN *");
        }
    }

    /// Clean up when a connection closes.
    pub fn connection_closed(&self, addr: &SocketAddr, change_stream: &ChangeStream) {
        self.unlisten_all(addr, change_stream);
    }

    /// Check if a connection is listening to a specific channel.
    pub fn is_listening(&self, addr: &SocketAddr, channel: &str) -> bool {
        let conns = self.connections.read().unwrap_or_else(|p| p.into_inner());
        conns
            .get(addr)
            .is_some_and(|s| s.channels.contains(channel))
    }

    /// Get all channels a connection is listening to.
    pub fn channels_for(&self, addr: &SocketAddr) -> Vec<String> {
        let conns = self.connections.read().unwrap_or_else(|p| p.into_inner());
        conns
            .get(addr)
            .map(|s| s.channels.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Check if a change event should be delivered to a connection.
    ///
    /// Returns true if the connection is listening to the event's collection.
    pub fn should_deliver(&self, addr: &SocketAddr, event: &ChangeEvent) -> bool {
        let conns = self.connections.read().unwrap_or_else(|p| p.into_inner());
        conns.get(addr).is_some_and(|s| {
            s.tenant_id == event.tenant_id && s.channels.contains(&event.collection)
        })
    }

    /// Format a change event as a PostgreSQL notification payload.
    ///
    /// Format: `"<operation>:<doc_id>"` (e.g., `"INSERT:user_42"`).
    pub fn format_notification(event: &ChangeEvent) -> String {
        format!("{}:{}", event.operation.as_str(), event.document_id)
    }

    /// Number of connections with active LISTEN subscriptions.
    pub fn listener_count(&self) -> usize {
        let conns = self.connections.read().unwrap_or_else(|p| p.into_inner());
        conns.values().filter(|s| !s.channels.is_empty()).count()
    }

    /// Total channels being listened across all connections.
    pub fn total_channels(&self) -> usize {
        let conns = self.connections.read().unwrap_or_else(|p| p.into_inner());
        conns.values().map(|s| s.channels.len()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::change_stream::ChangeOperation;

    fn addr(port: u16) -> SocketAddr {
        format!("127.0.0.1:{port}").parse().unwrap()
    }

    #[test]
    fn listen_and_check() {
        let mgr = ListenNotifyManager::new();
        let stream = ChangeStream::new(64);
        let a = addr(5000);

        mgr.listen(&a, "orders", TenantId::new(1), &stream);
        assert!(mgr.is_listening(&a, "orders"));
        assert!(!mgr.is_listening(&a, "users"));
        assert_eq!(mgr.listener_count(), 1);
    }

    #[test]
    fn unlisten() {
        let mgr = ListenNotifyManager::new();
        let stream = ChangeStream::new(64);
        let a = addr(5001);

        mgr.listen(&a, "orders", TenantId::new(1), &stream);
        mgr.unlisten(&a, "orders");
        assert!(!mgr.is_listening(&a, "orders"));
        assert_eq!(mgr.listener_count(), 0);
    }

    #[test]
    fn should_deliver() {
        let mgr = ListenNotifyManager::new();
        let stream = ChangeStream::new(64);
        let a = addr(5002);

        mgr.listen(&a, "orders", TenantId::new(1), &stream);

        let event = ChangeEvent {
            lsn: crate::types::Lsn::new(1),
            tenant_id: TenantId::new(1),
            collection: "orders".into(),
            document_id: "o1".into(),
            operation: ChangeOperation::Insert,
            timestamp_ms: 0,
        };
        assert!(mgr.should_deliver(&a, &event));

        // Wrong collection.
        let event2 = ChangeEvent {
            collection: "users".into(),
            ..event.clone()
        };
        assert!(!mgr.should_deliver(&a, &event2));

        // Wrong tenant.
        let event3 = ChangeEvent {
            tenant_id: TenantId::new(99),
            ..event
        };
        assert!(!mgr.should_deliver(&a, &event3));
    }

    #[test]
    fn format_notification() {
        let event = ChangeEvent {
            lsn: crate::types::Lsn::new(1),
            tenant_id: TenantId::new(1),
            collection: "orders".into(),
            document_id: "o42".into(),
            operation: ChangeOperation::Update,
            timestamp_ms: 0,
        };
        assert_eq!(
            ListenNotifyManager::format_notification(&event),
            "UPDATE:o42"
        );
    }
}
