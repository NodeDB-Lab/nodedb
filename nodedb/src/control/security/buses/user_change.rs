// SPDX-License-Identifier: BUSL-1.1

//! User-change broadcast channel.
//!
//! Producers publish a [`UserChanged`] event on every [`CredentialStore`]
//! mutation. The bus feeds the audit log: its consumer writes one
//! `PrivilegeChange` row per event. The user's version counter moves ahead of
//! the publish, so a lost event costs that row and nothing else.
//!
//! Shutdown: the bus consumer exits when the `Sender` is dropped.
//! Capacity: 1 024 events.

use tokio::sync::broadcast;

/// Event published whenever a `CredentialStore` mutation affects a user.
#[derive(Debug, Clone)]
pub struct UserChanged {
    /// The affected user's stable numeric ID.
    pub user_id: u64,
}

/// Bounded broadcast channel for user-change events.
///
/// Dropping the [`Sender`] half shuts down the consumer task.
pub struct UserChangeBus {
    tx: broadcast::Sender<UserChanged>,
}

/// Capacity of the user-change broadcast channel.
pub const USER_CHANGE_CHANNEL_CAPACITY: usize = 1_024;

impl UserChangeBus {
    /// Create a new bus and return the bus wrapper together with a
    /// pre-subscribed receiver for the consumer task.
    pub fn new() -> (Self, broadcast::Receiver<UserChanged>) {
        let (tx, rx) = broadcast::channel(USER_CHANGE_CHANNEL_CAPACITY);
        (Self { tx }, rx)
    }

    /// Wrap an existing sender as a publisher.  Used to give the
    /// `CredentialStore` a publishing handle to the same channel
    /// without creating a second independent bus.
    pub fn from_existing(tx: broadcast::Sender<UserChanged>) -> Self {
        Self { tx }
    }

    /// Publish a user-change event.
    ///
    /// Returns the number of active receivers that accepted the message.
    pub fn publish(&self, event: UserChanged) -> usize {
        self.tx.send(event).unwrap_or(0)
    }

    /// Subscribe a new receiver.  Used by the consumer task.
    pub fn subscribe(&self) -> broadcast::Receiver<UserChanged> {
        self.tx.subscribe()
    }

    /// Returns a clone of the underlying sender.
    ///
    /// Dropping all sender clones shuts down the consumer.
    pub fn sender(&self) -> broadcast::Sender<UserChanged> {
        self.tx.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn user_change_bus_produce_consume() {
        let (bus, mut rx) = UserChangeBus::new();
        bus.publish(UserChanged { user_id: 7 });
        let event = rx.recv().await.expect("should receive event");
        assert_eq!(event.user_id, 7);
    }

    #[tokio::test]
    async fn multiple_events_ordered() {
        let (bus, mut rx) = UserChangeBus::new();
        for id in [1u64, 2, 3] {
            bus.publish(UserChanged { user_id: id });
        }
        for expected in [1u64, 2, 3] {
            let ev = rx.recv().await.expect("event present");
            assert_eq!(ev.user_id, expected);
        }
    }
}
