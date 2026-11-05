// SPDX-License-Identifier: BUSL-1.1

//! Session-invalidation broadcast channel.
//!
//! Producers publish a [`SessionInvalidated`] event whenever an auth-state
//! mutation renders existing sessions incorrect (DROP USER, soft-delete via
//! `is_active=false`, GRANT/REVOKE ROLE, ALTER USER SET ROLE).
//!
//! The bus feeds the audit log: its consumer writes one `SessionRevoked` row per
//! event. It ends no session. A hard revoke ends its sessions on the mutation's
//! own path, before the event is published.
//!
//! Shutdown: the bus consumer exits when the `Sender` is dropped.
//! Capacity: 256 events.  Lag above that threshold costs audit rows: the
//! consumer records an `AuditBusLagged` row with the count and continues.

use tokio::sync::broadcast;

/// Reason a session was invalidated.
///
/// Every arm of this enum must be handled explicitly in the consumer.
/// No `_ =>` catch-all is permitted on matches over this type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SessionInvalidationReason {
    /// The user account was permanently deleted (`DROP USER`).
    UserDropped,
    /// The account was soft-deleted (`is_active = false`).
    UserDeactivated,
    /// A role was granted that changes the effective permission set.
    RoleGranted,
    /// A role was revoked from the user.
    RoleRevoked,
    /// The session's assigned role was altered (`ALTER USER … SET ROLE`).
    RoleAltered,
}

impl SessionInvalidationReason {
    /// Human-readable representation carried verbatim into audit detail strings.
    pub fn as_str(&self) -> &'static str {
        match self {
            SessionInvalidationReason::UserDropped => "UserDropped",
            SessionInvalidationReason::UserDeactivated => "UserDeactivated",
            SessionInvalidationReason::RoleGranted => "RoleGranted",
            SessionInvalidationReason::RoleRevoked => "RoleRevoked",
            SessionInvalidationReason::RoleAltered => "RoleAltered",
        }
    }

    /// Whether this reason demands a hard close of the session (as opposed to
    /// a soft rehydrate on the next request boundary).
    pub fn is_hard_revoke(&self) -> bool {
        match self {
            SessionInvalidationReason::UserDropped => true,
            SessionInvalidationReason::UserDeactivated => true,
            SessionInvalidationReason::RoleGranted => false,
            SessionInvalidationReason::RoleRevoked => false,
            SessionInvalidationReason::RoleAltered => false,
        }
    }
}

/// Event published on the session-invalidation bus.
#[derive(Debug, Clone)]
pub struct SessionInvalidated {
    /// The affected user's stable numeric ID.
    pub user_id: u64,
    /// Why the session was invalidated.
    pub reason: SessionInvalidationReason,
}

/// Bounded broadcast channel for session-invalidation events.
///
/// Dropping the [`Sender`] half shuts down the consumer task.
pub struct SessionInvalidationBus {
    tx: broadcast::Sender<SessionInvalidated>,
}

/// Capacity of the session-invalidation broadcast channel.
pub const SESSION_INVALIDATION_CHANNEL_CAPACITY: usize = 256;

impl SessionInvalidationBus {
    /// Create a new bus and return the bus wrapper together with a
    /// pre-subscribed receiver for the consumer task.
    pub fn new() -> (Self, broadcast::Receiver<SessionInvalidated>) {
        let (tx, rx) = broadcast::channel(SESSION_INVALIDATION_CHANNEL_CAPACITY);
        (Self { tx }, rx)
    }

    /// Wrap an existing sender (e.g. clone of the bus's sender) as a publisher.
    /// Used to give the `CredentialStore` a publishing handle to the same channel
    /// without creating a second independent bus.
    pub fn from_existing(tx: broadcast::Sender<SessionInvalidated>) -> Self {
        Self { tx }
    }

    /// Publish a session-invalidation event.
    ///
    /// Returns the number of active receivers that accepted the message.
    /// A return value of `0` means no consumer is subscribed — callers
    /// should not treat this as an error during startup or shutdown.
    pub fn publish(&self, event: SessionInvalidated) -> usize {
        self.tx.send(event).unwrap_or(0)
    }

    /// Subscribe a new receiver.  Used by the consumer task.
    pub fn subscribe(&self) -> broadcast::Receiver<SessionInvalidated> {
        self.tx.subscribe()
    }

    /// Returns a clone of the underlying sender.
    ///
    /// Dropping all sender clones shuts down the consumer.
    pub fn sender(&self) -> broadcast::Sender<SessionInvalidated> {
        self.tx.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn session_invalidation_bus_produce_consume() {
        let (bus, mut rx) = SessionInvalidationBus::new();
        bus.publish(SessionInvalidated {
            user_id: 42,
            reason: SessionInvalidationReason::UserDropped,
        });
        let event = rx.recv().await.expect("should receive event");
        assert_eq!(event.user_id, 42);
        assert!(matches!(
            event.reason,
            SessionInvalidationReason::UserDropped
        ));
    }

    #[test]
    fn reason_as_str_covers_all_variants() {
        let variants = [
            SessionInvalidationReason::UserDropped,
            SessionInvalidationReason::UserDeactivated,
            SessionInvalidationReason::RoleGranted,
            SessionInvalidationReason::RoleRevoked,
            SessionInvalidationReason::RoleAltered,
        ];
        for v in &variants {
            assert!(!v.as_str().is_empty());
        }
    }

    #[test]
    fn hard_revoke_correct_for_all_variants() {
        assert!(SessionInvalidationReason::UserDropped.is_hard_revoke());
        assert!(SessionInvalidationReason::UserDeactivated.is_hard_revoke());
        assert!(!SessionInvalidationReason::RoleGranted.is_hard_revoke());
        assert!(!SessionInvalidationReason::RoleRevoked.is_hard_revoke());
        assert!(!SessionInvalidationReason::RoleAltered.is_hard_revoke());
    }
}
