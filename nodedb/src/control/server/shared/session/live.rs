// SPDX-License-Identifier: BUSL-1.1

//! LIVE SELECT subscription methods on SessionStore.

use crate::control::change_stream::{ChangeCursor, SequencedChangeEvent, Subscription};

use super::connection::SessionId;
use super::store::SessionStore;

const LIVE_RESET_REQUIRED_LAGGED_PREFIX: &str = "RESET_REQUIRED:lagged:";
const LIVE_RESET_REQUIRED_CONTINUITY: &str = "RESET_REQUIRED:continuity";

/// Per-session LIVE state. Keeping the cursor beside its subscription makes
/// the continuity contract explicit and prevents accidental tuple-field swaps.
pub struct LiveSubscription {
    pub channel: String,
    pub subscription: Subscription,
    last_cursor: Option<ChangeCursor>,
}

impl LiveSubscription {
    fn new(channel: String, subscription: Subscription) -> Self {
        Self {
            channel,
            subscription,
            last_cursor: None,
        }
    }

    /// Accept the first cursor, then strictly increasing cursors in its epoch.
    /// A LIVE subscription filters the globally sequenced stream, so unrelated
    /// publications create legitimate sequence gaps. Older same-epoch
    /// duplicates are harmless queued overlap and are skipped.
    fn accept(&mut self, event: SequencedChangeEvent) -> LiveCursorResult {
        let cursor = event.cursor();
        let Some(previous) = self.last_cursor else {
            self.last_cursor = Some(cursor);
            return LiveCursorResult::Deliver(event);
        };
        if !cursor.same_epoch(previous) {
            return LiveCursorResult::Reset;
        }
        if cursor.sequence() <= previous.sequence() {
            return LiveCursorResult::Skip;
        }
        self.last_cursor = Some(cursor);
        LiveCursorResult::Deliver(event)
    }
}

enum LiveCursorResult {
    Deliver(SequencedChangeEvent),
    Skip,
    Reset,
}

/// The payload preserves the established `OPERATION:document_id` prefix.
/// New clients can parse the appended `;cursor=<opaque ChangeCursor>` suffix
/// to persist a precise delivery position without interpreting the token.
fn live_payload(event: &SequencedChangeEvent) -> String {
    format!(
        "{}:{};cursor={}",
        event.operation.as_str(),
        event.document_id,
        event.cursor()
    )
}

impl SessionStore {
    /// Store a LIVE SELECT subscription for a connection.
    ///
    /// `channel` is the notification channel name (e.g., "live_orders").
    pub fn add_live_subscription(
        &self,
        addr: impl Into<SessionId>,
        channel: String,
        sub: crate::control::change_stream::Subscription,
    ) {
        self.write_session(addr, |session| {
            session
                .live_subscriptions
                .push(LiveSubscription::new(channel, sub));
        });
    }

    /// Drain pending change events from all LIVE SELECT subscriptions
    /// for a connection. Returns `(channel, payload)` pairs ready to be
    /// sent as pgwire `NotificationResponse` messages.
    ///
    /// Non-blocking: uses `try_recv` to avoid waiting. Called between
    /// queries to deliver notifications in the PostgreSQL standard way.
    pub fn drain_live_notifications(&self, addr: impl Into<SessionId>) -> Vec<(String, String)> {
        self.write_session(addr, |session| {
            let mut notifications = Vec::new();
            let mut index = 0;
            while index < session.live_subscriptions.len() {
                let mut remove_subscription = false;
                {
                    let live = &mut session.live_subscriptions[index];
                    // Non-blocking drain: collect all pending events while
                    // preserving and validating their publication cursors.
                    loop {
                        match live.subscription.try_recv_sequenced() {
                            Ok(event) => match live.accept(event) {
                                LiveCursorResult::Deliver(event) => {
                                    notifications
                                        .push((live.channel.clone(), live_payload(&event)));
                                }
                                LiveCursorResult::Skip => {}
                                LiveCursorResult::Reset => {
                                    tracing::warn!(
                                        channel = live.channel.as_str(),
                                        "LIVE SELECT cursor discontinuity — reset required"
                                    );
                                    notifications.push((
                                        live.channel.clone(),
                                        LIVE_RESET_REQUIRED_CONTINUITY.into(),
                                    ));
                                    remove_subscription = true;
                                    break;
                                }
                            },
                            Err(tokio::sync::broadcast::error::TryRecvError::Empty) => break,
                            Err(tokio::sync::broadcast::error::TryRecvError::Lagged(n)) => {
                                tracing::warn!(
                                    channel = live.channel.as_str(),
                                    lagged = n,
                                    "LIVE SELECT subscription lagged — reset required"
                                );
                                notifications.push((
                                    live.channel.clone(),
                                    format!("{LIVE_RESET_REQUIRED_LAGGED_PREFIX}{n}"),
                                ));
                                remove_subscription = true;
                                break;
                            }
                            Err(tokio::sync::broadcast::error::TryRecvError::Closed) => {
                                remove_subscription = true;
                                break;
                            }
                        }
                    }
                }
                if remove_subscription {
                    session.live_subscriptions.remove(index);
                } else {
                    index += 1;
                }
            }
            notifications
        })
        .unwrap_or_default()
    }

    /// Check if a connection has any active LIVE SELECT subscriptions.
    pub fn has_live_subscriptions(&self, addr: impl Into<SessionId>) -> bool {
        self.read_session(addr, |s| !s.live_subscriptions.is_empty())
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::change_stream::{ChangeEvent, ChangeOperation, ChangeStream};
    use crate::types::{DatabaseId, Lsn, TenantId};
    use nodedb_types::RowIdentity;

    #[test]
    fn live_subscription_store_and_check() {
        let store = SessionStore::new();
        let addr: std::net::SocketAddr = "127.0.0.1:5001".parse().unwrap();
        store.ensure_session(addr);

        assert!(!store.has_live_subscriptions(addr));

        let stream = ChangeStream::new(64);
        let sub = stream.subscribe(Some("orders".into()), None);
        store.add_live_subscription(addr, "live_orders".into(), sub);

        assert!(store.has_live_subscriptions(addr));
    }

    #[test]
    fn live_subscription_drain_empty() {
        let store = SessionStore::new();
        let addr: std::net::SocketAddr = "127.0.0.1:5002".parse().unwrap();
        store.ensure_session(addr);

        let stream = ChangeStream::new(64);
        let sub = stream.subscribe(Some("orders".into()), None);
        store.add_live_subscription(addr, "live_orders".into(), sub);

        // No events published — drain returns empty.
        let notifications = store.drain_live_notifications(addr);
        assert!(notifications.is_empty());
    }

    #[test]
    fn live_subscription_drain_receives_events() {
        let store = SessionStore::new();
        let addr: std::net::SocketAddr = "127.0.0.1:5003".parse().unwrap();
        store.ensure_session(addr);

        let stream = ChangeStream::new(64);
        let sub = stream.subscribe(Some("orders".into()), None);
        store.add_live_subscription(addr, "live_orders".into(), sub);

        // Publish a matching event.
        stream.publish(ChangeEvent {
            lsn: Lsn::new(1),
            tenant_id: TenantId::new(1),
            collection: "orders".into(),
            document_id: RowIdentity::from_user_key("o42"),
            operation: ChangeOperation::Insert,
            timestamp_ms: 0,
            after: None,
        });

        let notifications = store.drain_live_notifications(addr);
        assert_eq!(notifications.len(), 1);
        assert_eq!(notifications[0].0, "live_orders");
        // The payload keeps the `OPERATION:document_id` prefix and appends an
        // opaque `;cursor=<token>` suffix clients persist as a delivery position.
        // The token itself is not asserted here — it is covered where it is built.
        assert!(
            notifications[0].1.starts_with("INSERT:o42;cursor="),
            "unexpected live payload: {}",
            notifications[0].1
        );
    }

    #[test]
    fn live_subscription_filters_by_collection() {
        let store = SessionStore::new();
        let addr: std::net::SocketAddr = "127.0.0.1:5004".parse().unwrap();
        store.ensure_session(addr);

        let stream = ChangeStream::new(64);
        let sub = stream.subscribe(Some("orders".into()), None);
        store.add_live_subscription(addr, "live_orders".into(), sub);

        // Publish event for a different collection — should be filtered out.
        stream.publish(ChangeEvent {
            lsn: Lsn::new(1),
            tenant_id: TenantId::new(1),
            collection: "users".into(),
            document_id: RowIdentity::from_user_key("u1"),
            operation: ChangeOperation::Update,
            timestamp_ms: 0,
            after: None,
        });

        let notifications = store.drain_live_notifications(addr);
        assert!(notifications.is_empty());
    }

    #[test]
    fn live_subscription_no_session_returns_empty() {
        let store = SessionStore::new();
        let addr: std::net::SocketAddr = "127.0.0.1:5005".parse().unwrap();
        // No session created — should return empty, not panic.
        let notifications = store.drain_live_notifications(addr);
        assert!(notifications.is_empty());
        assert!(!store.has_live_subscriptions(addr));
    }

    #[test]
    fn drain_live_notifications_isolates_selected_database() {
        let sessions = SessionStore::new();
        let stream = ChangeStream::new(8);
        let address: std::net::SocketAddr =
            "127.0.0.1:5007".parse().expect("valid test socket address");
        let session = SessionId::from(address);
        sessions.ensure_session(address);
        let selected_database = DatabaseId::new(9);
        let subscription = stream.subscribe_in_database(
            Some("orders".into()),
            Some(TenantId::new(1)),
            selected_database,
        );
        sessions.add_live_subscription(session, "live_orders".into(), subscription);

        for (database_id, lsn, document_id) in [
            (DatabaseId::DEFAULT, Lsn::new(1), "default-order"),
            (selected_database, Lsn::new(2), "selected-order"),
        ] {
            stream.publish_in_database(
                database_id,
                ChangeEvent {
                    lsn,
                    tenant_id: TenantId::new(1),
                    collection: "orders".into(),
                    document_id: RowIdentity::from_user_key(document_id),
                    operation: ChangeOperation::Insert,
                    timestamp_ms: 1,
                    after: None,
                },
            );
        }

        assert_eq!(
            sessions.drain_live_notifications(session),
            vec![(
                "live_orders".into(),
                "INSERT:selected-order;cursor=".to_owned()
                    + &stream
                        .query_changes_in_database(
                            TenantId::new(1),
                            selected_database,
                            Some("orders"),
                            crate::control::change_stream::ReplayStart::Timestamp(0),
                            1,
                        )
                        .expect("selected event cursor")
                        .events[0]
                        .cursor()
                        .to_string(),
            )]
        );
    }

    #[test]
    fn lagged_subscription_resets_and_is_removed() {
        let sessions = SessionStore::new();
        let stream = ChangeStream::new(1);
        let address: std::net::SocketAddr =
            "127.0.0.1:5009".parse().expect("valid test socket address");
        let session = SessionId::from(address);
        sessions.ensure_session(address);
        sessions.add_live_subscription(
            session,
            "live_orders".into(),
            stream.subscribe(Some("orders".into()), Some(TenantId::new(1))),
        );

        for (lsn, document_id) in [(Lsn::new(1), "dropped-order"), (Lsn::new(2), "gap-order")] {
            stream.publish(ChangeEvent {
                lsn,
                tenant_id: TenantId::new(1),
                collection: "orders".into(),
                document_id: RowIdentity::from_user_key(document_id),
                operation: ChangeOperation::Insert,
                timestamp_ms: 1,
                after: None,
            });
        }

        assert_eq!(
            sessions.drain_live_notifications(session),
            vec![("live_orders".into(), "RESET_REQUIRED:lagged:1".into())]
        );
        assert!(!sessions.has_live_subscriptions(session));
        assert_eq!(stream.subscriber_count(), 0);

        stream.publish(ChangeEvent {
            lsn: Lsn::new(3),
            tenant_id: TenantId::new(1),
            collection: "orders".into(),
            document_id: RowIdentity::from_user_key("later-order"),
            operation: ChangeOperation::Insert,
            timestamp_ms: 1,
            after: None,
        });
        assert!(sessions.drain_live_notifications(session).is_empty());
    }

    #[test]
    fn lagged_subscription_does_not_remove_healthy_sibling() {
        let sessions = SessionStore::new();
        let lagged_stream = ChangeStream::new(1);
        let healthy_stream = ChangeStream::new(8);
        let address: std::net::SocketAddr =
            "127.0.0.1:5010".parse().expect("valid test socket address");
        let session = SessionId::from(address);
        sessions.ensure_session(address);
        sessions.add_live_subscription(
            session,
            "lagged".into(),
            lagged_stream.subscribe(Some("orders".into()), Some(TenantId::new(1))),
        );
        sessions.add_live_subscription(
            session,
            "healthy".into(),
            healthy_stream.subscribe(Some("orders".into()), Some(TenantId::new(1))),
        );

        for lsn in [Lsn::new(1), Lsn::new(2)] {
            lagged_stream.publish(ChangeEvent {
                lsn,
                tenant_id: TenantId::new(1),
                collection: "orders".into(),
                document_id: RowIdentity::from_user_key("lagged-order"),
                operation: ChangeOperation::Insert,
                timestamp_ms: 1,
                after: None,
            });
        }
        healthy_stream.publish(ChangeEvent {
            lsn: Lsn::new(3),
            tenant_id: TenantId::new(1),
            collection: "orders".into(),
            document_id: RowIdentity::from_user_key("healthy-order"),
            operation: ChangeOperation::Insert,
            timestamp_ms: 1,
            after: None,
        });

        assert_eq!(
            sessions.drain_live_notifications(session),
            vec![
                ("lagged".into(), "RESET_REQUIRED:lagged:1".into()),
                (
                    "healthy".into(),
                    "INSERT:healthy-order;cursor=".to_owned()
                        + &healthy_stream
                            .query_changes(
                                TenantId::new(1),
                                None,
                                crate::control::change_stream::ReplayStart::Timestamp(0),
                                1,
                            )
                            .expect("healthy event cursor")
                            .events[0]
                            .cursor()
                            .to_string(),
                ),
            ]
        );
        assert!(sessions.has_live_subscriptions(session));
        assert_eq!(lagged_stream.subscriber_count(), 0);
        assert_eq!(healthy_stream.subscriber_count(), 1);
    }

    #[test]
    fn filtered_sequence_gaps_are_accepted_but_epoch_rotation_resets() {
        let stream = ChangeStream::new(8);
        let subscription = stream.subscribe(Some("orders".into()), Some(TenantId::new(1)));
        let mut live = LiveSubscription::new("live_orders".into(), subscription);
        let event = |cursor| {
            SequencedChangeEvent::new(
                cursor,
                DatabaseId::DEFAULT,
                ChangeEvent {
                    lsn: Lsn::new(1),
                    tenant_id: TenantId::new(1),
                    collection: "orders".into(),
                    document_id: RowIdentity::from_user_key("order"),
                    operation: ChangeOperation::Insert,
                    timestamp_ms: 1,
                    after: None,
                },
            )
        };
        assert!(matches!(
            live.accept(event(ChangeCursor::new(7, 1))),
            LiveCursorResult::Deliver(_)
        ));
        assert!(matches!(
            live.accept(event(ChangeCursor::new(7, 3))),
            LiveCursorResult::Deliver(_)
        ));

        let mut rotated = LiveSubscription::new(
            "rotated".into(),
            stream.subscribe(Some("orders".into()), Some(TenantId::new(1))),
        );
        assert!(matches!(
            rotated.accept(event(ChangeCursor::new(7, 1))),
            LiveCursorResult::Deliver(_)
        ));
        assert!(matches!(
            rotated.accept(event(ChangeCursor::new(8, 2))),
            LiveCursorResult::Reset
        ));
    }

    #[test]
    fn database_switch_drops_live_subscriptions_from_previous_database() {
        let sessions = SessionStore::new();
        let stream = ChangeStream::new(8);
        let address: std::net::SocketAddr =
            "127.0.0.1:5008".parse().expect("valid test socket address");
        let session = SessionId::from(address);
        sessions.ensure_session(address);
        let database_a = DatabaseId::new(8);
        let database_b = DatabaseId::new(9);
        let subscription =
            stream.subscribe_in_database(Some("orders".into()), Some(TenantId::new(1)), database_a);
        sessions.add_live_subscription(session, "live_orders".into(), subscription);
        assert_eq!(stream.subscriber_count(), 1);

        sessions.reset_for_database_switch(session, database_b);
        stream.publish_in_database(
            database_a,
            ChangeEvent {
                lsn: Lsn::new(3),
                tenant_id: TenantId::new(1),
                collection: "orders".into(),
                document_id: RowIdentity::from_user_key("old-database-order"),
                operation: ChangeOperation::Insert,
                timestamp_ms: 1,
                after: None,
            },
        );

        assert_eq!(stream.subscriber_count(), 0);
        assert!(sessions.drain_live_notifications(session).is_empty());
    }
}
