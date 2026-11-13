// SPDX-License-Identifier: BUSL-1.1

//! CRDT sync delivery: session registry + per-session push queues.
//!
//! Manages connected Lite sessions and delivers outbound deltas to them.
//! Each session has a bounded mpsc channel. The background delivery task
//! monitors channel health and removes disconnected sessions.
//!
//! **Deny by default:** a registration starts with no subscription and
//! receives nothing. A shape enters it only after the Control Plane
//! authorized the subscription. A row delta reaches a session only for a
//! collection one of its shapes names, and the session's push-time checks
//! then judge the row itself.
//!
//! **Backpressure:** a delta that finds its session's channel full is
//! dropped: the newest delta, never a queued one. The drop records the
//! collection on the session, and the session re-snapshots its shapes on it,
//! so the client does not stay diverged.
//!
//! **Definition notices:** a [`DeltaAudience::TenantAdmins`] notice has its
//! own bounded queue. A session registers that queue only while its
//! principal is a superuser or tenant admin, and the session checks the
//! privilege again at push time. A dropped notice marks its definition
//! family, and the session sends the family's full current set.
//!
//! **Locks:** the registry lock covers the session map, each session's
//! subscriptions have their own lock, and the interest index has its own.
//! The order is registry, then session subscriptions, then interest. A
//! subscription change takes the registry lock for reading only.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tracing::{debug, info, trace, warn};

use super::interest::CollectionInterest;
use super::subscriptions::{
    CollectionChange, SessionSubscriptions, SharedSubscriptions, SubscribedShape,
};
use super::types::{ControlFrame, DeliveryConfig, DeltaAudience, LiteSessionHandle, OutboundDelta};
use crate::types::DatabaseId;

/// The outcome of adding a shape to a session's registration.
#[derive(Debug, Clone, PartialEq)]
pub enum ShapeRegistration {
    /// The session has no delivery registration. Nothing changed.
    SessionNotRegistered,
    /// The registration holds the shape. `replaced` is the shape it held
    /// under the same ID before, so a failed subscription can restore it.
    Registered { replaced: Option<SubscribedShape> },
}

/// Registry of connected Lite sessions for delta delivery.
///
/// Thread-safe: `register()`, `unregister()` and the subscription updates
/// run on the Control Plane sync listener tasks. `has_subscribers()` and
/// `enqueue()` run on Event Plane consumer tasks. Both planes are tokio code,
/// and nothing here reaches the Data Plane.
pub struct CrdtSyncDelivery {
    /// session_id → session handle.
    sessions: RwLock<HashMap<String, LiteSessionHandle>>,
    /// Sessions with a shape, counted per collection.
    interest: Mutex<CollectionInterest>,
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
            interest: Mutex::new(CollectionInterest::default()),
            deltas_delivered: std::sync::atomic::AtomicU64::new(0),
            deltas_dropped: std::sync::atomic::AtomicU64::new(0),
            sessions_registered: std::sync::atomic::AtomicU64::new(0),
        }
    }

    pub(super) fn sessions_read(
        &self,
    ) -> std::sync::RwLockReadGuard<'_, HashMap<String, LiteSessionHandle>> {
        self.sessions.read().unwrap_or_else(|p| p.into_inner())
    }

    pub(super) fn sessions_write(
        &self,
    ) -> std::sync::RwLockWriteGuard<'_, HashMap<String, LiteSessionHandle>> {
        self.sessions.write().unwrap_or_else(|p| p.into_inner())
    }

    fn interest(&self) -> std::sync::MutexGuard<'_, CollectionInterest> {
        self.interest.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Remove a session from the map the caller holds, and release its
    /// interest. Work is linear in the session's subscribed collections.
    fn release(
        &self,
        sessions: &mut HashMap<String, LiteSessionHandle>,
        session_id: &str,
    ) -> Option<LiteSessionHandle> {
        let handle = sessions.remove(session_id)?;
        let subscriptions = handle.subscriptions.read();
        let mut interest = self.interest();
        for collection in subscriptions.subscribed_collections() {
            interest.remove(handle.tenant_id, handle.database_id, collection);
        }
        drop(interest);
        drop(subscriptions);
        Some(handle)
    }

    /// Apply `change` to one session's subscriptions and keep the interest
    /// index in step. `None` when the session is not registered. Takes the
    /// registry lock for reading only, and does constant interest work.
    fn modify<R>(
        &self,
        session_id: &str,
        change: impl FnOnce(&mut SessionSubscriptions) -> (R, CollectionChange),
    ) -> Option<R> {
        let sessions = self.sessions_read();
        let handle = sessions.get(session_id)?;
        let mut subscriptions = handle.subscriptions.write();
        let (result, moved) = change(&mut subscriptions);
        self.interest()
            .apply(handle.tenant_id, handle.database_id, &moved);
        Some(result)
    }

    /// Register a connected Lite session for delta delivery.
    ///
    /// The registration starts with no subscription, so it receives nothing
    /// until an authorized shape or held collection is added. Registering an
    /// ID again replaces the earlier registration and its subscriptions.
    ///
    /// Returns an `mpsc::Receiver<OutboundDelta>` that the session writer
    /// task drains and sends to the Lite device via WebSocket.
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
        tenant_id: u64,
        database_id: DatabaseId,
        config: &DeliveryConfig,
    ) -> (mpsc::Receiver<OutboundDelta>, mpsc::Receiver<ControlFrame>) {
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
            database_id,
            subscriptions: Arc::new(SharedSubscriptions::default()),
            resync: Arc::new(Mutex::new(std::collections::HashSet::new())),
            admin_resync: Arc::new(Mutex::new(std::collections::HashSet::new())),
            sender: tx,
            control_sender: control_tx,
            admin_sender: None,
        };

        let mut sessions = self.sessions_write();
        self.release(&mut sessions, &session_id);
        sessions.insert(session_id.clone(), handle);
        drop(sessions);
        self.sessions_registered
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        info!(
            session = %session_id,
            peer_id,
            tenant_id,
            database_id = database_id.as_u64(),
            "registered Lite session for CRDT sync delivery"
        );

        (rx, control_rx)
    }

    /// Add an authorized shape to a session's registration.
    ///
    /// The caller has authorized the shape's read before this call. The shape
    /// is held before its snapshot is read, so no write between the snapshot
    /// and the registration is missed.
    pub fn subscribe_shape(
        &self,
        session_id: &str,
        shape_id: &str,
        shape: SubscribedShape,
    ) -> ShapeRegistration {
        match self.modify(session_id, |subscriptions| {
            subscriptions.insert_shape(shape_id.to_string(), shape)
        }) {
            Some(replaced) => ShapeRegistration::Registered { replaced },
            None => ShapeRegistration::SessionNotRegistered,
        }
    }

    /// Put back the shape a [`Self::subscribe_shape`] replaced, or release
    /// the shape when it replaced none. Used when the subscription failed
    /// after it was registered.
    pub fn restore_shape(
        &self,
        session_id: &str,
        shape_id: &str,
        replaced: Option<SubscribedShape>,
    ) {
        self.modify(session_id, |subscriptions| {
            let moved = match replaced {
                Some(shape) => subscriptions.insert_shape(shape_id.to_string(), shape).1,
                None => subscriptions.remove_shape(shape_id).1,
            };
            ((), moved)
        });
    }

    /// Release one shape from a session's registration. Returns the shape
    /// released, `None` when the registration held none under `shape_id`.
    pub fn unsubscribe_shape(&self, session_id: &str, shape_id: &str) -> Option<SubscribedShape> {
        self.modify(session_id, |subscriptions| {
            subscriptions.remove_shape(shape_id)
        })
        .flatten()
    }

    /// Record that a session's client stores `collection`, so a purge of the
    /// collection reaches it. The caller has authorized the principal's read
    /// of the collection. A held collection receives no row pushes.
    pub fn hold_collection(&self, session_id: &str, collection: &str) -> bool {
        self.modify(session_id, |subscriptions| {
            (subscriptions.hold(collection), CollectionChange::default())
        })
        .unwrap_or(false)
    }

    /// The session's subscription state. `None` when the session is not
    /// registered.
    pub fn subscriptions(&self, session_id: &str) -> Option<Arc<SharedSubscriptions>> {
        self.sessions_read()
            .get(session_id)
            .map(|handle| Arc::clone(&handle.subscriptions))
    }

    /// Take the collections whose deltas the session's full queue dropped.
    /// The caller re-snapshots the session's shapes on them.
    pub fn take_resync(&self, session_id: &str) -> Vec<String> {
        let sessions = self.sessions_read();
        let Some(handle) = sessions.get(session_id) else {
            return Vec::new();
        };
        let mut resync = handle.resync.lock().unwrap_or_else(|p| p.into_inner());
        resync.drain().collect()
    }

    /// Mark `collection` for the session's resync again: a resync that did
    /// not complete, or that this wake had no room for.
    pub fn mark_resync(&self, session_id: &str, collection: &str) {
        let sessions = self.sessions_read();
        if let Some(handle) = sessions.get(session_id) {
            mark(&handle.resync, collection);
        }
    }

    /// Unregister a disconnected Lite session.
    ///
    /// The mpsc sender is dropped, which signals the receiver.
    pub fn unregister(&self, session_id: &str) {
        let mut sessions = self.sessions_write();
        if self.release(&mut sessions, session_id).is_some() {
            debug!(session = %session_id, "unregistered Lite session from CRDT sync delivery");
        }
    }

    /// Whether any connected session has a shape on this exact collection.
    ///
    /// One map lookup: a write to a collection no session subscribes to is
    /// skipped before it is packaged.
    pub fn has_subscribers(
        &self,
        tenant_id: u64,
        database_id: DatabaseId,
        collection: &str,
    ) -> bool {
        self.interest().any(tenant_id, database_id, collection)
    }

    /// Enqueue an outbound delta for delivery to all matching sessions.
    ///
    /// A session must match the delta's tenant and database. A
    /// [`DeltaAudience::Subscribers`] delta also needs a shape of the session
    /// on its collection, and goes to the session's row queue. A
    /// [`DeltaAudience::TenantAdmins`] notice goes to the admin queue of every
    /// session of the scope that registered one, and the session's push-time
    /// check admits it only for a superuser or tenant admin. Uses `try_send`
    /// (non-blocking): a delta that finds its queue full is dropped, and the
    /// drop marks the collection for the session's resync.
    pub fn enqueue(&self, delta: OutboundDelta) {
        let sessions = self.sessions_read();

        for session in sessions.values() {
            if session.tenant_id != delta.tenant_id || session.database_id != delta.database_id {
                continue;
            }
            let (sender, marks) = match delta.audience {
                DeltaAudience::Subscribers => {
                    if !session.subscriptions.read().subscribes(&delta.collection) {
                        continue;
                    }
                    (&session.sender, &session.resync)
                }
                DeltaAudience::TenantAdmins => match session.admin_sender.as_ref() {
                    Some(sender) => (sender, &session.admin_resync),
                    None => continue,
                },
            };

            match sender.try_send(delta.clone()) {
                Ok(()) => {
                    self.deltas_delivered
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                Err(mpsc::error::TrySendError::Full(_)) => {
                    self.deltas_dropped
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    mark(marks, &delta.collection);
                    warn!(
                        session = %session.session_id,
                        collection = %delta.collection,
                        "CRDT sync channel full: newest delta dropped, collection marked for resync"
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
        self.sessions_read().len()
    }

    /// Remove sessions with closed channels (disconnected Lite devices).
    fn prune_closed(&self) {
        let mut sessions = self.sessions_write();
        let closed: Vec<String> = sessions
            .iter()
            .filter(|(_, handle)| handle.sender.is_closed())
            .map(|(id, _)| id.clone())
            .collect();
        for session_id in &closed {
            self.release(&mut sessions, session_id);
        }
        if !closed.is_empty() {
            debug!(pruned = closed.len(), "pruned disconnected Lite sessions");
        }
    }
}

/// Add `collection` to a session's resync marks.
pub(super) fn mark(marks: &Mutex<std::collections::HashSet<String>>, collection: &str) {
    let mut marks = marks.lock().unwrap_or_else(|p| p.into_inner());
    // A full queue marks on every dropped delta, so the common case is a mark
    // already present and allocates nothing.
    if !marks.contains(collection) {
        marks.insert(collection.to_string());
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
            database_id: DatabaseId::new(7),
            collection: collection.into(),
            document_id: "doc-1".into(),
            payload: vec![1, 2, 3],
            prior_payload: None,
            audience: DeltaAudience::Subscribers,
            op: DeltaOp::Upsert,
            lsn,
            tenant_id: 1,
            peer_id: 42,
            sequence: lsn,
        }
    }

    fn shape(collection: &str) -> SubscribedShape {
        SubscribedShape::new(collection.into(), Vec::new()).expect("empty predicate decodes")
    }

    fn register(
        delivery: &CrdtSyncDelivery,
        session_id: &str,
        database_id: DatabaseId,
        collections: &[&str],
        config: &DeliveryConfig,
    ) -> (mpsc::Receiver<OutboundDelta>, mpsc::Receiver<ControlFrame>) {
        let channels = delivery.register(session_id.into(), 42, 1, database_id, config);
        for collection in collections {
            assert_eq!(
                delivery.subscribe_shape(
                    session_id,
                    &format!("sh-{collection}"),
                    shape(collection)
                ),
                ShapeRegistration::Registered { replaced: None }
            );
        }
        channels
    }

    #[test]
    fn register_and_unregister() {
        let delivery = CrdtSyncDelivery::new();
        let config = DeliveryConfig::default();

        let (_rx, _crx) = register(&delivery, "s1", DatabaseId::new(7), &["orders"], &config);
        assert_eq!(delivery.session_count(), 1);
        assert!(delivery.has_subscribers(1, DatabaseId::new(7), "orders"));
        assert!(!delivery.has_subscribers(1, DatabaseId::new(7), "users"));
        assert!(!delivery.has_subscribers(2, DatabaseId::new(7), "orders"));
        assert!(!delivery.has_subscribers(1, DatabaseId::new(8), "orders"));

        delivery.unregister("s1");
        assert_eq!(delivery.session_count(), 0);
        assert!(!delivery.has_subscribers(1, DatabaseId::new(7), "orders"));
    }

    /// A registration with no subscription receives nothing: empty means
    /// deny, never "all collections".
    #[tokio::test]
    async fn empty_registration_receives_nothing() {
        let delivery = CrdtSyncDelivery::new();
        let config = DeliveryConfig::default();

        let (mut rx, mut crx) = register(&delivery, "s1", DatabaseId::new(7), &[], &config);
        assert!(!delivery.has_subscribers(1, DatabaseId::new(7), "orders"));

        delivery.enqueue(make_delta("orders", 1));
        delivery.broadcast_collection_purged(
            1,
            DatabaseId::new(7),
            "orders",
            5,
            nodedb_types::Hlc::ZERO,
        );

        assert!(rx.try_recv().is_err());
        assert!(crx.try_recv().is_err());
    }

    #[tokio::test]
    async fn enqueue_delivers_to_matching_session() {
        let delivery = CrdtSyncDelivery::new();
        let config = DeliveryConfig::default();

        let (mut rx, _crx) = register(&delivery, "s1", DatabaseId::new(7), &["orders"], &config);

        delivery.enqueue(make_delta("orders", 100));
        delivery.enqueue(make_delta("users", 200)); // Should NOT deliver.

        let delta = rx.try_recv().unwrap();
        assert_eq!(delta.collection, "orders");
        assert_eq!(delta.lsn, 100);

        // No more — "users" was filtered out.
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn unsubscribe_releases_the_collection() {
        let delivery = CrdtSyncDelivery::new();
        let config = DeliveryConfig::default();
        let (mut rx, _crx) = register(&delivery, "s1", DatabaseId::new(7), &["orders"], &config);

        assert_eq!(
            delivery.unsubscribe_shape("s1", "sh-orders"),
            Some(shape("orders"))
        );
        assert!(delivery.unsubscribe_shape("s1", "sh-orders").is_none());
        assert!(!delivery.has_subscribers(1, DatabaseId::new(7), "orders"));

        delivery.enqueue(make_delta("orders", 1));
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn interest_counts_every_subscribed_session() {
        let delivery = CrdtSyncDelivery::new();
        let config = DeliveryConfig::default();
        let (_a, _ac) = register(&delivery, "a", DatabaseId::new(7), &["orders"], &config);
        let (_b, _bc) = register(&delivery, "b", DatabaseId::new(7), &["orders"], &config);

        delivery.unregister("a");
        assert!(delivery.has_subscribers(1, DatabaseId::new(7), "orders"));
        assert!(delivery.unsubscribe_shape("b", "sh-orders").is_some());
        assert!(!delivery.has_subscribers(1, DatabaseId::new(7), "orders"));
    }

    #[test]
    fn restore_shape_undoes_a_failed_subscription() {
        let delivery = CrdtSyncDelivery::new();
        let config = DeliveryConfig::default();
        let (_rx, _crx) = register(&delivery, "s1", DatabaseId::new(7), &["orders"], &config);

        // A new shape under a fresh ID is released on restore.
        let ShapeRegistration::Registered { replaced } =
            delivery.subscribe_shape("s1", "sh-new", shape("users"))
        else {
            panic!("session is registered");
        };
        assert!(replaced.is_none());
        delivery.restore_shape("s1", "sh-new", replaced);
        assert!(!delivery.has_subscribers(1, DatabaseId::new(7), "users"));

        // A shape that replaced an earlier one puts the earlier one back.
        let ShapeRegistration::Registered { replaced } =
            delivery.subscribe_shape("s1", "sh-orders", shape("users"))
        else {
            panic!("session is registered");
        };
        assert_eq!(replaced, Some(shape("orders")));
        delivery.restore_shape("s1", "sh-orders", replaced);
        assert!(delivery.has_subscribers(1, DatabaseId::new(7), "orders"));
        assert!(!delivery.has_subscribers(1, DatabaseId::new(7), "users"));
    }

    #[test]
    fn subscription_to_an_unregistered_session_changes_nothing() {
        let delivery = CrdtSyncDelivery::new();
        assert_eq!(
            delivery.subscribe_shape("ghost", "sh1", shape("orders")),
            ShapeRegistration::SessionNotRegistered
        );
        assert!(!delivery.hold_collection("ghost", "orders"));
        assert!(delivery.subscriptions("ghost").is_none());
        assert!(!delivery.has_subscribers(1, DatabaseId::new(7), "orders"));
    }

    #[test]
    fn reregistering_drops_the_earlier_subscriptions() {
        let delivery = CrdtSyncDelivery::new();
        let config = DeliveryConfig::default();
        let (_rx, _crx) = register(&delivery, "s1", DatabaseId::new(7), &["orders"], &config);
        let (_rx2, _crx2) = register(&delivery, "s1", DatabaseId::new(7), &[], &config);

        assert!(!delivery.has_subscribers(1, DatabaseId::new(7), "orders"));
        assert_eq!(
            delivery.subscriptions("s1").map(|s| s.read().shape_count()),
            Some(0)
        );
    }

    #[test]
    fn held_collection_hears_purges_but_no_rows() {
        let delivery = CrdtSyncDelivery::new();
        let config = DeliveryConfig::default();
        let (mut rx, mut crx) = register(&delivery, "s1", DatabaseId::new(7), &[], &config);
        assert!(delivery.hold_collection("s1", "orders"));

        assert!(!delivery.has_subscribers(1, DatabaseId::new(7), "orders"));
        delivery.enqueue(make_delta("orders", 1));
        assert!(rx.try_recv().is_err());

        delivery.broadcast_collection_purged(
            1,
            DatabaseId::new(7),
            "orders",
            9,
            nodedb_types::Hlc::ZERO,
        );
        assert!(crx.try_recv().is_ok());
    }

    #[test]
    fn purge_reaches_only_sessions_that_hold_the_collection() {
        let delivery = CrdtSyncDelivery::new();
        let config = DeliveryConfig::default();
        let (_a, mut subscribed) =
            register(&delivery, "a", DatabaseId::new(7), &["orders"], &config);
        let (_b, mut other) = register(&delivery, "b", DatabaseId::new(7), &["users"], &config);

        delivery.broadcast_collection_purged(
            1,
            DatabaseId::new(7),
            "orders",
            9,
            nodedb_types::Hlc::ZERO,
        );

        assert!(subscribed.try_recv().is_ok());
        assert!(other.try_recv().is_err());
    }

    fn admin_notice() -> OutboundDelta {
        let mut notice = make_delta("_schedules", 0);
        notice.audience = DeltaAudience::TenantAdmins;
        notice
    }

    #[test]
    fn admin_notice_reaches_the_admin_queues_of_its_scope_only() {
        let delivery = CrdtSyncDelivery::new();
        let config = DeliveryConfig::default();
        let (mut same_rows, _sc) = register(&delivery, "same", DatabaseId::new(7), &[], &config);
        let mut same = delivery.register_admin_notices("same").expect("registered");
        let (_o, _oc) = register(&delivery, "other", DatabaseId::new(8), &[], &config);
        let mut other_db = delivery
            .register_admin_notices("other")
            .expect("registered");
        let (mut plain_rows, _pc) = register(&delivery, "plain", DatabaseId::new(7), &[], &config);

        delivery.enqueue(admin_notice());

        assert!(same.try_recv().is_ok());
        assert!(other_db.try_recv().is_err());
        assert!(
            same_rows.try_recv().is_err(),
            "a notice never enters the row queue"
        );
        assert!(
            plain_rows.try_recv().is_err(),
            "a session with no admin queue receives no notice"
        );
    }

    #[test]
    fn full_admin_queue_marks_the_family() {
        let delivery = CrdtSyncDelivery::new();
        let config = DeliveryConfig::default();
        let (_rows, _c) = register(&delivery, "admin", DatabaseId::new(7), &[], &config);
        let _notices = delivery
            .register_admin_notices("admin")
            .expect("registered");

        for _ in 0..=super::super::notices::ADMIN_NOTICE_QUEUE {
            delivery.enqueue(admin_notice());
        }

        assert_eq!(
            delivery.take_admin_resync("admin"),
            vec!["_schedules".to_string()]
        );
        assert!(delivery.take_resync("admin").is_empty());
    }

    #[test]
    fn a_marked_collection_is_taken_again() {
        let delivery = CrdtSyncDelivery::new();
        let config = DeliveryConfig::default();
        let (_rows, _c) = register(&delivery, "s1", DatabaseId::new(7), &["orders"], &config);

        delivery.mark_resync("s1", "orders");
        assert_eq!(delivery.take_resync("s1"), vec!["orders".to_string()]);
        assert!(delivery.take_resync("s1").is_empty());
    }

    #[tokio::test]
    async fn backpressure_drops_the_newest_and_marks_resync() {
        let delivery = CrdtSyncDelivery::new();
        let config = DeliveryConfig {
            max_queue_per_session: 2,
            ..Default::default()
        };

        let (mut rx, _crx) = register(
            &delivery,
            "s1",
            DatabaseId::new(7),
            &["a", "b", "c"],
            &config,
        );

        // Fill the channel (capacity 2).
        delivery.enqueue(make_delta("a", 1));
        delivery.enqueue(make_delta("b", 2));
        // The third is dropped (backpressure).
        delivery.enqueue(make_delta("c", 3));

        assert_eq!(
            delivery
                .deltas_dropped
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        // The queued deltas stay; the newest one is the one dropped.
        assert_eq!(
            rx.try_recv().map(|d| d.collection).ok().as_deref(),
            Some("a")
        );
        assert_eq!(
            rx.try_recv().map(|d| d.collection).ok().as_deref(),
            Some("b")
        );
        assert!(rx.try_recv().is_err());
        assert_eq!(delivery.take_resync("s1"), vec!["c".to_string()]);
        assert!(delivery.take_resync("s1").is_empty());
    }

    #[test]
    fn same_tenant_other_database_does_not_receive_collection_purged() {
        let delivery = CrdtSyncDelivery::new();
        let config = DeliveryConfig::default();
        let (_source_delta_rx, mut source_control_rx) = register(
            &delivery,
            "source-database",
            DatabaseId::new(7),
            &["orders"],
            &config,
        );
        let (_other_delta_rx, mut other_control_rx) = register(
            &delivery,
            "other-database",
            DatabaseId::new(8),
            &["orders"],
            &config,
        );

        delivery.broadcast_collection_purged(
            1,
            DatabaseId::new(7),
            "orders",
            100,
            nodedb_types::Hlc::ZERO,
        );

        let control = source_control_rx
            .try_recv()
            .expect("source database is notified");
        assert_eq!(control.collection, "orders");
        assert_eq!(control.database_id, DatabaseId::new(7));
        let msg: nodedb_types::sync::wire::CollectionPurgedMsg =
            control.frame.decode_body().expect("purge frame decodes");
        assert_eq!(msg.database_id, DatabaseId::new(7));
        assert!(other_control_rx.try_recv().is_err());
    }

    #[test]
    fn same_tenant_other_database_does_not_receive_delta() {
        let delivery = CrdtSyncDelivery::new();
        let config = DeliveryConfig::default();
        let (mut source_database_rx, _source_control_rx) = register(
            &delivery,
            "source-database",
            DatabaseId::new(7),
            &["orders"],
            &config,
        );
        let (mut other_database_rx, _other_control_rx) = register(
            &delivery,
            "other-database",
            DatabaseId::new(8),
            &["orders"],
            &config,
        );

        delivery.enqueue(make_delta("orders", 100));

        assert!(source_database_rx.try_recv().is_ok());
        assert!(other_database_rx.try_recv().is_err());
    }

    #[test]
    fn prune_removes_closed_sessions() {
        let delivery = CrdtSyncDelivery::new();
        let config = DeliveryConfig::default();

        let (rx, _crx) = register(&delivery, "s1", DatabaseId::new(7), &["orders"], &config);
        assert_eq!(delivery.session_count(), 1);

        // Drop receiver → channel closed.
        drop(rx);
        delivery.prune_closed();
        assert_eq!(delivery.session_count(), 0);
        assert!(!delivery.has_subscribers(1, DatabaseId::new(7), "orders"));
    }

    #[test]
    fn multiple_sessions_receive() {
        let delivery = CrdtSyncDelivery::new();
        let config = DeliveryConfig::default();

        let (mut rx1, _c1) = register(&delivery, "s1", DatabaseId::new(7), &["orders"], &config);
        let (mut rx2, _c2) = register(&delivery, "s2", DatabaseId::new(7), &["orders"], &config);

        delivery.enqueue(make_delta("orders", 100));

        assert!(rx1.try_recv().is_ok());
        assert!(rx2.try_recv().is_ok());
    }
}
