// SPDX-License-Identifier: BUSL-1.1

//! The Control-Plane change stream: a bounded replay ring over partitioned
//! feeds, and the live broadcast that follows it.
//!
//! In a cluster every replica of a data group publishes the group's writes
//! as the apply loop settles them, in log order, at their log positions.
//! The group leader forwards the feed's new events to the nodes that do not
//! replicate the group (see [`super::fanout`]). Every node therefore holds
//! the full feed at the same positions, and a cursor from any node resumes
//! on any other. Each feed is journaled durably (see [`super::journaling`]),
//! so a cursor inside retention also resumes after a restart.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use tracing::debug;

use crate::control::change_stream::journal::ChangeJournal;
use crate::control::change_stream::unjournaled::UnjournaledEntries;
use crate::event::cdc::CdcOffset;
use crate::types::{DatabaseId, TenantId};

use super::fanout::ChangeFanout;
use super::journal_debt::JournalDebt;
use super::ring::{AppendedRun, ChangeRun, ReplayError, ReplayRing, ReplaySnapshot, ReplayStart};
use super::staged::{HeldGauge, StagedEntry};
use super::{ChangeEvent, ChangePartition, SequencedChangeEvent, Subscription};

pub(super) struct BusState {
    pub(super) ring: ReplayRing,
    /// `(group, log index)` to the entry's staged changes. Bounded by the
    /// apply pipeline's window: every staged entry settles.
    pub(super) staged: BTreeMap<(u64, u64), StagedEntry>,
    /// Per data group, settled events above the group's saved applied
    /// floor that the journal lacks, which it takes when the floor rises
    /// past them. A spilled page is in the journal already and never here.
    pub(super) group_pending: BTreeMap<u64, Vec<SequencedChangeEvent>>,
    /// The events `staged` and `group_pending` hold in memory.
    pub(super) held: HeldGauge,
    /// The groups whose settled events the journal refused.
    pub(super) debt: JournalDebt,
}

/// Control-plane change notification bus.
pub struct ChangeStream {
    sender: tokio::sync::broadcast::Sender<SequencedChangeEvent>,
    legacy_sender: tokio::sync::broadcast::Sender<ChangeEvent>,
    next_sub_id: AtomicU64,
    active_subscriptions: Arc<AtomicU64>,
    events_published: AtomicU64,
    last_lsn: AtomicU64,
    state: std::sync::Mutex<BusState>,
    pub(super) fanout: ChangeFanout,
    /// The durable journal every feed is written to, once attached.
    pub(super) journal: OnceLock<Arc<ChangeJournal>>,
    /// The applied entries whose events the journal does not hold yet.
    unjournaled: UnjournaledEntries,
    capacity: usize,
}

impl ChangeStream {
    pub fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        let (sender, _) = tokio::sync::broadcast::channel(capacity);
        let (legacy_sender, _) = tokio::sync::broadcast::channel(capacity);
        Self {
            sender,
            legacy_sender,
            next_sub_id: AtomicU64::new(1),
            active_subscriptions: Arc::new(AtomicU64::new(0)),
            events_published: AtomicU64::new(0),
            last_lsn: AtomicU64::new(0),
            state: std::sync::Mutex::new(BusState {
                ring: ReplayRing::new(capacity),
                staged: BTreeMap::new(),
                group_pending: BTreeMap::new(),
                held: HeldGauge::default(),
                debt: JournalDebt::default(),
            }),
            fanout: ChangeFanout::default(),
            journal: OnceLock::new(),
            unjournaled: UnjournaledEntries::new(),
            capacity,
        }
    }

    /// The applied data-group entries whose change events the journal does
    /// not hold yet, which a restart stages again.
    pub fn unjournaled(&self) -> &UnjournaledEntries {
        &self.unjournaled
    }

    /// Events the replay ring holds.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub(super) fn lock(&self) -> std::sync::MutexGuard<'_, BusState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    pub fn subscribe(
        &self,
        collection_filter: Option<String>,
        tenant_filter: Option<TenantId>,
    ) -> Subscription {
        self.subscribe_scoped(collection_filter, tenant_filter, Some(DatabaseId::DEFAULT))
    }

    pub fn subscribe_in_database(
        &self,
        collection_filter: Option<String>,
        tenant_filter: Option<TenantId>,
        database_id: DatabaseId,
    ) -> Subscription {
        self.subscribe_scoped(collection_filter, tenant_filter, Some(database_id))
    }

    /// The receivers and the start cursor are taken under the ring lock, so
    /// the subscription receives exactly the events past its start cursor.
    fn subscribe_scoped(
        &self,
        collection_filter: Option<String>,
        tenant_filter: Option<TenantId>,
        database_filter: Option<DatabaseId>,
    ) -> Subscription {
        let id = self.next_sub_id.fetch_add(1, Ordering::Relaxed);
        self.active_subscriptions.fetch_add(1, Ordering::Relaxed);
        debug!(
            id,
            ?collection_filter,
            ?tenant_filter,
            ?database_filter,
            "change stream: new subscription"
        );
        let state = self.lock();
        Subscription {
            id,
            receiver: self.legacy_sender.subscribe(),
            collection_filter,
            tenant_filter,
            field_filter: Vec::new(),
            sequenced_receiver: self.sender.subscribe(),
            database_filter,
            active_counter: Arc::clone(&self.active_subscriptions),
            start: state.ring.head(),
        }
    }

    /// Record that a snapshot install covered `group_id`'s entries through
    /// `last_included_index`. This node never applies them, so it holds none
    /// of their events. A cursor below the install expires at once. Staged
    /// changes of a covered entry never settle, so they are dropped.
    pub(crate) fn install_group_floor(&self, group_id: u64, last_included_index: u64) {
        let mut guard = self.lock();
        let state = &mut *guard;
        let held = &mut state.held;
        state.staged.retain(|&(group, index), entry| {
            let kept = group != group_id || index > last_included_index;
            if !kept {
                held.sub(entry.held.len());
            }
            kept
        });
        state.ring.raise_floor(
            ChangePartition(group_id),
            CdcOffset::whole_index(last_included_index),
        );
    }

    pub(super) fn append_locked(&self, state: &mut BusState, run: ChangeRun) -> AppendedRun {
        let appended = state.ring.append(run);
        for event in &appended.events {
            self.last_lsn
                .fetch_max(event.lsn.as_u64(), Ordering::Relaxed);
            let _ = self.sender.send(event.clone());
            // The raw compatibility channel represents only the legacy
            // default database. It carries no database identity and must not
            // disclose non-default database events to legacy consumers.
            if event.database_id() == DatabaseId::DEFAULT {
                let _ = self.legacy_sender.send(event.event().clone());
            }
            self.events_published.fetch_add(1, Ordering::Relaxed);
        }
        appended
    }

    /// Wake the feed of `partition`'s new events to the nodes that do not
    /// replicate it, when this node leads it.
    pub(crate) fn forward(
        &self,
        shared: &Arc<crate::control::state::SharedState>,
        partition: ChangePartition,
    ) {
        if self.fanout.wake(shared, partition, false) {
            self.fanout.ensure_heartbeat(shared);
        }
    }

    /// Wake the feed of every partition this node leads, with the feed
    /// position advanced past a stretch with no event too.
    pub(crate) fn heartbeat(&self, shared: &Arc<crate::control::state::SharedState>) {
        for partition in self.partitions() {
            self.fanout.wake(shared, partition, true);
        }
    }

    /// Every partition this node saw.
    pub(crate) fn partitions(&self) -> Vec<ChangePartition> {
        self.lock().ring.partitions()
    }

    pub fn subscriber_count(&self) -> u64 {
        self.active_subscriptions.load(Ordering::Relaxed)
    }

    pub fn events_published(&self) -> u64 {
        self.events_published.load(Ordering::Relaxed)
    }

    /// Maximum observed WAL LSN; this is observability only, never replay state.
    pub fn last_lsn(&self) -> u64 {
        self.last_lsn.load(Ordering::Relaxed)
    }

    pub fn query_changes(
        &self,
        tenant_id: TenantId,
        collection: Option<&str>,
        start: ReplayStart,
        limit: usize,
    ) -> Result<ReplaySnapshot, ReplayError> {
        self.query_changes_in_database(tenant_id, DatabaseId::DEFAULT, collection, start, limit)
    }

    pub fn query_changes_in_database(
        &self,
        tenant_id: TenantId,
        database_id: DatabaseId,
        collection: Option<&str>,
        start: ReplayStart,
        limit: usize,
    ) -> Result<ReplaySnapshot, ReplayError> {
        self.replay(
            super::ring::ReplayScope {
                tenant_id,
                database_id,
                collection,
            },
            &start,
            limit,
        )
    }

    pub fn unsubscribe(&self) {
        self.active_subscriptions.fetch_sub(1, Ordering::Relaxed);
    }

    /// Publish `events` as the data-group entry `(group_id, log_index)`,
    /// the way a replica's apply stages the entry's changes and its apply
    /// loop settles it.
    #[cfg(test)]
    pub(crate) fn settle_entry(
        &self,
        group_id: u64,
        log_index: u64,
        database_id: DatabaseId,
        events: Vec<ChangeEvent>,
    ) {
        self.stage(group_id, log_index, database_id, events);
        self.settle_group(group_id, log_index, log_index);
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::RowIdentity;

    use super::*;
    use crate::control::change_stream::ChangeOperation;
    use crate::types::Lsn;

    fn event(lsn: u64, tenant: u64, document: &str) -> ChangeEvent {
        ChangeEvent {
            lsn: Lsn::new(lsn),
            tenant_id: TenantId::new(tenant),
            collection: "orders".into(),
            document_id: RowIdentity::from_user_key(document),
            operation: ChangeOperation::Insert,
            timestamp_ms: 1,
            after: None,
        }
    }

    fn first_page(stream: &ChangeStream, limit: usize) -> ReplaySnapshot {
        stream
            .query_changes(TenantId::new(1), None, ReplayStart::Timestamp(0), limit)
            .expect("replay")
    }

    /// Publish each event as its own entry of data group 1, at consecutive
    /// log indexes from 1.
    fn settle_each(stream: &ChangeStream, events: Vec<ChangeEvent>) {
        for (index, event) in (1..).zip(events) {
            stream.settle_entry(1, index, DatabaseId::DEFAULT, vec![event]);
        }
    }

    #[test]
    fn publication_order_not_lsn() {
        let stream = ChangeStream::new(8);
        settle_each(
            &stream,
            vec![event(102, 1, "first"), event(101, 1, "second")],
        );
        let first = first_page(&stream, 1);
        let next = stream
            .query_changes(
                TenantId::new(1),
                None,
                ReplayStart::Cursor(first.events[0].cursor.clone()),
                8,
            )
            .expect("replay");
        assert_eq!(next.events[0].document_id.as_str(), "second");
    }

    /// One entry's events share its LSN and page one at a time.
    #[test]
    fn duplicate_lsn_events_paginate() {
        let stream = ChangeStream::new(8);
        stream.settle_entry(
            1,
            1,
            DatabaseId::DEFAULT,
            vec![event(1, 1, "a"), event(1, 1, "b")],
        );
        let first = first_page(&stream, 1);
        let next = stream
            .query_changes(TenantId::new(1), None, ReplayStart::Cursor(first.cursor), 1)
            .expect("replay");
        assert_eq!(next.events[0].document_id.as_str(), "b");
    }

    /// A cursor whose event the ring evicted expires, as does one below a
    /// hole in the feed.
    #[test]
    fn evicted_cursors_and_cursors_below_a_hole_expire() {
        let stream = ChangeStream::new(1);
        stream.settle_entry(1, 1, DatabaseId::DEFAULT, vec![event(1, 1, "a")]);
        let cursor = first_page(&stream, 1).events[0].cursor.clone();
        stream.settle_entry(1, 2, DatabaseId::DEFAULT, vec![event(2, 1, "b")]);
        stream.settle_entry(1, 3, DatabaseId::DEFAULT, vec![event(3, 1, "c")]);
        assert!(matches!(
            stream.query_changes(TenantId::new(1), None, ReplayStart::Cursor(cursor), 1),
            Err(ReplayError::Expired)
        ));

        let holed = ChangeStream::new(8);
        holed.settle_entry(1, 1, DatabaseId::DEFAULT, vec![event(1, 1, "a")]);
        let below = first_page(&holed, 1).cursor;
        // Entries 2..=4 never reached this node.
        holed.settle_entry(1, 5, DatabaseId::DEFAULT, vec![event(5, 1, "e")]);
        assert!(matches!(
            holed.query_changes(TenantId::new(1), None, ReplayStart::Cursor(below), 1),
            Err(ReplayError::Expired)
        ));
    }

    /// A snapshot install expires a cursor below it before any later entry
    /// settles, and drops the staged changes of the entries it covers.
    #[test]
    fn a_cursor_below_a_snapshot_install_expires_at_once() {
        let stream = ChangeStream::new(8);
        stream.settle_entry(1, 1, DatabaseId::DEFAULT, vec![event(1, 1, "a")]);
        let below = first_page(&stream, 1).cursor;
        stream.stage(1, 3, DatabaseId::DEFAULT, vec![event(3, 1, "covered")]);
        stream.install_group_floor(1, 4);
        assert!(matches!(
            stream.query_changes(TenantId::new(1), None, ReplayStart::Cursor(below), 1),
            Err(ReplayError::Expired)
        ));
        stream.settle_group(1, 5, 5);
        assert!(
            first_page(&stream, 8)
                .events
                .iter()
                .all(|event| event.document_id.as_str() != "covered")
        );
    }

    #[test]
    fn tenant_filter_precedes_limit() {
        let stream = ChangeStream::new(8);
        settle_each(&stream, vec![event(1, 2, "other"), event(2, 1, "mine")]);
        let result = first_page(&stream, 1);
        assert_eq!(result.events[0].document_id.as_str(), "mine");
    }

    #[tokio::test]
    async fn database_filter_precedes_limit_for_query_and_subscription() {
        let stream = ChangeStream::new(8);
        let database_a = DatabaseId::new(1024);
        let database_b = DatabaseId::new(1025);
        let mut subscription =
            stream.subscribe_in_database(Some("orders".into()), Some(TenantId::new(1)), database_a);
        stream.settle_entry(1, 1, database_b, vec![event(1, 1, "database-b")]);
        stream.settle_entry(1, 2, database_a, vec![event(2, 1, "database-a")]);
        let result = stream
            .query_changes_in_database(
                TenantId::new(1),
                database_a,
                Some("orders"),
                ReplayStart::Timestamp(0),
                1,
            )
            .expect("replay");
        assert_eq!(result.events[0].document_id.as_str(), "database-a");
        let received = subscription.recv_sequenced().await.expect("event");
        assert_eq!(received.document_id.as_str(), "database-a");
    }

    /// Two replicas settle one group's entries; a cursor from the first
    /// resumes on the second at the next event, and each settled run
    /// publishes the entry's changes once.
    #[test]
    fn replicas_publish_a_settled_entry_at_the_same_position() {
        let replicas = [ChangeStream::new(16), ChangeStream::new(16)];
        for stream in &replicas {
            stream.stage(
                7,
                10,
                DatabaseId::DEFAULT,
                vec![event(900, 1, "a"), event(900, 1, "b")],
            );
            stream.stage(7, 12, DatabaseId::DEFAULT, vec![event(901, 1, "c")]);
            stream.settle_group(7, 10, 12);
        }
        let page = first_page(&replicas[0], 2);
        assert!(page.has_more);
        let rest = replicas[1]
            .query_changes(TenantId::new(1), None, ReplayStart::Cursor(page.cursor), 8)
            .expect("the second replica holds the same feed");
        assert_eq!(rest.events.len(), 1);
        assert_eq!(rest.events[0].document_id.as_str(), "c");
        assert_eq!(rest.events[0].position(), CdcOffset::data_event(0, 12, 1));
        assert_eq!(replicas[1].events_published(), 3);
    }

    const TENANT_A: u64 = 10;
    const TENANT_B: u64 = 20;

    fn tenant_event(
        collection: &str,
        document: &str,
        tenant: u64,
        lsn: u64,
        timestamp_ms: u64,
    ) -> ChangeEvent {
        ChangeEvent {
            lsn: Lsn::new(lsn),
            tenant_id: TenantId::new(tenant),
            collection: collection.into(),
            document_id: RowIdentity::from_user_key(document),
            operation: ChangeOperation::Insert,
            timestamp_ms,
            after: None,
        }
    }

    fn tenant_replay(
        stream: &ChangeStream,
        tenant: u64,
        collection: &str,
        limit: usize,
    ) -> Vec<crate::control::change_stream::ReplayedChange> {
        stream
            .query_changes(
                TenantId::new(tenant),
                Some(collection),
                ReplayStart::Timestamp(0),
                limit,
            )
            .expect("timestamp replay cannot expire")
            .events
    }

    /// A tenant's replay returns only its own events on a shared collection.
    #[test]
    fn cdc_stream_isolated_between_tenants() {
        let stream = ChangeStream::new(1024);
        let _sub_b = stream.subscribe(Some("orders".into()), Some(TenantId::new(TENANT_B)));
        settle_each(
            &stream,
            vec![
                tenant_event("orders", "order_1", TENANT_A, 1, 1000),
                tenant_event("orders", "order_2", TENANT_B, 2, 2000),
            ],
        );

        let a_events = tenant_replay(&stream, TENANT_A, "orders", 100);
        let b_events = tenant_replay(&stream, TENANT_B, "orders", 100);
        assert_eq!(a_events.len(), 1);
        assert_eq!(a_events[0].tenant_id, TenantId::new(TENANT_A));
        assert_eq!(a_events[0].document_id.as_str(), "order_1");
        assert_eq!(b_events.len(), 1);
        assert_eq!(b_events[0].tenant_id, TenantId::new(TENANT_B));
        assert_eq!(b_events[0].document_id.as_str(), "order_2");
    }

    /// Events of one millisecond page one at a time through their cursors.
    #[test]
    fn cdc_opaque_cursor_keeps_same_millisecond_events_pageable() {
        let stream = ChangeStream::new(1024);
        let tenant_id = TenantId::new(TENANT_A);
        settle_each(
            &stream,
            ["first", "second", "third"]
                .into_iter()
                .zip(1..)
                .map(|(document, lsn)| tenant_event("orders", document, TENANT_A, lsn, 1_000))
                .collect(),
        );

        let first_page = stream
            .query_changes(tenant_id, Some("orders"), ReplayStart::Timestamp(0), 1)
            .expect("timestamp replay cannot expire");
        assert_eq!(first_page.events.len(), 1);
        assert_eq!(first_page.events[0].document_id.as_str(), "first");

        let second_page = stream
            .query_changes(
                tenant_id,
                Some("orders"),
                ReplayStart::Cursor(first_page.events[0].cursor.clone()),
                1,
            )
            .expect("fresh cursor must resume");
        assert_eq!(second_page.events.len(), 1);
        assert_eq!(second_page.events[0].document_id.as_str(), "second");
    }

    /// A replay of one collection returns none of another's events.
    #[test]
    fn cdc_different_collections_isolated() {
        let stream = ChangeStream::new(1024);
        settle_each(
            &stream,
            vec![
                tenant_event("orders", "o1", TENANT_A, 1, 1000),
                tenant_event("users", "u1", TENANT_A, 2, 2000),
            ],
        );
        let order_events = tenant_replay(&stream, TENANT_A, "orders", 100);
        assert!(!order_events.is_empty());
        for event in &order_events {
            assert_eq!(event.collection, "orders");
        }
    }

    /// A subscription scoped to Tenant B never delivers Tenant A's events,
    /// even when both tenants write to the same collection.
    #[tokio::test]
    async fn cdc_tenant_b_subscription_rejects_tenant_a_events() {
        let stream = ChangeStream::new(1024);
        let mut sub_b = stream.subscribe(Some("orders".into()), Some(TenantId::new(TENANT_B)));
        let mut events: Vec<ChangeEvent> = (0..5u64)
            .map(|i| {
                tenant_event(
                    "orders",
                    &format!("a_order_{i}"),
                    TENANT_A,
                    i + 1,
                    (i + 1) * 1000,
                )
            })
            .collect();
        events.push(tenant_event("orders", "b_order_1", TENANT_B, 100, 10_000));
        settle_each(&stream, events);

        let received =
            tokio::time::timeout(std::time::Duration::from_millis(500), sub_b.recv_filtered())
                .await
                .expect("timed out waiting for Tenant B's event")
                .expect("channel error");
        assert_eq!(received.tenant_id, TenantId::new(TENANT_B));
        assert_eq!(received.document_id.as_str(), "b_order_1");

        let second =
            tokio::time::timeout(std::time::Duration::from_millis(50), sub_b.recv_filtered()).await;
        assert!(
            second.is_err(),
            "Tenant A's events must have been filtered from Tenant B's subscription"
        );
    }

    /// A subscription with no tenant filter receives every tenant's events:
    /// the filter is opt-in.
    #[tokio::test]
    async fn cdc_unfiltered_subscription_receives_all_tenants() {
        let stream = ChangeStream::new(1024);
        let mut sub_all = stream.subscribe(None, None);
        settle_each(
            &stream,
            vec![
                tenant_event("events", "e_a", TENANT_A, 1, 1000),
                tenant_event("events", "e_b", TENANT_B, 2, 2000),
            ],
        );

        let mut tenant_ids = Vec::new();
        for _ in 0..2 {
            let received = tokio::time::timeout(
                std::time::Duration::from_millis(200),
                sub_all.recv_filtered(),
            )
            .await
            .expect("timed out waiting for an event")
            .expect("channel error");
            tenant_ids.push(received.tenant_id);
        }
        assert!(tenant_ids.contains(&TenantId::new(TENANT_A)));
        assert!(tenant_ids.contains(&TenantId::new(TENANT_B)));
    }

    /// `query_changes` filters by tenant before it applies the caller's limit.
    #[test]
    fn cdc_query_changes_scopes_the_requested_tenant_before_limit() {
        let stream = ChangeStream::new(1024);
        settle_each(
            &stream,
            vec![
                tenant_event("logs", "l_b_1", TENANT_B, 1, 1_000),
                tenant_event("logs", "l_b_2", TENANT_B, 2, 1_000),
                tenant_event("logs", "l_a", TENANT_A, 3, 1_000),
            ],
        );
        let a_events = tenant_replay(&stream, TENANT_A, "logs", 1);
        assert_eq!(a_events.len(), 1);
        assert_eq!(a_events[0].tenant_id, TenantId::new(TENANT_A));
        assert_eq!(a_events[0].document_id.as_str(), "l_a");
    }
}
