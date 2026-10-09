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

use nodedb_types::RowIdentity;
use tracing::debug;

use crate::control::change_stream::journal::ChangeJournal;
use crate::event::cdc::CdcOffset;
use crate::event::cross_shard::types::NotifyBroadcastMsg;
use crate::types::{DatabaseId, Lsn, TenantId};

use super::fanout::ChangeFanout;
use super::ring::{
    AppendedRun, ChangeRun, PositionedChange, ReplayError, ReplayRing, ReplayScope, ReplaySnapshot,
    ReplayStart,
};
use super::{ChangeEvent, ChangeOperation, ChangePartition, SequencedChangeEvent, Subscription};

/// A settled entry's changes, staged by the write funnel until the apply
/// loop settles the entry in log order.
type StagedChanges = Vec<(DatabaseId, ChangeEvent)>;

pub(super) struct BusState {
    pub(super) ring: ReplayRing,
    /// `(group, log index)` to the entry's changes. Bounded by the apply
    /// pipeline's window: every staged entry settles.
    staged: BTreeMap<(u64, u64), StagedChanges>,
    /// Per partition, the position through which this node forwarded it.
    forwarded: BTreeMap<ChangePartition, CdcOffset>,
    /// Per data group, settled events above the group's saved applied
    /// floor, which the journal takes when the floor rises past them.
    pub(super) group_pending: BTreeMap<u64, Vec<SequencedChangeEvent>>,
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
    fanout: ChangeFanout,
    /// The durable journal every feed is written to, once attached.
    pub(super) journal: OnceLock<Arc<ChangeJournal>>,
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
                forwarded: BTreeMap::new(),
                group_pending: BTreeMap::new(),
            }),
            fanout: ChangeFanout::default(),
            journal: OnceLock::new(),
            capacity,
        }
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

    /// Hold the changes of the data-group entry at `(group_id, log_index)`
    /// until the apply loop settles it.
    pub(crate) fn stage(
        &self,
        group_id: u64,
        log_index: u64,
        database_id: DatabaseId,
        events: Vec<ChangeEvent>,
    ) {
        if events.is_empty() {
            return;
        }
        let mut state = self.lock();
        state
            .staged
            .entry((group_id, log_index))
            .or_default()
            .extend(events.into_iter().map(|event| (database_id, event)));
    }

    /// Publish the staged changes of `group_id`'s entries `first..=last`,
    /// which the apply loop settled in log order. Returns the run for the
    /// nodes that do not replicate the group.
    pub(crate) fn settle_group(&self, group_id: u64, first: u64, last: u64) -> AppendedRun {
        let mut state = self.lock();
        let mut changes = Vec::new();
        let keys: Vec<(u64, u64)> = state
            .staged
            .range((group_id, 0)..=(group_id, last))
            .map(|(key, _)| *key)
            .collect();
        for key in keys {
            let Some(staged) = state.staged.remove(&key) else {
                continue;
            };
            // An entry below `first` settled in an earlier pass without
            // this node ever publishing it; it is not part of this run.
            if key.1 < first {
                continue;
            }
            for (ordinal, (database_id, event)) in staged.into_iter().enumerate() {
                changes.push(PositionedChange {
                    position: CdcOffset::data_event(0, key.1, ordinal as u64 + 1),
                    database_id,
                    event,
                });
            }
        }
        let run = ChangeRun {
            partition: ChangePartition(group_id),
            after: Some(CdcOffset::whole_index(first.saturating_sub(1))),
            through: CdcOffset::whole_index(last),
            changes,
        };
        let appended = self.append_locked(&mut state, run);
        self.queue_group(&mut state, group_id, &appended);
        appended
    }

    /// Record that a snapshot install covered `group_id`'s entries through
    /// `last_included_index`. This node never applies them, so it holds none
    /// of their events. A cursor below the install expires at once. Staged
    /// changes of a covered entry never settle, so they are dropped.
    pub(crate) fn install_group_floor(&self, group_id: u64, last_included_index: u64) {
        let mut state = self.lock();
        state
            .staged
            .retain(|&(group, index), _| group != group_id || index > last_included_index);
        state.ring.raise_floor(
            ChangePartition(group_id),
            CdcOffset::whole_index(last_included_index),
        );
    }

    /// Publish a run this node's own ordered feed produced, or a peer
    /// forwarded, and journal it before the caller acknowledges it.
    pub(crate) fn publish_run(&self, run: ChangeRun) -> AppendedRun {
        let appended = {
            let mut state = self.lock();
            self.append_locked(&mut state, run)
        };
        self.journal_run(&appended);
        appended
    }

    fn append_locked(&self, state: &mut BusState, run: ChangeRun) -> AppendedRun {
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

    /// Forward `partition`'s new events to the nodes that do not replicate
    /// it, when this node leads it.
    pub(crate) fn forward(
        &self,
        shared: &Arc<crate::control::state::SharedState>,
        partition: ChangePartition,
    ) {
        let targets = self.fanout.targets(shared, partition);
        if targets.is_empty() {
            return;
        }
        self.fanout.ensure_heartbeat(shared);
        if let Some(run) = self.outbound_run(partition, false) {
            self.fanout.send(shared, &run, &targets);
        }
    }

    /// Forward the position of every partition this node leads whose feed
    /// advanced since it last forwarded it, events or not.
    pub(crate) fn heartbeat(&self, shared: &crate::control::state::SharedState) {
        let partitions = self.lock().ring.partitions();
        for partition in partitions {
            let targets = self.fanout.targets(shared, partition);
            if targets.is_empty() {
                continue;
            }
            if let Some(run) = self.outbound_run(partition, true) {
                self.fanout.send(shared, &run, &targets);
            }
        }
    }

    /// The run this node forwards for `partition`: every held event past the
    /// position it last forwarded. `None` when the feed did not advance, or
    /// when it gained no event and `heartbeat` is off: the next run carries
    /// that stretch.
    pub(crate) fn outbound_run(
        &self,
        partition: ChangePartition,
        heartbeat: bool,
    ) -> Option<AppendedRun> {
        let mut state = self.lock();
        let (through, held_from) = state.ring.feed_bounds(partition)?;
        // Continuity is vouched only from where this node holds every event.
        let after = match state.forwarded.get(&partition) {
            Some(&sent) if sent >= held_from => sent,
            _ => held_from,
        };
        if through <= after {
            return None;
        }
        let events = state.ring.events_after(partition, after);
        if events.is_empty() && !heartbeat {
            return None;
        }
        state.forwarded.insert(partition, through);
        Some(AppendedRun {
            partition,
            after,
            through,
            events,
        })
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
        let state = self.lock();
        state.ring.replay(
            ReplayScope {
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

    /// Append a run a peer that leads the partition forwarded.
    pub fn deliver_remote_notify(&self, msg: &NotifyBroadcastMsg) {
        let Some(partition) = msg.partition() else {
            tracing::warn!(
                kind = msg.partition_kind,
                "change run names an unknown partition kind"
            );
            return;
        };
        let changes = msg
            .changes
            .iter()
            .map(|change| PositionedChange {
                position: change.position,
                database_id: DatabaseId::new(change.database_id),
                event: ChangeEvent {
                    lsn: Lsn::new(change.lsn),
                    tenant_id: TenantId::new(change.tenant_id),
                    collection: change.collection.clone(),
                    // The wire carries the identity as text; wrap it verbatim.
                    document_id: RowIdentity::from_user_key(change.document_id.clone()),
                    operation: parse_operation(&change.operation),
                    timestamp_ms: change.timestamp_ms,
                    after: None,
                },
            })
            .collect();
        self.publish_run(ChangeRun {
            partition,
            after: Some(msg.after),
            through: msg.through,
            changes,
        });
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

fn parse_operation(operation: &str) -> ChangeOperation {
    match operation {
        "UPDATE" => ChangeOperation::Update,
        "DELETE" => ChangeOperation::Delete,
        _ => ChangeOperation::Insert,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::change_stream::CursorStep;

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

    /// A node that does not replicate the group receives the leader's runs.
    /// A run that repeats a stretch appends nothing, and a lost run makes
    /// every cursor below it reset.
    #[test]
    fn forwarded_runs_deduplicate_and_report_a_lost_run() {
        let leader = ChangeStream::new(16);
        let outsider = ChangeStream::new(16);
        let mut live = outsider.subscribe(None, Some(TenantId::new(1)));
        let mut cursor = live.start_cursor().clone();
        let group = ChangePartition(3);
        let deliver = |run: &AppendedRun| {
            outsider.deliver_remote_notify(&NotifyBroadcastMsg::from_run(1, run))
        };

        leader.stage(3, 1, DatabaseId::DEFAULT, vec![event(1, 1, "a")]);
        leader.settle_group(3, 1, 1);
        let run = leader
            .outbound_run(group, false)
            .expect("a run with an event");
        deliver(&run);
        deliver(&run);
        let delivered = live.try_recv_sequenced().expect("first run");
        assert_eq!(cursor.accept(&delivered), CursorStep::Deliver);
        assert!(
            live.try_recv_sequenced().is_err(),
            "the repeat appended nothing"
        );

        // The run carrying entry 2 never arrives.
        leader.stage(3, 2, DatabaseId::DEFAULT, vec![event(2, 1, "lost")]);
        leader.settle_group(3, 2, 2);
        leader
            .outbound_run(group, false)
            .expect("a run with an event");
        leader.stage(3, 3, DatabaseId::DEFAULT, vec![event(3, 1, "c")]);
        leader.settle_group(3, 3, 3);
        deliver(
            &leader
                .outbound_run(group, false)
                .expect("a run with an event"),
        );
        let after_hole = live.try_recv_sequenced().expect("third run");
        assert_eq!(cursor.accept(&after_hole), CursorStep::Reset);
    }

    /// Settled stretches with no event send nothing. The next run with an
    /// event covers them, so the receiver sees no hole. An idle feed whose
    /// position advanced goes out on a heartbeat, once.
    #[test]
    fn a_stretch_with_no_event_is_carried_by_the_next_run() {
        let leader = ChangeStream::new(16);
        let outsider = ChangeStream::new(16);
        let mut live = outsider.subscribe(None, Some(TenantId::new(1)));
        let mut cursor = live.start_cursor().clone();
        let group = ChangePartition(5);
        let deliver = |run: &AppendedRun| {
            outsider.deliver_remote_notify(&NotifyBroadcastMsg::from_run(1, run))
        };

        leader.stage(5, 1, DatabaseId::DEFAULT, vec![event(1, 1, "a")]);
        leader.settle_group(5, 1, 1);
        deliver(
            &leader
                .outbound_run(group, false)
                .expect("a run with an event"),
        );
        let first = live.try_recv_sequenced().expect("first event");
        assert_eq!(cursor.accept(&first), CursorStep::Deliver);

        // Entries 2..=9 carry no event: no message.
        for index in 2..=9 {
            leader.settle_group(5, index, index);
            assert!(leader.outbound_run(group, false).is_none());
        }
        leader.stage(5, 10, DatabaseId::DEFAULT, vec![event(10, 1, "b")]);
        leader.settle_group(5, 10, 10);
        let run = leader
            .outbound_run(group, false)
            .expect("a run with an event");
        assert_eq!(run.after, CdcOffset::whole_index(1));
        assert_eq!(run.events.len(), 1);
        deliver(&run);
        let next = live.try_recv_sequenced().expect("second event");
        assert_eq!(cursor.accept(&next), CursorStep::Deliver, "no false hole");

        // An idle stretch goes out once on a heartbeat.
        leader.settle_group(5, 11, 11);
        let heartbeat = leader
            .outbound_run(group, true)
            .expect("the position advanced");
        assert!(heartbeat.events.is_empty());
        assert_eq!(heartbeat.through, CdcOffset::whole_index(11));
        deliver(&heartbeat);
        assert!(
            leader.outbound_run(group, true).is_none(),
            "nothing new to say"
        );
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
