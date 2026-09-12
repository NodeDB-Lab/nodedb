// SPDX-License-Identifier: BUSL-1.1

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use nodedb_types::RowIdentity;
use tracing::{debug, trace, warn};

use crate::types::{DatabaseId, Lsn, TenantId};

use super::{ChangeCursor, ChangeEvent, ChangeOperation, SequencedChangeEvent, Subscription};

/// Replay start is either an acknowledged opaque cursor or an initial timestamp.
#[derive(Clone, Copy, Debug)]
pub enum ReplayStart {
    Cursor(ChangeCursor),
    Timestamp(u64),
}

/// A consistent ring-buffer replay snapshot and its publication high-water mark.
#[derive(Clone, Debug)]
pub struct ReplaySnapshot {
    pub events: Vec<SequencedChangeEvent>,
    pub snapshot_cursor: ChangeCursor,
}

/// A cursor cannot safely resume this in-memory stream; clients must reset.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReplayError {
    Expired,
}

struct ReplayState {
    epoch: u128,
    next_sequence: u64,
    recent_changes: VecDeque<SequencedChangeEvent>,
}

/// Control-plane WAL change notification bus.
pub struct ChangeStream {
    sender: tokio::sync::broadcast::Sender<SequencedChangeEvent>,
    legacy_sender: tokio::sync::broadcast::Sender<ChangeEvent>,
    next_sub_id: AtomicU64,
    active_subscriptions: Arc<AtomicU64>,
    events_published: AtomicU64,
    last_lsn: AtomicU64,
    replay_state: std::sync::Mutex<ReplayState>,
    recent_capacity: usize,
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
            replay_state: std::sync::Mutex::new(ReplayState {
                epoch: new_epoch(),
                next_sequence: 1,
                recent_changes: VecDeque::with_capacity(capacity),
            }),
            recent_capacity: capacity,
        }
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
        Subscription {
            id,
            receiver: self.legacy_sender.subscribe(),
            collection_filter,
            tenant_filter,
            field_filter: Vec::new(),
            sequenced_receiver: self.sender.subscribe(),
            database_filter,
            active_counter: Arc::clone(&self.active_subscriptions),
        }
    }

    /// Publish into the default database for legacy producer compatibility.
    pub fn publish(&self, event: ChangeEvent) {
        self.publish_in_database(DatabaseId::DEFAULT, event);
    }

    /// Allocate sequence, append the replay ring, and publish under one mutex.
    pub fn publish_in_database(&self, database_id: DatabaseId, event: ChangeEvent) {
        self.last_lsn
            .fetch_max(event.lsn.as_u64(), Ordering::Relaxed);
        let mut state = self
            .replay_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.next_sequence == 0 {
            state.epoch = new_epoch();
            state.next_sequence = 1;
            state.recent_changes.clear();
        }
        let cursor = ChangeCursor::new(state.epoch, state.next_sequence);
        state.next_sequence = state.next_sequence.checked_add(1).unwrap_or(0);
        let sequenced = SequencedChangeEvent::new(cursor, database_id, event.clone());
        if state.recent_changes.len() == self.recent_capacity {
            state.recent_changes.pop_front();
        }
        state.recent_changes.push_back(sequenced.clone());
        let _ = self.sender.send(sequenced);
        // The raw compatibility channel represents only the legacy default
        // database. It carries no database identity and must not disclose
        // non-default database events to legacy consumers.
        if database_id == DatabaseId::DEFAULT {
            let _ = self.legacy_sender.send(event);
        }
        self.events_published.fetch_add(1, Ordering::Relaxed);
    }

    pub fn publish_batch(&self, events: &[ChangeEvent]) {
        for event in events {
            self.publish(event.clone());
        }
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
        let state = self
            .replay_state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let snapshot_sequence = if state.next_sequence == 0 {
            u64::MAX
        } else {
            state.next_sequence - 1
        };
        let snapshot_cursor = ChangeCursor::new(state.epoch, snapshot_sequence);
        if let ReplayStart::Cursor(cursor) = start {
            validate_cursor(&state, cursor)?;
        }
        let events = state
            .recent_changes
            .iter()
            .filter(|event| event.database_id() == database_id)
            .filter(|event| event.tenant_id == tenant_id)
            .filter(|event| collection.is_none_or(|name| event.collection == name))
            .filter(|event| match start {
                ReplayStart::Cursor(cursor) => event.cursor().is_after_in_same_epoch(cursor),
                ReplayStart::Timestamp(timestamp) => event.timestamp_ms >= timestamp,
            })
            .take(limit)
            .cloned()
            .collect();
        Ok(ReplaySnapshot {
            events,
            snapshot_cursor,
        })
    }

    pub fn unsubscribe(&self) {
        self.active_subscriptions.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn deliver_remote_notify(
        &self,
        msg: &crate::event::cross_shard::types::NotifyBroadcastMsg,
    ) {
        let operation = match msg.operation.as_str() {
            "INSERT" => ChangeOperation::Insert,
            "UPDATE" => ChangeOperation::Update,
            "DELETE" => ChangeOperation::Delete,
            _ => ChangeOperation::Insert,
        };
        self.publish_in_database(
            DatabaseId::new(msg.database_id),
            ChangeEvent {
                lsn: Lsn::new(msg.lsn),
                tenant_id: TenantId::new(msg.tenant_id),
                collection: msg.collection.clone(),
                // The wire carries the identity as text; wrap it verbatim.
                document_id: RowIdentity::from_user_key(msg.document_id.clone()),
                operation,
                timestamp_ms: msg.timestamp_ms,
                after: None,
            },
        );
    }
}

fn validate_cursor(state: &ReplayState, cursor: ChangeCursor) -> Result<(), ReplayError> {
    let current = if state.next_sequence == 0 {
        u64::MAX
    } else {
        state.next_sequence - 1
    };
    if cursor.epoch() != state.epoch || cursor.sequence() > current {
        return Err(ReplayError::Expired);
    }
    if let Some(oldest) = state.recent_changes.front()
        && cursor.sequence().saturating_add(1) < oldest.cursor().sequence()
    {
        return Err(ReplayError::Expired);
    }
    if state.recent_changes.is_empty() && cursor.sequence() != current {
        return Err(ReplayError::Expired);
    }
    Ok(())
}

fn new_epoch() -> u128 {
    uuid::Uuid::new_v4().as_u128()
}

/// Broadcast a `ChangeEvent` to all peer nodes in the cluster.
pub fn broadcast_notify_to_cluster(
    database_id: DatabaseId,
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
        tenant_id: event.tenant_id.as_u64(),
        database_id: database_id.as_u64(),
        collection: event.collection.clone(),
        document_id: event.document_id.to_string(),
        operation: event.operation.as_str().to_string(),
        timestamp_ms: event.timestamp_ms,
        lsn: event.lsn.as_u64(),
    };
    let payload = match zerompk::to_msgpack_vec(&msg) {
        Ok(payload) => payload,
        Err(error) => {
            warn!(error = %error, "failed to serialize NotifyBroadcast");
            return;
        }
    };
    let peer_ids: Vec<u64> = {
        let topology = topology
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        topology
            .active_nodes()
            .iter()
            .map(|node| node.node_id)
            .filter(|id| *id != node_id)
            .collect()
    };
    trace!(peer_count = peer_ids.len(), database_id = database_id.as_u64(), collection = %event.collection, "broadcasting NOTIFY to cluster peers");
    for peer_id in peer_ids {
        let envelope = VShardEnvelope::new(
            VShardMessageType::NotifyBroadcast,
            node_id,
            peer_id,
            0,
            payload.clone(),
        );
        let transport = Arc::clone(transport);
        tokio::spawn(async move {
            if let Err(error) = transport
                .send_rpc_oneway(peer_id, RaftRpc::VShardEnvelope(envelope.to_bytes()))
                .await
            {
                trace!(peer = peer_id, error = %error, "NOTIFY broadcast to peer failed (best-effort)");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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
    #[test]
    fn publication_sequence_not_lsn() {
        let stream = ChangeStream::new(8);
        stream.publish(event(102, 1, "first"));
        stream.publish(event(101, 1, "second"));
        let first = stream
            .query_changes(TenantId::new(1), None, ReplayStart::Timestamp(0), 1)
            .unwrap_or_else(|_| panic!());
        let next = stream
            .query_changes(
                TenantId::new(1),
                None,
                ReplayStart::Cursor(first.events[0].cursor()),
                8,
            )
            .unwrap_or_else(|_| panic!());
        assert_eq!(next.events[0].document_id.as_str(), "second");
    }
    #[test]
    fn duplicate_lsn_events_paginate() {
        let stream = ChangeStream::new(8);
        stream.publish(event(1, 1, "a"));
        stream.publish(event(1, 1, "b"));
        let first = stream
            .query_changes(TenantId::new(1), None, ReplayStart::Timestamp(0), 1)
            .unwrap_or_else(|_| panic!());
        let next = stream
            .query_changes(
                TenantId::new(1),
                None,
                ReplayStart::Cursor(first.events[0].cursor()),
                1,
            )
            .unwrap_or_else(|_| panic!());
        assert_eq!(next.events[0].document_id.as_str(), "b");
    }
    #[test]
    fn evicted_and_wrong_epoch_cursors_expire() {
        let stream = ChangeStream::new(1);
        stream.publish(event(1, 1, "a"));
        let cursor = stream
            .query_changes(TenantId::new(1), None, ReplayStart::Timestamp(0), 1)
            .unwrap_or_else(|_| panic!())
            .events[0]
            .cursor();
        stream.publish(event(2, 1, "b"));
        stream.publish(event(3, 1, "c"));
        assert!(matches!(
            stream.query_changes(TenantId::new(1), None, ReplayStart::Cursor(cursor), 1),
            Err(ReplayError::Expired)
        ));
        assert!(matches!(
            stream.query_changes(
                TenantId::new(1),
                None,
                ReplayStart::Cursor(ChangeCursor::new(0, 0)),
                1
            ),
            Err(ReplayError::Expired)
        ));
    }
    #[test]
    fn tenant_filter_precedes_limit() {
        let stream = ChangeStream::new(8);
        stream.publish(event(1, 2, "other"));
        stream.publish(event(2, 1, "mine"));
        let result = stream
            .query_changes(TenantId::new(1), None, ReplayStart::Timestamp(0), 1)
            .unwrap_or_else(|_| panic!());
        assert_eq!(result.events[0].document_id.as_str(), "mine");
    }

    #[tokio::test]
    async fn database_filter_precedes_limit_for_query_and_subscription() {
        let stream = ChangeStream::new(8);
        let database_a = DatabaseId::new(1024);
        let database_b = DatabaseId::new(1025);
        let mut subscription =
            stream.subscribe_in_database(Some("orders".into()), Some(TenantId::new(1)), database_a);
        stream.publish_in_database(database_b, event(1, 1, "database-b"));
        stream.publish_in_database(database_a, event(2, 1, "database-a"));
        let result = stream
            .query_changes_in_database(
                TenantId::new(1),
                database_a,
                Some("orders"),
                ReplayStart::Timestamp(0),
                1,
            )
            .unwrap_or_else(|_| panic!());
        assert_eq!(result.events[0].document_id.as_str(), "database-a");
        let received = subscription.recv_sequenced().await.unwrap();
        assert_eq!(received.document_id.as_str(), "database-a");
    }

    #[test]
    fn rotation_changes_epoch_without_cross_epoch_cursor_ordering() {
        let stream = ChangeStream::new(8);
        let old_epoch = 7;
        {
            let mut state = stream
                .replay_state
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            state.epoch = old_epoch;
            state.next_sequence = u64::MAX;
        }
        stream.publish(event(1, 1, "last-old-epoch"));
        let old_cursor = stream
            .query_changes(TenantId::new(1), None, ReplayStart::Timestamp(0), 1)
            .unwrap_or_else(|_| panic!())
            .events[0]
            .cursor();
        stream.publish(event(2, 1, "first-new-epoch"));
        let new_cursor = stream
            .query_changes(TenantId::new(1), None, ReplayStart::Timestamp(0), 1)
            .unwrap_or_else(|_| panic!())
            .events[0]
            .cursor();
        assert_ne!(old_cursor.epoch(), new_cursor.epoch());
        assert!(!new_cursor.is_after_in_same_epoch(old_cursor));
    }
}
