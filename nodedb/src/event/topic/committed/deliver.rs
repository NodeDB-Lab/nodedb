// SPDX-License-Identifier: BUSL-1.1

//! Holding and delivering committed transactions' messages.
//!
//! A trigger body or procedure publishes nothing while its transaction runs.
//! Its messages ride the transaction's redo record
//! ([`crate::wal::RedoRecord::publishes`]), so they commit or roll back with
//! its writes:
//!
//! 1. Every replica of the record's vShard installs it and emits one publish
//!    event per message. WAL catch-up rebuilds the same events after a crash.
//! 2. The Event Plane holds each message in its durable ledger under the
//!    message's origin before its watermark passes the event
//!    ([`hold_committed_publish`]).
//! 3. The node that holds the leader lease of the partition's data group
//!    delivers the partition's held messages in position order, from the
//!    replicated cursor, and commits the cursor past them
//!    ([`deliver_held_publishes`]). A new lease holder resumes from the
//!    cursor.
//! 4. The topic appends one message per origin, so a message delivered again
//!    by a new lease holder reaches it once.
//! 5. A message whose topic this node does not know waits until this node's
//!    catalog reaches the metadata index its publisher saw the topic at. A
//!    topic still unknown then was dropped: the message is counted in
//!    `nodedb_committed_publishes_dropped_total` and not delivered.

use std::sync::Arc;
use std::time::Duration;

use nodedb_cluster::METADATA_GROUP_ID;

use tokio::sync::watch;
use tracing::{debug, error, warn};

use crate::control::state::SharedState;
use crate::event::topic::publish::{PublishError, publish_committed};
use crate::event::topic::types::PublishOrigin;
use crate::event::types::{EventSource, WriteEvent};
use crate::types::DatabaseId;
use crate::wal::RedoPublish;

use super::cursor::{commit_delivered, delivered_through};
use super::event::decode_publish;
use super::key::{delivery_lease, origin_of_event};
use super::ledger::PublishLedger;

/// Messages one delivery pass sends from one partition.
const DELIVERY_BATCH: usize = 128;
/// Pause between delivery passes that found nothing to send.
const DELIVERY_IDLE: Duration = Duration::from_millis(100);
/// Longest pause between attempts to hold a message the ledger refused.
const HOLD_BACKOFF_MAX: Duration = Duration::from_secs(1);

/// The ledger committed messages are held in. `None` until the Event Plane
/// opens its sink ledgers.
fn ledger(state: &SharedState) -> Option<&PublishLedger> {
    state.sink_ledgers.get().map(|ledgers| &ledgers.publishes)
}

/// Hold the message a publish event carries until its delivery.
///
/// Returns once the message is durable in the ledger, so the Event Plane's
/// watermark never passes a message this node does not hold. A ledger write
/// the disk refuses is retried until it lands.
pub async fn hold_committed_publish(event: &WriteEvent, state: &Arc<SharedState>) {
    // A restored record re-issues rows, never messages.
    if event.source == EventSource::Restore {
        return;
    }
    let Some(bytes) = event.new_value.as_deref() else {
        error!(
            lsn = event.lsn.as_u64(),
            stream = %event.collection,
            "a committed publish event carries no message"
        );
        return;
    };
    let committed = match decode_publish(bytes) {
        Ok(committed) => committed,
        Err(error) => {
            error!(
                lsn = event.lsn.as_u64(),
                stream = %event.collection,
                error = %error,
                "a committed publish event does not decode"
            );
            return;
        }
    };
    let Some(origin) = origin_of_event(committed.ordinal, &committed.publish) else {
        error!(
            lsn = event.lsn.as_u64(),
            topic = %committed.publish.topic,
            "a committed publish carries no replicated position; it cannot be delivered"
        );
        return;
    };
    let Some(ledger) = ledger(state) else {
        error!(
            topic = %committed.publish.topic,
            "the publish ledger is not open; a committed publish cannot be held"
        );
        return;
    };
    let mut backoff = Duration::from_millis(10);
    while let Err(error) = ledger.hold(&origin, &committed.publish) {
        error!(
            partition = origin.partition,
            position = %origin.position,
            topic = %committed.publish.topic,
            error = %error,
            "a committed publish could not be held; retrying"
        );
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(HOLD_BACKOFF_MAX);
    }
}

/// Deliver the held messages of every partition whose lease this node holds,
/// and release on every node the messages its partition's cursor passed.
///
/// Returns how many messages the pass delivered under a committed cursor.
pub async fn deliver_held_publishes(state: &Arc<SharedState>) -> usize {
    let Some(ledger) = ledger(state) else {
        return 0;
    };
    #[cfg(feature = "failpoints")]
    if !crate::control::fail_gate::publish_delivery(
        state.node_id,
        "before_delivery",
        state.shutdown.raw_receiver(),
    )
    .await
    {
        return 0;
    }
    let partitions = match ledger.partitions() {
        Ok(partitions) => partitions,
        Err(error) => {
            warn!(error = %error, "publish ledger unreadable; no delivery this pass");
            return 0;
        }
    };
    let mut delivered = 0;
    for partition in partitions {
        let through = delivered_through(state, partition);
        if let Err(error) = ledger.release_through(partition, through) {
            warn!(partition, error = %error, "delivered publishes not released");
        }
        let Some(lease) = delivery_lease(state, partition) else {
            continue;
        };
        let held = match ledger.held_after(partition, through, DELIVERY_BATCH) {
            Ok(held) => held,
            Err(error) => {
                warn!(partition, error = %error, "held publishes unreadable");
                continue;
            }
        };
        delivered += deliver_partition(state, partition, lease, held).await;
    }
    delivered
}

/// Send one partition's held messages in order, stopping at the first the
/// topic did not take or once this node loses the lease, and commit the
/// cursor past the ones sent. Returns how many were sent under a committed
/// cursor: the delivery loop runs again at once only after progress.
async fn deliver_partition(
    state: &Arc<SharedState>,
    partition: u32,
    lease: (u64, u64),
    held: Vec<(PublishOrigin, RedoPublish)>,
) -> usize {
    let mut last = None;
    let mut sent = 0;
    for (origin, publish) in held {
        if delivery_lease(state, partition) != Some(lease) {
            break;
        }
        // A trigger body's cross-node request goes to its receiver, and the
        // cursor passes it once the receiver answered.
        if publish.topic == super::outbox::OUTBOX_TOPIC {
            let Some(ledger) = ledger(state) else {
                break;
            };
            match super::outbox::deliver_outbox(state, &ledger.outbox_refusals, origin, &publish)
                .await
            {
                super::outbox::OutboxDelivery::Delivered => sent += 1,
                super::outbox::OutboxDelivery::DeadLettered => {}
                super::outbox::OutboxDelivery::Retry => break,
            }
            last = Some(origin.position);
            continue;
        }
        match publish_committed(
            state,
            DatabaseId::new(publish.database_id),
            publish.tenant_id,
            &publish.topic,
            &publish.payload,
            origin,
        )
        .await
        {
            Ok(()) => sent += 1,
            Err(PublishError::TopicNotFound(topic)) => {
                // The publisher found the topic at its metadata floor. Until
                // this node's catalog has applied that far, the topic can
                // still be on its way here.
                let applied = state.applied_index_watcher(METADATA_GROUP_ID).current();
                if applied < publish.metadata_floor {
                    debug!(
                        owner = %publish.owner,
                        topic = %topic,
                        metadata_floor = publish.metadata_floor,
                        metadata_applied = applied,
                        "a committed publish waits for this node's catalog to reach its topic"
                    );
                    break;
                }
                // The catalog passed the floor and holds no such topic: it was
                // dropped after the transaction committed.
                if let Some(metrics) = &state.system_metrics {
                    metrics
                        .committed_publishes_dropped
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                warn!(
                    owner = %publish.owner,
                    topic = %topic,
                    "a committed publish names a dropped topic; not delivered"
                );
            }
            Err(error) => {
                debug!(
                    owner = %publish.owner,
                    topic = %publish.topic,
                    error = %error,
                    "a committed publish was not delivered; retried next pass"
                );
                break;
            }
        }
        last = Some(origin.position);
    }
    let Some(through) = last else {
        return sent;
    };
    // A lease lost mid-batch leaves the cursor to the next holder, which
    // sends the rest. The topic takes each message once either way.
    if delivery_lease(state, partition) != Some(lease) {
        return sent;
    }
    #[cfg(feature = "failpoints")]
    if !crate::control::fail_gate::publish_delivery(
        state.node_id,
        "before_cursor_commit",
        state.shutdown.raw_receiver(),
    )
    .await
    {
        return sent;
    }
    // The commit awaits the metadata group's apply on this task.
    match commit_delivered(state, partition, through).await {
        Ok(()) => sent,
        Err(error) => {
            debug!(
                partition,
                error = %error,
                "publish delivery cursor not committed; the messages are sent again"
            );
            0
        }
    }
}

/// Run delivery passes until shutdown. One task per node.
pub fn spawn_publish_delivery(
    state: Arc<SharedState>,
    mut shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            if *shutdown.borrow() {
                return;
            }
            if deliver_held_publishes(&state).await > 0 {
                tokio::task::yield_now().await;
                continue;
            }
            tokio::select! {
                _ = tokio::time::sleep(DELIVERY_IDLE) => {}
                _ = shutdown.changed() => {}
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;

    use super::*;
    use crate::event::cdc::CdcOffset;
    use crate::event::cdc::stream_def::RetentionConfig;
    use crate::event::topic::TopicDef;
    use crate::event::topic::committed::event::replayed_publish_events;
    use crate::event::topic::committed::event::tests::{publish, scope};
    use crate::wal::PublishPosition;

    const DATABASE: DatabaseId = DatabaseId::new(1);
    const TENANT: u64 = 1;
    /// The change-feed partition every test message is stamped in.
    const PARTITION: u32 = 4;

    /// A one-node cluster with its Event Plane ledgers open, once it holds
    /// the lease `PARTITION`'s messages are delivered under.
    async fn node(
        dir: &tempfile::TempDir,
    ) -> crate::control::cluster::test_one_node::OneNodeCluster {
        let cluster = crate::control::cluster::test_one_node::boot().await;
        let watermarks =
            crate::event::watermark::WatermarkStore::open(dir.path()).expect("open watermarks");
        crate::event::sink_ledger::load_sink_state(
            &cluster.state,
            &cluster.state.wal,
            &watermarks,
            1,
        )
        .expect("open the sink ledgers");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        while delivery_lease(&cluster.state, PARTITION).is_none() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the leader never took the partition group's lease"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        cluster
    }

    /// `message` stamped at `index` of `PARTITION`, as its record's apply
    /// stamps it.
    fn stamped(mut message: RedoPublish, index: u64) -> RedoPublish {
        message.position = Some(PublishPosition {
            partition: PARTITION,
            epoch: 0,
            index,
        });
        message
    }

    /// Create `name` in this node's catalog and registry, as the metadata
    /// apply of its creation does.
    fn create_topic(state: &SharedState, name: &str) {
        let definition = TopicDef {
            database_id: DATABASE,
            tenant_id: TENANT,
            name: name.into(),
            retention: RetentionConfig::default(),
            owner: "test".into(),
            created_at: 0,
            last_sequence: 0,
            last_lsn: 0,
            last_epoch: 0,
            modification_hlc: nodedb_types::Hlc::ZERO,
        };
        state
            .credentials
            .catalog()
            .put_ep_topic(&definition)
            .expect("persist topic");
        state.ep_topic_registry.register(definition);
    }

    /// The publish event that carries `message` as its record's first.
    fn event_of(message: &RedoPublish) -> WriteEvent {
        let mut sequence = 0;
        replayed_publish_events(std::slice::from_ref(message), &scope(), &mut sequence).remove(0)
    }

    /// The messages this node holds for delivery.
    fn held(state: &SharedState) -> usize {
        let ledger = ledger(state).expect("the publish ledger is open");
        ledger
            .partitions()
            .expect("partitions")
            .into_iter()
            .map(|partition| {
                ledger
                    .held_after(partition, CdcOffset::ZERO, 100)
                    .expect("held")
                    .len()
            })
            .sum()
    }

    fn payloads(state: &SharedState, topic: &str) -> Vec<String> {
        state
            .credentials
            .catalog()
            .load_ep_topic_messages(DATABASE, TENANT, topic)
            .expect("read topic")
            .into_iter()
            .map(|message| message.payload)
            .collect()
    }

    fn dropped(state: &SharedState) -> u64 {
        state
            .system_metrics
            .as_ref()
            .expect("system metrics")
            .committed_publishes_dropped
            .load(Ordering::Relaxed)
    }

    /// A message is named by the position its record's apply stamped on it.
    /// A position ledger that no longer holds the record's entry changes
    /// nothing: the message is held and delivered, once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_message_whose_ledger_entry_is_gone_is_held_and_delivered() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cluster = node(&dir).await;
        let state = &cluster.state;
        create_topic(state, "feed");
        let message = stamped(publish("feed", "a"), 77);
        let event = event_of(&message);
        // The node positions by replicated position, and its ledger holds no
        // entry for the record: the entry was evicted.
        state.cdc_router.positions().mark_replicated();
        assert!(
            state
                .cdc_router
                .positions()
                .get(event.lsn.as_u64())
                .is_none()
        );

        hold_committed_publish(&event, state).await;
        assert_eq!(held(state), 1, "the message is held without a ledger entry");
        deliver_held_publishes(state).await;
        assert_eq!(payloads(state, "feed"), vec!["a".to_string()]);

        // WAL catch-up after a crash holds and delivers the same event again.
        hold_committed_publish(&event, state).await;
        deliver_held_publishes(state).await;
        assert_eq!(
            payloads(state, "feed"),
            vec!["a".to_string()],
            "the topic appends the message once"
        );
        cluster.shutdown().await;
    }

    /// A message no apply stamped has no origin, so it is never held.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_unstamped_message_is_not_held() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cluster = node(&dir).await;
        let state = &cluster.state;
        create_topic(state, "feed");
        hold_committed_publish(&event_of(&publish("feed", "a")), state).await;
        assert_eq!(held(state), 0);
        cluster.shutdown().await;
    }

    /// The topic was created on another node, and this node's metadata apply
    /// lags behind the publisher's. The message waits for the catalog to
    /// reach the publisher's floor, then is delivered.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_topic_this_node_does_not_know_yet_is_waited_for() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cluster = node(&dir).await;
        let state = &cluster.state;
        let floor = state.applied_index_watcher(METADATA_GROUP_ID).current() + 1_000;
        let mut message = stamped(publish("late_feed", "a"), 78);
        message.metadata_floor = floor;
        hold_committed_publish(&event_of(&message), state).await;

        for _ in 0..3 {
            assert_eq!(deliver_held_publishes(state).await, 0);
        }
        assert_eq!(held(state), 1, "the message waits, held");
        assert_eq!(dropped(state), 0, "a lagging catalog drops nothing");

        // The metadata apply catches up: the topic's creation, then the
        // publisher's floor.
        create_topic(state, "late_feed");
        state.applied_index_watcher(METADATA_GROUP_ID).bump(floor);
        deliver_held_publishes(state).await;
        assert_eq!(payloads(state, "late_feed"), vec!["a".to_string()]);
        assert_eq!(dropped(state), 0);
        cluster.shutdown().await;
    }

    /// A topic still unknown once the catalog reached the publisher's floor
    /// was dropped: the message is counted and not delivered.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_topic_unknown_at_the_floor_counts_as_dropped() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cluster = node(&dir).await;
        let state = &cluster.state;
        let mut message = stamped(publish("gone_feed", "a"), 79);
        message.metadata_floor = state.applied_index_watcher(METADATA_GROUP_ID).current();
        hold_committed_publish(&event_of(&message), state).await;

        deliver_held_publishes(state).await;
        assert_eq!(dropped(state), 1);
        assert!(payloads(state, "gone_feed").is_empty());
        cluster.shutdown().await;
    }
}
