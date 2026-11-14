// SPDX-License-Identifier: BUSL-1.1

//! The leader's feed of one follower: a cursor per partition, fed from the
//! leader's own stream.
//!
//! Each follower node has one task. It runs a pass as it starts, then on
//! each wake and each backoff deadline. A wake coalesces: one that arrives
//! while a pass runs leaves one permit, so the next pass reads everything
//! published before it. A pass serves every partition this node leads to
//! the follower, one exchange per partition per round, until none is ready:
//!
//! - A partition with no cursor starts with a probe. The follower answers
//!   with its durable position, and the cursor takes it. A new leadership,
//!   seen as a new leadership term, and a task that just started therefore
//!   feed from what the follower holds.
//! - A partition with a cursor reads the next run past it from this node's
//!   stream (see [`super::forward_read`]), sends it, and sets the cursor to
//!   the position the follower reports.
//! - A refusal, a reply that does not decode, and a transport error put the
//!   partition in backoff until its own deadline, which doubles up to
//!   [`RESEND_BACKOFF_MAX`]. The other partitions go on. A rejection records
//!   a capture and stops the partition's feed.
//!
//! Between passes the task waits for a wake or the earliest deadline. The
//! cursor moves only to a position the follower reports, and the follower
//! reports only what its journal holds. No run is queued and no run is
//! dropped: a stretch the follower lacks is read again from the leader's
//! stream until the follower takes it.

use std::collections::{BTreeMap, HashSet};
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::time::Instant;
use tracing::warn;

use crate::control::shutdown::ShutdownReceiver;
use crate::event::cdc::CdcOffset;
use crate::event::cross_shard::types::{NotifyAnswer, NotifyBroadcastMsg, NotifyBroadcastReply};

use super::ChangePartition;
use super::ring::AppendedRun;

/// The wait before a stretch the follower did not take goes out again the
/// first time.
pub(super) const RESEND_BACKOFF_MIN: Duration = Duration::from_millis(50);

/// The longest wait between two sends of one stretch.
pub(super) const RESEND_BACKOFF_MAX: Duration = Duration::from_secs(5);

/// The wait after `current` before a stretch goes out again.
pub(super) fn next_backoff(current: Duration) -> Duration {
    current.saturating_mul(2).min(RESEND_BACKOFF_MAX)
}

/// Why one send of a run got no reply from the follower.
#[derive(Debug, thiserror::Error)]
pub(super) enum ExchangeError {
    #[error("encode the run: {detail}")]
    Encode { detail: String },
    #[error("the node's transport is closed")]
    TransportClosed,
    #[error("the reply is not a vShard envelope")]
    NotEnvelope,
    #[error("{detail}")]
    Transport { detail: String },
    #[error("the reply envelope did not decode")]
    EnvelopeDecode,
    #[error("the reply is a {msg_type}")]
    UnexpectedReply { msg_type: String },
    #[error("the reply did not decode: {detail}")]
    ReplyDecode { detail: String },
}

/// The leader's side of one follower: what the feed reads, and where it
/// sends.
pub(super) trait FeedPeer: Send + Sync {
    /// This node's id, the source of every run.
    fn node_id(&self) -> u64;

    /// The partitions this node feeds the follower now, each with the term
    /// of this node's leadership of it. `None` once the feed stops for
    /// good: this node shuts down.
    fn partitions(&self) -> Option<Vec<(ChangePartition, u64)>>;

    /// Whether the follower still takes `partition`'s feed from this node.
    fn feeds(&self, partition: ChangePartition) -> bool;

    /// The next run of `partition` past `after`, from this node's stream.
    fn read(
        &self,
        partition: ChangePartition,
        after: CdcOffset,
    ) -> impl Future<Output = crate::Result<Option<AppendedRun>>> + Send;

    /// Send `msg` and return the follower's reply, or why none came.
    fn exchange(
        &self,
        msg: &NotifyBroadcastMsg,
    ) -> impl Future<Output = Result<NotifyBroadcastReply, ExchangeError>> + Send;

    /// Record that the follower rejected `partition`'s feed.
    fn rejected(&self, partition: ChangePartition, detail: &str);
}

/// The wake of one follower's feed task. Wakes coalesce into one permit.
#[derive(Default)]
pub(super) struct PeerWake {
    notify: Notify,
    heartbeat: AtomicBool,
}

impl PeerWake {
    /// Wake the task. With `heartbeat`, its next pass also sends a stretch
    /// with no event, so an idle feed's position reaches the follower.
    pub fn wake(&self, heartbeat: bool) {
        if heartbeat {
            self.heartbeat.store(true, Ordering::Release);
        }
        self.notify.notify_one();
    }
}

/// One partition's feed of the follower.
struct PartitionFeed {
    /// The leadership term the cursor belongs to.
    term: u64,
    /// The follower's last reported position. `None` probes first.
    cursor: Option<CdcOffset>,
    /// Whether the partition waits for a pass.
    pending: bool,
    /// Whether the pass sends a stretch with no event too.
    heartbeat: bool,
    /// The wait the next failure sets.
    backoff: Duration,
    /// The partition waits in backoff until this deadline.
    ready_at: Option<Instant>,
}

impl PartitionFeed {
    fn new(term: u64) -> Self {
        Self {
            term,
            cursor: None,
            pending: true,
            heartbeat: false,
            backoff: RESEND_BACKOFF_MIN,
            ready_at: None,
        }
    }
}

/// What one exchange of a partition's feed did.
enum Step {
    /// The follower moved: the feed goes on.
    More,
    /// The follower holds this node's frontier.
    Caught,
    /// The follower did not take the stretch: it goes out again after a
    /// backoff.
    Retry(String),
    /// The follower no longer takes the feed from this node.
    Stopped,
    /// The follower rejected the feed.
    Rejected(String),
}

/// Every partition's feed of one follower.
#[derive(Default)]
pub(super) struct PeerFeeds {
    feeds: BTreeMap<ChangePartition, PartitionFeed>,
    rejected: HashSet<ChangePartition>,
}

impl PeerFeeds {
    /// Mark every partition in `led`, with its leadership term, as waiting
    /// for a pass, and forget every other. A partition whose term changed
    /// probes again: another leader can have fed the follower meanwhile.
    pub fn refresh(&mut self, led: Vec<(ChangePartition, u64)>, heartbeat: bool) {
        self.feeds
            .retain(|partition, _| led.iter().any(|(led, _)| led == partition));
        for (partition, term) in led {
            if self.rejected.contains(&partition) {
                continue;
            }
            let feed = self
                .feeds
                .entry(partition)
                .or_insert_with(|| PartitionFeed::new(term));
            if feed.term != term {
                *feed = PartitionFeed::new(term);
            }
            feed.pending = true;
            feed.heartbeat |= heartbeat;
        }
    }

    /// The follower's last reported position in `partition`.
    #[cfg(test)]
    pub fn cursor(&self, partition: ChangePartition) -> Option<CdcOffset> {
        self.feeds.get(&partition).and_then(|feed| feed.cursor)
    }

    /// Serve the ready partitions round-robin, one exchange each per round,
    /// until none is ready. Returns the earliest backoff deadline of a
    /// partition that still waits, if any.
    pub async fn serve<P: FeedPeer>(&mut self, peer: &P) -> Option<Instant> {
        loop {
            let now = Instant::now();
            let ready: Vec<ChangePartition> = self
                .feeds
                .iter()
                .filter(|(_, feed)| feed.pending && feed.ready_at.is_none_or(|at| at <= now))
                .map(|(partition, _)| *partition)
                .collect();
            if ready.is_empty() {
                return self
                    .feeds
                    .values()
                    .filter(|feed| feed.pending)
                    .filter_map(|feed| feed.ready_at)
                    .min();
            }
            for partition in ready {
                let Some(feed) = self.feeds.get_mut(&partition) else {
                    continue;
                };
                match step(peer, partition, feed).await {
                    Step::More => {
                        feed.backoff = RESEND_BACKOFF_MIN;
                        feed.ready_at = None;
                    }
                    Step::Caught => {
                        feed.pending = false;
                        feed.heartbeat = false;
                        feed.backoff = RESEND_BACKOFF_MIN;
                        feed.ready_at = None;
                    }
                    Step::Retry(detail) => {
                        let ChangePartition(group) = partition;
                        warn!(
                            group,
                            %detail,
                            retry_in_ms = u64::try_from(feed.backoff.as_millis()).unwrap_or(u64::MAX),
                            "a follower did not take a change run; the stretch goes out again"
                        );
                        feed.ready_at = Some(Instant::now() + feed.backoff);
                        feed.backoff = next_backoff(feed.backoff);
                    }
                    Step::Stopped => {
                        self.feeds.remove(&partition);
                    }
                    Step::Rejected(detail) => {
                        let ChangePartition(group) = partition;
                        warn!(group, %detail, "a follower rejected the change feed; the partition's feed stops");
                        peer.rejected(partition, &detail);
                        self.feeds.remove(&partition);
                        self.rejected.insert(partition);
                    }
                }
            }
        }
    }
}

/// Feed `peer` until the node's `shutdown` signal, or until
/// [`FeedPeer::partitions`] ends the feed: a pass at once, then one on each
/// wake and each backoff deadline. The signal also cuts off a pass in
/// flight, with its read or exchange, and the task drops `peer` as it ends.
pub(super) async fn run_peer<P: FeedPeer>(
    peer: P,
    wake: Arc<PeerWake>,
    mut shutdown: ShutdownReceiver,
) {
    let mut feeds = PeerFeeds::default();
    loop {
        if shutdown.is_cancelled() {
            return;
        }
        let Some(led) = peer.partitions() else {
            return;
        };
        let heartbeat = wake.heartbeat.swap(false, Ordering::AcqRel);
        feeds.refresh(led, heartbeat);
        let deadline = tokio::select! {
            () = shutdown.wait_cancelled() => return,
            deadline = feeds.serve(&peer) => deadline,
        };
        match deadline {
            Some(deadline) => {
                tokio::select! {
                    () = shutdown.wait_cancelled() => return,
                    () = wake.notify.notified() => {}
                    () = tokio::time::sleep_until(deadline) => {}
                }
            }
            None => {
                tokio::select! {
                    () = shutdown.wait_cancelled() => return,
                    () = wake.notify.notified() => {}
                }
            }
        }
    }
}

/// One exchange of `partition`'s feed: a probe without a cursor, else the
/// next run past it.
async fn step<P: FeedPeer>(peer: &P, partition: ChangePartition, feed: &mut PartitionFeed) -> Step {
    let ChangePartition(group) = partition;
    if !peer.feeds(partition) {
        return Step::Stopped;
    }
    let msg = match feed.cursor {
        None => NotifyBroadcastMsg::probe(peer.node_id(), partition),
        Some(after) => match peer.read(partition, after).await {
            Ok(None) => return Step::Caught,
            Ok(Some(run)) => {
                if run.events.is_empty() && !feed.heartbeat {
                    return Step::Caught;
                }
                if run.after > after && after != CdcOffset::ZERO {
                    warn!(
                        group,
                        ?after,
                        hole_through = ?run.after,
                        "this node no longer holds the events above a follower's position; the \
                         follower records a hole"
                    );
                }
                NotifyBroadcastMsg::from_run(peer.node_id(), &run)
            }
            Err(error) => return Step::Retry(format!("the change run read failed: {error}")),
        },
    };
    match peer.exchange(&msg).await {
        Ok(NotifyBroadcastReply {
            answer: NotifyAnswer::Taken,
            through,
        }) => {
            let before = feed.cursor;
            let moved = match through {
                Some(position) => {
                    feed.cursor = Some(position);
                    Some(position) > before
                }
                // A follower that never journaled the partition holds
                // nothing of it.
                None if msg.probe => {
                    feed.cursor = Some(CdcOffset::ZERO);
                    true
                }
                None => false,
            };
            if msg.probe || moved {
                Step::More
            } else {
                Step::Retry("the follower took a run without moving its position".to_owned())
            }
        }
        Ok(NotifyBroadcastReply {
            answer: NotifyAnswer::Refused { detail },
            through,
        }) => {
            if through.is_some() {
                feed.cursor = through;
            }
            Step::Retry(detail)
        }
        Ok(NotifyBroadcastReply {
            answer: NotifyAnswer::Rejected { detail },
            ..
        }) => Step::Rejected(detail),
        Err(error) => Step::Retry(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU64, AtomicUsize};

    use nodedb_types::RowIdentity;

    use super::*;
    use crate::control::change_stream::journal::ChangeJournal;
    use crate::control::change_stream::{
        ChangeCursor, ChangeEvent, ChangeOperation, ChangeStream, CursorStep, ReplayStart,
    };
    use crate::control::cluster::metadata_applier::MetadataApplyWedge;
    use crate::control::shutdown::ShutdownWatch;
    use crate::types::{DatabaseId, Lsn, TenantId};

    const GROUP: ChangePartition = ChangePartition(3);

    /// A leader's stream feeding a follower's stream in this process,
    /// through the wire encoding.
    struct LocalPeer {
        leader: Arc<ChangeStream>,
        follower: Arc<ChangeStream>,
        wedge: MetadataApplyWedge,
        run_events: usize,
        stopped: Arc<AtomicBool>,
        term: AtomicU64,
        sent: AtomicUsize,
        refused: AtomicUsize,
        /// Partitions whose runs and probes the follower refuses unread.
        blocked: Mutex<HashSet<ChangePartition>>,
        /// Whether an exchange waits for a reply that never comes.
        hang: AtomicBool,
    }

    impl LocalPeer {
        fn new(
            leader: &Arc<ChangeStream>,
            follower: &Arc<ChangeStream>,
            run_events: usize,
        ) -> Self {
            Self {
                leader: Arc::clone(leader),
                follower: Arc::clone(follower),
                wedge: MetadataApplyWedge::default(),
                run_events,
                stopped: Arc::new(AtomicBool::new(false)),
                term: AtomicU64::new(1),
                sent: AtomicUsize::new(0),
                refused: AtomicUsize::new(0),
                blocked: Mutex::new(HashSet::new()),
                hang: AtomicBool::new(false),
            }
        }

        fn sent(&self) -> usize {
            self.sent.load(Ordering::Relaxed)
        }
    }

    impl FeedPeer for LocalPeer {
        fn node_id(&self) -> u64 {
            1
        }

        fn partitions(&self) -> Option<Vec<(ChangePartition, u64)>> {
            let term = self.term.load(Ordering::Acquire);
            (!self.stopped.load(Ordering::Acquire)).then(|| {
                self.leader
                    .partitions()
                    .into_iter()
                    .map(|partition| (partition, term))
                    .collect()
            })
        }

        fn feeds(&self, _partition: ChangePartition) -> bool {
            true
        }

        async fn read(
            &self,
            partition: ChangePartition,
            after: CdcOffset,
        ) -> crate::Result<Option<AppendedRun>> {
            self.leader.forward_run(partition, after, self.run_events)
        }

        async fn exchange(
            &self,
            msg: &NotifyBroadcastMsg,
        ) -> Result<NotifyBroadcastReply, ExchangeError> {
            self.sent.fetch_add(1, Ordering::Relaxed);
            if self.hang.load(Ordering::Acquire) {
                std::future::pending::<()>().await;
            }
            let blocked = msg.partition().is_some_and(|partition| {
                self.blocked
                    .lock()
                    .expect("blocked partitions")
                    .contains(&partition)
            });
            if blocked {
                self.refused.fetch_add(1, Ordering::Relaxed);
                return Ok(NotifyBroadcastReply {
                    answer: NotifyAnswer::Refused {
                        detail: "blocked".into(),
                    },
                    through: None,
                });
            }
            let bytes = zerompk::to_msgpack_vec(msg).map_err(|error| ExchangeError::Encode {
                detail: error.to_string(),
            })?;
            let msg: NotifyBroadcastMsg =
                zerompk::from_msgpack(&bytes).map_err(|error| ExchangeError::ReplyDecode {
                    detail: error.to_string(),
                })?;
            let reply = self.follower.deliver_remote_notify(&msg, &self.wedge);
            if matches!(reply.answer, NotifyAnswer::Refused { .. }) {
                self.refused.fetch_add(1, Ordering::Relaxed);
            }
            Ok(reply)
        }

        fn rejected(&self, _partition: ChangePartition, _detail: &str) {}
    }

    fn event(index: u64, row: u64) -> ChangeEvent {
        ChangeEvent {
            lsn: Lsn::new(index),
            tenant_id: TenantId::new(1 + row % 2),
            collection: "orders".into(),
            document_id: RowIdentity::from_user_key(format!("e{index}-{row}")),
            operation: ChangeOperation::Insert,
            timestamp_ms: 1,
            after: None,
        }
    }

    /// A stream of `capacity` ring slots with a journal under `dir`.
    fn journaled(
        dir: &std::path::Path,
        capacity: usize,
    ) -> (Arc<ChangeStream>, Arc<ChangeJournal>) {
        let stream = Arc::new(ChangeStream::new(capacity));
        let journal = Arc::new(ChangeJournal::open(dir, 1 << 16).expect("open journal"));
        stream.attach_journal(Arc::clone(&journal)).expect("attach");
        (stream, journal)
    }

    /// Settle entries `indexes` of `group` with `events` events each.
    fn settle(
        stream: &ChangeStream,
        group: u64,
        indexes: std::ops::RangeInclusive<u64>,
        events: u64,
    ) {
        for index in indexes {
            let batch = (0..events).map(|row| event(index, row)).collect();
            stream.settle_entry(group, index, DatabaseId::DEFAULT, batch);
        }
    }

    /// The positions of entries `indexes` with `events` events each.
    fn expected(indexes: std::ops::RangeInclusive<u64>, events: u64) -> Vec<CdcOffset> {
        indexes
            .flat_map(|index| {
                (1..=events).map(move |ordinal| CdcOffset::data_event(0, index, ordinal))
            })
            .collect()
    }

    /// Every position the journal holds of `partition`.
    fn journaled_positions(journal: &ChangeJournal, partition: ChangePartition) -> Vec<CdcOffset> {
        journal
            .read(
                partition,
                CdcOffset::ZERO,
                CdcOffset::whole_index(1 << 20),
                None,
                1 << 20,
            )
            .expect("read the journal")
            .changes
            .iter()
            .map(|change| change.position)
            .collect()
    }

    /// The floor of the follower's feed of [`GROUP`]: it rises only on a
    /// hole.
    fn floor(stream: &ChangeStream) -> Option<CdcOffset> {
        stream
            .lock()
            .ring
            .feed_continuity(GROUP)
            .map(|(floor, _)| floor)
    }

    fn durable(stream: &ChangeStream, partition: ChangePartition) -> Option<CdcOffset> {
        stream
            .durable_through(partition)
            .expect("read the position")
    }

    /// Serve every partition of `peer` until each is caught up, waiting out
    /// each backoff deadline.
    async fn feed_until_caught(peer: &LocalPeer, feeds: &mut PeerFeeds) {
        loop {
            feeds.refresh(peer.partitions().expect("the feed runs"), false);
            match feeds.serve(peer).await {
                None => return,
                Some(deadline) => tokio::time::sleep_until(deadline).await,
            }
        }
    }

    /// A follower whose journal refuses far more writes than any queue
    /// holds, and long enough to halt, still gets every event once, in
    /// order, with no hole. The leader reads the stretch from its journal,
    /// its pending events, and its ring.
    #[tokio::test(start_paused = true)]
    async fn a_slow_follower_gets_every_event_once_in_order() {
        let dirs = [
            tempfile::tempdir().expect("tempdir"),
            tempfile::tempdir().expect("tempdir"),
        ];
        let (leader, _) = journaled(dirs[0].path(), 32);
        let (follower, follower_journal) = journaled(dirs[1].path(), 64);
        settle(&leader, 3, 1..=100, 15);
        leader
            .journal_group(3, 100)
            .expect("journal the first half");
        settle(&leader, 3, 101..=200, 15);
        let peer = LocalPeer::new(&leader, &follower, 100);
        let refusals = 2 * super::super::journal_debt::HALT_FAILURES;

        follower_journal.fail_next_persists_for_test(refusals);
        let mut feeds = PeerFeeds::default();
        feed_until_caught(&peer, &mut feeds).await;

        assert_eq!(feeds.cursor(GROUP), Some(CdcOffset::whole_index(200)));
        assert_eq!(
            journaled_positions(&follower_journal, GROUP),
            expected(1..=200, 15)
        );
        assert_eq!(follower.events_published(), 200 * 15, "each event once");
        assert_eq!(floor(&follower), Some(CdcOffset::whole_index(0)), "no hole");
        assert_eq!(
            peer.refused.load(Ordering::Relaxed),
            usize::try_from(refusals).expect("the count fits"),
        );
        assert!(
            !peer.wedge.is_wedged(),
            "the write that succeeds clears the halt"
        );
    }

    /// A new leader probes the follower and feeds from the position the
    /// follower reports: no stretch twice, no hole. The old leader's stale
    /// cursor sends a stretch the follower holds, which publishes nothing.
    /// A leadership in a new term probes before it sends.
    #[tokio::test(start_paused = true)]
    async fn a_new_leader_feeds_from_the_follower_position() {
        let dirs = [
            tempfile::tempdir().expect("tempdir"),
            tempfile::tempdir().expect("tempdir"),
            tempfile::tempdir().expect("tempdir"),
        ];
        let (old_leader, _) = journaled(dirs[0].path(), 16);
        let (new_leader, _) = journaled(dirs[1].path(), 16);
        let (follower, follower_journal) = journaled(dirs[2].path(), 64);
        for leader in [&old_leader, &new_leader] {
            settle(leader, 3, 1..=10, 1);
        }
        let old_peer = LocalPeer::new(&old_leader, &follower, 4);
        let mut old_feeds = PeerFeeds::default();
        feed_until_caught(&old_peer, &mut old_feeds).await;
        assert_eq!(old_feeds.cursor(GROUP), Some(CdcOffset::whole_index(10)));

        for leader in [&old_leader, &new_leader] {
            settle(leader, 3, 11..=20, 1);
        }
        let new_peer = LocalPeer::new(&new_leader, &follower, 4);
        let mut new_feeds = PeerFeeds::default();
        feed_until_caught(&new_peer, &mut new_feeds).await;
        assert_eq!(new_feeds.cursor(GROUP), Some(CdcOffset::whole_index(20)));
        assert_eq!(
            new_peer.sent(),
            4,
            "one probe, then the ten new events in runs of four"
        );

        let before = old_peer.sent();
        feed_until_caught(&old_peer, &mut old_feeds).await;
        assert_eq!(old_feeds.cursor(GROUP), Some(CdcOffset::whole_index(20)));
        assert_eq!(
            old_peer.sent(),
            before + 1,
            "one stale run, which the follower holds"
        );

        old_peer.term.store(2, Ordering::Release);
        let before = old_peer.sent();
        feed_until_caught(&old_peer, &mut old_feeds).await;
        assert_eq!(
            old_peer.sent(),
            before + 1,
            "a new term probes, and the follower is caught up"
        );

        assert_eq!(follower.events_published(), 20, "each event once");
        assert_eq!(
            journaled_positions(&follower_journal, GROUP),
            expected(1..=20, 1)
        );
        assert_eq!(floor(&follower), Some(CdcOffset::whole_index(0)), "no hole");
    }

    /// A follower position below the leader's floor gets one explicit hole:
    /// the run starts at the floor, and a cursor below the floor resets. A
    /// cursor at the floor resumes past the hole with no reset, through the
    /// live feed and through a replay.
    #[tokio::test(start_paused = true)]
    async fn an_expired_position_gets_an_explicit_hole_then_the_feed_resumes() {
        let dirs = [
            tempfile::tempdir().expect("tempdir"),
            tempfile::tempdir().expect("tempdir"),
        ];
        let (leader, _) = journaled(dirs[0].path(), 16);
        let (follower, _) = journaled(dirs[1].path(), 16);
        let mut live = follower.subscribe(None, None);
        let mut consumer = live.start_cursor().clone();
        let peer = LocalPeer::new(&leader, &follower, 16);
        let mut feeds = PeerFeeds::default();

        settle(&leader, 3, 1..=3, 1);
        feed_until_caught(&peer, &mut feeds).await;
        for _ in 1..=3 {
            let delivered = live.try_recv_sequenced().expect("an event before the hole");
            assert_eq!(consumer.accept(&delivered), CursorStep::Deliver);
        }

        // A snapshot install covers entries 4..=6 on the leader.
        leader.install_group_floor(3, 6);
        settle(&leader, 3, 7..=8, 1);
        feed_until_caught(&peer, &mut feeds).await;
        assert_eq!(feeds.cursor(GROUP), Some(CdcOffset::whole_index(8)));
        assert_eq!(
            floor(&follower),
            Some(CdcOffset::whole_index(6)),
            "the hole"
        );

        // The consumer below the hole resets on every event past it, until
        // it reconnects.
        let past_hole = live
            .try_recv_sequenced()
            .expect("the first event past the hole");
        assert_eq!(consumer.accept(&past_hole), CursorStep::Reset);
        let next = live.try_recv_sequenced().expect("the feed resumes");
        assert_eq!(next.position(), CdcOffset::data_event(0, 8, 1));
        assert_eq!(consumer.accept(&next), CursorStep::Reset);

        // A cursor at the floor sees no reset.
        let mut resumed = ChangeCursor::default();
        resumed.raise(GROUP, CdcOffset::whole_index(6));
        assert_eq!(resumed.accept(&past_hole), CursorStep::Deliver);
        assert_eq!(resumed.accept(&next), CursorStep::Deliver);
        let mut replay_from = ChangeCursor::default();
        replay_from.raise(GROUP, CdcOffset::whole_index(6));
        let replayed = follower
            .query_changes(TenantId::new(1), None, ReplayStart::Cursor(replay_from), 8)
            .expect("a cursor at the hole replays");
        let positions: Vec<CdcOffset> = replayed
            .events
            .iter()
            .map(|change| change.position())
            .collect();
        assert_eq!(
            positions,
            vec![
                CdcOffset::data_event(0, 7, 1),
                CdcOffset::data_event(0, 8, 1)
            ]
        );
    }

    /// A partition in backoff does not hold back the follower's other
    /// partitions: they catch up in the same pass, and the refused one
    /// catches up once its deadline passes.
    #[tokio::test(start_paused = true)]
    async fn a_partition_in_backoff_does_not_stall_the_others() {
        let dirs = [
            tempfile::tempdir().expect("tempdir"),
            tempfile::tempdir().expect("tempdir"),
        ];
        let (leader, _) = journaled(dirs[0].path(), 64);
        let (follower, follower_journal) = journaled(dirs[1].path(), 64);
        let other = ChangePartition(4);
        settle(&leader, 3, 1..=5, 2);
        settle(&leader, 4, 1..=5, 2);
        let peer = LocalPeer::new(&leader, &follower, 3);
        peer.blocked
            .lock()
            .expect("blocked partitions")
            .insert(GROUP);

        let mut feeds = PeerFeeds::default();
        feeds.refresh(peer.partitions().expect("the feed runs"), false);
        let deadline = feeds
            .serve(&peer)
            .await
            .expect("the blocked partition waits");
        assert_eq!(durable(&follower, other), Some(CdcOffset::whole_index(5)));
        assert_eq!(
            journaled_positions(&follower_journal, other),
            expected(1..=5, 2)
        );
        assert_eq!(
            durable(&follower, GROUP),
            None,
            "the blocked partition waits"
        );
        assert_eq!(
            peer.refused.load(Ordering::Relaxed),
            1,
            "one send, then the backoff"
        );

        peer.blocked.lock().expect("blocked partitions").clear();
        tokio::time::sleep_until(deadline).await;
        feed_until_caught(&peer, &mut feeds).await;
        assert_eq!(durable(&follower, GROUP), Some(CdcOffset::whole_index(5)));
        assert_eq!(
            journaled_positions(&follower_journal, GROUP),
            expected(1..=5, 2)
        );
        assert_eq!(follower.events_published(), 20, "each event once");
    }

    /// A node that replicated the group holds settled events its journal
    /// lacks when it becomes a follower of the group. It reports its
    /// journal's position, so the leader sends those events again, and the
    /// journal takes them before the follower acks. A restart then finds
    /// every event.
    #[tokio::test(start_paused = true)]
    async fn a_former_replica_journals_its_unjournaled_events_from_the_feed() {
        let dirs = [
            tempfile::tempdir().expect("tempdir"),
            tempfile::tempdir().expect("tempdir"),
        ];
        let (leader, _) = journaled(dirs[0].path(), 64);
        settle(&leader, 3, 1..=5, 2);
        {
            let (former, former_journal) = journaled(dirs[1].path(), 64);
            // As a replica it settled entries 1..=3. Its floor save
            // journaled entry 1 only.
            settle(&former, 3, 1..=3, 2);
            former.journal_group(3, 1).expect("journal entry 1");
            assert_eq!(
                former.held_events(),
                4,
                "entries 2 and 3 wait for a floor save"
            );
            assert_eq!(durable(&former, GROUP), Some(CdcOffset::whole_index(1)));

            let peer = LocalPeer::new(&leader, &former, 16);
            let mut feeds = PeerFeeds::default();
            feed_until_caught(&peer, &mut feeds).await;
            assert_eq!(feeds.cursor(GROUP), Some(CdcOffset::whole_index(5)));
            assert_eq!(
                journaled_positions(&former_journal, GROUP),
                expected(1..=5, 2)
            );
            assert_eq!(former.events_published(), 10, "each event once");
            assert_eq!(
                former.held_events(),
                0,
                "the feed journaled the pending events"
            );
            assert_eq!(floor(&former), Some(CdcOffset::whole_index(0)), "no hole");
        }
        let (restarted, journal) = journaled(dirs[1].path(), 64);
        assert_eq!(durable(&restarted, GROUP), Some(CdcOffset::whole_index(5)));
        assert_eq!(journaled_positions(&journal, GROUP), expected(1..=5, 2));
    }

    /// Wakes that arrive before the task runs, or while it runs, coalesce
    /// into one permit, and the pass that follows reads everything
    /// published before them.
    #[tokio::test]
    async fn coalesced_wakes_lose_nothing() {
        let dirs = [
            tempfile::tempdir().expect("tempdir"),
            tempfile::tempdir().expect("tempdir"),
        ];
        let (leader, _) = journaled(dirs[0].path(), 16);
        let (follower, follower_journal) = journaled(dirs[1].path(), 16);
        let peer = LocalPeer::new(&leader, &follower, 8);
        let stopped = Arc::clone(&peer.stopped);
        let wake = Arc::new(PeerWake::default());
        for index in 1..=50 {
            settle(&leader, 3, index..=index, 1);
            wake.wake(false);
        }
        let shutdown = ShutdownWatch::new();
        let task = tokio::spawn(run_peer(peer, Arc::clone(&wake), shutdown.subscribe()));
        wait_for(&follower, 50).await;

        for index in 51..=60 {
            settle(&leader, 3, index..=index, 1);
            wake.wake(false);
            tokio::task::yield_now().await;
        }
        wait_for(&follower, 60).await;

        stopped.store(true, Ordering::Release);
        wake.wake(false);
        task.await.expect("the feed task ends");
        assert_eq!(follower.events_published(), 60, "each event once");
        assert_eq!(
            journaled_positions(&follower_journal, GROUP),
            expected(1..=60, 1)
        );
    }

    /// The node's shutdown signal ends a feed task waiting out a backoff,
    /// and one whose exchange never gets a reply. Each task drops its peer,
    /// with every handle the peer holds.
    #[tokio::test(start_paused = true)]
    async fn feed_tasks_end_and_drop_their_handles_at_shutdown() {
        let dirs = [
            tempfile::tempdir().expect("tempdir"),
            tempfile::tempdir().expect("tempdir"),
        ];
        let (leader, _) = journaled(dirs[0].path(), 16);
        let (follower, _) = journaled(dirs[1].path(), 16);
        settle(&leader, 3, 1..=2, 1);
        let waiting = LocalPeer::new(&leader, &follower, 8);
        waiting
            .blocked
            .lock()
            .expect("blocked partitions")
            .insert(GROUP);
        let hung = LocalPeer::new(&leader, &follower, 8);
        hung.hang.store(true, Ordering::Release);
        let shutdown = ShutdownWatch::new();
        let tasks = [
            tokio::spawn(run_peer(
                waiting,
                Arc::new(PeerWake::default()),
                shutdown.subscribe(),
            )),
            tokio::spawn(run_peer(
                hung,
                Arc::new(PeerWake::default()),
                shutdown.subscribe(),
            )),
        ];
        tokio::time::sleep(Duration::from_millis(1)).await;
        assert_eq!(
            Arc::strong_count(&leader),
            3,
            "each running task holds its peer"
        );

        shutdown.signal();
        for task in tasks {
            task.await.expect("the feed task ends");
        }
        assert_eq!(Arc::strong_count(&leader), 1, "every task dropped its peer");
        assert_eq!(Arc::strong_count(&follower), 1);
    }

    /// The wait between two sends of one stretch doubles from
    /// [`RESEND_BACKOFF_MIN`] and stays at [`RESEND_BACKOFF_MAX`].
    #[test]
    fn the_resend_backoff_doubles_up_to_its_bound() {
        assert_eq!(next_backoff(RESEND_BACKOFF_MIN), RESEND_BACKOFF_MIN * 2);
        let mut backoff = RESEND_BACKOFF_MIN;
        for _ in 0..64 {
            backoff = next_backoff(backoff);
            assert!(backoff <= RESEND_BACKOFF_MAX);
        }
        assert_eq!(backoff, RESEND_BACKOFF_MAX);
    }

    /// Wait until the follower's journal holds [`GROUP`] through entry
    /// `index`.
    async fn wait_for(follower: &ChangeStream, index: u64) {
        for _ in 0..2000 {
            if durable(follower, GROUP) == Some(CdcOffset::whole_index(index)) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        panic!("the follower never reached entry {index}");
    }
}
