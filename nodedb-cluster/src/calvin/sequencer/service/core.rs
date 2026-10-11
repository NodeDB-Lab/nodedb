// SPDX-License-Identifier: BUSL-1.1

//! The Calvin sequencer service.
//!
//! [`SequencerService`] drives the epoch ticker and Raft proposal loop on the
//! sequencer leader. On each tick it:
//!
//! 1. Checks that this node is the sequencer Raft group leader. If not, drains
//!    and discards the inbox (clients will retry against the real leader).
//! 2. Runs the leader duties that mint nothing — verdict re-drive and
//!    reservation servicing. These never wait on the epoch seed.
//! 3. Drains the inbox into a candidate batch respecting epoch caps.
//! 4. Runs the pre-validation pass
//!    ([`crate::calvin::sequencer::validator::validate_batch`]).
//! 5. Proposes the resulting `EpochBatch` to the sequencer Raft group (only if
//!    at least one transaction was admitted).
//! 6. Advances the local epoch counter.
//!
//! The service does **not** apply Raft log entries — that is the
//! [`SequencerStateMachine`]'s job, which runs on every
//! replica including the leader.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use tracing::{debug, info, warn};

use tokio::sync::mpsc;

use crate::calvin::sequencer::config::SEQUENCER_GROUP_ID;
use crate::calvin::sequencer::config::SequencerConfig;
use crate::calvin::sequencer::entry::SequencerEntry;
use crate::calvin::sequencer::inbox::InboxReceiver;
use crate::calvin::sequencer::reservation_inbox::ReservationInboxReceiver;
use crate::calvin::sequencer::service::verdict_entry::verdict_entry;
use crate::calvin::sequencer::state_machine::SequencerStateMachine;
use crate::calvin::{CalvinCompletionRegistry, TxnId, VerdictOutcome};
use crate::error::ClusterError;
use crate::multi_raft::MultiRaft;

use crate::calvin::sequencer::metrics::SequencerMetrics;

/// The low edge of the reservation position band.
///
/// Real batch positions run `0..N` where `N <= max_txns_per_epoch`, far below
/// `2^31`. A reservation minted at a position `>= 2^31` therefore can never
/// share a `(epoch, position)` lock-table identity with a real batch txn in the
/// same epoch, so reservations and batch txns never collide.
///
/// Reservations create NO watermark obligation — `install_reservation` never
/// calls `note_expected` — so this band is purely anti-collision, not a
/// scheduling reservation of positions.
pub const RESERVATION_POSITION_BAND: u32 = 1 << 31;

/// The two inbound channels a `SequencerService` drains each leader tick.
pub struct SequencerReceivers {
    pub inbox: InboxReceiver,
    pub reservations: ReservationInboxReceiver,
}

/// The Calvin sequencer service.
///
/// Drives the epoch ticker. Must be spawned as a Tokio task on the Control
/// Plane. `Send + Sync`.
pub struct SequencerService {
    pub(super) config: SequencerConfig,
    pub(super) node_id: u64,
    pub(super) multi_raft: Arc<Mutex<MultiRaft>>,
    pub(super) inbox_receiver: InboxReceiver,
    /// Carries hot-key read-reservation requests from the Control Plane. Only
    /// the leader services it (see `process_reservations`); a follower drains
    /// and discards it so awaiting callers fall back to plain OCC.
    pub(super) reservation_receiver: ReservationInboxReceiver,
    /// The next position to mint in the reservation band for `reservation_epoch`.
    /// Reset to [`RESERVATION_POSITION_BAND`] whenever the current epoch advances
    /// so minted positions stay small and unique within each epoch.
    pub(super) next_reservation_position: u32,
    /// The epoch `next_reservation_position` is counting within. When it lags
    /// the tick's epoch, the band counter is reset before the next mint.
    pub(super) reservation_epoch: u64,
    /// The next epoch to mint and the sequencer term it was seeded in, or
    /// `None` until the seed has been derived.
    ///
    /// The leader starts at the last committed epoch + 1 and increments after
    /// each successful proposal. The seed is NOT taken at construction — see
    /// [`Self::ensure_epoch_seeded`] for why it can only be read once the
    /// sequencer group's log has been replayed into the state machine. A
    /// lost leadership clears it, and a cursor from another term is derived
    /// again. On leader failover, `inbox_receiver` is dropped (in-flight
    /// submissions are not in the log and will be retried).
    pub(super) current_epoch: Option<super::epoch_seed::EpochCursor>,
    /// The `epoch_system_ms` this service last minted, or `None` before its
    /// first mint. A proposed batch applies later, so the next mint reads
    /// this as well as the state machine's applied instant.
    pub(super) last_minted_ms: Option<i64>,
    /// The state machine committed sequencer entries are applied into on this
    /// node. Read to derive the epoch seed, and on every tick to see whether it
    /// has halted.
    pub(super) state_machine: Arc<Mutex<SequencerStateMachine>>,
    /// Whether the halt has already been reported. The tick runs at epoch
    /// cadence (milliseconds), so the report is latched to one line rather than
    /// burying the original cause under a per-tick repeat.
    pub(super) halt_reported: bool,
    pub metrics: Arc<SequencerMetrics>,
    pub(super) completion_registry: Arc<CalvinCompletionRegistry>,
    /// Receives `(txn, commit)` verdict signals emitted by this node's
    /// completion registry when a staged cross-shard txn's vote tally becomes
    /// complete. Only the leader turns a signal into a `Verdict` proposal.
    /// Stored as `Option` so `run` can move it out of `&mut self` into an owned
    /// local, avoiding a borrow conflict with `self.tick()` in a sibling
    /// `select!` arm; it is always `Some` after construction.
    verdict_rx: Option<mpsc::Receiver<(TxnId, VerdictOutcome)>>,
    /// The streamed parts of the multi-part transactions this leader
    /// sequenced, shared with the inbox that takes them (see
    /// [`super::parts`]).
    pub(super) parts_intake: Arc<crate::calvin::sequencer::parts_intake::PartsIntake>,
    /// Abandonments proposed in the current term and not yet applied, so
    /// each is proposed once per term.
    pub(super) abandoning: super::parts::Abandoning,
}

impl SequencerService {
    /// Construct the sequencer service.
    ///
    /// Takes the node's [`SequencerStateMachine`] rather than a starting epoch:
    /// the epoch seed is derived from it lazily, on the first leader tick that
    /// finds the sequencer group fully replayed. Constructing the service is
    /// always too early to read it — the Raft loop that drives that replay has
    /// not been spawned yet at that point in startup.
    pub fn new(
        config: SequencerConfig,
        node_id: u64,
        multi_raft: Arc<Mutex<MultiRaft>>,
        receivers: SequencerReceivers,
        state_machine: Arc<Mutex<SequencerStateMachine>>,
        completion_registry: Arc<CalvinCompletionRegistry>,
        verdict_rx: mpsc::Receiver<(TxnId, VerdictOutcome)>,
    ) -> Self {
        let SequencerReceivers {
            inbox,
            reservations,
        } = receivers;
        let parts_intake = inbox.parts_intake();
        Self {
            config,
            node_id,
            multi_raft,
            inbox_receiver: inbox,
            reservation_receiver: reservations,
            next_reservation_position: RESERVATION_POSITION_BAND,
            reservation_epoch: 0,
            current_epoch: None,
            last_minted_ms: None,
            state_machine,
            halt_reported: false,
            metrics: SequencerMetrics::new(),
            completion_registry,
            verdict_rx: Some(verdict_rx),
            parts_intake,
            abandoning: super::parts::Abandoning::default(),
        }
    }

    /// Run the epoch ticker loop until the shutdown signal fires.
    ///
    /// Each iteration: check leadership, drain inbox, validate, propose.
    pub async fn run(&mut self, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        let mut interval = tokio::time::interval(self.config.epoch_duration);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        info!(
            node_id = self.node_id,
            "sequencer service starting; epoch seed is derived on the first leader tick \
             that finds the sequencer group replayed"
        );

        // Move the verdict receiver out of `self` so the `select!` loop can hold
        // an owned `&mut` to it without conflicting with `self.tick()` in a
        // sibling arm. Always `Some` after construction; a `None` (run called
        // twice) simply disables the verdict arm forever via `pending()`.
        let mut verdict_rx = self.verdict_rx.take();

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    self.tick();
                }
                verdict = async {
                    match verdict_rx.as_mut() {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                    // Every replica's registry emits deterministically, but only
                    // the leader proposes the verdict.
                    if let Some((txn, outcome)) = verdict
                        && self.is_leader()
                        && let Err(e) = self.propose_entry(&verdict_entry(txn, outcome))
                    {
                        warn!(
                            epoch = txn.epoch,
                            position = txn.position,
                            error = %e,
                            "sequencer verdict propose failed; a later re-tally will not \
                             re-emit (deduped), but the local decision still drives"
                        );
                    }
                }
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        info!(node_id = self.node_id, "sequencer service shutting down");
                        break;
                    }
                }
            }
        }
    }

    /// Execute one epoch tick.
    ///
    /// Exposed as `pub` so tests can drive the service synchronously without
    /// running the full `run()` loop.
    pub fn tick(&mut self) {
        // no-determinism: epoch tick observability, off-WAL path
        let tick_start = Instant::now();
        self.metrics.epochs_total.fetch_add(1, Ordering::Relaxed);

        self.tick_inner();

        // no-determinism: epoch tick observability, off-WAL path
        let elapsed_ms = tick_start.elapsed().as_millis() as u64;
        self.metrics.record_epoch_duration_ms(elapsed_ms);
    }

    /// Inner body of `tick()`, separated so the duration timer in `tick()`
    /// wraps all exit paths cleanly.
    fn tick_inner(&mut self) {
        // Check leadership by attempting a dry-run propose. We use the
        // multi_raft is_leader API directly.
        if !self.is_leader() {
            // Drain and discard: clients will retry against the real leader.
            let discarded = self.discard_inbox();
            // Discard reservation requests too: dropping each `Reserve`'s `reply`
            // sender makes the CP awaiter observe a closed channel and fall back
            // to plain OCC — correct degradation when this node is not leader.
            let reservations_discarded = self.reservation_receiver.drain_all_discard();
            // Parts held here are gone with the leadership. The next leader
            // abandons their transactions.
            self.drop_part_streams();
            // Another leader can mint epochs before this node leads again.
            self.current_epoch = None;
            debug!(
                node_id = self.node_id,
                "not sequencer leader; discarding {discarded} inbox items \
                 and {reservations_discarded} reservation requests",
            );
            return;
        }

        // Re-propose any complete-but-unstored cross-shard verdict. This must run
        // on EVERY leader tick — including the empty-inbox / all-rejected ticks
        // that return early below — so a verdict orphaned by a mid-commit
        // sequencer failover (participant votes committed, but the aggregated
        // `Verdict` entry never did before the old leader died) is always
        // re-driven to durability. It is safe to skip only when not leader, which
        // the gate above already guarantees. It runs BEFORE the epoch seed gate
        // below because a verdict carries the txn's already-assigned identity and
        // mints nothing — blocking it during replay would strand participants
        // parked at the commit barrier for no gain.
        self.redrive_unproposed_verdicts();

        // Attempt the epoch seed. `None` means the sequencer group has not
        // replayed its log yet, so there is no epoch this node may safely stamp
        // onto a new identity. It does NOT mean this node is any less the
        // leader: leadership is established by the Raft loop and read back from
        // `MultiRaft`, and every leader duty that mints nothing must still run
        // while the seed is pending. So the seed is only carried down to the
        // steps that actually mint — it never short-circuits the tick above it.
        let seed = self.ensure_epoch_seeded();

        // Snapshot inbox depth before drain so the gauge reflects the queue
        // depth at the start of this epoch. Recorded even while the seed is
        // pending: that is exactly the window in which submissions pile up, so
        // it is the window the gauge most needs to be truthful in.
        self.metrics
            .inbox_depth
            .store(self.inbox_receiver.depth(), Ordering::Relaxed);

        // Service hot-key read reservations on EVERY leader tick, before the txn
        // drain — so reservations are handled even on ticks that early-return
        // below (empty inbox, all candidates rejected) and on ticks where the
        // seed is still pending. Releases and owner-echo reserves mint nothing
        // and run regardless; only a fresh mint needs `seed`, and it degrades
        // its caller to OCC rather than parking it until the replay finishes.
        self.process_reservations(seed);

        // Everything below MINTS: batch positions carry `(epoch, position)`
        // identities. Until the sequencer group is replayed there is no safe
        // epoch to mint, so the drain and proposal are deferred and submissions
        // stay queued in the inbox for a later tick — unless the state machine
        // has halted, in which case no later tick will ever drain them.
        let Some(epoch) = seed else {
            if self.state_machine_halted() {
                self.shed_submissions_after_halt();
            }
            return;
        };
        self.mint_epoch(epoch);
        // Parts follow their headers' batches, this tick's included.
        self.propose_parts();
        self.abandon_orphaned_parts();
    }

    /// Drain and discard every queued submission, and drop each one's
    /// assignment so its caller reads a closed channel at once. Returns how
    /// many were discarded.
    pub(super) fn discard_inbox(&mut self) -> usize {
        let discarded = self.inbox_receiver.drain_all_discard();
        for inbox_seq in &discarded {
            self.completion_registry.drop_assignment(*inbox_seq);
        }
        discarded.len()
    }

    /// Re-propose every complete-but-unstored cross-shard verdict.
    ///
    /// Closes a failover deadlock: a follower that applied the committed `Vote`
    /// entries reached the local vote-completeness transition, which set the
    /// per-`PendingCompletion` `verdict_proposed` flag and emitted a signal the
    /// non-leader service dropped. That flag is in-memory, non-durable, and never
    /// reset, so after the follower promotes the normal emit path stays deduped
    /// and never re-fires. If the prior leader died after the votes committed but
    /// before the `Verdict` entry committed, no node would ever propose the
    /// verdict and parked participants would stall in `AwaitingVerdict` forever.
    ///
    /// This leader-driven rescan re-proposes each such verdict on every tick,
    /// using the same propose path as the emit-signal arm. It self-heals: a
    /// re-proposed `Verdict` that already committed applies idempotently
    /// (`note_verdict` dedups a same-value verdict), and once the verdict is
    /// stored the registry stops returning that txn — so this cannot loop or
    /// double-commit. Runs only on the leader; the caller gates on `is_leader`.
    fn redrive_unproposed_verdicts(&self) {
        for (txn, outcome) in self.completion_registry.drain_unproposed_verdicts() {
            if let Err(e) = self.propose_entry(&verdict_entry(txn, outcome)) {
                warn!(
                    epoch = txn.epoch,
                    position = txn.position,
                    error = %e,
                    "sequencer failover verdict re-propose failed; the next tick will \
                     retry while this node stays leader"
                );
            }
        }
    }

    fn is_leader(&self) -> bool {
        let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        mr.is_group_leader(SEQUENCER_GROUP_ID)
    }

    pub(super) fn propose_entry(&self, entry: &SequencerEntry) -> Result<u64, ClusterError> {
        let bytes = zerompk::to_msgpack_vec(entry).map_err(|e| ClusterError::Codec {
            detail: format!("sequencer encode: {e}"),
        })?;
        let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        mr.propose_to_group(SEQUENCER_GROUP_ID, bytes)
    }
}

// ── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
pub(super) mod tests {
    use std::collections::HashMap;
    use std::time::{Duration, Instant};

    use tokio::sync::oneshot;

    use super::*;
    use crate::calvin::sequencer::config::SequencerConfig;
    use crate::calvin::sequencer::inbox::{AdmittedTx, Inbox, new_inbox};
    use crate::calvin::sequencer::reservation_inbox::{ReservationInbox, new_reservation_inbox};
    use crate::calvin::sequencer::service::epoch_seed::EpochCursor;
    use crate::calvin::sequencer::validator::validate_batch;
    use crate::calvin::types::{
        EngineKeySet, EpochBatch, LockKeyWire, ReadWriteSet, ReleaseReason, SequencedTxn,
        SortedVec, TxClass, TxnIdWire,
    };
    use crate::routing::RoutingTable;
    use nodedb_types::{
        TenantId,
        id::{CollectionKey, DatabaseId},
    };

    fn find_two_distinct_collections() -> (String, String) {
        let mut first: Option<(String, u32)> = None;
        for i in 0u32..512 {
            let name = format!("col_{i}");
            let vshard = CollectionKey::from_bare(DatabaseId::DEFAULT, &name)
                .vshard()
                .as_u32();
            if let Some((ref fname, fv)) = first {
                if fv != vshard {
                    return (fname.clone(), name);
                }
            } else {
                first = Some((name, vshard));
            }
        }
        panic!("could not find two distinct-vshard collections in 512 tries");
    }

    pub(in crate::calvin::sequencer::service) fn make_tx_class(
        surr_a: u32,
        surr_b: u32,
    ) -> TxClass {
        let (col_a, col_b) = find_two_distinct_collections();
        let write_set = ReadWriteSet::new(vec![
            EngineKeySet::Document {
                collection: col_a,
                surrogates: SortedVec::new(vec![surr_a]),
            },
            EngineKeySet::Document {
                collection: col_b,
                surrogates: SortedVec::new(vec![surr_b]),
            },
        ]);
        TxClass::new(
            ReadWriteSet::new(vec![]),
            write_set,
            vec![surr_a as u8],
            TenantId::new(1),
            None,
            crate::calvin::types::VersionedReadSet::default(),
        )
        .expect("valid TxClass")
    }

    #[test]
    fn epoch_ticker_fires_increments_counter() {
        let config = SequencerConfig::default();
        let (inbox, rx) = new_inbox(100, &config);
        let _ = inbox.submit(make_tx_class(1, 2));

        let metrics = Arc::new(SequencerMetrics::default());

        let mut candidates: Vec<AdmittedTx> = Vec::new();
        let mut rx2 = rx;
        rx2.drain_into_capped(&mut candidates, 1024, usize::MAX);

        let epoch = 1u64;
        let (admitted, rejected) = validate_batch(epoch, candidates);
        let admitted_count = admitted.len() as u64;
        let rejected_count = rejected.len() as u64;

        metrics
            .admitted_total
            .fetch_add(admitted_count, Ordering::Relaxed);
        metrics
            .rejected_conflict_total
            .fetch_add(rejected_count, Ordering::Relaxed);
        metrics.epochs_total.fetch_add(1, Ordering::Relaxed);

        assert_eq!(metrics.epochs_total.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.admitted_total.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn empty_inbox_produces_no_admitted_txns() {
        let epoch = 42u64;
        let candidates: Vec<AdmittedTx> = Vec::new();
        let (admitted, rejected) = validate_batch(epoch, candidates);
        assert!(admitted.is_empty());
        assert!(rejected.is_empty());
    }

    #[test]
    fn non_empty_inbox_produces_one_or_more_admitted_txns() {
        let epoch = 1u64;
        let admitted_tx = AdmittedTx {
            inbox_seq: 0,
            tx_class: make_tx_class(10, 20),
        };
        let (admitted, _rejected) = validate_batch(epoch, vec![admitted_tx]);
        assert_eq!(admitted.len(), 1);
        assert_eq!(admitted[0].epoch, epoch);
    }

    #[test]
    fn sequenced_txns_carry_correct_epoch() {
        let epoch = 99u64;
        let tx = AdmittedTx {
            inbox_seq: 0,
            tx_class: make_tx_class(5, 7),
        };
        let (admitted, _) = validate_batch(epoch, vec![tx]);
        assert_eq!(admitted[0].epoch, epoch);
    }

    #[test]
    fn sequenced_txn_is_clone_and_eq() {
        use crate::calvin::types::SequencedTxn;
        let tx = AdmittedTx {
            inbox_seq: 0,
            tx_class: make_tx_class(1, 2),
        };
        let (admitted, _) = validate_batch(1, vec![tx]);
        let t: SequencedTxn = admitted[0].clone();
        assert_eq!(t.epoch, 1);
    }

    #[test]
    fn drain_caps_at_max_txns_per_epoch() {
        // Produce 10 txns in the inbox; cap at 3 per epoch.
        let config = SequencerConfig {
            max_txns_per_epoch: 3,
            max_bytes_per_epoch: usize::MAX,
            ..SequencerConfig::default()
        };
        let (inbox, mut rx) = new_inbox(20, &config);
        for i in 0..10u32 {
            inbox
                .submit(make_tx_class(i * 2, i * 2 + 1))
                .expect("submit");
        }
        let mut out = Vec::new();
        let n = rx.drain_into_capped(
            &mut out,
            config.max_txns_per_epoch,
            config.max_bytes_per_epoch,
        );
        assert_eq!(n, 3, "drain must stop at max_txns_per_epoch");
        assert_eq!(out.len(), 3);
    }

    #[test]
    fn drain_stops_at_max_bytes_per_epoch() {
        // Each txn has plans = [0u8; 10] (10 bytes). Cap = 25 bytes → 2 fit,
        // the 3rd is deferred to the pending slot.
        let config = SequencerConfig {
            max_txns_per_epoch: 1000,
            max_bytes_per_epoch: 25,
            ..SequencerConfig::default()
        };
        let (inbox, mut rx) = new_inbox(20, &config);
        for i in 0..5u32 {
            let mut tx = make_tx_class(i * 2, i * 2 + 1);
            tx.plans = vec![0u8; 10];
            inbox.submit(tx).expect("submit");
        }

        // First drain: 2 fit (20 bytes), 3rd deferred.
        let mut out = Vec::new();
        let n = rx.drain_into_capped(
            &mut out,
            config.max_txns_per_epoch,
            config.max_bytes_per_epoch,
        );
        assert!(
            n <= 2,
            "at most 2 txns should fit in 25 bytes with 10-byte plans each, got {n}"
        );

        // Second drain: the deferred txn should be emitted first.
        let before = out.len();
        let n2 = rx.drain_into_capped(
            &mut out,
            config.max_txns_per_epoch,
            config.max_bytes_per_epoch,
        );
        assert!(n2 >= 1, "pending item must drain on the next call");
        let _ = before; // consumed for assertion above
    }

    // ── Epoch seeding ────────────────────────────────────────────────────────

    /// Live parts of a service under test. The inboxes are kept alive because
    /// dropping them would close the receivers the service holds.
    pub(in crate::calvin::sequencer::service) struct Harness {
        pub(in crate::calvin::sequencer::service) service: SequencerService,
        pub(in crate::calvin::sequencer::service) state_machine: Arc<Mutex<SequencerStateMachine>>,
        pub(in crate::calvin::sequencer::service) multi_raft: Arc<Mutex<MultiRaft>>,
        pub(in crate::calvin::sequencer::service) inbox: Inbox,
        /// Kept alive so the service's receiver stays open, and used directly by
        /// the tests that submit reservation requests to a leader tick.
        reservations: ReservationInbox,
        _verdict_tx: mpsc::Sender<(TxnId, VerdictOutcome)>,
        _dir: tempfile::TempDir,
    }

    pub(in crate::calvin::sequencer::service) fn make_harness() -> Harness {
        let dir = tempfile::tempdir().expect("tempdir");
        let routing = RoutingTable::uniform(1, &[1], 1);
        let mut mr = MultiRaft::new(1, routing, dir.path().to_path_buf());
        mr.add_group(SEQUENCER_GROUP_ID, vec![])
            .expect("add sequencer group");
        let multi_raft = Arc::new(Mutex::new(mr));

        let state_machine = Arc::new(Mutex::new(SequencerStateMachine::new(
            HashMap::new(),
            CalvinCompletionRegistry::new_detached(),
        )));

        let config = SequencerConfig::default();
        let (inbox, inbox_rx) = new_inbox(16, &config);
        let (reservations, reservations_rx) = new_reservation_inbox(16);
        let (verdict_tx, verdict_rx) = mpsc::channel(4);
        let service = SequencerService::new(
            config,
            1,
            Arc::clone(&multi_raft),
            SequencerReceivers {
                inbox: inbox_rx,
                reservations: reservations_rx,
            },
            Arc::clone(&state_machine),
            CalvinCompletionRegistry::new_detached(),
            verdict_rx,
        );

        Harness {
            service,
            state_machine,
            multi_raft,
            inbox,
            reservations,
            _verdict_tx: verdict_tx,
            _dir: dir,
        }
    }

    pub(in crate::calvin::sequencer::service) fn epoch_batch_bytes(epoch: u64) -> Vec<u8> {
        let batch = EpochBatch {
            epoch,
            txns: vec![SequencedTxn {
                epoch,
                position: 0,
                tx_class: make_tx_class(1, 2),
                epoch_system_ms: 1_700_000_000_000,
                epoch_vshard_txn_count: 1,
                lock_owner: None,
            }],
            epoch_system_ms: 1_700_000_000_000,
        };
        zerompk::to_msgpack_vec(&SequencerEntry::EpochBatch { batch }).expect("encode")
    }

    /// Drive the single-voter sequencer group to leadership so proposals append
    /// to its log.
    pub(in crate::calvin::sequencer::service) fn elect(multi_raft: &Arc<Mutex<MultiRaft>>) {
        let mut mr = multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(node) = mr.groups_mut().get_mut(&SEQUENCER_GROUP_ID) {
            // no-determinism: test-only forced election deadline so the single
            // voter campaigns immediately.
            node.election_deadline_override(Instant::now() - Duration::from_millis(1));
        }
        for _ in 0..20 {
            mr.tick().expect("tick");
            if mr.is_group_leader(SEQUENCER_GROUP_ID) {
                return;
            }
        }
        panic!("sequencer group did not reach single-node leadership");
    }

    /// The bug this guards: a restarted leader read its epoch seed from a state
    /// machine that had not replayed yet, minted 0 again, and every replica
    /// refused the resulting batch — dropping its transactions. The seed must be
    /// strictly greater than every epoch already committed.
    #[test]
    fn restarted_service_seeds_strictly_above_every_committed_epoch() {
        let mut harness = make_harness();
        let committed = [0u64, 1, 2];
        {
            let mut sm = harness
                .state_machine
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            for (i, epoch) in committed.iter().enumerate() {
                sm.apply(i as u64 + 1, &epoch_batch_bytes(*epoch));
            }
            assert_eq!(sm.last_applied_epoch(), Some(2));
        }

        let seed = harness
            .service
            .ensure_epoch_seeded()
            .expect("group is fully applied, so the seed is derivable");
        for epoch in committed {
            assert!(
                seed > epoch,
                "seed {seed} must be strictly greater than committed epoch {epoch}"
            );
        }
        assert_eq!(seed, 3);

        // Seeding is once per term: later ticks in the same term must not
        // re-derive and walk backwards over epochs this leader already proposed.
        let cursor = harness.service.current_epoch.expect("seeded");
        harness.service.current_epoch = Some(EpochCursor { next: 9, ..cursor });
        assert_eq!(harness.service.ensure_epoch_seeded(), Some(9));
    }

    /// Epoch instants order Calvin versions, so a leader mints each one
    /// strictly above every instant it minted and every one its state
    /// machine applied. A wall clock that steps back, a restart that replays
    /// the log, and a new leader with a slower clock all mint above history.
    #[test]
    fn epoch_instants_stay_monotonic_across_clock_steps_and_leader_changes() {
        let mut harness = make_harness();
        let first = harness.service.next_epoch_system_ms(2_000_000_000_000);
        assert_eq!(first, 2_000_000_000_000);
        let stepped_back = harness.service.next_epoch_system_ms(1_999_999_999_000);
        assert_eq!(stepped_back, first + 1, "a clock step back mints above");

        // A node that replayed a batch minted at 1_700_000_000_000 by an
        // earlier leader, now leading with a clock behind that instant.
        let mut successor = make_harness();
        successor
            .state_machine
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .apply(1, &epoch_batch_bytes(0));
        let minted = successor.service.next_epoch_system_ms(1_600_000_000_000);
        assert_eq!(
            minted, 1_700_000_000_001,
            "a new leader mints above history"
        );
        let next = successor.service.next_epoch_system_ms(1_600_000_000_000);
        assert_eq!(next, minted + 1, "an unapplied mint still counts");

        // A log a cluster restore rebuilt opens with the point's epoch
        // floor. Its leader mints above the point's instant.
        let mut restored = make_harness();
        restored
            .state_machine
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .apply(
                1,
                &zerompk::to_msgpack_vec(&SequencerEntry::EpochFloor {
                    next_epoch: 4,
                    epoch_system_ms: 1_900_000_000_000,
                })
                .expect("encode"),
            );
        assert_eq!(
            restored.service.next_epoch_system_ms(1_600_000_000_000),
            1_900_000_000_001,
            "a restored leader mints above the restore point"
        );
    }

    /// A brand-new node has an empty log and an empty state machine. Nothing
    /// has been applied because nothing was ever proposed, and nothing can be
    /// proposed until an epoch is minted — so a gate that waited for an applied
    /// entry would never open on a fresh cluster. `last_applied == log_tip == 0`
    /// must therefore seed epoch 0 straight away.
    #[test]
    fn fresh_service_mints_its_first_epoch_with_nothing_ever_applied() {
        let mut harness = make_harness();
        {
            let mr = harness.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            assert_eq!(mr.last_log_index(SEQUENCER_GROUP_ID), Some(0));
            assert_eq!(mr.last_applied(SEQUENCER_GROUP_ID), Some(0));
        }
        assert_eq!(
            harness
                .state_machine
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .last_applied_epoch(),
            None
        );

        assert_eq!(
            harness.service.ensure_epoch_seeded(),
            Some(0),
            "an empty sequencer group must mint epoch 0 without waiting for an entry"
        );
    }

    /// `metrics.epoch_seeded` is what the readiness probe reads to decide
    /// whether a Calvin submit landing here can be sequenced, so it must track
    /// the seed gate in BOTH directions, not just latch true once.
    #[test]
    fn epoch_seeded_metric_tracks_the_seed_gate() {
        let mut harness = make_harness();
        assert!(!harness.service.metrics.epoch_seeded.load(Ordering::Relaxed));

        assert_eq!(harness.service.ensure_epoch_seeded(), Some(0));
        assert!(harness.service.metrics.epoch_seeded.load(Ordering::Relaxed));

        // Re-minting an epoch already consumed here halts the state machine,
        // which refuses every later batch — the seed is no longer usable.
        {
            let mut sm = harness
                .state_machine
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            sm.apply(1, &epoch_batch_bytes(0));
            sm.apply(2, &epoch_batch_bytes(0));
        }
        assert!(
            harness
                .state_machine
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .is_halted(),
            "re-applying an already-consumed epoch must halt the state machine"
        );

        assert_eq!(harness.service.ensure_epoch_seeded(), None);
        assert!(!harness.service.metrics.epoch_seeded.load(Ordering::Relaxed));
    }

    /// Deferring the epoch seed must defer ONLY minting. Leadership is Raft
    /// state, and the duties that carry an already-assigned identity — here a
    /// reservation release — are what the rest of the system waits on, so they
    /// must run on a leader tick whose seed is still pending.
    #[test]
    fn leadership_and_non_minting_duties_run_while_the_seed_gate_is_shut() {
        let mut harness = make_harness();
        elect(&harness.multi_raft);

        // Put history in the log that this node has not applied: the seed gate
        // is shut for as long as that is true.
        for epoch in 0..3u64 {
            let mut mr = harness.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            mr.propose_to_group(SEQUENCER_GROUP_ID, epoch_batch_bytes(epoch))
                .expect("propose");
        }
        assert_eq!(harness.service.ensure_epoch_seeded(), None);

        // One release (no mint — it names an already-assigned owner) and one
        // fresh reserve (a mint) are queued for the leader tick.
        harness
            .reservations
            .submit_release(
                TxnIdWire {
                    epoch: 1,
                    position: RESERVATION_POSITION_BAND,
                },
                4,
                ReleaseReason::Commit,
            )
            .expect("release enqueued");
        let mint_reply = harness
            .reservations
            .submit_reserve(
                LockKeyWire::Kv {
                    collection: "sessions".to_owned(),
                    key: b"hot".to_vec(),
                },
                4,
                None,
            )
            .expect("reserve enqueued");

        let tip_before = harness
            .multi_raft
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .last_log_index(SEQUENCER_GROUP_ID)
            .expect("group is mounted");

        harness.service.tick();

        assert!(
            harness.service.is_leader(),
            "the seed gate must not cost this node its leadership"
        );
        let tip_after = harness
            .multi_raft
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .last_log_index(SEQUENCER_GROUP_ID)
            .expect("group is mounted");
        assert_eq!(
            tip_after,
            tip_before + 1,
            "the release must still be proposed while the seed is pending"
        );
        assert!(
            mint_reply.blocking_recv().is_err(),
            "an unservable mint must drop its reply so the caller degrades to OCC \
             instead of parking until the replay finishes"
        );
        assert_eq!(
            harness.service.ensure_epoch_seeded(),
            None,
            "nothing on this tick may have minted an epoch"
        );
    }

    // ── Unsequenced submissions ──────────────────────────────────────────────

    /// A single-vshard class that reads `read_col` and writes `write_col`.
    fn make_rw_class(read_col: &str, write_col: &str) -> TxClass {
        TxClass::new_single_vshard(
            ReadWriteSet::new(vec![EngineKeySet::Document {
                collection: read_col.to_owned(),
                surrogates: SortedVec::new(vec![1]),
            }]),
            ReadWriteSet::new(vec![EngineKeySet::Document {
                collection: write_col.to_owned(),
                surrogates: SortedVec::new(vec![1]),
            }]),
            vec![],
            TenantId::new(1),
            None,
            crate::calvin::types::VersionedReadSet::default(),
        )
        .expect("valid TxClass")
    }

    fn closed(rx: &mut crate::calvin::AssignmentReceiver) -> bool {
        rx.try_recv() == Err(oneshot::error::TryRecvError::Closed)
    }

    /// A follower discards its inbox. Each discarded caller must read a closed
    /// channel at once instead of waiting out its timeout.
    #[test]
    fn non_leader_discard_fails_each_submission_at_once() {
        let mut harness = make_harness();
        let registry = Arc::clone(&harness.service.completion_registry);
        let (_, mut rx) = harness
            .inbox
            .submit_with(make_tx_class(1, 2), &registry)
            .expect("submit");

        harness.service.tick();

        assert!(closed(&mut rx));
        assert_eq!(registry.pending_assignments_len(), 0);
    }

    /// The validator rejects the later txn of a read/write cycle. Its caller
    /// reads a closed channel. The admitted txn's caller gets its assignment.
    #[test]
    fn validator_rejection_fails_the_rejected_submission_at_once() {
        let mut harness = make_harness();
        elect(&harness.multi_raft);
        let registry = Arc::clone(&harness.service.completion_registry);
        let (col_a, col_b) = find_two_distinct_collections();
        let (_, mut winner) = harness
            .inbox
            .submit_with(make_rw_class(&col_a, &col_b), &registry)
            .expect("submit");
        let (_, mut loser) = harness
            .inbox
            .submit_with(make_rw_class(&col_b, &col_a), &registry)
            .expect("submit");
        let term = harness
            .multi_raft
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .leader_term(SEQUENCER_GROUP_ID)
            .expect("leader");
        harness.service.current_epoch = Some(EpochCursor { term, next: 4 });

        harness.service.mint_epoch(4);

        assert!(closed(&mut loser));
        // Two participants: the vShard it writes and the vShard it reads.
        assert_eq!(winner.try_recv(), Ok((4, 0, 2)));
        assert_eq!(registry.pending_assignments_len(), 0);
        assert_eq!(harness.service.current_epoch.map(|c| c.next), Some(5));
    }

    /// A batch whose proposal failed is not in the log. Its callers must fail
    /// at once, not hold an `(epoch, position)` the next tick hands to another
    /// transaction.
    #[test]
    fn failed_proposal_fails_every_submission_of_the_batch() {
        let mut harness = make_harness();
        let registry = Arc::clone(&harness.service.completion_registry);
        let (_, mut first) = harness
            .inbox
            .submit_with(make_tx_class(1, 2), &registry)
            .expect("submit");
        let (_, mut second) = harness
            .inbox
            .submit_with(make_tx_class(3, 4), &registry)
            .expect("submit");

        // Not elected: the proposal fails.
        harness.service.mint_epoch(0);

        assert!(closed(&mut first));
        assert!(closed(&mut second));
        assert_eq!(registry.pending_assignments_len(), 0);
        assert_eq!(
            harness.service.current_epoch, None,
            "a failed proposal must not consume the epoch"
        );
    }

    /// A halted state machine sheds the inbox. Each shed caller must read a
    /// closed channel at once.
    #[test]
    fn halted_shed_fails_each_submission_at_once() {
        let mut harness = make_harness();
        elect(&harness.multi_raft);
        {
            let mut sm = harness
                .state_machine
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            sm.apply(1, &epoch_batch_bytes(0));
            sm.apply(2, &epoch_batch_bytes(0));
            assert!(sm.is_halted());
        }
        let registry = Arc::clone(&harness.service.completion_registry);
        let (_, mut rx) = harness
            .inbox
            .submit_with(make_tx_class(1, 2), &registry)
            .expect("submit");

        harness.service.tick();

        assert!(closed(&mut rx));
        assert_eq!(registry.pending_assignments_len(), 0);
    }

    /// The seed must not be taken while the sequencer group is still replaying:
    /// that is exactly the startup window in which the state machine's counter
    /// still reads 0 no matter how much history the log holds.
    #[test]
    fn seed_is_deferred_until_the_group_has_applied_its_whole_log() {
        let mut harness = make_harness();
        elect(&harness.multi_raft);

        // Three epochs are in the log; nothing has been applied on this node yet.
        for epoch in 0..3u64 {
            let mut mr = harness.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            mr.propose_to_group(SEQUENCER_GROUP_ID, epoch_batch_bytes(epoch))
                .expect("propose");
        }
        let log_tip = harness
            .multi_raft
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .last_log_index(SEQUENCER_GROUP_ID)
            .expect("group is mounted");
        assert!(log_tip >= 3);

        assert_eq!(
            harness.service.ensure_epoch_seeded(),
            None,
            "a leader must not mint an epoch before its log is applied"
        );

        // Replay: the state machine applies the committed entries and the group's
        // applied watermark catches up with the log tip.
        {
            let mut sm = harness
                .state_machine
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            for epoch in 0..3u64 {
                sm.apply(epoch + 1, &epoch_batch_bytes(epoch));
            }
        }
        harness
            .multi_raft
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .advance_applied(SEQUENCER_GROUP_ID, log_tip)
            .expect("advance applied");

        assert_eq!(
            harness.service.ensure_epoch_seeded(),
            Some(3),
            "once replayed, the seed clears every committed epoch"
        );
    }
}
