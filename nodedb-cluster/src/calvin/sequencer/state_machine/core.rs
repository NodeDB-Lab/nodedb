// SPDX-License-Identifier: BUSL-1.1

//! The sequencer state machine's state and bookkeeping accessors.
//!
//! The apply path itself lives in [`super::apply`]; this file owns the struct,
//! its construction, and the read/arm/clear helpers the scheduler-side catch-up
//! drain and the sequencer-group log compactor call into.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;

use crate::calvin::CalvinCompletionRegistry;
use crate::calvin::sequencer::epoch_guard::UnrecoverableEpochHook;
use crate::calvin::types::SchedulerInput;

use super::counters::StateMachineMetrics;

/// One cluster restore point, as this replica applies the sequencer's cut
/// marker for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SequencerRestorePoint {
    pub id: u64,
    /// The point's watermark HLC.
    pub hlc: u64,
    /// The marker's sequencer log index.
    pub index: u64,
    /// The first epoch the sequencer proposes after the marker.
    pub next_epoch: u64,
    /// The highest epoch instant (ms) applied before the marker, or `None`
    /// when no epoch applied.
    pub epoch_system_ms: Option<i64>,
}

/// Records a restore point's sequencer place. Runs on the Raft tick thread,
/// so it must not block.
pub type RestorePointHook = Arc<dyn Fn(SequencerRestorePoint) + Send + Sync>;

/// Records the epoch instant at a cut marker: the marker's watermark HLC, its
/// sequencer log index, and the highest epoch instant (ms) applied before
/// it, `None` when no epoch applied. Every epoch before the marker has an
/// instant at or below it, and every epoch after it has a higher one. Runs
/// on the Raft tick thread, so it must not block.
pub type CutInstantHook = Arc<dyn Fn(u64, u64, Option<i64>) + Send + Sync>;

/// The Calvin sequencer Raft state machine.
///
/// One instance per replica (including leader). Applied on every `CommitApplier`
/// callback for the sequencer Raft group.
pub struct SequencerStateMachine {
    /// Last successfully applied epoch. Used for gap detection.
    /// The first valid epoch is 0; `last_applied_epoch = u64::MAX` means nothing
    /// has been applied yet (using `u64::MAX` avoids a separate `Option` and
    /// makes the "nothing applied" state explicit).
    pub(super) last_applied_epoch: u64,
    /// The highest `epoch_system_ms` of an epoch batch applied here, or
    /// `None` before the first one. Log replay rebuilds it, so a leader that
    /// seeds from it after a restart or a leader change never mints an epoch
    /// instant at or below a committed one.
    pub(super) last_epoch_system_ms: Option<i64>,
    /// Raft log index of the last committed entry applied on this replica.
    /// `NOT_YET_APPLIED` means nothing has been applied yet. Advanced for EVERY
    /// applied entry (not just `EpochBatch`), so it is a safe upper bound for the
    /// scheduler's catch-up `read_committed_entries(lo, hi)` range.
    pub(super) last_committed_index: u64,
    /// Per-vshard output channels. The scheduler subscribes on the other end.
    pub(super) vshard_senders: HashMap<u32, mpsc::Sender<SchedulerInput>>,
    /// Per-vShard armed catch-up: the Raft indexes whose inputs the vShard's
    /// scheduler still owes a replay of.
    ///
    /// - A fan-out `try_send` that fails (Full or Closed) arms the vShard at
    ///   the entry's index.
    /// - While a vShard is armed, every later input for it is deferred to the
    ///   replay, never sent live. The scheduler therefore receives its inputs
    ///   in exact log order: the channel holds only inputs from before the
    ///   arm, and the replay delivers the rest.
    /// - The scheduler-side drain replays the range and clears it.
    ///
    /// Bounded by the number of hosted vShards.
    pub(super) catch_up_from: Mutex<HashMap<u32, CatchUpRange>>,
    /// Set once an already-consumed epoch was proposed again. While set, no
    /// further `EpochBatch` is applied: this replica's epoch sequence and the
    /// proposing leader's have diverged, and fanning out under a colliding
    /// identity would corrupt lock-table and completion state rather than
    /// merely lose the offending batch.
    pub(super) halted: bool,
    /// Host escalation for the halt above. `None` in tests and in embedded
    /// callers with no fail-stop path; production wires it to node shutdown.
    pub(super) unrecoverable_hook: Option<UnrecoverableEpochHook>,
    /// Host hook that records the sequencer's place at a cluster restore
    /// point. `None` in tests and embedded callers.
    pub(super) restore_point_hook: Option<RestorePointHook>,
    /// Host hook that records the epoch instant at every cut marker. `None`
    /// in tests and embedded callers.
    pub(super) cut_instant_hook: Option<CutInstantHook>,
    pub metrics: Arc<StateMachineMetrics>,
    pub(super) completion_registry: Arc<CalvinCompletionRegistry>,
    /// Every multi-part transaction whose header applied here and whose
    /// parts have not all applied, nor been abandoned (see [`super::parts`]).
    pub(super) open_parts: super::parts::OpenParts,
    /// `Txn` inputs this node's schedulers received and may not have made
    /// durable (see [`super::undurable`]). Local to this replica.
    pub(super) undurable: super::undurable::UndurableInputs,
    /// Where the history this state holds begins (see [`super::history`]).
    pub(super) history: super::history::HistoryOrigin,
}

pub(super) const NOT_YET_APPLIED: u64 = u64::MAX;

/// The Raft indexes a vShard's armed catch-up covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct CatchUpRange {
    /// The lowest index whose input the scheduler has not received.
    pub from: u64,
    /// The highest index whose input was dropped or deferred to the replay.
    pub through: u64,
}

/// What became of one input for a vShard's scheduler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Delivery {
    /// This node hosts no scheduler for the vShard.
    NotHosted,
    /// Sent on the scheduler's channel.
    Sent,
    /// The vShard's catch-up is armed: the replay delivers it, in log order.
    Deferred,
    /// The channel was full: the catch-up is armed from this index.
    DroppedFull,
    /// The scheduler's channel is closed: the catch-up is armed from this
    /// index.
    DroppedClosed,
}

impl SequencerStateMachine {
    /// Construct a fresh state machine with no applied epochs.
    pub fn new(
        vshard_senders: HashMap<u32, mpsc::Sender<SchedulerInput>>,
        completion_registry: Arc<CalvinCompletionRegistry>,
    ) -> Self {
        Self {
            last_applied_epoch: NOT_YET_APPLIED,
            last_epoch_system_ms: None,
            last_committed_index: NOT_YET_APPLIED,
            vshard_senders,
            catch_up_from: Mutex::new(HashMap::new()),
            halted: false,
            unrecoverable_hook: None,
            restore_point_hook: None,
            cut_instant_hook: None,
            metrics: StateMachineMetrics::new(),
            completion_registry,
            open_parts: super::parts::OpenParts::default(),
            undurable: super::undurable::UndurableInputs::default(),
            history: super::history::HistoryOrigin::LogStart,
        }
    }

    /// Install the host's fail-stop escalation for an unrecoverable epoch
    /// regression.
    ///
    /// Without it the halt is still enforced locally and reported, but the node
    /// keeps serving with a sequencer that no longer accepts epochs — so a host
    /// that has a shutdown path SHOULD install one, and turn the halt into a
    /// visible stop rather than a silent stall.
    #[must_use]
    pub fn with_unrecoverable_hook(mut self, hook: UnrecoverableEpochHook) -> Self {
        self.unrecoverable_hook = Some(hook);
        self
    }

    /// Install the host's hook that records the sequencer's place at a
    /// cluster restore point.
    #[must_use]
    pub fn with_restore_point_hook(mut self, hook: RestorePointHook) -> Self {
        self.restore_point_hook = Some(hook);
        self
    }

    /// Install the host's hook that records the epoch instant at every cut
    /// marker.
    #[must_use]
    pub fn with_cut_instant_hook(mut self, hook: CutInstantHook) -> Self {
        self.cut_instant_hook = Some(hook);
        self
    }

    /// Whether this state machine has halted on an unrecoverable epoch
    /// regression and is refusing to apply further epoch batches.
    pub fn is_halted(&self) -> bool {
        self.halted
    }

    /// The last epoch number that was successfully applied, or `None` if no
    /// epoch has been applied yet.
    pub fn last_applied_epoch(&self) -> Option<u64> {
        if self.last_applied_epoch == NOT_YET_APPLIED {
            None
        } else {
            Some(self.last_applied_epoch)
        }
    }

    /// The highest epoch instant (`epoch_system_ms`) of an epoch batch
    /// applied here, or `None` before the first one.
    ///
    /// Like [`Self::next_epoch`], it answers for the whole log only once
    /// every committed entry has been applied here.
    pub fn last_epoch_system_ms(&self) -> Option<i64> {
        self.last_epoch_system_ms
    }

    /// The epoch number that the next proposal should use.
    ///
    /// INVARIANT: the first epoch a restarted leader proposes must be strictly
    /// greater than any epoch already committed to the sequencer log. This
    /// counter satisfies that ONLY once every committed entry has been applied
    /// here — it is in-memory and rebuilt purely by replaying the group's log,
    /// so reading it before that replay finishes answers 0 no matter how much
    /// history the log holds. Callers seeding a proposer must gate on the
    /// group's applied watermark reaching its log tip first (see
    /// `SequencerService::ensure_epoch_seeded`); an epoch minted early collides
    /// with committed history and every replica refuses the batch.
    pub fn next_epoch(&self) -> u64 {
        if self.last_applied_epoch == NOT_YET_APPLIED {
            0
        } else {
            self.last_applied_epoch + 1
        }
    }

    /// Register (or replace) the output sender for a vshard.
    ///
    /// Call this when a scheduler subscribes for a vshard hosted on this node.
    pub fn set_vshard_sender(&mut self, vshard: u32, sender: mpsc::Sender<SchedulerInput>) {
        self.vshard_senders.insert(vshard, sender);
    }

    /// Remove the output sender for a vshard (e.g. when a vshard is migrated
    /// away from this node).
    ///
    /// Its catch-up goes with it. No scheduler here will replay it, so an arm
    /// left behind would hold sequencer compaction down forever.
    pub fn remove_vshard_sender(&mut self, vshard: u32) {
        self.vshard_senders.remove(&vshard);
        self.drop_catch_up(vshard);
    }

    /// Forget `vshard`'s armed catch-up.
    fn drop_catch_up(&self, vshard: u32) {
        self.catch_up_from
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&vshard);
    }

    /// The highest epoch number that has been committed and applied on this
    /// replica, or `None` if no epoch has been applied yet.
    ///
    /// Used by the Calvin scheduler's rebuild path: the scheduler captures
    /// this value before processing the Raft log to determine the upper bound
    /// of the rebuild range (`E+1 ..= current_committed_epoch`).
    pub fn current_committed_epoch(&self) -> Option<u64> {
        self.last_applied_epoch()
    }

    /// The Raft log index of the highest committed entry applied on this replica,
    /// or `None` if nothing has been applied yet.
    ///
    /// Advanced for EVERY applied entry (not just `EpochBatch`), so the scheduler
    /// can use it as a safe upper bound (`hi`) for the catch-up replay range
    /// `read_committed_entries(SEQUENCER_GROUP, lo ..= hi)`.
    pub fn current_committed_index(&self) -> Option<u64> {
        if self.last_committed_index == NOT_YET_APPLIED {
            None
        } else {
            Some(self.last_committed_index)
        }
    }

    /// Record that the input for `vshard` at Raft index `index` goes to the
    /// replay.
    ///
    /// The range widens to cover `index`: the smallest index stays the
    /// replay's start, and the largest one tells the drain whether its clear
    /// leaves inputs owed. O(1), no I/O.
    pub(super) fn record_catch_up(&self, vshard: u32, index: u64) {
        let mut map = self.catch_up_from.lock().unwrap_or_else(|p| p.into_inner());
        widen(&mut map, vshard, index);
    }

    /// Deliver `input`, the input for `vshard` at Raft index `index`, to the
    /// vShard's scheduler.
    ///
    /// An armed vShard takes no live input: the input is deferred to the
    /// replay, so the scheduler never receives a later input before an
    /// earlier one. Otherwise a `try_send` sends it, and a full or closed
    /// channel arms the catch-up from `index`. Never blocks.
    pub(super) fn deliver(&self, index: u64, vshard: u32, input: SchedulerInput) -> Delivery {
        let Some(sender) = self.vshard_senders.get(&vshard) else {
            return Delivery::NotHosted;
        };
        let mut map = self.catch_up_from.lock().unwrap_or_else(|p| p.into_inner());
        if map.contains_key(&vshard) {
            widen(&mut map, vshard, index);
            return Delivery::Deferred;
        }
        match sender.try_send(input) {
            Ok(()) => Delivery::Sent,
            Err(mpsc::error::TrySendError::Full(_)) => {
                widen(&mut map, vshard, index);
                Delivery::DroppedFull
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                widen(&mut map, vshard, index);
                Delivery::DroppedClosed
            }
        }
    }

    /// Take (remove and return) the catch-up-from Raft index for `vshard`.
    ///
    /// Contract: TAKE semantics — the entry is cleared, so the scheduler-side
    /// drain consumes each recorded miss exactly once. Returns `None` when no
    /// drop is pending for the vShard. The next drop re-records a fresh index.
    pub fn take_catch_up_from(&self, vshard: u32) -> Option<u64> {
        let mut map = self.catch_up_from.lock().unwrap_or_else(|p| p.into_inner());
        map.remove(&vshard).map(|range| range.from)
    }

    /// Arm a catch-up for `vshard` from `index` (min-collapse), so the
    /// scheduler-side drain replays committed sequencer entries from there.
    ///
    /// Called when a scheduler subscribes for a vShard: the sequencer may have
    /// already committed (and fanned out to a then-absent sender — silently
    /// skipped) epochs for this vShard before the scheduler existed. A fresh
    /// node has nothing durably applied to rebuild from, so it would otherwise
    /// consider itself caught up and never replay those txns. Arming from the
    /// first available committed index makes the drain replay every committed
    /// entry for this vShard applied before subscription (idempotent: the
    /// scheduler's in-flight guard and Reserve/Release no-ops absorb re-apply).
    pub fn arm_catch_up_from(&self, vshard: u32, index: u64) {
        self.record_catch_up(vshard, index);
    }

    /// Read (WITHOUT removing) the catch-up-from Raft index for `vshard`.
    ///
    /// The scheduler drain peeks rather than takes so a replay that cannot
    /// complete this tick (committed index not yet known, transient log-read
    /// fault) leaves the entry armed for the next tick instead of silently
    /// dropping it — the loss the old take-then-early-return had.
    pub fn peek_catch_up_from(&self, vshard: u32) -> Option<u64> {
        let map = self.catch_up_from.lock().unwrap_or_else(|p| p.into_inner());
        map.get(&vshard).map(|range| range.from)
    }

    /// Mark `vshard`'s catch-up replayed through `up_to`.
    ///
    /// Called after a replay of `from ..= up_to`. An input deferred or
    /// dropped past `up_to` while the replay ran keeps the vShard armed from
    /// `up_to + 1`, so the next drain delivers it and no live input overtakes
    /// it. With none, the vShard is disarmed and live delivery resumes. An
    /// armed range that starts past `up_to` is left as it is.
    pub fn clear_catch_up_up_to(&self, vshard: u32, up_to: u64) {
        let mut map = self.catch_up_from.lock().unwrap_or_else(|p| p.into_inner());
        let Some(range) = map.get_mut(&vshard) else {
            return;
        };
        if range.from > up_to {
            return;
        }
        if range.through > up_to {
            range.from = up_to.saturating_add(1);
        } else {
            map.remove(&vshard);
        }
    }

    /// Arm `vshard`'s catch-up past the last applied entry, so every later
    /// input for it goes to the replay. Returns the armed start: the first
    /// index the replay owes, or the start already armed when that is lower.
    ///
    /// The scheduler calls it when it stops reading its channel: the channel
    /// then holds a finite run of inputs, and the log holds the rest.
    pub fn arm_catch_up_past_applied(&self, vshard: u32) -> u64 {
        let next = self
            .current_committed_index()
            .map_or(0, |i| i.saturating_add(1));
        let mut map = self.catch_up_from.lock().unwrap_or_else(|p| p.into_inner());
        widen(&mut map, vshard, next);
        map.get(&vshard).map_or(next, |range| range.from)
    }

    /// The smallest armed catch-up index across the hosted vShards, or `None`
    /// when no catch-up is pending.
    ///
    /// The sequencer-group log compactor floors its compaction index at this
    /// value so a dropped/undelivered fan-out is always replayable from the
    /// retained log — the hold-down the scheduler-side drain's `LogCompacted`
    /// arm depends on. Only a vShard with a registered sender counts. A
    /// scheduler that is exiting can arm after its sender is gone, and that
    /// arm must never pin compaction on a vShard this node does not serve.
    pub fn min_catch_up_from(&self) -> Option<u64> {
        let map = self.catch_up_from.lock().unwrap_or_else(|p| p.into_inner());
        map.iter()
            .filter(|(vshard, _)| self.vshard_senders.contains_key(vshard))
            .map(|(_, range)| range.from)
            .min()
    }
}

/// Widen `vshard`'s armed range in `map` to cover `index`, arming it at
/// `index` when unarmed.
fn widen(map: &mut HashMap<u32, CatchUpRange>, vshard: u32, index: u64) {
    map.entry(vshard)
        .and_modify(|range| {
            range.from = range.from.min(index);
            range.through = range.through.max(index);
        })
        .or_insert(CatchUpRange {
            from: index,
            through: index,
        });
}
