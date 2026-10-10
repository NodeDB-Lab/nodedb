// SPDX-License-Identifier: BUSL-1.1

//! `ProposeTracker`: lets proposers wait for a Raft entry to commit and
//! execute on this node, with race-safe resolution if the apply path beats
//! the proposer's `register()` call.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::oneshot;

use nodedb_cluster::GroupAppliedWatchers;

use super::applied_write::ProposeResult;
use super::applying::ApplyingEntry;
use super::committed_keys::{CarriedKeys, CommittedKeys};
use super::cut::GroupCuts;
use super::slots::WaiterSlots;
use crate::control::distributed_applier::apply_window::ApplyWindow;

/// How long a propose waiter can exist: the default statement deadline.
/// `start_raft` sets the configured deadline through
/// [`ProposeTracker::with_waiter_window`].
pub const DEFAULT_WAITER_WINDOW: Duration = Duration::from_secs(30);

/// Tracks pending proposals awaiting Raft commit.
///
/// Keyed by `(group_id, log_index)`. The proposer calls `register()` after
/// the proposal returns the log index; `run_apply_loop` calls `complete()`
/// after the entry is applied. Either side can win the race — `complete()`
/// stores the result if no waiter exists yet, and `register()` picks it up
/// immediately if `complete()` already fired. Stored results are bounded by
/// the waiter window.
pub struct ProposeTracker {
    slots: Mutex<WaiterSlots>,
    /// Keys of recently committed entries, carried by a data-group snapshot.
    committed: Mutex<CommittedKeys>,
    /// Keys a snapshot install restored, per group, until the install adopts.
    installed_keys: Mutex<HashMap<u64, InstalledKeys>>,
    /// Keys a snapshot install restored, until the apply loop takes them into
    /// its proposal ledger.
    restored_for_ledger: Mutex<Vec<u64>>,
    /// Per-Raft-group apply watermark registry. Bumped by
    /// [`Self::note_applied`] once every entry of the group up to the index
    /// finished, so the watcher reflects "data applied on this node up to
    /// index N" — the only semantic that's useful for cross-node visibility
    /// waits. Tick-loop bumps cover the metadata group (sync redb apply);
    /// this tracker covers data groups (async SPSC dispatch through
    /// `run_apply_loop`). `None` only in tests that don't exercise the
    /// watcher.
    group_watchers: Option<Arc<GroupAppliedWatchers>>,
    /// Per group, the oldest committed entry the apply loop has not finished.
    applying: Mutex<HashMap<u64, ApplyingEntry>>,
    /// Per-group bound on entries between hand-off and settle, shared by the
    /// applier that hands entries off and the loop that settles them.
    window: Arc<ApplyWindow>,
    /// Per group, the indexes a data-group snapshot is cut at.
    cuts: Mutex<GroupCuts>,
}

/// The keys one install carried, and the lowest index they are complete from.
#[derive(Default)]
struct InstalledKeys {
    keys: HashSet<u64>,
    complete_from: u64,
}

impl Default for ProposeTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl ProposeTracker {
    pub fn new() -> Self {
        Self {
            slots: Mutex::new(WaiterSlots::new(DEFAULT_WAITER_WINDOW)),
            committed: Mutex::new(CommittedKeys::new(DEFAULT_WAITER_WINDOW)),
            installed_keys: Mutex::new(HashMap::new()),
            restored_for_ledger: Mutex::new(Vec::new()),
            group_watchers: None,
            applying: Mutex::new(HashMap::new()),
            window: Arc::new(ApplyWindow::default()),
            cuts: Mutex::new(GroupCuts::default()),
        }
    }

    /// Wire the per-group apply watermark registry. Called by
    /// `start_raft` after `SharedState` is constructed.
    pub fn with_group_watchers(mut self, watchers: Arc<GroupAppliedWatchers>) -> Self {
        self.group_watchers = Some(watchers);
        self
    }

    /// Set how long a propose waiter can exist: the statement deadline.
    /// Stored results and committed keys are kept this long.
    pub fn with_waiter_window(self, window: Duration) -> Self {
        self.slots
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .set_window(window);
        self.committed
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .set_window(window);
        self
    }

    /// The per-group apply window.
    pub fn window(&self) -> &Arc<ApplyWindow> {
        &self.window
    }

    fn slots(&self) -> std::sync::MutexGuard<'_, WaiterSlots> {
        self.slots.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Record the oldest entry of `group_id` the apply loop has not finished,
    /// or `None` once every entry it holds for the group finished.
    pub fn note_applying(&self, group_id: u64, entry: Option<ApplyingEntry>) {
        let mut applying = self.applying.lock().unwrap_or_else(|p| p.into_inner());
        match entry {
            Some(entry) => {
                applying.insert(group_id, entry);
            }
            None => {
                applying.remove(&group_id);
            }
        }
    }

    /// The oldest entry of `group_id` the apply loop has not finished, if any.
    pub fn applying(&self, group_id: u64) -> Option<ApplyingEntry> {
        self.applying
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&group_id)
            .cloned()
    }

    /// Advance `group_id`'s applied watermark to `log_index`. The apply loop
    /// calls it once every entry of the group up to `log_index` finished.
    pub fn note_applied(&self, group_id: u64, log_index: u64) {
        if let Some(w) = &self.group_watchers {
            w.bump(group_id, log_index);
        }
    }

    /// Register a waiter for a proposed entry. Returns a receiver that
    /// resolves when the entry is committed and executed.
    ///
    /// If `complete()` was called first, the receiver is pre-resolved. At an
    /// index an installed snapshot covers, it is answered at once by the
    /// proposal's key.
    pub fn register(
        &self,
        group_id: u64,
        log_index: u64,
        expected_key: u64,
    ) -> oneshot::Receiver<ProposeResult> {
        let (tx, rx) = oneshot::channel();
        self.slots()
            .register(group_id, log_index, expected_key, tx, Instant::now());
        rx
    }

    /// Drop the waiter a proposer registered at `(group_id, log_index)` and
    /// stopped waiting on: its deadline passed, or this node left the group
    /// and will never apply the index. A result already stored there stays.
    pub fn abandon(&self, group_id: u64, log_index: u64) {
        self.slots().abandon(group_id, log_index);
    }

    /// Complete a waiter after the entry has been committed and executed.
    ///
    /// If the proposer has already called `register()`, the result is sent
    /// immediately. If not, the result is stored, within the waiter window,
    /// so the next `register()` picks it up without waiting.
    ///
    /// When the applied entry's key differs from the proposer's, a leader
    /// change overwrote the proposal, and the waiter gets
    /// `RetryableLeaderChange` instead of another proposal's result.
    ///
    /// Returns true if a live waiter was found and notified, false otherwise.
    pub fn complete(
        &self,
        group_id: u64,
        log_index: u64,
        applied_key: u64,
        result: ProposeResult,
    ) -> bool {
        self.slots()
            .complete(group_id, log_index, applied_key, result, Instant::now())
    }

    /// Note that the apply loop sent entry `log_index` of `group_id` toward a
    /// core. Its own `complete` answers its waiter, so an install does not.
    pub fn note_dispatched(&self, group_id: u64, log_index: u64) {
        self.slots().note_dispatched(group_id, log_index);
    }

    /// Note that a dispatched entry concluded.
    pub fn note_concluded(&self, group_id: u64, log_index: u64) {
        self.slots().note_concluded(group_id, log_index);
    }

    /// Record the keys of committed entries the apply loop received, for the
    /// snapshots this node builds.
    pub fn note_committed(&self, group_id: u64, entries: impl IntoIterator<Item = (u64, u64)>) {
        let now = Instant::now();
        let mut committed = self.committed.lock().unwrap_or_else(|p| p.into_inner());
        for (log_index, key) in entries {
            committed.note(group_id, log_index, key, now);
        }
    }

    fn cuts(&self) -> std::sync::MutexGuard<'_, GroupCuts> {
        self.cuts.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Note that the apply loop took entry `log_index` of `group_id` off its
    /// backlog. Entries start in log order.
    pub fn note_started(&self, group_id: u64, log_index: u64) {
        self.cuts().note_started(group_id, log_index);
    }

    /// Highest entry of `group_id` the apply loop started. With the group's
    /// apply fenced, a snapshot is cut here once the group settled through it.
    pub fn started_through(&self, group_id: u64) -> u64 {
        self.cuts().started_through(group_id)
    }

    /// Note that a snapshot this node installed holds `group_id`'s entries
    /// through `log_index`.
    pub fn cover_through(&self, group_id: u64, log_index: u64) {
        self.cuts().cover_through(group_id, log_index);
    }

    /// Highest entry of `group_id` an installed snapshot holds. The apply
    /// loop concludes an entry at or below it without applying it.
    pub fn covered_through(&self, group_id: u64) -> u64 {
        self.cuts().covered_through(group_id)
    }

    /// `group_id`'s committed keys at or below `through`, within the waiter
    /// window, and the lowest index they are complete from. A snapshot at
    /// `through` carries them.
    pub fn committed_keys_through(&self, group_id: u64, through: u64) -> CarriedKeys {
        self.committed
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .through(group_id, through, Instant::now())
    }

    /// Take the committed keys a snapshot install carried, complete from
    /// index `complete_from`. The install's adopt answers waiters by them,
    /// and the apply loop adds them to its proposal ledger.
    pub fn restore_committed_keys(&self, group_id: u64, keys: &[(u64, u64)], complete_from: u64) {
        {
            let mut installed = self
                .installed_keys
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            let group = installed.entry(group_id).or_default();
            group.keys.extend(keys.iter().map(|&(_, key)| key));
            group.complete_from = group.complete_from.max(complete_from);
        }
        self.restored_for_ledger
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .extend(keys.iter().map(|&(_, key)| key));
    }

    /// Keys restored by snapshot installs since the last call.
    pub fn take_restored_keys(&self) -> Vec<u64> {
        std::mem::take(
            &mut *self
                .restored_for_ledger
                .lock()
                .unwrap_or_else(|p| p.into_inner()),
        )
    }

    /// Answer every waiter of `group_id` at or below `through`, the index of
    /// a snapshot this node adopted, whose entry the apply loop has not
    /// dispatched. A proposal key the snapshot carried committed:
    /// [`crate::Error::CommittedResultUnavailable`]. A key it lacks was
    /// overwritten and never commits: `RetryableLeaderChange`. Below the
    /// index the keys are complete from, a missing key proves nothing:
    /// [`crate::Error::ProposalOutcomeUnknown`]. A later `register` at or
    /// below `through` is answered the same way at once.
    ///
    /// An install that restored no keys (an empty stub) leaves every covered
    /// index unknown.
    pub fn resolve_covered(&self, group_id: u64, through: u64) {
        let installed = self
            .installed_keys
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&group_id)
            .unwrap_or(InstalledKeys {
                keys: HashSet::new(),
                complete_from: through.saturating_add(1),
            });
        self.slots().resolve_covered(
            group_id,
            through,
            installed.keys,
            installed.complete_from,
            Instant::now(),
        );
    }

    /// Answer the waiter at `(group_id, log_index)`, an entry the apply loop
    /// skipped because an installed snapshot covers it. The entry's own
    /// `applied_key` decides the answer. Stores nothing when no waiter exists.
    pub fn complete_covered(&self, group_id: u64, log_index: u64, applied_key: u64) -> bool {
        self.slots()
            .complete_covered(group_id, log_index, applied_key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::distributed_applier::propose_tracker::AppliedWrite;

    #[test]
    fn propose_tracker_register_and_complete() {
        let tracker = ProposeTracker::new();
        let mut rx = tracker.register(1, 5, 0xdead_beef);

        // The waiter receives the payload and the versions the apply stamped.
        let version = nodedb_types::WriteVersion::logged(2, 5);
        let vshard = crate::types::VShardId::new(3);
        assert!(tracker.complete(
            1,
            5,
            0xdead_beef,
            Ok(AppliedWrite {
                payload: b"result".to_vec(),
                write_versions: crate::types::ReadVersions::single(vshard, version),
            }),
        ));

        let result = rx.try_recv().unwrap().unwrap();
        assert_eq!(result.payload, b"result");
        assert_eq!(result.write_versions.of(vshard), Some(version));
    }

    #[test]
    fn propose_tracker_no_waiter_returns_false() {
        let tracker = ProposeTracker::new();
        assert!(!tracker.complete(1, 99, 0, Ok(AppliedWrite::unversioned(Vec::new()))));
    }

    #[test]
    fn propose_tracker_key_mismatch_surfaces_retryable_leader_change() {
        let tracker = ProposeTracker::new();
        let mut rx = tracker.register(1, 5, 0xaaaa);

        // A different proposer's entry committed at the same (group_id,
        // log_index); waiter must see RetryableLeaderChange, not its result.
        assert!(tracker.complete(
            1,
            5,
            0xbbbb,
            Ok(AppliedWrite::unversioned(
                b"other-proposers-payload".to_vec()
            )),
        ));

        match rx.try_recv().unwrap() {
            Err(crate::Error::RetryableLeaderChange {
                group_id,
                log_index,
            }) => {
                assert_eq!(group_id, 1);
                assert_eq!(log_index, 5);
            }
            other => panic!("expected RetryableLeaderChange, got {other:?}"),
        }
    }

    #[test]
    fn propose_tracker_zero_applied_key_passes_through_explicit_error() {
        // applied_key = 0 (leader-change no-op) must forward the explicit
        // RetryableLeaderChange, not be treated as a key mismatch.
        let tracker = ProposeTracker::new();
        let mut rx = tracker.register(1, 5, 0xaaaa);
        assert!(tracker.complete(
            1,
            5,
            0,
            Err(crate::Error::RetryableLeaderChange {
                group_id: 1,
                log_index: 5,
            }),
        ));
        assert!(matches!(
            rx.try_recv().unwrap(),
            Err(crate::Error::RetryableLeaderChange { .. })
        ));
    }

    /// The keys a leader carries in its snapshot decide each covered waiter
    /// on the follower that installs it: a committed proposal gets
    /// `CommittedResultUnavailable`, an overwritten one `RetryableLeaderChange`.
    #[test]
    fn snapshot_keys_decide_covered_waiters() {
        let leader = ProposeTracker::new();
        leader.note_committed(1, [(5, 0xa), (6, 0xc), (9, 0xd)]);
        let carried = leader.committed_keys_through(1, 7);
        assert_eq!(carried.keys, vec![(5, 0xa), (6, 0xc)]);
        assert_eq!(carried.complete_from, 5, "first index this leader saw");

        let follower = ProposeTracker::new();
        let mut committed = follower.register(1, 5, 0xa);
        let mut overwritten = follower.register(1, 6, 0xb);
        let mut unknown = follower.register(1, 4, 0xe);
        follower.restore_committed_keys(1, &carried.keys, carried.complete_from);
        follower.resolve_covered(1, 7);

        assert!(matches!(
            committed.try_recv().expect("answered"),
            Err(crate::Error::CommittedResultUnavailable { log_index: 5, .. })
        ));
        assert!(matches!(
            overwritten.try_recv().expect("answered"),
            Err(crate::Error::RetryableLeaderChange { log_index: 6, .. })
        ));
        assert!(matches!(
            unknown.try_recv().expect("answered"),
            Err(crate::Error::ProposalOutcomeUnknown { log_index: 4, .. })
        ));
        assert_eq!(follower.take_restored_keys(), vec![0xa, 0xc]);
    }

    /// An install that restored no keys, an empty stub, leaves every covered
    /// waiter's outcome unknown.
    #[test]
    fn a_stub_install_leaves_covered_outcomes_unknown() {
        let tracker = ProposeTracker::new();
        let mut rx = tracker.register(1, 5, 0xa);
        tracker.resolve_covered(1, 7);
        assert!(matches!(
            rx.try_recv().expect("answered"),
            Err(crate::Error::ProposalOutcomeUnknown { log_index: 5, .. })
        ));
        assert!(tracker.take_restored_keys().is_empty());
    }
}
