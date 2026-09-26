// SPDX-License-Identifier: BUSL-1.1

//! Propose tracker — slot map keyed by `(group_id, log_index)` that lets
//! proposers wait for a Raft entry to commit and execute, with race-safe
//! resolution if the apply path beats the proposer's `register()` call.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::{Arc, Mutex};

use super::apply_window::ApplyWindow;

use tokio::sync::oneshot;

use nodedb_cluster::GroupAppliedWatchers;

use crate::bridge::envelope::Response;
use crate::types::Lsn;

/// What a committed entry produced on the replica that applied it.
///
/// Carries the write's per-collection version alongside the payload because the
/// proposer has no other way to learn it: the version is minted inside the apply
/// path (the write funnel's WAL append), never on the wire, and the propose
/// tracker resolves on the very node that applied locally — so the version this
/// carries is that node's own, which is exactly what shard-local OCC validates
/// against.
#[derive(Debug, Clone)]
pub struct AppliedWrite {
    /// The Data Plane's response payload, verbatim.
    pub payload: Vec<u8>,
    /// The written collection's `coll_write_lsn` AFTER this write, in the local
    /// WAL-LSN domain — the one domain OCC's read validator compares in. It is
    /// NOT the Raft log index: the log index is a per-group counter that shares
    /// no scale with the WAL LSNs every other feed of that map records, and
    /// mixing the two silently breaks both directions of the comparison.
    pub write_version: Lsn,
}

impl AppliedWrite {
    /// Take both fields off the Data Plane's response to a committed write.
    ///
    /// `Response::read_version_lsn` is stamped by the core loop from the written
    /// collection's `coll_write_lsn`, read AFTER the handler recorded this
    /// write's LSN into the version index — so on a write response it is the
    /// post-write version. It is `Lsn::ZERO` for a plan that maps to no single
    /// user collection (see [`AppliedWrite::write_version`]).
    pub fn from_response(response: &Response) -> Self {
        Self {
            payload: response.payload.to_vec(),
            write_version: response.read_version_lsn,
        }
    }

    /// An applied entry that publishes no per-collection write-version: it wrote
    /// no Data-Plane collection state (a decode skip, a forwarded read result, a
    /// schema snapshot), or it was deduplicated before reaching the funnel. There
    /// is no version to floor a later read at, and `Lsn::ZERO` is the read-set
    /// capture's "no own-write floor" value — never a fabricated stand-in for a
    /// version that exists but was not read back.
    pub fn unversioned(payload: Vec<u8>) -> Self {
        Self {
            payload,
            write_version: Lsn::ZERO,
        }
    }
}

/// Result sent back to the proposer after commit + execution.
pub type ProposeResult = std::result::Result<AppliedWrite, crate::Error>;

/// Slot in the propose tracker — either a pending waiter or a completed result
/// that arrived before the waiter was registered.
enum TrackerSlot {
    /// Waiter registered by the proposer; awaiting `complete()`.
    ///
    /// `expected_key` is the proposer's idempotency key. The apply path
    /// passes the applied entry's key to `complete`; if they differ the
    /// proposer's reservation was overwritten by a different proposer's
    /// entry under a leader change and we surface
    /// `RetryableLeaderChange` instead of the (success-shaped) result
    /// that would otherwise leak the wrong entry's payload back to the
    /// proposer. `expected_key == 0` is a wildcard accepting any key
    /// (used for legacy synthetic registrations).
    Waiting {
        tx: oneshot::Sender<ProposeResult>,
        expected_key: u64,
    },
    /// `complete()` was called before `register()`. Stored so `register()`
    /// can resolve the channel immediately.
    Completed(ProposeResult),
}

/// Tracks pending proposals awaiting Raft commit.
///
/// Keyed by `(group_id, log_index)`. The proposer calls `register()` after
/// the proposal returns the log index; `run_apply_loop` calls `complete()`
/// after the entry is applied. Either side may win the race — `complete()`
/// stores the result if no waiter exists yet, and `register()` picks it up
/// immediately if `complete()` already fired.
pub struct ProposeTracker {
    slots: Mutex<HashMap<(u64, u64), TrackerSlot>>,
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
}

/// The oldest committed entry of a group the apply loop has not finished:
/// what a propose waiter that timed out names as the entry its group's
/// applied index waits behind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyingEntry {
    pub group_id: u64,
    pub log_index: u64,
    /// The collection the entry's plan writes, once the apply decoded it.
    pub collection: Option<String>,
}

impl std::fmt::Display for ApplyingEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "group {} index {}", self.group_id, self.log_index)?;
        if let Some(collection) = &self.collection {
            write!(f, " writing '{collection}'")?;
        }
        Ok(())
    }
}

impl Default for ProposeTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl ProposeTracker {
    pub fn new() -> Self {
        Self {
            slots: Mutex::new(HashMap::new()),
            group_watchers: None,
            applying: Mutex::new(HashMap::new()),
            window: Arc::new(ApplyWindow::default()),
        }
    }

    /// Wire the per-group apply watermark registry. Called by
    /// `start_raft` after `SharedState` is constructed.
    pub fn with_group_watchers(mut self, watchers: Arc<GroupAppliedWatchers>) -> Self {
        self.group_watchers = Some(watchers);
        self
    }

    /// The per-group apply window.
    pub fn window(&self) -> &Arc<ApplyWindow> {
        &self.window
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
    /// If `complete()` was called first (the entry was applied before this
    /// node could register), the receiver is pre-resolved and ready
    /// immediately.
    pub fn register(
        &self,
        group_id: u64,
        log_index: u64,
        expected_key: u64,
    ) -> oneshot::Receiver<ProposeResult> {
        let (tx, rx) = oneshot::channel();
        let mut slots = self.slots.lock().unwrap_or_else(|p| p.into_inner());
        match slots.entry((group_id, log_index)) {
            Entry::Vacant(e) => {
                e.insert(TrackerSlot::Waiting { tx, expected_key });
            }
            Entry::Occupied(e) => {
                match e.get() {
                    TrackerSlot::Completed(_) => {
                        // complete() already fired — extract the result, resolve
                        // the receiver immediately, and clean up the slot.
                        if let TrackerSlot::Completed(result) = e.remove() {
                            let _ = tx.send(result);
                        }
                    }
                    TrackerSlot::Waiting { .. } => {
                        // Duplicate register — shouldn't happen. Insert the new
                        // sender; the old receiver will see channel-closed.
                        *e.into_mut() = TrackerSlot::Waiting { tx, expected_key };
                    }
                }
            }
        }
        rx
    }

    /// Drop the waiter a proposer registered at `(group_id, log_index)` and
    /// stopped waiting on: its deadline passed, or this node left the group
    /// and will never apply the index. A result already stored there stays.
    pub fn abandon(&self, group_id: u64, log_index: u64) {
        let mut slots = self.slots.lock().unwrap_or_else(|p| p.into_inner());
        if let Entry::Occupied(e) = slots.entry((group_id, log_index))
            && matches!(e.get(), TrackerSlot::Waiting { .. })
        {
            e.remove();
        }
    }

    /// Complete a waiter after the entry has been committed and executed.
    ///
    /// If the proposer has already called `register()`, the result is sent
    /// immediately. If not, the result is stored so the next `register()`
    /// call picks it up without waiting.
    ///
    /// Entries of one group complete in any order. The applied watermark
    /// moves only through [`Self::note_applied`], in log order.
    ///
    /// Returns true if a live waiter was found and notified, false otherwise.
    pub fn complete(
        &self,
        group_id: u64,
        log_index: u64,
        applied_key: u64,
        result: ProposeResult,
    ) -> bool {
        let mut slots = self.slots.lock().unwrap_or_else(|p| p.into_inner());
        match slots.entry((group_id, log_index)) {
            Entry::Vacant(e) => {
                // No waiter yet — store result for the upcoming register().
                e.insert(TrackerSlot::Completed(result));
                false
            }
            Entry::Occupied(e) => {
                match e.get() {
                    TrackerSlot::Waiting { expected_key, .. } => {
                        // Idempotency-key gate: the entry that committed
                        // at this (group_id, log_index) must be the one
                        // the proposer reserved. If the keys disagree,
                        // a leader change overwrote the proposer's entry
                        // with a different one — surface the retryable
                        // signal instead of the (success-shaped) result
                        // that belongs to a different proposer. A zero
                        // applied_key means "no key carried" (empty
                        // entry / legacy); a zero expected_key means the
                        // registration is wildcard (legacy callers).
                        let mismatch =
                            applied_key != 0 && *expected_key != 0 && applied_key != *expected_key;
                        let final_result = if mismatch {
                            tracing::warn!(
                                group_id,
                                log_index,
                                applied_key,
                                expected_key = *expected_key,
                                "raft entry at proposer's index was overwritten by \
                                 a different proposal (idempotency_key mismatch); \
                                 surfacing RetryableLeaderChange"
                            );
                            Err(crate::Error::RetryableLeaderChange {
                                group_id,
                                log_index,
                            })
                        } else {
                            result
                        };
                        if let TrackerSlot::Waiting { tx, .. } = e.remove() {
                            let _ = tx.send(final_result);
                            return true;
                        }
                    }
                    TrackerSlot::Completed(_) => {
                        // Already completed — overwrite with newer result.
                        // Duplicate completes should not occur in practice;
                        // last write wins.
                        *e.into_mut() = TrackerSlot::Completed(result);
                    }
                }
                false
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn propose_tracker_register_and_complete() {
        let tracker = ProposeTracker::new();
        let mut rx = tracker.register(1, 5, 0xdead_beef);

        // Waiter must receive both payload and coll_write_lsn — its only channel
        // for a version minted on the apply path.
        assert!(tracker.complete(
            1,
            5,
            0xdead_beef,
            Ok(AppliedWrite {
                payload: b"result".to_vec(),
                write_version: Lsn::new(137),
            }),
        ));

        let result = rx.try_recv().unwrap().unwrap();
        assert_eq!(result.payload, b"result");
        assert_eq!(result.write_version, Lsn::new(137));
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

        let result = rx.try_recv().unwrap();
        match result {
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
        let result = rx.try_recv().unwrap();
        assert!(matches!(
            result,
            Err(crate::Error::RetryableLeaderChange { .. })
        ));
    }
}
