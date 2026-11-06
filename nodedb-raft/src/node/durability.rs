// SPDX-License-Identifier: BUSL-1.1

//! Applied-index durability and log compaction.
//!
//! Two watermarks are tracked separately and must not be confused:
//! `last_applied` is the DELIVERY watermark, advanced as entries are handed
//! to the state machine; `durable_applied` is the DURABILITY floor, advanced
//! only once those effects are fsynced. Compaction gates on the floor, never
//! on delivery — discarding an entry whose redo record is not yet durable
//! destroys the only source that can rebuild the memory-only engines.

use crate::error::{RaftError, Result};
use crate::message::LogEntry;
use crate::node::core::RaftNode;
use crate::storage::LogStorage;

impl<S: LogStorage> RaftNode<S> {
    /// Advance `last_applied` after the caller has applied entries.
    ///
    /// This is the DELIVERY watermark: it advances as entries are handed to
    /// the state machine, before their effects are necessarily durable. Use
    /// [`Self::save_durable_applied_index`] for the durability floor.
    ///
    /// Monotonic: a snapshot install can raise the watermark while the caller
    /// applies a batch taken earlier, so a lower `applied_to` is a no-op.
    pub fn advance_applied(&mut self, applied_to: u64) {
        self.volatile.last_applied = self.volatile.last_applied.max(applied_to);
    }

    /// Return a committed batch taken from `Ready` that the caller did not
    /// apply, so the next `take_ready` delivers it again.
    ///
    /// Entries at or below `last_applied` are dropped: a snapshot installed
    /// since the batch was taken covers them. Entries queued after the take
    /// that repeat the batch are dropped too, so each index is queued once.
    pub fn requeue_committed(&mut self, batch: Vec<LogEntry>) {
        let applied = self.volatile.last_applied;
        let mut merged: Vec<LogEntry> = batch
            .into_iter()
            .filter(|entry| entry.index > applied)
            .collect();
        let through = merged.last().map_or(applied, |entry| entry.index);
        merged.extend(
            self.ready
                .committed_entries
                .drain(..)
                .filter(|entry| entry.index > through),
        );
        self.ready.committed_entries = merged;
    }

    /// Whether committed entries this node has not applied are gone from its
    /// log. Only a snapshot can supply them.
    pub fn has_apply_gap(&self) -> bool {
        self.volatile.last_applied < self.log.snapshot_index()
    }

    /// Highest log index whose apply is durable on this node.
    pub fn durable_applied_index(&self) -> u64 {
        self.durable_applied
    }

    /// The lowest log index still available in the retained (post-compaction)
    /// log — `snapshot_index + 1`. A committed-entry read below this yields
    /// [`RaftError::LogCompacted`]. Used to arm a Calvin scheduler catch-up from
    /// the earliest replayable index so the drain never faults on a compacted
    /// range.
    pub fn first_available_index(&self) -> u64 {
        self.log.snapshot_index() + 1
    }

    /// Persist `index` as the durable applied floor.
    ///
    /// The caller MUST only pass an index whose state-machine effects are
    /// already durable — for data groups, an index whose redo record the WAL
    /// has fsynced. The next boot resumes delivery at `index + 1`, so an index
    /// saved ahead of durability silently drops the entries in between.
    ///
    /// Monotonic: an `index` at or below the current floor is a no-op, so an
    /// out-of-order or retrying caller can never move the floor backwards and
    /// re-expose an entry to a second apply.
    pub fn save_durable_applied_index(&mut self, index: u64) -> Result<()> {
        if index <= self.durable_applied {
            return Ok(());
        }
        self.log.storage_mut().save_applied_index(index)?;
        self.durable_applied = index;
        Ok(())
    }

    /// Auto-compaction threshold: entries retained past `snapshot_index`
    /// before the log is compacted. `None` disables auto-compaction.
    pub fn log_compaction_threshold(&self) -> Option<u64> {
        self.config.log_compaction_threshold
    }

    /// Compact the log up to `up_to_index` after the DATA-PLANE state
    /// machine has durably applied every entry `<= up_to_index`.
    ///
    /// Resolves the term at `up_to_index` from the in-memory log and
    /// calls [`RaftLog::apply_snapshot`], which discards entries
    /// `<= up_to_index` and persists the new snapshot boundary. The
    /// snapshot bytes themselves are NOT materialized here — the
    /// `SnapshotBuilder` hook rebuilds them on demand from live engine
    /// state when a lagging follower needs an `InstallSnapshot`.
    ///
    /// # Safety / gating
    ///
    /// The CALLER MUST pass an `up_to_index` that the DATA-PLANE state
    /// machine has durably applied. Compacting past a data-plane-unapplied
    /// index would let the `SnapshotBuilder` serialize incomplete state.
    /// The sole caller path (`run_apply_loop` → [`Self::maybe_compact_log`])
    /// guarantees this: it only compacts an index after the SPSC round-trip
    /// that applies that entry to the Data Plane has returned.
    ///
    /// This method additionally clamps to the DURABLE applied index
    /// (returning [`RaftError::CompactionAheadOfApplied`] otherwise).
    /// Deliberately not `volatile.last_applied`: that advances at
    /// commit/enqueue time, so clamping to it would let compaction discard
    /// entries whose redo record is not yet fsynced — losing the only recovery
    /// source for the memory-only engines.
    ///
    /// It also clamps to `volatile.last_applied`. The data plane can make an
    /// entry durable before the tick records its hand-off. Compacting past
    /// `last_applied` then would discard entries the next collection still
    /// reads.
    ///
    /// Every request is first clamped to the lowest compaction ceiling (see
    /// [`Self::set_compaction_ceiling`]).
    ///
    /// Returns `Ok(false)` when there is nothing to compact
    /// (`up_to_index <= snapshot_index`). Returns
    /// `Err(RaftError::LogCompacted)` if the term at `up_to_index` is no
    /// longer available (already compacted away).
    pub fn compact_log_up_to(&mut self, up_to_index: u64) -> Result<bool> {
        let up_to_index = self.compaction_ceilings.clamp(up_to_index);
        if up_to_index <= self.log.snapshot_index() {
            return Ok(false);
        }
        let ceiling = self.durable_applied.min(self.volatile.last_applied);
        if up_to_index > ceiling {
            return Err(RaftError::CompactionAheadOfApplied {
                requested: up_to_index,
                last_applied: ceiling,
            });
        }
        let term = self
            .log
            .term_at(up_to_index)
            .ok_or(RaftError::LogCompacted {
                requested: up_to_index,
                first_available: self.log.snapshot_index() + 1,
            })?;
        self.log.apply_snapshot(up_to_index, term)?;
        Ok(true)
    }

    /// The entry bytes this replica's log holds in memory.
    pub fn log_retained_bytes(&self) -> u64 {
        self.log.retained_bytes()
    }

    /// Committed entries from `lo` whose data sums to at most `max_bytes`. The
    /// first entry always ships, so an oversized one still ships alone.
    pub fn log_committed_within(&self, lo: u64, max_bytes: usize) -> Result<&[LogEntry]> {
        self.log
            .entries_within(lo, self.volatile.commit_index, max_bytes)
    }

    /// Bound every compaction of this log by `owner`'s `ceiling`: no entry
    /// above the value it holds is discarded, whoever asks. Other owners'
    /// ceilings stay, and the lowest binds.
    pub fn set_compaction_ceiling(
        &mut self,
        owner: &'static str,
        ceiling: std::sync::Arc<std::sync::atomic::AtomicU64>,
    ) {
        self.compaction_ceilings.set(owner, ceiling);
    }

    /// Replace every compaction ceiling of this log, for a group that mounts.
    pub fn set_compaction_ceilings(
        &mut self,
        ceilings: super::compaction_ceiling::CompactionCeilings,
    ) {
        self.compaction_ceilings = ceilings;
    }

    /// Check the configured auto-compaction threshold against the
    /// data-plane applied index `applied_index` and compact the log up to
    /// `applied_index` if the retained-entry count has reached the
    /// threshold.
    ///
    /// `applied_index` is the index the DATA-PLANE state machine has
    /// durably applied up to (NOT raft's commit index) — see
    /// [`RaftConfig::log_compaction_threshold`]. No-op when the threshold
    /// is `None` or the retained count is below it.
    ///
    /// Returns `Ok(true)` when a compaction was performed.
    pub fn maybe_compact_log(&mut self, applied_index: u64) -> Result<bool> {
        let Some(threshold) = self.config.log_compaction_threshold else {
            return Ok(false);
        };
        let snapshot_index = self.log.snapshot_index();
        if applied_index <= snapshot_index {
            return Ok(false);
        }
        if applied_index - snapshot_index < threshold {
            return Ok(false);
        }
        // Never compact past an entry whose apply is not yet durable, or that
        // the delivery watermark has not recorded yet.
        let up_to = applied_index
            .min(self.durable_applied)
            .min(self.volatile.last_applied);
        self.compact_log_up_to(up_to)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemStorage;
    use crate::test_support::{apply_durably, leader_with_applied_noop, test_config};

    #[test]
    fn threshold_some_compacts_after_enough_applied() {
        // Single-voter group so every propose commits immediately.
        let mut cfg = test_config(1, vec![]);
        cfg.log_compaction_threshold = Some(4);
        let mut node = leader_with_applied_noop(cfg);

        // Propose entries and apply each as the data plane would.
        for _ in 0..8 {
            let idx = node.propose(b"write".to_vec()).unwrap();
            let _ = node.take_ready();
            apply_durably(&mut node, idx);

            // Trigger gated on the data-plane applied watermark (= idx here).
            node.maybe_compact_log(idx).unwrap();
        }

        let snap = node.log_snapshot_index();
        // With threshold 4, the log keeps at most 4 entries past the
        // snapshot boundary; the boundary must have advanced.
        assert!(
            snap > 0,
            "snapshot_index should have advanced past 0, got {snap}"
        );
        assert!(
            node.last_log_index() - snap <= 4,
            "retained entries ({}) must be <= threshold (4)",
            node.last_log_index() - snap
        );

        // Entries at or before the snapshot boundary are discarded.
        assert!(
            node.log.entry_at(snap).is_none(),
            "entry at snapshot boundary must be gone"
        );
        assert!(
            node.log.entries_range(1, snap).is_err(),
            "range into compacted region must fail"
        );
    }

    #[test]
    fn threshold_none_never_compacts() {
        let cfg = test_config(1, vec![]); // log_compaction_threshold: None
        let mut node = leader_with_applied_noop(cfg);

        for _ in 0..12 {
            let idx = node.propose(b"write".to_vec()).unwrap();
            let _ = node.take_ready();
            apply_durably(&mut node, idx);
            // No-op: threshold is None.
            assert!(!node.maybe_compact_log(idx).unwrap());
        }

        assert_eq!(
            node.log_snapshot_index(),
            0,
            "no compaction must occur when threshold is None"
        );
        // Every entry from index 1 is still present.
        assert!(node.log.entry_at(1).is_some());
        assert!(node.log.entries_range(1, node.last_log_index()).is_ok());
    }

    #[test]
    fn no_compaction_passes_the_ceiling() {
        let mut node = leader_with_applied_noop(test_config(1, vec![]));
        let mut last = 0;
        for _ in 0..4 {
            last = node.propose(b"write".to_vec()).unwrap();
            apply_durably(&mut node, last);
        }
        let ceiling = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        node.set_compaction_ceiling("archive", std::sync::Arc::clone(&ceiling));
        assert!(
            !node.compact_log_up_to(last).unwrap(),
            "a zero ceiling holds every entry"
        );
        ceiling.store(2, std::sync::atomic::Ordering::Release);
        assert!(node.compact_log_up_to(last).unwrap());
        assert_eq!(node.first_available_index(), 3);
    }

    /// A second owner's ceiling holds entries the first owner released.
    #[test]
    fn the_lowest_owner_holds_the_log() {
        let mut node = leader_with_applied_noop(test_config(1, vec![]));
        let mut last = 0;
        for _ in 0..4 {
            last = node.propose(b"write".to_vec()).unwrap();
            apply_durably(&mut node, last);
        }
        let archive = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(u64::MAX));
        let pin = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(2));
        node.set_compaction_ceiling("archive", archive);
        node.set_compaction_ceiling("pin", std::sync::Arc::clone(&pin));
        assert!(node.compact_log_up_to(last).unwrap());
        assert_eq!(node.first_available_index(), 3);
        pin.store(u64::MAX, std::sync::atomic::Ordering::Release);
        assert!(node.compact_log_up_to(last).unwrap());
        assert_eq!(node.first_available_index(), last + 1);
    }

    #[test]
    fn compact_log_up_to_rejects_ahead_of_applied() {
        let mut cfg = test_config(1, vec![]);
        cfg.log_compaction_threshold = Some(2);
        let mut node = leader_with_applied_noop(cfg);

        let idx = node.propose(b"write".to_vec()).unwrap();
        let _ = node.take_ready();
        // Deliberately do NOT apply past the noop — the data plane has not
        // applied `idx` yet.
        let err = node.compact_log_up_to(idx).unwrap_err();
        assert!(matches!(err, RaftError::CompactionAheadOfApplied { .. }));
    }

    /// Compaction gates on the DURABLE applied floor, not the delivery
    /// watermark. An entry that has been handed to the state machine but whose
    /// redo is not yet fsynced must NOT be compacted away: the log is the only
    /// thing that can rebuild the memory-only engines for it.
    #[test]
    fn compact_log_up_to_rejects_delivered_but_not_durable() {
        let mut cfg = test_config(1, vec![]);
        cfg.log_compaction_threshold = Some(2);
        let mut node = leader_with_applied_noop(cfg);

        let idx = node.propose(b"write".to_vec()).unwrap();
        let _ = node.take_ready();
        // Delivery watermark advances; the durable floor does not.
        node.advance_applied(idx);

        let err = node.compact_log_up_to(idx).unwrap_err();
        assert!(matches!(err, RaftError::CompactionAheadOfApplied { .. }));

        // Once the apply is durable the same index compacts.
        node.save_durable_applied_index(idx).unwrap();
        assert!(node.compact_log_up_to(idx).unwrap());
    }

    /// A batch taken before a snapshot install finishes after it: its lower
    /// watermark must not pull `last_applied` back below the snapshot.
    #[test]
    fn delivery_watermark_never_regresses() {
        let mut node = RaftNode::new(test_config(1, vec![]), MemStorage::new());
        node.advance_applied(10);
        assert_eq!(node.last_applied(), 10);

        node.advance_applied(4);
        assert_eq!(node.last_applied(), 10);

        node.advance_applied(12);
        assert_eq!(node.last_applied(), 12);
    }

    /// The data plane can make an entry durable before the tick records its
    /// hand-off. Compaction then stops at `last_applied`, so no apply gap
    /// opens.
    #[test]
    fn compaction_never_passes_the_delivery_watermark() {
        let mut cfg = test_config(1, vec![]);
        cfg.log_compaction_threshold = Some(1);
        let mut node = leader_with_applied_noop(cfg);
        let idx = node
            .propose(b"write".to_vec())
            .expect("single voter commits");
        let _ = node.take_ready();
        node.save_durable_applied_index(idx).expect("save floor");

        let err = node.compact_log_up_to(idx).expect_err("ahead of delivery");
        assert!(matches!(err, RaftError::CompactionAheadOfApplied { .. }));
        node.maybe_compact_log(idx).expect("clamped compaction");
        assert!(node.log_snapshot_index() <= node.last_applied());
        assert!(!node.has_apply_gap());
    }

    /// A requeued batch comes back once, merged with entries queued after it
    /// was taken, and without the entries a snapshot has since covered.
    #[test]
    fn requeued_batch_is_delivered_again_once() {
        let mut node = leader_with_applied_noop(test_config(1, vec![]));
        let first = node.propose(b"a".to_vec()).expect("single voter commits");
        node.propose(b"b".to_vec()).expect("single voter commits");
        let batch = node.take_ready().committed_entries;
        let third = node.propose(b"c".to_vec()).expect("single voter commits");

        node.requeue_committed(batch.clone());
        let indices: Vec<u64> = node
            .take_ready()
            .committed_entries
            .iter()
            .map(|entry| entry.index)
            .collect();
        assert_eq!(indices, vec![first, first + 1, third]);

        node.advance_applied(first);
        node.requeue_committed(batch);
        let indices: Vec<u64> = node
            .take_ready()
            .committed_entries
            .iter()
            .map(|entry| entry.index)
            .collect();
        assert_eq!(indices, vec![first + 1]);
    }

    /// The durable floor never moves backwards, however a caller retries.
    #[test]
    fn durable_applied_index_is_monotonic() {
        let mut node = RaftNode::new(test_config(1, vec![]), MemStorage::new());
        assert_eq!(node.durable_applied_index(), 0);

        node.save_durable_applied_index(5).unwrap();
        assert_eq!(node.durable_applied_index(), 5);

        node.save_durable_applied_index(3).unwrap();
        assert_eq!(node.durable_applied_index(), 5);
    }
}
