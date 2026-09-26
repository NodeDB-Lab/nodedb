// SPDX-License-Identifier: BUSL-1.1

//! Exactly-once applied gate for the Calvin scheduler.
//!
//! A Calvin epoch carries N independent transactions, one per `position`. Each
//! `(epoch, position)` is a distinct user transaction: a multi-participant txn
//! is ONE `(epoch, position)` spread across several vShard *slices*, but one
//! epoch routinely carries several positions touching the SAME vShard. Applying
//! one position of an epoch therefore says nothing about the others.
//!
//! [`AppliedGate`] tracks which `(epoch, position)` pairs a vShard has applied,
//! so the scheduler skips exactly the positions that already committed — never a
//! whole epoch on the strength of its first completing position. Skipping a
//! whole epoch after one position lost every other position of that epoch across
//! a restart (a torn transaction); the per-position gate is what prevents it.
//!
//! Two representations, one meaning ("this position is applied, do not re-run"):
//!
//! - [`AppliedGate::fully_applied_epoch`] (`W`) — a watermark: every position of
//!   every epoch `<= W` is applied. Compresses a contiguous fully-applied
//!   prefix into a single number so the tail set stays bounded.
//! - `applied_tail` — the applied `(epoch, position)` pairs for epochs `> W`.
//!
//! The gate is exact regardless of `W`'s value: `W` is only a memory bound, not
//! a correctness device. At recovery, `W` starts at the not-yet-applied sentinel
//! (nothing is *proven* fully applied without the per-epoch expected counts) and
//! the tail carries every applied marker read from the WAL; the watermark then
//! advances as the sequencer re-fans-out the log and the per-`(epoch, vShard)`
//! expected counts are learned.

use std::collections::{BTreeMap, BTreeSet};

use super::recovery::NOT_YET_APPLIED_EPOCH;

/// Per-vShard exactly-once applied gate: a fully-applied watermark plus the set
/// of applied `(epoch, position)` pairs above it.
///
/// Deterministic: `BTreeSet`/`BTreeMap` throughout so iteration order (and thus
/// watermark advancement) is identical across replicas.
#[derive(Debug)]
pub struct AppliedGate {
    /// Fully-applied watermark `W`: every position of every epoch `<= W` is
    /// applied. [`NOT_YET_APPLIED_EPOCH`] means no epoch is fully applied yet.
    fully_applied_epoch: u64,
    /// Applied `(epoch, position)` pairs for epochs `> W`. Pruned as `W` folds
    /// each epoch, so its size is bounded by the in-flight / un-folded epoch
    /// window (at steady state) or the recovered WAL replay window (until the
    /// re-fan-out folds the recovered prefix into `W`).
    applied_tail: BTreeSet<(u64, u32)>,
    /// Learned per-`(epoch, vShard)` expected position count, keyed by epoch,
    /// for epochs delivered with a KNOWN count (`epoch_vshard_txn_count >= 1`).
    /// Populated from each delivered `SequencedTxn`'s `epoch_vshard_txn_count`.
    /// An epoch folds into `W` once its applied count reaches this. Pruned with
    /// the tail as `W` advances, so it is bounded by the same window.
    epoch_expected: BTreeMap<u64, u32>,
    /// Delivered positions of epochs whose count is UNKNOWN, keyed by epoch. An
    /// epoch lands here when it is delivered with `epoch_vshard_txn_count == 0`,
    /// which only happens for a batch encoded before the count field existed:
    /// the sequencer stamps every delivered copy with a count `>= 1`, so `0`
    /// unambiguously means "count not recorded", never a legitimate zero. Since
    /// the count is not known ahead of time, the epoch cannot fold on an
    /// applied-vs-expected match; instead it folds via in-order delivery — once
    /// a higher epoch is seen, no more of this epoch's positions can arrive, so
    /// the set here is the complete delivered set and the epoch folds when every
    /// member is applied. An epoch is one batch encoded once, so its positions
    /// are uniformly pre- or post-migration; it never appears in both this map
    /// and `epoch_expected`. Pruned with the tail as `W` advances.
    delivered_unknown: BTreeMap<u64, BTreeSet<u32>>,
    /// Highest epoch ever delivered to this vShard ([`NOT_YET_APPLIED_EPOCH`] if
    /// none). Epochs are fanned out in nondecreasing order, so any epoch `<=
    /// highest_seen` that participates on this vShard has already been recorded
    /// in `epoch_expected` or `delivered_unknown`; an epoch `<= highest_seen`
    /// absent from both simply does not touch this vShard and the watermark may
    /// advance over it. Without this bound the watermark would stall at the
    /// first non-participating epoch and the tail would grow without limit.
    highest_seen_epoch: u64,
}

impl AppliedGate {
    /// Construct a gate from a recovery scan: a fully-applied watermark and the
    /// set of applied `(epoch, position)` pairs above it.
    pub fn new(fully_applied_epoch: u64, applied_tail: BTreeSet<(u64, u32)>) -> Self {
        Self {
            fully_applied_epoch,
            applied_tail,
            epoch_expected: BTreeMap::new(),
            delivered_unknown: BTreeMap::new(),
            highest_seen_epoch: NOT_YET_APPLIED_EPOCH,
        }
    }

    /// The fully-applied watermark `W` ([`NOT_YET_APPLIED_EPOCH`] if none).
    pub fn fully_applied_epoch(&self) -> u64 {
        self.fully_applied_epoch
    }

    /// Whether `W` is the not-yet-applied sentinel (nothing fully applied).
    fn is_sentinel(&self) -> bool {
        self.fully_applied_epoch == NOT_YET_APPLIED_EPOCH
    }

    /// The highest epoch delivered to this vShard, or `None` when none was.
    pub fn highest_seen_epoch(&self) -> Option<u64> {
        (self.highest_seen_epoch != NOT_YET_APPLIED_EPOCH).then_some(self.highest_seen_epoch)
    }

    /// Whether every position of every epoch at or below `epoch` is applied.
    pub fn is_fully_applied_through(&self, epoch: u64) -> bool {
        !self.is_sentinel() && self.fully_applied_epoch >= epoch
    }

    /// Record the delivery of `(epoch, position)` on this vShard, carrying the
    /// sequencer's per-`(epoch, vShard)` position `count`.
    ///
    /// A `count >= 1` is the authoritative expected count: every position of the
    /// epoch targeting this vShard carries the same value, so recording it is
    /// idempotent (first writer wins) and `position` is unused. A `count == 0`
    /// means the count was not recorded (a pre-field batch); the position is
    /// tracked instead so the epoch can fold via in-order delivery. Ignored for
    /// epochs already folded into `W`.
    pub fn note_expected(&mut self, epoch: u64, position: u32, count: u32) {
        if !self.is_sentinel() && epoch <= self.fully_applied_epoch {
            // Already folded (⇒ already at or below `highest_seen`); nothing to
            // learn.
            return;
        }
        if count == 0 {
            self.delivered_unknown
                .entry(epoch)
                .or_default()
                .insert(position);
        } else {
            self.epoch_expected.entry(epoch).or_insert(count);
        }
        if self.highest_seen_epoch == NOT_YET_APPLIED_EPOCH || epoch > self.highest_seen_epoch {
            self.highest_seen_epoch = epoch;
        }
    }

    /// Whether `(epoch, position)` is already applied — an EXACT check.
    ///
    /// True iff the epoch is at or below the fully-applied watermark, or the
    /// exact pair is in the applied tail. Re-running an applied position would
    /// re-fire its side effects, so this gate is the exactly-once mechanism.
    pub fn is_applied(&self, epoch: u64, position: u32) -> bool {
        (!self.is_sentinel() && epoch <= self.fully_applied_epoch)
            || self.applied_tail.contains(&(epoch, position))
    }

    /// Mark `(epoch, position)` applied, then try to advance the watermark.
    ///
    /// Returns `Some(new_W)` if the watermark advanced, so the caller can
    /// publish it (metrics + cross-shard snapshot anchor).
    pub fn mark_applied(&mut self, epoch: u64, position: u32) -> Option<u64> {
        if !self.is_sentinel() && epoch <= self.fully_applied_epoch {
            // Already folded; nothing to record and nothing can advance.
            return None;
        }
        self.applied_tail.insert((epoch, position));
        self.advance()
    }

    /// Fold every now-contiguous fully-applied epoch into the watermark, pruning
    /// its tail and expected-count entries.
    ///
    /// Returns `Some(new_W)` if the watermark advanced, else `None`.
    ///
    /// Folding is driven by the learned expected counts, so it works both for
    /// live epochs (each position completes and is marked) and for a recovered
    /// prefix (positions seeded from the WAL, counts learned from the
    /// re-fan-out): as soon as an epoch's applied count reaches its expected
    /// count AND it is contiguous with `W`, it folds and its tail entries are
    /// reclaimed.
    pub fn advance(&mut self) -> Option<u64> {
        let start = self.fully_applied_epoch;
        if self.highest_seen_epoch == NOT_YET_APPLIED_EPOCH {
            // Nothing delivered yet — no basis to advance.
            return None;
        }

        loop {
            let next = if self.is_sentinel() {
                // No epoch folded yet: begin at the lowest participating epoch.
                // Re-fan-out delivers epochs in nondecreasing order, so the
                // lowest observed epoch is the first that can fold; folding it
                // also covers every lower (non-participating or truncated) epoch
                // via the `epoch <= W` gate.
                match self.next_participating(0) {
                    Some(e) => e,
                    None => break,
                }
            } else {
                let n = self.fully_applied_epoch + 1;
                if n > self.highest_seen_epoch {
                    break;
                }
                n
            };

            if let Some(expected) = self.epoch_expected.get(&next).copied() {
                // Known count: fold once every expected position is applied.
                if expected == 0 || self.epoch_applied_count(next) < expected {
                    // A participating epoch that is not yet fully applied stops
                    // the walk — the watermark must stay contiguous.
                    break;
                }
                self.fold_epoch(next);
            } else if self.delivered_unknown.contains_key(&next) {
                // Unknown count: the expected total is not known ahead of time,
                // so fold via in-order delivery. Delivery to a vShard is
                // epoch-ordered (the sequencer enqueues all of an epoch's
                // positions before moving to the next epoch), so once a higher
                // epoch has been seen no further positions of `next` can arrive
                // and the recorded delivered set is complete. Until then (`next`
                // is still the highest seen) more positions may arrive, so hold.
                if self.highest_seen_epoch <= next
                    || self.epoch_applied_count(next) < self.delivered_count(next)
                {
                    break;
                }
                self.fold_epoch(next);
            } else {
                // `next` does not touch this vShard (ordered delivery ⇒ any
                // participating epoch <= highest_seen is already recorded in one
                // of the two maps). Non-participating epochs are never delivered
                // here, so they hold no tail/expected/delivered entries — there
                // is nothing to prune. Jump the watermark to just below the next
                // participating epoch, or to highest_seen if none remain —
                // O(log n), never O(gap-width).
                match self.next_participating(next) {
                    Some(k) if k <= self.highest_seen_epoch => {
                        self.fully_applied_epoch = k - 1;
                    }
                    _ => {
                        self.fully_applied_epoch = self.highest_seen_epoch;
                        break;
                    }
                }
            }
        }

        if self.fully_applied_epoch != start {
            Some(self.fully_applied_epoch)
        } else {
            None
        }
    }

    /// Count applied positions recorded for `epoch` in the tail.
    fn epoch_applied_count(&self, epoch: u64) -> u32 {
        self.applied_tail
            .range((epoch, 0)..=(epoch, u32::MAX))
            .count() as u32
    }

    /// Number of distinct positions delivered for an unknown-count `epoch`.
    fn delivered_count(&self, epoch: u64) -> u32 {
        self.delivered_unknown
            .get(&epoch)
            .map(|positions| positions.len() as u32)
            .unwrap_or(0)
    }

    /// Lowest participating epoch `>= from` across both the known-count and
    /// unknown-count maps ([`None`] if this vShard has no such epoch recorded).
    fn next_participating(&self, from: u64) -> Option<u64> {
        let known = self.epoch_expected.range(from..).next().map(|(k, _)| *k);
        let unknown = self.delivered_unknown.range(from..).next().map(|(k, _)| *k);
        match (known, unknown) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        }
    }

    /// Fold `epoch` into `W` and reclaim its tail, expected-count, and
    /// delivered-position entries (whichever it holds).
    fn fold_epoch(&mut self, epoch: u64) {
        self.fully_applied_epoch = epoch;
        self.epoch_expected.remove(&epoch);
        self.delivered_unknown.remove(&epoch);
        self.prune_epoch(epoch);
    }

    /// Remove all tail entries for `epoch` (called after it folds into `W`).
    fn prune_epoch(&mut self, epoch: u64) {
        let doomed: Vec<(u64, u32)> = self
            .applied_tail
            .range((epoch, 0)..=(epoch, u32::MAX))
            .copied()
            .collect();
        for key in doomed {
            self.applied_tail.remove(&key);
        }
    }

    /// Number of `(epoch, position)` pairs held in the tail. Test/observability
    /// helper for asserting the tail is bounded (pruned as `W` advances).
    #[cfg(test)]
    pub fn tail_len(&self) -> usize {
        self.applied_tail.len()
    }

    /// Whether any bookkeeping map still holds an entry for `epoch`. Test helper
    /// for asserting a folded epoch leaks nothing across the three maps.
    #[cfg(test)]
    pub fn has_epoch_state(&self, epoch: u64) -> bool {
        self.epoch_expected.contains_key(&epoch)
            || self.delivered_unknown.contains_key(&epoch)
            || self
                .applied_tail
                .range((epoch, 0)..=(epoch, u32::MAX))
                .next()
                .is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_gate() -> AppliedGate {
        AppliedGate::new(NOT_YET_APPLIED_EPOCH, BTreeSet::new())
    }

    #[test]
    fn per_position_skip_is_exact() {
        // A tail containing (5, 0) but NOT (5, 1): position 0 is applied,
        // position 1 is not — the whole epoch must NOT be skipped.
        let mut tail = BTreeSet::new();
        tail.insert((5u64, 0u32));
        let gate = AppliedGate::new(NOT_YET_APPLIED_EPOCH, tail);

        assert!(
            gate.is_applied(5, 0),
            "(5,0) is applied and must be skipped"
        );
        assert!(
            !gate.is_applied(5, 1),
            "(5,1) is NOT applied and must NOT be skipped — else a torn txn"
        );
    }

    #[test]
    fn watermark_skips_at_or_below() {
        let gate = AppliedGate::new(4, BTreeSet::new());
        assert!(gate.is_applied(3, 9), "epoch below W is applied");
        assert!(gate.is_applied(4, 0), "epoch at W is applied");
        assert!(!gate.is_applied(5, 0), "epoch above W is not applied by W");
    }

    #[test]
    fn sentinel_never_skips_by_watermark() {
        let gate = empty_gate();
        assert!(!gate.is_applied(0, 0), "sentinel W must not skip epoch 0");
    }

    #[test]
    fn full_epoch_folds_and_prunes_tail() {
        let mut gate = empty_gate();
        // Epoch 0 has two positions on this vShard.
        gate.note_expected(0, 0, 2);
        assert_eq!(gate.mark_applied(0, 0), None, "1 of 2 applied — no advance");
        assert_eq!(gate.fully_applied_epoch(), NOT_YET_APPLIED_EPOCH);
        assert_eq!(gate.tail_len(), 1);

        // Second position completes the epoch: W folds to 0, tail is pruned.
        assert_eq!(gate.mark_applied(0, 1), Some(0), "2 of 2 applied — advance");
        assert_eq!(gate.fully_applied_epoch(), 0);
        assert_eq!(gate.tail_len(), 0, "folded epoch's tail entries are pruned");
    }

    #[test]
    fn partial_epoch_does_not_advance() {
        let mut gate = empty_gate();
        gate.note_expected(3, 0, 3);
        gate.mark_applied(3, 0);
        gate.mark_applied(3, 2);
        assert_eq!(
            gate.fully_applied_epoch(),
            NOT_YET_APPLIED_EPOCH,
            "2 of 3 positions applied must not fold the epoch"
        );
        assert_eq!(gate.tail_len(), 2);
    }

    #[test]
    fn advance_walks_contiguous_epochs_and_stops_at_gap() {
        let mut gate = empty_gate();
        // Epoch 0: 1 position; epoch 1: 1 position; epoch 2: 2 positions (only
        // one applied so far, so epoch 2 is incomplete).
        gate.note_expected(0, 0, 1);
        gate.note_expected(1, 0, 1);
        gate.note_expected(2, 0, 2);

        // Apply epoch 2's first position and epoch 1's — epoch 0 not yet applied,
        // so nothing folds (no contiguous prefix from the start).
        gate.mark_applied(2, 0);
        assert_eq!(gate.fully_applied_epoch(), NOT_YET_APPLIED_EPOCH);
        gate.mark_applied(1, 0);
        assert_eq!(gate.fully_applied_epoch(), NOT_YET_APPLIED_EPOCH);

        // Apply epoch 0: now 0 and 1 are complete and contiguous → W folds to 1,
        // but stops at epoch 2 (incomplete). Epoch 2's tail entry remains.
        assert_eq!(gate.mark_applied(0, 0), Some(1));
        assert_eq!(gate.fully_applied_epoch(), 1);
        assert_eq!(gate.tail_len(), 1, "only the incomplete epoch 2 remains");

        // Completing epoch 2 folds W to 2 and prunes.
        assert_eq!(gate.mark_applied(2, 1), Some(2));
        assert_eq!(gate.fully_applied_epoch(), 2);
        assert_eq!(gate.tail_len(), 0);
    }

    #[test]
    fn watermark_skips_non_participating_epochs() {
        // This vShard participates in epochs 0 and 2 but NOT 1 (epoch 1's txns
        // touch other vShards, so it is never delivered here). The watermark must
        // advance over epoch 1 rather than stalling — otherwise the tail grows
        // without bound.
        let mut gate = empty_gate();
        gate.note_expected(0, 0, 1);
        gate.note_expected(2, 0, 1); // epoch 1 never noted — non-participating

        // Applying epoch 0 folds it and jumps over the non-participating epoch 1
        // up to (but not past) the not-yet-complete epoch 2.
        assert_eq!(gate.mark_applied(0, 0), Some(1));
        assert_eq!(
            gate.fully_applied_epoch(),
            1,
            "watermark jumps over non-participating epoch 1"
        );

        // Completing epoch 2 folds it too.
        assert_eq!(gate.mark_applied(2, 0), Some(2));
        assert_eq!(gate.fully_applied_epoch(), 2);
        assert_eq!(gate.tail_len(), 0, "no tail entries leak across the gap");
    }

    #[test]
    fn watermark_does_not_advance_past_highest_delivered() {
        // With epoch 0 applied and delivered, the watermark must not run ahead of
        // the highest epoch actually delivered (a later epoch may still be
        // in-flight in the channel).
        let mut gate = empty_gate();
        gate.note_expected(0, 0, 1);
        assert_eq!(gate.mark_applied(0, 0), Some(0));
        assert_eq!(
            gate.fully_applied_epoch(),
            0,
            "watermark stops at the highest delivered epoch, not beyond"
        );
    }

    #[test]
    fn recovered_prefix_folds_when_counts_are_learned() {
        // Simulate recovery: epoch 5 fully committed pre-crash (both positions
        // seeded in the tail from WAL markers), W sentinel (counts unknown).
        let mut tail = BTreeSet::new();
        tail.insert((5u64, 0u32));
        tail.insert((5u64, 1u32));
        let mut gate = AppliedGate::new(NOT_YET_APPLIED_EPOCH, tail);
        assert_eq!(gate.tail_len(), 2);

        // Re-fan-out delivers the count for epoch 5: both positions are already
        // applied, so learning the count folds epoch 5 into W and prunes it.
        gate.note_expected(5, 0, 2);
        assert_eq!(gate.advance(), Some(5));
        assert_eq!(gate.fully_applied_epoch(), 5);
        assert_eq!(gate.tail_len(), 0, "recovered prefix is reclaimed");
        assert!(gate.is_applied(5, 0));
        assert!(gate.is_applied(5, 1));
    }

    #[test]
    fn recovered_epoch_with_missing_position_reprocesses_it() {
        // Epoch 7 has 2 positions but only (7,0) has a WAL marker: (7,1) was
        // in-flight (or dropped) at the crash. The tail must NOT skip (7,1).
        let mut tail = BTreeSet::new();
        tail.insert((7u64, 0u32));
        let mut gate = AppliedGate::new(NOT_YET_APPLIED_EPOCH, tail);

        gate.note_expected(7, 0, 2);
        assert!(gate.is_applied(7, 0), "committed position stays skipped");
        assert!(
            !gate.is_applied(7, 1),
            "in-flight position must be re-processed, not skipped"
        );
        // The epoch does not fold until (7,1) is applied on re-processing.
        assert_eq!(gate.advance(), None);
        assert_eq!(gate.fully_applied_epoch(), NOT_YET_APPLIED_EPOCH);

        assert_eq!(gate.mark_applied(7, 1), Some(7), "now the epoch folds");
        assert_eq!(gate.tail_len(), 0);
    }

    #[test]
    fn unknown_count_epoch_folds_once_a_higher_epoch_is_seen() {
        // A batch encoded before the count field existed decodes with count 0.
        // Epoch 5 has two positions on this vShard, both delivered with count 0.
        let mut gate = empty_gate();
        gate.note_expected(5, 0, 0);
        gate.note_expected(5, 1, 0);

        // Applying both positions must NOT advance the watermark: with no count
        // and epoch 5 still the highest seen, more positions of 5 might arrive.
        assert_eq!(
            gate.mark_applied(5, 0),
            None,
            "unknown count, still highest"
        );
        assert_eq!(
            gate.mark_applied(5, 1),
            None,
            "both applied but count unknown"
        );
        assert_eq!(
            gate.fully_applied_epoch(),
            NOT_YET_APPLIED_EPOCH,
            "unknown-count epoch must not fold while it is the highest seen"
        );

        // A txn of epoch 6 arrives: delivery is epoch-ordered, so no further
        // position of epoch 5 can arrive. Epoch 5's delivered set is complete and
        // fully applied, so it folds — and all of its bookkeeping is reclaimed.
        gate.note_expected(6, 0, 0);
        assert_eq!(gate.advance(), Some(5), "seeing epoch 6 folds epoch 5");
        assert_eq!(gate.fully_applied_epoch(), 5);
        assert!(
            !gate.has_epoch_state(5),
            "folded epoch leaks no tail/expected/delivered entries"
        );
        assert_eq!(gate.tail_len(), 0, "epoch 5's tail positions are pruned");
    }

    #[test]
    fn unknown_count_epoch_does_not_fold_with_unapplied_position() {
        // Epoch 5 delivers two positions with unknown count, but only one is
        // applied — the other is still in-flight.
        let mut gate = empty_gate();
        gate.note_expected(5, 0, 0);
        gate.note_expected(5, 1, 0);
        gate.mark_applied(5, 0);

        // Even after a higher epoch is seen, the epoch must NOT fold: one of its
        // delivered positions is unapplied and re-running is forbidden.
        gate.note_expected(6, 0, 0);
        assert_eq!(gate.advance(), None, "unapplied position blocks the fold");
        assert_eq!(
            gate.fully_applied_epoch(),
            NOT_YET_APPLIED_EPOCH,
            "watermark stays below an epoch with an unapplied delivered position"
        );

        assert!(gate.is_applied(5, 0), "applied position stays skipped");
        assert!(
            !gate.is_applied(5, 1),
            "unapplied position must be re-processed, not skipped"
        );
    }

    #[test]
    fn known_count_epoch_still_folds_immediately() {
        // Regression: a post-migration epoch carries a real count (>= 1) and must
        // fold as soon as its expected positions are applied — with no need to
        // wait for a higher epoch.
        let mut gate = empty_gate();
        gate.note_expected(4, 0, 2);
        gate.note_expected(4, 1, 2);

        assert_eq!(gate.mark_applied(4, 0), None, "1 of 2 applied — no advance");
        assert_eq!(
            gate.mark_applied(4, 1),
            Some(4),
            "2 of 2 applied — folds immediately, no higher epoch required"
        );
        assert_eq!(gate.fully_applied_epoch(), 4);
        assert!(
            !gate.has_epoch_state(4),
            "folded epoch leaks no tail/expected/delivered entries"
        );
    }
}
