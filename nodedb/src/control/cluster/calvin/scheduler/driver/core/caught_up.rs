// SPDX-License-Identifier: BUSL-1.1

//! The scheduler's caught-up report and its fully-applied watermark.

use super::scheduler::Scheduler;
use crate::control::cluster::calvin::scheduler::NOT_YET_APPLIED_EPOCH;

impl Scheduler {
    /// Whether the scheduler has caught up to the rebuild target epoch.
    ///
    /// `rebuild_target_epoch` is seeded from the ledger's highest applied
    /// epoch (see `CalvinAppliedLedger::max_applied_epoch`):
    /// [`NOT_YET_APPLIED_EPOCH`] means the ledger holds NO applied position
    /// for this vShard — a greenfield node with no Calvin history — never a
    /// real epoch (epoch 0 with markers reports `0`, distinct from the
    /// sentinel). With nothing to rebuild, such a node is trivially caught up.
    ///
    /// `fully_applied_epoch()` is conservatively seeded to the same sentinel by
    /// recovery (the watermark only advances once the sequencer's re-fan-out
    /// supplies per-epoch expected-position counts) — it does NOT mean "nothing
    /// left to apply". Comparing `u64::MAX >= rebuild_target_epoch` will
    /// therefore report caught-up before a single epoch was actually
    /// re-applied. So: sentinel `fully_applied_epoch` is caught-up ONLY when
    /// there is genuinely no rebuild target; otherwise it must NOT be treated as
    /// "ahead of everything".
    pub fn is_caught_up(&self) -> bool {
        if self.rebuild_target_epoch == NOT_YET_APPLIED_EPOCH {
            // No Calvin history for this vShard — nothing to rebuild.
            return true;
        }
        let fully_applied = self.applied.fully_applied_epoch();
        if fully_applied == NOT_YET_APPLIED_EPOCH {
            // Nothing proven fully-applied yet, but a real target exists.
            return false;
        }
        fully_applied >= self.rebuild_target_epoch
    }

    /// Report this scheduler caught up to the readiness gate, once
    /// [`Self::is_caught_up`] holds.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn publish_caught_up(&self) {
        if self.caught_up.is_caught_up() || !self.is_caught_up() {
            return;
        }
        self.caught_up.mark_caught_up();
        tracing::info!(
            vshard_id = self.vshard_id,
            rebuild_target_epoch = self.rebuild_target_epoch,
            fully_applied_epoch = self.applied.fully_applied_epoch(),
            "calvin scheduler caught up to its rebuild target"
        );
    }

    /// Publish an advanced fully-applied watermark to the metrics gauge and the
    /// shared applied-epoch counter.
    ///
    /// `CalvinLocalState::last_applied_epoch` MUST reflect the FULLY-applied
    /// epoch, never an epoch that has only some of its positions committed.
    /// `fetch_max` keeps it monotonic across all per-vShard schedulers writing
    /// the counter.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn publish_watermark(
        &mut self,
        watermark: u64,
    ) {
        self.metrics.update_last_applied_epoch(watermark);
        self.ledger.fold(watermark);
        self.caught_up.report_watermark(watermark);
        self.shared
            .calvin
            .last_applied_epoch
            .fetch_max(watermark, std::sync::atomic::Ordering::Release);
        // A marker passes once every epoch delivered before it folded.
        self.report_passed_cuts();
        self.publish_caught_up();
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use super::*;
    use crate::control::cluster::calvin::scheduler::AppliedGate;

    use crate::control::cluster::calvin::scheduler::driver::core::test_support::build_test_scheduler;

    /// A freshly-recovered scheduler (`fully_applied_epoch` still the
    /// `NOT_YET_APPLIED_EPOCH` sentinel) with a REAL, non-zero rebuild target must
    /// NOT report caught-up. Comparing `u64::MAX >= rebuild_target_epoch`
    /// will say "caught up" before a single epoch was re-applied.
    #[tokio::test]
    async fn is_caught_up_false_when_fully_applied_is_sentinel_and_target_is_real() {
        let (mut scheduler, _dir) = build_test_scheduler(0);
        scheduler.rebuild_target_epoch = 5;
        assert_eq!(
            scheduler.applied.fully_applied_epoch(),
            NOT_YET_APPLIED_EPOCH
        );

        assert!(
            !scheduler.is_caught_up(),
            "sentinel fully_applied_epoch with a real rebuild target must not be caught up"
        );
    }

    /// Once the applied watermark advances to (or past) a real rebuild target,
    /// the scheduler correctly reports caught-up.
    #[tokio::test]
    async fn is_caught_up_true_once_fully_applied_reaches_target() {
        let (mut scheduler, _dir) = build_test_scheduler(0);
        scheduler.rebuild_target_epoch = 5;

        scheduler.applied = AppliedGate::new(5, BTreeSet::new());
        assert!(
            scheduler.is_caught_up(),
            "fully_applied_epoch == rebuild_target_epoch must be caught up"
        );

        scheduler.applied = AppliedGate::new(7, BTreeSet::new());
        assert!(
            scheduler.is_caught_up(),
            "fully_applied_epoch > rebuild_target_epoch must be caught up"
        );
    }

    /// A greenfield node with NO Calvin history at all: `recover_all_applied`
    /// seeds `max_applied_epoch` (hence `rebuild_target_epoch`) to
    /// `NOT_YET_APPLIED_EPOCH` too (see `recovery.rs`'s
    /// `greenfield_returns_sentinel_and_empty_tail` test) — this is distinct from
    /// a real target of epoch 0 (which will report `max_applied_epoch == 0`).
    /// With nothing to rebuild, the scheduler is trivially caught up even though
    /// `fully_applied_epoch` is still the sentinel.
    #[tokio::test]
    async fn is_caught_up_true_when_no_rebuild_target_exists() {
        let (mut scheduler, _dir) = build_test_scheduler(0);
        // `build_test_scheduler` defaults `rebuild_target_epoch` to `0` (a REAL
        // target) for its own catch-up-drain tests; set it to the sentinel here to
        // model the actual greenfield-recovery value.
        scheduler.rebuild_target_epoch = NOT_YET_APPLIED_EPOCH;
        assert_eq!(
            scheduler.applied.fully_applied_epoch(),
            NOT_YET_APPLIED_EPOCH
        );

        assert!(
            scheduler.is_caught_up(),
            "no rebuild target (greenfield node) must report caught-up"
        );
    }

    fn lagging(scheduler: &Scheduler) -> Vec<u32> {
        scheduler.shared.calvin.caught_up.lagging()
    }

    /// A scheduler with a real rebuild target holds the readiness gate until
    /// its published watermark reaches that target.
    #[tokio::test]
    async fn a_scheduler_lags_until_its_watermark_reaches_the_rebuild_target() {
        let (mut scheduler, _dir) = build_test_scheduler(4);
        scheduler.rebuild_target_epoch = 3;
        assert_eq!(lagging(&scheduler), vec![4]);

        scheduler.applied = AppliedGate::new(2, BTreeSet::new());
        scheduler.publish_watermark(2);
        assert_eq!(lagging(&scheduler), vec![4], "epoch 2 is below the target");

        scheduler.applied = AppliedGate::new(3, BTreeSet::new());
        scheduler.publish_watermark(3);
        assert!(lagging(&scheduler).is_empty());
    }

    /// A published watermark folds the vShard's applied ledger, which
    /// coverage, checkpoints and snapshot capture read.
    #[tokio::test]
    async fn a_published_watermark_folds_the_ledger() {
        let (mut scheduler, _dir) = build_test_scheduler(4);
        let ledger = scheduler
            .shared
            .calvin
            .applied
            .get(4)
            .expect("the scheduler's ledger");
        assert!(Arc::ptr_eq(&ledger, &scheduler.ledger));
        assert!(!ledger.is_applied(3, 9));

        scheduler.applied = AppliedGate::new(3, BTreeSet::new());
        scheduler.publish_watermark(3);

        assert!(ledger.is_applied(3, 9));
        assert!(!ledger.is_applied(4, 0));
    }

    /// With no Calvin history there is nothing to rebuild, so the scheduler
    /// reports caught up without a watermark.
    #[tokio::test]
    async fn a_scheduler_with_nothing_to_rebuild_reports_caught_up() {
        let (mut scheduler, _dir) = build_test_scheduler(4);
        scheduler.rebuild_target_epoch = NOT_YET_APPLIED_EPOCH;
        scheduler.publish_caught_up();
        assert!(lagging(&scheduler).is_empty());
    }

    /// A stopped scheduler no longer holds the readiness gate.
    #[tokio::test]
    async fn a_dropped_scheduler_leaves_the_readiness_registry() {
        let (scheduler, _dir) = build_test_scheduler(4);
        let shared = Arc::clone(&scheduler.shared);
        assert_eq!(lagging(&scheduler), vec![4]);
        drop(scheduler);
        assert!(shared.calvin.caught_up.lagging().is_empty());
    }
}
