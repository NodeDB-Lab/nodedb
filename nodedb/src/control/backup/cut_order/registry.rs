// SPDX-License-Identifier: BUSL-1.1

//! Where a node tracks each ordered cut's barrier per data group.
//!
//! For every cut it knows, the node keeps, per group:
//!
//! - where it proposed the cut's barrier while it led the group, as the
//!   leader term and log index;
//! - the log index of the first barrier of the cut it applied in the group.
//!
//! A leader's schedulers hold the redo of every transaction sequenced after
//! a cut's marker until the group's barrier applied here. An applied entry is
//! committed, so every redo proposed afterwards sits after it in the log,
//! whatever term the proposer leads.
//!
//! One driver task per cut runs on a node (see [`super::driver`]). It
//! proposes the barrier of every group this node leads, and proposes it
//! again when a later term overwrote the proposal.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use tokio::sync::Notify;
use tokio::sync::futures::Notified;

use super::ordered_cut::{CutKey, OrderedCut};

/// How long a node remembers a cut no driver runs for. Every hold and every
/// wait on a cut ends within its window, far below this.
const CUT_TTL: Duration = Duration::from_secs(30 * 60);

/// Where this node proposed a cut's barrier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Proposed {
    /// The group's leader term the proposal was made in.
    pub term: u64,
    pub log_index: u64,
}

#[derive(Debug, Default, Clone, Copy)]
struct GroupBarrier {
    proposed: Option<Proposed>,
    applied: Option<u64>,
}

#[derive(Debug)]
struct TrackedCut {
    cut: OrderedCut,
    groups: BTreeMap<u64, GroupBarrier>,
    driving: bool,
    touched: Instant,
}

impl TrackedCut {
    fn new(cut: &OrderedCut, now: Instant) -> Self {
        Self {
            cut: cut.clone(),
            groups: BTreeMap::new(),
            driving: false,
            touched: now,
        }
    }
}

/// Every ordered cut this node knows, by key.
#[derive(Debug, Default)]
pub struct CutBarriers {
    cuts: Mutex<BTreeMap<CutKey, TrackedCut>>,
    changed: Notify,
}

impl CutBarriers {
    pub fn new() -> Self {
        Self::default()
    }

    /// Track `cut` and claim its driver. `true` when no driver ran for it:
    /// the caller starts one.
    pub(crate) fn claim_driver(&self, cut: &OrderedCut) -> bool {
        self.with_cut(cut, |tracked| {
            !std::mem::replace(&mut tracked.driving, true)
        })
    }

    /// The driver of `key` stopped. A later claim starts a new one.
    pub(crate) fn driver_stopped(&self, key: CutKey) {
        let mut cuts = self.cuts.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(tracked) = cuts.get_mut(&key) {
            tracked.driving = false;
            tracked.touched = Instant::now();
        }
    }

    /// The cut `key` names, while this node tracks it.
    pub(crate) fn cut(&self, key: CutKey) -> Option<OrderedCut> {
        self.cuts
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&key)
            .map(|tracked| tracked.cut.clone())
    }

    /// Note a barrier of `cut` this node applies at `log_index` of `group`.
    /// The first barrier of the cut in the group keeps its index.
    pub(crate) fn note_applied(&self, cut: &OrderedCut, group: u64, log_index: u64) {
        self.with_cut(cut, |tracked| {
            let barrier = tracked.groups.entry(group).or_default();
            barrier.applied.get_or_insert(log_index);
        });
        self.changed.notify_waiters();
    }

    /// The log index of the first barrier of `key` this node applied in
    /// `group`.
    pub(crate) fn applied(&self, key: CutKey, group: u64) -> Option<u64> {
        self.group(key, group).and_then(|barrier| barrier.applied)
    }

    /// Note that this node proposed `key`'s barrier into `group` while it
    /// led the group.
    pub(crate) fn note_proposed(&self, key: CutKey, group: u64, proposed: Proposed) {
        {
            let mut cuts = self.cuts.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(tracked) = cuts.get_mut(&key) {
                tracked.groups.entry(group).or_default().proposed = Some(proposed);
            }
        }
        self.changed.notify_waiters();
    }

    /// Where this node proposed `key`'s barrier into `group`.
    pub(crate) fn proposal(&self, key: CutKey, group: u64) -> Option<Proposed> {
        self.group(key, group).and_then(|barrier| barrier.proposed)
    }

    /// Forget the proposal `lost` of `key` in `group`: a later term
    /// overwrote it. A newer proposal stays.
    pub(crate) fn forget_proposal(&self, key: CutKey, group: u64, lost: Proposed) {
        let mut cuts = self.cuts.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(barrier) = cuts
            .get_mut(&key)
            .and_then(|tracked| tracked.groups.get_mut(&group))
            && barrier.proposed == Some(lost)
        {
            barrier.proposed = None;
        }
    }

    /// A wait that resolves at the next change of any cut. Enable it before
    /// the check it guards.
    pub(crate) fn notified(&self) -> Notified<'_> {
        self.changed.notified()
    }

    fn group(&self, key: CutKey, group: u64) -> Option<GroupBarrier> {
        self.cuts
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&key)
            .and_then(|tracked| tracked.groups.get(&group).copied())
    }

    /// Apply `update` to the tracked `cut`, tracking it when new. Drops every
    /// cut past its TTL that no driver runs for.
    fn with_cut<T>(&self, cut: &OrderedCut, update: impl FnOnce(&mut TrackedCut) -> T) -> T {
        let now = Instant::now();
        let mut cuts = self.cuts.lock().unwrap_or_else(|p| p.into_inner());
        cuts.retain(|_, tracked| {
            tracked.driving || now.saturating_duration_since(tracked.touched) < CUT_TTL
        });
        let tracked = cuts
            .entry(cut.key())
            .or_insert_with(|| TrackedCut::new(cut, now));
        tracked.touched = now;
        update(tracked)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cut(hlc: u64) -> OrderedCut {
        OrderedCut {
            hlc,
            restore_point: 0,
            capture: None,
        }
    }

    #[test]
    fn one_driver_runs_per_cut() {
        let barriers = CutBarriers::new();
        assert!(barriers.claim_driver(&cut(5)));
        assert!(!barriers.claim_driver(&cut(5)), "a copy finds the driver");
        assert!(barriers.claim_driver(&cut(6)), "another cut has its own");
        barriers.driver_stopped(cut(5).key());
        assert!(
            barriers.claim_driver(&cut(5)),
            "a stopped driver starts again"
        );
    }

    #[test]
    fn the_first_applied_barrier_keeps_its_index() {
        let barriers = CutBarriers::new();
        let key = cut(5).key();
        assert_eq!(barriers.applied(key, 1), None);
        barriers.note_applied(&cut(5), 1, 40);
        barriers.note_applied(&cut(5), 1, 52);
        assert_eq!(barriers.applied(key, 1), Some(40));
        assert_eq!(barriers.applied(key, 2), None, "a barrier binds its group");
        assert_eq!(barriers.cut(key), Some(cut(5)));
    }

    /// A lost proposal is forgotten only when it is the one recorded.
    #[test]
    fn a_lost_proposal_is_forgotten_once() {
        let barriers = CutBarriers::new();
        let key = cut(5).key();
        barriers.claim_driver(&cut(5));
        assert_eq!(barriers.proposal(key, 1), None);

        let proposed = Proposed {
            term: 3,
            log_index: 17,
        };
        barriers.note_proposed(key, 1, proposed);
        assert_eq!(barriers.proposal(key, 1), Some(proposed));
        assert_eq!(barriers.proposal(key, 2), None, "another group");

        barriers.forget_proposal(
            key,
            1,
            Proposed {
                term: 2,
                log_index: 9,
            },
        );
        assert_eq!(
            barriers.proposal(key, 1),
            Some(proposed),
            "an older loss keeps the newer proposal"
        );
        barriers.forget_proposal(key, 1, proposed);
        assert_eq!(barriers.proposal(key, 1), None);
    }

    #[tokio::test]
    async fn an_applied_barrier_wakes_a_waiter() {
        let barriers = std::sync::Arc::new(CutBarriers::new());
        let waiter = std::sync::Arc::clone(&barriers);
        let woken = tokio::spawn(async move {
            let notified = waiter.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if waiter.applied(cut(5).key(), 1).is_none() {
                notified.await;
            }
            waiter.applied(cut(5).key(), 1)
        });
        tokio::task::yield_now().await;
        barriers.note_applied(&cut(5), 1, 8);
        let applied = tokio::time::timeout(Duration::from_secs(5), woken)
            .await
            .expect("the waiter wakes")
            .expect("the waiter runs");
        assert_eq!(applied, Some(8));
    }
}
