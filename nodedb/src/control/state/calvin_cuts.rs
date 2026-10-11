// SPDX-License-Identifier: BUSL-1.1

//! The backup cut markers each Calvin scheduler on this node passed.
//!
//! A scheduler passes a marker once every transaction delivered to it before
//! the marker finished. A backup's cut proposes a marker carrying its
//! watermark and waits here until every scheduler this node runs passed it.
//!
//! The sequencer state machine also records each marker's epoch instant
//! here: the highest epoch instant applied before the marker. Every epoch
//! before the marker has an instant at or below it, and every later epoch a
//! higher one. A graph read cut reads edge versions at or below that instant
//! once every scheduler passed the marker.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex, Weak};

use tokio::sync::Notify;

use super::SharedState;

/// The most recent markers kept. The lowest watermark is dropped first.
const MAX_RECENT_MARKERS: usize = 1024;

/// What this node knows of one recent marker.
#[derive(Debug, Default)]
struct RecentMarker {
    /// The epoch instant (ms) applied before the marker, `Some(None)` when
    /// no epoch applied, `None` until the marker applied here. The first
    /// marker applied with a watermark keeps its instant: a copy proposed
    /// again applies later in the log.
    instant: Option<Option<i64>>,
    /// The sequencer log index of the first copy of the marker applied here,
    /// `None` until one applied.
    index: Option<u64>,
    /// The vShards whose scheduler passed exactly this marker.
    passed: BTreeSet<u32>,
}

/// The local schedulers a cut waits on, and what this node knows of each
/// recent marker, by watermark.
#[derive(Debug, Default)]
pub struct CalvinCuts {
    registered: Mutex<BTreeSet<u32>>,
    recent: Mutex<BTreeMap<u64, RecentMarker>>,
    changed: Notify,
}

/// The sequencer hook that records every cut marker's epoch instant in
/// `shared`'s [`CalvinCuts`].
pub fn cut_instant_hook(shared: Weak<SharedState>) -> nodedb_cluster::calvin::CutInstantHook {
    Arc::new(move |hlc, index, instant| {
        if let Some(shared) = shared.upgrade() {
            shared.calvin.cuts.note_instant(hlc, index, instant);
        }
    })
}

impl CalvinCuts {
    /// Record the log index and the epoch instant of the marker `hlc`,
    /// unless a marker with that watermark applied first.
    pub fn note_instant(&self, hlc: u64, index: u64, instant: Option<i64>) {
        self.with_recent(hlc, |marker| {
            marker.instant.get_or_insert(instant);
            marker.index.get_or_insert(index);
        });
        self.changed.notify_waiters();
    }

    /// Wait until the marker `hlc` applied here and the scheduler of every
    /// vShard in `vshards` that runs here passed it, or `deadline`. Returns
    /// the sequencer log index of the marker's first applied copy, or `None`
    /// at the deadline.
    ///
    /// Every input sequenced below that index reached each of those
    /// schedulers before the marker, so each finished it: installed it, or
    /// dropped it under an abort verdict.
    pub async fn await_vshards_passed(
        &self,
        hlc: u64,
        vshards: &BTreeSet<u32>,
        deadline: tokio::time::Instant,
    ) -> Option<u64> {
        loop {
            // Registered before the check, so a change between the check
            // and the wait still wakes it.
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(index) = self.passed_index(hlc, vshards) {
                return Some(index);
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                return None;
            }
        }
    }

    /// The marker `hlc`'s first applied index once the scheduler of every
    /// vShard in `vshards` that runs here passed it.
    fn passed_index(&self, hlc: u64, vshards: &BTreeSet<u32>) -> Option<u64> {
        let registered = self.registered.lock().unwrap_or_else(|p| p.into_inner());
        let recent = self.recent.lock().unwrap_or_else(|p| p.into_inner());
        let marker = recent.get(&hlc)?;
        let index = marker.index?;
        vshards
            .iter()
            .filter(|vshard_id| registered.contains(vshard_id))
            .all(|vshard_id| marker.passed.contains(vshard_id))
            .then_some(index)
    }

    /// Apply `update` to the recent marker `hlc`, keeping the most recent
    /// markers only.
    fn with_recent(&self, hlc: u64, update: impl FnOnce(&mut RecentMarker)) {
        let mut recent = self.recent.lock().unwrap_or_else(|p| p.into_inner());
        update(recent.entry(hlc).or_default());
        while recent.len() > MAX_RECENT_MARKERS {
            recent.pop_first();
        }
    }

    /// The epoch instant of the marker `hlc`, once it applied here.
    pub fn instant(&self, hlc: u64) -> Option<Option<i64>> {
        self.recent
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&hlc)
            .and_then(|marker| marker.instant)
    }

    /// Wait until the marker `hlc` applied here and every scheduler passed
    /// exactly it, or `deadline`. Returns the marker's epoch instant, or
    /// `None` at the deadline.
    pub async fn await_instant(
        &self,
        hlc: u64,
        deadline: tokio::time::Instant,
    ) -> Option<Option<i64>> {
        loop {
            // Registered before the check, so a change between the check
            // and the wait still wakes it.
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if let Some(instant) = self.instant(hlc)
                && self.lagging(hlc).is_empty()
            {
                return Some(instant);
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                return None;
            }
        }
    }

    /// Register the scheduler of `vshard_id`. A cut waits on it from now on.
    pub fn register(&self, vshard_id: u32) {
        self.registered
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(vshard_id);
    }

    /// Stop waiting on the scheduler of `vshard_id`: this node left the
    /// vShard's group and its scheduler stopped. The vShard's state lives on
    /// the nodes that host it, and their cuts cover it.
    pub fn unregister(&self, vshard_id: u32) {
        self.registered
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&vshard_id);
        self.changed.notify_waiters();
    }

    /// Record that the scheduler of `vshard_id` passed the marker `hlc`.
    pub fn note_passed(&self, vshard_id: u32, hlc: u64) {
        self.with_recent(hlc, |marker| {
            marker.passed.insert(vshard_id);
        });
        self.changed.notify_waiters();
    }

    /// Whether any scheduler runs on this node.
    pub fn is_empty(&self) -> bool {
        self.registered
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .is_empty()
    }

    /// The registered vShards whose scheduler has not passed exactly the
    /// marker `hlc`. A different marker passing says nothing of this one:
    /// two cuts can place their markers in either log order.
    pub fn lagging(&self, hlc: u64) -> Vec<u32> {
        let registered: Vec<u32> = self
            .registered
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .copied()
            .collect();
        let recent = self.recent.lock().unwrap_or_else(|p| p.into_inner());
        let passed = recent.get(&hlc).map(|marker| &marker.passed);
        registered
            .into_iter()
            .filter(|vshard_id| passed.is_none_or(|passed| !passed.contains(vshard_id)))
            .collect()
    }

    /// The vShards of `vshards` whose scheduler runs here and has not passed
    /// exactly the marker `hlc`. A vShard with no scheduler here never lags.
    pub fn lagging_among(&self, hlc: u64, vshards: &[u32]) -> Vec<u32> {
        let registered = self.registered.lock().unwrap_or_else(|p| p.into_inner());
        let recent = self.recent.lock().unwrap_or_else(|p| p.into_inner());
        let passed = recent.get(&hlc).map(|marker| &marker.passed);
        vshards
            .iter()
            .copied()
            .filter(|vshard_id| registered.contains(vshard_id))
            .filter(|vshard_id| passed.is_none_or(|passed| !passed.contains(vshard_id)))
            .collect()
    }

    /// Wait until every scheduler passed the marker `hlc`, or `deadline`.
    /// Returns the vShards still lagging, empty once every one passed.
    pub async fn await_passed(&self, hlc: u64, deadline: tokio::time::Instant) -> Vec<u32> {
        loop {
            // Registered before the check, so a pass between the check and
            // the wait still wakes it.
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let lagging = self.lagging(hlc);
            if lagging.is_empty() {
                return lagging;
            }
            if tokio::time::timeout_at(deadline, changed).await.is_err() {
                return self.lagging(hlc);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_cut_waits_for_every_registered_scheduler() {
        let cuts = CalvinCuts::default();
        cuts.register(1);
        cuts.register(2);
        cuts.note_passed(1, 50);
        let soon = tokio::time::Instant::now() + std::time::Duration::from_millis(20);
        assert_eq!(cuts.await_passed(50, soon).await, vec![2]);

        cuts.note_passed(2, 50);
        let later = tokio::time::Instant::now() + std::time::Duration::from_secs(1);
        assert!(cuts.await_passed(50, later).await.is_empty());
        assert_eq!(
            cuts.lagging(55),
            vec![1, 2],
            "no scheduler passed marker 55"
        );
    }

    /// A read cut waits for the marker's instant and every scheduler's pass,
    /// and the first marker with a watermark keeps its instant.
    #[tokio::test]
    async fn a_read_cut_waits_for_the_instant_and_every_pass() {
        let cuts = CalvinCuts::default();
        cuts.register(1);
        cuts.note_passed(1, 70);
        let soon = tokio::time::Instant::now() + std::time::Duration::from_millis(20);
        assert_eq!(cuts.await_instant(70, soon).await, None, "no instant yet");

        cuts.note_instant(70, 4, Some(1_000));
        cuts.note_instant(70, 9, Some(2_000));
        let later = tokio::time::Instant::now() + std::time::Duration::from_secs(1);
        assert_eq!(cuts.await_instant(70, later).await, Some(Some(1_000)));

        // A higher marker passing first says nothing of a lower one placed
        // later in the log.
        cuts.note_instant(60, 12, Some(3_000));
        assert_eq!(cuts.lagging(60), vec![1]);
        cuts.note_passed(1, 60);
        assert!(cuts.lagging(60).is_empty());

        // A retired scheduler is no longer waited on.
        cuts.register(2);
        assert_eq!(cuts.lagging(60), vec![2]);
        cuts.unregister(2);
        assert!(cuts.lagging(60).is_empty());
    }

    /// A group's cut waits for the marker to apply and for the schedulers of
    /// that group's vShards only, and reports the first copy's index.
    #[tokio::test]
    async fn a_group_cut_waits_for_its_own_vshards() {
        let cuts = CalvinCuts::default();
        cuts.register(1);
        cuts.register(2);
        cuts.register(3);
        let group: BTreeSet<u32> = [1, 2, 9].into_iter().collect();
        let soon = || tokio::time::Instant::now() + std::time::Duration::from_millis(20);

        cuts.note_passed(1, 80);
        cuts.note_passed(2, 80);
        assert_eq!(
            cuts.await_vshards_passed(80, &group, soon()).await,
            None,
            "the marker has not applied here"
        );

        cuts.note_instant(80, 33, None);
        cuts.note_instant(80, 40, None);
        assert_eq!(
            cuts.await_vshards_passed(80, &group, soon()).await,
            Some(33),
            "vShard 3 is not in the group and vShard 9 runs no scheduler here"
        );

        let other: BTreeSet<u32> = [3].into_iter().collect();
        assert_eq!(cuts.await_vshards_passed(80, &other, soon()).await, None);
    }

    /// A group's barrier waits only on the schedulers of its vShards that
    /// run here, and only until each passed exactly the cut's marker.
    #[test]
    fn a_group_lags_on_its_own_registered_vshards() {
        let cuts = CalvinCuts::default();
        cuts.register(1);
        cuts.register(2);
        assert_eq!(cuts.lagging_among(90, &[1, 2, 9]), vec![1, 2]);
        cuts.note_passed(1, 90);
        cuts.note_passed(2, 91);
        assert_eq!(
            cuts.lagging_among(90, &[1, 2, 9]),
            vec![2],
            "vShard 2 passed another marker; vShard 9 runs no scheduler here"
        );
        cuts.note_passed(2, 90);
        assert!(cuts.lagging_among(90, &[1, 2, 9]).is_empty());
        assert!(cuts.lagging_among(95, &[]).is_empty(), "no vShard, no wait");
    }
}
