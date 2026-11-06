// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

/// Nanoseconds in one millisecond, for the cut's HLC wall time.
const NANOS_PER_MS: u64 = 1_000_000;

/// A mirror's place in one source Raft group: every entry at or below
/// `log_index` belongs to an applied cut.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct MirrorGroupPosition {
    /// The source Raft group.
    pub group_id: u64,
    /// The last source log index this position covers.
    pub log_index: u64,
}

/// How far a mirror followed its source, persisted in
/// `_system.mirror_cut_state`.
///
/// Every node holds the same row, because the metadata group replicates it.
#[derive(
    Debug,
    Clone,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct MirrorCutState {
    /// The newest source cut whose entries committed in every mirror group.
    pub ready_cut: u64,
    /// The newest source cut every mirror group applied. `0` for none.
    pub applied_cut: u64,
    /// HLC wall time of `applied_cut` on the source, in nanoseconds.
    pub applied_cut_hlc_ns: u64,
    /// The newest system-time ordinal `applied_cut` holds in full.
    pub cap_ordinal: i64,
    /// Where each source group resumes after `ready_cut`.
    pub positions: Vec<MirrorGroupPosition>,
    /// Which applier epoch owns each range of cuts, oldest first.
    pub epochs: Vec<MirrorEpochRange>,
    /// The newest applier epoch the metadata log took. A lower epoch never
    /// raises `ready_cut`.
    pub fence_epoch: u64,
    /// The source's metadata timeline when its capture was taken. `None` until
    /// a capture installs. Another timeline means the source restored.
    pub source_timeline: Option<u64>,
}

/// The cuts from `first_cut` up to the next range's start, and the applier
/// epoch whose stamped entries apply for them.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct MirrorEpochRange {
    /// The first cut of the range.
    pub first_cut: u64,
    /// The applier epoch that owns the range.
    pub epoch: u64,
}

/// What one `MirrorCut` metadata entry asks of a mirror's cut state.
///
/// `cut` above `ready_cut` raises it. `cut` equal to `ready_cut` raises only
/// the epoch fence and the applied fields, which releases a former applier's
/// partial cut.
#[derive(
    Debug,
    Clone,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct MirrorCutAdvance {
    /// The cut whose entries committed in every mirror group.
    pub cut: u64,
    /// The epoch of the applier that proposed the entry.
    pub epoch: u64,
    /// The newest cut every mirror group applied.
    pub applied_cut: u64,
    /// HLC wall time of `applied_cut` on the source, in nanoseconds.
    pub applied_cut_hlc_ns: u64,
    /// The newest system-time ordinal `applied_cut` holds in full.
    pub cap_ordinal: i64,
    /// Where each source group resumes after `cut`.
    pub positions: Vec<MirrorGroupPosition>,
    /// The source's metadata timeline, on a bootstrap's first cut.
    pub source_timeline: Option<u64>,
}

/// What a source's cut tells its mirrors, in the cut's Calvin marker and in
/// every group's barrier.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct MirrorCutMark {
    /// The cut's number. Its high half is the metadata term that placed it,
    /// so a later leader's cuts number above every earlier leader's.
    pub seq: u64,
    /// The newest system-time ordinal the cut holds in full.
    pub cap_ordinal: i64,
    /// The source's metadata index the cut's schema covers.
    pub metadata_index: u64,
    /// The source's surrogate high-water mark when it took the cut.
    pub surrogate_hwm: u32,
}

/// Where a mirror entry came from and which cut holds it. The mirror's applier
/// stamps it on every entry it proposes.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct MirrorEntryStamp {
    /// The source Raft group of the entry. `0` for an entry the applier wrote.
    pub source_group: u64,
    /// The entry's source log index. `0` for an entry the applier wrote.
    pub source_index: u64,
    /// The source cut the entry belongs to.
    pub cut: u64,
    /// The epoch of the applier that proposed the entry.
    pub epoch: u64,
}

impl MirrorCutState {
    /// The applier epoch whose stamped entries apply for `cut`, or `None` for
    /// a cut no `MirrorCut` raised `ready_cut` over.
    pub fn epoch_of(&self, cut: u64) -> Option<u64> {
        if cut > self.ready_cut {
            return None;
        }
        self.epochs
            .iter()
            .rev()
            .find(|range| range.first_cut <= cut)
            .map(|range| range.epoch)
    }

    /// The state after `advance`, or `None` when it changes nothing.
    ///
    /// Every replica computes it from the same row, in log order, so each
    /// reaches the same state. An epoch below the fence or a cut below
    /// `ready_cut` is a former applier's and changes nothing.
    pub fn advanced(&self, advance: &MirrorCutAdvance) -> Option<Self> {
        if advance.epoch < self.fence_epoch || advance.cut < self.ready_cut {
            return None;
        }
        let mut next = self.clone();
        next.fence_epoch = advance.epoch;
        if advance.cut > self.ready_cut {
            next.ready_cut = advance.cut;
            next.positions.clone_from(&advance.positions);
            if self.epochs.last().map(|range| range.epoch) != Some(advance.epoch) {
                next.epochs.push(MirrorEpochRange {
                    first_cut: self.ready_cut.saturating_add(1),
                    epoch: advance.epoch,
                });
            }
        }
        if advance.applied_cut > self.applied_cut {
            next.applied_cut = advance.applied_cut;
            next.applied_cut_hlc_ns = advance.applied_cut_hlc_ns;
            next.cap_ordinal = advance.cap_ordinal;
        }
        if advance.source_timeline.is_some() {
            next.source_timeline = advance.source_timeline;
        }
        (next != *self).then_some(next)
    }

    /// Milliseconds since the UNIX epoch of the newest applied cut, or `None`
    /// when no cut applied.
    pub fn applied_cut_ms(&self) -> Option<u64> {
        (self.applied_cut != 0).then_some(self.applied_cut_hlc_ns / NANOS_PER_MS)
    }

    /// How far the mirror trails the source at `now_ms`, or `None` when no
    /// cut applied. An idle source still places cuts, so idle is not stale.
    pub fn lag_ms(&self, now_ms: u64) -> Option<u64> {
        self.applied_cut_ms()
            .map(|cut_ms| now_ms.saturating_sub(cut_ms))
    }

    /// Whether `self` is at or past `other`. A replay or a former applier's
    /// late state is behind, and writes nothing.
    pub fn is_at_or_past(&self, other: &Self) -> bool {
        (self.ready_cut, self.applied_cut, self.fence_epoch)
            >= (other.ready_cut, other.applied_cut, other.fence_epoch)
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::{round_trip_msgpack, round_trip_serde};
    use super::*;

    fn cut(ready_cut: u64, applied_cut: u64) -> MirrorCutState {
        MirrorCutState {
            ready_cut,
            applied_cut,
            applied_cut_hlc_ns: 5_000 * NANOS_PER_MS,
            cap_ordinal: 7,
            positions: vec![MirrorGroupPosition {
                group_id: 1,
                log_index: 12,
            }],
            epochs: vec![MirrorEpochRange {
                first_cut: 1,
                epoch: 2,
            }],
            fence_epoch: 2,
            source_timeline: Some(3),
        }
    }

    fn advance(cut: u64, epoch: u64) -> MirrorCutAdvance {
        MirrorCutAdvance {
            cut,
            epoch,
            ..MirrorCutAdvance::default()
        }
    }

    /// A new epoch owns every cut above `ready_cut`, so a former applier's
    /// copy of a cut the new one skipped over names the wrong epoch.
    #[test]
    fn each_epoch_owns_its_range_of_cuts() {
        let first = MirrorCutState::default()
            .advanced(&advance(3, 2))
            .expect("raises");
        let second = first.advanced(&advance(6, 5)).expect("raises");
        assert_eq!(second.ready_cut, 6);
        assert_eq!(second.epoch_of(3), Some(2));
        assert_eq!(second.epoch_of(4), Some(5));
        assert_eq!(second.epoch_of(6), Some(5));
        assert_eq!(second.epoch_of(7), None, "no cut raised it yet");
    }

    #[test]
    fn a_stale_epoch_or_a_cut_below_ready_changes_nothing() {
        let state = MirrorCutState::default()
            .advanced(&advance(3, 5))
            .expect("raises");
        assert_eq!(
            state.advanced(&advance(9, 4)),
            None,
            "epoch below the fence"
        );
        assert_eq!(state.advanced(&advance(2, 5)), None, "cut below ready");
        assert_eq!(state.advanced(&advance(3, 5)), None, "a replay");
    }

    /// A new applier fences at `ready_cut`: the cut stays and the epoch moves,
    /// and no range opens until it raises a cut.
    #[test]
    fn a_fence_raises_the_epoch_and_no_cut() {
        let state = MirrorCutState::default()
            .advanced(&advance(3, 2))
            .expect("raises");
        let fenced = state.advanced(&advance(3, 7)).expect("fences");
        assert_eq!(fenced.ready_cut, 3);
        assert_eq!(fenced.fence_epoch, 7);
        assert_eq!(fenced.epoch_of(3), Some(2));
        assert!(fenced.is_at_or_past(&state));
        assert!(!state.is_at_or_past(&fenced));
    }

    #[test]
    fn the_applied_fields_only_rise() {
        let state = MirrorCutState::default()
            .advanced(&MirrorCutAdvance {
                applied_cut: 2,
                applied_cut_hlc_ns: 20,
                cap_ordinal: 200,
                ..advance(3, 1)
            })
            .expect("raises");
        let late = state.advanced(&MirrorCutAdvance {
            applied_cut: 1,
            applied_cut_hlc_ns: 10,
            cap_ordinal: 100,
            ..advance(4, 1)
        });
        let late = late.expect("raises the cut");
        assert_eq!(late.applied_cut, 2);
        assert_eq!(late.applied_cut_hlc_ns, 20);
        assert_eq!(late.cap_ordinal, 200);
    }

    #[test]
    fn cut_state_round_trips() {
        let state = cut(4, 3);
        assert_eq!(round_trip_msgpack(&state), state);
        assert_eq!(round_trip_serde(&state), state);
    }

    #[test]
    fn lag_is_measured_from_the_applied_cut() {
        assert_eq!(cut(4, 3).lag_ms(5_250), Some(250));
        assert_eq!(cut(4, 3).lag_ms(4_000), Some(0), "a skewed clock saturates");
    }

    #[test]
    fn no_applied_cut_has_no_lag() {
        assert_eq!(cut(4, 0).lag_ms(5_250), None);
        assert_eq!(cut(4, 0).applied_cut_ms(), None);
    }

    #[test]
    fn cut_state_orders_by_ready_then_applied() {
        assert!(cut(4, 3).is_at_or_past(&cut(4, 3)));
        assert!(cut(4, 3).is_at_or_past(&cut(4, 2)));
        assert!(cut(5, 0).is_at_or_past(&cut(4, 3)));
        assert!(!cut(3, 3).is_at_or_past(&cut(4, 3)));
    }
}
