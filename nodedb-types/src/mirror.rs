// SPDX-License-Identifier: Apache-2.0

//! Mirror catalog types shared between the catalog layer and the control plane.
//!
//! A mirror database is a continuously-updated read-only replica of a source
//! database in another cluster. Promotion to writable is one-way and permanent.
//! Exhaustive matches are required everywhere these enums are matched — no
//! `_ =>` arms.

use serde::{Deserialize, Serialize};

use crate::DatabaseId;

/// Nanoseconds in one millisecond, for the cut's HLC wall time.
const NANOS_PER_MS: u64 = 1_000_000;

/// Identifies the source of a mirror database.
///
/// Stored on `DatabaseDescriptor.mirror_origin`; the read planner and write
/// rejector consult it to enforce mirror semantics.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct MirrorOrigin {
    /// The cluster that the source database lives in.
    pub source_cluster: String,
    /// The source database being mirrored.
    pub source_database: DatabaseId,
    /// The name `MIRROR DATABASE` gave the source database.
    pub source_database_name: String,
    /// Replication mode: whether the source waits for mirror ack.
    pub mode: MirrorMode,
    /// Current mirror lifecycle status.
    pub status: MirrorStatus,
}

/// Replication mode for a mirror database.
///
/// Exhaustive matches are required everywhere this enum is matched — no
/// `_ =>` arms.
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
pub enum MirrorMode {
    /// Source waits for mirror ack before commit. Strict latency cost;
    /// not recommended cross-region.
    Sync,
    /// Mirror trails source; `MirrorCutState` gives its lag.
    Async,
}

/// Lifecycle status of a mirror database.
///
/// Exhaustive matches are required everywhere this enum is matched — no
/// `_ =>` arms.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum MirrorStatus {
    /// Initial snapshot transfer is in progress.
    Bootstrapping {
        /// Bytes of snapshot data received so far.
        bytes_done: u64,
        /// Total snapshot size in bytes (0 = unknown).
        bytes_total: u64,
    },
    /// Log replication is active; mirror is caught up within normal lag bounds.
    Following,
    /// Mirror is receiving entries but has fallen behind the lag threshold.
    /// The lag itself comes from [`MirrorCutState`], so this status does not
    /// churn the metadata log as the lag moves.
    Degraded,
    /// Source is unreachable; mirror is serving stale reads with growing lag.
    Disconnected,
    /// `ALTER DATABASE … PROMOTE` fixed its last cut. The database stays
    /// read-only until every group applied that cut.
    Promoting {
        /// The last source cut the mirror applies.
        final_cut: u64,
    },
    /// The link stopped at its last ready cut. Reads and PROMOTE stay available.
    Failed {
        /// Why the link stopped.
        failure: MirrorFailure,
    },
    /// Mirror was promoted to a writable database. Source link is severed.
    /// `mirror_origin` is retained as a historical lineage record.
    Promoted,
}

/// Why a mirror link stopped. Each variant names what an operator acts on.
///
/// Exhaustive matches are required everywhere this enum is matched — no
/// `_ =>` arms.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
    thiserror::Error,
)]
pub enum MirrorFailure {
    /// A source entry did not apply on the mirror.
    #[error("source entry {source_index} of group {source_group} did not apply: {detail}")]
    ApplyError {
        source_group: u64,
        source_index: u64,
        detail: String,
    },
    /// A source entry names a collection state the mirror catalog lacks.
    #[error(
        "source entry {source_index} of group {source_group} names a state of collection \
         '{collection}' the mirror catalog lacks"
    )]
    SchemaMismatch {
        collection: String,
        source_group: u64,
        source_index: u64,
    },
    /// A source entry does not decode on the mirror.
    #[error("source entry {source_index} of group {source_group} does not decode: {detail}")]
    PoisonEntry {
        source_group: u64,
        source_index: u64,
        detail: String,
    },
    /// The source compacted past what the mirror needs. Only an operator's
    /// rebuild clears it, because a rebuild removes the readable copy.
    #[error(
        "source group {group} retains its log from index {first_available}, past this \
         mirror's position"
    )]
    LogTruncated { group: u64, first_available: u64 },
    /// The source restored to another log timeline.
    #[error("the source cluster restored to another log timeline")]
    SourceTimelineChanged,
    /// The source database was dropped. The mirror keeps its data.
    #[error("the source database was dropped")]
    SourceDropped,
    /// The source ran an operation a mirror cannot follow.
    #[error("the source ran an operation a mirror cannot follow: {detail}")]
    UnsupportedSourceOp { detail: String },
    /// The source's retention bound passed before the bootstrap finished.
    #[error("the source's log retention bound passed before the bootstrap finished")]
    BootstrapOutrun,
    /// A capture could not install, or its install stopped part way. A partial
    /// install plus a newer capture leaves rows the source deleted, so only an
    /// operator's rebuild clears it.
    #[error("the bootstrap did not finish: {detail}")]
    BootstrapFailed { detail: String },
}

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
}

/// Whether a retention pin still holds the source's log.
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
pub enum MirrorPinState {
    /// The pin holds the log at its positions.
    Live,
    /// The pin passed its age or byte bound. It holds nothing, and its link
    /// must bootstrap again.
    Expired,
    /// The source database dropped. The pin holds nothing.
    SourceDropped,
}

/// One mirror link's hold on the source's Raft logs.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct MirrorPin {
    /// The link's id, unique on the source.
    pub link_id: u64,
    /// The allowlist name of the client that owns the link.
    pub client: String,
    /// The source database the link follows.
    pub database: u64,
    /// The last acked index per group. A group with no position is held whole.
    pub positions: Vec<MirrorGroupPosition>,
    /// Wall-clock milliseconds of the newest ack, or of the hello before one.
    pub acked_ms: u64,
    pub state: MirrorPinState,
}

impl MirrorPin {
    /// The highest index of `group_id` the log can discard for this pin. A
    /// pin that no longer holds places no bound.
    pub fn ceiling(&self, group_id: u64) -> u64 {
        match self.state {
            MirrorPinState::Live => self
                .positions
                .iter()
                .find(|position| position.group_id == group_id)
                .map_or(0, |position| position.log_index),
            MirrorPinState::Expired | MirrorPinState::SourceDropped => u64::MAX,
        }
    }

    /// `self` after `incoming`, the next committed row of the link. Every node
    /// computes it in log order, so each holds the same row.
    ///
    /// Positions and the ack time only rise, so a late ack moves nothing back.
    /// A pin that stopped holding stays stopped: the log it held is gone.
    pub fn merged(&self, incoming: &Self) -> Self {
        let mut next = self.clone();
        next.acked_ms = self.acked_ms.max(incoming.acked_ms);
        for position in &incoming.positions {
            match next
                .positions
                .iter_mut()
                .find(|held| held.group_id == position.group_id)
            {
                Some(held) => held.log_index = held.log_index.max(position.log_index),
                None => next.positions.push(*position),
            }
        }
        next.state = match (self.state, incoming.state) {
            (MirrorPinState::SourceDropped, _) | (_, MirrorPinState::SourceDropped) => {
                MirrorPinState::SourceDropped
            }
            (MirrorPinState::Expired, _) | (_, MirrorPinState::Expired) => MirrorPinState::Expired,
            (MirrorPinState::Live, MirrorPinState::Live) => MirrorPinState::Live,
        };
        next
    }
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
    use super::*;

    fn round_trip_msgpack<T>(val: &T) -> T
    where
        T: zerompk::ToMessagePack + for<'a> zerompk::FromMessagePack<'a>,
    {
        let bytes = zerompk::to_msgpack_vec(val).expect("serialize");
        zerompk::from_msgpack(&bytes).expect("deserialize")
    }

    fn round_trip_serde<T>(val: &T) -> T
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        let json = sonic_rs::to_string(val).expect("serde serialize");
        sonic_rs::from_str(&json).expect("serde deserialize")
    }

    fn sample_origin() -> MirrorOrigin {
        MirrorOrigin {
            source_cluster: "prod-us".to_string(),
            source_database: DatabaseId::DEFAULT,
            source_database_name: "orders".to_string(),
            mode: MirrorMode::Async,
            status: MirrorStatus::Following,
        }
    }

    #[test]
    fn mirror_mode_msgpack_roundtrip() {
        assert_eq!(round_trip_msgpack(&MirrorMode::Sync), MirrorMode::Sync);
        assert_eq!(round_trip_msgpack(&MirrorMode::Async), MirrorMode::Async);
    }

    #[test]
    fn mirror_mode_serde_roundtrip() {
        assert_eq!(round_trip_serde(&MirrorMode::Sync), MirrorMode::Sync);
        assert_eq!(round_trip_serde(&MirrorMode::Async), MirrorMode::Async);
    }

    #[test]
    fn mirror_status_following_msgpack() {
        let s = MirrorStatus::Following;
        assert_eq!(round_trip_msgpack(&s), s);
    }

    #[test]
    fn mirror_status_bootstrapping_msgpack() {
        let s = MirrorStatus::Bootstrapping {
            bytes_done: 1024,
            bytes_total: 4096,
        };
        assert_eq!(round_trip_msgpack(&s), s);
    }

    #[test]
    fn mirror_status_degraded_msgpack() {
        let s = MirrorStatus::Degraded;
        assert_eq!(round_trip_msgpack(&s), s);
    }

    #[test]
    fn mirror_status_promoting_msgpack() {
        let s = MirrorStatus::Promoting { final_cut: 41 };
        assert_eq!(round_trip_msgpack(&s), s);
    }

    #[test]
    fn every_mirror_failure_round_trips_inside_its_status() {
        for failure in [
            MirrorFailure::ApplyError {
                source_group: 1,
                source_index: 2,
                detail: "disk".into(),
            },
            MirrorFailure::SchemaMismatch {
                collection: "orders".into(),
                source_group: 1,
                source_index: 2,
            },
            MirrorFailure::PoisonEntry {
                source_group: 1,
                source_index: 2,
                detail: "decode".into(),
            },
            MirrorFailure::LogTruncated {
                group: 3,
                first_available: 90,
            },
            MirrorFailure::SourceTimelineChanged,
            MirrorFailure::SourceDropped,
            MirrorFailure::UnsupportedSourceOp {
                detail: "move tenant".into(),
            },
            MirrorFailure::BootstrapOutrun,
            MirrorFailure::BootstrapFailed {
                detail: "stopped during the re-issue".into(),
            },
        ] {
            let s = MirrorStatus::Failed { failure };
            assert_eq!(round_trip_msgpack(&s), s);
            assert_eq!(round_trip_serde(&s), s);
        }
    }

    #[test]
    fn a_truncated_log_failure_names_the_group_and_its_first_index() {
        let text = MirrorFailure::LogTruncated {
            group: 3,
            first_available: 90,
        }
        .to_string();
        assert!(
            text.contains("group 3") && text.contains("index 90"),
            "{text}"
        );
    }

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
        }
    }

    fn pin(positions: &[(u64, u64)], acked_ms: u64, state: MirrorPinState) -> MirrorPin {
        MirrorPin {
            link_id: 1,
            client: "dr".to_string(),
            database: 1024,
            positions: positions
                .iter()
                .map(|(group_id, log_index)| MirrorGroupPosition {
                    group_id: *group_id,
                    log_index: *log_index,
                })
                .collect(),
            acked_ms,
            state,
        }
    }

    #[test]
    fn a_live_pin_holds_a_group_it_never_acked_whole() {
        let live = pin(&[(1, 40)], 5, MirrorPinState::Live);
        assert_eq!(live.ceiling(1), 40);
        assert_eq!(live.ceiling(2), 0);
        assert_eq!(
            pin(&[(1, 40)], 5, MirrorPinState::Expired).ceiling(2),
            u64::MAX
        );
    }

    #[test]
    fn a_pin_only_rises_and_an_expiry_sticks() {
        let held = pin(&[(1, 40), (2, 9)], 5, MirrorPinState::Live);
        let late = pin(&[(1, 30), (3, 1)], 4, MirrorPinState::Live);
        let merged = held.merged(&late);
        assert_eq!(merged.ceiling(1), 40);
        assert_eq!(merged.ceiling(2), 9);
        assert_eq!(merged.ceiling(3), 1);
        assert_eq!(merged.acked_ms, 5);

        let expired = held.merged(&pin(&[], 6, MirrorPinState::Expired));
        let revived = expired.merged(&pin(&[(1, 50)], 7, MirrorPinState::Live));
        assert_eq!(revived.state, MirrorPinState::Expired);
        assert_eq!(
            revived
                .merged(&pin(&[], 8, MirrorPinState::SourceDropped))
                .state,
            MirrorPinState::SourceDropped
        );
        assert_eq!(round_trip_msgpack(&revived), revived);
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

    #[test]
    fn mirror_status_disconnected_msgpack() {
        let s = MirrorStatus::Disconnected;
        assert_eq!(round_trip_msgpack(&s), s);
    }

    #[test]
    fn mirror_status_promoted_msgpack() {
        let s = MirrorStatus::Promoted;
        assert_eq!(round_trip_msgpack(&s), s);
    }

    #[test]
    fn mirror_status_serde_roundtrip() {
        for s in [
            MirrorStatus::Following,
            MirrorStatus::Bootstrapping {
                bytes_done: 0,
                bytes_total: 0,
            },
            MirrorStatus::Degraded,
            MirrorStatus::Disconnected,
            MirrorStatus::Promoting { final_cut: 9 },
            MirrorStatus::Promoted,
        ] {
            assert_eq!(round_trip_serde(&s), s);
        }
    }

    #[test]
    fn mirror_origin_msgpack_roundtrip() {
        let o = sample_origin();
        assert_eq!(round_trip_msgpack(&o), o);
    }

    #[test]
    fn mirror_origin_serde_roundtrip() {
        let o = sample_origin();
        assert_eq!(round_trip_serde(&o), o);
    }
}
