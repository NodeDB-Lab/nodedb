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
}

impl MirrorCutState {
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
        (self.ready_cut, self.applied_cut) >= (other.ready_cut, other.applied_cut)
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
        }
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
