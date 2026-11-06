// SPDX-License-Identifier: Apache-2.0

use serde::{Deserialize, Serialize};

use super::failure::MirrorFailure;

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
    ///
    /// [`MirrorCutState`]: super::cut::MirrorCutState
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

/// One status change a mirror's metadata log decides at apply.
///
/// The log orders each step against every `MirrorCut`, so every node reaches
/// one answer. A step the row no longer permits changes nothing.
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
pub enum MirrorStatusStep {
    /// The lag monitor moves `from` to `to`, while the row still holds `from`.
    Lag {
        from: MirrorStatus,
        to: MirrorStatus,
    },
    /// PROMOTE fixes the last cut at the row's `ready_cut`.
    Seal,
    /// PROMOTE ends: the database becomes writable.
    Promote,
    /// A bootstrap reports how far its transfer got.
    Progress { bytes_done: u64, bytes_total: u64 },
    /// A bootstrap installed its capture, and the mirror follows its source.
    Installed,
    /// The link stopped on a failure an operator acts on.
    Fail { failure: MirrorFailure },
    /// `ALTER DATABASE ... MIRROR RESUME`: the link retries from its positions.
    Resume,
    /// `ALTER DATABASE ... MIRROR REBOOTSTRAP`: the mirror starts over. Apply
    /// also clears the cut state, keeping only the fence.
    Rebootstrap,
}

impl MirrorStatusStep {
    /// The status this step leaves on a row at `current` with `ready_cut`,
    /// or `None` when the step changes nothing.
    pub fn next(&self, current: &MirrorStatus, ready_cut: u64) -> Option<MirrorStatus> {
        match self {
            Self::Progress {
                bytes_done,
                bytes_total,
            } => matches!(current, MirrorStatus::Bootstrapping { .. }).then_some(
                MirrorStatus::Bootstrapping {
                    bytes_done: *bytes_done,
                    bytes_total: *bytes_total,
                },
            ),
            Self::Installed => matches!(current, MirrorStatus::Bootstrapping { .. })
                .then_some(MirrorStatus::Following),
            Self::Fail { failure } => match current {
                MirrorStatus::Bootstrapping { .. }
                | MirrorStatus::Following
                | MirrorStatus::Degraded
                | MirrorStatus::Disconnected => Some(MirrorStatus::Failed {
                    failure: failure.clone(),
                }),
                MirrorStatus::Promoting { .. }
                | MirrorStatus::Failed { .. }
                | MirrorStatus::Promoted => None,
            },
            Self::Resume => match current {
                MirrorStatus::Failed { failure } if failure.is_resumable() && ready_cut > 0 => {
                    Some(MirrorStatus::Following)
                }
                MirrorStatus::Bootstrapping { .. }
                | MirrorStatus::Following
                | MirrorStatus::Degraded
                | MirrorStatus::Disconnected
                | MirrorStatus::Promoting { .. }
                | MirrorStatus::Failed { .. }
                | MirrorStatus::Promoted => None,
            },
            Self::Rebootstrap => matches!(current, MirrorStatus::Failed { .. }).then_some(
                MirrorStatus::Bootstrapping {
                    bytes_done: 0,
                    bytes_total: 0,
                },
            ),
            Self::Lag { from, to } => {
                (from == current && is_lag_status(from) && is_lag_status(to)).then(|| to.clone())
            }
            Self::Seal => match current {
                MirrorStatus::Following
                | MirrorStatus::Degraded
                | MirrorStatus::Disconnected
                | MirrorStatus::Failed { .. }
                    if ready_cut > 0 =>
                {
                    Some(MirrorStatus::Promoting {
                        final_cut: ready_cut,
                    })
                }
                MirrorStatus::Bootstrapping { .. }
                | MirrorStatus::Following
                | MirrorStatus::Degraded
                | MirrorStatus::Disconnected
                | MirrorStatus::Failed { .. }
                | MirrorStatus::Promoting { .. }
                | MirrorStatus::Promoted => None,
            },
            Self::Promote => match current {
                MirrorStatus::Promoting { .. } => Some(MirrorStatus::Promoted),
                MirrorStatus::Bootstrapping { .. }
                | MirrorStatus::Following
                | MirrorStatus::Degraded
                | MirrorStatus::Disconnected
                | MirrorStatus::Failed { .. }
                | MirrorStatus::Promoted => None,
            },
        }
    }
}

/// Whether the lag monitor owns this status.
fn is_lag_status(status: &MirrorStatus) -> bool {
    match status {
        MirrorStatus::Following | MirrorStatus::Degraded | MirrorStatus::Disconnected => true,
        MirrorStatus::Bootstrapping { .. }
        | MirrorStatus::Promoting { .. }
        | MirrorStatus::Failed { .. }
        | MirrorStatus::Promoted => false,
    }
}

#[cfg(test)]
mod tests {
    use super::super::test_support::{round_trip_msgpack, round_trip_serde};
    use super::*;

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

    fn failed() -> MirrorStatus {
        MirrorStatus::Failed {
            failure: MirrorFailure::SourceDropped,
        }
    }

    /// A lag step acts only on the status it read, so it never undoes a seal.
    #[test]
    fn a_lag_step_needs_the_status_it_read() {
        let step = MirrorStatusStep::Lag {
            from: MirrorStatus::Following,
            to: MirrorStatus::Degraded,
        };
        assert_eq!(
            step.next(&MirrorStatus::Following, 5),
            Some(MirrorStatus::Degraded)
        );
        for later in [
            MirrorStatus::Disconnected,
            MirrorStatus::Promoting { final_cut: 5 },
            MirrorStatus::Promoted,
            failed(),
        ] {
            assert_eq!(step.next(&later, 5), None, "{later:?}");
        }
    }

    #[test]
    fn a_lag_step_only_moves_between_lag_statuses() {
        let step = MirrorStatusStep::Lag {
            from: MirrorStatus::Promoted,
            to: MirrorStatus::Following,
        };
        assert_eq!(step.next(&MirrorStatus::Promoted, 5), None);
        let step = MirrorStatusStep::Lag {
            from: MirrorStatus::Following,
            to: MirrorStatus::Promoted,
        };
        assert_eq!(step.next(&MirrorStatus::Following, 5), None);
    }

    /// The final cut is the `ready_cut` the step finds: nothing else can name it.
    #[test]
    fn a_seal_takes_the_ready_cut_it_finds() {
        let expected = Some(MirrorStatus::Promoting { final_cut: 7 });
        for status in [
            MirrorStatus::Following,
            MirrorStatus::Degraded,
            MirrorStatus::Disconnected,
            failed(),
        ] {
            assert_eq!(
                MirrorStatusStep::Seal.next(&status, 7),
                expected,
                "{status:?}"
            );
        }
    }

    #[test]
    fn a_seal_needs_a_ready_cut_and_never_moves_a_sealed_mirror() {
        let bootstrapping = MirrorStatus::Bootstrapping {
            bytes_done: 0,
            bytes_total: 0,
        };
        assert_eq!(MirrorStatusStep::Seal.next(&bootstrapping, 3), None);
        assert_eq!(
            MirrorStatusStep::Seal.next(&MirrorStatus::Following, 0),
            None
        );
        let sealed = MirrorStatus::Promoting { final_cut: 3 };
        assert_eq!(MirrorStatusStep::Seal.next(&sealed, 9), None);
        assert_eq!(
            MirrorStatusStep::Seal.next(&MirrorStatus::Promoted, 9),
            None
        );
    }

    #[test]
    fn a_bootstrap_alone_reports_progress_and_installs() {
        let bootstrapping = MirrorStatus::Bootstrapping {
            bytes_done: 1,
            bytes_total: 2,
        };
        let progress = MirrorStatusStep::Progress {
            bytes_done: 2,
            bytes_total: 2,
        };
        assert_eq!(
            progress.next(&bootstrapping, 0),
            Some(MirrorStatus::Bootstrapping {
                bytes_done: 2,
                bytes_total: 2,
            })
        );
        assert_eq!(
            MirrorStatusStep::Installed.next(&bootstrapping, 0),
            Some(MirrorStatus::Following)
        );
        for status in [
            MirrorStatus::Following,
            MirrorStatus::Promoting { final_cut: 3 },
            MirrorStatus::Promoted,
            failed(),
        ] {
            assert_eq!(progress.next(&status, 3), None, "{status:?}");
            assert_eq!(MirrorStatusStep::Installed.next(&status, 3), None);
        }
    }

    #[test]
    fn a_failure_keeps_the_first_and_never_undoes_a_seal() {
        let step = MirrorStatusStep::Fail {
            failure: MirrorFailure::SourceTimelineChanged,
        };
        assert_eq!(
            step.next(&MirrorStatus::Following, 3),
            Some(MirrorStatus::Failed {
                failure: MirrorFailure::SourceTimelineChanged,
            })
        );
        assert_eq!(step.next(&failed(), 3), None);
        assert_eq!(
            step.next(&MirrorStatus::Promoting { final_cut: 3 }, 3),
            None
        );
        assert_eq!(step.next(&MirrorStatus::Promoted, 3), None);
    }

    #[test]
    fn resume_needs_a_resumable_failure_and_a_ready_cut() {
        let resumable = MirrorStatus::Failed {
            failure: MirrorFailure::UnsupportedSourceOp {
                detail: "a move".to_string(),
            },
        };
        assert_eq!(
            MirrorStatusStep::Resume.next(&resumable, 3),
            Some(MirrorStatus::Following)
        );
        assert_eq!(MirrorStatusStep::Resume.next(&resumable, 0), None);
        for fatal in [
            MirrorFailure::SourceDropped,
            MirrorFailure::SourceTimelineChanged,
            MirrorFailure::LogTruncated {
                group: 1,
                first_available: 9,
            },
            MirrorFailure::BootstrapFailed {
                detail: "a stop".to_string(),
            },
        ] {
            let status = MirrorStatus::Failed { failure: fatal };
            assert_eq!(MirrorStatusStep::Resume.next(&status, 3), None);
            assert_eq!(
                MirrorStatusStep::Rebootstrap.next(&status, 3),
                Some(MirrorStatus::Bootstrapping {
                    bytes_done: 0,
                    bytes_total: 0,
                })
            );
        }
    }

    #[test]
    fn only_a_failed_mirror_rebuilds() {
        for status in [
            MirrorStatus::Following,
            MirrorStatus::Degraded,
            MirrorStatus::Promoting { final_cut: 3 },
            MirrorStatus::Promoted,
        ] {
            assert_eq!(MirrorStatusStep::Rebootstrap.next(&status, 3), None);
        }
    }

    #[test]
    fn only_a_sealed_mirror_promotes() {
        let sealed = MirrorStatus::Promoting { final_cut: 3 };
        assert_eq!(
            MirrorStatusStep::Promote.next(&sealed, 3),
            Some(MirrorStatus::Promoted)
        );
        assert_eq!(
            MirrorStatusStep::Promote.next(&MirrorStatus::Following, 3),
            None
        );
        assert_eq!(
            MirrorStatusStep::Promote.next(&MirrorStatus::Promoted, 3),
            None
        );
    }

    #[test]
    fn a_status_step_round_trips() {
        let step = MirrorStatusStep::Lag {
            from: MirrorStatus::Degraded,
            to: MirrorStatus::Following,
        };
        assert_eq!(round_trip_msgpack(&step), step);
        assert_eq!(round_trip_serde(&step), step);
    }
}
