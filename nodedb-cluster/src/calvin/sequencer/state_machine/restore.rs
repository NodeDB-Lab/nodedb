// SPDX-License-Identifier: BUSL-1.1

//! The sequencer entries a cut and a cluster restore write: the cut marker a
//! backup or restore point takes, and the epoch floor a restored log opens
//! with.

use std::sync::Arc;

use tracing::warn;

use crate::calvin::types::{CutBarrierWire, SchedulerInput};

use super::core::{Delivery, NOT_YET_APPLIED, SequencerRestorePoint, SequencerStateMachine};

/// The fields of one applied `SequencerEntry::CutMarker`.
pub(super) struct CutMarkerEntry {
    pub hlc: u64,
    pub restore_point: u64,
    pub barrier: Option<CutBarrierWire>,
    /// The ordinal the marker's cut must cover, `0` for none.
    pub floor: i64,
}

impl SequencerStateMachine {
    /// A restored sequencer log opens with the epoch that followed its
    /// restore point. No epoch below it is minted again, and no epoch
    /// instant at or below `epoch_system_ms` (`0` for none).
    pub(super) fn apply_epoch_floor(&mut self, next_epoch: u64, epoch_system_ms: i64) {
        if let Some(floor) = next_epoch.checked_sub(1)
            && (self.last_applied_epoch == NOT_YET_APPLIED || self.last_applied_epoch < floor)
        {
            self.last_applied_epoch = floor;
        }
        if epoch_system_ms > 0 {
            self.last_epoch_system_ms = self.last_epoch_system_ms.max(Some(epoch_system_ms));
        }
    }

    /// Fold the marker's floor into the applied epoch instant, record a
    /// restore point's place in this log, then fan the cut marker out to
    /// every vShard scheduler this node hosts. Same delivery as
    /// `ReserveRead`: a marker for an armed vShard, or one dropped at a full
    /// channel, is replayed by the scheduler's catch-up drain in log order.
    /// Every scheduler shares one copy of the marker's `barrier`.
    ///
    /// The fold runs first, so the instant the marker records covers its
    /// floor. Every replica applies the same log, so every replica records
    /// the same instant. A leader seeds every later epoch instant above the
    /// applied one.
    pub(super) fn apply_cut_marker(&mut self, index: u64, marker: CutMarkerEntry) {
        let CutMarkerEntry {
            hlc,
            restore_point,
            barrier,
            floor,
        } = marker;
        if floor > 0 {
            let floor_ms = nodedb_types::ordinal_to_ms(floor);
            self.last_epoch_system_ms = self.last_epoch_system_ms.max(Some(floor_ms));
        }
        if restore_point != 0
            && let Some(hook) = &self.restore_point_hook
        {
            hook(SequencerRestorePoint {
                id: restore_point,
                hlc,
                index,
                next_epoch: self.next_epoch(),
                epoch_system_ms: self.last_epoch_system_ms,
            });
        }
        // Recorded before the marker reaches any scheduler, so a scheduler
        // that passed the marker implies its instant is recorded.
        if let Some(hook) = &self.cut_instant_hook {
            hook(hlc, index, self.last_epoch_system_ms);
        }
        let barrier = barrier.map(Arc::new);
        let vshards: Vec<u32> = self.vshard_senders.keys().copied().collect();
        for vshard in vshards {
            let marker = SchedulerInput::CutMarker {
                hlc,
                restore_point,
                barrier: barrier.clone(),
            };
            match self.deliver(index, vshard, marker) {
                Delivery::NotHosted | Delivery::Sent | Delivery::Deferred => {}
                Delivery::DroppedFull => warn!(
                    vshard,
                    hlc,
                    "sequencer apply: vshard channel full (backpressure); \
                     cut marker left to the catch-up replay"
                ),
                Delivery::DroppedClosed => warn!(
                    vshard,
                    "sequencer apply: vshard sender gone; \
                     scheduler may have exited (cut marker)"
                ),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use tokio::sync::mpsc;

    use super::*;
    use crate::calvin::CalvinCompletionRegistry;
    use crate::calvin::sequencer::entry::SequencerEntry;

    fn encode(entry: &SequencerEntry) -> Vec<u8> {
        zerompk::to_msgpack_vec(entry).unwrap()
    }

    fn floor(next_epoch: u64, epoch_system_ms: i64) -> Vec<u8> {
        encode(&SequencerEntry::EpochFloor {
            next_epoch,
            epoch_system_ms,
        })
    }

    #[test]
    fn a_restored_log_resumes_at_its_epoch_floor() {
        let mut sm =
            SequencerStateMachine::new(HashMap::new(), CalvinCompletionRegistry::new_detached());
        sm.apply(11, &floor(40, 1_900_000_000_000));
        assert_eq!(sm.last_applied_epoch(), Some(39));
        assert_eq!(sm.next_epoch(), 40);
        assert_eq!(sm.last_epoch_system_ms(), Some(1_900_000_000_000));

        // A floor never lowers an epoch or an epoch instant already applied.
        sm.apply(12, &floor(5, 1_000));
        assert_eq!(sm.next_epoch(), 40);
        assert_eq!(sm.last_epoch_system_ms(), Some(1_900_000_000_000));

        // A floor of epoch 0 and no instant leaves a fresh machine at epoch 0.
        let mut fresh =
            SequencerStateMachine::new(HashMap::new(), CalvinCompletionRegistry::new_detached());
        fresh.apply(1, &floor(0, 0));
        assert_eq!(fresh.next_epoch(), 0);
        assert_eq!(fresh.last_epoch_system_ms(), None);
    }

    #[test]
    fn a_restore_point_cut_marker_reports_the_sequencer_place() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&seen);
        let (tx, mut rx) = mpsc::channel(4);
        let mut senders = HashMap::new();
        senders.insert(3, tx);
        let mut sm = SequencerStateMachine::new(senders, CalvinCompletionRegistry::new_detached())
            .with_restore_point_hook(Arc::new(move |point: SequencerRestorePoint| {
                sink.lock().unwrap().push(point);
            }));
        sm.apply(20, &floor(8, 1_800_000_000_000));

        sm.apply(
            21,
            &encode(&SequencerEntry::CutMarker {
                hlc: 900,
                restore_point: 0,
                barrier: None,
                floor: 0,
            }),
        );
        sm.apply(
            22,
            &encode(&SequencerEntry::CutMarker {
                hlc: 1_000,
                restore_point: 77,
                barrier: Some(CutBarrierWire { capture: None }),
                floor: 0,
            }),
        );

        assert_eq!(
            *seen.lock().unwrap(),
            [SequencerRestorePoint {
                id: 77,
                hlc: 1_000,
                index: 22,
                next_epoch: 8,
                epoch_system_ms: Some(1_800_000_000_000),
            }],
            "only a restore point's marker is reported"
        );
        for (hlc, point, ordered) in [(900, 0, false), (1_000, 77, true)] {
            assert!(matches!(
                rx.try_recv(),
                Ok(SchedulerInput::CutMarker { hlc: got, restore_point, barrier })
                    if got == hlc && restore_point == point && barrier.is_some() == ordered
            ));
        }
    }

    /// Every cut marker reports its log index and the epoch instant applied
    /// before it.
    #[test]
    fn every_cut_marker_reports_its_epoch_instant() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&seen);
        let mut sm =
            SequencerStateMachine::new(HashMap::new(), CalvinCompletionRegistry::new_detached())
                .with_cut_instant_hook(Arc::new(move |hlc, index, instant| {
                    sink.lock().unwrap().push((hlc, index, instant));
                }));
        let marker = |hlc| {
            encode(&SequencerEntry::CutMarker {
                hlc,
                restore_point: 0,
                barrier: None,
                floor: 0,
            })
        };
        sm.apply(1, &marker(10));
        sm.apply(2, &floor(4, 1_800_000_000_000));
        sm.apply(3, &marker(20));
        assert_eq!(
            *seen.lock().unwrap(),
            [(10, 1, None), (20, 3, Some(1_800_000_000_000))]
        );
    }

    /// A marker's floor raises the applied epoch instant to the floor's
    /// millisecond before the marker records its instant. A floor below the
    /// applied instant changes nothing.
    #[test]
    fn a_cut_marker_floor_raises_the_applied_epoch_instant() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&seen);
        let mut sm =
            SequencerStateMachine::new(HashMap::new(), CalvinCompletionRegistry::new_detached())
                .with_cut_instant_hook(Arc::new(move |hlc, _index, instant| {
                    sink.lock().unwrap().push((hlc, instant));
                }));
        sm.apply(1, &floor(4, 1_800_000_000_000));
        let floored = |hlc, floor| {
            encode(&SequencerEntry::CutMarker {
                hlc,
                restore_point: 0,
                barrier: None,
                floor,
            })
        };
        // An ordinal inside millisecond 1_800_000_000_500.
        let ahead = nodedb_types::ms_to_ordinal_upper(1_800_000_000_500) - 7;
        sm.apply(2, &floored(10, ahead));
        assert_eq!(sm.last_epoch_system_ms(), Some(1_800_000_000_500));
        sm.apply(3, &floored(20, 1));
        assert_eq!(sm.last_epoch_system_ms(), Some(1_800_000_000_500));
        assert_eq!(
            *seen.lock().unwrap(),
            [(10, Some(1_800_000_000_500)), (20, Some(1_800_000_000_500))]
        );
        assert!(
            nodedb_types::ms_to_ordinal_upper(1_800_000_000_500) >= ahead,
            "the recorded instant's cut covers the floor"
        );
    }
}
