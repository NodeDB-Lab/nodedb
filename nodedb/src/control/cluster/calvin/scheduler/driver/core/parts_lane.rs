// SPDX-License-Identifier: BUSL-1.1

//! The backlog lane: the inputs a scheduler still takes while its backlog
//! bound holds the intake gate closed.
//!
//! A closed gate processes no new txn, so the backlog stays bounded. Some
//! of the backlog, though, finishes only on input:
//!
//! - A multi-part txn that holds its locks finishes only once its parts
//!   arrive, and every later whole-collection resolve on this vShard waits
//!   for it.
//! - A blocked txn below it can wait on a reservation release.
//!
//! While the gate is closed for the backlog, the lane lets exactly those
//! inputs through, and holds every other input back:
//!
//! 1. The scheduler arms its own catch-up past the last applied sequencer
//!    entry. The sequencer then sends it nothing live, so its channel holds
//!    a finite run of inputs, and the log holds the rest.
//! 2. It reads the whole channel. A part or an abandonment of a txn whose
//!    assembly is open, and a release no held input depends on, is
//!    processed at once. Every other input joins the held queue, in
//!    arrival order. The queue never outgrows the channel.
//! 3. Once the channel is empty, it scans the committed log from the armed
//!    catch-up start, one window per pass, and processes the same kinds of
//!    input it finds there. The catch-up drain replays that range again
//!    later, in full. A part delivered twice is ignored, and a release
//!    applies once.
//!
//! When the gate opens, the held queue is processed first, in order, before
//! the channel and the catch-up drain. No input is lost or reordered, except
//! the lane's inputs, which commute with every held input:
//!
//! - A part only fills its txn's assembly, which reads parts by index. An
//!   abandonment only ends its txn. Neither touches another txn.
//! - A release frees locks. Lock queues grant in arrival order, so a
//!   release before a held acquire leaves the same holders once the acquire
//!   is processed. A release is held back when a held input names its
//!   owner, so it never overtakes the acquire it releases.
//!
//! No deadlock follows. A txn waiting for parts holds its locks, so the
//! parts and any abandonment of it follow its header in the log. They sit
//! in the channel or in the log past the armed start, and the lane reaches
//! both. So does a release a blocked txn waits on. The txn then stages,
//! applies, and the backlog behind it drains, which opens the gate.

use std::collections::VecDeque;

use nodedb_cluster::calvin::SEQUENCER_GROUP_ID;
use nodedb_cluster::calvin::types::SchedulerInput;

use super::intake::IntakeClosure;
use super::scheduler::Scheduler;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;

/// What the backlog lane holds between passes.
#[derive(Debug, Default)]
pub(in crate::control::cluster::calvin::scheduler::driver::core) struct PartsLane {
    /// Inputs read while the gate was closed and not yet processed, in
    /// arrival order.
    held: VecDeque<SchedulerInput>,
    /// The next sequencer log index the lane's scan reads.
    scan_next: Option<u64>,
}

#[cfg(test)]
impl PartsLane {
    /// How many inputs the lane holds.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn held_len(&self) -> usize {
        self.held.len()
    }
}

impl Scheduler {
    /// Run the intake gate for one loop pass, and return whether the run
    /// loop can read new input.
    ///
    /// An open gate first processes the held inputs, in order, re-checking
    /// the gate after each one. A gate closed for the backlog runs the lane.
    /// The run loop reads new input only once nothing is held.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn pass_intake_lane(
        &mut self,
    ) -> bool {
        loop {
            if !self.refresh_intake_gate() {
                if self.intake_closure() == Some(IntakeClosure::BacklogFull) {
                    self.run_backlog_lane();
                }
                return false;
            }
            let Some(input) = self.parts.lane.held.pop_front() else {
                // Nothing held: the lane's scan starts afresh next time.
                self.parts.lane.scan_next = None;
                return true;
            };
            self.process_scheduler_input(input);
        }
    }

    /// One pass of the backlog lane: arm the catch-up, read the channel, and
    /// scan one window of the log.
    fn run_backlog_lane(&mut self) {
        let armed_from = self
            .sequencer_state_machine
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .arm_catch_up_past_applied(self.vshard_id);
        // An empty or closed channel ends the read. A closed channel ends the
        // run loop once the gate opens.
        while let Ok(input) = self.receiver.try_recv() {
            self.take_in_lane(input);
        }
        let from = self
            .parts
            .lane
            .scan_next
            .map_or(armed_from, |next| next.max(armed_from));
        self.parts.lane.scan_next = Some(self.scan_log_for_lane(from));
    }

    /// Process `input` now when the lane lets it through, else hold it.
    fn take_in_lane(&mut self, input: SchedulerInput) {
        if self.passes_lane(&input) {
            self.process_scheduler_input(input);
        } else {
            self.parts.lane.held.push_back(input);
        }
    }

    /// Whether the lane processes `input` while the gate is closed.
    ///
    /// - A part or an abandonment of a txn whose assembly is open.
    /// - A release whose owner no held input names: no held reservation of
    ///   it, and no held txn that locks as it.
    fn passes_lane(&self, input: &SchedulerInput) -> bool {
        match input {
            SchedulerInput::TxnPart { txn, .. } | SchedulerInput::PartsAbandoned { txn } => {
                self.parts.has_assembly(TxnId::from(*txn))
            }
            SchedulerInput::Release { owner, .. } => {
                !self.parts.lane.held.iter().any(|held| match held {
                    SchedulerInput::Reserve {
                        owner: reserved, ..
                    } => reserved == owner,
                    SchedulerInput::Txn(txn) => txn.lock_owner.as_ref() == Some(owner),
                    _ => false,
                })
            }
            SchedulerInput::Txn(_)
            | SchedulerInput::Reserve { .. }
            | SchedulerInput::CutMarker { .. } => false,
        }
    }

    /// Scan one window of the committed sequencer log from `from` for the
    /// lane's inputs, and return the next index to scan.
    ///
    /// A failed read leaves `from` for the next pass. The compaction
    /// hold-down keeps every index at or past the armed catch-up start.
    fn scan_log_for_lane(&mut self, from: u64) -> u64 {
        let hi = self
            .sequencer_state_machine
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .current_committed_index();
        let Some(hi) = hi.filter(|&hi| hi >= from) else {
            return from;
        };
        let window = self.config.catch_up_window.max(1);
        let end = from.saturating_add(window - 1).min(hi);
        let entries = {
            let mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
            match mr.read_committed_entries(SEQUENCER_GROUP_ID, from, end) {
                Ok(entries) => entries,
                Err(error) => {
                    tracing::warn!(
                        vshard = self.vshard_id,
                        from,
                        end,
                        %error,
                        "calvin backlog lane: failed to read committed sequencer entries"
                    );
                    return from;
                }
            }
        };
        let inputs = self
            .sequencer_state_machine
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .replay_epochs_for_vshard(&entries, self.vshard_id, 0, u64::MAX);
        for input in inputs {
            if self.passes_lane(&input) {
                self.process_scheduler_input(input);
            }
        }
        end.saturating_add(1)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_cluster::calvin::CalvinCompletionRegistry;
    use nodedb_cluster::calvin::types::{
        MultiPartPlans, PartStreamId, SequencedTxn, TxnIdWire, VShardParts,
    };
    use nodedb_physical::physical_plan::wire as plan_wire;
    use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan};
    use nodedb_types::QualifiedCollection;

    use super::*;
    use crate::control::cluster::calvin::scheduler::driver::core::test_support::{
        build_test_scheduler_with_data_side, make_local_write_txn, test_coll_vshard,
    };
    use crate::types::DatabaseId;

    fn truncate_plan() -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::Truncate {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "test_coll"),
            restart_identity: false,
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        })
    }

    /// The header of a one-part txn at `(epoch, 0)` targeting the
    /// `test_coll` vShard.
    fn one_part_header(epoch: u64) -> SequencedTxn {
        let mut txn = make_local_write_txn(epoch, 0);
        txn.epoch_vshard_txn_count = 1;
        txn.tx_class.plans = Vec::new();
        txn.tx_class.multi_part = Some(MultiPartPlans {
            stream: PartStreamId { node: 1, seq: 1 },
            part_count: 1,
            total_tasks: 1,
            user_write: true,
            client_write: true,
            per_vshard: vec![VShardParts {
                vshard: test_coll_vshard(),
                parts: 1,
            }],
        });
        txn
    }

    /// With the gate closed at the backlog bound, a txn waiting for its
    /// part still receives it through the lane, and stages. A new txn read
    /// with it is held, unprocessed, until the gate opens.
    #[tokio::test]
    async fn a_closed_gate_lets_an_awaited_part_through_and_holds_new_txns() {
        let (mut scheduler, _dir, _data_side) = build_test_scheduler_with_data_side(
            test_coll_vshard(),
            CalvinCompletionRegistry::new_detached(),
        );
        let (input_tx, input_rx) = tokio::sync::mpsc::channel(8);
        scheduler.receiver = input_rx;
        scheduler.config.max_inflight_backlog = 1;
        let awaiting = TxnId::new(3, 0);
        scheduler.process_scheduler_input(SchedulerInput::Txn(Box::new(one_part_header(3))));
        assert!(scheduler.parts.is_awaiting(awaiting));
        assert_eq!(
            scheduler.intake_closure(),
            Some(IntakeClosure::BacklogFull),
            "a txn waiting for parts counts against the bound"
        );

        let later = make_local_write_txn(4, 0);
        input_tx
            .try_send(SchedulerInput::Txn(Box::new(later)))
            .expect("room on the channel");
        let plans = plan_wire::encode_batch(&vec![truncate_plan()]).expect("encode");
        input_tx
            .try_send(SchedulerInput::TxnPart {
                txn: TxnIdWire {
                    epoch: 3,
                    position: 0,
                },
                index: 0,
                first_task: 0,
                plans: Arc::new(plans),
                chunk: None,
            })
            .expect("room on the channel");

        assert!(!scheduler.pass_intake_lane(), "the gate stays closed");
        assert!(
            !scheduler.parts.is_awaiting(awaiting),
            "the part got through the lane"
        );
        assert!(scheduler.pending.contains_key(&awaiting), "it staged");
        assert_eq!(scheduler.parts.lane.held_len(), 1, "the new txn is held");
        assert!(!scheduler.pending.contains_key(&TxnId::new(4, 0)));
    }
}
