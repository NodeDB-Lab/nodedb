// SPDX-License-Identifier: BUSL-1.1

//! The data-group leader proposes a committed Calvin slice's stamped redo.
//!
//! The leader proposes right after the resolve, straight to its group with
//! `MultiRaft::propose`: no vShard admission slot and no forward. The
//! leader's write gate exempts a stamped redo, and the scheduler's lock
//! table already orders it against every conflicting write. Entries of
//! non-conflicting txns can enter the log in any order.
//!
//! A redo whose entry passes `tuning.calvin.max_redo_entry_bytes` travels
//! as chunk entries under a stream that names its position and attempt,
//! then a final entry naming the stream.
//!
//! The txn completes once its redo applies. Until then:
//!
//! | Outcome | Action |
//! |---|---|
//! | `NotLeader` | Demote. The next leader stages the txn again. |
//! | Proposed, then the group applied past the entry with no install | Propose again. |
//! | A transient refusal | Propose again on the stall tick. |
//! | The entry cannot be built | Halt: the verdict is COMMIT, so the txn cannot drop. |
//! | Sequenced after an ordered cut's marker whose barrier this node has not applied | Hold. Propose once the barrier applied. |
//!
//! Every copy that commits installs at most once: the apply loop claims the
//! position in the vShard's applied ledger.

use nodedb_cluster::ClusterError;
use nodedb_raft::RaftError;

use super::super::types::CommitState;
use super::halt::{HaltReason, HaltStep};
use super::scheduler::Scheduler;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;
use crate::control::wal_replication::ReplicatedWrite;
use crate::control::wal_replication::encode::{
    CalvinEntryStamps, RedoEntryTarget, RedoProposal, calvin_redo_proposal,
};
use crate::wal::RedoStreamId;

/// A committed slice's redo, kept while it is proposed.
#[derive(Debug, Clone)]
pub(in crate::control::cluster::calvin::scheduler::driver) struct OwedRedo {
    pub payload: crate::control::wal_replication::transaction_redo::TransactionRedoPayload,
    pub stamps: CalvinEntryStamps,
    pub target: RedoEntryTarget,
    /// Proposals made so far. Each names its own chunk stream.
    pub attempts: u32,
}

/// Why a proposal did not reach the log.
enum ProposeRefusal {
    /// This node does not lead the vShard's data group.
    NotLeader,
    /// The group cannot take it now. The stall tick proposes again.
    Transient(String),
    /// The entries cannot be built.
    Unbuildable(String),
}

impl Scheduler {
    /// Propose the redo `txn_id` owes, and record where it landed.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn propose_calvin_redo(
        &mut self,
        txn_id: TxnId,
    ) {
        if self
            .pending
            .get(&txn_id)
            .and_then(|pending| pending.redo.as_ref())
            .is_none()
        {
            return;
        }
        #[cfg(feature = "failpoints")]
        if self.redo_propose_held(txn_id) {
            return;
        }
        // A txn sequenced after an ordered cut's marker proposes its redo
        // only once this node applied the cut's barrier in the vShard's
        // group, so the redo lands after the barrier. The pass that ends the
        // hold proposes it again.
        if self.held_for_cut(txn_id.epoch) {
            return;
        }
        // A snapshot install replaced this vShard's base since this
        // scheduler started. The next scheduler starts from the installed
        // state and stages the txn again.
        if self.install_gate.is_retired(&self.shared, self.vshard_id) {
            return;
        }
        // A halt released the txn's gates: take them back, or refuse a redo
        // into a recreated collection.
        if let Err(error) = self.regate(txn_id) {
            self.halt_apply(
                txn_id,
                HaltReason::RedoProposeFailed,
                HaltStep::RedoPropose,
                error.to_string(),
            );
            return;
        }
        let proposed = self
            .attempt_entries(txn_id)
            .and_then(|entries| self.propose_entries(entries));
        match proposed {
            Ok(proposed) => {
                if let Some(pending) = self.pending.get_mut(&txn_id) {
                    pending.commit_state = CommitState::AwaitingRedoApply {
                        proposed: Some(proposed),
                    };
                }
            }
            Err(ProposeRefusal::NotLeader) => self.on_not_leader(),
            Err(ProposeRefusal::Transient(error)) => {
                tracing::debug!(
                    vshard_id = self.vshard_id,
                    epoch = txn_id.epoch,
                    position = txn_id.position,
                    %error,
                    "calvin: redo not proposed; the stall tick proposes it again"
                );
                if let Some(pending) = self.pending.get_mut(&txn_id) {
                    pending.commit_state = CommitState::AwaitingRedoApply { proposed: None };
                }
            }
            Err(ProposeRefusal::Unbuildable(error)) => self.halt_apply(
                txn_id,
                HaltReason::RedoProposeFailed,
                HaltStep::RedoPropose,
                format!("the committed slice's redo entry cannot be built: {error}"),
            ),
        }
    }

    /// Whether a crash test holds `txn_id`'s redo: it resolved and no copy
    /// entered the log. The stall tick asks again.
    #[cfg(feature = "failpoints")]
    fn redo_propose_held(&self, txn_id: TxnId) -> bool {
        let held = self
            .pending
            .get(&txn_id)
            .and_then(|pending| pending.redo.as_ref())
            .is_some_and(|owed| {
                crate::control::fail_gate::holds_redo_propose(&owed.payload.collections)
            });
        if held {
            tracing::info!(
                node_id = self.shared.node_id,
                vshard_id = self.vshard_id,
                epoch = txn_id.epoch,
                position = txn_id.position,
                "calvin: redo proposal held at a fail point"
            );
        }
        held
    }

    /// The encoded entries of the next proposal attempt of `txn_id`.
    fn attempt_entries(&mut self, txn_id: TxnId) -> Result<Vec<Vec<u8>>, ProposeRefusal> {
        let max_entry_bytes = self.shared.redo_chunks.limits().max_entry_bytes;
        let vshard = self.vshard_id;
        let until = self.request_deadline();
        let Some(owed) = self
            .pending
            .get_mut(&txn_id)
            .and_then(|pending| pending.redo.as_mut())
        else {
            return Err(ProposeRefusal::Unbuildable(
                "the txn owes no redo".to_owned(),
            ));
        };
        let stream = RedoStreamId::Calvin {
            vshard,
            epoch: txn_id.epoch,
            position: txn_id.position,
            attempt: owed.attempts,
        };
        owed.attempts = owed.attempts.saturating_add(1);
        let unbuildable = |error: crate::Error| ProposeRefusal::Unbuildable(error.to_string());
        let proposal = calvin_redo_proposal(
            owed.target,
            &owed.payload,
            &owed.stamps,
            stream,
            max_entry_bytes,
        )
        .map_err(unbuildable)?;
        let (chunks, last) = match proposal {
            RedoProposal::Inline(last) => (Vec::new(), last),
            RedoProposal::Chunked {
                stream,
                chunks,
                last,
            } => {
                let len = if let Some(ReplicatedWrite::RedoChunk { len, .. }) =
                    chunks.first().map(|chunk| &chunk.write)
                {
                    *len
                } else {
                    0
                };
                // The leader bounds the node's open stream bytes: a stream
                // past the cap waits for the stall tick.
                self.shared
                    .redo_chunks
                    .admit_stream(stream, len, until)
                    .map_err(|refusal| ProposeRefusal::Transient(refusal.to_string()))?;
                (chunks, last)
            }
        };
        let mut entries = Vec::with_capacity(chunks.len() + 1);
        for entry in chunks.iter().chain(std::iter::once(&last)) {
            entries.push(entry.encode().map_err(unbuildable)?);
        }
        Ok(entries)
    }

    /// Append `entries` to this vShard's data-group log, in order. Returns
    /// `(group_id, log_index)` of the last.
    fn propose_entries(&self, entries: Vec<Vec<u8>>) -> Result<(u64, u64), ProposeRefusal> {
        let mut mr = self.multi_raft.lock().unwrap_or_else(|p| p.into_inner());
        let mut last = None;
        for bytes in entries {
            match mr.propose(self.vshard_id, bytes) {
                Ok(at) => last = Some(at),
                Err(ClusterError::Raft(RaftError::NotLeader { .. })) => {
                    return Err(ProposeRefusal::NotLeader);
                }
                Err(error) => return Err(ProposeRefusal::Transient(error.to_string())),
            }
        }
        last.ok_or_else(|| ProposeRefusal::Unbuildable("a redo with no entry".to_owned()))
    }

    /// Propose again every owed redo that is not in the log: never
    /// proposed, refused, or proposed and passed by the group's applied
    /// index with no install, which a new term's entries overwrote. Runs
    /// on the stall tick, after the inbox drained, on the leader only.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn retry_redo_proposals(
        &mut self,
    ) {
        if !self.role.is_leader() {
            return;
        }
        let owed: Vec<(TxnId, Option<(u64, u64)>)> = self
            .pending
            .iter()
            .filter_map(|(txn_id, pending)| match pending.commit_state {
                CommitState::AwaitingRedoApply { proposed } => Some((*txn_id, proposed)),
                CommitState::Following
                | CommitState::Staged
                | CommitState::AwaitingVerdict
                | CommitState::AwaitingResolveTurn
                | CommitState::AwaitingRedoResolve
                | CommitState::AwaitingDrop
                | CommitState::AwaitingRestage
                | CommitState::ReadingPassive => None,
            })
            .collect();
        for (txn_id, proposed) in owed {
            let lost = match proposed {
                None => true,
                Some((group_id, log_index)) => {
                    self.shared.applied_index_watcher(group_id).current() >= log_index
                }
            };
            if lost {
                if proposed.is_some() {
                    tracing::warn!(
                        vshard_id = self.vshard_id,
                        epoch = txn_id.epoch,
                        position = txn_id.position,
                        "calvin: the group applied past the slice's redo with no install; \
                         proposing it again"
                    );
                }
                self.propose_calvin_redo(txn_id);
            }
            if !self.role.is_leader() {
                return;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_cluster::calvin::CalvinCompletionRegistry;

    use super::*;
    use crate::bridge::dispatch::CoreChannelDataSide;
    use crate::bridge::envelope::{Payload, Response, Status};
    use crate::control::cluster::calvin::scheduler::driver::core::test_proposer::{
        lead_data_group, step_down_from_data_group,
    };
    use crate::control::cluster::calvin::scheduler::driver::core::test_support::{
        build_test_scheduler_with_data_side, make_sequenced_txn, staged_pending, staged_response,
    };
    use crate::control::wal_replication::ReplicatedEntry;
    use crate::wal::RedoRecord;

    const VSHARD: u32 = 7;

    /// A leader on [`VSHARD`] with `txn_id` committed and its resolve
    /// awaited.
    fn resolving_leader(txn_id: TxnId) -> (Scheduler, tempfile::TempDir, CoreChannelDataSide) {
        let (mut scheduler, dir, data_side) =
            build_test_scheduler_with_data_side(VSHARD, CalvinCompletionRegistry::new_detached());
        lead_data_group(&mut scheduler);
        let mut pending = staged_pending(make_sequenced_txn(txn_id.epoch, txn_id.position), txn_id);
        pending.commit_state = CommitState::AwaitingRedoResolve;
        scheduler.pending.insert(txn_id, pending);
        (scheduler, dir, data_side)
    }

    /// A resolve answer whose redo record carries no ops.
    fn resolved_answer() -> Response {
        let redo = RedoRecord {
            version: 1,
            ops: Vec::new(),
            calvin_stamp: None,
            cross_shard_applied: None,
            row_sources: Vec::new(),
            publishes: Vec::new(),
            row_changes: Vec::new(),
        };
        let resolved = nodedb_physical::physical_plan::CalvinResolved {
            redo: redo.to_bytes().expect("encode redo"),
            reply: nodedb_physical::physical_plan::CalvinReplySpec::Count(Vec::new()),
        };
        let mut response = staged_response(Status::Ok, None);
        response.payload =
            Payload::from_vec(zerompk::to_msgpack_vec(&resolved).expect("encode resolved"));
        response
    }

    /// Where `txn_id`'s redo entry landed, if it is in the log.
    fn proposed_at(scheduler: &Scheduler, txn_id: TxnId) -> Option<(u64, u64)> {
        match scheduler.pending.get(&txn_id).map(|p| p.commit_state) {
            Some(CommitState::AwaitingRedoApply { proposed }) => proposed,
            other => panic!("the txn awaits its redo apply, found {other:?}"),
        }
    }

    /// The committed entry of `group_id` at `index`. Ticks the single-voter
    /// group until the entry commits.
    fn committed_entry(scheduler: &Scheduler, group_id: u64, index: u64) -> Vec<u8> {
        let mut mr = scheduler
            .multi_raft
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        for _ in 0..20 {
            let entries = mr
                .read_committed_entries(group_id, index, index)
                .expect("read the group's log");
            if let Some(entry) = entries.into_iter().find(|entry| entry.index == index) {
                return entry.data;
            }
            mr.wait_all_durable_blocking();
            mr.tick().expect("tick");
        }
        panic!("the entry at {index} never committed");
    }

    /// A committed write slice's resolve proposes exactly one entry: the
    /// stamped redo of the slice, on the vShard's data group.
    #[tokio::test]
    async fn a_committed_write_slice_proposes_one_stamped_entry() {
        let txn_id = TxnId::new(14, 2);
        let (mut scheduler, _dir, _data_side) = resolving_leader(txn_id);
        let group_id = {
            let mr = scheduler
                .multi_raft
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            mr.routing()
                .read()
                .unwrap_or_else(|p| p.into_inner())
                .group_for_vshard(VSHARD)
                .expect("the fixture routes every vShard")
        };
        let last_before = scheduler
            .multi_raft
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .last_log_index(group_id)
            .expect("the group is mounted");

        scheduler.finish_redo_resolve(txn_id, resolved_answer());

        let (proposed_group, index) =
            proposed_at(&scheduler, txn_id).expect("the redo is in the log");
        assert_eq!(proposed_group, group_id);
        assert_eq!(index, last_before + 1, "one entry, no chunks");
        let entry = ReplicatedEntry::from_bytes(&committed_entry(&scheduler, group_id, index))
            .expect("a replicated entry");
        assert_eq!(entry.vshard_id, VSHARD);
        assert!(
            matches!(
                entry.write,
                ReplicatedWrite::TransactionRedo {
                    calvin: Some(_),
                    ..
                }
            ),
            "the entry is the slice's stamped redo"
        );
        assert!(!scheduler.applied.is_applied(14, 2));
        assert!(!scheduler.is_apply_halted());
    }

    /// The group applied past the proposed entry with no install: a new
    /// term overwrote it. The stall tick proposes the redo again.
    #[tokio::test]
    async fn an_overwritten_proposal_is_proposed_again() {
        let txn_id = TxnId::new(14, 2);
        let (mut scheduler, _dir, _data_side) = resolving_leader(txn_id);
        scheduler.finish_redo_resolve(txn_id, resolved_answer());
        let (group_id, first) = proposed_at(&scheduler, txn_id).expect("proposed");

        scheduler.retry_redo_proposals();
        assert_eq!(
            proposed_at(&scheduler, txn_id),
            Some((group_id, first)),
            "an entry the group has not applied past stays"
        );

        scheduler.shared.applied_index_watcher(group_id).bump(first);
        scheduler.retry_redo_proposals();

        let (_, second) = proposed_at(&scheduler, txn_id).expect("proposed again");
        assert!(second > first, "a new entry carries the redo");
        if let Some(pending) = scheduler.pending.get(&txn_id) {
            assert_eq!(
                pending.redo.as_ref().map(|owed| owed.attempts),
                Some(2),
                "each proposal names its own attempt"
            );
        }
    }

    /// A slice sequenced after an ordered cut's marker proposes nothing until
    /// this node applied the cut's barrier in the vShard's group. Then the
    /// pass that ends the hold proposes it.
    #[tokio::test]
    async fn a_slice_after_a_cut_marker_waits_for_the_barrier() {
        use crate::control::backup::cut_order::OrderedCut;
        use nodedb_cluster::calvin::types::SchedulerInput;

        let txn_id = TxnId::new(14, 2);
        let (mut scheduler, _dir, _data_side) = resolving_leader(txn_id);
        let cut = OrderedCut {
            hlc: scheduler.shared.hlc_clock.now().wall_ns,
            restore_point: 0,
            capture: None,
        };
        scheduler.process_scheduler_input(SchedulerInput::CutMarker {
            hlc: cut.hlc,
            restore_point: 0,
            barrier: Some(std::sync::Arc::new(cut.to_wire())),
        });

        scheduler.finish_redo_resolve(txn_id, resolved_answer());
        assert_eq!(proposed_at(&scheduler, txn_id), None, "the redo is held");
        scheduler.retry_redo_proposals();
        assert_eq!(
            proposed_at(&scheduler, txn_id),
            None,
            "the stall tick keeps it held"
        );

        let group_id = {
            let mr = scheduler
                .multi_raft
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            mr.routing()
                .read()
                .unwrap_or_else(|p| p.into_inner())
                .group_for_vshard(VSHARD)
                .expect("the fixture routes every vShard")
        };
        scheduler
            .shared
            .calvin
            .cut_barriers
            .note_applied(&cut, group_id, 1);
        assert!(scheduler.release_cut_holds());
        scheduler.retry_redo_proposals();
        assert!(
            proposed_at(&scheduler, txn_id).is_some(),
            "the barrier applied, so the redo is proposed"
        );
        assert!(!scheduler.is_apply_halted());
    }

    /// A proposal refused because this node no longer leads demotes the
    /// scheduler: the txn is held `Following` for the next leader.
    #[tokio::test]
    async fn not_leader_demotes() {
        let txn_id = TxnId::new(14, 2);
        let (mut scheduler, _dir, _data_side) = resolving_leader(txn_id);
        step_down_from_data_group(&scheduler);

        scheduler.finish_redo_resolve(txn_id, resolved_answer());

        assert!(!scheduler.role.is_leader());
        assert_eq!(
            scheduler.pending.get(&txn_id).map(|p| p.commit_state),
            Some(CommitState::Following)
        );
        assert!(!scheduler.is_apply_halted());
        assert!(!scheduler.applied.is_applied(14, 2));
    }
}
