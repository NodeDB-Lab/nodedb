// SPDX-License-Identifier: BUSL-1.1

//! A dependent-read txn on the data-group leader of one of its vShards.
//!
//! The vShard's role in the txn decides the route:
//!
//! - passive (the txn reads rows here): the leader dispatches
//!   `MetaOp::CalvinExecutePassive` under the txn's locks, and proposes the
//!   values it returns to the data group of every active vShard;
//! - active (the write set names this vShard): the leader opens the
//!   dependent-read barrier, once its own passive read answered when the
//!   vShard is passive too;
//! - neither: a read-set participant, staged as a static slice.
//!
//! A passive-only vShard writes nothing. Its read answer is its vote: a
//! successful read votes commit, a failed one votes abort.

use std::sync::Arc;
use std::time::Instant;

use nodedb_cluster::calvin::AbortReason;
use nodedb_cluster::calvin::types::SequencedTxn;
use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_physical::physical_plan::meta::MetaOp;

use super::deferred::{DispatchOutcome, DispatchStep};
use super::halt::error_response_text;
use super::propose::{CalvinReadResultProposal, propose_calvin_read_result};
use super::scheduler::Scheduler;
use crate::bridge::envelope::{Response, Status};
use crate::control::cluster::calvin::scheduler::driver::types::{CommitState, PendingTxn};
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;

/// This vShard's role in a dependent-read txn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DependentRole {
    /// The txn reads rows on this vShard.
    passive: bool,
    /// The txn's write set names this vShard.
    active: bool,
}

impl Scheduler {
    /// Route a granted dependent-read txn on this leader by the vShard's
    /// role in it.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn route_dependent(
        &mut self,
        txn: SequencedTxn,
        txn_id: TxnId,
        lock_owner: TxnId,
    ) {
        let role = match self.dependent_role(&txn) {
            Ok(role) => role,
            Err(error) => {
                self.reject_plan(txn, txn_id, lock_owner, &error);
                return;
            }
        };
        match role {
            DependentRole { passive: true, .. } => {
                self.dispatch_passive_read(txn, txn_id, lock_owner);
            }
            DependentRole {
                passive: false,
                active: true,
            } => self.insert_dependent_barrier(txn, txn_id, lock_owner),
            DependentRole {
                passive: false,
                active: false,
            } => self.dispatch_txn(txn, txn_id, lock_owner),
        }
    }

    /// This vShard's role in dependent-read txn `txn`.
    fn dependent_role(&self, txn: &SequencedTxn) -> crate::Result<DependentRole> {
        let passive = txn
            .tx_class
            .dependent_reads
            .as_ref()
            .is_some_and(|spec| spec.passive_reads.contains_key(&self.vshard_id));
        let active = txn
            .tx_class
            .active_vshards()
            .map_err(|error| crate::Error::Internal {
                detail: format!(
                    "calvin txn {}/{}: the write set names no vShard of its database: {error}",
                    txn.epoch, txn.position
                ),
            })?
            .contains(&self.vshard_id);
        Ok(DependentRole { passive, active })
    }

    /// Dispatch the passive read of `txn` on this vShard, and hold the txn
    /// in [`CommitState::ReadingPassive`] until it answers.
    fn dispatch_passive_read(&mut self, txn: SequencedTxn, txn_id: TxnId, lock_owner: TxnId) {
        let keys_to_read = txn
            .tx_class
            .dependent_reads
            .as_ref()
            .and_then(|spec| spec.passive_reads.get(&self.vshard_id))
            .cloned()
            .unwrap_or_default();
        let tenant_id = txn.tx_class.tenant_id;
        let database_id = txn.tx_class.database_id;
        let plan = PhysicalPlan::Meta(MetaOp::CalvinExecutePassive {
            epoch: txn_id.epoch,
            position: txn_id.position,
            tenant_id,
            keys_to_read,
        });
        let request_id = self.next_request_id();
        let request = self.build_exempt_request(request_id, tenant_id, database_id, plan);
        // A passive-only vShard votes on its read, so it checks the
        // collections the txn names as a stage does.
        let (superseded, gates) = self.check_incarnations(&txn.tx_class);
        // The slice's own scope: a vShard that is passive and active writes
        // here, whatever step the txn is at.
        let scope = self.local_slice_scope(&txn, txn_id);
        self.pending.insert(
            txn_id,
            PendingTxn {
                txn,
                lock_owner,
                // no-determinism: dispatch time is observability only.
                dispatch_time: Instant::now(),
                has_primary_write: false,
                raises_write_mark: false,
                has_returning: false,
                commit_state: CommitState::ReadingPassive,
                // The dispatch below records its request.
                awaiting: None,
                verdict_deadline: None,
                stage_error: None,
                scope,
                redo: None,
                superseded,
                gates,
                ungated: false,
            },
        );
        self.absorb_buffered_reads(txn_id);
        if let DispatchOutcome::Failed(error) =
            self.dispatch_sequenced(txn_id, DispatchStep::ReadPassive, request)
        {
            self.fail_dispatch_step(txn_id, DispatchStep::ReadPassive, error);
        }
    }

    /// Act on the answer of `txn_id`'s passive read: propose the values to
    /// every active vShard, then vote on them on a passive-only vShard, or
    /// open the barrier on an active one.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn finish_passive_read(
        &mut self,
        txn_id: TxnId,
        response: &Response,
    ) {
        let Some(role) = self
            .pending
            .get(&txn_id)
            .map(|pending| self.dependent_role(&pending.txn))
        else {
            return;
        };
        let role = match role {
            Ok(role) => role,
            Err(error) => {
                if let Some(pending) = self.pending.remove(&txn_id) {
                    self.reject_plan(pending.txn, txn_id, pending.lock_owner, &error);
                }
                return;
            }
        };
        let read = response.status == Status::Ok;
        if read {
            self.propose_read_results(txn_id, response.payload.to_vec());
        }
        if !role.active {
            // A passive-only vShard writes nothing: its read answer carries
            // its vote, and a COMMIT verdict drops the empty slice.
            self.resolve_staged_commit(txn_id, response);
            return;
        }
        let Some(pending) = self.pending.remove(&txn_id) else {
            return;
        };
        if read {
            self.insert_dependent_barrier(pending.txn, txn_id, pending.lock_owner);
        } else {
            self.park_unstaged(
                pending.txn,
                txn_id,
                pending.lock_owner,
                AbortReason::ParticipantError,
                error_response_text("passive read", response),
            );
        }
    }

    /// Propose `values`, this vShard's passive read of `txn_id`, to the data
    /// group of every active vShard of the txn. Each proposal runs on its
    /// own task until `config.passive_timeout()` passes.
    fn propose_read_results(&self, txn_id: TxnId, values: Vec<u8>) {
        let Some(pending) = self.pending.get(&txn_id) else {
            return;
        };
        let tx_class = &pending.txn.tx_class;
        let targets = match tx_class.active_vshards() {
            Ok(targets) => targets,
            Err(error) => {
                tracing::error!(
                    vshard_id = self.vshard_id,
                    epoch = txn_id.epoch,
                    position = txn_id.position,
                    %error,
                    "calvin: no active vShard to propose the passive read to"
                );
                return;
            }
        };
        #[cfg(feature = "failpoints")]
        let gates: Vec<String> = tx_class
            .dependent_reads
            .as_ref()
            .and_then(|spec| spec.passive_reads.get(&self.vshard_id))
            .map(|keys| {
                keys.iter()
                    .map(|key| {
                        format!(
                            "calvin::before_read_result_propose::{}",
                            key.engine_key.collection()
                        )
                    })
                    .collect()
            })
            .unwrap_or_default();
        let budget = self.config.passive_timeout();
        for target_vshard in targets {
            let proposal = CalvinReadResultProposal {
                target_vshard,
                epoch: txn_id.epoch,
                position: txn_id.position,
                tenant_id: tx_class.tenant_id,
                database_id: tx_class.database_id,
                passive_vshard: self.vshard_id,
                values: values.clone(),
            };
            let state = Arc::downgrade(&self.shared);
            #[cfg(feature = "failpoints")]
            let gates = gates.clone();
            #[cfg(feature = "failpoints")]
            let fail_scope = crate::fail_point::FailScope::Node(self.shared.node_id);
            tokio::spawn(async move {
                #[cfg(feature = "failpoints")]
                for gate in &gates {
                    crate::control::fail_gate::wait(fail_scope, gate).await;
                }
                let deadline = tokio::time::Instant::now() + budget;
                if let Err(error) = propose_calvin_read_result(state, proposal, deadline).await {
                    tracing::warn!(
                        target_vshard,
                        epoch = txn_id.epoch,
                        position = txn_id.position,
                        %error,
                        "calvin: passive read result not proposed before its deadline; \
                         the active barrier's timeout entry ends the txn"
                    );
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use nodedb_cluster::calvin::types::{
        DependentReadSpec, EngineKeySet, PassiveReadKey, SchedulerInput, SortedVec,
    };
    use nodedb_cluster::calvin::{CalvinCompletionRegistry, SequencerEntry};

    use super::*;
    use crate::bridge::dispatch::CoreChannelDataSide;
    use crate::bridge::envelope::{ErrorCode, Payload, StageVote};
    use crate::control::cluster::calvin::scheduler::driver::core::test_proposer::{
        CapturingProposer, lead_data_group,
    };
    use crate::control::cluster::calvin::scheduler::driver::core::test_support::{
        build_test_scheduler_with_data_side, make_local_write_txn, test_coll_vshard,
    };

    /// A dependent txn that writes vShard `active` only, and reads `items`
    /// on the scheduler's own vShard.
    fn passive_only_txn(epoch: u64) -> SequencedTxn {
        let mut txn = make_local_write_txn(epoch, 0);
        let other = (0u32..4096)
            .map(|i| format!("passive_target_{i}"))
            .find(|name| {
                nodedb_types::CollectionKey::from_bare(nodedb_types::DatabaseId::DEFAULT, name)
                    .vshard()
                    .as_u32()
                    != test_coll_vshard()
            })
            .expect("a collection on another vShard");
        txn.tx_class.write_set =
            nodedb_cluster::calvin::types::ReadWriteSet::new(vec![EngineKeySet::Document {
                collection: other,
                surrogates: SortedVec::new(vec![1]),
            }]);
        txn.tx_class.dependent_reads = Some(DependentReadSpec {
            passive_reads: BTreeMap::from([(
                test_coll_vshard(),
                vec![PassiveReadKey {
                    engine_key: EngineKeySet::Kv {
                        collection: "test_coll".to_owned(),
                        keys: SortedVec::new(vec![b"k".to_vec()]),
                    },
                }],
            )]),
            expected: BTreeMap::new(),
        });
        txn
    }

    fn leader() -> (
        Scheduler,
        tempfile::TempDir,
        CoreChannelDataSide,
        Arc<CapturingProposer>,
    ) {
        let registry = CalvinCompletionRegistry::new_detached();
        let (mut scheduler, dir, data_side) =
            build_test_scheduler_with_data_side(test_coll_vshard(), registry);
        let proposer = CapturingProposer::accepting();
        scheduler.sequencer_proposer = proposer.clone();
        lead_data_group(&mut scheduler);
        (scheduler, dir, data_side, proposer)
    }

    /// The request id of the passive read the Data Plane received.
    fn passive_read_request(
        data_side: &mut CoreChannelDataSide,
    ) -> Option<crate::types::RequestId> {
        let mut found = None;
        while let Ok(request) = data_side.request_rx.try_pop() {
            if matches!(
                request.inner.plan,
                PhysicalPlan::Meta(MetaOp::CalvinExecutePassive { .. })
            ) {
                found = Some(request.inner.request_id);
            }
        }
        found
    }

    fn answer(request_id: crate::types::RequestId, status: Status) -> Response {
        Response {
            request_id,
            status,
            attempt: 1,
            partial: false,
            payload: Payload::from_vec(
                zerompk::to_msgpack_vec(&Vec::<(
                    nodedb_physical::physical_plan::meta::PassiveReadKeyId,
                    nodedb_types::Value,
                )>::new())
                .expect("encode"),
            ),
            watermark_lsn: crate::types::Lsn::ZERO,
            error_code: (status == Status::Error).then(|| {
                Box::new(ErrorCode::Internal {
                    detail: "read failed".into(),
                })
            }),
            stage_vote: (status == Status::Ok).then_some(StageVote::Commit),
            read_versions: crate::types::ReadVersions::new(),
            write_set: Vec::new(),
        }
    }

    /// A passive-only vShard dispatches its read under the txn's locks and
    /// votes commit on a successful answer.
    #[tokio::test]
    async fn a_passive_only_vshard_reads_then_votes_commit() {
        let (mut scheduler, _dir, mut data_side, proposer) = leader();
        scheduler.process_scheduler_input(SchedulerInput::Txn(Box::new(passive_only_txn(70))));
        let txn_id = TxnId::new(70, 0);
        assert_eq!(
            scheduler.pending.get(&txn_id).map(|p| p.commit_state),
            Some(CommitState::ReadingPassive)
        );
        let request_id = passive_read_request(&mut data_side).expect("the passive read");

        scheduler.handle_completion(txn_id, request_id, Some(answer(request_id, Status::Ok)));

        assert_eq!(
            scheduler.pending.get(&txn_id).map(|p| p.commit_state),
            Some(CommitState::AwaitingVerdict)
        );
        assert!(proposer.accepted().iter().any(|entry| matches!(
            entry,
            SequencerEntry::Vote {
                epoch: 70,
                position: 0,
                ..
            }
        )));
    }

    /// A failed passive read votes abort on a passive-only vShard.
    #[tokio::test]
    async fn a_failed_passive_read_votes_abort() {
        let (mut scheduler, _dir, mut data_side, proposer) = leader();
        scheduler.process_scheduler_input(SchedulerInput::Txn(Box::new(passive_only_txn(71))));
        let txn_id = TxnId::new(71, 0);
        let request_id = passive_read_request(&mut data_side).expect("the passive read");

        scheduler.handle_completion(txn_id, request_id, Some(answer(request_id, Status::Error)));

        assert!(proposer.accepted().iter().any(|entry| matches!(
            entry,
            SequencerEntry::AbortVote {
                epoch: 71,
                reason: AbortReason::ParticipantError,
                ..
            }
        )));
    }

    /// A vShard both passive and active opens its barrier once its own read
    /// answered. The barrier waits for its own result through the log.
    #[tokio::test]
    async fn a_passive_and_active_vshard_opens_its_barrier_after_reading() {
        let (mut scheduler, _dir, mut data_side, _proposer) = leader();
        let mut txn = make_local_write_txn(72, 0);
        txn.tx_class.dependent_reads = Some(DependentReadSpec {
            passive_reads: BTreeMap::from([(test_coll_vshard(), Vec::new())]),
            expected: BTreeMap::new(),
        });
        scheduler.process_scheduler_input(SchedulerInput::Txn(Box::new(txn)));
        let txn_id = TxnId::new(72, 0);
        let request_id = passive_read_request(&mut data_side).expect("the passive read");

        scheduler.handle_completion(txn_id, request_id, Some(answer(request_id, Status::Ok)));

        assert!(!scheduler.pending.contains_key(&txn_id));
        assert!(scheduler.dependent_barrier.contains_key(&txn_id));
    }

    /// A leader that loses the vShard while its passive read runs follows
    /// the txn with the slice's own write scope. A COMMIT verdict then waits
    /// for the slice's redo, and the position stays unapplied until it
    /// installs. With the passive read's empty scope, the follower ended the
    /// COMMIT with no entry, marked the position applied, and skipped the
    /// redo the next leader proposed.
    #[tokio::test]
    async fn a_demoted_passive_and_active_leader_waits_for_the_redo() {
        let (mut scheduler, _dir, mut data_side, _proposer) = leader();
        let mut txn = make_local_write_txn(73, 0);
        txn.tx_class.dependent_reads = Some(DependentReadSpec {
            passive_reads: BTreeMap::from([(test_coll_vshard(), Vec::new())]),
            expected: BTreeMap::new(),
        });
        scheduler.process_scheduler_input(SchedulerInput::Txn(Box::new(txn)));
        let txn_id = TxnId::new(73, 0);
        assert!(passive_read_request(&mut data_side).is_some());
        assert!(
            scheduler
                .pending
                .get(&txn_id)
                .is_some_and(|pending| pending.scope.writes),
            "the passive read holds the slice's write scope"
        );

        scheduler.demote("the test moves the vShard's leadership");
        assert_eq!(
            scheduler.pending.get(&txn_id).map(|p| p.commit_state),
            Some(CommitState::Following)
        );
        scheduler.resume_on_verdict(txn_id, true);

        assert_eq!(
            scheduler.pending.get(&txn_id).map(|p| p.commit_state),
            Some(CommitState::Following),
            "a COMMIT for a write slice waits for its redo"
        );
        assert!(
            !scheduler.ledger.is_applied(73, 0),
            "the position stays unapplied, so the redo installs"
        );
    }
}
