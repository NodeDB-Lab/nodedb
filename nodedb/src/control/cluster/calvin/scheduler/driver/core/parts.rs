// SPDX-License-Identifier: BUSL-1.1

//! Multi-part transactions on this vShard's scheduler.
//!
//! A multi-part transaction's header arrives as an ordinary sequenced txn,
//! with its full read and write sets and no plans. The scheduler takes its
//! locks at the header's position, as for any txn, so lock order stays the
//! sequence order. Its plans arrive later as parts, each targeting the
//! vShards its tasks route to.
//!
//! - At the header, the scheduler opens an assembly: how many parts target
//!   this vShard, per the header's manifest.
//! - Each part is kept by its index in the transaction. A part delivered
//!   again is ignored. Parts can arrive in any order.
//! - Once every part arrived, the scheduler reads them in index order. Each
//!   part decodes once, and only the tasks that route here are kept, with
//!   their index in the whole transaction.
//! - A task too large for one part travels as a run of chunk parts with
//!   consecutive indexes. Every chunk targets the task's homes, so a home
//!   receives the whole run. The task decodes from its joined chunks.
//! - The assembly reads only the parts' contents, never their arrival
//!   order. A part that cannot be read therefore fails the transaction on
//!   every replica alike.
//! - A txn whose locks are granted before its parts arrived waits in
//!   `awaiting`, holding its locks. It counts as unfinished, so every later
//!   whole-collection resolve on this vShard waits for it.
//! - Once every part that targets this vShard arrived, the txn stages as a
//!   single-entry txn whose plans are its local tasks. Its manifest keeps
//!   the whole-transaction facts the dispatch reads.
//! - An abandoned txn stages nothing. It releases its locks and completes
//!   when its locks are granted, or at once when it already holds them.

use std::collections::BTreeMap;
use std::sync::Arc;

use nodedb_cluster::calvin::types::{SequencedTxn, TaskChunk, TxnIdWire};
use nodedb_physical::physical_plan::PhysicalPlan;
use tracing::warn;

use super::parts_error::PartsError;
use super::routing::{PlanRouting, plan_vshard_in_database};
use super::scheduler::Scheduler;
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;

/// One part as it arrived: its first task, its bytes, and its chunk.
#[derive(Debug)]
struct ReceivedPart {
    first_task: u32,
    plans: Arc<Vec<u8>>,
    chunk: Option<TaskChunk>,
}

/// The parts one multi-part txn owes this vShard, and the ones that arrived.
#[derive(Debug)]
struct Assembly {
    database_id: crate::types::DatabaseId,
    /// How many parts target this vShard.
    expected: u32,
    /// Every part that arrived, by its index in the txn.
    received: BTreeMap<u32, ReceivedPart>,
    /// The sequencer abandoned the txn: it stages nothing.
    abandoned: bool,
}

impl Assembly {
    fn is_complete(&self) -> bool {
        self.received.len() >= self.expected as usize
    }
}

/// A task whose chunks are being joined.
#[derive(Debug)]
struct SpanningTask {
    task: u32,
    bytes: Vec<u8>,
    total_len: u64,
}

/// Reads an assembly's parts, in index order, into its local tasks.
#[derive(Debug)]
struct Assembler {
    database_id: crate::types::DatabaseId,
    /// `(task index in the whole txn, plan)` of every local task so far.
    local: Vec<(u32, PhysicalPlan)>,
    /// The task whose chunks are being joined.
    spanning: Option<SpanningTask>,
    /// Why a part cannot be read. The txn fails at dispatch.
    failed: Option<PartsError>,
}

impl Assembler {
    fn fail(&mut self, error: impl FnOnce() -> PartsError) {
        self.failed.get_or_insert_with(error);
    }

    /// Keep `plan`, the txn's task `task` from part `index`, when it routes
    /// to `vshard_id`.
    fn keep_if_local(&mut self, index: u32, task: u32, plan: PhysicalPlan, vshard_id: u32) {
        match plan_vshard_in_database(&plan, self.database_id) {
            PlanRouting::Vshards(vshards) => {
                if vshards.iter().any(|v| v.as_u32() == vshard_id) {
                    self.local.push((task, plan));
                }
            }
            PlanRouting::ControlPlaneOnly => {
                self.fail(|| PartsError::ControlPlaneOnly { part: index, task });
            }
            PlanRouting::NotAWrite => {
                self.fail(|| PartsError::NotAWrite { part: index, task });
            }
            PlanRouting::Unroutable(reason) => {
                self.fail(|| PartsError::Unroutable {
                    part: index,
                    task,
                    reason,
                });
            }
        }
    }

    /// Take part `index`, a batch of whole tasks from task `first_task`.
    fn take_tasks(&mut self, index: u32, first_task: u32, plans: &[u8], vshard_id: u32) {
        if let Some(spanning) = &self.spanning {
            let task = spanning.task;
            self.fail(|| PartsError::InterruptsChunks { part: index, task });
            return;
        }
        match super::super::helpers::decode_plans(plans) {
            Ok(decoded) => {
                for (task, plan) in (first_task..).zip(decoded) {
                    self.keep_if_local(index, task, plan, vshard_id);
                }
            }
            Err(error) => self.fail(|| PartsError::PartUndecodable {
                part: index,
                source: Box::new(error),
            }),
        }
    }

    /// Take part `index`, one chunk of task `task`.
    fn take_chunk(
        &mut self,
        index: u32,
        task: u32,
        bytes: &[u8],
        chunk: TaskChunk,
        vshard_id: u32,
    ) {
        let continues = match &self.spanning {
            None => chunk.offset == 0,
            Some(spanning) => {
                spanning.task == task
                    && spanning.bytes.len() as u64 == chunk.offset
                    && spanning.total_len == chunk.total_len
            }
        };
        if !continues {
            self.fail(|| PartsError::BrokenChunks { part: index, task });
            self.spanning = None;
            return;
        }
        let spanning = self.spanning.get_or_insert_with(|| SpanningTask {
            task,
            bytes: Vec::new(),
            total_len: chunk.total_len,
        });
        spanning.bytes.extend_from_slice(bytes);
        let len = spanning.bytes.len() as u64;
        if len < spanning.total_len {
            return;
        }
        let Some(whole) = self.spanning.take() else {
            return;
        };
        if len > whole.total_len {
            self.fail(|| PartsError::ChunksOverrun {
                task,
                total_len: whole.total_len,
            });
            return;
        }
        match nodedb_physical::physical_plan::wire::decode(&whole.bytes) {
            Ok(plan) => self.keep_if_local(index, task, plan, vshard_id),
            Err(source) => self.fail(|| PartsError::TaskUndecodable { task, source }),
        }
    }
}

/// A txn holding its locks while its parts arrive.
#[derive(Debug)]
struct AwaitingParts {
    txn: SequencedTxn,
    lock_owner: TxnId,
}

/// The multi-part txns this vShard participates in and has not staged.
#[derive(Debug, Default)]
pub(in crate::control::cluster::calvin::scheduler::driver::core) struct PartsState {
    assemblies: BTreeMap<TxnId, Assembly>,
    awaiting: BTreeMap<TxnId, AwaitingParts>,
    /// The inputs a closed gate still takes (see [`super::parts_lane`]).
    pub(in crate::control::cluster::calvin::scheduler::driver::core) lane:
        super::parts_lane::PartsLane,
}

impl PartsState {
    /// Whether `txn_id` holds its locks and waits for parts.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn is_awaiting(
        &self,
        txn_id: TxnId,
    ) -> bool {
        self.awaiting.contains_key(&txn_id)
    }

    /// How many txns hold their locks and wait for parts.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn awaiting_len(
        &self,
    ) -> usize {
        self.awaiting.len()
    }

    /// Whether the assembly of `txn` is open: its header was taken here and
    /// its parts are not all assembled.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn has_assembly(
        &self,
        txn: TxnId,
    ) -> bool {
        self.assemblies.contains_key(&txn)
    }

    /// The lowest txn that holds its locks and waits for parts.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn lowest_awaiting(
        &self,
    ) -> Option<TxnId> {
        self.awaiting.keys().next().copied()
    }

    /// Stop waiting for the parts of `txn_id`, which finished without them:
    /// its redo installed from another replica's stage. Returns the owner of
    /// the locks it holds.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn release_awaiting(
        &mut self,
        txn_id: TxnId,
    ) -> Option<TxnId> {
        let awaiting = self.awaiting.remove(&txn_id)?;
        self.assemblies.remove(&txn_id);
        Some(awaiting.lock_owner)
    }
}

impl Scheduler {
    /// Open the assembly of `txn` when it is a multi-part txn this vShard
    /// has not seen.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn open_assembly(
        &mut self,
        txn: &SequencedTxn,
    ) {
        let Some(manifest) = &txn.tx_class.multi_part else {
            return;
        };
        let txn_id = TxnId::new(txn.epoch, txn.position);
        let vshard_id = self.vshard_id;
        self.parts
            .assemblies
            .entry(txn_id)
            .or_insert_with(|| Assembly {
                database_id: txn.tx_class.database_id,
                expected: manifest.parts_for(vshard_id),
                received: BTreeMap::new(),
                abandoned: false,
            });
    }

    /// Pass a ready txn to the dispatch, or hold it until its parts arrived.
    ///
    /// A multi-part txn with every part that targets this vShard is
    /// assembled here. One still missing parts waits with its locks. An
    /// abandoned one completes here. One whose part cannot be read votes
    /// abort on its rejected plans.
    ///
    /// Returns the txn to dispatch: a single-entry txn, or a multi-part one
    /// with every part that targets this vShard assembled. `None` means
    /// nothing dispatches: the txn waits for parts, or it completed.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn gate_on_parts(
        &mut self,
        mut txn: SequencedTxn,
        txn_id: TxnId,
        lock_owner: TxnId,
    ) -> Option<SequencedTxn> {
        if txn.tx_class.multi_part.is_none() {
            return Some(txn);
        }
        // Intake opens the assembly before the locks, and only this gate or
        // an abandonment of a txn holding its locks closes it.
        let (abandoned, missing) = match self.parts.assemblies.get(&txn_id) {
            Some(assembly) => (assembly.abandoned, !assembly.is_complete()),
            None => (false, false),
        };
        if abandoned {
            self.parts.assemblies.remove(&txn_id);
            self.on_unpending_txn_complete(txn_id, lock_owner);
            return None;
        }
        if missing {
            self.parts
                .awaiting
                .insert(txn_id, AwaitingParts { txn, lock_owner });
            return None;
        }
        let vshard_id = self.vshard_id;
        let assembled = match self.parts.assemblies.remove(&txn_id) {
            Some(assembly) => assemble(&mut txn, assembly, vshard_id),
            None => Err(PartsError::NoAssembly),
        };
        match assembled {
            Ok(()) => Some(txn),
            Err(detail) => {
                let error = crate::Error::Internal {
                    detail: format!(
                        "calvin multi-part txn {}/{} for vshard {}: {detail}",
                        txn_id.epoch, txn_id.position, self.vshard_id
                    ),
                };
                self.reject_plan(txn, txn_id, lock_owner, &error);
                None
            }
        }
    }

    /// Receive part `index` of `txn`, whose first task is the txn's task
    /// `first_task`. `chunk` is set on a part that holds one byte range of
    /// that task. The part is kept by its index, so any delivery order
    /// assembles alike. A part this vShard already holds, or of a txn it
    /// holds no assembly for, is ignored.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn receive_part(
        &mut self,
        txn: TxnIdWire,
        index: u32,
        first_task: u32,
        plans: Arc<Vec<u8>>,
        chunk: Option<TaskChunk>,
    ) {
        let txn_id = TxnId::from(txn);
        let Some(assembly) = self.parts.assemblies.get_mut(&txn_id) else {
            return;
        };
        // A catch-up replay delivers the parts that follow an abandonment.
        if assembly.abandoned || assembly.is_complete() || assembly.received.contains_key(&index) {
            return;
        }
        assembly.received.insert(
            index,
            ReceivedPart {
                first_task,
                plans,
                chunk,
            },
        );
        let complete = assembly.is_complete();
        if complete && let Some(awaiting) = self.parts.awaiting.remove(&txn_id) {
            self.dispatch_or_barrier(awaiting.txn, txn_id, awaiting.lock_owner);
        }
    }

    /// The sequencer abandoned `txn`: it lost parts. A txn holding its locks
    /// completes at once. One still waiting for locks completes when granted.
    ///
    /// An abandonment that follows every part targeting this vShard is
    /// ignored. The sequencer applies an abandonment only while the txn is
    /// open, and a catch-up replay delivers one it ignored. Here the verdict
    /// decides instead: the registry holds `Abort(PartsLost)` when the
    /// sequencer applied the abandonment, and the staged txn reads it.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn receive_parts_abandoned(
        &mut self,
        txn: TxnIdWire,
    ) {
        let txn_id = TxnId::from(txn);
        let Some(assembly) = self.parts.assemblies.get_mut(&txn_id) else {
            return;
        };
        if assembly.is_complete() {
            return;
        }
        assembly.abandoned = true;
        warn!(
            vshard_id = self.vshard_id,
            epoch = txn_id.epoch,
            position = txn_id.position,
            "calvin scheduler: multi-part txn abandoned before its parts arrived"
        );
        if let Some(awaiting) = self.parts.awaiting.remove(&txn_id) {
            self.parts.assemblies.remove(&txn_id);
            self.on_unpending_txn_complete(txn_id, awaiting.lock_owner);
        }
    }
}

/// The single-entry form of `txn`: its local tasks as `plans`, in their
/// order in the whole txn, and `body_plans` as indexes into them. The
/// manifest stays for the whole-transaction facts.
///
/// Reads the parts in index order, so the result depends only on their
/// contents. Every replica that holds the same parts assembles the same
/// plans, or fails alike. A failure leaves `txn` unchanged.
fn assemble(txn: &mut SequencedTxn, assembly: Assembly, vshard_id: u32) -> Result<(), PartsError> {
    let mut assembler = Assembler {
        database_id: assembly.database_id,
        local: Vec::new(),
        spanning: None,
        failed: None,
    };
    for (index, part) in &assembly.received {
        match part.chunk {
            Some(chunk) => {
                assembler.take_chunk(*index, part.first_task, &part.plans, chunk, vshard_id);
            }
            None => assembler.take_tasks(*index, part.first_task, &part.plans, vshard_id),
        }
    }
    if let Some(failed) = assembler.failed {
        return Err(failed);
    }
    if let Some(spanning) = assembler.spanning {
        return Err(PartsError::Unfinished {
            task: spanning.task,
            received: spanning.bytes.len(),
            total_len: spanning.total_len,
        });
    }
    let mut local = assembler.local;
    local.sort_by_key(|(task, _)| *task);
    let body_plans: Vec<u32> = (0u32..)
        .zip(&local)
        .filter(|(_, (task, _))| txn.tx_class.body_plans.contains(task))
        .map(|(local_index, _)| local_index)
        .collect();
    let plans: Vec<&PhysicalPlan> = local.iter().map(|(_, plan)| plan).collect();
    let bytes = zerompk::to_msgpack_vec(&plans)
        .map_err(|source| PartsError::PlansDoNotEncode { source })?;
    txn.tx_class.plans = bytes;
    txn.tx_class.body_plans = body_plans;
    Ok(())
}

#[cfg(test)]
mod tests {
    use nodedb_cluster::calvin::types::{
        MultiPartPlans, PartStreamId, SchedulerInput, VShardParts,
    };
    use nodedb_cluster::calvin::{AbortReason, CalvinCompletionRegistry, SequencerEntry};
    use nodedb_physical::physical_plan::DocumentOp;
    use nodedb_physical::physical_plan::wire as plan_wire;
    use nodedb_types::QualifiedCollection;

    use super::*;
    use crate::bridge::dispatch::CoreChannelDataSide;
    use crate::bridge::envelope::Status;
    use crate::control::cluster::calvin::scheduler::driver::core::owed::OwedKind;
    use crate::control::cluster::calvin::scheduler::driver::core::test_proposer::{
        CapturingProposer, lead_data_group,
    };
    use crate::control::cluster::calvin::scheduler::driver::core::test_support::{
        build_test_scheduler_with_data_side, make_local_write_txn, staged_response,
        test_coll_vshard,
    };
    use crate::control::cluster::calvin::scheduler::driver::types::CommitState;
    use crate::types::DatabaseId;

    fn truncate_plan() -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::Truncate {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "test_coll"),
            restart_identity: false,
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        })
    }

    /// A single-entry truncate of `test_coll` at `(epoch, position)`. Each
    /// test's epoch holds two txns on this vShard, so the epoch folds only
    /// once both completed.
    fn plain_write(epoch: u64, position: u32) -> SequencedTxn {
        let mut txn = make_local_write_txn(epoch, position);
        txn.epoch_vshard_txn_count = 2;
        txn
    }

    /// The header of a txn of `parts` parts carrying `tasks` tasks, every
    /// part targeting the `test_coll` vShard.
    fn header(epoch: u64, position: u32, parts: u32, tasks: u32) -> SequencedTxn {
        let mut txn = plain_write(epoch, position);
        txn.tx_class.plans = Vec::new();
        txn.tx_class.multi_part = Some(MultiPartPlans {
            stream: PartStreamId { node: 1, seq: 1 },
            part_count: parts,
            total_tasks: tasks,
            user_write: true,
            client_write: true,
            per_vshard: vec![VShardParts {
                vshard: test_coll_vshard(),
                parts,
            }],
        });
        txn
    }

    /// The header of a two-part txn: one truncate task per part.
    fn two_part_header(epoch: u64, position: u32) -> SequencedTxn {
        header(epoch, position, 2, 2)
    }

    fn part(epoch: u64, position: u32, index: u32, plans: Vec<u8>) -> SchedulerInput {
        SchedulerInput::TxnPart {
            txn: TxnIdWire { epoch, position },
            index,
            first_task: index,
            plans: Arc::new(plans),
            chunk: None,
        }
    }

    fn chunk_part(
        epoch: u64,
        index: u32,
        task: u32,
        bytes: &[u8],
        offset: u64,
        total_len: u64,
    ) -> SchedulerInput {
        SchedulerInput::TxnPart {
            txn: TxnIdWire { epoch, position: 0 },
            index,
            first_task: task,
            plans: Arc::new(bytes.to_vec()),
            chunk: Some(TaskChunk { offset, total_len }),
        }
    }

    fn one_truncate() -> Vec<u8> {
        plan_wire::encode_batch(&vec![truncate_plan()]).expect("encode one truncate")
    }

    /// A scheduler on the `test_coll` vShard that leads its data group and
    /// captures its proposals.
    fn leading_scheduler() -> (
        Scheduler,
        tempfile::TempDir,
        CoreChannelDataSide,
        Arc<CapturingProposer>,
    ) {
        let (mut scheduler, dir, data_side) = build_test_scheduler_with_data_side(
            test_coll_vshard(),
            CalvinCompletionRegistry::new_detached(),
        );
        let proposer = CapturingProposer::accepting();
        scheduler.sequencer_proposer = proposer.clone();
        lead_data_group(&mut scheduler);
        (scheduler, dir, data_side, proposer)
    }

    /// Whether a truncate of `test_coll` reached the Data Plane. Drains the
    /// request ring.
    fn truncate_reached_data_plane(data_side: &mut CoreChannelDataSide) -> bool {
        let mut reached = false;
        while let Ok(request) = data_side.request_rx.try_pop() {
            reached |= request.inner.plan == truncate_plan();
        }
        reached
    }

    /// `txn_id` is rejected before it stages. It waits in `pending` for its
    /// verdict with a stage error, owes a `PlanRejected` vote, and holds its
    /// locks: `next`, on the same key, waits behind it. At the abort verdict
    /// it drops, marks its position applied, and frees its locks, so `next`
    /// stages.
    async fn rejected_then_dropped_at_its_verdict(
        scheduler: &mut Scheduler,
        data_side: &mut CoreChannelDataSide,
        proposer: &CapturingProposer,
        txn_id: TxnId,
        next: SequencedTxn,
    ) {
        let pending = scheduler
            .pending
            .get(&txn_id)
            .expect("waits for its verdict");
        assert_eq!(pending.commit_state, CommitState::AwaitingVerdict);
        assert!(pending.stage_error.is_some(), "the rejection is recorded");
        assert!(!scheduler.parts.is_awaiting(txn_id));
        assert!(scheduler.owed.contains_key(&(txn_id, OwedKind::Vote)));
        assert!(proposer.accepted().contains(&SequencerEntry::AbortVote {
            epoch: txn_id.epoch,
            position: txn_id.position,
            vshard: test_coll_vshard(),
            reason: AbortReason::PlanRejected,
        }));
        assert!(!scheduler.applied.is_applied(txn_id.epoch, txn_id.position));

        let next_id = TxnId::new(next.epoch, next.position);
        scheduler.process_scheduler_input(SchedulerInput::Txn(Box::new(next)));
        assert!(
            scheduler.blocked.contains_key(&next_id),
            "the rejected txn holds its locks"
        );

        scheduler.registry.note_verdict(
            nodedb_cluster::calvin::TxnId::new(txn_id.epoch, txn_id.position),
            nodedb_cluster::calvin::VerdictOutcome::Abort(AbortReason::PlanRejected),
        );
        scheduler.resume_on_verdict(txn_id, false);
        assert_eq!(
            scheduler.pending.get(&txn_id).map(|p| p.commit_state),
            Some(CommitState::AwaitingDrop)
        );
        assert!(
            !truncate_reached_data_plane(data_side),
            "no plan of the rejected txn stages"
        );
        assert!(!scheduler.applied.is_applied(txn_id.epoch, txn_id.position));

        scheduler.finish_drop(txn_id, &staged_response(Status::Ok, None));
        assert!(!scheduler.pending.contains_key(&txn_id));
        assert!(scheduler.applied.is_applied(txn_id.epoch, txn_id.position));
        assert!(
            proposer.accepted().iter().any(|entry| matches!(
                entry,
                SequencerEntry::CompletionAck { epoch, position, .. }
                    if *epoch == txn_id.epoch && *position == txn_id.position
            )),
            "the dropped txn acks"
        );
        assert!(!scheduler.blocked.contains_key(&next_id));
        assert!(
            scheduler.pending.contains_key(&next_id),
            "the freed locks let the next txn stage"
        );
    }

    /// A txn whose locks are granted before its parts holds them and waits.
    /// It stages once the last part that targets this vShard arrived, with
    /// every local task of every part.
    #[tokio::test]
    async fn a_multi_part_txn_stages_once_every_part_arrived() {
        let (mut scheduler, _dir, _data_side) = build_test_scheduler_with_data_side(
            test_coll_vshard(),
            CalvinCompletionRegistry::new_detached(),
        );
        let txn_id = TxnId::new(3, 0);
        scheduler.process_scheduler_input(SchedulerInput::Txn(Box::new(two_part_header(3, 0))));
        assert!(
            scheduler.parts.is_awaiting(txn_id),
            "locks held, parts missing"
        );
        assert!(!scheduler.pending.contains_key(&txn_id));

        scheduler.process_scheduler_input(part(3, 0, 0, one_truncate()));
        assert!(
            scheduler.parts.is_awaiting(txn_id),
            "one part still missing"
        );

        scheduler.process_scheduler_input(part(3, 0, 1, one_truncate()));
        assert!(!scheduler.parts.is_awaiting(txn_id));
        let pending = scheduler.pending.get(&txn_id).expect("staged");
        let plans = plan_wire::decode_batch(&pending.txn.tx_class.plans).expect("decode");
        assert_eq!(plans.len(), 2, "both parts' tasks stage together");
    }

    /// A task split over two chunk parts decodes whole once both arrived,
    /// and stages beside the whole task of the part before it.
    #[tokio::test]
    async fn a_task_split_over_chunk_parts_stages_whole() {
        let (mut scheduler, _dir, _data_side) = build_test_scheduler_with_data_side(
            test_coll_vshard(),
            CalvinCompletionRegistry::new_detached(),
        );
        let txn_id = TxnId::new(7, 0);
        let task = zerompk::to_msgpack_vec(&truncate_plan()).expect("encode one task");
        let half = task.len() / 2;
        let total = task.len() as u64;
        scheduler.process_scheduler_input(SchedulerInput::Txn(Box::new(header(7, 0, 3, 2))));
        scheduler.process_scheduler_input(part(7, 0, 0, one_truncate()));
        scheduler.process_scheduler_input(chunk_part(7, 1, 1, &task[..half], 0, total));
        assert!(scheduler.parts.is_awaiting(txn_id), "the task is half here");
        scheduler.process_scheduler_input(chunk_part(7, 2, 1, &task[half..], half as u64, total));

        let pending = scheduler.pending.get(&txn_id).expect("staged");
        let plans = plan_wire::decode_batch(&pending.txn.tx_class.plans).expect("decode");
        assert_eq!(plans, vec![truncate_plan(), truncate_plan()]);
    }

    /// Chunk parts that arrive out of order, and again, assemble exactly as
    /// in order: the assembly reads parts by index, never by arrival. A
    /// replica whose live channel and catch-up replay interleave its parts
    /// stages the same plans as every other replica.
    #[tokio::test]
    async fn chunk_parts_out_of_order_and_repeated_stage_whole() {
        let (mut scheduler, _dir, _data_side) = build_test_scheduler_with_data_side(
            test_coll_vshard(),
            CalvinCompletionRegistry::new_detached(),
        );
        let txn_id = TxnId::new(9, 0);
        let task = zerompk::to_msgpack_vec(&truncate_plan()).expect("encode one task");
        let third = task.len() / 3;
        let total = task.len() as u64;
        scheduler.process_scheduler_input(SchedulerInput::Txn(Box::new(header(9, 0, 3, 1))));
        scheduler.process_scheduler_input(chunk_part(
            9,
            1,
            0,
            &task[third..2 * third],
            third as u64,
            total,
        ));
        scheduler.process_scheduler_input(chunk_part(9, 0, 0, &task[..third], 0, total));
        scheduler.process_scheduler_input(chunk_part(
            9,
            1,
            0,
            &task[third..2 * third],
            third as u64,
            total,
        ));
        assert!(scheduler.parts.is_awaiting(txn_id), "one chunk missing");
        scheduler.process_scheduler_input(chunk_part(
            9,
            2,
            0,
            &task[2 * third..],
            2 * third as u64,
            total,
        ));

        let pending = scheduler.pending.get(&txn_id).expect("staged");
        let plans = plan_wire::decode_batch(&pending.txn.tx_class.plans).expect("decode");
        assert_eq!(plans, vec![truncate_plan()]);
    }

    /// A chunk that skips bytes rejects the txn before it stages. It votes
    /// abort and drops at the verdict.
    #[tokio::test]
    async fn a_chunk_that_skips_bytes_is_rejected_before_it_stages() {
        let (mut scheduler, _dir, mut data_side, proposer) = leading_scheduler();
        let txn_id = TxnId::new(8, 0);
        let task = zerompk::to_msgpack_vec(&truncate_plan()).expect("encode one task");
        let total = task.len() as u64;
        scheduler.process_scheduler_input(SchedulerInput::Txn(Box::new(header(8, 0, 2, 1))));
        scheduler.process_scheduler_input(chunk_part(8, 0, 0, &task[..2], 0, total));
        scheduler.process_scheduler_input(chunk_part(8, 1, 0, &task[3..], 3, total));

        rejected_then_dropped_at_its_verdict(
            &mut scheduler,
            &mut data_side,
            &proposer,
            txn_id,
            plain_write(8, 1),
        )
        .await;
    }

    /// A late part that does not decode rejects the txn before it stages:
    /// the earlier part's task never reaches the Data Plane. The txn votes
    /// abort and drops at the verdict.
    #[tokio::test]
    async fn a_late_part_that_fails_rejects_the_txn_before_the_earlier_parts_stage() {
        let (mut scheduler, _dir, mut data_side, proposer) = leading_scheduler();
        let txn_id = TxnId::new(3, 0);
        scheduler.process_scheduler_input(SchedulerInput::Txn(Box::new(two_part_header(3, 0))));
        scheduler.process_scheduler_input(part(3, 0, 0, one_truncate()));
        scheduler.process_scheduler_input(part(3, 0, 1, vec![0xc1, 0xff, 0x00]));

        rejected_then_dropped_at_its_verdict(
            &mut scheduler,
            &mut data_side,
            &proposer,
            txn_id,
            plain_write(3, 1),
        )
        .await;
    }

    /// An abandoned txn holding its locks completes at once and frees them.
    #[tokio::test]
    async fn an_abandoned_txn_holding_its_locks_completes_and_frees_them() {
        let (mut scheduler, _dir, _data_side) = build_test_scheduler_with_data_side(
            test_coll_vshard(),
            CalvinCompletionRegistry::new_detached(),
        );
        let txn_id = TxnId::new(4, 0);
        scheduler.process_scheduler_input(SchedulerInput::Txn(Box::new(two_part_header(4, 0))));
        scheduler.process_scheduler_input(part(4, 0, 0, one_truncate()));
        scheduler.process_scheduler_input(SchedulerInput::PartsAbandoned {
            txn: TxnIdWire {
                epoch: 4,
                position: 0,
            },
        });

        assert!(!scheduler.parts.is_awaiting(txn_id));
        assert!(!scheduler.pending.contains_key(&txn_id));
        assert!(scheduler.applied.is_applied(4, 0));
        // A part that arrives after the abandonment is ignored.
        scheduler.process_scheduler_input(part(4, 0, 1, one_truncate()));
        assert!(!scheduler.pending.contains_key(&txn_id));

        scheduler.process_scheduler_input(SchedulerInput::Txn(Box::new(plain_write(4, 1))));
        assert!(scheduler.pending.contains_key(&TxnId::new(4, 1)));
    }

    /// An abandonment after every part that targets this vShard is ignored:
    /// a catch-up replay delivers one the sequencer ignored. A part replayed
    /// after an abandonment the sequencer applied is ignored too.
    #[tokio::test]
    async fn an_abandonment_after_the_last_local_part_leaves_the_txn_to_its_verdict() {
        let (mut scheduler, _dir, _data_side) = build_test_scheduler_with_data_side(
            test_coll_vshard(),
            CalvinCompletionRegistry::new_detached(),
        );
        let holder = TxnId::new(6, 0);
        let multi = TxnId::new(6, 1);
        scheduler.process_scheduler_input(SchedulerInput::Txn(Box::new(plain_write(6, 0))));
        scheduler.process_scheduler_input(SchedulerInput::Txn(Box::new(two_part_header(6, 1))));
        scheduler.process_scheduler_input(part(6, 1, 0, one_truncate()));
        scheduler.process_scheduler_input(part(6, 1, 1, one_truncate()));
        scheduler.process_scheduler_input(SchedulerInput::PartsAbandoned {
            txn: TxnIdWire {
                epoch: 6,
                position: 1,
            },
        });

        scheduler.pending.remove(&holder);
        scheduler.on_unpending_txn_complete(holder, holder);
        assert!(
            scheduler.pending.contains_key(&multi),
            "stages with its parts"
        );
    }

    /// An abandoned txn still waiting for its locks completes when they are
    /// granted, and stages nothing.
    #[tokio::test]
    async fn an_abandoned_txn_waiting_for_locks_completes_when_granted() {
        let (mut scheduler, _dir, _data_side) = build_test_scheduler_with_data_side(
            test_coll_vshard(),
            CalvinCompletionRegistry::new_detached(),
        );
        let holder = TxnId::new(5, 0);
        let multi = TxnId::new(5, 1);
        scheduler.process_scheduler_input(SchedulerInput::Txn(Box::new(plain_write(5, 0))));
        assert!(scheduler.pending.contains_key(&holder));
        scheduler.process_scheduler_input(SchedulerInput::Txn(Box::new(two_part_header(5, 1))));
        assert!(
            scheduler.blocked.contains_key(&multi),
            "waits for the holder's locks"
        );
        scheduler.process_scheduler_input(SchedulerInput::PartsAbandoned {
            txn: TxnIdWire {
                epoch: 5,
                position: 1,
            },
        });
        assert!(
            scheduler.blocked.contains_key(&multi),
            "still waits for its locks"
        );

        scheduler.pending.remove(&holder);
        scheduler.on_unpending_txn_complete(holder, holder);
        assert!(!scheduler.blocked.contains_key(&multi));
        assert!(!scheduler.pending.contains_key(&multi), "stages nothing");
        assert!(scheduler.applied.is_applied(5, 1));
    }
}
