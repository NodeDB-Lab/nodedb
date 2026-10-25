// SPDX-License-Identifier: BUSL-1.1

//! Read a multi-part txn's parts, in index order, into its local plans.
//!
//! - A whole-task part names each task's homes. This vShard decodes only the
//!   tasks whose homes name it, then checks each one routes to exactly those
//!   homes.
//! - A chunk part holds one byte range of one task. The task decodes from its
//!   joined chunks, and is kept when it routes here.
//! - The result depends only on the parts' contents. Every replica holding
//!   the same parts assembles the same plans, or fails alike.

use std::collections::BTreeMap;
use std::sync::Arc;

use nodedb_cluster::calvin::types::{LocalPlans, PartTasks, SequencedTxn, TaskChunk};
use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_physical::physical_plan::wire as plan_wire;

use super::parts_error::PartsError;
use super::routing::{PlanRouting, plan_vshard_in_database};

/// One part as it arrived: its first task, its bytes, and its chunk.
#[derive(Debug)]
pub(super) struct ReceivedPart {
    pub(super) first_task: u32,
    pub(super) plans: Arc<Vec<u8>>,
    pub(super) chunk: Option<TaskChunk>,
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
    vshard_id: u32,
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

    /// The vShards task `task` of part `index` routes to, sorted, with no
    /// repeats. Fails when the task routes to no vShard.
    fn routed_homes(
        &self,
        index: u32,
        task: u32,
        plan: &PhysicalPlan,
    ) -> Result<Vec<u32>, PartsError> {
        match plan_vshard_in_database(plan, self.database_id) {
            PlanRouting::Vshards(vshards) => {
                let mut routed: Vec<u32> = vshards.iter().map(|v| v.as_u32()).collect();
                routed.sort_unstable();
                routed.dedup();
                Ok(routed)
            }
            PlanRouting::ControlPlaneOnly => {
                Err(PartsError::ControlPlaneOnly { part: index, task })
            }
            PlanRouting::NotAWrite => Err(PartsError::NotAWrite { part: index, task }),
            PlanRouting::Unroutable(reason) => Err(PartsError::Unroutable {
                part: index,
                task,
                reason,
            }),
        }
    }

    /// Keep `plan`, the txn's task `task` from part `index`, when it routes
    /// to exactly `homes`. The part named `homes`, so they hold this vShard.
    fn keep_homed(&mut self, index: u32, task: u32, plan: PhysicalPlan, homes: &[u32]) {
        match self.routed_homes(index, task, &plan) {
            Ok(routed) if routed == homes => self.local.push((task, plan)),
            Ok(_) => self.fail(|| PartsError::HomesMismatch { part: index, task }),
            Err(error) => self.fail(|| error),
        }
    }

    /// Keep `plan`, the txn's task `task` from part `index`, when it routes
    /// to this vShard.
    fn keep_if_local(&mut self, index: u32, task: u32, plan: PhysicalPlan) {
        match self.routed_homes(index, task, &plan) {
            Ok(routed) => {
                if routed.binary_search(&self.vshard_id).is_ok() {
                    self.local.push((task, plan));
                }
            }
            Err(error) => self.fail(|| error),
        }
    }

    /// Take part `index`, whole tasks from task `first_task`. Decodes only
    /// the tasks whose homes name this vShard.
    fn take_tasks(&mut self, index: u32, first_task: u32, plans: &[u8]) {
        if let Some(spanning) = &self.spanning {
            let task = spanning.task;
            self.fail(|| PartsError::InterruptsChunks { part: index, task });
            return;
        }
        let tasks = match PartTasks::decode(plans) {
            Ok(tasks) => tasks,
            Err(source) => {
                self.fail(|| PartsError::PartTasksInvalid {
                    part: index,
                    source,
                });
                return;
            }
        };
        for (offset, bytes) in tasks.tasks_for(self.vshard_id) {
            let task = u32::try_from(offset)
                .ok()
                .and_then(|offset| first_task.checked_add(offset));
            let Some(task) = task else {
                self.fail(|| PartsError::TaskIndexOverflow { part: index });
                return;
            };
            match plan_wire::decode(bytes) {
                Ok(plan) => self.keep_homed(index, task, plan, tasks.homes(offset)),
                Err(source) => self.fail(|| PartsError::TaskUndecodable { task, source }),
            }
        }
    }

    /// Take part `index`, one chunk of task `task`.
    fn take_chunk(&mut self, index: u32, task: u32, bytes: &[u8], chunk: TaskChunk) {
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
        match plan_wire::decode(&whole.bytes) {
            Ok(plan) => self.keep_if_local(index, task, plan),
            Err(source) => self.fail(|| PartsError::TaskUndecodable { task, source }),
        }
    }
}

/// Set `txn`'s local plans: its local tasks as a plan batch, in their order
/// in the whole txn, and `body_plans` as sorted indexes into them. The
/// shared class stays as it is, with the manifest's whole-txn facts.
///
/// Reads `received` in index order. A failure leaves `txn` unchanged.
pub(super) fn assemble(
    txn: &mut SequencedTxn,
    database_id: crate::types::DatabaseId,
    received: &BTreeMap<u32, ReceivedPart>,
    vshard_id: u32,
) -> Result<(), PartsError> {
    let mut assembler = Assembler {
        database_id,
        vshard_id,
        local: Vec::new(),
        spanning: None,
        failed: None,
    };
    for (index, part) in received {
        match part.chunk {
            Some(chunk) => assembler.take_chunk(*index, part.first_task, &part.plans, chunk),
            None => assembler.take_tasks(*index, part.first_task, &part.plans),
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
    if let Some(pair) = local.windows(2).find(|pair| pair[0].0 == pair[1].0) {
        return Err(PartsError::TaskRepeated { task: pair[0].0 });
    }
    let class_body = txn.body_plans();
    let body_plans: Vec<u32> = (0u32..)
        .zip(&local)
        .filter(|(_, (task, _))| class_body.binary_search(task).is_ok())
        .map(|(local_index, _)| local_index)
        .collect();
    let plans: Vec<&PhysicalPlan> = local.iter().map(|(_, plan)| plan).collect();
    let bytes = zerompk::to_msgpack_vec(&plans)
        .map_err(|source| PartsError::PlansDoNotEncode { source })?;
    txn.local_plans = Some(Arc::new(LocalPlans {
        plans: bytes,
        body_plans,
    }));
    Ok(())
}
