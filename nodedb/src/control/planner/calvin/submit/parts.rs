// SPDX-License-Identifier: BUSL-1.1

//! Split a Calvin transaction's plans into sequencer parts.
//!
//! One sequencer entry carries at most `max_plans_bytes_per_txn` plan bytes
//! for at most `max_participating_vshards_per_txn` vShards. A transaction
//! over either travels as a multi-part transaction instead: a header with its
//! full read and write sets, and parts the coordinator streams to the
//! sequencer leader after it. The transaction stays one transaction: one
//! sequence position, every lock from the header to the last part's apply,
//! one verdict. No cap bounds its size.
//!
//! - A part holds consecutive whole tasks, within both caps.
//! - A task whose encoding alone is over the byte cap travels as a run of
//!   chunk parts, each one byte range of it and nothing else.
//! - A part targets the vShards its tasks route to, by the routing every
//!   participant's scheduler applies, so a participant receives exactly the
//!   parts that hold its tasks.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};

use nodedb_cluster::calvin::sequencer::config::SequencerConfig;
use nodedb_cluster::calvin::types::{
    MultiPartPlans, PartStreamId, PartTasksBuilder, PlanPart, StreamedPart, TaskChunk, TxClass,
    VShardParts,
};
use nodedb_physical::physical_plan::PhysicalPlan;

use super::edge_slices::SlicedPlans;
use super::stream::PartStream;
use crate::Error;
use crate::control::cluster::calvin::scheduler::driver::core::routing::{
    PlanRouting, plan_vshard_in_database,
};
use crate::control::state::SharedState;

/// The next stream sequence of this process. It starts at the wall clock in
/// nanoseconds, so a restarted coordinator never reuses a name its earlier
/// process streamed under.
static NEXT_STREAM_SEQ: LazyLock<AtomicU64> = LazyLock::new(|| {
    // no-determinism: names a coordinator-local stream, never in the log.
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX))
        .unwrap_or(0);
    AtomicU64::new(now)
});

/// Carry `tx_class`'s plans as parts when one sequencer entry cannot, and
/// return the parts to stream after the header. `sliced` holds the class's
/// decoded plans after edge slicing. Sliced plans replace the class's plan
/// bytes, encoded once. A class that fits one entry, or that is already
/// split into parts, streams nothing.
pub(crate) fn split_into_parts(
    state: &SharedState,
    tx_class: &mut TxClass,
    sliced: &SlicedPlans,
) -> crate::Result<Option<PartStream>> {
    if tx_class.is_multi_part() {
        return Ok(None);
    }
    if sliced.sliced {
        tx_class.plans = nodedb_physical::physical_plan::wire::encode_batch(&sliced.plans)
            .map_err(|e| Error::Serialization {
                format: "msgpack".into(),
                detail: format!("calvin edge slicing: plan encode: {e}"),
            })?;
    }
    let limits = SequencerConfig::default();
    if tx_class.plans.len() <= limits.max_plans_bytes_per_txn
        && tx_class.participating_vshards().len() <= limits.max_participating_vshards_per_txn
    {
        return Ok(None);
    }
    // The one call that encodes a submit's parts.
    let (mut manifest, parts) = plan_parts(
        &sliced.plans,
        tx_class.database_id,
        &tx_class.body_plans,
        PartLimits {
            max_bytes: limits.max_plans_bytes_per_txn,
            max_targets: limits.max_participating_vshards_per_txn,
        },
    )?;
    let id = PartStreamId {
        node: state.node_id,
        seq: NEXT_STREAM_SEQ.fetch_add(1, Ordering::Relaxed),
    };
    manifest.stream = id;
    tx_class.plans = Vec::new();
    tx_class.multi_part = Some(manifest);
    Ok(Some(PartStream { id, parts }))
}

/// The caps [`plan_parts`] packs within.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PartLimits {
    pub max_bytes: usize,
    pub max_targets: usize,
}

/// Pack `plans` into parts, each within `limits`, in task order, and the
/// manifest that names them. `body_plans` are the indexes of the tasks a
/// trigger body buffered, sorted and deduplicated. The manifest's stream is
/// left for the caller.
pub(crate) fn plan_parts(
    plans: &[PhysicalPlan],
    database_id: crate::types::DatabaseId,
    body_plans: &[u32],
    limits: PartLimits,
) -> crate::Result<(MultiPartPlans, Vec<StreamedPart>)> {
    let total_tasks = u32::try_from(plans.len()).map_err(|_| Error::BadRequest {
        detail: format!(
            "a transaction of {} tasks is more than one transaction indexes",
            plans.len()
        ),
    })?;
    let mut packer = Packer::new(limits);
    for (task, plan) in (0u32..).zip(plans) {
        let homes = task_homes(plan, database_id, task)?;
        let encoded = zerompk::to_msgpack_vec(plan).map_err(|e| Error::Serialization {
            format: "msgpack".into(),
            detail: format!("calvin part split: task {task} encode: {e}"),
        })?;
        packer.add(task, homes, encoded)?;
    }
    let parts = packer.finish()?;
    let part_count = u32::try_from(parts.len()).map_err(|_| Error::BadRequest {
        detail: format!(
            "a transaction of {} parts is more than one transaction indexes",
            parts.len()
        ),
    })?;
    let mut per_vshard: BTreeMap<u32, u32> = BTreeMap::new();
    for part in &parts {
        for target in &part.targets {
            *per_vshard.entry(*target).or_insert(0) += 1;
        }
    }
    let manifest = MultiPartPlans {
        stream: PartStreamId::default(),
        part_count,
        total_tasks,
        user_write: crate::control::planner::calvin::write_class::plans_have_user_write(plans),
        client_write: (0..total_tasks).any(|task| body_plans.binary_search(&task).is_err()),
        per_vshard: per_vshard
            .into_iter()
            .map(|(vshard, parts)| VShardParts { vshard, parts })
            .collect(),
    };
    Ok((manifest, parts))
}

/// The vShards task `task` routes to, sorted.
fn task_homes(
    plan: &PhysicalPlan,
    database_id: crate::types::DatabaseId,
    task: u32,
) -> crate::Result<BTreeSet<u32>> {
    match plan_vshard_in_database(plan, database_id) {
        PlanRouting::Vshards(vshards) => Ok(vshards.iter().map(|v| v.as_u32()).collect()),
        PlanRouting::ControlPlaneOnly => Err(unroutable(task, "a control-plane-only plan")),
        PlanRouting::NotAWrite => Err(unroutable(task, "not a write")),
        PlanRouting::Unroutable(reason) => Err(unroutable(task, reason)),
    }
}

fn unroutable(task: u32, reason: &str) -> Error {
    Error::Internal {
        detail: format!("calvin part split: task {task} does not route to a vShard: {reason}"),
    }
}

/// Packs consecutive tasks into parts.
struct Packer {
    limits: PartLimits,
    parts: Vec<StreamedPart>,
    first_task: u32,
    tasks: PartTasksBuilder,
    homes: BTreeSet<u32>,
}

impl Packer {
    fn new(limits: PartLimits) -> Self {
        Self {
            limits,
            parts: Vec::new(),
            first_task: 0,
            tasks: PartTasksBuilder::new(),
            homes: BTreeSet::new(),
        }
    }

    fn add(&mut self, task: u32, homes: BTreeSet<u32>, encoded: Vec<u8>) -> crate::Result<()> {
        // A task routes to its one home, or to the two homes of an edge.
        if homes.len() > self.limits.max_targets {
            return Err(Error::Internal {
                detail: format!(
                    "calvin part split: task {task} routes to {} vShards, more than one entry \
                     targets",
                    homes.len()
                ),
            });
        }
        let task_homes: Vec<u32> = homes.iter().copied().collect();
        if PartTasksBuilder::new().encoded_len_with(encoded.len(), task_homes.len())
            > self.limits.max_bytes
        {
            if !self.tasks.is_empty() {
                self.close()?;
            }
            self.push_chunks(task, &task_homes, &encoded);
            return Ok(());
        }
        let widened = self.homes.union(&homes).count();
        if !self.tasks.is_empty()
            && (self.tasks.encoded_len_with(encoded.len(), task_homes.len())
                > self.limits.max_bytes
                || widened > self.limits.max_targets)
        {
            self.close()?;
        }
        if self.tasks.is_empty() {
            self.first_task = task;
        }
        self.tasks
            .push(&encoded, &task_homes)
            .map_err(|e| Error::Internal {
                detail: format!("calvin part split: task {task}: {e}"),
            })?;
        self.homes.extend(homes);
        Ok(())
    }

    /// Push task `task`'s encoding as a run of chunk parts.
    fn push_chunks(&mut self, task: u32, homes: &[u32], encoded: &[u8]) {
        let total_len = encoded.len() as u64;
        let mut offset = 0u64;
        for bytes in encoded.chunks(self.limits.max_bytes) {
            self.push(
                homes.to_vec(),
                PlanPart {
                    first_task: task,
                    plans: bytes.to_vec(),
                    chunk: Some(TaskChunk { offset, total_len }),
                },
            );
            offset += bytes.len() as u64;
        }
    }

    fn push(&mut self, targets: Vec<u32>, part: PlanPart) {
        let index = u32::try_from(self.parts.len()).unwrap_or(u32::MAX);
        self.parts.push(StreamedPart {
            index,
            targets,
            part,
        });
    }

    /// Close the open part.
    fn close(&mut self) -> crate::Result<()> {
        let plans = self.tasks.take().map_err(|e| Error::Serialization {
            format: "msgpack".into(),
            detail: format!("calvin part split: part encode: {e}"),
        })?;
        let targets = std::mem::take(&mut self.homes).into_iter().collect();
        self.push(
            targets,
            PlanPart {
                first_task: self.first_task,
                plans,
                chunk: None,
            },
        );
        Ok(())
    }

    fn finish(mut self) -> crate::Result<Vec<StreamedPart>> {
        if !self.tasks.is_empty() {
            self.close()?;
        }
        if self.parts.len() > u32::MAX as usize {
            return Err(Error::BadRequest {
                detail: "a transaction of more parts than one transaction indexes".to_owned(),
            });
        }
        Ok(self.parts)
    }
}

#[cfg(test)]
mod tests {
    use nodedb_cluster::calvin::types::PartTasks;
    use nodedb_physical::physical_plan::DocumentOp;
    use nodedb_physical::physical_plan::wire as plan_wire;
    use nodedb_types::QualifiedCollection;

    use super::*;
    use crate::types::DatabaseId;

    const LIMITS: PartLimits = PartLimits {
        max_bytes: 1 << 20,
        max_targets: 64,
    };

    fn truncate(collection: &str) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::Truncate {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, collection),
            restart_identity: false,
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        })
    }

    fn homes(plan: &PhysicalPlan) -> BTreeSet<u32> {
        task_homes(plan, DatabaseId::DEFAULT, 0).expect("routes")
    }

    /// Every part is within both caps, the parts carry every task in order,
    /// each part targets exactly the homes of its tasks, and the manifest
    /// counts the parts of each vShard.
    fn assert_parts_cover(
        plans: &[PhysicalPlan],
        manifest: &MultiPartPlans,
        parts: &[StreamedPart],
    ) {
        assert_eq!(manifest.part_count as usize, parts.len());
        let mut decoded_tasks: Vec<PhysicalPlan> = Vec::new();
        let mut spanning: Vec<u8> = Vec::new();
        let mut counts: BTreeMap<u32, u32> = BTreeMap::new();
        for (index, streamed) in (0u32..).zip(parts) {
            assert_eq!(streamed.index, index);
            let part = &streamed.part;
            assert!(
                part.plans.len() <= LIMITS.max_bytes,
                "part over the byte cap"
            );
            assert!(
                streamed.targets.len() <= LIMITS.max_targets,
                "over the target cap"
            );
            assert!(
                streamed.targets.windows(2).all(|w| w[0] < w[1]),
                "targets sorted"
            );
            for target in &streamed.targets {
                *counts.entry(*target).or_insert(0) += 1;
            }
            let first = part.first_task as usize;
            match part.chunk {
                None => {
                    assert_eq!(first, decoded_tasks.len(), "parts are consecutive");
                    let tasks = PartTasks::decode(&part.plans).expect("part decodes");
                    let mut want: BTreeSet<u32> = BTreeSet::new();
                    for (bytes, named) in tasks.iter() {
                        let task = plan_wire::decode(bytes).expect("task decodes");
                        let routed: Vec<u32> = homes(&task).into_iter().collect();
                        assert_eq!(named, routed.as_slice(), "task homes are its routed homes");
                        want.extend(named.iter().copied());
                        decoded_tasks.push(task);
                    }
                    assert_eq!(
                        streamed.targets.iter().copied().collect::<BTreeSet<u32>>(),
                        want
                    );
                }
                Some(chunk) => {
                    assert_eq!(
                        chunk.offset as usize,
                        spanning.len(),
                        "chunks are contiguous"
                    );
                    spanning.extend_from_slice(&part.plans);
                    if spanning.len() as u64 == chunk.total_len {
                        assert_eq!(first, decoded_tasks.len(), "the task is next");
                        let task = plan_wire::decode(&spanning).expect("task decodes");
                        assert_eq!(
                            streamed.targets,
                            homes(&task).into_iter().collect::<Vec<_>>()
                        );
                        decoded_tasks.push(task);
                        spanning.clear();
                    }
                }
            }
        }
        assert!(spanning.is_empty(), "no task is left part way");
        assert_eq!(decoded_tasks.as_slice(), plans, "every task, in order");
        let manifest_counts: BTreeMap<u32, u32> = manifest
            .per_vshard
            .iter()
            .map(|entry| (entry.vshard, entry.parts))
            .collect();
        assert_eq!(manifest_counts, counts);
    }

    #[test]
    fn tasks_over_many_vshards_split_at_the_target_cap() {
        let plans: Vec<PhysicalPlan> = (0..400).map(|i| truncate(&format!("c_{i}"))).collect();
        let distinct: BTreeSet<u32> = plans.iter().flat_map(homes).collect();
        assert!(
            distinct.len() > LIMITS.max_targets,
            "the tasks span over one entry"
        );

        let (manifest, parts) =
            plan_parts(&plans, DatabaseId::DEFAULT, &[], LIMITS).expect("splits");
        assert!(parts.len() >= 2);
        assert_parts_cover(&plans, &manifest, &parts);
        assert!(manifest.user_write);
        assert!(manifest.client_write);
    }

    #[test]
    fn plan_bytes_over_one_entry_split_at_the_byte_cap() {
        let name = "x".repeat(10_000);
        let plans: Vec<PhysicalPlan> = (0..300).map(|_| truncate(&name)).collect();
        let (manifest, parts) =
            plan_parts(&plans, DatabaseId::DEFAULT, &[], LIMITS).expect("splits");
        assert!(parts.len() >= 3);
        assert_parts_cover(&plans, &manifest, &parts);
    }

    /// A single task over one entry travels as a run of chunk parts
    /// between whole-task parts. Nothing refuses it.
    #[test]
    fn a_task_over_one_entry_is_split_across_chunk_parts() {
        let plans = vec![
            truncate("before"),
            truncate(&"x".repeat((5 << 20) / 2)),
            truncate("after"),
        ];
        let (manifest, parts) =
            plan_parts(&plans, DatabaseId::DEFAULT, &[], LIMITS).expect("splits");
        let chunks = parts.iter().filter(|p| p.part.chunk.is_some()).count();
        assert!(chunks >= 3, "a 2.5 MiB task over three chunk parts");
        assert_parts_cover(&plans, &manifest, &parts);
    }

    /// Plans over the 64 MiB RPC limit split, with no ceiling.
    #[test]
    fn plans_over_64_mib_split_with_no_ceiling() {
        let name = "y".repeat(100_000);
        let plans: Vec<PhysicalPlan> = (0..700).map(|_| truncate(&name)).collect();
        let (manifest, parts) =
            plan_parts(&plans, DatabaseId::DEFAULT, &[], LIMITS).expect("splits");
        let bytes: usize = parts.iter().map(|p| p.part.plans.len()).sum();
        assert!(bytes > 64 << 20, "the plans exceed 64 MiB");
        assert_parts_cover(&plans, &manifest, &parts);
    }

    /// Tasks a trigger body buffered do not make a client write.
    #[test]
    fn body_tasks_alone_are_not_a_client_write() {
        let plans = vec![truncate("a"), truncate("b")];
        let (manifest, parts) =
            plan_parts(&plans, DatabaseId::DEFAULT, &[0, 1], LIMITS).expect("packs");
        assert!(!manifest.client_write);
        assert_eq!(parts.len(), 1);
    }
}
