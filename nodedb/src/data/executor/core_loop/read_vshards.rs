// SPDX-License-Identifier: BUSL-1.1

//! The vShards a read scanned, reported on its response.

use nodedb_physical::physical_plan::KvOp;

use crate::bridge::envelope::PhysicalPlan;
use crate::types::{ReadShards, VShardId, core_for_vshard};

use super::super::task::ExecutionTask;
use super::CoreLoop;

impl CoreLoop {
    /// The vShards `task` scanned on this core.
    ///
    /// - A write reports none: it scans nothing a read-set entry names.
    /// - A graph read scans this core's CSR partition, which holds every
    ///   vShard the core hosts. It reports each of them, written or not, so
    ///   a later first edge on any of them makes the read stale.
    /// - An array read reports the vShard its task covers: the tile vShard
    ///   its leg reads.
    /// - Any other read reports its collection's home vShard.
    pub(in crate::data::executor) fn read_vshards(&self, task: &ExecutionTask) -> ReadShards {
        let mut shards = ReadShards::new();
        if task.wal_lsn().is_some() {
            return shards;
        }
        let plan = task.plan();
        match plan {
            PhysicalPlan::Graph(_) => {
                let num_cores = self.redo_apply.num_cores;
                for vshard in (0..VShardId::COUNT).map(VShardId::new) {
                    if core_for_vshard(vshard, num_cores) == self.core_id {
                        shards.note(vshard);
                    }
                }
                return shards;
            }
            PhysicalPlan::Array(_) | PhysicalPlan::ClusterArray(_) => {
                shards.note(task.request.vshard_id);
                return shards;
            }
            PhysicalPlan::Kv(KvOp::ResolvedWrite { mutations, .. }) => {
                for mutation in mutations {
                    self.note_home(task, mutation.collection().as_str(), &mut shards);
                }
                return shards;
            }
            PhysicalPlan::Document(_)
            | PhysicalPlan::Vector(_)
            | PhysicalPlan::Crdt(_)
            | PhysicalPlan::Timeseries(_)
            | PhysicalPlan::Kv(_)
            | PhysicalPlan::Text(_)
            | PhysicalPlan::Spatial(_)
            | PhysicalPlan::Columnar(_)
            | PhysicalPlan::Meta(_)
            | PhysicalPlan::Query(_)
            | PhysicalPlan::ClusterEvent(_) => {}
        }
        if let Some(collection) = plan.collection() {
            self.note_home(task, collection, &mut shards);
        }
        shards
    }

    /// Note the home vShard of `collection`. A name that does not de-qualify
    /// names no vShard.
    fn note_home(&self, task: &ExecutionTask, collection: &str, shards: &mut ReadShards) {
        if let Ok(key) =
            nodedb_types::CollectionKey::from_qualified_str(task.request.database_id, collection)
        {
            shards.note(key.vshard());
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::GraphOp;

    use crate::bridge::envelope::PhysicalPlan;
    use crate::data::executor::core_loop::tests::{make_core, make_request};
    use crate::data::executor::task::ExecutionTask;
    use crate::types::{Lsn, VShardId};

    fn graph_stats() -> PhysicalPlan {
        PhysicalPlan::Graph(GraphOp::Stats {
            collection: None,
            as_of: None,
        })
    }

    /// A graph read on a core no write ever reached reports every vShard the
    /// core hosts: the CSR partition it scanned holds all of them.
    #[test]
    fn a_graph_read_reports_every_hosted_vshard_with_no_write_recorded() {
        let (mut core, _, _, _dir) = make_core();
        core.set_num_cores(4);
        core.core_id = 1;
        let shards = core.read_vshards(&ExecutionTask::new(make_request(graph_stats())));
        let reported: Vec<u32> = shards.iter().map(VShardId::as_u32).collect();
        let hosted: Vec<u32> = (0..VShardId::COUNT).filter(|v| v % 4 == 1).collect();
        assert_eq!(reported, hosted);
    }

    /// A write scans nothing a read-set entry names.
    #[test]
    fn a_write_reports_no_vshard() {
        let (core, _, _, _dir) = make_core();
        let task = ExecutionTask::with_wal_lsn(make_request(graph_stats()), Some(Lsn::new(9)));
        assert!(core.read_vshards(&task).is_empty());
    }
}
