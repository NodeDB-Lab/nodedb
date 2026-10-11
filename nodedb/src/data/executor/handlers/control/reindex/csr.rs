// SPDX-License-Identifier: BUSL-1.1

//! CSR rebuild of the tenant partition that holds a collection's edges.
//!
//! A tenant's edges from every collection share one CSR partition, so the
//! rebuild covers the whole partition. It runs only when the collection
//! has edges in it.
//!
//! Start, on the owning core: snapshot the partition and open its write
//! journal in one step, then compact the snapshot on a plain OS thread.
//! The core keeps serving from the live partition; every mutation of it
//! records itself in the journal.
//!
//! Cutover, on the owning core: restore the compacted copy, replay the
//! journal onto it and swap it into the partition map in one call. A
//! traversal sees the old partition or the new one, never a mix. The
//! cutover emits `atomic_cutover` on the `nodedb::reindex` target.

use std::sync::mpsc;

use nodedb_graph::csr::rebuild::{CsrRebuildSeed, CsrRebuilt};
use nodedb_mem::{EngineId, ScopedMemory};
use tracing::info;

use super::hold::{CSR_BUILD_HOLD, hold_build};
use super::pending::{PendingBuild, PendingReindex, RebuildTarget};
use crate::data::executor::core_loop::CoreLoop;

/// Bound on the bytes of mutations one CSR rebuild journal records. Past
/// it the rebuild is discarded at cutover and the live partition stays.
pub const CSR_REBUILD_JOURNAL_MAX_BYTES: usize = 64 << 20;

impl CoreLoop {
    /// Start a CSR rebuild for `target` on its own thread.
    pub(super) fn start_csr_rebuild(&mut self, target: &RebuildTarget) -> crate::Result<()> {
        let Some(seed) = self.begin_csr_rebuild(target)? else {
            return Ok(());
        };
        let token = seed.token();
        let memory = self.graph_memory(target);
        let fail_scope = self.fail_scope;
        let (tx, rx) = mpsc::sync_channel::<Result<CsrRebuilt, nodedb_graph::GraphError>>(1);
        let spawned = std::thread::Builder::new()
            .name(format!("reindex-csr-{}", self.core_id))
            .spawn(move || {
                hold_build(fail_scope, CSR_BUILD_HOLD);
                // The receiver is gone only when the core shut down.
                let _ = tx.send(seed.build(memory));
            });
        if let Err(e) = spawned {
            self.abort_csr_rebuild(target, token);
            return Err(crate::Error::Io(e));
        }
        self.maintenance.pending_reindex.push(PendingReindex {
            target: target.clone(),
            build: PendingBuild::Csr { token, rx },
        });
        Ok(())
    }

    /// Snapshot the partition and open its journal, or `None` when the
    /// collection has no edges in it, or when a rebuild of the partition
    /// already runs: that rebuild covers this collection too.
    fn begin_csr_rebuild(
        &mut self,
        target: &RebuildTarget,
    ) -> crate::Result<Option<CsrRebuildSeed>> {
        let core_id = self.core_id;
        let Some(partition) = self.csr.partition_mut(target.database_id, target.tenant_id) else {
            return Ok(None);
        };
        if partition.collection_id(&target.collection).is_none() {
            return Ok(None);
        }
        if partition.rebuild_in_progress() {
            info!(
                target: "nodedb::reindex",
                core = core_id,
                index = "csr",
                collection = %target.collection,
                "CSR partition rebuild already running; it covers this collection"
            );
            return Ok(None);
        }
        let seed = partition.begin_rebuild(CSR_REBUILD_JOURNAL_MAX_BYTES)?;
        info!(
            target: "nodedb::reindex",
            core = core_id,
            index = "csr",
            collection = %target.collection,
            "rebuild_started"
        );
        Ok(Some(seed))
    }

    /// Replay the journal onto `rebuilt` and swap it in on this core.
    pub(super) fn install_csr(
        &mut self,
        target: &RebuildTarget,
        rebuilt: CsrRebuilt,
    ) -> crate::Result<()> {
        let memory = self.graph_memory(target);
        let Some(live) = self.csr.partition_mut(target.database_id, target.tenant_id) else {
            return Err(nodedb_graph::GraphError::RebuildSuperseded.into());
        };
        let copy = live.finish_rebuild(rebuilt, memory)?;
        let nodes = copy.node_count();
        let edges = copy.edge_count();
        self.csr
            .install_partition(target.database_id, target.tenant_id, copy);
        info!(
            target: "nodedb::reindex",
            core = self.core_id,
            index = "csr",
            collection = %target.collection,
            nodes,
            edges,
            "atomic_cutover"
        );
        Ok(())
    }

    /// Close the partition's journal of rebuild `token`.
    pub(super) fn abort_csr_rebuild(&mut self, target: &RebuildTarget, token: u64) {
        if let Some(partition) = self.csr.partition_mut(target.database_id, target.tenant_id) {
            partition.abort_rebuild(token);
        }
    }

    fn graph_memory(&self, target: &RebuildTarget) -> ScopedMemory {
        ScopedMemory::new(
            self.governor.clone(),
            target.database_id,
            target.tenant_id,
            EngineId::Graph,
        )
    }
}
