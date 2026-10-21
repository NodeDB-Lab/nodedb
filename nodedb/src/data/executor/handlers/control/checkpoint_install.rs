// SPDX-License-Identifier: BUSL-1.1

//! Make a snapshot install durable before the core acknowledges it.
//!
//! A snapshot install writes no WAL records. Engine state it leaves only in
//! memory has no copy a restart can rebuild it from, so every memory-only
//! engine the install writes is checkpointed here: KV, CRDT, columnar,
//! timeseries memtables, vectors, spatial R-trees, and the CSR's node labels.
//! The redb-backed stores (documents, full-text postings, graph edges, node
//! label versions) commit durably at write time and need no step.
//!
//! Unlike the coordinated checkpoint, a failed flush is an error here, not a
//! clamp: an install whose state is not durable must not be acknowledged,
//! because the caller then moves the Raft durable floor past it.

use crate::data::executor::core_loop::CoreLoop;

impl CoreLoop {
    /// Checkpoint every memory-only engine a snapshot install writes, and
    /// advance each engine's durable floor to the flushed point.
    pub(in crate::data::executor) fn persist_snapshot_install(&mut self) -> crate::Result<()> {
        self.floors.kv_durable_lsn = self.checkpoint_kv_engines()?;

        let crdt = self.checkpoint_crdt_engines()?;
        self.checkpoint_coordinator
            .record_flush("crdt", crdt.files_written);
        self.floors.crdt_durable_lsn = crdt.durable_lsn;

        self.floors.columnar_durable_lsn = self.checkpoint_columnar_engines()?;
        self.floors.ts_durable_lsn = self.checkpoint_timeseries_memtables()?;

        let vector = self.checkpoint_vector_indexes()?;
        self.checkpoint_coordinator
            .record_flush("vector", vector.files_written);
        self.floors.vector_durable_lsn = vector.durable_lsn;

        let spatial = self.checkpoint_spatial_indexes()?;
        self.floors.spatial_durable_lsn = spatial.durable_lsn;

        self.floors.graph_label_durable_lsn = self.checkpoint_graph_labels()?;
        Ok(())
    }
}
