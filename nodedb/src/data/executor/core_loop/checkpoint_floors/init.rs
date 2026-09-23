// SPDX-License-Identifier: BUSL-1.1

//! What a freshly-opened core's floors start at, and why each starts there.

use crate::data::executor::replay_floors::ReplayFloors;
use crate::types::Lsn;

use super::state::CheckpointFloors;

impl CheckpointFloors {
    /// The floors a freshly-opened core starts with, before any checkpoint has
    /// been restored or written by this process.
    pub(in crate::data::executor) fn new() -> Self {
        Self {
            // Nothing is checkpointed yet on a fresh core, so KV is durable
            // through nothing: until the first successful flush, a failed
            // checkpoint clamps truncation to zero rather than trusting the
            // watermark.
            kv_durable_lsn: Lsn::ZERO,
            // Same rule for the other engines whose only non-WAL copy is a
            // checkpoint file: until this process has either restored one or
            // written one, they are durable through nothing, and a failed flush
            // must clamp truncation to zero rather than trust the watermark.
            sparse_vector_durable_lsn: Lsn::ZERO,
            sync_hwm_durable_lsn: Lsn::ZERO,
            columnar_durable_lsn: Lsn::ZERO,
            graph_label_durable_lsn: Lsn::ZERO,
            // The array engine restores no durable LSN at boot — its segments
            // are self-describing but say nothing about what this CORE is
            // durable through — so it stays at zero until this process's own
            // flush succeeds. Clamping to zero in the meantime costs WAL growth,
            // never data.
            array_durable_lsn: Lsn::ZERO,
            // Same for timeseries: `load_ts_registries` restores which RECORDS
            // its partitions already contain, but not what this core is durable
            // through, so this stays at zero until this process's own flush
            // succeeds.
            ts_durable_lsn: Lsn::ZERO,
            // Vector, CRDT and spatial checkpoint files carry no core-level LSN
            // to restore — a vector file holds its collection's replay gate, a
            // Loro snapshot holds CRDT versions, and an R-tree file holds
            // neither — so all three stay at zero until this process's own flush
            // succeeds, and clamp truncation to zero until it does.
            vector_durable_lsn: Lsn::ZERO,
            // No generation is published until one is loaded or written.
            vector_published_lsn: Lsn::ZERO,
            kv_published_lsn: Lsn::ZERO,
            columnar_published_lsn: Lsn::ZERO,
            crdt_durable_lsn: Lsn::ZERO,
            spatial_durable_lsn: Lsn::ZERO,
            replay_floors: ReplayFloors::default(),
        }
    }
}
