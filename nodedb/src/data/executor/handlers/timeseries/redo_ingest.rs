// SPDX-License-Identifier: BUSL-1.1

//! The admission test every ILP ingest runs before its rows land, and the
//! pre-image a committed-redo install records.
//!
//! An install never flushes per sub-record: the replay arm flushed the
//! memtable before the record's first sub-record when it could not take the
//! whole record (`group_flush`). The pre-image is recorded after that flush,
//! so the undo restores a memtable that holds nothing the flush moved to disk.
//! A batched install also flushes between its batches (`install_flush`). The
//! undo of each such flush takes its partitions back before the pre-image
//! applies, so the restored rows are on no partition.

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::undo::UndoEntry;
use crate::engine::timeseries::ilp;
use crate::types::{DatabaseId, TenantId};

use super::admission;

impl CoreLoop {
    /// Whether the memtable of `key` must flush before `lines` land in it: it
    /// is at its soft or hard limit, the engine budget is under pressure, or
    /// its tag dictionaries have no room for the lines' tags.
    pub(super) fn ts_ingest_needs_flush(
        &self,
        key: &(DatabaseId, TenantId, String),
        lines: &[ilp::IlpLine<'_>],
    ) -> bool {
        let max_tag_cardinality = self.ts_tuning.max_tag_cardinality;
        self.ts_memtable_pressed(key)
            || self
                .columnar_memtables
                .get(key)
                .is_some_and(|mt| !admission::has_tag_headroom(mt, lines, max_tag_cardinality))
    }

    /// Whether the governor reports the timeseries engine budget at
    /// `Critical` pressure or above: the level at which every engine runs
    /// its own flush (`check_engine_pressure`).
    pub(super) fn ts_engine_pressed(&self) -> bool {
        self.governor
            .engine_pressure(nodedb_mem::EngineId::Timeseries)
            >= nodedb_mem::PressureLevel::Critical
    }

    /// Whether the memtable of `key` must flush before any row lands in it:
    /// it is at its soft or hard limit, or the timeseries engine budget is
    /// under pressure (`ts_engine_pressed`). An absent memtable is not.
    pub(super) fn ts_memtable_pressed(&self, key: &(DatabaseId, TenantId, String)) -> bool {
        let governor_pressure = self.ts_engine_pressed();
        let soft_limit = self.ts_tuning.memtable_budget_bytes;
        let hard_limit = self.ts_tuning.memtable_hard_limit_bytes;
        self.columnar_memtables.get(key).is_some_and(|mt| {
            let resident = mt.memory_bytes();
            resident >= soft_limit || resident >= hard_limit || governor_pressure
        })
    }

    /// Record the pre-image the undo of one installed sub-record into
    /// `collection` restores.
    pub(super) fn record_redo_ts_pre_image(
        &mut self,
        database_id: DatabaseId,
        tid: TenantId,
        collection: &str,
    ) {
        let key = (database_id, tid, collection.to_string());
        let undo = self.capture_timeseries_ingest_undo(&key);
        self.record_redo_undo([UndoEntry::TimeseriesIngest(undo)]);
    }
}
