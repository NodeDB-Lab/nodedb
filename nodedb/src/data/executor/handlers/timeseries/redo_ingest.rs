// SPDX-License-Identifier: BUSL-1.1

//! The admission test every ILP ingest runs before its rows land, and the
//! pre-image a committed-redo install records.
//!
//! An install never flushes per sub-record: the replay arm flushed the
//! memtable before the record's first sub-record when it could not take the
//! whole record (`group_flush`). The pre-image is recorded after that flush,
//! so the undo restores a memtable that holds nothing the flush moved to disk.

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
        let governor_pressure = self
            .governor
            .try_reserve(key.0, key.1, nodedb_mem::EngineId::Timeseries, 0)
            .is_err();
        let soft_limit = self.ts_tuning.memtable_budget_bytes;
        let hard_limit = self.ts_tuning.memtable_hard_limit_bytes;
        let max_tag_cardinality = self.ts_tuning.max_tag_cardinality;
        self.columnar_memtables.get(key).is_some_and(|mt| {
            let resident = mt.memory_bytes();
            resident >= soft_limit
                || resident >= hard_limit
                || governor_pressure
                || !admission::has_tag_headroom(mt, lines, max_tag_cardinality)
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
