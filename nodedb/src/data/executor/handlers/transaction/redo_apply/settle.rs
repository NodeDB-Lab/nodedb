// SPDX-License-Identifier: BUSL-1.1

//! The work a committed-redo install defers until every sub-record landed.
//!
//! A write the undo cannot reverse waits here: a memtable flush drains rows
//! the undo would restore in memory, a vector seal moves inserted nodes out of
//! the growing segment, and a truncate that removes files cannot be renamed
//! back. Once the install succeeded, this runs them in one step. The record's
//! events wait until this step succeeded.
//!
//! A failure here comes after every sub-record landed. The undo cannot
//! reverse a partial flush, and a flush failure is an I/O failure no validate
//! pass can predict. So the apply fail-stops the core, and the funnel keeps
//! the record for restart replay.

use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

use super::state::{CollectionKey, RedoApplyPass, RedoApplyScope};

impl CoreLoop {
    /// Record that the install wrote rows into the columnar collection `key`.
    pub(in crate::data::executor) fn note_redo_columnar_written(&mut self, key: CollectionKey) {
        if let Some(scope) = self.redo_apply.scope.as_mut()
            && scope.pass == RedoApplyPass::Install
            && !scope.columnar_written.contains(&key)
        {
            scope.columnar_written.push(key);
        }
    }

    /// Record that the open redo-apply scope wrote cells to `array_id`, so
    /// the settle runs its threshold flush.
    pub(in crate::data::executor) fn note_redo_array_written(
        &mut self,
        array_id: &nodedb_array::types::ArrayId,
    ) {
        if let Some(scope) = self.redo_apply.scope.as_mut()
            && !scope.arrays_written.contains(array_id)
        {
            scope.arrays_written.push(array_id.clone());
        }
    }

    /// Record that the install ingested rows into the timeseries collection
    /// `key`.
    pub(in crate::data::executor) fn note_redo_timeseries_written(&mut self, key: CollectionKey) {
        if let Some(scope) = self.redo_apply.scope.as_mut()
            && scope.pass == RedoApplyPass::Install
            && !scope.timeseries_written.contains(&key)
        {
            scope.timeseries_written.push(key);
        }
    }

    /// Run everything the successful install deferred.
    pub(super) fn settle_redo_install(
        &mut self,
        task: &ExecutionTask,
        scope: &mut RedoApplyScope,
    ) -> Result<(), ErrorCode> {
        self.finalize_vector_truncates(&mut scope.undo);
        self.settle_filled_vector_collections();
        for key in std::mem::take(&mut scope.columnar_written) {
            // The install's versions join the collection's history charge.
            self.recharge_columnar_history(&key);
            let collection = key.2.clone();
            self.flush_columnar_memtable_if_needed(task, &key, &collection)
                .map_err(|response| {
                    response.error_code.map_or_else(
                        || ErrorCode::Internal {
                            detail: format!("columnar flush of '{collection}' failed"),
                        },
                        |error| *error,
                    )
                })?;
        }
        for key in std::mem::take(&mut scope.timeseries_written) {
            self.settle_redo_timeseries(key)?;
        }
        for array_id in &scope.arrays_written {
            self.flush_array_if_full(array_id)
                .map_err(|e| ErrorCode::Internal {
                    detail: format!("array '{}' threshold flush failed: {e}", array_id.name),
                })?;
        }
        // The record's document versions and index entries are stored. The
        // tenant's charge takes them, so the next write gate sees the
        // history this record added, on a Calvin slice as on any commit.
        if let Err(e) =
            self.recharge_document_history(task.request.database_id, task.request.tenant_id)
        {
            // The record installed. The charge keeps its prior value until
            // the next write recharges it.
            crate::diag::history_charge_stale(&e, "redo install");
            tracing::warn!(
                core = self.core_id,
                error = %e,
                "document history recharge after a redo install failed; the prior charge stays"
            );
        }
        Ok(())
    }

    /// Settle every vector collection the install filled while it held its
    /// seals and IVF-PQ trainings back.
    fn settle_filled_vector_collections(&mut self) {
        let full: Vec<_> = self
            .vector_collections
            .iter()
            .filter(|(_, coll)| coll.needs_seal() || coll.needs_ivf_training())
            .map(|(key, _)| key.clone())
            .collect();
        for key in full {
            self.settle_vector_collection(&key);
        }
    }

    /// Settle one timeseries collection the install ingested into: charge
    /// the memory budget for its memtable, and flush it when it is over its
    /// soft limit. The install noted the record before this runs, so the
    /// flush's stamp names the whole record (`group_flush`).
    fn settle_redo_timeseries(&mut self, key: CollectionKey) -> Result<(), ErrorCode> {
        let (database_id, tid, collection) = key;
        self.recharge_ts_memtable_budget(tid, database_id, &collection);
        self.checkpoint_coordinator.mark_dirty("timeseries", 1);
        // no-determinism: Instant::now feeds only the idle-flush timer.
        self.last_ts_ingest = Some(std::time::Instant::now());
        let memtable_key = (database_id, tid, collection.clone());
        let over_budget = self
            .columnar_memtables
            .get(&memtable_key)
            .is_some_and(|mt| mt.memory_bytes() >= self.ts_tuning.memtable_budget_bytes);
        if !over_budget {
            return Ok(());
        }
        let now_ms = self.ingest_now_ms();
        self.flush_ts_collection(tid, database_id, &collection, now_ms)
            .map_err(|e| ErrorCode::Internal {
                detail: format!("flushing '{collection}' after a committed-redo install: {e}"),
            })
    }
}
