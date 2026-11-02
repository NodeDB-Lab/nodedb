// SPDX-License-Identifier: BUSL-1.1

//! Helpers of a resolved timeseries ingest: the unfit-record answer, the flush
//! decision, and landed-row event emission.

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::event_emit::RowWriteEvent;
use crate::data::executor::core_loop::fail_stop::FailStopCause;
use crate::data::executor::task::ExecutionTask;
use crate::engine::timeseries::resolved_ingest::ResolvedTsBatch;
use crate::event::WriteOp;
use crate::event::types::RowId;

use super::admission;
use super::ingest_resolved_fit::CollKey;

impl CoreLoop {
    /// The answer to a resolved record whose rows cannot land as carried.
    ///
    /// A refusable install stores nothing and answers `OllpRetryRequired`:
    /// its record is cancelled and the writer resolves again. Every other
    /// install is a record whose landing rows every node stores, so skipping
    /// one for a node-local reason diverges this node from its peers.
    /// The core fail-stops instead: it refuses every request until a restart
    /// replays the record.
    pub(super) fn unfit_resolved_ingest(
        &mut self,
        task: &ExecutionTask,
        refusable: bool,
        detail: &str,
    ) -> Response {
        if refusable {
            return self.response_error(task, ErrorCode::OllpRetryRequired);
        }
        self.fail_stop_core(FailStopCause::CommittedRowUnfit, detail);
        self.response_error(
            task,
            ErrorCode::RetryableRefusal {
                reason: detail.to_string(),
            },
        )
    }

    /// Whether the live memtable of `key` must flush before `batch` lands
    /// whole: it is at its soft or hard limit, the engine budget is under
    /// pressure, it cannot also hold the bytes the batch took at resolve, or
    /// its tag dictionaries have no room for the batch's tags.
    pub(super) fn ts_resolved_needs_flush(&self, key: &CollKey, batch: &ResolvedTsBatch) -> bool {
        let governor_pressure = self
            .governor
            .try_reserve(key.0, key.1, nodedb_mem::EngineId::Timeseries, 0)
            .is_err();
        let soft_limit = self.ts_tuning.memtable_budget_bytes;
        let hard_limit = self.ts_tuning.memtable_hard_limit_bytes;
        self.columnar_memtables.get(key).is_some_and(|mt| {
            let resident = mt.memory_bytes();
            let with_batch = resident
                .saturating_add(usize::try_from(batch.resolved_bytes).unwrap_or(usize::MAX));
            resident >= soft_limit
                || resident >= hard_limit
                || (resident > 0 && with_batch >= hard_limit)
                || governor_pressure
                || !admission::rows_have_tag_headroom(
                    mt,
                    &batch.columns,
                    &batch.rows,
                    self.ts_tuning.max_tag_cardinality,
                )
        })
    }

    /// Emit one Insert event per landed row's image, in row order.
    pub(super) fn emit_resolved_ts_events(
        &mut self,
        task: &crate::data::executor::task::ExecutionTask,
        collection: &str,
        images: &[&[u8]],
    ) {
        for image in images {
            self.emit_event_with_row_id_as(
                task,
                task.request.event_source,
                RowWriteEvent {
                    collection,
                    op: WriteOp::Insert,
                    row_id: RowId::Batch,
                    new_value: Some(*image),
                    old_value: None,
                    image_fault: None,
                },
            );
        }
    }
}
