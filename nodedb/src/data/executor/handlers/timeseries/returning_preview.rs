// SPDX-License-Identifier: BUSL-1.1

//! The rows a staged timeseries ingest will store, rendered as a `SELECT`
//! renders them.
//!
//! A Calvin transaction decides a plan's `RETURNING` rows when the plan
//! stages, before the flush installs anything. The live ingest reads its
//! stored rows back out of the memtable through the raw-scan row emitter.
//! This preview ingests the same stamped lines into a scratch memtable that
//! carries the collection's current schema, and reads them back through the
//! same emitter. The live memtable, its dictionaries and its series catalog
//! are not touched.

use nodedb_types::timeseries::SeriesCatalog;

use super::raw_scan::emit_memtable_rows_at;
use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::timeseries::columnar_memtable::{ColumnarMemtable, ColumnarMemtableConfig};
use crate::engine::timeseries::ilp;
use crate::engine::timeseries::ilp_ingest;
use crate::types::TenantId;

impl CoreLoop {
    /// The rows `lines` store in `collection`, each rendered by the raw-scan
    /// row emitter. `now_ms` is the ingest instant: the default timestamp of
    /// an untimed line and the system time of a bitemporal row.
    ///
    /// A `RETURNING` ingest takes every row or none, so a line the memtable
    /// would reject refuses the whole plan before anything is staged.
    pub(in crate::data::executor) fn preview_ilp_ingest_rows(
        &self,
        task: &ExecutionTask,
        tid: TenantId,
        collection: &str,
        lines: &[ilp::IlpLine<'_>],
        now_ms: i64,
    ) -> Result<Vec<rmpv::Value>, ErrorCode> {
        let key = (task.request.database_id, tid, collection.to_string());
        let bitemporal =
            self.is_bitemporal(task.request.database_id.as_u64(), tid.as_u64(), collection);
        let mut scratch = match self.columnar_memtables.get(&key) {
            Some(live) => {
                let mut scratch = ColumnarMemtable::new(live.schema().clone(), live.config());
                ilp_ingest::evolve_schema(&mut scratch, lines);
                scratch
            }
            None => {
                let mut schema = self.initial_ts_schema(task, tid, collection, lines);
                if bitemporal {
                    ilp_ingest::ensure_bitemporal_columns(&mut schema);
                }
                ColumnarMemtable::new(schema, ColumnarMemtableConfig::from_tuning(&self.ts_tuning))
            }
        };
        let mut catalog = SeriesCatalog::new();
        let outcome = ilp_ingest::ingest_batch_with_lvc(ilp_ingest::IngestBatchArgs {
            memtable: &mut scratch,
            lines,
            catalog: &mut catalog,
            default_timestamp_ms: now_ms,
            lvc: None,
            bitemporal: bitemporal.then_some(ilp_ingest::BitempStamps { system_ms: now_ms }),
            collect_row_indices: true,
        });
        if outcome.rejected > 0 {
            let reason = outcome
                .first_rejection
                .unwrap_or_else(|| "no reason recorded".to_string());
            return Err(ErrorCode::RejectedPrevalidation {
                reason: format!(
                    "timeseries ingest with RETURNING would reject {} of {} rows, and a row set \
                     cannot report a rejected row; first rejection: {reason}",
                    outcome.rejected,
                    outcome.accepted + outcome.rejected
                ),
            });
        }
        emit_memtable_rows_at(&scratch, &outcome.accepted_row_indices).map_err(ErrorCode::from)
    }
}
