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
//! are not touched. A transaction resolve reads the same scratch rows for the
//! write events its redo record carries (`events`).

use nodedb_types::columnar::schema::{TS_SERIES, TS_SYSTEM};
use nodedb_types::timeseries::SeriesCatalog;

use super::raw_scan::emit_memtable_rows_at;
use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::timeseries::columnar_memtable::{
    ColumnType, ColumnarMemtable, ColumnarMemtableConfig, ColumnarSchema,
};
use crate::engine::timeseries::ilp;
use crate::engine::timeseries::ilp_ingest;
use crate::types::TenantId;

impl CoreLoop {
    /// The rows `lines` store in `collection`, each rendered by the raw-scan
    /// row emitter. `now_ms` is the ingest instant: the default timestamp of
    /// an untimed line.
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
        let (outcome, scratch) =
            self.scratch_ilp_ingest(task, tid, collection, lines, now_ms, None);
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

    /// Ingest `lines` into a scratch memtable that carries `collection`'s
    /// current schema, or `base` when given, evolved for the lines. Returns
    /// the outcome, with the row index of every accepted row, and the scratch
    /// memtable those indices name.
    pub(in crate::data::executor) fn scratch_ilp_ingest(
        &self,
        task: &ExecutionTask,
        tid: TenantId,
        collection: &str,
        lines: &[ilp::IlpLine<'_>],
        now_ms: i64,
        base: Option<&ColumnarSchema>,
    ) -> (ilp_ingest::IngestBatchOutcome, ColumnarMemtable) {
        let key = (task.request.database_id, tid, collection.to_string());
        let bitemporal =
            self.is_bitemporal(task.request.database_id.as_u64(), tid.as_u64(), collection);
        let live = self
            .columnar_memtables
            .get(&key)
            .map(|live| (live.schema().clone(), live.config()));
        let base = base.cloned().map(|schema| {
            let config = live.as_ref().map_or_else(
                || ColumnarMemtableConfig::from_tuning(&self.ts_tuning),
                |(_, config)| config.clone(),
            );
            (schema, config)
        });
        let mut scratch = match base.or(live) {
            Some((schema, config)) => {
                let mut scratch = ColumnarMemtable::new(schema, config);
                ilp_ingest::evolve_schema(&mut scratch, lines);
                scratch.add_column(TS_SYSTEM.to_string(), ColumnType::Int64);
                scratch.add_column(TS_SERIES.to_string(), ColumnType::Int64);
                scratch
            }
            None => {
                let mut schema = self.initial_ts_schema(task, tid, collection, lines);
                ilp_ingest::ensure_system_column(&mut schema);
                if bitemporal {
                    ilp_ingest::ensure_bitemporal_columns(&mut schema);
                }
                ColumnarMemtable::new(schema, ColumnarMemtableConfig::from_tuning(&self.ts_tuning))
            }
        };
        let mut catalog = SeriesCatalog::new();
        let outcome = ilp_ingest::ingest_batch_with_lvc(ilp_ingest::IngestBatchArgs {
            memtable: &mut scratch,
            database_id: task.request.database_id,
            lines,
            catalog: &mut catalog,
            default_timestamp_ms: now_ms,
            lvc: None,
            // The scratch rows stamp the last ordinal of the statement
            // instant. The apply restamps every row with the batch's
            // ordinal (`ts_batch_ordinal`).
            stamp: Some(ilp_ingest::RowStamp {
                system_ordinal: nodedb_types::ms_to_ordinal_upper(now_ms),
                bitemporal,
            }),
            collect_row_indices: true,
        });
        (outcome, scratch)
    }
}
