// SPDX-License-Identifier: BUSL-1.1

//! Dispatch for ColumnarOp variants (scan, insert, update, delete).

use crate::bridge::envelope::Response;
use nodedb_physical::physical_plan::ColumnarOp;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::columnar_read::ColumnarScanParams;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    pub(super) fn dispatch_columnar(&mut self, task: &ExecutionTask, op: &ColumnarOp) -> Response {
        match op {
            ColumnarOp::Scan {
                collection,
                projection,
                limit,
                filters,
                rls_filters,
                sort_keys,
                system_time,
                valid_at_ms,
                prefilter,
                computed_columns,
            } => self.execute_columnar_scan(
                task,
                ColumnarScanParams {
                    collection: collection.as_str(),
                    projection,
                    limit: *limit,
                    filters,
                    rls_filters,
                    sort_keys,
                    system_time: *system_time,
                    valid_at_ms: *valid_at_ms,
                    prefilter: prefilter.as_ref(),
                    computed_columns,
                    txn_id: task.request.txn_id,
                },
            ),

            ColumnarOp::Insert {
                collection,
                payload,
                format,
                intent,
                on_conflict_updates,
                surrogates,
                schema_bytes,
                provenance,
                wal_lsn: _,
                rls_write_check,
                returning,
                rls_filters,
            } => {
                if let Some(r) = self.check_engine_pressure(task, nodedb_mem::EngineId::Columnar) {
                    return r;
                }
                self.execute_columnar_insert(
                    task,
                    crate::data::executor::handlers::columnar_write::ColumnarInsertParams {
                        collection: collection.as_str(),
                        payload,
                        format,
                        intent: *intent,
                        on_conflict_updates,
                        surrogates,
                        schema_bytes,
                        provenance: provenance.as_ref(),
                        rls_write_check,
                        returning: returning.as_ref(),
                        rls_filters,
                        spatial_undo: None,
                    },
                )
            }

            ColumnarOp::Update {
                collection,
                filters,
                updates,
                rls_write_check,
            } => {
                if let Some(r) = self.check_engine_pressure(task, nodedb_mem::EngineId::Columnar) {
                    return r;
                }
                self.execute_columnar_update(
                    task,
                    collection.as_str(),
                    filters,
                    updates,
                    rls_write_check,
                    None,
                )
            }

            ColumnarOp::Delete {
                collection,
                filters,
                rls_write_check,
            } => {
                if let Some(r) = self.check_engine_pressure(task, nodedb_mem::EngineId::Columnar) {
                    return r;
                }
                self.execute_columnar_delete(
                    task,
                    collection.as_str(),
                    filters,
                    rls_write_check,
                    None,
                )
            }

            ColumnarOp::MaterializeScan {
                collection,
                cursor,
                count,
                system_as_of_ms,
            } => self.execute_columnar_materialize_scan(
                task,
                collection.as_str(),
                cursor,
                *count,
                *system_as_of_ms,
            ),

            ColumnarOp::ResolvedUpdate {
                collection,
                rows,
                rls_write_check,
            } => {
                if let Some(r) = self.check_engine_pressure(task, nodedb_mem::EngineId::Columnar) {
                    return r;
                }
                self.execute_columnar_resolved_update(
                    task,
                    collection.as_str(),
                    rows,
                    rls_write_check,
                    None,
                )
            }

            ColumnarOp::ResolvedDelete {
                collection,
                pks,
                rls_write_check,
            } => {
                if let Some(r) = self.check_engine_pressure(task, nodedb_mem::EngineId::Columnar) {
                    return r;
                }
                self.execute_columnar_resolved_delete(
                    task,
                    collection.as_str(),
                    pks,
                    rls_write_check,
                    None,
                )
            }

            ColumnarOp::ResolveDml {
                collection,
                filters,
                updates,
                is_update,
                rls_write_check,
            } => self.execute_columnar_resolve_dml(
                task,
                collection.as_str(),
                filters,
                updates,
                *is_update,
                rls_write_check,
            ),

            ColumnarOp::Truncate {
                collection,
                restart_identity: _,
            } => self.execute_columnar_truncate(task, collection.as_str(), None),
        }
    }
}
