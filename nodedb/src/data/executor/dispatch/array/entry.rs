// SPDX-License-Identifier: BUSL-1.1

//! Top-level `ArrayOp` dispatch — routes every variant to its handler.

use std::sync::Arc;

use nodedb_array::schema::ArraySchema;
use nodedb_array::types::ArrayId;

use crate::bridge::envelope::{ErrorCode, Response};
use nodedb_mem;
use nodedb_physical::physical_plan::ArrayOp;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::dispatch::array::aggregate::AggParams;
use crate::data::executor::dispatch::array::read::SliceParams;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    pub(in crate::data::executor) fn dispatch_array(
        &mut self,
        task: &ExecutionTask,
        op: &ArrayOp,
    ) -> Response {
        // Pressure guard for write operations.
        let is_write = matches!(op, ArrayOp::Put { .. } | ArrayOp::Delete { .. });
        if is_write && let Some(r) = self.check_engine_pressure(task, nodedb_mem::EngineId::Array) {
            return r;
        }
        // A write, flush or compaction addresses an array this core may not
        // have opened since it booted: restart replay opens only the arrays
        // its WAL tail writes, and a read opens its own array. Opened from the
        // catalog here, like a read, or the engine refuses the array as
        // unknown.
        if let ArrayOp::Put { array_id, .. }
        | ArrayOp::Delete { array_id, .. }
        | ArrayOp::Flush { array_id, .. }
        | ArrayOp::Compact { array_id, .. } = op
            && let Err(resp) = self.ensure_array_open(task, array_id)
        {
            return resp;
        }
        match op {
            ArrayOp::OpenArray {
                array_id,
                schema_msgpack,
                schema_hash,
                prefix_bits,
                ..
            } => self.handle_array_open(task, array_id, schema_msgpack, *schema_hash, *prefix_bits),
            ArrayOp::Put {
                array_id,
                cells_msgpack,
                wal_lsn,
                provenance,
            } => {
                self.handle_array_put(task, array_id, cells_msgpack, *wal_lsn, provenance.as_ref())
            }
            ArrayOp::Delete {
                array_id,
                coords_msgpack,
                wal_lsn,
                provenance,
            } => self.handle_array_delete(
                task,
                array_id,
                coords_msgpack,
                *wal_lsn,
                provenance.as_ref(),
            ),
            ArrayOp::Flush { array_id, wal_lsn } => {
                self.handle_array_flush(task, array_id, *wal_lsn)
            }
            ArrayOp::Compact {
                array_id,
                audit_retain_ms,
            } => self.handle_array_compact(task, array_id, *audit_retain_ms),
            ArrayOp::DropArray { array_id } => self.handle_array_drop(task, array_id),
            ArrayOp::RestoreArrayDrop { array_id } => {
                self.handle_array_drop_restore(task, array_id)
            }
            ArrayOp::PurgeArrayDrop { array_id } => self.handle_array_drop_purge(task, array_id),
            ArrayOp::Slice {
                array_id,
                slice_msgpack,
                attr_projection,
                limit,
                cell_filter,
                hilbert_range,
                system_time,
                valid_at_ms,
            } => self.dispatch_array_slice(
                task,
                SliceParams {
                    array_id,
                    slice_msgpack,
                    attr_projection,
                    limit: *limit,
                    cell_filter: cell_filter.as_ref(),
                    hilbert_range: *hilbert_range,
                    system_time: *system_time,
                    valid_at_ms: *valid_at_ms,
                },
            ),
            ArrayOp::SurrogateBitmapScan {
                array_id,
                slice_msgpack,
            } => self.dispatch_array_surrogate_bitmap_scan(task, array_id, slice_msgpack),
            ArrayOp::Project {
                array_id,
                attr_indices,
            } => self.dispatch_array_project(task, array_id, attr_indices),
            ArrayOp::Aggregate {
                array_id,
                attr_idx,
                reducer,
                group_by_dim,
                cell_filter,
                return_partial,
                hilbert_range,
                system_as_of,
                valid_at_ms,
            } => self.dispatch_array_aggregate(
                task,
                AggParams {
                    array_id,
                    attr_idx: *attr_idx,
                    reducer: *reducer,
                    group_by_dim_idx: *group_by_dim,
                    cell_filter: cell_filter.as_ref(),
                    return_partial: *return_partial,
                    hilbert_range: *hilbert_range,
                    system_as_of: *system_as_of,
                    valid_at_ms: *valid_at_ms,
                },
            ),
            ArrayOp::Elementwise {
                left,
                right,
                op,
                attr_idx,
                cell_filter,
            } => self.dispatch_array_elementwise(
                task,
                left,
                right,
                *op,
                *attr_idx,
                cell_filter.as_ref(),
            ),
        }
    }

    /// [`Self::ensure_array_open`] when the catalog holds `array_id`. An array
    /// with no catalog entry was dropped, and there is nothing to open.
    pub(in crate::data::executor) fn open_array_if_cataloged(
        &mut self,
        task: &ExecutionTask,
        array_id: &ArrayId,
    ) -> Result<(), Response> {
        let cataloged = self
            .array_catalog
            .read()
            .map_err(|_| {
                self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: "array catalog lock poisoned".to_string(),
                    },
                )
            })?
            .lookup_by_id(array_id)
            .is_some();
        if cataloged {
            self.ensure_array_open(task, array_id)?;
        }
        Ok(())
    }

    /// Idempotently open the array on this core, looking the schema up
    /// from the shared `ArrayCatalogHandle`. Every read handler (Slice /
    /// Project / Aggregate / Elementwise), and `dispatch_array` for every
    /// write, flush and compaction, calls this at entry, so an op against a
    /// per-core engine that has not yet seen an explicit `OpenArray` dispatch
    /// (the first op after a restart) auto-opens via the catalog instead of
    /// erroring.
    pub(in crate::data::executor) fn ensure_array_open(
        &mut self,
        task: &ExecutionTask,
        array_id: &ArrayId,
    ) -> Result<(), Response> {
        let (schema_msgpack, schema_hash) = {
            let cat = self.array_catalog.read().map_err(|_| {
                self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: "array catalog lock poisoned".to_string(),
                    },
                )
            })?;
            let entry = cat.lookup_by_id(array_id).ok_or_else(|| {
                self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("array '{}' not found in catalog", array_id.name),
                    },
                )
            })?;
            (entry.schema_msgpack.clone(), entry.schema_hash)
        };
        let schema: ArraySchema = zerompk::from_msgpack(&schema_msgpack).map_err(|e| {
            self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("array schema decode: {e}"),
                },
            )
        })?;
        self.array_engine
            .open_array(array_id.clone(), Arc::new(schema), schema_hash)
            .map_err(|e| {
                self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("array engine open: {e}"),
                    },
                )
            })
    }
}
