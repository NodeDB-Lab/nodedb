//! Top-level `ArrayOp` dispatch — routes every variant to its handler.

use std::sync::Arc;

use nodedb_array::schema::ArraySchema;
use nodedb_array::types::ArrayId;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::physical_plan::ArrayOp;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    pub(in crate::data::executor) fn dispatch_array(
        &mut self,
        task: &ExecutionTask,
        op: &ArrayOp,
    ) -> Response {
        match op {
            ArrayOp::OpenArray {
                array_id,
                schema_msgpack,
                schema_hash,
            } => self.handle_array_open(task, array_id, schema_msgpack, *schema_hash),
            ArrayOp::Put {
                array_id,
                cells_msgpack,
                wal_lsn,
            } => self.handle_array_put(task, array_id, cells_msgpack, *wal_lsn),
            ArrayOp::Delete {
                array_id,
                coords_msgpack,
                wal_lsn,
            } => self.handle_array_delete(task, array_id, coords_msgpack, *wal_lsn),
            ArrayOp::Flush { array_id, wal_lsn } => {
                self.handle_array_flush(task, array_id, *wal_lsn)
            }
            ArrayOp::Compact { array_id } => self.handle_array_compact(task, array_id),
            ArrayOp::DropArray { array_id } => self.handle_array_drop(task, array_id),
            ArrayOp::Slice {
                array_id,
                slice_msgpack,
                attr_projection,
                limit,
            } => self.dispatch_array_slice(task, array_id, slice_msgpack, attr_projection, *limit),
            ArrayOp::Project {
                array_id,
                attr_indices,
            } => self.dispatch_array_project(task, array_id, attr_indices),
            ArrayOp::Aggregate {
                array_id,
                attr_idx,
                reducer,
                group_by_dim,
            } => self.dispatch_array_aggregate(task, array_id, *attr_idx, *reducer, *group_by_dim),
            ArrayOp::Elementwise {
                left,
                right,
                op,
                attr_idx,
            } => self.dispatch_array_elementwise(task, left, right, *op, *attr_idx),
        }
    }

    /// Idempotently open the array on this core, looking the schema up
    /// from the shared `ArrayCatalogHandle`. Read handlers (Slice /
    /// Project / Aggregate / Elementwise) call this at entry so that a
    /// SQL read against a per-core engine that has not yet seen an
    /// explicit `OpenArray` dispatch (e.g. the very first read after a
    /// restart) auto-opens via the catalog instead of erroring.
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
            let entry = cat.lookup_by_name(&array_id.name).ok_or_else(|| {
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
