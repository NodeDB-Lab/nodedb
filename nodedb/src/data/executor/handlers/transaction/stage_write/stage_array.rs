// SPDX-License-Identifier: BUSL-1.1

//! Statement-time staging for the two stageable array writes: `ArrayOp::Put`
//! and `ArrayOp::Delete`. These stage into a dedicated [`ArrayTxnOverlay`]
//! rather than the shared surrogate-keyed `TxnOverlay`, purely as in-memory
//! read-your-own-writes plumbing for Slice / Project / Aggregate /
//! Elementwise. Commit durable replay still runs the buffered `ArrayOp` plan
//! through the real `handle_array_put` / `handle_array_delete` handlers.

use nodedb_physical::physical_plan::ArrayOp;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::overlay::{
    ArrayTxnOverlay, MAX_TXN_OVERLAY_BYTES, StagedCellPut,
};
use crate::data::executor::task::ExecutionTask;
use crate::engine::array::wal::{ArrayDeleteCell, ArrayPutCell};
use crate::types::TxnId;

impl CoreLoop {
    /// Route a stageable `ArrayOp` to its staging handler. `op` must be
    /// `Put` or `Delete`: the two ops `is_stageable_write` accepts. Every other
    /// `ArrayOp` is unreachable here.
    pub(in crate::data::executor) fn execute_stage_array(
        &mut self,
        task: &ExecutionTask,
        txn_id: TxnId,
        op: &ArrayOp,
    ) -> Response {
        match op {
            ArrayOp::Put {
                array_id,
                cells_msgpack,
                ..
            } => {
                let cells: Vec<ArrayPutCell> = match zerompk::from_msgpack(cells_msgpack) {
                    Ok(c) => c,
                    Err(e) => {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: format!("array put decode: {e}"),
                            },
                        );
                    }
                };
                if let Err(e) = self.stage_array_capped(cells_msgpack.len()) {
                    return self.response_error(task, e);
                }
                let overlay: &mut ArrayTxnOverlay = self.array_txn_overlay_mut(txn_id);
                let count = cells.len();
                for cell in cells {
                    overlay.stage_cell_put(
                        array_id.clone(),
                        cell.coord,
                        StagedCellPut {
                            attrs: cell.attrs,
                            surrogate: cell.surrogate,
                            system_from_ms: cell.system_from_ms,
                            valid_from_ms: cell.valid_from_ms,
                            valid_until_ms: cell.valid_until_ms,
                        },
                    );
                }
                self.stage_count_response(task, count)
            }

            ArrayOp::Delete {
                array_id,
                coords_msgpack,
                ..
            } => {
                let cells: Vec<ArrayDeleteCell> = match zerompk::from_msgpack(coords_msgpack) {
                    Ok(c) => c,
                    Err(e) => {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: format!("array delete decode: {e}"),
                            },
                        );
                    }
                };
                // The base lookup below needs the store open on this core;
                // a read arriving before any autocommit write would find it
                // closed otherwise.
                if let Err(resp) = self.ensure_array_open(task, array_id) {
                    return resp;
                }
                let mut existed: usize = 0;
                for cell in cells {
                    // BASE ∪ OVERLAY: this transaction's own overlay wins when
                    // it has already touched the cell; otherwise fall back to
                    // the durable store, exactly like every other stage
                    // handler's real affected-count computation.
                    let staged = self
                        .array_txn_overlay_mut(txn_id)
                        .staged_cell_presence(array_id, &cell.coord);
                    let present = match staged {
                        Some(present) => present,
                        None => match self.array_engine.contains_cell(array_id, &cell.coord) {
                            Ok(present) => present,
                            Err(e) => {
                                return self.response_error(
                                    task,
                                    ErrorCode::Internal {
                                        detail: format!("array delete lookup: {e}"),
                                    },
                                );
                            }
                        },
                    };
                    if present {
                        existed += 1;
                    }
                    self.array_txn_overlay_mut(txn_id)
                        .stage_cell_delete(array_id.clone(), cell.coord);
                }
                self.stage_count_response(task, existed)
            }

            ArrayOp::OpenArray { .. }
            | ArrayOp::Slice { .. }
            | ArrayOp::Project { .. }
            | ArrayOp::Aggregate { .. }
            | ArrayOp::Elementwise { .. }
            | ArrayOp::Flush { .. }
            | ArrayOp::Compact { .. }
            | ArrayOp::SurrogateBitmapScan { .. }
            | ArrayOp::DropArray { .. }
            | ArrayOp::RestoreArrayDrop { .. }
            | ArrayOp::PurgeArrayDrop { .. } => self.stage_not_point_write(task),
        }
    }

    /// Enforce the per-transaction ARRAY overlay memory cap, reusing the same
    /// budget the surrogate-keyed overlay uses (summed across every in-flight
    /// transaction's ARRAY overlay on this core).
    fn stage_array_capped(&self, incoming_bytes: usize) -> crate::Result<()> {
        let current: usize = self
            .array_txn_overlays
            .values()
            .map(ArrayTxnOverlay::memory_size_estimate)
            .sum();
        if current.saturating_add(incoming_bytes) > MAX_TXN_OVERLAY_BYTES {
            return Err(crate::Error::TxnOverlayMemoryExceeded {
                limit: MAX_TXN_OVERLAY_BYTES,
            });
        }
        Ok(())
    }
}
