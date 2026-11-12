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
use crate::data::executor::handlers::transaction::overlay::{ArrayTxnOverlay, StagedCellPut};
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
                // One unbound cell refuses the whole put before any cell is
                // staged, as the autocommit handler does.
                for cell in &cells {
                    if let Some(refusal) =
                        crate::data::executor::handlers::unbound_surrogate::refuse_unbound(
                            "array",
                            &array_id.name,
                            cell.surrogate,
                        )
                    {
                        return self.response_error(task, refusal);
                    }
                }
                let overlay: &mut ArrayTxnOverlay = self.array_txn_overlay_mut(txn_id);
                let count = cells.len();
                for cell in cells {
                    let staged = overlay.stage_cell_put(
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
                    if let Err(e) = staged {
                        return self.response_error(task, e);
                    }
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
                    let staged = match self
                        .array_txn_overlay_mut(txn_id)
                        .staged_cell_presence(array_id, &cell.coord)
                    {
                        Ok(staged) => staged,
                        Err(e) => return self.response_error(task, e),
                    };
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
                    if let Err(e) = self
                        .array_txn_overlay_mut(txn_id)
                        .stage_cell_delete(array_id.clone(), cell.coord)
                    {
                        return self.response_error(task, e);
                    }
                }
                self.stage_count_response(task, existed)
            }

            ArrayOp::OpenArray { .. }
            | ArrayOp::Slice { .. }
            | ArrayOp::Aggregate { .. }
            | ArrayOp::Elementwise { .. }
            | ArrayOp::Flush { .. }
            | ArrayOp::Compact { .. }
            | ArrayOp::SurrogateBitmapScan { .. }
            | ArrayOp::DropArray { .. }
            | ArrayOp::RekeyArray { .. }
            | ArrayOp::PurgeArrayDrop { .. } => self.stage_not_point_write(task),
        }
    }
}
