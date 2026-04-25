//! `ArrayOp::Elementwise` handler.
//!
//! Coord-aligned pairwise op between two open arrays sharing the same
//! schema hash. We union both sides into one sparse tile each (schema
//! comes from the left store); the inner `elementwise` routine then
//! handles outer-join semantics on coordinates exactly. Per-tile fast-
//! path pairing is future work.

use nodedb_array::query::elementwise::{BinaryOp, elementwise};
use nodedb_array::schema::ArraySchema;
use nodedb_array::segment::{MbrQueryPredicate, TilePayload};
use nodedb_array::tile::sparse_tile::{SparseTile, SparseTileBuilder};
use nodedb_array::types::ArrayId;
use nodedb_types::{SurrogateBitmap, Value};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::physical_plan::ArrayBinaryOp;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

use super::convert::sparse_tile_to_array_cells;
use super::encode::encode_value_rows;

impl CoreLoop {
    pub(in crate::data::executor) fn dispatch_array_elementwise(
        &mut self,
        task: &ExecutionTask,
        left: &ArrayId,
        right: &ArrayId,
        op: ArrayBinaryOp,
        _attr_idx: u32,
        cell_filter: Option<&SurrogateBitmap>,
    ) -> Response {
        if let Err(resp) = self.ensure_array_open(task, left) {
            return resp;
        }
        if let Err(resp) = self.ensure_array_open(task, right) {
            return resp;
        }
        let (schema, left_hash) = match self.array_engine.store(left) {
            Ok(s) => (s.schema().clone(), s.schema_hash()),
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Unsupported {
                        detail: format!("array '{}' not open: {e}", left.name),
                    },
                );
            }
        };
        let right_hash = match self.array_engine.store(right) {
            Ok(s) => s.schema_hash(),
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Unsupported {
                        detail: format!("array '{}' not open: {e}", right.name),
                    },
                );
            }
        };
        if left_hash != right_hash {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!(
                        "elementwise schema hash mismatch: left={left_hash:#x} right={right_hash:#x}"
                    ),
                },
            );
        }

        let left_tiles = match self
            .array_engine
            .scan_tiles(left, &MbrQueryPredicate::default())
        {
            Ok(t) => t,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("array elementwise scan left: {e}"),
                    },
                );
            }
        };
        let right_tiles = match self
            .array_engine
            .scan_tiles(right, &MbrQueryPredicate::default())
        {
            Ok(t) => t,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("array elementwise scan right: {e}"),
                    },
                );
            }
        };

        let left_union = match union_tiles(&schema, left_tiles) {
            Ok(t) => t,
            Err(code) => return self.response_error(task, code),
        };
        let left_union = match filter_by_surrogates(&schema, left_union, cell_filter) {
            Ok(t) => t,
            Err(code) => return self.response_error(task, code),
        };
        let right_union = match union_tiles(&schema, right_tiles) {
            Ok(t) => t,
            Err(code) => return self.response_error(task, code),
        };
        let right_union = match filter_by_surrogates(&schema, right_union, cell_filter) {
            Ok(t) => t,
            Err(code) => return self.response_error(task, code),
        };

        let bin = map_op(op);
        let combined = match elementwise(&schema, &left_union, &right_union, bin) {
            Ok(t) => t,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("array elementwise: {e}"),
                    },
                );
            }
        };

        let rows: Vec<Value> = sparse_tile_to_array_cells(&schema, &combined)
            .into_iter()
            .map(Value::NdArrayCell)
            .collect();
        encode_value_rows(self, task, &rows)
    }
}

fn map_op(op: ArrayBinaryOp) -> BinaryOp {
    match op {
        ArrayBinaryOp::Add => BinaryOp::Add,
        ArrayBinaryOp::Sub => BinaryOp::Sub,
        ArrayBinaryOp::Mul => BinaryOp::Mul,
        ArrayBinaryOp::Div => BinaryOp::Div,
    }
}

/// Return a copy of `tile` containing only rows whose surrogate is present in
/// `filter`. When `filter` is `None` the original tile is returned unchanged.
fn filter_by_surrogates(
    schema: &ArraySchema,
    tile: SparseTile,
    filter: Option<&SurrogateBitmap>,
) -> Result<SparseTile, ErrorCode> {
    let f = match filter {
        None => return Ok(tile),
        Some(f) => f,
    };
    let n = tile.nnz() as usize;
    let mut b = SparseTileBuilder::new(schema);
    for row in 0..n {
        let sur = tile
            .surrogates
            .get(row)
            .copied()
            .unwrap_or(nodedb_types::Surrogate::ZERO);
        if !f.contains(sur) {
            continue;
        }
        let coord: Vec<_> = tile
            .dim_dicts
            .iter()
            .map(|d| d.values[d.indices[row] as usize].clone())
            .collect();
        let attrs: Vec<_> = tile.attr_cols.iter().map(|c| c[row].clone()).collect();
        b.push_with_surrogate(&coord, &attrs, sur)
            .map_err(|e| ErrorCode::Internal {
                detail: format!("array elementwise filter: {e}"),
            })?;
    }
    Ok(b.build())
}

fn union_tiles(schema: &ArraySchema, tiles: Vec<TilePayload>) -> Result<SparseTile, ErrorCode> {
    let mut b = SparseTileBuilder::new(schema);
    for t in tiles {
        let sparse = match t {
            TilePayload::Sparse(s) => s,
            TilePayload::Dense(_) => {
                return Err(ErrorCode::Unsupported {
                    detail: "dense tile payload in elementwise".to_string(),
                });
            }
        };
        let n = sparse.nnz() as usize;
        for row in 0..n {
            let coord: Vec<_> = sparse
                .dim_dicts
                .iter()
                .map(|d| d.values[d.indices[row] as usize].clone())
                .collect();
            let attrs: Vec<_> = sparse.attr_cols.iter().map(|c| c[row].clone()).collect();
            let surrogate = sparse
                .surrogates
                .get(row)
                .copied()
                .unwrap_or(nodedb_types::Surrogate::ZERO);
            b.push_with_surrogate(&coord, &attrs, surrogate)
                .map_err(|e| ErrorCode::Internal {
                    detail: format!("array elementwise union: {e}"),
                })?;
        }
    }
    Ok(b.build())
}
