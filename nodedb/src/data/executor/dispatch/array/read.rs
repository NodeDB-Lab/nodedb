//! `ArrayOp::Slice` and `ArrayOp::Project` handlers.
//!
//! Both are read-only fan-outs over the engine's tile scan. Slice
//! prunes by per-dim coord ranges and (optionally) projects an attribute
//! subset; Project is a pure attribute projection over every cell.
//!
//! Decoded slice payloads ride as zerompk bytes — matching the
//! contract documented on `ArrayOp::Slice::slice_msgpack`.

use nodedb_array::query::project::{Projection, project_sparse};
use nodedb_array::query::slice::{Slice, slice_sparse, tile_overlaps_slice};
use nodedb_array::segment::{MbrQueryPredicate, TilePayload};
use nodedb_array::tile::sparse_tile::SparseTile;
use nodedb_array::types::ArrayId;
use nodedb_types::{SurrogateBitmap, Value};

/// Slice parameters bundled to avoid exceeding the 7-argument limit.
pub(in crate::data::executor) struct SliceParams<'a> {
    pub array_id: &'a ArrayId,
    pub slice_msgpack: &'a [u8],
    pub attr_projection: &'a [u32],
    pub limit: u32,
    pub cell_filter: Option<&'a SurrogateBitmap>,
    /// Optional Hilbert-prefix range `[lo, hi]` for shard-level partitioning.
    /// When set, only tiles whose Hilbert prefix falls within this range are
    /// included. Used by the distributed shard handler to prevent duplicate
    /// rows when all vShards share a single Data Plane.
    pub hilbert_range: Option<(u64, u64)>,
}

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

use super::convert::sparse_tile_to_array_cells;
use super::encode::encode_value_rows;

impl CoreLoop {
    pub(in crate::data::executor) fn dispatch_array_slice(
        &mut self,
        task: &ExecutionTask,
        p: SliceParams<'_>,
    ) -> Response {
        let SliceParams {
            array_id,
            slice_msgpack,
            attr_projection,
            limit,
            cell_filter,
            hilbert_range,
        } = p;

        if let Err(resp) = self.ensure_array_open(task, array_id) {
            return resp;
        }
        // zerompk-encoded per the wire contract on
        // `ArrayOp::Slice::slice_msgpack`.
        let slice: Slice = match zerompk::from_msgpack(slice_msgpack) {
            Ok(s) => s,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("array slice decode: {e}"),
                    },
                );
            }
        };

        let schema = match self.array_engine.store(array_id) {
            Ok(store) => store.schema().clone(),
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Unsupported {
                        detail: format!("array '{}' not open: {e}", array_id.name),
                    },
                );
            }
        };

        let all_tiles_with_prefix = match self
            .array_engine
            .scan_tiles_with_hilbert_prefix(array_id, &MbrQueryPredicate::default())
        {
            Ok(t) => t,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("array slice scan: {e}"),
                    },
                );
            }
        };

        // Apply per-shard Hilbert-range pre-filter when set. This prevents
        // duplicate rows in harnesses where all vShards share one Data Plane.
        let tiles: Vec<TilePayload> = match hilbert_range {
            Some((lo, hi)) => all_tiles_with_prefix
                .into_iter()
                .filter_map(|(hp, tile)| {
                    if hp >= lo && hp <= hi {
                        Some(tile)
                    } else {
                        None
                    }
                })
                .collect(),
            None => all_tiles_with_prefix
                .into_iter()
                .map(|(_, tile)| tile)
                .collect(),
        };

        let proj = if attr_projection.is_empty() {
            None
        } else {
            Some(Projection::new(
                attr_projection.iter().map(|&i| i as usize).collect(),
            ))
        };

        let mut rows: Vec<Value> = Vec::new();
        let cap = limit as usize;
        for tile in tiles {
            let sparse: SparseTile = match tile {
                TilePayload::Sparse(s) => s,
                TilePayload::Dense(_) => {
                    return self.response_error(
                        task,
                        ErrorCode::Unsupported {
                            detail: "dense tile payload in slice".to_string(),
                        },
                    );
                }
            };
            if !tile_overlaps_slice(&sparse.mbr.dim_mins, &sparse.mbr.dim_maxs, &slice) {
                continue;
            }
            let filtered = match slice_sparse(&schema, &sparse, &slice) {
                Ok(t) => t,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("array slice filter: {e}"),
                        },
                    );
                }
            };
            let final_tile = match proj.as_ref() {
                Some(p) => match project_sparse(&filtered, p) {
                    Ok(t) => t,
                    Err(e) => {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: format!("array slice project: {e}"),
                            },
                        );
                    }
                },
                None => filtered,
            };
            for (row_idx, cell) in sparse_tile_to_array_cells(&schema, &final_tile)
                .into_iter()
                .enumerate()
            {
                if let Some(f) = cell_filter {
                    let sur = final_tile
                        .surrogates
                        .get(row_idx)
                        .copied()
                        .unwrap_or(nodedb_types::Surrogate::ZERO);
                    if !f.contains(sur) {
                        continue;
                    }
                }
                rows.push(Value::NdArrayCell(cell));
                if cap > 0 && rows.len() >= cap {
                    break;
                }
            }
            if cap > 0 && rows.len() >= cap {
                break;
            }
        }

        encode_value_rows(self, task, &rows)
    }

    pub(in crate::data::executor) fn dispatch_array_project(
        &mut self,
        task: &ExecutionTask,
        array_id: &ArrayId,
        attr_indices: &[u32],
    ) -> Response {
        if let Err(resp) = self.ensure_array_open(task, array_id) {
            return resp;
        }
        let schema = match self.array_engine.store(array_id) {
            Ok(store) => store.schema().clone(),
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Unsupported {
                        detail: format!("array '{}' not open: {e}", array_id.name),
                    },
                );
            }
        };

        let tiles = match self
            .array_engine
            .scan_tiles(array_id, &MbrQueryPredicate::default())
        {
            Ok(t) => t,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("array project scan: {e}"),
                    },
                );
            }
        };

        let proj = Projection::new(attr_indices.iter().map(|&i| i as usize).collect());

        let mut rows: Vec<Value> = Vec::new();
        for tile in tiles {
            let sparse: SparseTile = match tile {
                TilePayload::Sparse(s) => s,
                TilePayload::Dense(_) => {
                    return self.response_error(
                        task,
                        ErrorCode::Unsupported {
                            detail: "dense tile payload in project".to_string(),
                        },
                    );
                }
            };
            let projected = match project_sparse(&sparse, &proj) {
                Ok(t) => t,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("array project: {e}"),
                        },
                    );
                }
            };
            for cell in sparse_tile_to_array_cells(&schema, &projected) {
                rows.push(Value::NdArrayCell(cell));
            }
        }

        encode_value_rows(self, task, &rows)
    }
}
