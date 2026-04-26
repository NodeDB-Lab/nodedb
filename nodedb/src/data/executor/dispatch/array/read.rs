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
    /// Bitemporal system-time cutoff. `None` = live read.
    pub system_as_of: Option<i64>,
    /// Bitemporal valid-time point. `None` = no valid-time filter.
    pub valid_at_ms: Option<i64>,
}

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec::ArraySliceResponse;
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
            system_as_of,
            valid_at_ms,
        } = p;

        if let Err(resp) = self.ensure_array_open(task, array_id) {
            return resp;
        }
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

        let proj = if attr_projection.is_empty() {
            None
        } else {
            Some(Projection::new(
                attr_projection.iter().map(|&i| i as usize).collect(),
            ))
        };
        let cap = limit as usize;

        // Run through the Ceiling resolver. When no temporal filter is specified
        // the cutoff is `i64::MAX` (live read); Ceiling still deduplicates
        // multiple system-time versions of the same coord. The response shape
        // is `ArraySliceResponse` (rows + truncated_before_horizon flag) for
        // both local single-node and cluster shard responses.
        let cutoff = system_as_of.unwrap_or(i64::MAX);
        {
            let store = match self.array_engine.store(array_id) {
                Ok(s) => s,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("array '{}' not open: {e}", array_id.name),
                        },
                    );
                }
            };
            let (resolved_tiles, truncated_before_horizon) =
                match store.scan_tiles_at(cutoff, valid_at_ms) {
                    Ok(r) => r,
                    Err(e) => {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: format!("array bitemporal scan: {e}"),
                            },
                        );
                    }
                };

            let mut rows: Vec<Value> = Vec::new();
            'outer: for (hp, sparse) in resolved_tiles {
                if let Some((lo, hi)) = hilbert_range
                    && (hp < lo || hp > hi)
                {
                    continue;
                }
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
                        break 'outer;
                    }
                }
            }

            // Encode rows into the structured response. Build the rows msgpack
            // in-line (same shape as encode_value_rows) then wrap in the response.
            let rows_msgpack = {
                let mut buf: Vec<u8> = Vec::with_capacity(rows.len() * 64);
                let n = rows.len();
                if n < 16 {
                    buf.push(0x90 | n as u8);
                } else if n <= u16::MAX as usize {
                    buf.push(0xDC);
                    buf.extend_from_slice(&(n as u16).to_be_bytes());
                } else {
                    buf.push(0xDD);
                    buf.extend_from_slice(&(n as u32).to_be_bytes());
                }
                for row in &rows {
                    match nodedb_types::value_to_msgpack(row) {
                        Ok(b) => buf.extend_from_slice(&b),
                        Err(e) => {
                            return self.response_error(
                                task,
                                ErrorCode::Internal {
                                    detail: format!("array response encode: {e}"),
                                },
                            );
                        }
                    }
                }
                buf
            };
            let resp = ArraySliceResponse {
                rows_msgpack,
                truncated_before_horizon,
            };
            match zerompk::to_msgpack_vec(&resp) {
                Ok(bytes) => self.response_with_payload(task, bytes),
                Err(e) => self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("array slice response encode: {e}"),
                    },
                ),
            }
        }
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
