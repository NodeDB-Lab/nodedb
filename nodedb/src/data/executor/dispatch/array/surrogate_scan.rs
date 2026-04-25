//! `ArrayOp::SurrogateBitmapScan` handler.
//!
//! Scans the array's tiles, applies the slice predicate, and emits one
//! row per matching cell where `id` is the cell's bound `Surrogate`
//! formatted as 8-char zero-padded lowercase hex (substrate row-key
//! format). Used by the cross-engine fusion path: the vector engine
//! invokes this as an `inline_prefilter_plan` and reads the response
//! through `collect_surrogates` to materialize a `SurrogateBitmap`.

use nodedb_array::query::slice::{Slice, slice_sparse, tile_overlaps_slice};
use nodedb_array::segment::{MbrQueryPredicate, TilePayload};
use nodedb_array::tile::sparse_tile::SparseTile;
use nodedb_array::types::ArrayId;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec::encode_raw_document_rows;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    pub(in crate::data::executor) fn dispatch_array_surrogate_bitmap_scan(
        &mut self,
        task: &ExecutionTask,
        array_id: &ArrayId,
        slice_msgpack: &[u8],
    ) -> Response {
        if let Err(resp) = self.ensure_array_open(task, array_id) {
            return resp;
        }
        let slice: Slice = match zerompk::from_msgpack(slice_msgpack) {
            Ok(s) => s,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("array surrogate-scan slice decode: {e}"),
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

        let tiles = match self
            .array_engine
            .scan_tiles(array_id, &MbrQueryPredicate::default())
        {
            Ok(t) => t,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("array surrogate-scan: {e}"),
                    },
                );
            }
        };

        let mut rows: Vec<(String, Vec<u8>)> = Vec::new();
        for tile in tiles {
            let sparse: SparseTile = match tile {
                TilePayload::Sparse(s) => s,
                TilePayload::Dense(_) => {
                    return self.response_error(
                        task,
                        ErrorCode::Unsupported {
                            detail: "dense tile payload in surrogate-scan".to_string(),
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
                            detail: format!("array surrogate-scan filter: {e}"),
                        },
                    );
                }
            };
            for sur in &filtered.surrogates {
                if sur.as_u32() == 0 {
                    continue;
                }
                let hex = format!("{:08x}", sur.as_u32());
                // Empty msgpack map as the row body — the consumer
                // (`collect_surrogates`) only reads `id`.
                rows.push((hex, vec![0x80]));
            }
        }

        match encode_raw_document_rows(&rows) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("surrogate-scan encode: {e}"),
                },
            ),
        }
    }
}
