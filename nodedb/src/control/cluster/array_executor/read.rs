// SPDX-License-Identifier: BUSL-1.1

//! Read/scan handlers for [`DataPlaneArrayExecutor`] — slice, aggregate, and
//! surrogate-bitmap scan — plus the Data-Plane response-row parsers they use.

use nodedb_array::types::ArrayId;
use nodedb_cluster::distributed_array::merge::ArrayAggPartial;
use nodedb_cluster::distributed_array::wire::ArrayShardAggReq;
use nodedb_cluster::distributed_array::{ArrayAggExec, ArraySliceExec};
use nodedb_cluster::error::{ClusterError, Result};
use nodedb_query::msgpack_scan;
use nodedb_types::Surrogate;

use crate::types::{TxnId, VShardId};
use nodedb_types::SurrogateBitmap;

use super::executor::DataPlaneArrayExecutor;
use crate::data::executor::response_codec::ArraySliceResponse;
use nodedb_physical::physical_plan::{ArrayOp, ArrayReducer, PhysicalPlan};

impl DataPlaneArrayExecutor {
    pub(super) async fn slice(
        &self,
        local_vshard_id: u32,
        req: &nodedb_cluster::distributed_array::wire::ArrayShardSliceReq,
    ) -> Result<ArraySliceExec> {
        let array_id: ArrayId =
            zerompk::from_msgpack(&req.array_id_msgpack).map_err(|e| ClusterError::Codec {
                detail: format!("array_id decode in exec_slice: {e}"),
            })?;

        let cell_filter: Option<SurrogateBitmap> = if req.cell_filter_msgpack.is_empty() {
            None
        } else {
            Some(
                zerompk::from_msgpack(&req.cell_filter_msgpack).map_err(|e| {
                    ClusterError::Codec {
                        detail: format!("cell_filter decode in exec_slice: {e}"),
                    }
                })?,
            )
        };

        let plan = PhysicalPlan::Array(ArrayOp::Slice {
            array_id: array_id.clone(),
            slice_msgpack: req.slice_msgpack.clone(),
            attr_projection: req.attr_projection.clone(),
            limit: req.limit,
            cell_filter,
            hilbert_range: req.shard_hilbert_range,
            system_time: req.system_time,
            valid_at_ms: req.valid_at_ms,
        });

        let resp = self
            .dispatch_and_await(
                &array_id,
                VShardId::new(local_vshard_id),
                plan,
                req.txn_id.map(TxnId::new),
            )
            .await?;

        if resp.status == crate::bridge::envelope::Status::Error {
            let detail = resp
                .error_code
                .as_ref()
                .map(|c| format!("{c:?}"))
                .unwrap_or_else(|| "unknown Data Plane error".into());
            return Err(ClusterError::Storage {
                detail: format!("array slice Data Plane error: {detail}"),
            });
        }

        // Decode the structured `ArraySliceResponse` envelope, then split the
        // inner rows_msgpack into per-row byte slices for the cluster coordinator.
        // The `truncated_before_horizon` flag must be threaded back so the
        // coordinator can OR-reduce it across shards — dropping it here would
        // silently report complete results for below-horizon bitemporal reads.
        let slice_resp: ArraySliceResponse =
            zerompk::from_msgpack(&resp.payload).map_err(|e| ClusterError::Codec {
                detail: format!("array slice response decode: {e}"),
            })?;
        let rows = split_msgpack_array_rows(&slice_resp.rows_msgpack)?;
        Ok(ArraySliceExec {
            rows,
            truncated_before_horizon: slice_resp.truncated_before_horizon,
        })
    }

    pub(super) async fn agg(
        &self,
        local_vshard_id: u32,
        req: &ArrayShardAggReq,
    ) -> Result<ArrayAggExec> {
        let array_id: ArrayId =
            zerompk::from_msgpack(&req.array_id_msgpack).map_err(|e| ClusterError::Codec {
                detail: format!("array_id decode in exec_agg: {e}"),
            })?;

        let reducer: ArrayReducer =
            zerompk::from_msgpack(&req.reducer_msgpack).map_err(|e| ClusterError::Codec {
                detail: format!("reducer decode in exec_agg: {e}"),
            })?;

        let cell_filter: Option<SurrogateBitmap> = if req.cell_filter_msgpack.is_empty() {
            None
        } else {
            Some(
                zerompk::from_msgpack(&req.cell_filter_msgpack).map_err(|e| {
                    ClusterError::Codec {
                        detail: format!("cell_filter decode in exec_agg: {e}"),
                    }
                })?,
            )
        };

        let plan = PhysicalPlan::Array(ArrayOp::Aggregate {
            array_id: array_id.clone(),
            attr_idx: req.attr_idx,
            reducer,
            group_by_dim: req.group_by_dim,
            cell_filter,
            return_partial: true,
            hilbert_range: req.shard_hilbert_range,
            system_as_of: req.system_as_of,
            valid_at_ms: req.valid_at_ms,
        });

        let resp = self
            .dispatch_and_await(
                &array_id,
                VShardId::new(local_vshard_id),
                plan,
                req.txn_id.map(TxnId::new),
            )
            .await?;

        if resp.status == crate::bridge::envelope::Status::Error {
            let detail = resp
                .error_code
                .as_ref()
                .map(|c| format!("{c:?}"))
                .unwrap_or_else(|| "unknown Data Plane error".into());
            return Err(ClusterError::Storage {
                detail: format!("array agg Data Plane error: {detail}"),
            });
        }

        if resp.payload.is_empty() {
            return Ok(ArrayAggExec {
                partials: Vec::new(),
                truncated_before_horizon: false,
            });
        }

        // The Data Plane's `return_partial` path encodes a `(partials, flag)`
        // tuple (see `encode_bitemporal_agg_partial`), so decode the tuple —
        // decoding it as a bare `Vec<ArrayAggPartial>` is a shape mismatch and
        // fails outright, and would also drop the below-horizon signal.
        let (partials, truncated_before_horizon): (Vec<ArrayAggPartial>, bool) =
            zerompk::from_msgpack(&resp.payload).map_err(|e| ClusterError::Codec {
                detail: format!("ArrayAggPartial decode in exec_agg: {e}"),
            })?;
        Ok(ArrayAggExec {
            partials,
            truncated_before_horizon,
        })
    }

    pub(super) async fn surrogate_bitmap_scan(
        &self,
        local_vshard_id: u32,
        array_id_msgpack: &[u8],
        slice_msgpack: &[u8],
    ) -> Result<Vec<u8>> {
        let array_id: ArrayId =
            zerompk::from_msgpack(array_id_msgpack).map_err(|e| ClusterError::Codec {
                detail: format!("array_id decode in exec_surrogate_bitmap_scan: {e}"),
            })?;

        let plan = PhysicalPlan::Array(ArrayOp::SurrogateBitmapScan {
            array_id: array_id.clone(),
            slice_msgpack: slice_msgpack.to_vec(),
        });

        let resp = self
            .dispatch_and_await(&array_id, VShardId::new(local_vshard_id), plan, None)
            .await?;

        if resp.status == crate::bridge::envelope::Status::Error {
            let detail = resp
                .error_code
                .as_ref()
                .map(|c| format!("{c:?}"))
                .unwrap_or_else(|| "unknown Data Plane error".into());
            return Err(ClusterError::Storage {
                detail: format!("surrogate bitmap scan Data Plane error: {detail}"),
            });
        }

        collect_surrogate_bitmap(&resp.payload)
    }
}

/// Parse a flat msgpack array payload (produced by `encode_value_rows`) into
/// individual per-row byte slices.
///
/// The payload layout is: `[array_header][row0_bytes][row1_bytes]...`
/// where each row is a complete msgpack value (map or any other type).
///
/// Returns one `Vec<u8>` per row. Returns an empty `Vec` if the payload is
/// empty or contains an empty array.
fn split_msgpack_array_rows(payload: &[u8]) -> Result<Vec<Vec<u8>>> {
    if payload.is_empty() {
        return Ok(Vec::new());
    }

    let (count, mut offset) =
        msgpack_scan::array_header(payload, 0).ok_or_else(|| ClusterError::Codec {
            detail: "slice response: failed to read msgpack array header".into(),
        })?;

    let mut rows = Vec::with_capacity(count);
    for i in 0..count {
        let row_start = offset;
        let row_end =
            msgpack_scan::skip_value(payload, offset).ok_or_else(|| ClusterError::Codec {
                detail: format!("slice response: failed to skip row {i} at offset {offset}"),
            })?;
        rows.push(payload[row_start..row_end].to_vec());
        offset = row_end;
    }

    Ok(rows)
}

/// Parse a surrogate-bitmap-scan response payload (produced by
/// `encode_raw_document_rows`) and build a zerompk-serialized `SurrogateBitmap`.
///
/// The payload is a msgpack array of maps `{"id": "<hex_u32>", "data": ...}`.
/// The `id` field holds the surrogate value as a zero-padded 8-character
/// lowercase hex string (e.g. `"0000001a"`).
fn collect_surrogate_bitmap(payload: &[u8]) -> Result<Vec<u8>> {
    let mut bitmap = SurrogateBitmap::new();

    if payload.is_empty() {
        return serialize_bitmap(&bitmap);
    }

    let (count, mut offset) =
        msgpack_scan::array_header(payload, 0).ok_or_else(|| ClusterError::Codec {
            detail: "surrogate-scan response: failed to read msgpack array header".into(),
        })?;

    for _ in 0..count {
        // Extract the "id" field (hex-encoded u32 surrogate).
        if let Some((field_start, _field_end)) = msgpack_scan::extract_field(payload, offset, "id")
            && let Some(hex_str) = msgpack_scan::read_str(payload, field_start)
            && let Ok(val) = u32::from_str_radix(hex_str, 16)
            && val != 0
        {
            bitmap.insert(Surrogate::new(val));
        }

        // Advance past this entire map entry.
        offset = msgpack_scan::skip_value(payload, offset).ok_or_else(|| ClusterError::Codec {
            detail: "surrogate-scan response: failed to skip row".into(),
        })?;
    }

    serialize_bitmap(&bitmap)
}

fn serialize_bitmap(bitmap: &SurrogateBitmap) -> Result<Vec<u8>> {
    zerompk::to_msgpack_vec(bitmap).map_err(|e| ClusterError::Codec {
        detail: format!("SurrogateBitmap serialize: {e}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_empty_payload_returns_empty() {
        let rows = split_msgpack_array_rows(&[]).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn split_fixarray_zero_elements() {
        // fixarray with 0 elements = 0x90
        let rows = split_msgpack_array_rows(&[0x90]).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn split_fixarray_two_nil_elements() {
        // fixarray with 2 elements, each nil (0xc0)
        let payload = [0x92, 0xc0, 0xc0];
        let rows = split_msgpack_array_rows(&payload).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0], &[0xc0]);
        assert_eq!(rows[1], &[0xc0]);
    }

    #[test]
    fn collect_surrogate_bitmap_empty_payload() {
        let bytes = collect_surrogate_bitmap(&[]).unwrap();
        // Deserialize and confirm empty.
        let bm: SurrogateBitmap = zerompk::from_msgpack(&bytes).unwrap();
        assert!(bm.is_empty());
    }

    #[test]
    fn collect_surrogate_bitmap_with_entries() {
        // Build a fake payload: fixarray[2], each element is a fixmap{id->str, data->fixmap{}}
        // Row format: 0x82 (fixmap 2 entries)
        //   "id" -> "0000001a"
        //   "data" -> 0x80 (empty fixmap)
        fn encode_row(hex: &str) -> Vec<u8> {
            let mut v = vec![0x82u8]; // fixmap 2 entries
            // "id" key
            v.push(0xa2);
            v.extend_from_slice(b"id");
            // hex value as fixstr
            let hb = hex.as_bytes();
            v.push(0xa0 | hb.len() as u8);
            v.extend_from_slice(hb);
            // "data" key
            v.push(0xa4);
            v.extend_from_slice(b"data");
            v.push(0x80); // empty fixmap
            v
        }

        let row1 = encode_row("0000001a"); // 26
        let row2 = encode_row("0000002b"); // 43

        let mut payload = vec![0x92u8]; // fixarray 2
        payload.extend_from_slice(&row1);
        payload.extend_from_slice(&row2);

        let bytes = collect_surrogate_bitmap(&payload).unwrap();
        let bm: SurrogateBitmap = zerompk::from_msgpack(&bytes).unwrap();
        assert!(bm.contains(Surrogate::new(26)));
        assert!(bm.contains(Surrogate::new(43)));
        assert_eq!(bm.len(), 2);
    }
}
