//! Concrete `ArrayLocalExecutor` implementation for the shard-side array handler.
//!
//! `DataPlaneArrayExecutor` implements the `ArrayLocalExecutor` trait defined in
//! `nodedb-cluster`. It bridges incoming distributed array RPC requests into the
//! local Data Plane via the SPSC bridge, awaits the response, and converts the
//! Data Plane response format into the zerompk-encoded shapes the cluster handler
//! expects.
//!
//! # Slice rows
//! The Data Plane encodes slice results as a flat msgpack array (one element per
//! row). This executor parses the array header and uses `skip_value` to extract
//! per-row byte slices, returning them as `Vec<Vec<u8>>`.
//!
//! # Surrogate bitmap scan
//! The Data Plane encodes surrogate scan results as a msgpack array of
//! `{"id": "<hex_surrogate>", "data": <empty_map>}` document rows. This executor
//! collects the hex surrogate strings, builds a `SurrogateBitmap`, and
//! zerompk-serializes it as the response.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use nodedb_array::types::ArrayId;
use nodedb_cluster::distributed_array::ArrayLocalExecutor;
use nodedb_cluster::distributed_array::merge::ArrayAggPartial;
use nodedb_cluster::distributed_array::wire::{ArrayShardAggReq, ArrayShardPutReq};
use nodedb_cluster::error::{ClusterError, Result};
use nodedb_query::msgpack_scan;
use nodedb_types::Surrogate;
use nodedb_types::SurrogateBitmap;
use zerompk;

use crate::bridge::envelope::{Priority, Request};
use crate::bridge::physical_plan::{ArrayOp, ArrayReducer, PhysicalPlan};
use crate::control::state::SharedState;
use crate::data::executor::response_codec::ArraySliceResponse;
use crate::event::types::EventSource;
use crate::types::{ReadConsistency, RequestId, TenantId, VShardId};

/// Timeout for a single shard-side array operation dispatched through the
/// local SPSC bridge. This bounds how long the cluster handler waits for the
/// Data Plane to respond before returning an error to the coordinator.
const LOCAL_DISPATCH_TIMEOUT: Duration = Duration::from_secs(30);

/// Base offset for request IDs allocated by this executor. Chosen to avoid
/// collision with client-originated IDs (which start at 1) and forwarded
/// execution IDs (which start at 2_000_000_000 in `LocalPlanExecutor`).
const REQUEST_ID_BASE: u64 = 3_000_000_000;

/// Concrete implementation of `ArrayLocalExecutor` backed by the local Data Plane.
///
/// Holds a reference to `SharedState` so it can dispatch `PhysicalPlan::Array`
/// variants through the SPSC bridge and await their responses via the
/// `RequestTracker`.
pub struct DataPlaneArrayExecutor {
    state: Arc<SharedState>,
    next_request_id: AtomicU64,
}

impl DataPlaneArrayExecutor {
    /// Construct an executor backed by the given shared state.
    pub fn new(state: Arc<SharedState>) -> Self {
        Self {
            state,
            next_request_id: AtomicU64::new(REQUEST_ID_BASE),
        }
    }

    fn next_id(&self) -> RequestId {
        RequestId::new(self.next_request_id.fetch_add(1, Ordering::Relaxed))
    }

    /// Dispatch a `PhysicalPlan` through the local SPSC bridge and await the
    /// single (non-streaming) response.
    async fn dispatch_and_await(
        &self,
        plan: PhysicalPlan,
    ) -> Result<crate::bridge::envelope::Response> {
        let request_id = self.next_id();

        let request = Request {
            request_id,
            tenant_id: TenantId::new(0),
            vshard_id: VShardId::new(0),
            plan,
            deadline: Instant::now() + LOCAL_DISPATCH_TIMEOUT,
            priority: Priority::Normal,
            trace_id: 0,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: EventSource::User,
            user_roles: Vec::new(),
        };

        let rx = self.state.tracker.register_oneshot(request_id);

        let dispatch_result = match self.state.dispatcher.lock() {
            Ok(mut d) => d.dispatch(request),
            Err(poisoned) => poisoned.into_inner().dispatch(request),
        };

        if let Err(e) = dispatch_result {
            return Err(ClusterError::Storage {
                detail: format!("array executor dispatch: {e}"),
            });
        }

        match tokio::time::timeout(LOCAL_DISPATCH_TIMEOUT, rx).await {
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(_)) => Err(ClusterError::Storage {
                detail: "array executor: response channel closed".into(),
            }),
            Err(_) => Err(ClusterError::Storage {
                detail: "array executor: local dispatch timed out".into(),
            }),
        }
    }
}

#[async_trait]
impl ArrayLocalExecutor for DataPlaneArrayExecutor {
    async fn exec_slice(
        &self,
        req: &nodedb_cluster::distributed_array::wire::ArrayShardSliceReq,
    ) -> Result<Vec<Vec<u8>>> {
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
            array_id,
            slice_msgpack: req.slice_msgpack.clone(),
            attr_projection: req.attr_projection.clone(),
            limit: req.limit,
            cell_filter,
            hilbert_range: req.shard_hilbert_range,
            system_as_of: req.system_as_of,
            valid_at_ms: req.valid_at_ms,
        });

        let resp = self.dispatch_and_await(plan).await?;

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
        let slice_resp: ArraySliceResponse =
            zerompk::from_msgpack(&resp.payload).map_err(|e| ClusterError::Codec {
                detail: format!("array slice response decode: {e}"),
            })?;
        split_msgpack_array_rows(&slice_resp.rows_msgpack)
    }

    async fn exec_agg(&self, req: &ArrayShardAggReq) -> Result<Vec<ArrayAggPartial>> {
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
            array_id,
            attr_idx: req.attr_idx,
            reducer,
            group_by_dim: req.group_by_dim,
            cell_filter,
            return_partial: true,
            hilbert_range: req.shard_hilbert_range,
            system_as_of: req.system_as_of,
            valid_at_ms: req.valid_at_ms,
        });

        let resp = self.dispatch_and_await(plan).await?;

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
            return Ok(Vec::new());
        }

        zerompk::from_msgpack::<Vec<ArrayAggPartial>>(&resp.payload).map_err(|e| {
            ClusterError::Codec {
                detail: format!("ArrayAggPartial decode in exec_agg: {e}"),
            }
        })
    }

    async fn exec_put(&self, req: &ArrayShardPutReq) -> Result<u64> {
        let array_id: ArrayId =
            zerompk::from_msgpack(&req.array_id_msgpack).map_err(|e| ClusterError::Codec {
                detail: format!("array_id decode in exec_put: {e}"),
            })?;

        // The coordinator encodes cells as `Vec<Vec<u8>>` (a blob-vec where
        // each inner bytes is a separately-encoded `ArrayPutCell`). The Data
        // Plane handler expects `Vec<ArrayPutCell>` encoded as a flat msgpack
        // array. Decode the outer blob-vec, parse each blob, and re-encode.
        let cell_blobs: Vec<Vec<u8>> =
            zerompk::from_msgpack(&req.cells_msgpack).map_err(|e| ClusterError::Codec {
                detail: format!("cell blob-vec decode in exec_put: {e}"),
            })?;

        let cells: Vec<crate::engine::array::wal::ArrayPutCell> = cell_blobs
            .iter()
            .map(|blob| {
                zerompk::from_msgpack(blob).map_err(|e| ClusterError::Codec {
                    detail: format!("ArrayPutCell decode in exec_put: {e}"),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let cells_msgpack = zerompk::to_msgpack_vec(&cells).map_err(|e| ClusterError::Codec {
            detail: format!("cells re-encode in exec_put: {e}"),
        })?;

        let plan = PhysicalPlan::Array(ArrayOp::Put {
            array_id,
            cells_msgpack,
            wal_lsn: req.wal_lsn,
        });

        let resp = self.dispatch_and_await(plan).await?;

        if resp.status == crate::bridge::envelope::Status::Error {
            let detail = resp
                .error_code
                .as_ref()
                .map(|c| format!("{c:?}"))
                .unwrap_or_else(|| "unknown Data Plane error".into());
            return Err(ClusterError::Storage {
                detail: format!("array put Data Plane error: {detail}"),
            });
        }

        Ok(req.wal_lsn)
    }

    async fn exec_delete(
        &self,
        array_id_msgpack: &[u8],
        coords_msgpack: &[u8],
        wal_lsn: u64,
    ) -> Result<u64> {
        let array_id: ArrayId =
            zerompk::from_msgpack(array_id_msgpack).map_err(|e| ClusterError::Codec {
                detail: format!("array_id decode in exec_delete: {e}"),
            })?;

        let plan = PhysicalPlan::Array(ArrayOp::Delete {
            array_id,
            coords_msgpack: coords_msgpack.to_vec(),
            wal_lsn,
        });

        let resp = self.dispatch_and_await(plan).await?;

        if resp.status == crate::bridge::envelope::Status::Error {
            let detail = resp
                .error_code
                .as_ref()
                .map(|c| format!("{c:?}"))
                .unwrap_or_else(|| "unknown Data Plane error".into());
            return Err(ClusterError::Storage {
                detail: format!("array delete Data Plane error: {detail}"),
            });
        }

        Ok(wal_lsn)
    }

    async fn exec_surrogate_bitmap_scan(
        &self,
        array_id_msgpack: &[u8],
        slice_msgpack: &[u8],
    ) -> Result<Vec<u8>> {
        let array_id: ArrayId =
            zerompk::from_msgpack(array_id_msgpack).map_err(|e| ClusterError::Codec {
                detail: format!("array_id decode in exec_surrogate_bitmap_scan: {e}"),
            })?;

        let plan = PhysicalPlan::Array(ArrayOp::SurrogateBitmapScan {
            array_id,
            slice_msgpack: slice_msgpack.to_vec(),
        });

        let resp = self.dispatch_and_await(plan).await?;

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
