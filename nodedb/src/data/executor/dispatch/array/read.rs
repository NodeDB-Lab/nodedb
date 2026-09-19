// SPDX-License-Identifier: BUSL-1.1

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
use nodedb_types::{ArrayCell, SurrogateBitmap, SystemTimeScope, Value};

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
    /// Bitemporal system-time scope.
    ///
    /// - `Current`: live read (effective cutoff `i64::MAX`).
    /// - `AsOf(t)`: point-in-time snapshot at `t`.
    /// - `AllVersions`: audit-log — every live cell-version, sorted ascending
    ///   by `system_from_ms`, limit bounds total versions.
    pub system_time: SystemTimeScope,
    /// Bitemporal valid-time point. `None` = no valid-time filter.
    pub valid_at_ms: Option<i64>,
}

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::overlay::ArrayOverlayMergeParams;
use crate::data::executor::task::ExecutionTask;

use super::convert::{cell_value_to_value, coord_value_to_value, sparse_tile_to_array_cells};
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
            system_time,
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

        match system_time {
            SystemTimeScope::AllVersions => {
                // Audit-log path: emit one row per live cell-version.
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
                let mut versions = match store.scan_tiles_all_versions(valid_at_ms) {
                    Ok(v) => v,
                    Err(e) => {
                        return self.response_error(
                            task,
                            ErrorCode::Internal {
                                detail: format!("array audit-log scan: {e}"),
                            },
                        );
                    }
                };

                // Apply Hilbert-range and slice coord filters.
                versions.retain(|(hp, coord, _sys_ms, _payload)| {
                    if let Some((lo, hi)) = hilbert_range
                        && (*hp < lo || *hp > hi)
                    {
                        return false;
                    }
                    // Check coord against slice predicate dim-by-dim.
                    slice_coord_matches(coord, &slice)
                });

                // Sort ascending by system_from_ms, ties by coord lexicographic order.
                versions.sort_by(|(_, coord_a, sys_a, _), (_, coord_b, sys_b, _)| {
                    sys_a.cmp(sys_b).then_with(|| {
                        coord_a
                            .partial_cmp(coord_b)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    })
                });

                // Collect rows.
                let mut rows: Vec<Value> = Vec::new();
                'av_outer: for (_, coord, sys_ms, payload) in versions {
                    // Apply cell_filter via surrogate.
                    if let Some(f) = cell_filter
                        && !f.contains(payload.surrogate)
                    {
                        continue;
                    }
                    // Build coords from CoordValue.
                    let coords: Vec<Value> = coord.iter().map(coord_value_to_value).collect();
                    // Build attrs from payload, applying projection.
                    let all_attrs: Vec<Value> =
                        payload.attrs.iter().map(cell_value_to_value).collect();
                    let attrs = if let Some(p) = proj.as_ref() {
                        p.attr_indices
                            .iter()
                            .map(|&i| all_attrs.get(i).cloned().unwrap_or(Value::Null))
                            .collect()
                    } else {
                        all_attrs
                    };
                    rows.push(Value::ArrayCell(ArrayCell {
                        coords,
                        attrs,
                        system_time: Some(sys_ms),
                    }));
                    if cap > 0 && rows.len() >= cap {
                        break 'av_outer;
                    }
                }

                encode_slice_rows(self, task, rows, false)
            }

            SystemTimeScope::Current | SystemTimeScope::AsOf(_) => {
                // Point-in-time / live path — use existing Ceiling resolver.
                let cutoff = match system_time {
                    SystemTimeScope::AsOf(t) => t,
                    SystemTimeScope::Current => i64::MAX,
                    SystemTimeScope::AllVersions => unreachable!(),
                };
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
                let (mut resolved_tiles, truncated_before_horizon) =
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
                // Read-your-own-writes: a live read inside a transaction sees
                // that transaction's staged cells. `AsOf` is a snapshot of
                // committed history and skips the overlay.
                if matches!(system_time, SystemTimeScope::Current)
                    && let Err(e) = self.merge_array_overlay_tiles(
                        ArrayOverlayMergeParams {
                            txn_id: task.request.txn_id,
                            array_id,
                            schema: &schema,
                            valid_at_ms,
                        },
                        &mut resolved_tiles,
                    )
                {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("array slice overlay merge: {e}"),
                        },
                    );
                }

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
                        rows.push(Value::ArrayCell(cell));
                        if cap > 0 && rows.len() >= cap {
                            break 'outer;
                        }
                    }
                }

                encode_slice_rows(self, task, rows, truncated_before_horizon)
            }
        }
    }
}

/// Check whether a coordinate tuple matches a slice predicate.
///
/// Returns `true` if for every constrained dimension the coord value falls
/// within `[lo, hi]`. Unconstrained dims (None) are always included.
fn slice_coord_matches(
    coord: &[nodedb_array::types::coord::value::CoordValue],
    slice: &Slice,
) -> bool {
    for (i, dr_opt) in slice.dim_ranges.iter().enumerate() {
        let Some(dr) = dr_opt else { continue };
        let Some(cv) = coord.get(i) else { return false };
        // Compare coord value against [lo, hi] domain bounds.
        if !coord_in_dim_range(cv, dr) {
            return false;
        }
    }
    true
}

fn coord_in_dim_range(
    cv: &nodedb_array::types::coord::value::CoordValue,
    dr: &nodedb_array::query::slice::DimRange,
) -> bool {
    // Convert DomainBound to CoordValue for comparison.
    let lo_cv = domain_bound_to_coord_value(&dr.lo);
    let hi_cv = domain_bound_to_coord_value(&dr.hi);
    let (Some(lo), Some(hi)) = (lo_cv, hi_cv) else {
        return true;
    };
    cv >= &lo && cv <= &hi
}

fn domain_bound_to_coord_value(
    b: &nodedb_array::types::domain::DomainBound,
) -> Option<nodedb_array::types::coord::value::CoordValue> {
    use nodedb_array::types::coord::value::CoordValue;
    use nodedb_array::types::domain::DomainBound;
    match b {
        DomainBound::Int64(v) => Some(CoordValue::Int64(*v)),
        DomainBound::Float64(v) => Some(CoordValue::Float64(*v)),
        DomainBound::TimestampMs(v) => Some(CoordValue::TimestampMs(*v)),
        DomainBound::String(v) => Some(CoordValue::String(v.clone())),
    }
}

/// Encode the slice result rows into an `ArraySliceResponse` and return a `Response`.
fn encode_slice_rows(
    core: &mut crate::data::executor::core_loop::CoreLoop,
    task: &crate::data::executor::task::ExecutionTask,
    rows: Vec<Value>,
    truncated_before_horizon: bool,
) -> crate::bridge::envelope::Response {
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
                    return core.response_error(
                        task,
                        crate::bridge::envelope::ErrorCode::Internal {
                            detail: format!("array response encode: {e}"),
                        },
                    );
                }
            }
        }
        buf
    };
    let resp = crate::data::executor::response_codec::ArraySliceResponse {
        rows_msgpack,
        truncated_before_horizon,
    };
    match zerompk::to_msgpack_vec(&resp) {
        Ok(bytes) => core.response_with_payload(task, bytes),
        Err(e) => core.response_error(
            task,
            crate::bridge::envelope::ErrorCode::Internal {
                detail: format!("array slice response encode: {e}"),
            },
        ),
    }
}

impl CoreLoop {
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

        let mut tiles = match self
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
        // Read-your-own-writes for a same-transaction read.
        if let Err(e) = self.merge_array_overlay_payloads(
            ArrayOverlayMergeParams {
                txn_id: task.request.txn_id,
                array_id,
                schema: &schema,
                valid_at_ms: None,
            },
            &mut tiles,
        ) {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("array project overlay merge: {e}"),
                },
            );
        }

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
                rows.push(Value::ArrayCell(cell));
            }
        }

        encode_value_rows(self, task, &rows)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use nodedb_array::query::slice::{DimRange, Slice as ArraySlice};
    use nodedb_array::schema::ArraySchema;
    use nodedb_array::schema::ArraySchemaBuilder;
    use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
    use nodedb_array::schema::dim_spec::{DimSpec, DimType};
    use nodedb_array::types::cell_value::value::CellValue;
    use nodedb_array::types::coord::value::CoordValue;
    use nodedb_array::types::domain::{Domain, DomainBound};
    use nodedb_bridge::buffer::{Consumer, Producer, RingBuffer};
    use nodedb_types::{ArrayCell, Value};

    use nodedb_types::{Surrogate, SurrogateBitmap};

    use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
    use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Status};
    use crate::data::executor::core_loop::CoreLoop;
    use crate::engine::array::wal::ArrayPutCell;
    use crate::types::*;
    use nodedb_physical::physical_plan::ArrayOp;

    use super::*;

    fn make_request(plan: PhysicalPlan, id: u64) -> Request {
        Request {
            request_id: RequestId::new(id),
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            plan,
            deadline: Instant::now() + Duration::from_secs(5),
            priority: Priority::Normal,
            trace_id: TraceId::ZERO,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn: None,
            resolved_now_ms: None,
            admission: crate::bridge::envelope::Admission::Admitted,
        }
    }

    fn schema_2d_f64(name: &str) -> ArraySchema {
        ArraySchemaBuilder::new(name)
            .dim(DimSpec::new(
                "x",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(15)),
            ))
            .dim(DimSpec::new(
                "y",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(15)),
            ))
            .attr(AttrSpec::new("v", AttrType::Float64, true))
            .tile_extents(vec![4, 4])
            .build()
            .unwrap()
    }

    struct Harness {
        core: CoreLoop,
        req_tx: Producer<BridgeRequest>,
        resp_rx: Consumer<BridgeResponse>,
        next_id: u64,
        _dir: tempfile::TempDir,
    }

    impl Harness {
        fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
            let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
            let core = CoreLoop::open(
                0,
                req_rx,
                resp_tx,
                dir.path(),
                Arc::new(nodedb_types::OrdinalClock::new()),
                crate::data::executor::core_loop::test_governor(),
            )
            .unwrap();
            Harness {
                core,
                req_tx,
                resp_rx,
                next_id: 1,
                _dir: dir,
            }
        }

        fn send(&mut self, op: ArrayOp) -> crate::bridge::envelope::Response {
            self.send_plan(PhysicalPlan::Array(op))
        }

        fn send_plan(&mut self, plan: PhysicalPlan) -> crate::bridge::envelope::Response {
            let id = self.next_id;
            self.next_id += 1;
            self.req_tx
                .try_push(BridgeRequest {
                    inner: make_request(plan, id),
                })
                .unwrap();
            self.core.tick();
            let resp = self.resp_rx.try_pop().unwrap();
            resp.inner
        }

        fn open(&mut self, aid: &ArrayId, schema: &ArraySchema, schema_hash: u64) {
            let bytes = zerompk::to_msgpack_vec(schema).unwrap();
            let r = self.send(ArrayOp::OpenArray {
                array_id: aid.clone(),
                schema_msgpack: bytes,
                schema_hash,
                prefix_bits: 8,
                audit_retain_ms: None,
                minimum_audit_retain_ms: None,
            });
            assert_eq!(r.status, Status::Ok, "open failed: {r:?}");
        }

        fn put(&mut self, aid: &ArrayId, cells: Vec<ArrayPutCell>, lsn: u64) {
            let bytes = zerompk::to_msgpack_vec(&cells).unwrap();
            let r = self.send(ArrayOp::Put {
                array_id: aid.clone(),
                cells_msgpack: bytes,
                wal_lsn: lsn,
                provenance: None,
            });
            assert_eq!(r.status, Status::Ok, "put failed: {r:?}");
        }

        fn flush(&mut self, aid: &ArrayId) {
            let r = self.send(ArrayOp::Flush {
                array_id: aid.clone(),
                wal_lsn: 0,
            });
            assert_eq!(r.status, Status::Ok, "flush failed: {r:?}");
        }
    }

    fn cell(x: i64, y: i64, v: f64) -> ArrayPutCell {
        ArrayPutCell {
            coord: vec![CoordValue::Int64(x), CoordValue::Int64(y)],
            attrs: vec![CellValue::Float64(v)],
            surrogate: nodedb_types::Surrogate::ZERO,
            system_from_ms: 0,
            valid_from_ms: 0,
            valid_until_ms: i64::MAX,
        }
    }

    fn cell_sur(x: i64, y: i64, v: f64, sur: u32) -> ArrayPutCell {
        ArrayPutCell {
            coord: vec![CoordValue::Int64(x), CoordValue::Int64(y)],
            attrs: vec![CellValue::Float64(v)],
            surrogate: Surrogate(sur),
            system_from_ms: 0,
            valid_from_ms: 0,
            valid_until_ms: i64::MAX,
        }
    }

    fn decode_slice_rows(bytes: &[u8]) -> Vec<Value> {
        // Slice responses are wrapped in `ArraySliceResponse { rows_msgpack, truncated_before_horizon }`.
        use crate::data::executor::response_codec::ArraySliceResponse;
        let envelope: ArraySliceResponse =
            zerompk::from_msgpack(bytes).expect("ArraySliceResponse envelope decode");
        let json = nodedb_types::msgpack_to_json_string(&envelope.rows_msgpack)
            .expect("slice rows msgpack→json");
        let arr: serde_json::Value = serde_json::from_str(&json).expect("slice rows json parse");
        let arr = arr.as_array().expect("slice rows are an array").clone();
        arr.into_iter().map(json_to_value).collect()
    }

    fn json_to_value(v: serde_json::Value) -> Value {
        match v {
            serde_json::Value::Object(map)
                if map.contains_key("coords") && map.contains_key("attrs") =>
            {
                let coords = map["coords"]
                    .as_array()
                    .expect("coords array")
                    .iter()
                    .cloned()
                    .map(json_to_value)
                    .collect();
                let attrs = map["attrs"]
                    .as_array()
                    .expect("attrs array")
                    .iter()
                    .cloned()
                    .map(json_to_value)
                    .collect();
                Value::ArrayCell(ArrayCell {
                    coords,
                    attrs,
                    system_time: None,
                })
            }
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Value::Integer(i)
                } else if let Some(f) = n.as_f64() {
                    Value::Float(f)
                } else {
                    Value::Null
                }
            }
            serde_json::Value::String(s) => Value::String(s),
            serde_json::Value::Bool(b) => Value::Bool(b),
            serde_json::Value::Null => Value::Null,
            serde_json::Value::Array(a) => Value::Array(a.into_iter().map(json_to_value).collect()),
            serde_json::Value::Object(_) => Value::Null,
        }
    }

    #[test]
    fn slice_returns_only_cells_in_range() {
        let mut h = Harness::new();
        let s = schema_2d_f64("t6_slice");
        let aid = ArrayId::new(TenantId::new(1), "t6_slice");
        h.open(&aid, &s, 0xA1);
        h.put(
            &aid,
            vec![
                cell(0, 0, 1.0),
                cell(1, 1, 2.0),
                cell(5, 5, 3.0),
                cell(7, 7, 4.0),
            ],
            100,
        );
        h.flush(&aid);

        // Slice x in [4, 9]: expects (5,5)=3 and (7,7)=4.
        let slice = ArraySlice::new(vec![
            Some(DimRange::new(DomainBound::Int64(4), DomainBound::Int64(9))),
            None,
        ]);
        let slice_bytes = zerompk::to_msgpack_vec(&slice).unwrap();
        let r = h.send(ArrayOp::Slice {
            array_id: aid.clone(),
            slice_msgpack: slice_bytes,
            attr_projection: vec![],
            limit: 0,
            cell_filter: None,
            hilbert_range: None,
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
        });
        assert_eq!(r.status, Status::Ok, "slice failed: {r:?}");
        let rows = decode_slice_rows(r.payload.as_ref());
        assert_eq!(rows.len(), 2, "expected two cells, got {rows:?}");
        let mut sums = 0.0;
        for v in rows {
            match v {
                Value::ArrayCell(ArrayCell { attrs, .. }) => match &attrs[0] {
                    Value::Float(f) => sums += f,
                    other => panic!("attr not Float: {other:?}"),
                },
                other => panic!("row not ArrayCell: {other:?}"),
            }
        }
        assert!((sums - 7.0).abs() < 1e-9);
    }

    #[test]
    fn slice_all_versions_emits_audit_log_ascending() {
        let mut h = Harness::new();
        let s = schema_2d_f64("t6_audit");
        let aid = ArrayId::new(TenantId::new(1), "t6_audit");
        h.open(&aid, &s, 0xA7);

        // Three system-time versions of the same cell (0,0), each sealed into
        // its own segment so they are distinct retained tile-versions.
        let mk = |v: f64, sys: i64| ArrayPutCell {
            coord: vec![CoordValue::Int64(0), CoordValue::Int64(0)],
            attrs: vec![CellValue::Float64(v)],
            surrogate: Surrogate::ZERO,
            system_from_ms: sys,
            valid_from_ms: 0,
            valid_until_ms: i64::MAX,
        };
        h.put(&aid, vec![mk(1.0, 100)], 1);
        h.flush(&aid);
        h.put(&aid, vec![mk(2.0, 200)], 2);
        h.flush(&aid);
        h.put(&aid, vec![mk(3.0, 300)], 3);
        h.flush(&aid);

        let slice = ArraySlice::new(vec![None, None]);
        let slice_bytes = zerompk::to_msgpack_vec(&slice).unwrap();
        let r = h.send(ArrayOp::Slice {
            array_id: aid.clone(),
            slice_msgpack: slice_bytes,
            attr_projection: vec![],
            limit: 0,
            cell_filter: None,
            hilbert_range: None,
            system_time: nodedb_types::SystemTimeScope::AllVersions,
            valid_at_ms: None,
        });
        assert_eq!(r.status, Status::Ok, "all-versions slice failed: {r:?}");

        // Read rows as raw JSON: `json_to_value` drops the injected `_ts_system`
        // column, so inspect the envelope directly.
        use crate::data::executor::response_codec::ArraySliceResponse;
        let env: ArraySliceResponse =
            zerompk::from_msgpack(r.payload.as_ref()).expect("ArraySliceResponse envelope");
        assert!(
            !env.truncated_before_horizon,
            "AllVersions applies no system-time horizon"
        );
        let json =
            nodedb_types::msgpack_to_json_string(&env.rows_msgpack).expect("rows msgpack→json");
        let rows: Vec<serde_json::Value> = serde_json::from_str(&json).expect("rows json parse");

        let got: Vec<(i64, f64)> = rows
            .iter()
            .map(|row| {
                let ts = row
                    .get("_ts_system")
                    .and_then(|v| v.as_i64())
                    .expect("_ts_system column present on audit-log rows");
                let v = row
                    .get("attrs")
                    .and_then(|a| a.as_array())
                    .and_then(|a| a.first())
                    .and_then(|v| v.as_f64())
                    .expect("attr value");
                (ts, v)
            })
            .collect();
        assert_eq!(
            got,
            vec![(100, 1.0), (200, 2.0), (300, 3.0)],
            "audit log must return every version ascending by system time"
        );
    }

    #[test]
    fn slice_cell_filter_excludes_non_member_surrogates() {
        let mut h = Harness::new();
        let s = schema_2d_f64("t6_sf_slice");
        let aid = ArrayId::new(TenantId::new(1), "t6_sf_slice");
        h.open(&aid, &s, 0xC1);
        // Three cells in the same tile region; surrogates 1, 2, 3.
        h.put(
            &aid,
            vec![
                cell_sur(0, 0, 10.0, 1),
                cell_sur(1, 1, 20.0, 2),
                cell_sur(2, 2, 30.0, 3),
            ],
            1,
        );
        h.flush(&aid);

        // Filter allows only surrogates 1 and 3 — surrogate 2 must be absent.
        let mut bm = SurrogateBitmap::new();
        bm.insert(Surrogate(1));
        bm.insert(Surrogate(3));

        let slice = nodedb_array::query::slice::Slice::new(vec![None, None]);
        let slice_bytes = zerompk::to_msgpack_vec(&slice).unwrap();
        let r = h.send(ArrayOp::Slice {
            array_id: aid.clone(),
            slice_msgpack: slice_bytes,
            attr_projection: vec![],
            limit: 0,
            cell_filter: Some(bm),
            hilbert_range: None,
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
        });
        assert_eq!(r.status, Status::Ok, "slice+filter failed: {r:?}");
        let rows = decode_slice_rows(r.payload.as_ref());
        assert_eq!(rows.len(), 2, "expected 2 cells, got {rows:?}");
        let mut total = 0.0;
        for v in rows {
            match v {
                Value::ArrayCell(ArrayCell { attrs, .. }) => match &attrs[0] {
                    Value::Float(f) => total += f,
                    other => panic!("attr not Float: {other:?}"),
                },
                other => panic!("row not ArrayCell: {other:?}"),
            }
        }
        // 10.0 + 30.0 = 40.0; 20.0 must have been excluded.
        assert!((total - 40.0).abs() < 1e-9, "total got {total}");
    }
}
