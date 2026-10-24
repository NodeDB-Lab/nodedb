// SPDX-License-Identifier: BUSL-1.1

//! `ArrayOp::Slice` handler.
//!
//! A read-only fan-out over the engine's tile scan. Slice prunes by
//! per-dim coord ranges and (optionally) projects an attribute subset.
//! `ARRAY_PROJECT` plans to a slice over the whole domain.
//!
//! Decoded slice payloads ride as zerompk bytes — matching the
//! contract documented on `ArrayOp::Slice::slice_msgpack`. Each emitted
//! row is the named map `slice_row::named_cell_row` builds.

use nodedb_array::query::physical::{physical_cells, physical_sparse};
use nodedb_array::query::project::{Projection, project_sparse};
use nodedb_array::query::region::{ArrayRegion, PolygonZone};
use nodedb_array::query::slice::{Slice, filter_sparse, slice_sparse_owned};
use nodedb_array::types::ArrayId;
use nodedb_types::array_route::shard_bucket_admits;
use nodedb_types::{ShardBucket, SurrogateBitmap, SystemTimeScope, Value};

/// Slice parameters bundled to avoid exceeding the 7-argument limit.
pub(in crate::data::executor) struct SliceParams<'a> {
    pub array_id: &'a ArrayId,
    pub slice_msgpack: &'a [u8],
    pub attr_projection: &'a [u32],
    pub limit: u32,
    pub cell_filter: Option<&'a SurrogateBitmap>,
    /// Optional shard bucket. When set, only tiles whose key routes to this
    /// bucket are included, so vShards that share one Data Plane return no
    /// duplicate rows.
    pub shard_bucket: Option<ShardBucket>,
    /// Bitemporal system-time scope.
    ///
    /// - `Current`: live read (effective cutoff `i64::MAX`).
    /// - `AsOf(t)`: point-in-time snapshot at `t`.
    /// - `AllVersions`: audit-log — every live cell-version, sorted ascending
    ///   by `system_from_ms`, limit bounds total versions.
    pub system_time: SystemTimeScope,
    /// Bitemporal valid-time point. `None` = no valid-time filter.
    pub valid_at_ms: Option<i64>,
    /// zerompk-encoded [`PolygonZone`]. Empty = no zone.
    pub zone_msgpack: &'a [u8],
    /// Read attrs physically: a nodata cell as NULL, and a cell of an attr
    /// with a scale or offset as `raw * scale + offset`.
    pub physical: bool,
}

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::overlay::ArrayOverlayMergeParams;
use crate::data::executor::task::ExecutionTask;

use super::convert::{cell_value_to_value, coord_value_to_value, sparse_tile_to_array_cells};
use super::encode::encode_slice_rows;
use super::slice_row::named_cell_row;

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
            shard_bucket,
            system_time,
            valid_at_ms,
            zone_msgpack,
            physical,
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
        let zone: Option<PolygonZone> = if zone_msgpack.is_empty() {
            None
        } else {
            match zerompk::from_msgpack(zone_msgpack) {
                Ok(z) => Some(z),
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("array slice zone decode: {e}"),
                        },
                    );
                }
            }
        };
        // The slice box and the zone prune together. The zone's transformer
        // is built once for the whole scan.
        let region = ArrayRegion::new(slice.clone(), zone);
        let zone = region.zone.as_ref().map(PolygonZone::prepare);

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
                let mut versions = match store.scan_tiles_all_versions(valid_at_ms, shard_bucket) {
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

                // Apply the shard bucket, slice coord and zone filters.
                versions.retain(|(hp, coord, _sys_ms, _payload)| {
                    if !shard_bucket_admits(shard_bucket, *hp) {
                        return false;
                    }
                    // Check coord against slice predicate dim-by-dim.
                    slice_coord_matches(coord, &slice)
                        && zone.as_ref().is_none_or(|z| z.contains(coord))
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
                let mut rows: Vec<Vec<u8>> = Vec::new();
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
                    let all_attrs: Vec<Value> = if physical {
                        physical_cells(&schema, payload.attrs.clone())
                            .iter()
                            .map(cell_value_to_value)
                            .collect()
                    } else {
                        payload.attrs.iter().map(cell_value_to_value).collect()
                    };
                    let attrs = if let Some(p) = proj.as_ref() {
                        p.attr_indices
                            .iter()
                            .map(|&i| all_attrs.get(i).cloned().unwrap_or(Value::Null))
                            .collect()
                    } else {
                        all_attrs
                    };
                    match named_cell_row(&schema, coords, attrs, proj.as_ref(), Some(sys_ms)) {
                        Ok(row) => rows.push(row),
                        Err(e) => {
                            return self.response_error(
                                task,
                                ErrorCode::Internal {
                                    detail: format!("array audit-log row: {e}"),
                                },
                            );
                        }
                    }
                    if cap > 0 && rows.len() >= cap {
                        break 'av_outer;
                    }
                }

                encode_slice_rows(self, task, rows, false)
            }

            SystemTimeScope::Current
            | SystemTimeScope::AsOf(_)
            | SystemTimeScope::Pinned { .. } => {
                // Point-in-time / live path — use existing Ceiling resolver.
                // Array cells stamp system time in milliseconds.
                let cutoff = system_time.as_of_ms().unwrap_or(i64::MAX);
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
                // The region prunes whole prefixes by footer MBR before decode.
                let (mut resolved_tiles, truncated_before_horizon) = match store
                    .scan_shard_tiles_at(cutoff, valid_at_ms, shard_bucket, Some(&region))
                {
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

                let mut rows: Vec<Vec<u8>> = Vec::new();
                'outer: for (hp, sparse) in resolved_tiles {
                    if !shard_bucket_admits(shard_bucket, hp) {
                        continue;
                    }
                    if !region.tile_may_contain(&sparse.mbr) {
                        continue;
                    }
                    let sliced = slice_sparse_owned(&schema, sparse, &slice);
                    let filtered = match (sliced, zone.as_ref()) {
                        (Ok(t), None) => Ok(t),
                        (Ok(t), Some(z)) => filter_sparse(&schema, &t, |c| z.contains(c)),
                        (Err(e), _) => Err(e),
                    };
                    let filtered = match filtered {
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
                    let filtered = if physical {
                        physical_sparse(&schema, filtered)
                    } else {
                        filtered
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
                            // A slice holds live rows only, one per cell, and
                            // a stored live row always holds its surrogate.
                            let sur = match final_tile.live_surrogate(row_idx) {
                                Ok(sur) => sur,
                                Err(e) => {
                                    return self.response_error(
                                        task,
                                        ErrorCode::Internal {
                                            detail: format!("array slice filter: {e}"),
                                        },
                                    );
                                }
                            };
                            if !f.contains(sur) {
                                continue;
                            }
                        }
                        match named_cell_row(&schema, cell.coords, cell.attrs, proj.as_ref(), None)
                        {
                            Ok(row) => rows.push(row),
                            Err(e) => {
                                return self.response_error(
                                    task,
                                    ErrorCode::Internal {
                                        detail: format!("array slice row: {e}"),
                                    },
                                );
                            }
                        }
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
            commit_hlc: None,
            entry_version: None,
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
                .try_push(BridgeRequest::unfloored(make_request(plan, id)))
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
                vshard_id: 0,
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
            surrogate: nodedb_types::Surrogate::new((x * 100 + y + 1) as u32),
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

    /// The named rows of a slice response, one `{x, y, v}` map per cell.
    fn decode_slice_rows(bytes: &[u8]) -> Vec<serde_json::Map<String, serde_json::Value>> {
        // Slice responses are wrapped in `ArraySliceResponse { rows_msgpack, truncated_before_horizon }`.
        use crate::data::executor::response_codec::ArraySliceResponse;
        let envelope: ArraySliceResponse =
            zerompk::from_msgpack(bytes).expect("ArraySliceResponse envelope decode");
        let json = nodedb_types::msgpack_to_json_string(&envelope.rows_msgpack)
            .expect("slice rows msgpack→json");
        let rows: Vec<serde_json::Value> =
            serde_json::from_str(&json).expect("slice rows json parse");
        rows.into_iter()
            .map(|row| match row {
                serde_json::Value::Object(map) => map,
                other => panic!("slice row is not a map: {other}"),
            })
            .collect()
    }

    fn row_v(row: &serde_json::Map<String, serde_json::Value>) -> f64 {
        row.get("v")
            .and_then(serde_json::Value::as_f64)
            .unwrap_or_else(|| panic!("row has no float `v`: {row:?}"))
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
            shard_bucket: None,
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            zone_msgpack: Vec::new(),
            physical: false,
        });
        assert_eq!(r.status, Status::Ok, "slice failed: {r:?}");
        let rows = decode_slice_rows(r.payload.as_ref());
        assert_eq!(rows.len(), 2, "expected two cells, got {rows:?}");
        let mut coords: Vec<(i64, i64)> = rows
            .iter()
            .map(|row| (row["x"].as_i64().expect("x"), row["y"].as_i64().expect("y")))
            .collect();
        coords.sort();
        assert_eq!(coords, vec![(5, 5), (7, 7)]);
        let sums: f64 = rows.iter().map(row_v).sum();
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
            surrogate: Surrogate::new(1),
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
            shard_bucket: None,
            system_time: nodedb_types::SystemTimeScope::AllVersions,
            valid_at_ms: None,
            zone_msgpack: Vec::new(),
            physical: false,
        });
        assert_eq!(r.status, Status::Ok, "all-versions slice failed: {r:?}");

        // Read the envelope directly to check the horizon flag too.
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
                    .get("v")
                    .and_then(|v| v.as_f64())
                    .expect("attr `v` value");
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
            shard_bucket: None,
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            zone_msgpack: Vec::new(),
            physical: false,
        });
        assert_eq!(r.status, Status::Ok, "slice+filter failed: {r:?}");
        let rows = decode_slice_rows(r.payload.as_ref());
        assert_eq!(rows.len(), 2, "expected 2 cells, got {rows:?}");
        let total: f64 = rows.iter().map(row_v).sum();
        // 10.0 + 30.0 = 40.0; 20.0 must have been excluded.
        assert!((total - 40.0).abs() < 1e-9, "total got {total}");
    }

    fn schema_full_range_2d(name: &str) -> ArraySchema {
        let full = || Domain::new(DomainBound::Int64(i64::MIN), DomainBound::Int64(i64::MAX));
        ArraySchemaBuilder::new(name)
            .dim(DimSpec::new("x", DimType::Int64, full()))
            .dim(DimSpec::new("y", DimType::Int64, full()))
            .attr(AttrSpec::new("v", AttrType::Float64, true))
            .tile_extents(vec![1, 1])
            .build()
            .unwrap()
    }

    fn slice_in_bucket(
        h: &mut Harness,
        aid: &ArrayId,
        bucket: Option<ShardBucket>,
    ) -> Vec<serde_json::Map<String, serde_json::Value>> {
        let slice = ArraySlice::new(vec![None, None]);
        let r = h.send(ArrayOp::Slice {
            array_id: aid.clone(),
            slice_msgpack: zerompk::to_msgpack_vec(&slice).unwrap(),
            attr_projection: vec![],
            limit: 0,
            cell_filter: None,
            shard_bucket: bucket,
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            zone_msgpack: Vec::new(),
            physical: false,
        });
        assert_eq!(r.status, Status::Ok, "slice failed: {r:?}");
        decode_slice_rows(r.payload.as_ref())
    }

    /// A pixel-mode schema over [`schema_2d_f64`]: 1-degree pixels from
    /// (100°E, 8°N), x the column, y the row, in WGS 84.
    fn georef_schema(name: &str) -> ArraySchema {
        schema_2d_f64(name)
            .with_georef(Some(nodedb_array::schema::ArrayGeoref {
                srid: nodedb_types::geometry::Srid::WGS84,
                x_dim: 0,
                y_dim: 1,
                transform: Some(nodedb_array::schema::GeoTransform([
                    100.0, 1.0, 0.0, 8.0, 0.0, -1.0,
                ])),
            }))
            .unwrap()
    }

    /// A 4326 zone from 101°E to 103°E and 5°N to 7°N, framed over `schema`.
    /// It holds the pixels with x in 1..=2 and y in 1..=2.
    fn lon_lat_zone(schema: &ArraySchema) -> Vec<u8> {
        use nodedb_array::query::region::ZonePolygon;
        use nodedb_array::query::zone_frame::build_zone_frame;
        let polygons = vec![ZonePolygon {
            exterior: vec![
                [101.0, 5.0],
                [103.0, 5.0],
                [103.0, 7.0],
                [101.0, 7.0],
                [101.0, 5.0],
            ],
            holes: Vec::new(),
        }];
        let frame = build_zone_frame(
            schema,
            nodedb_types::geometry::Srid::WGS84,
            &polygons,
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect("frame");
        zerompk::to_msgpack_vec(&PolygonZone {
            x_dim: 0,
            y_dim: 1,
            polygons,
            frame: Some(frame),
        })
        .unwrap()
    }

    fn sorted_coords(rows: &[serde_json::Map<String, serde_json::Value>]) -> Vec<(i64, i64)> {
        let mut coords: Vec<(i64, i64)> = rows.iter().map(row_coords).collect();
        coords.sort();
        coords
    }

    #[test]
    fn slice_with_a_zone_returns_only_the_cells_inside_it() {
        let mut h = Harness::new();
        let s = georef_schema("t6_zone");
        let aid = ArrayId::new(TenantId::new(1), "t6_zone");
        h.open(&aid, &s, 0xD1);
        let cells: Vec<ArrayPutCell> = (0..5)
            .flat_map(|x| (0..5).map(move |y| cell(x, y, (x * 5 + y) as f64)))
            .collect();
        h.put(&aid, cells, 1);
        h.flush(&aid);

        let slice = ArraySlice::new(vec![None, None]);
        let r = h.send(ArrayOp::Slice {
            array_id: aid.clone(),
            slice_msgpack: zerompk::to_msgpack_vec(&slice).unwrap(),
            attr_projection: vec![],
            limit: 0,
            cell_filter: None,
            shard_bucket: None,
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
            zone_msgpack: lon_lat_zone(&s),
            physical: false,
        });
        assert_eq!(r.status, Status::Ok, "zone slice failed: {r:?}");
        let rows = decode_slice_rows(r.payload.as_ref());
        assert_eq!(
            sorted_coords(&rows),
            vec![(1, 1), (1, 2), (2, 1), (2, 2)],
            "pixel centres 101.5..102.5°E, 5.5..6.5°N"
        );
    }

    #[test]
    fn slice_all_versions_with_a_zone_keeps_only_the_cells_inside_it() {
        let mut h = Harness::new();
        let s = georef_schema("t6_zone_av");
        let aid = ArrayId::new(TenantId::new(1), "t6_zone_av");
        h.open(&aid, &s, 0xD2);
        let at = |x: i64, y: i64, v: f64, sys: i64| ArrayPutCell {
            system_from_ms: sys,
            ..cell(x, y, v)
        };
        h.put(&aid, vec![at(1, 1, 1.0, 100), at(4, 4, 9.0, 100)], 1);
        h.flush(&aid);
        h.put(&aid, vec![at(1, 1, 2.0, 200), at(4, 4, 8.0, 200)], 2);
        h.flush(&aid);

        let slice = ArraySlice::new(vec![None, None]);
        let r = h.send(ArrayOp::Slice {
            array_id: aid.clone(),
            slice_msgpack: zerompk::to_msgpack_vec(&slice).unwrap(),
            attr_projection: vec![],
            limit: 0,
            cell_filter: None,
            shard_bucket: None,
            system_time: nodedb_types::SystemTimeScope::AllVersions,
            valid_at_ms: None,
            zone_msgpack: lon_lat_zone(&s),
            physical: false,
        });
        assert_eq!(r.status, Status::Ok, "zone audit slice failed: {r:?}");
        let rows = decode_slice_rows(r.payload.as_ref());
        let got: Vec<((i64, i64), f64)> = rows.iter().map(|r| (row_coords(r), row_v(r))).collect();
        assert_eq!(
            got,
            vec![((1, 1), 1.0), ((1, 1), 2.0)],
            "both versions of the cell inside the zone, none of the cell outside it"
        );
    }

    fn row_coords(row: &serde_json::Map<String, serde_json::Value>) -> (i64, i64) {
        (
            row["x"].as_i64().expect("x coord"),
            row["y"].as_i64().expect("y coord"),
        )
    }

    /// Over every bucket, the shard filter returns each cell exactly once.
    #[test]
    fn shard_buckets_partition_the_cells_of_a_full_range_array() {
        let mut h = Harness::new();
        let s = schema_full_range_2d("t6_buckets");
        let aid = ArrayId::new(TenantId::new(1), "t6_buckets");
        h.open(&aid, &s, 0xB1);
        let cells: Vec<ArrayPutCell> = (0..8)
            .flat_map(|x| (0..8).map(move |y| cell(x, y, (x * 8 + y) as f64)))
            .collect();
        h.put(&aid, cells, 1);
        h.flush(&aid);

        let prefix_bits = 8u8;
        let mut seen: Vec<String> = Vec::new();
        let mut non_empty = 0usize;
        for bucket in 0..(1u32 << prefix_bits) {
            let rows = slice_in_bucket(
                &mut h,
                &aid,
                Some(ShardBucket {
                    bucket,
                    prefix_bits,
                }),
            );
            if !rows.is_empty() {
                non_empty += 1;
            }
            seen.extend(rows.iter().map(|r| format!("{:?}", row_coords(r))));
        }
        assert_eq!(
            seen.len(),
            64,
            "every cell is returned by exactly one bucket"
        );
        seen.sort();
        seen.dedup();
        assert_eq!(seen.len(), 64, "no cell is returned by two buckets");
        assert!(non_empty >= 32, "64 tiles land in only {non_empty} buckets");
        assert_eq!(slice_in_bucket(&mut h, &aid, None).len(), 64);
    }

    /// Two tiles `2^bits` apart share one key, so they share one bucket, and
    /// a read of that bucket returns both cells.
    #[test]
    fn tiles_that_share_a_key_route_together_and_both_cells_read_back() {
        let mut h = Harness::new();
        let s = schema_full_range_2d("t6_collide");
        let aid = ArrayId::new(TenantId::new(1), "t6_collide");
        h.open(&aid, &s, 0xB2);
        let far = 1i64 << nodedb_array::coord::bits_per_dim(2);
        let near_coord = [CoordValue::Int64(3), CoordValue::Int64(5)];
        let far_coord = [CoordValue::Int64(3 + far), CoordValue::Int64(5)];
        let key = nodedb_array::tile::cell_tile_prefix(&s, &near_coord).unwrap();
        assert_eq!(
            nodedb_array::tile::cell_tile_prefix(&s, &far_coord).unwrap(),
            key,
            "the fixture's two tiles share one key"
        );
        h.put(
            &aid,
            vec![cell_sur(3, 5, 1.0, 7), cell_sur(3 + far, 5, 2.0, 8)],
            1,
        );
        h.flush(&aid);

        let prefix_bits = 8u8;
        let owner = ShardBucket {
            bucket: nodedb_types::array_route::prefix_bucket(key, prefix_bits),
            prefix_bits,
        };
        let rows = slice_in_bucket(&mut h, &aid, Some(owner));
        assert_eq!(
            rows.len(),
            2,
            "both cells of the shared key read back: {rows:?}"
        );
        let other = ShardBucket {
            bucket: (owner.bucket + 1) % (1u32 << prefix_bits),
            prefix_bits,
        };
        assert!(slice_in_bucket(&mut h, &aid, Some(other)).is_empty());
    }
}
