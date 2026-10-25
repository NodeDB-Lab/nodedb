// SPDX-License-Identifier: BUSL-1.1

//! `ArrayOp::Elementwise` handler.
//!
//! Coord-aligned pairwise op between two open arrays sharing the same
//! schema hash. Each side reads its cells in the shard bucket, each cell
//! once at its newest version visible at the `AS OF` scope. A read with no
//! system-time cutoff also folds in the transaction's staged cells. Each
//! side unions into one sparse tile (schema comes from the left store). The
//! inner `elementwise` routine then handles outer-join semantics on
//! coordinates exactly.
//!
//! Each row's `attrs` holds one value: the op applied to the named attr. A
//! temporal read appends one `{"truncated_before_horizon": bool}` summary
//! row, as an aggregate does.
//!
//! Both arrays share one tile layout and routing, so the shard that owns a
//! coordinate holds that coordinate's cell on both sides.

use nodedb_array::query::elementwise::{BinaryOp, elementwise};
use nodedb_array::schema::ArraySchema;
use nodedb_array::tile::sparse_tile::{RowKind, SparseRow, SparseTile, SparseTileBuilder};
use nodedb_array::types::ArrayId;
use nodedb_types::array_route::shard_bucket_admits;
use nodedb_types::{ShardBucket, SurrogateBitmap, Value};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::overlay::ArrayOverlayMergeParams;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::ArrayBinaryOp;

use super::convert::sparse_tile_to_array_cells;
use super::encode::encode_value_rows;

/// Elementwise parameters bundled to stay under the argument limit.
pub(in crate::data::executor) struct ElementwiseParams<'a> {
    pub left: &'a ArrayId,
    pub right: &'a ArrayId,
    pub op: ArrayBinaryOp,
    /// Index of the attr the op reads.
    pub attr_idx: u32,
    pub cell_filter: Option<&'a SurrogateBitmap>,
    /// Only cells whose tile key routes to this bucket take part. `None`
    /// includes every cell.
    pub shard_bucket: Option<ShardBucket>,
    /// System-time cutoff for both sides. `None` = live read.
    pub system_as_of: Option<i64>,
    /// Valid-time point for both sides. `None` = no valid-time filter.
    pub valid_at_ms: Option<i64>,
}

/// The temporal scope one operand is read at.
#[derive(Clone, Copy)]
struct SideScan {
    shard_bucket: Option<ShardBucket>,
    system_as_of: Option<i64>,
    valid_at_ms: Option<i64>,
}

impl CoreLoop {
    pub(in crate::data::executor) fn dispatch_array_elementwise(
        &mut self,
        task: &ExecutionTask,
        p: ElementwiseParams<'_>,
    ) -> Response {
        let ElementwiseParams {
            left,
            right,
            op,
            attr_idx,
            cell_filter,
            shard_bucket,
            system_as_of,
            valid_at_ms,
        } = p;
        let scan = SideScan {
            shard_bucket,
            system_as_of,
            valid_at_ms,
        };
        let attr_idx = attr_idx as usize;
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

        let (left_tiles, left_truncated) = match self.elementwise_side(task, left, &schema, scan) {
            Ok(t) => t,
            Err(detail) => return self.response_error(task, ErrorCode::Internal { detail }),
        };
        let (right_tiles, right_truncated) = match self.elementwise_side(task, right, &schema, scan)
        {
            Ok(t) => t,
            Err(detail) => return self.response_error(task, ErrorCode::Internal { detail }),
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
        let combined = match elementwise(&schema, &left_union, &right_union, bin, attr_idx) {
            Ok(t) => t,
            Err(e @ nodedb_array::ArrayError::NumericOverflow { .. }) => {
                return self.response_error(
                    task,
                    ErrorCode::NumericValueOutOfRange {
                        detail: format!("array elementwise: {e}"),
                    },
                );
            }
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("array elementwise: {e}"),
                    },
                );
            }
        };

        let mut rows: Vec<Value> = Vec::with_capacity(combined.row_count() + 1);
        for mut cell in sparse_tile_to_array_cells(&schema, &combined) {
            // `elementwise` checked `attr_idx` against the schema's attrs.
            let Some(result) = cell.attrs.get(attr_idx).cloned() else {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!(
                            "array elementwise: row has {} attrs, op attr is {attr_idx}",
                            cell.attrs.len()
                        ),
                    },
                );
            };
            cell.attrs = vec![result];
            rows.push(Value::ArrayCell(cell));
        }
        if system_as_of.is_some() || valid_at_ms.is_some() {
            rows.push(horizon_summary(left_truncated || right_truncated));
        }
        encode_value_rows(self, task, &rows)
    }

    /// The tiles of one operand in the scan's bucket: each cell once at its
    /// newest version visible at the scan's `AS OF` scope. Returns the
    /// tiles and whether the system-time cutoff predates the retained
    /// history.
    ///
    /// A read with no system-time cutoff merges the transaction's staged
    /// cells. `AS OF SYSTEM TIME` reads committed history, so it skips them.
    /// The overlay merges before the union and the surrogate filter, so
    /// `cell_filter` still applies to both sides and an outer-join
    /// fallthrough from a staged cell drops exactly as one from a base cell.
    /// The bucket filter runs after the merge, so a staged cell outside the
    /// bucket drops too.
    fn elementwise_side(
        &self,
        task: &ExecutionTask,
        array_id: &ArrayId,
        schema: &ArraySchema,
        scan: SideScan,
    ) -> Result<(Vec<SparseTile>, bool), String> {
        let store = self
            .array_engine
            .store(array_id)
            .map_err(|e| format!("array '{}' not open: {e}", array_id.name))?;
        let cutoff = scan.system_as_of.unwrap_or(i64::MAX);
        let (mut tiles, truncated_before_horizon) = store
            .scan_shard_tiles_at(cutoff, scan.valid_at_ms, scan.shard_bucket, None)
            .map_err(|e| format!("array elementwise scan '{}': {e}", array_id.name))?;
        let overlay_txn = if scan.system_as_of.is_some() {
            None
        } else {
            task.request.txn_id
        };
        self.merge_array_overlay_tiles(
            ArrayOverlayMergeParams {
                txn_id: overlay_txn,
                array_id,
                schema,
                valid_at_ms: scan.valid_at_ms,
            },
            &mut tiles,
        )
        .map_err(|e| format!("array elementwise overlay merge '{}': {e}", array_id.name))?;
        let tiles = tiles
            .into_iter()
            .filter(|(hp, _)| shard_bucket_admits(scan.shard_bucket, *hp))
            .map(|(_, tile)| tile)
            .collect();
        Ok((tiles, truncated_before_horizon))
    }
}

/// The `{"truncated_before_horizon": bool}` row a temporal read appends.
fn horizon_summary(truncated_before_horizon: bool) -> Value {
    let mut summary = std::collections::HashMap::with_capacity(1);
    summary.insert(
        "truncated_before_horizon".to_string(),
        Value::Bool(truncated_before_horizon),
    );
    Value::Object(summary)
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
    let n = tile.row_count();
    let mut live_idx = 0usize;
    let mut b = SparseTileBuilder::new(schema);
    for row in 0..n {
        let kind = tile.row_kind(row).map_err(|e| ErrorCode::Internal {
            detail: format!("array elementwise filter row_kind: {e}"),
        })?;
        if kind != RowKind::Live {
            continue;
        }
        let attr_row = live_idx;
        live_idx += 1;
        // A stored live row always holds its bound surrogate.
        let sur = tile.live_surrogate(row).map_err(|e| ErrorCode::Internal {
            detail: format!("array elementwise filter: {e}"),
        })?;
        if !f.contains(sur) {
            continue;
        }
        let coord: Vec<_> = tile
            .dim_dicts
            .iter()
            .map(|d| d.values[d.indices[row] as usize].clone())
            .collect();
        let attrs: Vec<_> = tile.attr_cols.iter().map(|c| c[attr_row].clone()).collect();
        let valid_from_ms = tile.valid_from_ms.get(row).copied().unwrap_or(0);
        let valid_until_ms = tile
            .valid_until_ms
            .get(row)
            .copied()
            .unwrap_or(nodedb_types::OPEN_UPPER);
        b.push_row(SparseRow::live(
            &coord,
            &attrs,
            sur,
            valid_from_ms,
            valid_until_ms,
        ))
        .map_err(|e| ErrorCode::Internal {
            detail: format!("array elementwise filter: {e}"),
        })?;
    }
    Ok(b.build())
}

fn union_tiles(schema: &ArraySchema, tiles: Vec<SparseTile>) -> Result<SparseTile, ErrorCode> {
    let mut b = SparseTileBuilder::new(schema);
    for sparse in tiles {
        let n = sparse.row_count();
        let mut live_idx = 0usize;
        for row in 0..n {
            let kind = sparse.row_kind(row).map_err(|e| ErrorCode::Internal {
                detail: format!("array elementwise union row_kind: {e}"),
            })?;
            if kind != RowKind::Live {
                continue;
            }
            let attr_row = live_idx;
            live_idx += 1;
            let coord: Vec<_> = sparse
                .dim_dicts
                .iter()
                .map(|d| d.values[d.indices[row] as usize].clone())
                .collect();
            let attrs: Vec<_> = sparse
                .attr_cols
                .iter()
                .map(|c| c[attr_row].clone())
                .collect();
            // A stored live row always holds its bound surrogate.
            let surrogate = sparse
                .live_surrogate(row)
                .map_err(|e| ErrorCode::Internal {
                    detail: format!("array elementwise union: {e}"),
                })?;
            let valid_from_ms = sparse.valid_from_ms.get(row).copied().unwrap_or(0);
            let valid_until_ms = sparse
                .valid_until_ms
                .get(row)
                .copied()
                .unwrap_or(nodedb_types::OPEN_UPPER);
            b.push_row(SparseRow::live(
                &coord,
                &attrs,
                surrogate,
                valid_from_ms,
                valid_until_ms,
            ))
            .map_err(|e| ErrorCode::Internal {
                detail: format!("array elementwise union: {e}"),
            })?;
        }
    }
    Ok(b.build())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use nodedb_array::schema::ArraySchemaBuilder;
    use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
    use nodedb_array::schema::dim_spec::{DimSpec, DimType};
    use nodedb_array::types::cell_value::value::CellValue;
    use nodedb_array::types::coord::value::CoordValue;
    use nodedb_array::types::domain::{Domain, DomainBound};
    use nodedb_bridge::buffer::{Consumer, Producer, RingBuffer};
    use nodedb_types::{ArrayCell, Surrogate, Value};

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

    fn decode_value_vec(bytes: &[u8]) -> Vec<Value> {
        // Plain msgpack array of `value_to_msgpack`-encoded values (elementwise).
        let json = nodedb_types::msgpack_to_json_string(bytes).expect("payload msgpack→json");
        let arr: serde_json::Value = serde_json::from_str(&json).expect("payload json parse");
        let arr = arr.as_array().expect("rows are an array").clone();
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
    fn elementwise_add_two_arrays() {
        let mut h = Harness::new();
        let s = schema_2d_f64("t6_ew");
        let left = ArrayId::new(TenantId::new(1), "t6_ew_l");
        let right = ArrayId::new(TenantId::new(1), "t6_ew_r");
        h.open(&left, &s, 0xA4);
        h.open(&right, &s, 0xA4);
        h.put(&left, vec![cell(0, 0, 1.0), cell(1, 1, 2.0)], 1);
        h.put(&right, vec![cell(0, 0, 10.0), cell(1, 1, 20.0)], 2);
        h.flush(&left);
        h.flush(&right);

        let r = h.send(ArrayOp::Elementwise {
            left: left.clone(),
            right: right.clone(),
            op: ArrayBinaryOp::Add,
            attr_idx: 0,
            cell_filter: None,
            shard_bucket: None,
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
        });
        assert_eq!(r.status, Status::Ok, "ew failed: {r:?}");
        let rows = decode_value_vec(r.payload.as_ref());
        assert_eq!(rows.len(), 2);
        let mut total = 0.0;
        for v in rows {
            match v {
                Value::ArrayCell(ArrayCell { attrs, .. }) => match &attrs[0] {
                    Value::Float(f) => total += f,
                    Value::Integer(i) => total += *i as f64,
                    other => panic!("attr not numeric: {other:?}"),
                },
                other => panic!("row not ArrayCell: {other:?}"),
            }
        }
        assert!((total - 33.0).abs() < 1e-9);
    }

    #[test]
    fn elementwise_cell_filter_excludes_non_member_surrogates() {
        let mut h = Harness::new();
        let s = schema_2d_f64("t6_sf_ew");
        let left = ArrayId::new(TenantId::new(1), "t6_sf_ew_l");
        let right = ArrayId::new(TenantId::new(1), "t6_sf_ew_r");
        h.open(&left, &s, 0xC3);
        h.open(&right, &s, 0xC3);
        // Two cells per array; surrogates 1 and 2 on left.
        h.put(
            &left,
            vec![cell_sur(0, 0, 1.0, 1), cell_sur(1, 1, 2.0, 2)],
            1,
        );
        h.put(
            &right,
            vec![cell_sur(0, 0, 10.0, 1), cell_sur(1, 1, 20.0, 2)],
            2,
        );
        h.flush(&left);
        h.flush(&right);

        // Allow only surrogate 1 on left — only (0,0) participates: 1+10=11.
        let mut bm = nodedb_types::SurrogateBitmap::new();
        bm.insert(Surrogate(1));

        let r = h.send(ArrayOp::Elementwise {
            left: left.clone(),
            right: right.clone(),
            op: ArrayBinaryOp::Add,
            attr_idx: 0,
            cell_filter: Some(bm),
            shard_bucket: None,
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
        });
        assert_eq!(r.status, Status::Ok, "ew+filter failed: {r:?}");
        let rows = decode_value_vec(r.payload.as_ref());
        // After filtering left to surrogate 1 only, elementwise outer-join
        // with right yields one matching coord (0,0).
        assert_eq!(rows.len(), 1, "expected 1 cell, got {rows:?}");
        match &rows[0] {
            Value::ArrayCell(ArrayCell { attrs, .. }) => match &attrs[0] {
                Value::Float(f) => assert!((*f - 11.0).abs() < 1e-9, "add got {f}"),
                Value::Integer(i) => assert!((*i as f64 - 11.0).abs() < 1e-9, "add got {i}"),
                other => panic!("attr not numeric: {other:?}"),
            },
            other => panic!("row not ArrayCell: {other:?}"),
        }
    }

    #[test]
    fn elementwise_schema_hash_mismatch_errors() {
        let mut h = Harness::new();
        let s = schema_2d_f64("t6_ew_mis");
        let left = ArrayId::new(TenantId::new(1), "t6_ew_mis_l");
        let right = ArrayId::new(TenantId::new(1), "t6_ew_mis_r");
        h.open(&left, &s, 0xB1);
        h.open(&right, &s, 0xB2); // different hash
        let r = h.send(ArrayOp::Elementwise {
            left,
            right,
            op: ArrayBinaryOp::Add,
            attr_idx: 0,
            cell_filter: None,
            shard_bucket: None,
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
        });
        assert_ne!(r.status, Status::Ok);
    }

    /// The `Add` results of one elementwise call, by `(x, y)`.
    fn sums_by_coord(h: &mut Harness, op: ArrayOp) -> Vec<((i64, i64), f64)> {
        let r = h.send(op);
        assert_eq!(r.status, Status::Ok, "ew failed: {r:?}");
        let mut out: Vec<((i64, i64), f64)> = decode_value_vec(r.payload.as_ref())
            .into_iter()
            .map(|v| match v {
                Value::ArrayCell(ArrayCell { coords, attrs, .. }) => {
                    let int = |v: &Value| match v {
                        Value::Integer(i) => *i,
                        other => panic!("coord not an integer: {other:?}"),
                    };
                    let num = match &attrs[0] {
                        Value::Float(f) => *f,
                        Value::Integer(i) => *i as f64,
                        other => panic!("attr not numeric: {other:?}"),
                    };
                    ((int(&coords[0]), int(&coords[1])), num)
                }
                other => panic!("row not ArrayCell: {other:?}"),
            })
            .collect();
        out.sort_by_key(|(c, _)| *c);
        out
    }

    fn add(left: &ArrayId, right: &ArrayId, shard_bucket: Option<ShardBucket>) -> ArrayOp {
        ArrayOp::Elementwise {
            left: left.clone(),
            right: right.clone(),
            op: ArrayBinaryOp::Add,
            attr_idx: 0,
            cell_filter: None,
            shard_bucket,
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
        }
    }

    /// A cell overwritten in a later segment takes part once, at its newest
    /// value, on either side.
    #[test]
    fn an_overwritten_cell_pairs_once_at_its_newest_value() {
        let mut h = Harness::new();
        let s = schema_2d_f64("ew_ow");
        let left = ArrayId::new(TenantId::new(1), "ew_ow_l");
        let right = ArrayId::new(TenantId::new(1), "ew_ow_r");
        h.open(&left, &s, 0xD1);
        h.open(&right, &s, 0xD1);
        let at = |x: i64, y: i64, v: f64, sys: i64| ArrayPutCell {
            system_from_ms: sys,
            ..cell(x, y, v)
        };
        h.put(&left, vec![at(0, 0, 1.0, 100)], 1);
        h.flush(&left);
        h.put(&left, vec![at(0, 0, 5.0, 200)], 2);
        h.flush(&left);
        h.put(&right, vec![at(0, 0, 10.0, 100)], 3);
        h.flush(&right);

        assert_eq!(
            sums_by_coord(&mut h, add(&left, &right, None)),
            vec![((0, 0), 15.0)]
        );
    }

    /// Each bucket pairs only its own cells, and the buckets together pair
    /// every cell once.
    #[test]
    fn the_buckets_partition_the_pairs() {
        let mut h = Harness::new();
        let s = schema_2d_f64("ew_bk");
        let left = ArrayId::new(TenantId::new(1), "ew_bk_l");
        let right = ArrayId::new(TenantId::new(1), "ew_bk_r");
        h.open(&left, &s, 0xD2);
        h.open(&right, &s, 0xD2);
        let coords: Vec<(i64, i64)> = (0..4)
            .flat_map(|x| (0..4).map(move |y| (x * 4, y * 4)))
            .collect();
        h.put(
            &left,
            coords.iter().map(|&(x, y)| cell(x, y, 1.0)).collect(),
            1,
        );
        h.put(
            &right,
            coords.iter().map(|&(x, y)| cell(x, y, 2.0)).collect(),
            2,
        );
        h.flush(&left);
        h.flush(&right);

        let whole = sums_by_coord(&mut h, add(&left, &right, None));
        assert_eq!(whole.len(), coords.len());
        let mut parts = Vec::new();
        for bucket in 0..4u32 {
            let b = ShardBucket {
                bucket,
                prefix_bits: 2,
            };
            for (coord, sum) in sums_by_coord(&mut h, add(&left, &right, Some(b))) {
                let key = nodedb_array::tile::cell_tile_prefix(
                    &s,
                    &[CoordValue::Int64(coord.0), CoordValue::Int64(coord.1)],
                )
                .expect("tile key");
                assert!(b.contains(key), "{coord:?} paired outside bucket {bucket}");
                parts.push((coord, sum));
            }
        }
        parts.sort_by_key(|(c, _)| *c);
        assert_eq!(parts, whole, "the buckets together pair every cell once");
        assert!(whole.iter().all(|(_, sum)| *sum == 3.0));
    }

    /// The `attrs` of every row of one elementwise call, and the summary
    /// row's below-horizon flag when the call appends one.
    fn attrs_and_horizon(h: &mut Harness, op: ArrayOp) -> (Vec<Vec<Value>>, Option<bool>) {
        let r = h.send(op);
        assert_eq!(r.status, Status::Ok, "ew failed: {r:?}");
        let json = nodedb_types::msgpack_to_json_string(r.payload.as_ref()).expect("json");
        let rows: serde_json::Value = serde_json::from_str(&json).expect("parse");
        let mut attrs = Vec::new();
        let mut horizon = None;
        for row in rows.as_array().expect("rows are an array") {
            if let Some(flag) = row.get("truncated_before_horizon") {
                horizon = Some(flag.as_bool().expect("flag is a bool"));
                continue;
            }
            match json_to_value(row.clone()) {
                Value::ArrayCell(ArrayCell { attrs: a, .. }) => attrs.push(a),
                other => panic!("row not ArrayCell: {other:?}"),
            }
        }
        (attrs, horizon)
    }

    /// An `Add` on attr `attr_idx` at system-time cutoff `system_as_of`.
    fn add_at(
        left: &ArrayId,
        right: &ArrayId,
        attr_idx: u32,
        system_as_of: Option<i64>,
    ) -> ArrayOp {
        ArrayOp::Elementwise {
            left: left.clone(),
            right: right.clone(),
            op: ArrayBinaryOp::Add,
            attr_idx,
            cell_filter: None,
            shard_bucket: None,
            system_time: system_as_of.map_or(
                nodedb_types::SystemTimeScope::Current,
                nodedb_types::SystemTimeScope::AsOf,
            ),
            valid_at_ms: None,
        }
    }

    fn number(v: &Value) -> f64 {
        match v {
            Value::Float(f) => *f,
            Value::Integer(i) => *i as f64,
            other => panic!("attr not numeric: {other:?}"),
        }
    }

    /// The op reads the named attr of a two-attr array, and each row holds
    /// that one result.
    #[test]
    fn the_op_reads_the_named_attr() {
        let mut h = Harness::new();
        let s = ArraySchemaBuilder::new("ew_two")
            .dim(DimSpec::new(
                "x",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(15)),
            ))
            .attr(AttrSpec::new("a", AttrType::Float64, true))
            .attr(AttrSpec::new("b", AttrType::Float64, true))
            .tile_extents(vec![4])
            .build()
            .unwrap();
        let left = ArrayId::new(TenantId::new(1), "ew_two_l");
        let right = ArrayId::new(TenantId::new(1), "ew_two_r");
        h.open(&left, &s, 0xE1);
        h.open(&right, &s, 0xE1);
        let two = |a: f64, b: f64, sur: u32| ArrayPutCell {
            coord: vec![CoordValue::Int64(3)],
            attrs: vec![CellValue::Float64(a), CellValue::Float64(b)],
            surrogate: Surrogate(sur),
            system_from_ms: 0,
            valid_from_ms: 0,
            valid_until_ms: i64::MAX,
        };
        h.put(&left, vec![two(1.0, 2.0, 1)], 1);
        h.put(&right, vec![two(10.0, 20.0, 2)], 2);

        for (attr_idx, want) in [(0u32, 11.0), (1, 22.0)] {
            let (rows, horizon) = attrs_and_horizon(&mut h, add_at(&left, &right, attr_idx, None));
            assert_eq!(horizon, None, "a live read appends no summary row");
            assert_eq!(rows.len(), 1, "attr {attr_idx}: {rows:?}");
            assert_eq!(rows[0].len(), 1, "one result per row: {rows:?}");
            assert_eq!(number(&rows[0][0]), want, "attr {attr_idx}");
        }
    }

    /// `AS OF SYSTEM TIME` reads both sides at the cutoff and appends the
    /// below-horizon summary row.
    #[test]
    fn as_of_system_time_reads_both_sides_at_the_cutoff() {
        let mut h = Harness::new();
        let s = schema_2d_f64("ew_asof");
        let left = ArrayId::new(TenantId::new(1), "ew_asof_l");
        let right = ArrayId::new(TenantId::new(1), "ew_asof_r");
        h.open(&left, &s, 0xE2);
        h.open(&right, &s, 0xE2);
        let at = |v: f64, sys: i64| ArrayPutCell {
            system_from_ms: sys,
            ..cell(0, 0, v)
        };
        h.put(&left, vec![at(1.0, 100)], 1);
        h.put(&right, vec![at(10.0, 100)], 2);
        h.flush(&left);
        h.flush(&right);
        h.put(&left, vec![at(5.0, 200)], 3);
        h.put(&right, vec![at(50.0, 200)], 4);
        h.flush(&left);
        h.flush(&right);

        let as_of = |t: i64| add_at(&left, &right, 0, Some(t));
        let (rows, horizon) = attrs_and_horizon(&mut h, as_of(150));
        assert_eq!(horizon, Some(false));
        assert_eq!(rows.len(), 1);
        assert_eq!(
            number(&rows[0][0]),
            11.0,
            "both sides at their first version"
        );

        let (rows, _) = attrs_and_horizon(&mut h, as_of(250));
        assert_eq!(
            number(&rows[0][0]),
            55.0,
            "both sides at their second version"
        );

        let (live, horizon) = attrs_and_horizon(&mut h, add(&left, &right, None));
        assert_eq!(horizon, None);
        assert_eq!(number(&live[0][0]), 55.0);
    }
}
