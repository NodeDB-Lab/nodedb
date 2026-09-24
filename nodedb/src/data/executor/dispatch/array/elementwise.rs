// SPDX-License-Identifier: BUSL-1.1

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
use nodedb_array::tile::sparse_tile::{RowKind, SparseRow, SparseTile, SparseTileBuilder};
use nodedb_array::types::ArrayId;
use nodedb_types::{SurrogateBitmap, Value};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::overlay::ArrayOverlayMergeParams;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::ArrayBinaryOp;

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

        let mut left_tiles = match self
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
        let mut right_tiles = match self
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
        // Read-your-own-writes: each operand merges its own staged cells
        // BEFORE the union and the surrogate filter, so `cell_filter` still
        // applies to both sides and outer-join fallthroughs from a staged
        // cell are excluded exactly like those from a base cell.
        for (array_id, tiles, side) in [
            (left, &mut left_tiles, "left"),
            (right, &mut right_tiles, "right"),
        ] {
            if let Err(e) = self.merge_array_overlay_payloads(
                ArrayOverlayMergeParams {
                    txn_id: task.request.txn_id,
                    array_id,
                    schema: &schema,
                    valid_at_ms: None,
                },
                tiles,
            ) {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("array elementwise overlay merge {side}: {e}"),
                    },
                );
            }
        }

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
            .map(Value::ArrayCell)
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
        let attrs: Vec<_> = tile.attr_cols.iter().map(|c| c[attr_row].clone()).collect();
        let valid_from_ms = tile.valid_from_ms.get(row).copied().unwrap_or(0);
        let valid_until_ms = tile
            .valid_until_ms
            .get(row)
            .copied()
            .unwrap_or(nodedb_types::OPEN_UPPER);
        b.push_row(SparseRow {
            coord: &coord,
            attrs: &attrs,
            surrogate: sur,
            valid_from_ms,
            valid_until_ms,
            kind: RowKind::Live,
        })
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
            let surrogate = sparse
                .surrogates
                .get(row)
                .copied()
                .unwrap_or(nodedb_types::Surrogate::ZERO);
            let valid_from_ms = sparse.valid_from_ms.get(row).copied().unwrap_or(0);
            let valid_until_ms = sparse
                .valid_until_ms
                .get(row)
                .copied()
                .unwrap_or(nodedb_types::OPEN_UPPER);
            b.push_row(SparseRow {
                coord: &coord,
                attrs: &attrs,
                surrogate,
                valid_from_ms,
                valid_until_ms,
                kind: RowKind::Live,
            })
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
        });
        assert_ne!(r.status, Status::Ok);
    }
}
