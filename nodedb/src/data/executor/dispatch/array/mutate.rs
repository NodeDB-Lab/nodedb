//! `ArrayOp::Put` / `Delete` / `Flush` / `Compact` handlers.

use nodedb_array::types::ArrayId;
use nodedb_array::types::coord::value::CoordValue;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::array::wal::ArrayPutCell;

impl CoreLoop {
    pub(in crate::data::executor) fn handle_array_put(
        &mut self,
        task: &ExecutionTask,
        array_id: &ArrayId,
        cells_msgpack: &[u8],
        wal_lsn: u64,
    ) -> Response {
        let cells: Vec<ArrayPutCell> = match zerompk::from_msgpack(cells_msgpack) {
            Ok(c) => c,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("array put decode: {e}"),
                    },
                );
            }
        };
        let n = cells.len();
        if let Err(e) = self.array_engine.put_cells(array_id, cells, wal_lsn) {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("array put: {e}"),
                },
            );
        }
        encode_count_response(self, task, "inserted", n)
    }

    pub(in crate::data::executor) fn handle_array_delete(
        &mut self,
        task: &ExecutionTask,
        array_id: &ArrayId,
        coords_msgpack: &[u8],
        wal_lsn: u64,
    ) -> Response {
        let coords: Vec<Vec<CoordValue>> = match zerompk::from_msgpack(coords_msgpack) {
            Ok(c) => c,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("array delete decode: {e}"),
                    },
                );
            }
        };
        let n = coords.len();
        if let Err(e) = self.array_engine.delete_cells(array_id, coords, wal_lsn) {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("array delete: {e}"),
                },
            );
        }
        encode_count_response(self, task, "deleted", n)
    }

    pub(in crate::data::executor) fn handle_array_flush(
        &mut self,
        task: &ExecutionTask,
        array_id: &ArrayId,
        wal_lsn: u64,
    ) -> Response {
        // The Control Plane allocated `wal_lsn` from the central WAL
        // writer; the engine just stamps it as the segment's flush
        // watermark.
        if let Err(e) = self.array_engine.flush(array_id, wal_lsn) {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("array flush: {e}"),
                },
            );
        }
        encode_count_response(self, task, "flushed", 1)
    }

    /// `ArrayOp::DropArray` handler — broadcast on `DROP ARRAY` after
    /// the Control-Plane catalog mutation. Releases the per-core store
    /// and removes the on-disk segment directory so a subsequent
    /// `CREATE ARRAY` with the same name (and possibly a different
    /// schema) starts from a clean slate. Idempotent: silently
    /// succeeds when this core never opened the array.
    pub(in crate::data::executor) fn handle_array_drop(
        &mut self,
        task: &ExecutionTask,
        array_id: &ArrayId,
    ) -> Response {
        if let Err(e) = self.array_engine.drop_array(array_id) {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("array drop: {e}"),
                },
            );
        }
        encode_count_response(self, task, "dropped", 1)
    }

    pub(in crate::data::executor) fn handle_array_compact(
        &mut self,
        task: &ExecutionTask,
        array_id: &ArrayId,
    ) -> Response {
        let merged = match self.array_engine.maybe_compact(array_id) {
            Ok(m) => m,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("array compact: {e}"),
                    },
                );
            }
        };
        encode_count_response(self, task, "compacted", usize::from(merged))
    }
}

fn encode_count_response(core: &CoreLoop, task: &ExecutionTask, key: &str, n: usize) -> Response {
    match super::super::super::response_codec::encode_count(key, n) {
        Ok(bytes) => core.response_with_payload(task, bytes),
        Err(e) => core.response_error(
            task,
            ErrorCode::Internal {
                detail: e.to_string(),
            },
        ),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use nodedb_array::schema::ArraySchemaBuilder;
    use nodedb_array::schema::attr_spec::{AttrSpec, AttrType};
    use nodedb_array::schema::dim_spec::{DimSpec, DimType};
    use nodedb_array::types::ArrayId;
    use nodedb_array::types::cell_value::value::CellValue;
    use nodedb_array::types::coord::value::CoordValue;
    use nodedb_array::types::domain::{Domain, DomainBound};
    use nodedb_bridge::buffer::RingBuffer;

    use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};
    use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Status};
    use crate::bridge::physical_plan::ArrayOp;
    use crate::data::executor::core_loop::CoreLoop;
    use crate::engine::array::wal::ArrayPutCell;
    use crate::types::*;

    fn make_request(plan: PhysicalPlan, id: u64) -> Request {
        Request {
            request_id: RequestId::new(id),
            tenant_id: TenantId::new(1),
            vshard_id: VShardId::new(0),
            plan,
            deadline: Instant::now() + Duration::from_secs(5),
            priority: Priority::Normal,
            trace_id: 0,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
        }
    }

    #[test]
    fn array_open_put_flush_smoke() {
        let dir = tempfile::tempdir().unwrap();
        let (req_tx_a, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        let mut req_tx = req_tx_a;
        let mut resp_rx = resp_rx;
        let mut core = CoreLoop::open(
            0,
            req_rx,
            resp_tx,
            dir.path(),
            Arc::new(nodedb_types::OrdinalClock::new()),
        )
        .unwrap();

        // 2D Int64 array with one Float64 attr.
        let schema = ArraySchemaBuilder::new("smoke")
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
            .unwrap();

        let schema_bytes = zerompk::to_msgpack_vec(&schema).unwrap();
        let schema_hash: u64 = 0xA11CEBEEF;
        let aid = ArrayId::new(TenantId::new(1), "smoke");

        // 1) OpenArray
        req_tx
            .try_push(BridgeRequest {
                inner: make_request(
                    PhysicalPlan::Array(ArrayOp::OpenArray {
                        array_id: aid.clone(),
                        schema_msgpack: schema_bytes.clone(),
                        schema_hash,
                    }),
                    1,
                ),
            })
            .unwrap();
        core.tick();
        let resp = resp_rx.try_pop().unwrap();
        assert_eq!(
            resp.inner.status,
            Status::Ok,
            "open response: {:?}",
            resp.inner
        );

        // 2) Put one cell
        let cells = vec![ArrayPutCell {
            coord: vec![CoordValue::Int64(1), CoordValue::Int64(2)],
            attrs: vec![CellValue::Float64(3.5)],
        }];
        let cells_bytes = zerompk::to_msgpack_vec(&cells).unwrap();
        req_tx
            .try_push(BridgeRequest {
                inner: make_request(
                    PhysicalPlan::Array(ArrayOp::Put {
                        array_id: aid.clone(),
                        cells_msgpack: cells_bytes,
                        wal_lsn: 42,
                    }),
                    2,
                ),
            })
            .unwrap();
        core.tick();
        let resp = resp_rx.try_pop().unwrap();
        assert_eq!(
            resp.inner.status,
            Status::Ok,
            "put response: {:?}",
            resp.inner
        );

        // 3) Flush
        req_tx
            .try_push(BridgeRequest {
                inner: make_request(
                    PhysicalPlan::Array(ArrayOp::Flush {
                        array_id: aid.clone(),
                        wal_lsn: 99,
                    }),
                    3,
                ),
            })
            .unwrap();
        core.tick();
        let resp = resp_rx.try_pop().unwrap();
        assert_eq!(
            resp.inner.status,
            Status::Ok,
            "flush response: {:?}",
            resp.inner
        );
    }

    /// `OpenArray` → `Put` → `DropArray` → re-`OpenArray` with a
    /// *different* schema_hash → `Slice` returns zero rows. Without
    /// `drop_array` releasing the per-core store, the second `OpenArray`
    /// would either reject the schema_hash mismatch or surface stale
    /// memtable cells.
    #[test]
    fn array_drop_clears_per_core_state() {
        use nodedb_array::types::cell_value::value::CellValue;

        let dir = tempfile::tempdir().unwrap();
        let (req_tx_a, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        let mut req_tx = req_tx_a;
        let mut resp_rx = resp_rx;
        let mut core = CoreLoop::open(
            0,
            req_rx,
            resp_tx,
            dir.path(),
            Arc::new(nodedb_types::OrdinalClock::new()),
        )
        .unwrap();

        // v1 schema: one float attr.
        let v1 = ArraySchemaBuilder::new("recyc")
            .dim(DimSpec::new(
                "k",
                DimType::Int64,
                Domain::new(DomainBound::Int64(0), DomainBound::Int64(15)),
            ))
            .attr(AttrSpec::new("qual", AttrType::Float64, true))
            .tile_extents(vec![16])
            .build()
            .unwrap();
        let v1_bytes = zerompk::to_msgpack_vec(&v1).unwrap();
        let aid = ArrayId::new(TenantId::new(1), "recyc");

        // 1) Open v1.
        req_tx
            .try_push(BridgeRequest {
                inner: make_request(
                    PhysicalPlan::Array(ArrayOp::OpenArray {
                        array_id: aid.clone(),
                        schema_msgpack: v1_bytes.clone(),
                        schema_hash: 0xAAAA,
                    }),
                    1,
                ),
            })
            .unwrap();
        core.tick();
        let resp = resp_rx.try_pop().unwrap();
        assert_eq!(resp.inner.status, Status::Ok, "open v1: {:?}", resp.inner);

        // 2) Put a cell — establishes memtable state.
        let cells = vec![ArrayPutCell {
            coord: vec![CoordValue::Int64(3)],
            attrs: vec![CellValue::Float64(42.0)],
        }];
        let cells_bytes = zerompk::to_msgpack_vec(&cells).unwrap();
        req_tx
            .try_push(BridgeRequest {
                inner: make_request(
                    PhysicalPlan::Array(ArrayOp::Put {
                        array_id: aid.clone(),
                        cells_msgpack: cells_bytes,
                        wal_lsn: 7,
                    }),
                    2,
                ),
            })
            .unwrap();
        core.tick();
        let resp = resp_rx.try_pop().unwrap();
        assert_eq!(resp.inner.status, Status::Ok, "put v1: {:?}", resp.inner);

        // 3) DropArray — releases per-core store + on-disk segment dir.
        req_tx
            .try_push(BridgeRequest {
                inner: make_request(
                    PhysicalPlan::Array(ArrayOp::DropArray {
                        array_id: aid.clone(),
                    }),
                    3,
                ),
            })
            .unwrap();
        core.tick();
        let resp = resp_rx.try_pop().unwrap();
        assert_eq!(resp.inner.status, Status::Ok, "drop: {:?}", resp.inner);

        // The Control Plane owns the shared array_catalog and unregisters
        // on `DROP ARRAY` *before* scattering `ArrayOp::DropArray`. Mirror
        // that here so the next `OpenArray` registers the new schema_hash
        // rather than colliding with v1's entry.
        {
            let mut cat = core.array_catalog.write().unwrap();
            cat.unregister("recyc");
        }

        // 4) Re-open with a DIFFERENT schema_hash. Without the drop, this
        //    would fail with `SchemaMismatch`. The post-drop state must
        //    accept the new hash.
        req_tx
            .try_push(BridgeRequest {
                inner: make_request(
                    PhysicalPlan::Array(ArrayOp::OpenArray {
                        array_id: aid.clone(),
                        schema_msgpack: v1_bytes,
                        schema_hash: 0xBBBB,
                    }),
                    4,
                ),
            })
            .unwrap();
        core.tick();
        let resp = resp_rx.try_pop().unwrap();
        assert_eq!(
            resp.inner.status,
            Status::Ok,
            "re-open after drop: {:?}",
            resp.inner
        );
    }
}
