// SPDX-License-Identifier: BUSL-1.1

//! `ArrayOp::Put` / `Delete` / `Flush` / `Compact` handlers.

use nodedb_array::types::ArrayId;
use nodedb_types::sync::wire::SyncProvenance;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::array::wal::{ArrayDeleteCell, ArrayPutCell};

impl CoreLoop {
    pub(in crate::data::executor) fn handle_array_put(
        &mut self,
        task: &ExecutionTask,
        array_id: &ArrayId,
        cells_msgpack: &[u8],
        wal_lsn: u64,
        provenance: Option<&SyncProvenance>,
    ) -> Response {
        // Epoch fence — reject stale/zombie producers. Array's HLC dedup
        // handles cell-level idempotency; this is additive epoch protection.
        if let Some(prov) = provenance
            && !self.sync_fence(prov)
        {
            return self.response_error(
                task,
                ErrorCode::RejectedPrevalidation {
                    reason: format!(
                        "array put fenced: producer_id={} epoch={}",
                        prov.producer_id, prov.epoch
                    ),
                },
            );
        }
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
        // Advance the collection floor for this committed array write. The
        // Control Plane allocated `wal_lsn` from the central WAL writer, so it
        // is the committed write LSN in the same space as the core watermark.
        if wal_lsn > 0 {
            self.note_write_lsn(
                task.request.database_id,
                task.request.tenant_id,
                &array_id.name,
                None,
                crate::types::Lsn::new(wal_lsn),
            );
        }
        // Advance HWM after durable write so the producer's array stream
        // frontier is tracked and reconstructable on replay.
        if let Some(prov) = provenance {
            self.sync_commit(prov);
        }
        encode_count_response(self, task, "inserted", n)
    }

    pub(in crate::data::executor) fn handle_array_delete(
        &mut self,
        task: &ExecutionTask,
        array_id: &ArrayId,
        coords_msgpack: &[u8],
        wal_lsn: u64,
        provenance: Option<&SyncProvenance>,
    ) -> Response {
        // Epoch fence — additive to HLC dedup.
        if let Some(prov) = provenance
            && !self.sync_fence(prov)
        {
            return self.response_error(
                task,
                ErrorCode::RejectedPrevalidation {
                    reason: format!(
                        "array delete fenced: producer_id={} epoch={}",
                        prov.producer_id, prov.epoch
                    ),
                },
            );
        }
        let cells: Vec<ArrayDeleteCell> = match zerompk::from_msgpack(coords_msgpack) {
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
        // Report the number of cells that existed, not the number of
        // coordinates named: a delete of an absent coordinate affects nothing.
        let mut existed: usize = 0;
        for cell in &cells {
            match self.array_engine.contains_cell(array_id, &cell.coord) {
                Ok(true) => existed += 1,
                Ok(false) => {}
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("array delete lookup: {e}"),
                        },
                    );
                }
            }
        }
        if let Err(e) = self.array_engine.delete_cells(array_id, cells, wal_lsn) {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("array delete: {e}"),
                },
            );
        }
        // Advance the collection floor for this committed array delete (see
        // `handle_array_put` for why `wal_lsn` is the committed write LSN).
        if wal_lsn > 0 {
            self.note_write_lsn(
                task.request.database_id,
                task.request.tenant_id,
                &array_id.name,
                None,
                crate::types::Lsn::new(wal_lsn),
            );
        }
        if let Some(prov) = provenance {
            self.sync_commit(prov);
        }
        encode_count_response(self, task, "deleted", existed)
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

    /// Stage, restore, and purge form the reversible physical side of DROP.
    pub(in crate::data::executor) fn handle_array_drop(
        &mut self,
        task: &ExecutionTask,
        array_id: &ArrayId,
    ) -> Response {
        if let Err(e) = self.array_engine.stage_drop_array(array_id) {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("array drop stage: {e}"),
                },
            );
        }
        encode_count_response(self, task, "dropped", 1)
    }

    pub(in crate::data::executor) fn handle_array_drop_restore(
        &mut self,
        task: &ExecutionTask,
        array_id: &ArrayId,
    ) -> Response {
        if let Err(e) = self.array_engine.restore_drop_array(array_id) {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("array drop restore: {e}"),
                },
            );
        }
        encode_count_response(self, task, "restored", 1)
    }

    pub(in crate::data::executor) fn handle_array_drop_purge(
        &mut self,
        task: &ExecutionTask,
        array_id: &ArrayId,
    ) -> Response {
        if let Err(e) = self.array_engine.purge_drop_array(array_id) {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("array drop purge: {e}"),
                },
            );
        }
        encode_count_response(self, task, "purged", 1)
    }

    pub(in crate::data::executor) fn handle_array_compact(
        &mut self,
        task: &ExecutionTask,
        array_id: &ArrayId,
        audit_retain_ms: Option<i64>,
    ) -> Response {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let merged = match self
            .array_engine
            .maybe_compact(array_id, audit_retain_ms, now_ms)
        {
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
    use crate::data::executor::core_loop::CoreLoop;
    use crate::engine::array::wal::ArrayPutCell;
    use crate::types::*;
    use nodedb_physical::physical_plan::ArrayOp;

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
            crate::data::executor::core_loop::test_governor(),
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
            .try_push(BridgeRequest::unfloored(make_request(
                PhysicalPlan::Array(ArrayOp::OpenArray {
                    array_id: aid.clone(),
                    schema_msgpack: schema_bytes.clone(),
                    schema_hash,
                    prefix_bits: 8,
                    audit_retain_ms: None,
                    minimum_audit_retain_ms: None,
                }),
                1,
            )))
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
            surrogate: nodedb_types::Surrogate::ZERO,
            system_from_ms: 0,
            valid_from_ms: 0,
            valid_until_ms: i64::MAX,
        }];
        let cells_bytes = zerompk::to_msgpack_vec(&cells).unwrap();
        req_tx
            .try_push(BridgeRequest::unfloored(make_request(
                PhysicalPlan::Array(ArrayOp::Put {
                    array_id: aid.clone(),
                    cells_msgpack: cells_bytes,
                    wal_lsn: 42,
                    provenance: None,
                }),
                2,
            )))
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
            .try_push(BridgeRequest::unfloored(make_request(
                PhysicalPlan::Array(ArrayOp::Flush {
                    array_id: aid.clone(),
                    wal_lsn: 99,
                }),
                3,
            )))
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
            crate::data::executor::core_loop::test_governor(),
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
            .try_push(BridgeRequest::unfloored(make_request(
                PhysicalPlan::Array(ArrayOp::OpenArray {
                    array_id: aid.clone(),
                    schema_msgpack: v1_bytes.clone(),
                    schema_hash: 0xAAAA,
                    prefix_bits: 8,
                    audit_retain_ms: None,
                    minimum_audit_retain_ms: None,
                }),
                1,
            )))
            .unwrap();
        core.tick();
        let resp = resp_rx.try_pop().unwrap();
        assert_eq!(resp.inner.status, Status::Ok, "open v1: {:?}", resp.inner);

        // 2) Put a cell — establishes memtable state.
        let cells = vec![ArrayPutCell {
            coord: vec![CoordValue::Int64(3)],
            attrs: vec![CellValue::Float64(42.0)],
            surrogate: nodedb_types::Surrogate::ZERO,
            system_from_ms: 0,
            valid_from_ms: 0,
            valid_until_ms: i64::MAX,
        }];
        let cells_bytes = zerompk::to_msgpack_vec(&cells).unwrap();
        req_tx
            .try_push(BridgeRequest::unfloored(make_request(
                PhysicalPlan::Array(ArrayOp::Put {
                    array_id: aid.clone(),
                    cells_msgpack: cells_bytes,
                    wal_lsn: 7,
                    provenance: None,
                }),
                2,
            )))
            .unwrap();
        core.tick();
        let resp = resp_rx.try_pop().unwrap();
        assert_eq!(resp.inner.status, Status::Ok, "put v1: {:?}", resp.inner);

        // 3) DropArray — releases per-core store + on-disk segment dir.
        req_tx
            .try_push(BridgeRequest::unfloored(make_request(
                PhysicalPlan::Array(ArrayOp::DropArray {
                    array_id: aid.clone(),
                }),
                3,
            )))
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

        // Finalization purges the reversible tombstone before recreation.
        req_tx
            .try_push(BridgeRequest::unfloored(make_request(
                PhysicalPlan::Array(ArrayOp::PurgeArrayDrop {
                    array_id: aid.clone(),
                }),
                4,
            )))
            .unwrap();
        core.tick();
        let resp = resp_rx.try_pop().unwrap();
        assert_eq!(resp.inner.status, Status::Ok, "purge: {:?}", resp.inner);

        // 5) Re-open with a DIFFERENT schema_hash. Without the drop, this
        //    would fail with `SchemaMismatch`. The finalized post-drop state
        //    must accept the new hash.
        req_tx
            .try_push(BridgeRequest::unfloored(make_request(
                PhysicalPlan::Array(ArrayOp::OpenArray {
                    array_id: aid.clone(),
                    schema_msgpack: v1_bytes,
                    schema_hash: 0xBBBB,
                    prefix_bits: 8,
                    audit_retain_ms: None,
                    minimum_audit_retain_ms: None,
                }),
                5,
            )))
            .unwrap();
        core.tick();
        let resp = resp_rx.try_pop().unwrap();
        assert_eq!(
            resp.inner.status,
            Status::Ok,
            "re-open after finalized drop: {:?}",
            resp.inner
        );
    }
}
