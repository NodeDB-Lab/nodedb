// SPDX-License-Identifier: BUSL-1.1

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
use crate::engine::document::store::StorageKey;

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
                let hex = StorageKey::for_surrogate(*sur).to_string();
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
    use nodedb_types::{QualifiedCollection, Surrogate};

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

    #[test]
    fn vector_search_with_array_surrogate_prefilter() {
        use nodedb_physical::physical_plan::VectorOp;
        use nodedb_types::vector_distance::DistanceMetric;

        // 2D array tiling chr × pos, cells bound to surrogates 1..=10.
        // Row "chr=0" carries surrogates 1..=5; row "chr=1" carries 6..=10.
        let mut h = Harness::new();
        let s = schema_2d_f64("genome");
        let aid = ArrayId::new(TenantId::new(1), "genome");
        h.open(&aid, &s, 0xC1);
        let mut cells = Vec::new();
        for i in 0u32..10 {
            let chr = (i / 5) as i64;
            let pos = (i % 5) as i64;
            cells.push(cell_sur(chr, pos, i as f64, i + 1));
        }
        h.put(&aid, cells, 1);
        h.flush(&aid);

        // Insert 10 vectors with matching surrogates 1..=10.
        for i in 0u32..10 {
            let r = h.send_plan(PhysicalPlan::Vector(VectorOp::Insert {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "embeddings"),
                vector: vec![(i + 1) as f32, 0.0, 0.0],
                dim: 3,
                field_name: String::new(),
                surrogate: Surrogate(i + 1),
                pk_bytes: None,
                provenance: None,
            }));
            assert_eq!(r.status, Status::Ok, "vector insert {i} failed: {r:?}");
        }

        // Slice: chr=0 only (matches surrogates 1..=5).
        let slice = ArraySlice::new(vec![
            Some(DimRange::new(DomainBound::Int64(0), DomainBound::Int64(0))),
            None,
        ]);
        let slice_msgpack = zerompk::to_msgpack_vec(&slice).unwrap();

        // Vector search with inline prefilter sub-plan.
        let r = h.send_plan(PhysicalPlan::Vector(VectorOp::Search {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "embeddings"),
            query_vector: vec![5.5f32, 0.0, 0.0],
            top_k: 10,
            ef_search: 0,
            filter_bitmap: None,
            field_name: String::new(),
            rls_filters: Vec::new(),
            inline_prefilter_plan: Some(Box::new(PhysicalPlan::Array(
                ArrayOp::SurrogateBitmapScan {
                    array_id: aid.clone(),
                    slice_msgpack,
                },
            ))),
            ann_options: Default::default(),
            skip_payload_fetch: false,
            payload_filters: Vec::new(),
            metric: DistanceMetric::L2,
        }));
        assert_eq!(r.status, Status::Ok, "vector+prefilter failed: {r:?}");

        // Result hits MUST all carry surrogate ids in 1..=5.
        let json =
            nodedb_types::msgpack_to_json_string(r.payload.as_ref()).expect("hits msgpack→json");
        let hits: Vec<serde_json::Value> = serde_json::from_str(&json).expect("hits json parse");
        assert!(!hits.is_empty(), "expected at least one hit, got none");
        for hit in &hits {
            let id = hit["id"].as_u64().expect("hit.id present") as u32;
            assert!(
                (1..=5).contains(&id),
                "hit surrogate {id} outside slice prefilter range 1..=5"
            );
        }
    }
}
