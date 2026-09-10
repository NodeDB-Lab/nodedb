// SPDX-License-Identifier: BUSL-1.1

//! Batch vector insert and node-id–based vector delete handlers.
//!
//! Extracted from `vector.rs` to keep file sizes within the 500-line limit.

use nodedb_types::Surrogate;
use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::vector_upsert::decode_payload_lowercased;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::surrogate_to_doc_id;
use crate::types::TenantId;
use nodedb_types::DatabaseId;

impl CoreLoop {
    /// Execute batch vector insert (always to the default/unnamed field).
    pub(in crate::data::executor) fn execute_vector_batch_insert(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        vectors: &[Vec<f32>],
        dim: usize,
        surrogates: &[Surrogate],
    ) -> Response {
        debug!(core = self.core_id, %collection, dim, count = vectors.len(), "vector batch insert");
        let database_id = task.request.database_id.as_u64();
        let index_key = CoreLoop::vector_index_key(database_id, tid, collection, "");
        match self.get_or_create_vector_index(database_id, tid, collection, dim, "") {
            Ok(collection_ref) => {
                for (i, vector) in vectors.iter().enumerate() {
                    if vector.len() != dim {
                        return self.response_error(
                            task,
                            ErrorCode::RejectedConstraint {
                                detail: String::new(),
                                constraint: format!(
                                    "dimension mismatch in batch: expected {dim}, got {}",
                                    vector.len()
                                ),
                            },
                        );
                    }
                    let s = surrogates.get(i).copied().unwrap_or(Surrogate::ZERO);
                    collection_ref.insert_with_surrogate(vector.clone(), s);
                }
                // Advance the checkpoint watermark so a later vector checkpoint
                // records these writes as absorbed; startup replay then skips the
                // straddling WAL records instead of appending duplicate nodes.
                if let Some(lsn) = task.wal_lsn() {
                    collection_ref.note_checkpoint_lsn(lsn.as_u64());
                }
                let seal_key = CoreLoop::vector_build_key(&index_key);
                if collection_ref.needs_seal()
                    && let Some(req) = collection_ref.seal(&seal_key)
                    && let Some(tx) = &self.build_tx
                    && let Err(e) = tx.send(req)
                {
                    warn!(core = self.core_id, error = %e, "failed to send HNSW build request");
                }
                self.checkpoint_coordinator
                    .mark_dirty("vector", vectors.len());
                // Record this write's version so cross-shard OCC read-set
                // validation sees this batch. Per-surrogate when the batch
                // carries a bound surrogate (a superset of the collection
                // floor); floor-only when headless (no surrogates at all, or
                // none of them bound to a real identity).
                let mut any_surrogate_recorded = false;
                for s in surrogates {
                    if *s != Surrogate::ZERO {
                        self.note_surrogate_write_lsn(task, tid, collection, s.as_u32());
                        any_surrogate_recorded = true;
                    }
                }
                if !any_surrogate_recorded {
                    self.note_collection_write_lsn(task, collection);
                }
                match super::super::response_codec::encode_count("inserted", vectors.len()) {
                    Ok(bytes) => self.response_with_payload(task, bytes),
                    Err(e) => self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
                }
            }
            Err(err) => self.response_error(task, err),
        }
    }

    pub(in crate::data::executor) fn execute_vector_delete(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        vector_id: u32,
    ) -> Response {
        debug!(core = self.core_id, %collection, vector_id, "vector delete");
        // Resolve the actual index key. Legacy `CREATE VECTOR INDEX` uses
        // an empty field segment; vector-primary collections use
        // `"{collection}:{field}"`. Try the legacy key first, then scan
        // for any field-suffixed key under the same (tenant, collection).
        let database_id = task.request.database_id.as_u64();
        let tenant = TenantId::new(tid);
        let db = DatabaseId::new(database_id);
        let plain_key = (db, tenant, collection.to_string());
        let prefix = format!("{collection}:");
        let resolved_key = if self.vector_collections.contains_key(&plain_key) {
            Some(plain_key)
        } else {
            self.vector_collections
                .keys()
                .find(|(d, t, c)| *d == db && *t == tenant && c.starts_with(&prefix))
                .cloned()
        };
        let Some(index_key) = resolved_key else {
            return self.response_error(task, ErrorCode::NotFound);
        };

        // Capture the surrogate before deletion so we can fetch the
        // payload row from the sparse store and update the bitmap. The
        // bitmap stores node-id -> field-value membership; without the
        // original field values we cannot remove the entries cleanly.
        //
        // Asymmetric with the insert path (`vector_upsert`): insert
        // atomically rolls back the HNSW node if the sparse write fails,
        // because a phantom node would be returned by future searches.
        // Delete is best-effort cleanup — if the sparse read or decode
        // fails we still drop the HNSW node and skip bitmap cleanup.
        // Phantom bitmap entries are safe (the bitmap is filtered against
        // live node ids on read), so leaving them is preferable to
        // aborting the delete and leaking the vector.
        let surrogate_opt = self
            .vector_collections
            .get(&index_key)
            .and_then(|c| c.get_surrogate(vector_id));

        if let Some(surrogate) = surrogate_opt {
            let row_key = surrogate_to_doc_id(surrogate);
            let fields =
                match self
                    .sparse
                    .get(task.request.database_id.as_u64(), tid, collection, &row_key)
                {
                    Ok(Some(bytes)) => decode_payload_lowercased(&bytes).ok(),
                    _ => None,
                };
            if let Some(fields) = fields
                && let Some(coll) = self.vector_collections.get_mut(&index_key)
            {
                coll.payload.delete_row(vector_id, &fields);
            }
        }

        let Some(collection_ref) = self.vector_collections.get_mut(&index_key) else {
            return self.response_error(task, ErrorCode::NotFound);
        };
        if collection_ref.delete(vector_id) {
            self.checkpoint_coordinator.mark_dirty("vector", 1);
            // `vector_id` is the internal HNSW node id, not the cross-engine
            // surrogate — recording it as a `KeyRepr::Surrogate` would be a
            // wrong identity in a different key space. Floor-only: a
            // predicate reader validates against the collection floor.
            self.note_collection_write_lsn(task, collection);
            self.response_ok(task)
        } else {
            self.response_error(task, ErrorCode::NotFound)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::{
        Admission, ExemptReason, PhysicalPlan, Priority, Request, Status,
    };
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::core_loop::write_index::{CollKey, KeyRepr, WriteKey};
    use crate::types::{Lsn, ReadConsistency, RequestId, TraceId, VShardId};
    use nodedb_bridge::buffer::RingBuffer;
    use nodedb_physical::physical_plan::VectorOp;
    use std::time::{Duration, Instant};

    struct CoreHarness {
        core: CoreLoop,
        _req_tx: nodedb_bridge::buffer::Producer<crate::bridge::dispatch::BridgeRequest>,
        _resp_rx: nodedb_bridge::buffer::Consumer<crate::bridge::dispatch::BridgeResponse>,
        _dir: tempfile::TempDir,
    }

    fn make_core() -> CoreHarness {
        use crate::bridge::dispatch::{BridgeRequest, BridgeResponse};

        let dir = tempfile::tempdir().expect("tempdir");
        let (req_tx, req_rx) = RingBuffer::channel::<BridgeRequest>(64);
        let (resp_tx, resp_rx) = RingBuffer::channel::<BridgeResponse>(64);
        let core = CoreLoop::open(
            0,
            req_rx,
            resp_tx,
            dir.path(),
            std::sync::Arc::new(nodedb_types::OrdinalClock::new()),
            crate::data::executor::core_loop::test_governor(),
        )
        .expect("open core");
        CoreHarness {
            core,
            _req_tx: req_tx,
            _resp_rx: resp_rx,
            _dir: dir,
        }
    }

    /// A task carrying `wal_lsn` so `note_collection_write_lsn` (gated on
    /// `task.wal_lsn().is_some()`) actually fires, mirroring a live write
    /// dispatched with an allocated WAL LSN.
    fn make_task_with_lsn(lsn: u64) -> ExecutionTask {
        ExecutionTask::new(Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            plan: PhysicalPlan::Vector(VectorOp::Search {
                collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
                query_vector: Vec::new(),
                top_k: 0,
                ef_search: 0,
                metric: nodedb_types::vector_distance::DistanceMetric::L2,
                filter_bitmap: None,
                field_name: String::new(),
                rls_filters: Vec::new(),
                inline_prefilter_plan: None,
                ann_options: Default::default(),
                skip_payload_fetch: false,
                payload_filters: Vec::new(),
            }),
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
            wal_lsn: Some(Lsn::new(lsn)),
            resolved_now_ms: None,
            admission: Admission::Exempt(ExemptReason::Read),
        })
    }

    #[test]
    fn vector_delete_populates_collection_floor_only_not_vector_id_as_surrogate() {
        let mut h = make_core();
        // Insert directly against the engine (bypassing the insert handler)
        // so the only write-version record under test is the delete's.
        let surrogate = Surrogate::new(500);
        let vector_id = {
            let coll = h
                .core
                .get_or_create_vector_index(0, 1, "docs", 2, "")
                .expect("create index");
            coll.insert_with_surrogate(vec![1.0, 2.0], surrogate)
        };
        // The internal node id must differ from the surrogate for this test
        // to actually distinguish the two key spaces.
        assert_ne!(vector_id, surrogate.as_u32());

        let task = make_task_with_lsn(51);
        let response = h.core.execute_vector_delete(&task, 1, "docs", vector_id);
        assert_eq!(response.status, Status::Ok);

        let coll_key = CollKey {
            db: DatabaseId::DEFAULT,
            tenant: TenantId::new(1),
            collection: Box::from("docs"),
        };
        assert_eq!(
            h.core.write_index.collection_write_lsn(&coll_key),
            Some(Lsn::new(51)),
            "vector delete must advance the collection write-version floor"
        );

        // BRIGHT-LINE: `vector_id` (the internal HNSW node id) must never be
        // recorded as a `KeyRepr::Surrogate` — that is a different key space
        // from the cross-engine surrogate.
        let would_be_key = WriteKey {
            db: DatabaseId::DEFAULT,
            tenant: TenantId::new(1),
            collection: Box::from("docs"),
            key: KeyRepr::Surrogate(vector_id),
        };
        assert_eq!(
            h.core.write_index.key_write_lsn(&would_be_key),
            None,
            "vector delete must not record vector_id as a surrogate key"
        );
    }
}
