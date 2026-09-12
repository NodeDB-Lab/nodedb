// SPDX-License-Identifier: BUSL-1.1

//! Sparse vector index handlers: insert, search, delete.
//!
//! Operates on `SparseInvertedIndex` instances owned by the CoreLoop.

use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::vector::sparse::SparseInvertedIndex;

impl CoreLoop {
    /// Get or create a sparse inverted index for a collection/field.
    pub(in crate::data::executor) fn get_or_create_sparse_index(
        &mut self,
        database_id: u64,
        tid: u64,
        collection: &str,
        field_name: &str,
    ) -> &mut SparseInvertedIndex {
        let key = Self::sparse_index_key(database_id, tid, collection, field_name);
        self.sparse_vector_indexes.entry(key).or_default()
    }

    /// Build the tuple key for sparse vector indexes.
    pub(in crate::data::executor) fn sparse_index_key(
        database_id: u64,
        tid: u64,
        collection: &str,
        field_name: &str,
    ) -> (
        nodedb_types::DatabaseId,
        crate::types::TenantId,
        String,
        String,
    ) {
        let field = if field_name.is_empty() {
            "_sparse".to_string()
        } else {
            field_name.to_string()
        };
        (
            nodedb_types::DatabaseId::new(database_id),
            crate::types::TenantId::new(tid),
            collection.to_string(),
            field,
        )
    }

    /// Insert a sparse vector for a document.
    pub(in crate::data::executor) fn execute_sparse_insert(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        field_name: &str,
        doc_id: &str,
        entries: &[(u32, f32)],
    ) -> Response {
        debug!(
            core = self.core_id,
            %collection,
            %field_name,
            %doc_id,
            nnz = entries.len(),
            "sparse insert"
        );

        let sv = match nodedb_types::SparseVector::from_entries(entries.to_vec()) {
            Ok(sv) => sv,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::RejectedConstraint {
                        detail: String::new(),
                        constraint: e.to_string(),
                    },
                );
            }
        };

        let database_id = task.request.database_id.as_u64();
        let index = self.get_or_create_sparse_index(database_id, tid, collection, field_name);
        index.insert(doc_id, &sv);
        self.checkpoint_coordinator.mark_dirty("vector", 1);
        // Sparse vectors are keyed by `doc_id: String` — no cross-engine
        // surrogate — so only the collection floor is recorded.
        self.note_collection_write_lsn(task, collection);
        self.response_ok(task)
    }

    /// Search the sparse inverted index via dot-product scoring.
    pub(in crate::data::executor) fn execute_sparse_search(
        &self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        field_name: &str,
        query_entries: &[(u32, f32)],
        top_k: usize,
    ) -> Response {
        debug!(
            core = self.core_id,
            %collection,
            %field_name,
            query_nnz = query_entries.len(),
            top_k,
            "sparse search"
        );

        let database_id = task.request.database_id.as_u64();
        let key = Self::sparse_index_key(database_id, tid, collection, field_name);
        let Some(index) = self.sparse_vector_indexes.get(&key) else {
            // No index exists — return empty results (not an error).
            return match super::super::response_codec::encode(&Vec::<
                super::super::response_codec::VectorSearchHit,
            >::new())
            {
                Ok(payload) => self.response_with_payload(task, payload),
                Err(e) => self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                ),
            };
        };

        let query = match nodedb_types::SparseVector::from_entries(query_entries.to_vec()) {
            Ok(sv) => sv,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::RejectedConstraint {
                        detail: String::new(),
                        constraint: e.to_string(),
                    },
                );
            }
        };

        let results = crate::engine::vector::sparse::search::dot_product_topk(index, &query, top_k);

        // Convert to VectorSearchHit. The sparse index keys documents by their
        // hex surrogate `doc_id`; emit that surrogate's storage key as the hit
        // `id` and leave `doc_id` unset, exactly like the dense vector search —
        // the Control-Plane response translator (`translate_vector_search_payload`)
        // then resolves the surrogate back to the user PK for the projection.
        // Fall back to `Headless(internal_id)` only if `doc_id` is somehow not
        // a hex surrogate.
        let hits: Vec<super::super::response_codec::VectorSearchHit> = results
            .iter()
            .map(|r| {
                let id = r
                    .doc_id
                    .as_deref()
                    .and_then(nodedb_types::StorageKey::parse)
                    .map(super::hybrid_key::HybridFusionKey::Bound)
                    .unwrap_or(super::hybrid_key::HybridFusionKey::Headless(r.internal_id));
                super::super::response_codec::VectorSearchHit {
                    id,
                    distance: r.score,
                    doc_id: None,
                    body: None,
                }
            })
            .collect();

        match super::super::response_codec::encode(&hits) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => {
                warn!(core = self.core_id, error = %e, "sparse search encode failed");
                self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                )
            }
        }
    }

    /// Delete a document from the sparse inverted index.
    pub(in crate::data::executor) fn execute_sparse_delete(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        field_name: &str,
        doc_id: &str,
    ) -> Response {
        debug!(core = self.core_id, %collection, %field_name, %doc_id, "sparse delete");

        let database_id = task.request.database_id.as_u64();
        let key = Self::sparse_index_key(database_id, tid, collection, field_name);
        let Some(index) = self.sparse_vector_indexes.get_mut(&key) else {
            return self.response_error(task, ErrorCode::NotFound);
        };

        if index.delete(doc_id) {
            self.checkpoint_coordinator.mark_dirty("vector", 1);
            // Sparse vectors are keyed by `doc_id: String` — no cross-engine
            // surrogate — so only the collection floor is recorded.
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
    use crate::types::{DatabaseId, Lsn, ReadConsistency, RequestId, TenantId, TraceId, VShardId};
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
    fn sparse_insert_populates_collection_floor_only_no_surrogate_entry() {
        let mut h = make_core();
        let task = make_task_with_lsn(41);

        let response = h.core.execute_sparse_insert(
            &task,
            1,
            "sparse_docs",
            "vec",
            "doc-1",
            &[(3, 0.5), (7, 1.5)],
        );
        assert_eq!(response.status, Status::Ok);

        let coll_key = CollKey {
            db: DatabaseId::DEFAULT,
            tenant: TenantId::new(1),
            collection: Box::from("sparse_docs"),
        };
        assert_eq!(
            h.core.write_index.collection_write_lsn(&coll_key),
            Some(Lsn::new(41)),
            "sparse insert must advance the collection write-version floor"
        );

        // Sparse vectors carry no cross-engine surrogate: no would-be
        // per-key `Surrogate` entry must exist for this write.
        let would_be_key = WriteKey {
            db: DatabaseId::DEFAULT,
            tenant: TenantId::new(1),
            collection: Box::from("sparse_docs"),
            key: KeyRepr::Surrogate(0),
        };
        assert_eq!(
            h.core.write_index.key_write_lsn(&would_be_key),
            None,
            "sparse insert must not record a per-key surrogate entry"
        );
    }
}
