// SPDX-License-Identifier: BUSL-1.1

//! Sparse vector index handlers: insert, search, delete.
//!
//! Operates on `SparseInvertedIndex` instances owned by the CoreLoop.

use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::vector::sparse::SparseInvertedIndex;

/// Parameters for [`CoreLoop::execute_sparse_search`].
pub(in crate::data::executor) struct SparseSearchParams<'a> {
    pub collection: &'a str,
    pub field_name: &'a str,
    pub query_entries: &'a [(u32, f32)],
    pub top_k: usize,
    /// Serialized `Vec<ScanFilter>`: the statement's residual `WHERE`.
    /// Empty when none applies.
    pub filters: &'a [u8],
    /// The system time the search reads at.
    pub system_time: nodedb_types::SystemTimeScope,
}

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
                    ErrorCode::DataException {
                        detail: e.to_string(),
                    },
                );
            }
        };

        let database_id = task.request.database_id.as_u64();
        // The document's live version ends at this write's system time, so a
        // read at an earlier cut still scores it.
        let ordinal = self.bitemporal_ordinal();
        let index = self.get_or_create_sparse_index(database_id, tid, collection, field_name);
        index.insert_at(doc_id, &sv, ordinal);
        self.checkpoint_coordinator.mark_dirty("vector", 1);
        // Sparse vectors are keyed by `doc_id: String` — no cross-engine
        // surrogate — so the write is noted with no row key.
        if let Err(e) = self.note_collection_write(task, collection) {
            return self.response_error(task, e);
        }
        self.response_ok(task)
    }

    /// Search the sparse inverted index via dot-product scoring.
    ///
    /// `filters` is the statement's residual `WHERE` (serialized
    /// `Vec<ScanFilter>`). With one, the candidate window widens, as a dense
    /// vector search's does. Each hit carries its body for the filter and
    /// the projection. The Control Plane applies the filter and the `top_k`
    /// cut.
    pub(in crate::data::executor) fn execute_sparse_search(
        &self,
        task: &ExecutionTask,
        tid: u64,
        params: SparseSearchParams<'_>,
    ) -> Response {
        let SparseSearchParams {
            collection,
            field_name,
            query_entries,
            top_k,
            filters,
            system_time,
        } = params;
        // A read at a cut scores the version of each document visible there.
        // A transaction snapshot also merges the block's staged writes.
        let cut = match super::search_cut::SearchCut::of(system_time) {
            Ok(cut) => cut,
            Err(e) => return self.response_error(task, e),
        };
        debug!(
            core = self.core_id,
            %collection,
            %field_name,
            query_nnz = query_entries.len(),
            top_k,
            filtered = !filters.is_empty(),
            "sparse search"
        );

        let query = match nodedb_types::SparseVector::from_entries(query_entries.to_vec()) {
            Ok(sv) => sv,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::DataException {
                        detail: e.to_string(),
                    },
                );
            }
        };

        // The Control Plane encoded these bytes. Bytes that do not decode
        // fail the search: ranking without the filter cannot size the window.
        let residual: Vec<nodedb_query::scan_filter::ScanFilter> = if filters.is_empty() {
            Vec::new()
        } else {
            match zerompk::from_msgpack(filters) {
                Ok(filters) => filters,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("sparse search residual filter decode: {e}"),
                        },
                    );
                }
            }
        };

        let database_id = task.request.database_id.as_u64();
        let key = Self::sparse_index_key(database_id, tid, collection, field_name);
        // Each hit's `id` is its surrogate's storage key and `doc_id` stays
        // unset, as in the dense vector search. The Control-Plane translator
        // resolves the surrogate to the user PK. An absent index ranks
        // nothing, and the staged rows still merge below.
        let mut hits = match self.sparse_vector_indexes.get(&key) {
            None => Vec::new(),
            Some(index) => match super::vector_sparse_window::rank_sparse_window(
                top_k,
                &residual,
                &self.crs_context(),
                |k| match cut.at {
                    Some(at) => crate::engine::vector::sparse::search::dot_product_topk_at(
                        index, &query, k, at,
                    ),
                    None => {
                        crate::engine::vector::sparse::search::dot_product_topk(index, &query, k)
                    }
                },
                |hit| self.attach_body(database_id, tid, collection, true, cut.at, hit),
            ) {
                Ok(hits) => hits,
                Err(e) => return self.response_error(task, e),
            },
        };

        // A historical read sees committed rows only, never staged ones. With
        // a residual filter the Control Plane makes the top-k cut, so every
        // ranked and staged hit travels.
        if let Some(txn_id) = task.request.txn_id.filter(|_| cut.merge_overlay) {
            let truncate_to = if residual.is_empty() {
                top_k
            } else {
                usize::MAX
            };
            if let Err(e) = self.merge_sparse_overlay_into_search(
                super::transaction::overlay::SparseMergeParams {
                    txn_id,
                    database_id: task.request.database_id,
                    tid: crate::types::TenantId::new(tid),
                    collection,
                    field_name,
                    query: &query,
                    truncate_to,
                },
                &mut hits,
            ) {
                return self.response_error(task, e);
            }
        }

        match super::super::response_codec::encode(&hits) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => {
                warn!(core = self.core_id, error = %e, "sparse search encode failed");
                self.response_error(task, ErrorCode::from(e))
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
        let ordinal = self.bitemporal_ordinal();
        let Some(index) = self.sparse_vector_indexes.get_mut(&key) else {
            return self.response_error(task, ErrorCode::NotFound);
        };

        if index.delete_at(doc_id, ordinal) {
            self.checkpoint_coordinator.mark_dirty("vector", 1);
            // Sparse vectors are keyed by `doc_id: String` — no cross-engine
            // surrogate — so the write is noted with no row key.
            if let Err(e) = self.note_collection_write(task, collection) {
                return self.response_error(task, e);
            }
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

    /// A task carrying `wal_lsn` so `note_collection_write` (gated on
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
                system_time: nodedb_types::SystemTimeScope::Current,
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
            commit_hlc: None,
            system_ordinal: None,
            admission: Admission::Exempt(ExemptReason::Read),
        })
    }

    #[test]
    fn sparse_insert_notes_its_write() {
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

        assert_eq!(
            h.core.watermark,
            Lsn::new(41),
            "sparse insert must note its write"
        );
    }
}
