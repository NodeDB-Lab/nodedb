// SPDX-License-Identifier: BUSL-1.1

//! Multi-vector document handlers: insert N vectors per doc, delete all,
//! and aggregated scoring search (MaxSim / AvgSim / SumSim).

use std::collections::HashMap;

use nodedb_types::Surrogate;
use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

/// Parameters for [`CoreLoop::execute_multi_vector_insert`].
pub(in crate::data::executor) struct MultiVectorInsertParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub collection: &'a str,
    pub field_name: &'a str,
    pub document_surrogate: Surrogate,
    pub vectors_flat: &'a [f32],
    pub count: usize,
    pub dim: usize,
}

/// Parameters for [`CoreLoop::execute_multi_vector_score_search`].
pub(in crate::data::executor) struct MultiVectorScoreSearchParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub collection: &'a str,
    pub field_name: &'a str,
    pub query_vector: &'a [f32],
    pub top_k: usize,
    pub ef_search: usize,
    pub mode_str: &'a str,
}

impl CoreLoop {
    /// Insert multiple vectors for a single document into the HNSW index.
    ///
    /// All vectors share the same `document_surrogate` in `surrogate_map`
    /// and are tracked in `multi_doc_map` for bulk deletion.
    pub(in crate::data::executor) fn execute_multi_vector_insert(
        &mut self,
        params: MultiVectorInsertParams<'_>,
    ) -> Response {
        let MultiVectorInsertParams {
            task,
            tid,
            collection,
            field_name,
            document_surrogate,
            vectors_flat,
            count,
            dim,
        } = params;
        debug!(
            core = self.core_id,
            %collection, %field_name, doc_surrogate = document_surrogate.as_u32(), count, dim,
            "multi-vector insert"
        );

        if count == 0 || dim == 0 {
            return self.response_error(
                task,
                ErrorCode::DataException {
                    detail: "multi-vector count and dim must be > 0".into(),
                },
            );
        }
        if let Some(refusal) =
            super::unbound_surrogate::refuse_unbound("multi-vector", collection, document_surrogate)
        {
            return self.response_error(task, refusal);
        }
        if vectors_flat.len() != count * dim {
            return self.response_error(
                task,
                ErrorCode::DataException {
                    detail: format!(
                        "multi-vector data length mismatch: expected {} ({}×{}), got {}",
                        count * dim,
                        count,
                        dim,
                        vectors_flat.len()
                    ),
                },
            );
        }

        let database_id = task.request.database_id.as_u64();
        let index_key = CoreLoop::vector_index_key(database_id, tid, collection, field_name);

        // A committed-redo install seals once the whole record landed.
        let defer_seal = self.recording_redo_undo();
        let coll =
            match self.get_or_create_vector_index(database_id, tid, collection, dim, field_name) {
                Ok(coll) => coll,
                Err(err) => return self.response_error(task, err),
            };

        // Build vector slices from flat data.
        let vector_slices: Vec<&[f32]> = (0..count)
            .map(|i| &vectors_flat[i * dim..(i + 1) * dim])
            .collect();

        // Delete old multi-vector entries for this doc if they exist (upsert).
        coll.delete_multi_vector(document_surrogate);

        // Insert all vectors with shared surrogate.
        let ids = match coll.insert_multi_vector(&vector_slices, document_surrogate) {
            Ok(ids) => ids,
            Err(e) => return self.response_error(task, crate::Error::from(e)),
        };

        if !defer_seal {
            self.settle_vector_collection(&index_key);
        }

        self.checkpoint_coordinator.mark_dirty("vector", ids.len());
        // Record this write's version keyed by the shared document surrogate
        // so cross-shard OCC read-set validation (predicate reads always
        // record the collection floor) sees this insert.
        self.note_surrogate_write(task, tid, collection, document_surrogate.as_u32());

        match super::super::response_codec::encode_count("inserted_vectors", ids.len()) {
            Ok(bytes) => self.response_with_payload(task, bytes),
            Err(e) => self.response_error(task, ErrorCode::from(e)),
        }
    }

    /// Delete all vectors for a multi-vector document.
    ///
    /// Idempotent: a document with no vectors, or a field with no index,
    /// answers `Ok` and changes nothing. A re-issue deletes before it
    /// inserts, so the first delete of a document finds nothing.
    pub(in crate::data::executor) fn execute_multi_vector_delete(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        collection: &str,
        field_name: &str,
        document_surrogate: Surrogate,
    ) -> Response {
        debug!(
            core = self.core_id,
            %collection, %field_name, doc_surrogate = document_surrogate.as_u32(),
            "multi-vector delete"
        );

        let database_id = task.request.database_id.as_u64();
        let index_key = CoreLoop::vector_index_key(database_id, tid, collection, field_name);
        let Some(coll) = self.vector_collections.get_mut(&index_key) else {
            return self.response_ok(task);
        };

        let deleted = coll.delete_multi_vector(document_surrogate);
        if deleted > 0 {
            self.checkpoint_coordinator.mark_dirty("vector", deleted);
            // Record this write's version keyed by the shared document
            // surrogate so cross-shard OCC read-set validation sees this
            // delete.
            self.note_surrogate_write(task, tid, collection, document_surrogate.as_u32());
        }
        self.response_ok(task)
    }

    /// Search with multi-vector aggregated scoring.
    ///
    /// 1. Over-fetch from HNSW: top_k × over_fetch_factor candidates
    /// 2. Group candidates by doc_id
    /// 3. For each document, collect all its candidate distances
    /// 4. Aggregate per-document using the specified mode (MaxSim/AvgSim/SumSim)
    /// 5. Sort by aggregated score, dedup, return top-K documents
    pub(in crate::data::executor) fn execute_multi_vector_score_search(
        &self,
        params: MultiVectorScoreSearchParams<'_>,
    ) -> Response {
        let MultiVectorScoreSearchParams {
            task,
            tid,
            collection,
            field_name,
            query_vector,
            top_k,
            ef_search,
            mode_str,
        } = params;
        debug!(
            core = self.core_id,
            %collection, %field_name, top_k, %mode_str,
            "multi-vector score search"
        );

        let mode = match nodedb_types::MultiVectorScoreMode::parse(mode_str) {
            Some(m) => m,
            None => {
                return self.response_error(
                    task,
                    ErrorCode::DataException {
                        detail: format!(
                            "unknown score mode '{mode_str}'; supported: max_sim, avg_sim, sum_sim"
                        ),
                    },
                );
            }
        };

        let database_id = task.request.database_id.as_u64();
        let index_key = CoreLoop::vector_index_key(database_id, tid, collection, field_name);
        let Some(coll) = self.vector_collections.get(&index_key) else {
            return self.response_error(task, ErrorCode::NotFound);
        };

        if coll.is_empty() {
            return super::vector_search::empty_hits_response(self, task);
        }

        // Over-fetch: we need enough candidates so that after grouping by doc_id,
        // we still have top_k distinct documents. Factor of 10 is conservative
        // for typical multi-vector docs with 50-500 tokens.
        let over_fetch = (top_k * 10).clamp(100, 10_000);
        let ef = if ef_search > 0 {
            ef_search.max(over_fetch)
        } else {
            over_fetch.saturating_mul(2).max(64)
        };

        let candidates = match coll.search(query_vector, over_fetch, ef) {
            Ok(candidates) => candidates,
            Err(e) => return self.response_error(task, crate::Error::from(e)),
        };

        // Group by surrogate. For distance metrics where lower = better
        // (L2, cosine) we convert similarity = 1 / (1 + distance) so
        // higher = better. For inner product, distance is already a
        // similarity score. Candidates without a bound surrogate fall
        // back to the local node id wrapped as `Surrogate(local)` so
        // headless inserts still group.
        let mut doc_scores: HashMap<Surrogate, Vec<f32>> = HashMap::new();
        for result in &candidates {
            let key = coll
                .get_surrogate(result.id)
                .unwrap_or_else(|| Surrogate::new(result.id));
            let similarity = 1.0 / (1.0 + result.distance);
            doc_scores.entry(key).or_default().push(similarity);
        }

        let mut scored_docs: Vec<(Surrogate, f32)> = doc_scores
            .iter()
            .map(|(s, scores)| (*s, mode.aggregate(scores)))
            .collect();
        scored_docs.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scored_docs.truncate(top_k);

        // DP emits surrogate as `id`; CP translates to user PK at the response boundary.
        let hits: Vec<super::super::response_codec::VectorSearchHit> = scored_docs
            .iter()
            .map(|(s, score)| super::super::response_codec::VectorSearchHit {
                id: super::hybrid_key::HybridFusionKey::for_surrogate(*s),
                distance: *score,
                doc_id: None,
                body: None,
            })
            .collect();

        match super::super::response_codec::encode(&hits) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => {
                warn!(core = self.core_id, error = %e, "multi-vector search encode failed");
                self.response_error(task, ErrorCode::from(e))
            }
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
    use crate::data::executor::core_loop::write_index::tests::local;
    use crate::data::executor::core_loop::write_index::{CollKey, KeyRepr, WriteKey};
    use crate::types::{DatabaseId, Lsn, ReadConsistency, RequestId, TenantId, TraceId, VShardId};
    use nodedb_bridge::buffer::RingBuffer;
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

    /// A task carrying `wal_lsn` so `note_surrogate_write` (gated on
    /// `task.wal_lsn().is_some()`) actually fires, mirroring a live write
    /// dispatched with an allocated WAL LSN.
    fn make_task_with_lsn(lsn: u64) -> ExecutionTask {
        ExecutionTask::new(Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            plan: PhysicalPlan::Vector(nodedb_physical::physical_plan::VectorOp::Search {
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
            commit_hlc: None,
            entry_version: None,
            admission: Admission::Exempt(ExemptReason::Read),
        })
    }

    #[test]
    fn multi_vector_insert_populates_write_version_index_surrogate_and_floor() {
        let mut h = make_core();
        let task = make_task_with_lsn(31);
        let document_surrogate = Surrogate::new(9);

        let response = h.core.execute_multi_vector_insert(MultiVectorInsertParams {
            task: &task,
            tid: 1,
            collection: "chunks",
            field_name: "emb",
            document_surrogate,
            vectors_flat: &[1.0, 2.0, 3.0, 4.0],
            count: 2,
            dim: 2,
        });
        assert_eq!(response.status, Status::Ok);

        let key = WriteKey {
            vshard: VShardId::new(0),
            db: DatabaseId::DEFAULT,
            tenant: TenantId::new(1),
            collection: Box::from("chunks"),
            key: KeyRepr::Surrogate(document_surrogate.as_u32()),
        };
        assert_eq!(
            h.core.write_index.key_version(&key),
            Some(local(31)),
            "multi-vector insert must populate the per-key (surrogate) write-version index"
        );

        let coll_key = CollKey {
            vshard: VShardId::new(0),
            db: DatabaseId::DEFAULT,
            tenant: TenantId::new(1),
            collection: Box::from("chunks"),
        };
        assert_eq!(
            h.core.write_index.collection_version(&coll_key),
            Some(local(31)),
            "multi-vector insert must advance the collection write version"
        );
    }

    /// The restore re-issue deletes a multi-vector document, then inserts its
    /// full set. The delete answers Ok when nothing exists, and a repeated
    /// delete-then-insert leaves the set once.
    #[test]
    fn delete_then_insert_is_idempotent_and_keeps_every_vector() {
        let mut h = make_core();
        let task = make_task_with_lsn(40);
        let document_surrogate = Surrogate::new(11);
        let vectors = [1.0, 0.0, 0.0, 1.0, 0.5, 0.5];

        let absent =
            h.core
                .execute_multi_vector_delete(&task, 1, "chunks", "emb", document_surrogate);
        assert_eq!(absent.status, Status::Ok, "deleting nothing answers Ok");

        for _ in 0..2 {
            let deleted =
                h.core
                    .execute_multi_vector_delete(&task, 1, "chunks", "emb", document_surrogate);
            assert_eq!(deleted.status, Status::Ok);
            let inserted = h.core.execute_multi_vector_insert(MultiVectorInsertParams {
                task: &task,
                tid: 1,
                collection: "chunks",
                field_name: "emb",
                document_surrogate,
                vectors_flat: &vectors,
                count: 3,
                dim: 2,
            });
            assert_eq!(inserted.status, Status::Ok);
        }

        let index_key = CoreLoop::vector_index_key(0, 1, "chunks", "emb");
        let live = h
            .core
            .vector_collections
            .get(&index_key)
            .map(|coll| coll.live_count());
        assert_eq!(live, Some(3), "the document keeps all three vectors, once");
    }
}
