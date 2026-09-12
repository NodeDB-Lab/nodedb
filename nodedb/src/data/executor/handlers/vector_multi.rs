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
                ErrorCode::RejectedConstraint {
                    detail: String::new(),
                    constraint: "multi-vector count and dim must be > 0".into(),
                },
            );
        }
        if vectors_flat.len() != count * dim {
            return self.response_error(
                task,
                ErrorCode::RejectedConstraint {
                    detail: String::new(),
                    constraint: format!(
                        "data length mismatch: expected {} ({}×{}), got {}",
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

        // Validate dimension compatibility before taking mutable reference.
        if let Some(existing) = self.vector_collections.get(&index_key)
            && existing.dim() != dim
        {
            return self.response_error(
                task,
                ErrorCode::RejectedConstraint {
                    detail: String::new(),
                    constraint: format!(
                        "dimension mismatch: index has {}, got {dim}",
                        existing.dim()
                    ),
                },
            );
        }

        // Get or create the vector collection.
        let core_id = self.core_id;
        let params = self
            .vector_params
            .get(&index_key)
            .cloned()
            .unwrap_or_default();
        let coll = self
            .vector_collections
            .entry(index_key.clone())
            .or_insert_with(|| {
                debug!(
                    core = core_id,
                    dim, "creating vector collection for multi-vector"
                );
                crate::engine::vector::collection::VectorCollection::new(dim, params)
            });

        // Build vector slices from flat data.
        let vector_slices: Vec<&[f32]> = (0..count)
            .map(|i| &vectors_flat[i * dim..(i + 1) * dim])
            .collect();

        // Delete old multi-vector entries for this doc if they exist (upsert).
        coll.delete_multi_vector(document_surrogate);

        // Insert all vectors with shared surrogate.
        let ids = coll.insert_multi_vector(&vector_slices, document_surrogate);
        // Advance the checkpoint watermark to this write's WAL LSN so a later
        // checkpoint records these nodes as absorbed; startup replay then skips
        // the straddling WAL record instead of appending duplicate HNSW nodes.
        // `None` (unassigned LSN) leaves the watermark untouched.
        if let Some(lsn) = task.wal_lsn() {
            coll.note_checkpoint_lsn(lsn.as_u64());
        }

        // Auto-seal if needed.
        let seal_key = CoreLoop::vector_build_key(&index_key);
        if coll.needs_seal()
            && let Some(req) = coll.seal(&seal_key)
            && let Some(tx) = &self.build_tx
            && let Err(e) = tx.send(req)
        {
            warn!(core = self.core_id, error = %e, "failed to send HNSW build after multi-vector insert");
        }

        self.checkpoint_coordinator.mark_dirty("vector", ids.len());
        // Record this write's version keyed by the shared document surrogate
        // so cross-shard OCC read-set validation (predicate reads always
        // record the collection floor) sees this insert.
        self.note_surrogate_write_lsn(task, tid, collection, document_surrogate.as_u32());

        match super::super::response_codec::encode_count("inserted_vectors", ids.len()) {
            Ok(bytes) => self.response_with_payload(task, bytes),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Delete all vectors for a multi-vector document.
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
            return self.response_error(task, ErrorCode::NotFound);
        };

        let deleted = coll.delete_multi_vector(document_surrogate);
        // Advance the watermark to this delete's WAL LSN (single per-collection
        // value covering inserts and deletes), so a checkpoint records the
        // removal as absorbed and replay does not re-run it below the mark.
        if let Some(lsn) = task.wal_lsn() {
            coll.note_checkpoint_lsn(lsn.as_u64());
        }
        if deleted > 0 {
            self.checkpoint_coordinator.mark_dirty("vector", deleted);
            // Record this write's version keyed by the shared document
            // surrogate so cross-shard OCC read-set validation sees this
            // delete.
            self.note_surrogate_write_lsn(task, tid, collection, document_surrogate.as_u32());
            self.response_ok(task)
        } else {
            self.response_error(task, ErrorCode::NotFound)
        }
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
                    ErrorCode::RejectedConstraint {
                        detail: String::new(),
                        constraint: format!(
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
            return self.response_with_payload(task, b"[]".to_vec());
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

        let candidates = coll.search(query_vector, over_fetch, ef);

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
                self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                )
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

    /// A task carrying `wal_lsn` so `note_surrogate_write_lsn` (gated on
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
            db: DatabaseId::DEFAULT,
            tenant: TenantId::new(1),
            collection: Box::from("chunks"),
            key: KeyRepr::Surrogate(document_surrogate.as_u32()),
        };
        assert_eq!(
            h.core.write_index.key_write_lsn(&key),
            Some(Lsn::new(31)),
            "multi-vector insert must populate the per-key (surrogate) write-version index"
        );

        let coll_key = CollKey {
            db: DatabaseId::DEFAULT,
            tenant: TenantId::new(1),
            collection: Box::from("chunks"),
        };
        assert_eq!(
            h.core.write_index.collection_write_lsn(&coll_key),
            Some(Lsn::new(31)),
            "multi-vector insert must advance the collection write-version floor"
        );
    }
}
