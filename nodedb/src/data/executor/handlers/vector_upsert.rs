// SPDX-License-Identifier: BUSL-1.1

//! Direct upsert handler for vector-primary collections.
//!
//! Bypasses MessagePack document encoding. The caller (Control Plane) has
//! already serialised only the payload-indexed fields into `payload` bytes;
//! this handler inserts the vector into HNSW and updates the bitmap indexes.
//!
//! **Ordering invariant** (enforced below):
//!   1. Validate dimension.
//!   2. Decode `payload` bytes → `HashMap<String, Value>`.
//!   3. Insert vector into HNSW (surrogate bound).
//!   4. Update payload bitmap indexes.
//!
//! If step 3 fails, step 4 is not reached — no partial state.
//! If step 4 panics (should not happen — pure in-memory), the handler
//! attempts to delete the just-inserted HNSW node and returns an error.

use std::collections::HashMap;

use nodedb_types::{Surrogate, Value};
use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

/// Decode MessagePack payload bytes into `HashMap<String, Value>` and
/// lower-case all field names so bitmap inserts agree with SELECT
/// pre-filters regardless of caller capitalisation.
pub(in crate::data::executor) fn decode_payload_lowercased(
    bytes: &[u8],
) -> Result<HashMap<String, Value>, zerompk::Error> {
    zerompk::from_msgpack::<HashMap<String, Value>>(bytes).map(|m| {
        m.into_iter()
            .map(|(k, v)| (k.to_ascii_lowercase(), v))
            .collect()
    })
}

/// Parameters for [`CoreLoop::execute_vector_direct_upsert`].
pub(in crate::data::executor) struct VectorDirectUpsertParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub collection: &'a str,
    pub field: &'a str,
    pub surrogate: Surrogate,
    pub vector: &'a [f32],
    pub payload: &'a [u8],
    pub quantization: nodedb_types::VectorQuantization,
    pub storage_dtype: nodedb_types::VectorStorageDtype,
    pub payload_indexes: &'a [(String, nodedb_types::PayloadIndexKind)],
    /// Projection for a `RETURNING` clause, when the statement carried one.
    /// WAL replay and replication build this op with no client session behind
    /// them, so they leave it `None`.
    pub returning: Option<&'a nodedb_physical::physical_plan::ReturningSpec>,
    /// Compiled row-level-security READ predicate gating the row `returning`
    /// emits — a separate gate from the write admission the Control Plane
    /// already applied to `payload`.
    pub rls_filters: &'a [u8],
}

impl CoreLoop {
    /// Handle `VectorOp::DirectUpsert`.
    pub(in crate::data::executor) fn execute_vector_direct_upsert(
        &mut self,
        params: VectorDirectUpsertParams<'_>,
    ) -> Response {
        let VectorDirectUpsertParams {
            task,
            tid,
            collection,
            field,
            surrogate,
            vector,
            payload,
            quantization,
            storage_dtype,
            payload_indexes,
            returning,
            rls_filters,
        } = params;
        debug!(
            core = self.core_id,
            %collection,
            %field,
            dim = vector.len(),
            "vector direct upsert"
        );

        let dim = vector.len();
        let database_id = task.request.database_id.as_u64();
        let index_key = CoreLoop::vector_index_key(database_id, tid, collection, field);

        // Step 1: validate dimension and storage dtype against any existing
        // index. The dtype is a creation-time choice baked into segment
        // layout — changing it after the fact would invalidate every node
        // already in the graph.
        if let Some(existing) = self.vector_collections.get(&index_key) {
            if existing.dim() != dim {
                return self.response_error(
                    task,
                    ErrorCode::RejectedConstraint {
                        detail: String::new(),
                        constraint: format!(
                            "vector dimension mismatch: index has {}, got {dim}",
                            existing.dim()
                        ),
                    },
                );
            }
            let existing_dtype = existing.params().dtype;
            if existing_dtype != storage_dtype {
                return self.response_error(
                    task,
                    ErrorCode::RejectedConstraint {
                        detail: String::new(),
                        constraint: format!(
                            "vector storage_dtype mismatch: index has {existing_dtype}, got {storage_dtype}; \
                             dtype is immutable after collection creation"
                        ),
                    },
                );
            }
        }

        // Step 2: decode payload bytes.
        // Empty slice → empty map (collection has no payload indexes).
        // Field names are lower-cased so the bitmap insert and the SELECT
        // pre-filter agree regardless of how the SQL caller capitalized them.
        let payload_fields: HashMap<String, Value> = if payload.is_empty() {
            HashMap::new()
        } else {
            match decode_payload_lowercased(payload) {
                Ok(m) => m,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("payload decode error: {e}"),
                        },
                    );
                }
            }
        };

        // Step 3: insert into HNSW (with surrogate binding).
        // When a new vector-primary collection is created here, request a
        // dedicated jemalloc arena from the registry so its allocations are
        // isolated from document-engine workloads. Resolve the arena up front
        // to avoid borrowing `self` immutably while also holding a mutable
        // borrow on `self.vector_collections` via `coll`.
        let is_new_collection = !self.vector_collections.contains_key(&index_key);
        let core_id = self.core_id;

        // For a brand-new vector-primary collection, seed `vector_params`
        // with the requested storage dtype so `get_or_create_vector_index`
        // constructs the HNSW graph with the right `NodeStorage` variant
        // (F32 / F16 / BF16). If `set_vector_params` ran first (CREATE
        // COLLECTION path), the existing params are preserved and we only
        // override the dtype.
        if is_new_collection {
            let params = self.vector_params.entry(index_key.clone()).or_default();
            params.dtype = storage_dtype;
        }

        let arena_handle = if is_new_collection {
            self.collection_arena_registry.clone().and_then(|reg| {
                match reg.get_or_create(tid, collection) {
                    Ok(handle) => Some(handle),
                    Err(e) => {
                        tracing::debug!(
                            core = core_id,
                            %collection,
                            error = %e,
                            "per-collection arena allocation failed; using global allocator"
                        );
                        None
                    }
                }
            })
        } else {
            None
        };
        let coll = match self.get_or_create_vector_index(database_id, tid, collection, dim, field) {
            Ok(c) => c,
            Err(e) => return self.response_error(task, e),
        };
        if let Some(handle) = arena_handle {
            coll.arena_index = handle.arena_index();
        }
        if is_new_collection {
            coll.set_quantization(quantization);
            for (f, kind) in payload_indexes {
                coll.payload.add_index(f.to_ascii_lowercase(), *kind);
            }
        }

        let node_id = coll.insert_with_surrogate(vector.to_vec(), surrogate);
        // Advance the checkpoint watermark so a later vector checkpoint records
        // this write as absorbed; startup replay then skips the straddling WAL
        // record instead of appending a duplicate node.
        if let Some(lsn) = task.wal_lsn() {
            coll.note_checkpoint_lsn(lsn.as_u64());
        }

        // Step 4: update payload bitmap indexes.
        // If this panics (pure in-memory, should not happen), attempt rollback.
        coll.payload.insert_row(node_id, &payload_fields);

        // Persist the metadata sidecar to the sparse store keyed by
        // surrogate-hex. Written UNCONDITIONALLY, including for a row whose
        // statement supplied only the vector: the sparse row is what makes the
        // row scannable at all, so skipping it made such a row invisible to
        // `SELECT *` while every other path still counted it as stored. An
        // empty tagged map is the honest sidecar for "no non-vector columns".
        let storage_key = nodedb_types::StorageKey::for_surrogate(surrogate);
        let sidecar: std::borrow::Cow<'_, [u8]> = if payload.is_empty() {
            match zerompk::to_msgpack_vec(&HashMap::<String, Value>::new()) {
                Ok(bytes) => std::borrow::Cow::Owned(bytes),
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("vector-primary empty sidecar encode failed: {e}"),
                        },
                    );
                }
            }
        } else {
            std::borrow::Cow::Borrowed(payload)
        };
        if let Err(e) = self.sparse.put(
            task.request.database_id.as_u64(),
            tid,
            collection,
            &storage_key,
            &sidecar,
        ) {
            // Roll back Steps 3 + 4 so the HNSW node and bitmap entries
            // do not survive a failed payload persist. Without this,
            // the orphan node would be returned by future searches
            // with `body: null` on the slow path.
            if let Some(coll) = self.vector_collections.get_mut(&index_key) {
                coll.payload.delete_row(node_id, &payload_fields);
                coll.delete(node_id);
            }
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("vector-primary payload sparse write failed: {e}"),
                },
            );
        }

        // Trigger segment seal if needed.
        let seal_key = CoreLoop::vector_build_key(&index_key);
        let coll = self
            .vector_collections
            .get_mut(&index_key)
            .expect("vector collection must exist after insert_with_surrogate");
        if coll.needs_seal()
            && let Some(req) = coll.seal(&seal_key)
            && let Some(tx) = &self.build_tx
            && let Err(e) = tx.send(req)
        {
            tracing::warn!(
                core = self.core_id,
                error = %e,
                "failed to send HNSW build request"
            );
        }

        self.checkpoint_coordinator.mark_dirty("vector", 1);
        // Record this write's version so cross-shard OCC read-set validation
        // (predicate reads always record the collection floor) sees this
        // upsert.
        self.note_surrogate_write_lsn(task, tid, collection, surrogate.as_u32());
        // Answered only once every step above has succeeded, so a statement
        // that fails after the row landed reports the failure rather than a row
        // set. The bytes projected are the ones just handed to the sparse store,
        // which is what a later `SELECT` re-reads verbatim.
        if let Some(spec) = returning {
            return self.vector_stored_returning_response(
                task,
                spec,
                rls_filters,
                &storage_key,
                &sidecar,
            );
        }
        self.response_ok(task)
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

    /// A task carrying `wal_lsn` so `note_surrogate_write_lsn` (gated on
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
    fn direct_upsert_populates_write_version_index_surrogate_and_floor() {
        let mut h = make_core();
        let task = make_task_with_lsn(21);
        let surrogate = Surrogate::new(7);

        let response = h
            .core
            .execute_vector_direct_upsert(VectorDirectUpsertParams {
                task: &task,
                tid: 1,
                collection: "primary_docs",
                field: "emb",
                surrogate,
                vector: &[1.0, 2.0],
                payload: &[],
                quantization: nodedb_types::VectorQuantization::None,
                storage_dtype: nodedb_types::VectorStorageDtype::F32,
                payload_indexes: &[],
                returning: None,
                rls_filters: &[],
            });
        assert_eq!(response.status, Status::Ok);

        let key = WriteKey {
            db: DatabaseId::DEFAULT,
            tenant: TenantId::new(1),
            collection: Box::from("primary_docs"),
            key: KeyRepr::Surrogate(surrogate.as_u32()),
        };
        assert_eq!(
            h.core.write_index.key_write_lsn(&key),
            Some(Lsn::new(21)),
            "direct upsert must populate the per-key (surrogate) write-version index"
        );

        let coll_key = CollKey {
            db: DatabaseId::DEFAULT,
            tenant: TenantId::new(1),
            collection: Box::from("primary_docs"),
        };
        assert_eq!(
            h.core.write_index.collection_write_lsn(&coll_key),
            Some(Lsn::new(21)),
            "direct upsert must advance the collection write-version floor"
        );
    }
}
