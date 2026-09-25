// SPDX-License-Identifier: BUSL-1.1

//! Insert-family handler for vector-primary collections:
//! `VectorOp::DirectInsert`, `DirectInsertIfAbsent`, and `DirectUpsert`.
//!
//! Bypasses MessagePack document encoding. The Control Plane serialised the
//! non-vector columns into `payload`; this handler decides the row's
//! existence per `intent`, then stores the vector in HNSW, the bitmap
//! entries, and the payload sidecar through the shared row primitives.
//!
//! Ordering per row:
//!   1. Resolve the index (dimension and dtype checked).
//!   2. Decode `payload` and probe the surrogate.
//!   3. Apply the intent: refuse, skip, replace, or patch.
//!   4. Remove the old row, then write the new one.
//!
//! A failed sidecar write rolls the new node back, so no partial row lands.

use std::collections::HashMap;

use nodedb_physical::physical_plan::{UpdateValue, VectorDirectWriteIntent};
use nodedb_types::{RlsWriteCheck, Surrogate, Value};
use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

use super::rls_write_gate;
use super::upsert::apply_on_conflict_updates;
use super::vector_direct_row::{
    VectorDirectIndexSpec, VectorDirectRowWrite, VectorSidecarRow, encode_sidecar,
};

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
    /// What an existing surrogate means for this row.
    pub intent: VectorDirectWriteIntent,
    /// `ON CONFLICT DO UPDATE SET` patch applied to the stored payload when
    /// the surrogate exists. Empty means whole-row replace.
    pub on_conflict_updates: &'a [(String, UpdateValue)],
    /// Write policy for the patched post-image. Read only on the patch path;
    /// every other image was decided on the Control Plane.
    pub rls_write_check: &'a RlsWriteCheck,
    /// Projection for a `RETURNING` clause, when the statement carried one.
    /// WAL replay and replication build this op with no client session behind
    /// them, so they leave it `None`.
    pub returning: Option<&'a nodedb_physical::physical_plan::ReturningSpec>,
    /// Compiled row-level-security READ predicate gating the row `returning`
    /// emits — a separate gate from the write admission the Control Plane
    /// already applied to `payload`.
    pub rls_filters: &'a [u8],
}

/// The statement-wide inputs one row's `ON CONFLICT DO UPDATE SET` merge
/// reads.
#[derive(Clone, Copy)]
pub(in crate::data::executor) struct VectorUpsertPatch<'a> {
    pub database_id: u64,
    pub tid: u64,
    pub collection: &'a str,
    pub field: &'a str,
    pub on_conflict_updates: &'a [(String, UpdateValue)],
    pub rls_write_check: &'a RlsWriteCheck,
}

/// One row's planned conflict patch: the merged, lower-cased payload and
/// the stored sidecar it replaces.
pub(in crate::data::executor) struct VectorPlannedUpsert {
    pub fields: HashMap<String, Value>,
    /// The stored sidecar the merge read; `None` when the bound node had no
    /// sidecar row behind it.
    pub old_sidecar: Option<Vec<u8>>,
}

/// Merge the conflict patch into the row's current sidecar (`stored`, as
/// the writer currently sees it: base storage for a live write, base ∪
/// overlay for a staged one), with the proposed `fields` as `EXCLUDED`, and
/// decide the write policy on the result. The merged image is what the
/// policy decides, since it exists nowhere before this point. The one merge
/// every upsert path runs, so a live and a staged `ON CONFLICT` agree.
pub(in crate::data::executor) fn merge_vector_upsert_patch(
    patch: &VectorUpsertPatch<'_>,
    stored: Option<VectorSidecarRow>,
    fields: HashMap<String, Value>,
) -> Result<VectorPlannedUpsert, ErrorCode> {
    let VectorUpsertPatch {
        database_id: _,
        tid,
        collection,
        field,
        on_conflict_updates,
        rls_write_check,
    } = *patch;
    let (stored, old_sidecar) = match stored {
        Some(row) => (row.fields, Some(row.bytes)),
        None => (HashMap::new(), None),
    };
    // Assignments see the stored row; `EXCLUDED.col` reads the proposed one.
    let merged = match apply_on_conflict_updates(
        Value::Object(stored),
        &Value::Object(fields),
        on_conflict_updates,
    )? {
        Value::Object(map) => map,
        other => {
            return Err(ErrorCode::Internal {
                detail: format!(
                    "ON CONFLICT merge on '{collection}' produced a non-object row: {other:?}"
                ),
            });
        }
    };
    let image = Value::Object(merged.clone());
    rls_write_gate::admit_document_value(rls_write_check, &image, tid, collection)?;
    // The vector column never lives in the sidecar: the proposed row's
    // vector replaces the stored node whatever the patch names, so an
    // assignment to it is dropped from the payload.
    let fields = merged
        .into_iter()
        .filter(|(k, _)| !k.eq_ignore_ascii_case(field))
        .map(|(k, v)| (k.to_ascii_lowercase(), v))
        .collect();
    Ok(VectorPlannedUpsert {
        fields,
        old_sidecar,
    })
}

impl CoreLoop {
    /// Merge the conflict patch into `surrogate`'s stored sidecar: read the
    /// base row, then [`merge_vector_upsert_patch`].
    pub(in crate::data::executor) fn plan_vector_upsert_patch(
        &self,
        patch: &VectorUpsertPatch<'_>,
        surrogate: Surrogate,
        fields: HashMap<String, Value>,
    ) -> Result<VectorPlannedUpsert, ErrorCode> {
        let stored =
            self.vector_sidecar_row(patch.database_id, patch.tid, patch.collection, surrogate)?;
        merge_vector_upsert_patch(patch, stored, fields)
    }

    /// Handle `VectorOp::DirectInsert` / `DirectInsertIfAbsent` /
    /// `DirectUpsert`.
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
            intent,
            on_conflict_updates,
            rls_write_check,
            returning,
            rls_filters,
        } = params;
        debug!(
            core = self.core_id,
            %collection,
            %field,
            dim = vector.len(),
            ?intent,
            "vector direct write"
        );
        let database_id = task.request.database_id.as_u64();

        let index_key = match self.vector_direct_index(
            task,
            tid,
            &VectorDirectIndexSpec {
                collection,
                field,
                dim: vector.len(),
                quantization,
                storage_dtype,
                payload_indexes,
            },
        ) {
            Ok(key) => key,
            Err(e) => return self.response_error(task, e),
        };

        // Empty slice → empty map (the row carried no non-vector column).
        // Field names are lower-cased so the bitmap insert and the SELECT
        // pre-filter agree regardless of how the SQL caller capitalized them.
        let mut fields: HashMap<String, Value> = if payload.is_empty() {
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

        let existing = self.vector_direct_node(&index_key, surrogate).is_some();
        match intent {
            VectorDirectWriteIntent::Insert if existing => {
                return self.response_error(
                    task,
                    crate::Error::RejectedConstraint {
                        collection: collection.to_string(),
                        constraint: "unique".to_string(),
                        detail: format!(
                            "duplicate key value violates primary-key uniqueness on \
                             '{collection}'"
                        ),
                    },
                );
            }
            VectorDirectWriteIntent::InsertIfAbsent if existing => {
                // `ON CONFLICT DO NOTHING`: nothing is written, so the count is
                // 0 and a `RETURNING` clause has no post-image to project.
                if let Some(spec) = returning {
                    return self.vector_stored_returning_response(task, spec, rls_filters, &[]);
                }
                return self.response_affected(task, 0);
            }
            VectorDirectWriteIntent::Upsert if existing && !on_conflict_updates.is_empty() => {
                let patch = VectorUpsertPatch {
                    database_id,
                    tid,
                    collection,
                    field,
                    on_conflict_updates,
                    rls_write_check,
                };
                fields = match self.plan_vector_upsert_patch(&patch, surrogate, fields) {
                    Ok(planned) => planned.fields,
                    Err(e) => return self.response_error(task, e),
                };
            }
            VectorDirectWriteIntent::Insert
            | VectorDirectWriteIntent::InsertIfAbsent
            | VectorDirectWriteIntent::Upsert => {}
        }

        // The sidecar is written UNCONDITIONALLY, including for a row whose
        // statement supplied only the vector: the sparse row is what makes the
        // row scannable at all. An empty tagged map is the honest sidecar for
        // "no non-vector columns".
        let sidecar = match encode_sidecar(&fields) {
            Ok(bytes) => bytes,
            Err(e) => return self.response_error(task, e),
        };

        if existing
            && let Err(e) = self.remove_vector_direct_row(&index_key, tid, collection, surrogate)
        {
            return self.response_error(task, e);
        }
        if let Err(e) = self.write_vector_direct_row(VectorDirectRowWrite {
            index_key: &index_key,
            tid,
            collection,
            surrogate,
            vector,
            fields: &fields,
            sidecar: &sidecar,
        }) {
            return self.response_error(task, e);
        }
        self.finish_vector_direct_write(task, &index_key, tid, collection, &[surrogate]);

        // Answered only once every step above has succeeded, so a statement
        // that fails after the row landed reports the failure rather than a row
        // set. The bytes projected are the ones just handed to the sparse store,
        // which is what a later `SELECT` re-reads verbatim.
        if let Some(spec) = returning {
            let storage_key = nodedb_types::StorageKey::for_surrogate(surrogate);
            return self.vector_stored_returning_response(
                task,
                spec,
                rls_filters,
                &[(&storage_key, sidecar.as_slice())],
            );
        }
        if !on_conflict_updates.is_empty() {
            let op = if existing { "update" } else { "insert" };
            return self.response_affected_with_op(task, 1, op);
        }
        self.response_affected(task, 1)
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

    fn write(
        h: &mut CoreHarness,
        task: &ExecutionTask,
        surrogate: Surrogate,
        vector: &[f32],
        intent: VectorDirectWriteIntent,
    ) -> Response {
        h.core
            .execute_vector_direct_upsert(VectorDirectUpsertParams {
                task,
                tid: 1,
                collection: "primary_docs",
                field: "emb",
                surrogate,
                vector,
                payload: &[],
                quantization: nodedb_types::VectorQuantization::None,
                storage_dtype: nodedb_types::VectorStorageDtype::F32,
                payload_indexes: &[],
                intent,
                on_conflict_updates: &[],
                rls_write_check: &RlsWriteCheck::decided_earlier_in_request(),
                returning: None,
                rls_filters: &[],
            })
    }

    fn index_key() -> (DatabaseId, TenantId, String) {
        CoreLoop::vector_index_key(DatabaseId::DEFAULT.as_u64(), 1, "primary_docs", "emb")
    }

    #[test]
    fn direct_upsert_populates_write_version_index_surrogate_and_floor() {
        let mut h = make_core();
        let task = make_task_with_lsn(21);
        let surrogate = Surrogate::new(7);

        let response = write(
            &mut h,
            &task,
            surrogate,
            &[1.0, 2.0],
            VectorDirectWriteIntent::Upsert,
        );
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

    #[test]
    fn direct_insert_refuses_an_existing_surrogate() {
        let mut h = make_core();
        let task = make_task_with_lsn(1);
        let s = Surrogate::new(3);
        assert_eq!(
            write(
                &mut h,
                &task,
                s,
                &[1.0, 0.0],
                VectorDirectWriteIntent::Insert
            )
            .status,
            Status::Ok
        );
        let dup = write(
            &mut h,
            &task,
            s,
            &[0.0, 1.0],
            VectorDirectWriteIntent::Insert,
        );
        assert_eq!(dup.status, Status::Error);
        assert!(matches!(
            dup.error_code.as_deref(),
            Some(ErrorCode::RejectedConstraint { constraint, .. }) if constraint == "unique"
        ));
        let coll = h.core.vector_collections.get(&index_key()).expect("index");
        assert_eq!(
            coll.live_count(),
            1,
            "the refused insert must write nothing"
        );
    }

    #[test]
    fn direct_insert_if_absent_skips_an_existing_surrogate() {
        let mut h = make_core();
        let task = make_task_with_lsn(1);
        let s = Surrogate::new(4);
        write(
            &mut h,
            &task,
            s,
            &[1.0, 0.0],
            VectorDirectWriteIntent::Insert,
        );
        let skip = write(
            &mut h,
            &task,
            s,
            &[0.0, 1.0],
            VectorDirectWriteIntent::InsertIfAbsent,
        );
        assert_eq!(skip.status, Status::Ok);
        let affected =
            crate::control::server::shared::sql::staging_predicates::extract_affected_count(
                skip.payload.as_bytes(),
            )
            .expect("affected count");
        assert_eq!(affected, 0);
        let coll = h.core.vector_collections.get(&index_key()).expect("index");
        assert_eq!(coll.live_count(), 1);
    }

    #[test]
    fn direct_upsert_replaces_the_node_and_keeps_one_live() {
        let mut h = make_core();
        let task = make_task_with_lsn(1);
        let s = Surrogate::new(5);
        write(
            &mut h,
            &task,
            s,
            &[1.0, 0.0],
            VectorDirectWriteIntent::Insert,
        );
        let first = h
            .core
            .vector_direct_node(&index_key(), s)
            .expect("bound after insert");
        let again = write(
            &mut h,
            &task,
            s,
            &[0.0, 1.0],
            VectorDirectWriteIntent::Upsert,
        );
        assert_eq!(again.status, Status::Ok);
        let coll = h.core.vector_collections.get(&index_key()).expect("index");
        assert_eq!(coll.live_count(), 1, "the old node must be tombstoned");
        assert_ne!(coll.local_for_surrogate(s), Some(first));
    }
}
