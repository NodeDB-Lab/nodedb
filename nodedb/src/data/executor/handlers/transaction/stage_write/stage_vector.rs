// SPDX-License-Identifier: BUSL-1.1

//! Statement-time staging for vector-primary direct writes issued inside a
//! `BEGIN..COMMIT` block: the insert family here (`DirectInsert`,
//! `DirectInsertIfAbsent`, `DirectUpsert`), `DirectDelete` / `DirectUpdate`
//! in `stage_vector_targets`.
//!
//! A vector-primary row is one surrogate-keyed row. Its staged body is a
//! [`StagedVectorRow`]: the vector plus the exact sidecar bytes the live
//! handler stores, so a same-transaction search ranks the vector and a
//! same-transaction point read or scan renders the sidecar. Each op decides
//! its outcome against BASE ∪ OVERLAY with the live handler's own rules:
//! a duplicate key refuses, `ON CONFLICT DO NOTHING` skips, a conflict
//! patch merges through the live handler's merge. COMMIT replays the
//! buffered plan through the live handler, which stays the durable apply.

use std::collections::HashMap;

use nodedb_physical::physical_plan::{UpdateValue, VectorDirectWriteIntent, VectorOp};
use nodedb_types::{RlsWriteCheck, RowIdentity, StorageKey, Surrogate, Value};

use super::context::StageCtx;
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::overlay::{Staged, StagedVectorRow};
use crate::data::executor::handlers::vector_direct_row::{
    VectorDirectIndexSpec, VectorIndexKey, VectorSidecarRow, encode_sidecar,
};
use crate::data::executor::handlers::vector_upsert::{
    VectorUpsertPatch, decode_payload_lowercased, merge_vector_upsert_patch,
};
use crate::data::executor::scan_normalize::sparse_body_to_msgpack;
use crate::data::executor::sparse_body_format::SparseBodyFormatRef;
use crate::data::executor::task::ExecutionTask;
use crate::types::TxnId;

use super::stage_vector_targets::{StageVectorDeleteParams, StageVectorUpdateParams};

/// A vector-primary row as this transaction currently sees it: the staged
/// image when the transaction wrote it, else the base row.
pub(super) struct VectorCurrentRow {
    pub vector: Vec<f32>,
    pub sidecar: VectorSidecarRow,
}

/// Inputs of one staged insert-family write.
pub(super) struct StageVectorInsertParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub txn_id: TxnId,
    pub collection: &'a str,
    pub field: &'a str,
    pub surrogate: Surrogate,
    pub pk_bytes: &'a [u8],
    pub vector: &'a [f32],
    pub payload: &'a [u8],
    pub quantization: nodedb_types::VectorQuantization,
    pub storage_dtype: nodedb_types::VectorStorageDtype,
    pub payload_indexes: &'a [(String, nodedb_types::PayloadIndexKind)],
    pub intent: VectorDirectWriteIntent,
    pub on_conflict_updates: &'a [(String, UpdateValue)],
    pub rls_write_check: &'a RlsWriteCheck,
}

/// The client identity of a vector-primary row, read from its sidecar by
/// the rule INSERT mints it with. A sidecar with no identity column yields
/// the decimal surrogate.
pub(super) fn staged_vector_row_identity(sidecar: &[u8], key: StorageKey) -> RowIdentity {
    let normalized = sparse_body_to_msgpack(sidecar, SparseBodyFormatRef::VectorSidecar);
    RowIdentity::of_stored_row(&normalized, None, key)
}

/// The staged body for `vector` and `fields`: the sidecar encoded exactly
/// as the live handler stores it, packed with the vector.
pub(super) fn encode_staged_vector_row(
    vector: &[f32],
    fields: &HashMap<String, Value>,
) -> Result<(Vec<u8>, Vec<u8>), ErrorCode> {
    let sidecar = encode_sidecar(fields)?;
    let staged = StagedVectorRow {
        vector: vector.to_vec(),
        sidecar: sidecar.clone(),
    }
    .to_bytes()
    .map_err(ErrorCode::from)?;
    Ok((staged, sidecar))
}

impl CoreLoop {
    /// Route a stageable `VectorOp` to its staging handler. Every op that is
    /// not a vector-primary direct write is internal here.
    pub(in crate::data::executor) fn execute_stage_vector(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        txn_id: TxnId,
        op: &VectorOp,
    ) -> Response {
        match op {
            VectorOp::DirectInsert {
                collection,
                field,
                surrogate,
                pk_bytes,
                vector,
                payload,
                quantization,
                storage_dtype,
                payload_indexes,
                // A row-returning write inside a transaction is refused by the
                // dispatch loop before staging; the count is the whole reply.
                returning: _,
                rls_filters: _,
            }
            | VectorOp::DirectInsertIfAbsent {
                collection,
                field,
                surrogate,
                pk_bytes,
                vector,
                payload,
                quantization,
                storage_dtype,
                payload_indexes,
                returning: _,
                rls_filters: _,
            } => {
                let intent = if let VectorOp::DirectInsertIfAbsent { .. } = op {
                    VectorDirectWriteIntent::InsertIfAbsent
                } else {
                    VectorDirectWriteIntent::Insert
                };
                // The Control Plane decided the write policy on the whole
                // image; no patch is merged here.
                let decided = RlsWriteCheck::decided_earlier_in_request();
                self.stage_vector_insert_family(StageVectorInsertParams {
                    task,
                    tid,
                    txn_id,
                    collection: collection.as_str(),
                    field,
                    surrogate: *surrogate,
                    pk_bytes,
                    vector,
                    payload,
                    quantization: *quantization,
                    storage_dtype: *storage_dtype,
                    payload_indexes,
                    intent,
                    on_conflict_updates: &[],
                    rls_write_check: &decided,
                })
            }
            VectorOp::DirectUpsert {
                collection,
                field,
                surrogate,
                pk_bytes,
                vector,
                payload,
                quantization,
                storage_dtype,
                payload_indexes,
                returning: _,
                rls_filters: _,
                on_conflict_updates,
                rls_write_check,
            } => self.stage_vector_insert_family(StageVectorInsertParams {
                task,
                tid,
                txn_id,
                collection: collection.as_str(),
                field,
                surrogate: *surrogate,
                pk_bytes,
                vector,
                payload,
                quantization: *quantization,
                storage_dtype: *storage_dtype,
                payload_indexes,
                intent: VectorDirectWriteIntent::Upsert,
                on_conflict_updates,
                rls_write_check,
            }),
            VectorOp::DirectDelete {
                collection,
                field,
                targets,
                returning: _,
                rls_filters: _,
                rls_write_check,
            } => self.stage_vector_delete(StageVectorDeleteParams {
                task,
                tid,
                txn_id,
                collection: collection.as_str(),
                field,
                targets,
                rls_write_check,
            }),
            VectorOp::DirectUpdate {
                collection,
                field,
                targets,
                new_vector,
                payload_patch,
                quantization,
                storage_dtype,
                payload_indexes,
                returning: _,
                rls_filters: _,
                rls_write_check,
            } => self.stage_vector_update(StageVectorUpdateParams {
                task,
                tid,
                txn_id,
                collection: collection.as_str(),
                field,
                targets,
                new_vector: new_vector.as_deref(),
                payload_patch,
                quantization: *quantization,
                storage_dtype: *storage_dtype,
                payload_indexes,
                rls_write_check,
            }),
            VectorOp::Search { .. }
            | VectorOp::Insert { .. }
            | VectorOp::BatchInsert { .. }
            | VectorOp::MultiSearch { .. }
            | VectorOp::Delete { .. }
            | VectorOp::DeleteBySurrogate { .. }
            | VectorOp::SetParams { .. }
            | VectorOp::DropIndex { .. }
            | VectorOp::QueryStats { .. }
            | VectorOp::Seal { .. }
            | VectorOp::CompactIndex { .. }
            | VectorOp::Rebuild { .. }
            | VectorOp::SparseInsert { .. }
            | VectorOp::SparseSearch { .. }
            | VectorOp::SparseDelete { .. }
            | VectorOp::MultiVectorInsert { .. }
            | VectorOp::MultiVectorDelete { .. }
            | VectorOp::MultiVectorScoreSearch { .. }
            // Autocommit-then-Raft by construction, never staged.
            | VectorOp::ResolveDirectWrite(_)
            | VectorOp::ResolvedDirectWrite { .. } => self.stage_not_point_write(task),
        }
    }

    /// The index key a staged direct write lands in, with the live
    /// handler's width and dtype check against an existing index. With no
    /// index yet, the collection's declared width is the one check left;
    /// COMMIT's replay creates the index.
    pub(super) fn stage_vector_index_key(
        &self,
        ctx: &StageCtx<'_>,
        spec: &VectorDirectIndexSpec<'_>,
    ) -> Result<VectorIndexKey, ErrorCode> {
        if let Some(key) = self.check_vector_direct_index(ctx.database_id, ctx.tid, spec)? {
            return Ok(key);
        }
        let declared_dim = self
            .doc_configs
            .get(&ctx.coll_key)
            .and_then(|config| config.vector_primary.as_ref())
            .map(|primary| primary.dim as usize);
        if let Some(dim) = declared_dim
            && dim != spec.dim
        {
            return Err(ErrorCode::RejectedConstraint {
                detail: String::new(),
                constraint: format!(
                    "vector dimension mismatch: collection declares {dim}, got {}",
                    spec.dim
                ),
            });
        }
        Ok(CoreLoop::vector_index_key(
            ctx.database_id,
            ctx.tid,
            spec.collection,
            spec.field,
        ))
    }

    /// The row `surrogate` currently is under BASE ∪ OVERLAY: the staged
    /// image, `None` past a staged tombstone, else the base node's vector
    /// and sidecar. A base node with no sidecar row reads as an empty
    /// payload, the same as the live handlers read it.
    pub(super) fn stage_vector_current_row(
        &self,
        ctx: &StageCtx<'_>,
        index_key: &VectorIndexKey,
        surrogate: Surrogate,
    ) -> Result<Option<VectorCurrentRow>, ErrorCode> {
        let staged = self
            .txn_overlays
            .get(&ctx.txn_id)
            .and_then(|o| o.get(&ctx.coll_key, surrogate.0));
        match staged {
            Some(Staged::Tombstone) => return Ok(None),
            Some(Staged::Put(body)) => {
                let row = StagedVectorRow::from_bytes(body).map_err(ErrorCode::from)?;
                let fields =
                    decode_payload_lowercased(&row.sidecar).map_err(|e| ErrorCode::Internal {
                        detail: format!(
                            "staged vector-primary sidecar decode failed for {}: {e}",
                            StorageKey::for_surrogate(surrogate)
                        ),
                    })?;
                return Ok(Some(VectorCurrentRow {
                    vector: row.vector,
                    sidecar: VectorSidecarRow {
                        bytes: row.sidecar,
                        fields,
                    },
                }));
            }
            None => {}
        }
        let Some(coll) = self.vector_collections.get(index_key) else {
            return Ok(None);
        };
        let Some(vector) = coll.vector_for_surrogate(surrogate) else {
            return Ok(None);
        };
        let sidecar = self
            .vector_sidecar_row(ctx.database_id, ctx.tid, ctx.collection, surrogate)?
            .unwrap_or_else(|| VectorSidecarRow {
                bytes: Vec::new(),
                fields: HashMap::new(),
            });
        Ok(Some(VectorCurrentRow { vector, sidecar }))
    }

    /// Stage `DirectInsert` / `DirectInsertIfAbsent` / `DirectUpsert`.
    fn stage_vector_insert_family(&mut self, params: StageVectorInsertParams<'_>) -> Response {
        let StageVectorInsertParams {
            task,
            tid,
            txn_id,
            collection,
            field,
            surrogate,
            pk_bytes,
            vector,
            payload,
            quantization,
            storage_dtype,
            payload_indexes,
            intent,
            on_conflict_updates,
            rls_write_check,
        } = params;
        let pk = match std::str::from_utf8(pk_bytes) {
            Ok(pk) => pk,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("vector-primary key on '{collection}' is not UTF-8: {e}"),
                    },
                );
            }
        };
        let ctx = StageCtx::new(
            task,
            tid,
            txn_id,
            collection,
            RowIdentity::from_user_key(pk),
            surrogate,
        );
        let index_key = match self.stage_vector_index_key(
            &ctx,
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
        // Empty slice → empty map; field names lower-cased, as the live
        // handler decodes them.
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

        let current = match self.stage_vector_current_row(&ctx, &index_key, surrogate) {
            Ok(current) => current,
            Err(e) => return self.response_error(task, e),
        };
        let existing = current.is_some();
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
                return self.stage_count_response(task, 0);
            }
            VectorDirectWriteIntent::Upsert if existing && !on_conflict_updates.is_empty() => {
                let patch = VectorUpsertPatch {
                    database_id: ctx.database_id,
                    tid,
                    collection,
                    field,
                    on_conflict_updates,
                    rls_write_check,
                };
                let stored = current.map(|row| row.sidecar);
                fields = match merge_vector_upsert_patch(&patch, stored, fields) {
                    Ok(planned) => planned.fields,
                    Err(e) => return self.response_error(task, e),
                };
            }
            VectorDirectWriteIntent::Insert
            | VectorDirectWriteIntent::InsertIfAbsent
            | VectorDirectWriteIntent::Upsert => {}
        }

        let (staged, _sidecar) = match encode_staged_vector_row(vector, &fields) {
            Ok(encoded) => encoded,
            Err(e) => return self.response_error(task, e),
        };
        if let Err(e) = self.stage_put_capped(&ctx, staged) {
            return self.response_error(task, e);
        }
        if !on_conflict_updates.is_empty() {
            let op = if existing { "update" } else { "insert" };
            return self.response_affected_with_op(task, 1, op);
        }
        self.stage_count_response(task, 1)
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use nodedb_physical::physical_plan::{DocumentOp, VectorWriteTargets};

    use super::*;
    use crate::bridge::envelope::{
        Admission, ExemptReason, PhysicalPlan, Priority, Request, Status,
    };
    use crate::control::server::shared::sql::staging_predicates::extract_affected_count;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::data::executor::handlers::transaction::overlay::VectorMergeParams;
    use crate::data::executor::handlers::vector_upsert::VectorDirectUpsertParams;
    use crate::types::*;

    const COLL: &str = "vp";
    const FIELD: &str = "vec";

    fn make_task(txn_id: Option<TxnId>) -> ExecutionTask {
        let plan = PhysicalPlan::Document(DocumentOp::PointGet {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, COLL),
            document_id: "r".into(),
            surrogate: Surrogate::ZERO,
            pk_bytes: Vec::new(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
        });
        ExecutionTask::new(Request {
            request_id: RequestId::new(1),
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
            txn_id,
            wal_lsn: None,
            resolved_now_ms: None,
            admission: Admission::Exempt(ExemptReason::Read),
        })
    }

    fn payload(owner: &str) -> Vec<u8> {
        let mut fields = HashMap::new();
        fields.insert("id".to_string(), Value::String("r".into()));
        fields.insert("owner".to_string(), Value::String(owner.into()));
        zerompk::to_msgpack_vec(&fields).expect("encode payload")
    }

    fn insert_op(intent: VectorDirectWriteIntent, surrogate: u32, vector: Vec<f32>) -> VectorOp {
        let collection = nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, COLL);
        match intent {
            VectorDirectWriteIntent::Insert => VectorOp::DirectInsert {
                collection,
                field: FIELD.into(),
                surrogate: Surrogate::new(surrogate),
                pk_bytes: b"r".to_vec(),
                vector,
                payload: payload("alice"),
                quantization: nodedb_types::VectorQuantization::None,
                storage_dtype: nodedb_types::VectorStorageDtype::F32,
                payload_indexes: Vec::new(),
                returning: None,
                rls_filters: Vec::new(),
            },
            VectorDirectWriteIntent::InsertIfAbsent => VectorOp::DirectInsertIfAbsent {
                collection,
                field: FIELD.into(),
                surrogate: Surrogate::new(surrogate),
                pk_bytes: b"r".to_vec(),
                vector,
                payload: payload("alice"),
                quantization: nodedb_types::VectorQuantization::None,
                storage_dtype: nodedb_types::VectorStorageDtype::F32,
                payload_indexes: Vec::new(),
                returning: None,
                rls_filters: Vec::new(),
            },
            VectorDirectWriteIntent::Upsert => VectorOp::DirectUpsert {
                collection,
                field: FIELD.into(),
                surrogate: Surrogate::new(surrogate),
                pk_bytes: b"r".to_vec(),
                vector,
                payload: payload("bob"),
                quantization: nodedb_types::VectorQuantization::None,
                storage_dtype: nodedb_types::VectorStorageDtype::F32,
                payload_indexes: Vec::new(),
                returning: None,
                rls_filters: Vec::new(),
                on_conflict_updates: Vec::new(),
                rls_write_check: RlsWriteCheck::decided_earlier_in_request(),
            },
        }
    }

    fn delete_op(surrogates: Vec<u32>) -> VectorOp {
        VectorOp::DirectDelete {
            collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, COLL),
            field: FIELD.into(),
            targets: VectorWriteTargets::Surrogates(
                surrogates.into_iter().map(Surrogate::new).collect(),
            ),
            returning: None,
            rls_filters: Vec::new(),
            rls_write_check: RlsWriteCheck::decided_earlier_in_request(),
        }
    }

    fn affected(resp: &Response) -> u64 {
        assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
        extract_affected_count(resp.payload.as_bytes()).expect("count")
    }

    fn write_base(core: &mut CoreLoop, surrogate: u32, vector: &[f32]) {
        let task = make_task(None);
        let resp = core.execute_vector_direct_upsert(VectorDirectUpsertParams {
            task: &task,
            tid: 1,
            collection: COLL,
            field: FIELD,
            surrogate: Surrogate::new(surrogate),
            vector,
            payload: &payload("alice"),
            quantization: nodedb_types::VectorQuantization::None,
            storage_dtype: nodedb_types::VectorStorageDtype::F32,
            payload_indexes: &[],
            intent: VectorDirectWriteIntent::Insert,
            on_conflict_updates: &[],
            rls_write_check: &RlsWriteCheck::decided_earlier_in_request(),
            returning: None,
            rls_filters: &[],
        });
        assert_eq!(resp.status, Status::Ok);
    }

    fn ranked(core: &CoreLoop, txn_id: TxnId, query: &[f32]) -> Vec<u32> {
        let mut hits = Vec::new();
        core.merge_vector_primary_overlay_into_search(
            VectorMergeParams {
                txn_id,
                database_id: DatabaseId::DEFAULT,
                tid: TenantId::new(1),
                collection: COLL,
                field_name: FIELD,
                query_vector: query,
                metric: nodedb_types::vector_distance::DistanceMetric::L2,
                top_k: 10,
                filter_bitmap: None,
                payload_filters: &[],
            },
            &mut hits,
        )
        .expect("merge");
        hits.iter()
            .filter_map(|h| h.id.storage_key().map(|k| k.surrogate().as_u32()))
            .collect()
    }

    #[test]
    fn staged_insert_refuses_a_duplicate_and_if_absent_skips() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let txn_id = TxnId::new(7);
        let task = make_task(Some(txn_id));

        let first = core.execute_stage_vector(
            &task,
            1,
            txn_id,
            &insert_op(VectorDirectWriteIntent::Insert, 3, vec![1.0, 0.0]),
        );
        assert_eq!(affected(&first), 1);

        let dup = core.execute_stage_vector(
            &task,
            1,
            txn_id,
            &insert_op(VectorDirectWriteIntent::Insert, 3, vec![0.0, 1.0]),
        );
        assert_eq!(dup.status, Status::Error);
        assert!(matches!(
            dup.error_code.as_deref(),
            Some(ErrorCode::RejectedConstraint { constraint, .. }) if constraint == "unique"
        ));

        let skip = core.execute_stage_vector(
            &task,
            1,
            txn_id,
            &insert_op(VectorDirectWriteIntent::InsertIfAbsent, 3, vec![0.0, 1.0]),
        );
        assert_eq!(affected(&skip), 0);
        assert_eq!(ranked(&core, txn_id, &[1.0, 0.0]), vec![3]);
    }

    #[test]
    fn staged_upsert_replaces_the_staged_vector() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let txn_id = TxnId::new(8);
        let task = make_task(Some(txn_id));
        core.execute_stage_vector(
            &task,
            1,
            txn_id,
            &insert_op(VectorDirectWriteIntent::Insert, 3, vec![1.0, 0.0]),
        );
        let up = core.execute_stage_vector(
            &task,
            1,
            txn_id,
            &insert_op(VectorDirectWriteIntent::Upsert, 3, vec![0.0, 1.0]),
        );
        assert_eq!(affected(&up), 1);
        let coll_key = (DatabaseId::DEFAULT, TenantId::new(1), COLL.to_string());
        let Some(Staged::Put(body)) = core
            .txn_overlays
            .get(&txn_id)
            .and_then(|o| o.get(&coll_key, 3))
        else {
            panic!("the upsert must leave a staged put");
        };
        let row = StagedVectorRow::from_bytes(body).expect("decode");
        assert_eq!(row.vector, vec![0.0, 1.0]);
        let fields = decode_payload_lowercased(&row.sidecar).expect("sidecar");
        assert_eq!(fields.get("owner"), Some(&Value::String("bob".into())));
    }

    #[test]
    fn staged_delete_hides_a_base_row_and_a_redelete_counts_zero() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        write_base(&mut core, 5, &[1.0, 0.0]);
        let txn_id = TxnId::new(9);
        let task = make_task(Some(txn_id));

        let del = core.execute_stage_vector(&task, 1, txn_id, &delete_op(vec![5, 5, 0]));
        assert_eq!(affected(&del), 1);
        let again = core.execute_stage_vector(&task, 1, txn_id, &delete_op(vec![5]));
        assert_eq!(affected(&again), 0);

        let dup = core.execute_stage_vector(
            &task,
            1,
            txn_id,
            &insert_op(VectorDirectWriteIntent::Insert, 5, vec![0.0, 1.0]),
        );
        assert_eq!(affected(&dup), 1, "a tombstoned key is free to re-insert");
        assert_eq!(ranked(&core, txn_id, &[0.0, 1.0]), vec![5]);
    }

    #[test]
    fn staged_tombstone_removes_the_base_hit_from_a_search() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        write_base(&mut core, 5, &[1.0, 0.0]);
        let txn_id = TxnId::new(10);
        let task = make_task(Some(txn_id));
        core.execute_stage_vector(&task, 1, txn_id, &delete_op(vec![5]));

        let key = crate::data::executor::handlers::hybrid_key::HybridFusionKey::for_surrogate(
            Surrogate::new(5),
        );
        let mut hits = vec![crate::data::executor::response_codec::VectorSearchHit {
            id: key,
            distance: 0.0,
            doc_id: None,
            body: None,
        }];
        core.merge_vector_primary_overlay_into_search(
            VectorMergeParams {
                txn_id,
                database_id: DatabaseId::DEFAULT,
                tid: TenantId::new(1),
                collection: COLL,
                field_name: FIELD,
                query_vector: &[1.0, 0.0],
                metric: nodedb_types::vector_distance::DistanceMetric::L2,
                top_k: 10,
                filter_bitmap: None,
                payload_filters: &[],
            },
            &mut hits,
        )
        .expect("merge");
        assert!(hits.is_empty(), "the tombstoned base hit must be hidden");
    }
}
