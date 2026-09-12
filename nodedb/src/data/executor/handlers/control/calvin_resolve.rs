// SPDX-License-Identifier: BUSL-1.1

//! `MetaOp::CalvinResolve` handler: resolve a staged Calvin transaction's
//! write plans into ONE replayable [`RedoRecord`][crate::wal::RedoRecord],
//! WITHOUT mutating base.
//!
//! Mirrors `MetaOp::ResolveTxn`'s `CoreLoop::execute_resolve_txn` exactly, but
//! sources its plans and tenant scope from Calvin's own staging state instead
//! of a session transaction's: the plans buffered in `commit_pending` under
//! `(epoch, position, vshard)` (by [`CoreLoop::execute_calvin_execute_static`])
//! and the per-core `txn_overlays` / `graph_txn_overlays` entries staged under
//! the corresponding synthetic `TxnId` (by
//! [`CoreLoop::stage_calvin_overlay`][super::calvin_overlay_stage]). Reusing
//! `execute_resolve_txn` directly means the redo serialization logic itself is
//! never duplicated.

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

use super::calvin_txn_id::calvin_synthetic_txn_id;

impl CoreLoop {
    /// Resolve the Calvin transaction staged under `(epoch, position)` on
    /// this vshard into a [`RedoRecord`][crate::wal::RedoRecord] and return
    /// its encoded bytes, without touching any base engine.
    ///
    /// Errors (rather than silently dropping data or producing an empty
    /// record) when no `commit_pending` entry exists for
    /// `(epoch, position, vshard)` — the transaction was never staged (or was
    /// already flushed/dropped), and there is nothing to resolve.
    ///
    /// `DocumentOp::BulkUpdate` / `BulkDelete` plans are staged into the
    /// overlay by `stage_calvin_overlay` (via the predicted-surrogate-set
    /// primitives in `calvin_overlay_stage_bulk`) the same as any other
    /// write, so no separate completeness check is needed here — a plan
    /// missing its required `ollp_predicted_surrogates` already failed loudly
    /// at staging time, before it could ever reach `commit_pending`.
    pub(in crate::data::executor) fn execute_calvin_resolve(
        &mut self,
        task: &ExecutionTask,
        epoch: u64,
        position: u32,
    ) -> Response {
        let vshard_id = task.request.vshard_id.as_u32();
        let synthetic_txn_id = match calvin_synthetic_txn_id(epoch, position, vshard_id) {
            Ok(id) => id,
            Err(e) => return self.response_error(task, e),
        };

        // Clone the staged plans (and the deterministic epoch anchor) out of
        // `commit_pending` so the `&mut self` resolve call — which assigns
        // bitemporal stamps into the overlay — does not overlap the immutable
        // borrow of the pending buffer.
        let (tid, plans, epoch_system_ms) =
            match self.commit_pending.get(&(epoch, position, vshard_id)) {
                Some(pending) => (
                    pending.tenant_id.as_u64(),
                    pending.plans.clone(),
                    pending.epoch_system_ms,
                ),
                None => {
                    return self.response_error(
                        task,
                        crate::Error::Internal {
                            detail: format!(
                                "calvin resolve: no staged commit for epoch={epoch} \
                                 position={position} vshard={vshard_id} (must be staged via \
                                 CalvinExecuteStatic before CalvinResolve)"
                            ),
                        },
                    );
                }
            };

        // Restore the epoch's deterministic time anchor around resolve so the
        // bitemporal stamps `execute_resolve_txn` assigns are identical across
        // replicas (mirrors `execute_calvin_flush`). `CalvinFlush` reads these
        // stamps back from the overlay, so redo and base install agree.
        let prev_epoch_ms = self.epoch_system_ms;
        self.epoch_system_ms = Some(epoch_system_ms);
        let resp = self.execute_resolve_txn(task, tid, synthetic_txn_id, &plans);
        self.epoch_system_ms = prev_epoch_ms;
        resp
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan, UpdateValue};
    use nodedb_types::QualifiedCollection;
    use nodedb_types::Surrogate;
    use nodedb_types::Value;

    use super::*;
    use crate::bridge::envelope::{Admission, ExemptReason, Priority, Request, Status};
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::data::executor::handlers::control::calvin::CalvinExecCtx;
    use crate::types::{DatabaseId, RequestId, TenantId, TraceId, VShardId};
    use crate::wal::RedoRecord;

    /// A minimal `ExecutionTask` homing to vShard 0, tenant 1, database
    /// DEFAULT, matching what the Calvin scheduler dispatches with (see
    /// `dispatch.rs`'s `DatabaseId::DEFAULT` for `CalvinExecuteStatic` /
    /// `CalvinFlush` / `CalvinDrop`; `CalvinResolve` must match).
    fn make_task() -> ExecutionTask {
        let plan = PhysicalPlan::Document(DocumentOp::PointGet {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "x"),
            document_id: "y".into(),
            surrogate: Surrogate::ZERO,
            pk_bytes: Vec::new(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
        });
        let request = Request {
            request_id: RequestId::new(1),
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            plan,
            deadline: Instant::now() + Duration::from_secs(5),
            priority: Priority::Normal,
            trace_id: TraceId::ZERO,
            consistency: crate::types::ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn: None,
            resolved_now_ms: None,
            admission: Admission::Exempt(ExemptReason::Read),
        };
        ExecutionTask::new(request)
    }

    fn doc_value(field: &str, val: &str) -> Vec<u8> {
        let mut obj = std::collections::HashMap::new();
        obj.insert(field.to_string(), Value::String(val.into()));
        zerompk::to_msgpack_vec(&Value::Object(obj)).unwrap()
    }

    fn point_insert_plan(collection: &str, document_id: &str, surrogate: u32) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::PointInsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, collection),
            document_id: document_id.to_string(),
            value: doc_value("a", "1"),
            if_absent: false,
            surrogate: Surrogate::new(surrogate),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            deferred_sum_targets: Vec::new(),
        })
    }

    fn bulk_delete_plan(
        collection: &str,
        ollp_predicted_surrogates: Option<Vec<u32>>,
    ) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::BulkDelete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, collection),
            filters: Vec::new(),
            returning: None,
            ollp_predicted_surrogates,
            ollp_predicted_edges: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        })
    }

    fn bulk_update_plan(
        collection: &str,
        updates: Vec<(String, UpdateValue)>,
        ollp_predicted_surrogates: Option<Vec<u32>>,
    ) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::BulkUpdate {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, collection),
            filters: Vec::new(),
            updates,
            returning: None,
            ollp_predicted_surrogates,
            ollp_predicted_edges: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        })
    }

    /// Seed a row directly into base storage (bypassing Calvin staging), the
    /// pre-existing state the predicate-write staging tests below apply
    /// their predicted surrogate set against.
    fn seed_row(core: &mut CoreLoop, collection: &str, surrogate: u32, field: &str, val: &str) {
        let doc_id = nodedb_types::StorageKey::for_surrogate(Surrogate::new(surrogate));
        let body = crate::data::executor::doc_format::canonicalize_document_for_storage(
            &doc_value(field, val),
        );
        core.sparse
            .put(DatabaseId::DEFAULT.as_u64(), 1, collection, &doc_id, &body)
            .expect("seed row");
    }

    fn stage(
        core: &mut CoreLoop,
        task: &ExecutionTask,
        epoch: u64,
        position: u32,
        plans: &[PhysicalPlan],
    ) {
        let ctx = CalvinExecCtx {
            epoch,
            position,
            epoch_system_ms: 0,
            is_group_leader: true,
        };
        let resp = core.execute_calvin_execute_static(task, ctx, &TenantId::new(1), plans, &[]);
        assert_eq!(resp.status, Status::Ok, "staging must succeed: {resp:?}");
    }

    #[test]
    fn calvin_resolve_returns_redo_for_staged_point_writes() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let task = make_task();

        stage(
            &mut core,
            &task,
            1,
            0,
            &[point_insert_plan("orders", "o1", 7)],
        );

        let resp = core.execute_calvin_resolve(&task, 1, 0);
        assert_eq!(resp.status, Status::Ok, "resolve must succeed: {resp:?}");

        let record = RedoRecord::from_bytes(resp.payload.as_bytes()).expect("decode redo record");
        assert!(
            record.calvin_stamp.is_none(),
            "calvin_stamp is filled in by a later unit, not resolve itself"
        );
        assert_eq!(record.ops.len(), 1, "one staged document put");

        let (collection, doc_id, value, prov, surrogate): (
            String,
            String,
            Vec<u8>,
            Option<nodedb_types::sync::wire::SyncProvenance>,
            u32,
        ) = zerompk::from_msgpack(&record.ops[0].payload).expect("decode document put sub-record");
        assert_eq!(collection, "orders");
        assert_eq!(doc_id, "o1");
        assert_eq!(
            value,
            crate::data::executor::doc_format::canonicalize_document_for_storage(&doc_value(
                "a", "1"
            ))
        );
        assert!(prov.is_none());
        assert_eq!(surrogate, 7);
    }

    /// Decode every `Delete` sub-record's surrogate out of a resolved redo.
    fn deleted_surrogates(record: &RedoRecord) -> Vec<u32> {
        let mut out: Vec<u32> = record
            .ops
            .iter()
            .map(|op| {
                let (_collection, _doc_id, _prov, surrogate): (
                    String,
                    String,
                    Option<nodedb_types::sync::wire::SyncProvenance>,
                    u32,
                ) = zerompk::from_msgpack(&op.payload).expect("decode document delete sub-record");
                surrogate
            })
            .collect();
        out.sort_unstable();
        out
    }

    #[test]
    fn calvin_stages_bulk_delete_from_predicted_set() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let task = make_task();

        // Seed 3 rows; the predicted set names a known 2-row subset.
        seed_row(&mut core, "orders", 10, "a", "1");
        seed_row(&mut core, "orders", 11, "a", "1");
        seed_row(&mut core, "orders", 12, "a", "1");

        stage(
            &mut core,
            &task,
            3,
            0,
            &[bulk_delete_plan("orders", Some(vec![10, 12]))],
        );

        let resp = core.execute_calvin_resolve(&task, 3, 0);
        assert_eq!(resp.status, Status::Ok, "resolve must succeed: {resp:?}");
        let record = RedoRecord::from_bytes(resp.payload.as_bytes()).expect("decode redo record");
        assert_eq!(
            record.ops.len(),
            2,
            "exactly one Delete sub-record per predicted surrogate"
        );
        assert_eq!(deleted_surrogates(&record), vec![10, 12]);
    }

    #[test]
    fn calvin_bulk_delete_ignores_drift_from_predicted_set() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let task = make_task();

        // 3 rows exist in base and would ALL match an empty-predicate
        // (match-everything) live rescan. The predicted set names only 2 of
        // them -- if staging re-derived the row set via a live scan instead
        // of trusting the predicted set, the redo would carry 3 deletes, not
        // 2. This is the determinism proof: staging must key off the
        // predicted set the flush applies, not a fresh predicate scan.
        seed_row(&mut core, "orders", 20, "a", "1");
        seed_row(&mut core, "orders", 21, "a", "1");
        seed_row(&mut core, "orders", 22, "a", "1");

        stage(
            &mut core,
            &task,
            4,
            0,
            &[bulk_delete_plan("orders", Some(vec![20, 21]))],
        );

        let resp = core.execute_calvin_resolve(&task, 4, 0);
        assert_eq!(resp.status, Status::Ok, "resolve must succeed: {resp:?}");
        let record = RedoRecord::from_bytes(resp.payload.as_bytes()).expect("decode redo record");
        assert_eq!(
            record.ops.len(),
            2,
            "redo must reflect ONLY the predicted surrogates, not the live \
             (drifted) match-everything set"
        );
        assert_eq!(deleted_surrogates(&record), vec![20, 21]);
    }

    #[test]
    fn calvin_stages_bulk_update_matches_execute_bulk_update() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let task = make_task();

        seed_row(&mut core, "orders", 30, "a", "1");
        seed_row(&mut core, "orders", 31, "a", "1");

        let literal = nodedb_types::json_to_msgpack(&serde_json::json!("2")).unwrap();
        let updates = vec![("a".to_string(), UpdateValue::Literal(literal))];

        stage(
            &mut core,
            &task,
            5,
            0,
            &[bulk_update_plan("orders", updates, Some(vec![30, 31]))],
        );

        let resp = core.execute_calvin_resolve(&task, 5, 0);
        assert_eq!(resp.status, Status::Ok, "resolve must succeed: {resp:?}");
        let record = RedoRecord::from_bytes(resp.payload.as_bytes()).expect("decode redo record");
        assert_eq!(record.ops.len(), 2, "one Put sub-record per predicted row");

        // The expected post-image is the exact same decode -> apply -> encode
        // pipeline `execute_bulk_update` runs for a plain literal assignment
        // with no generated columns / strict schema in play.
        let expected = crate::data::executor::doc_format::encode_to_msgpack(&serde_json::json!({
            "a": "2"
        }));

        for op in &record.ops {
            let (_collection, _doc_id, value, _prov, _surrogate): (
                String,
                String,
                Vec<u8>,
                Option<nodedb_types::sync::wire::SyncProvenance>,
                u32,
            ) = zerompk::from_msgpack(&op.payload).expect("decode document put sub-record");
            assert_eq!(
                value, expected,
                "post-image must match execute_bulk_update's transform"
            );
        }
    }

    #[test]
    fn calvin_bulk_missing_predicted_surrogates_errors() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let task = make_task();

        let ctx = CalvinExecCtx {
            epoch: 6,
            position: 0,
            epoch_system_ms: 0,
            is_group_leader: true,
        };
        let resp = core.execute_calvin_execute_static(
            &task,
            ctx,
            &TenantId::new(1),
            &[bulk_delete_plan("orders", None)],
            &[],
        );
        assert_eq!(
            resp.status,
            Status::Error,
            "staging a Calvin bulk predicate write with no predicted surrogate \
             set must error loudly, never silently skip"
        );
    }

    #[test]
    fn calvin_resolve_missing_pending_errors() {
        let dir = tempfile::tempdir().unwrap();
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let task = make_task();

        let resp = core.execute_calvin_resolve(&task, 99, 0);
        assert_eq!(
            resp.status,
            Status::Error,
            "resolving an (epoch, position) that was never staged must error"
        );
    }
}
