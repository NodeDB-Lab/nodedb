// SPDX-License-Identifier: BUSL-1.1

//! Read-only bounded CRDT delta preview handler.

use nodedb_types::CrdtPreviewResult;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Preview a delta without creating tenant or collection state and without
    /// touching HWM, DLQ, checkpoints, sparse storage, or WAL state.
    pub(in crate::data::executor) fn execute_crdt_preview_apply(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        document_id: &str,
        delta: &[u8],
    ) -> Response {
        let key = (task.request.database_id, task.request.tenant_id);
        let preview = if let Some(engine) = self.crdt_engines.get(&key) {
            engine
                .preview_delta(collection, document_id, delta)
                .map(|preview| {
                    (
                        preview,
                        engine.frontier_digest(task.request.database_id, collection),
                    )
                })
        } else {
            let engine = match crate::engine::crdt::tenant_state::TenantCrdtEngine::new(
                task.request.tenant_id,
                self.core_id as u64,
                nodedb_crdt::ConstraintSet::new(),
            ) {
                Ok(engine) => engine,
                Err(error) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: error.to_string(),
                        },
                    );
                }
            };
            engine
                .preview_delta(collection, document_id, delta)
                .map(|preview| {
                    (
                        preview,
                        engine.frontier_digest(task.request.database_id, collection),
                    )
                })
        };
        let (preview, frontier_digest) = match preview {
            Ok(preview) => preview,
            // Causally-pending operations are the one preview failure that is
            // not permanent: the identical bytes preview cleanly once the
            // missing history arrives. Classifying it as a prevalidation
            // rejection told every caller the delta would never apply, which on
            // the sync path became a terminal reject and lost the write.
            Err(crate::Error::Crdt(nodedb_crdt::CrdtError::PreviewPendingDependencies)) => {
                return self.response_error(
                    task,
                    ErrorCode::RetryableRefusal {
                        reason: format!(
                            "delta for {collection}/{document_id} depends on operations absent \
                             from this collection's document; nothing was applied"
                        ),
                    },
                );
            }
            Err(error) => {
                return self.response_error(
                    task,
                    ErrorCode::RejectedPrevalidation {
                        reason: error.to_string(),
                    },
                );
            }
        };
        let counts = u64::try_from(preview.imported_ops)
            .ok()
            .zip(u64::try_from(preview.trimmed_ops).ok());
        let Some((imported_ops, trimmed_ops)) = counts else {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "CRDT preview operation count exceeds wire range".into(),
                },
            );
        };
        let result = CrdtPreviewResult {
            post_image_msgpack: preview.post_image_msgpack,
            imported_ops,
            frontier_digest,
            trimmed_ops,
        };
        match zerompk::to_msgpack_vec(&result) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(error) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("CRDT preview response encode: {error}"),
                },
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use loro::LoroValue;
    use nodedb_types::sync::wire::SyncProvenance;
    use nodedb_types::{DatabaseId, QualifiedCollection, Surrogate, TenantId, Value};

    use super::*;
    use crate::bridge::envelope::{ErrorCode, Status};
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::data::executor::task::ExecutionTask;

    fn task() -> ExecutionTask {
        crate::data::executor::core_loop::tests::make_default_task()
    }

    fn snapshot_delta(collection: &str, row: &str, value: &str) -> Vec<u8> {
        let source = nodedb_crdt::CrdtState::new(42).expect("source state");
        source
            .upsert(
                collection,
                row,
                &[("value", LoroValue::String(value.into()))],
            )
            .expect("source write");
        source.export_snapshot().expect("source snapshot")
    }

    fn preview_response(
        core: &mut CoreLoop,
        task: &ExecutionTask,
        collection: &str,
        row: &str,
        delta: &[u8],
    ) -> Response {
        core.execute_crdt_preview_apply(task, collection, row, delta)
    }

    #[test]
    fn restore_handler_generates_delta_without_mutating_authoritative_state() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _request_tx, _response_rx) = make_core_with_dir(dir.path());
        let mut task = task();
        let source = nodedb_crdt::CrdtState::new(42).expect("source");
        source
            .upsert("docs", "one", &[("value", LoroValue::String("v1".into()))])
            .expect("v1");
        source
            .upsert("docs", "one", &[("value", LoroValue::String("v2".into()))])
            .expect("v2");
        let snapshot = source.export_snapshot().expect("snapshot");
        let mut tenant = crate::engine::crdt::tenant_state::TenantCrdtEngine::new(
            TenantId::new(1),
            42,
            nodedb_crdt::ConstraintSet::new(),
        )
        .expect("tenant engine");
        tenant
            .import_snapshot_bytes("docs", &snapshot)
            .expect("tenant snapshot");
        let version_json = tenant.version_vector_json("docs").expect("version json");
        assert!(
            tenant
                .preview_restore_to_version("docs", "one", &version_json)
                .expect("tenant preview")
                .is_empty(),
            "sanity: imported snapshot is at its current version"
        );
        let historical_json = {
            let historical_source = nodedb_crdt::CrdtState::new(42).expect("historical source");
            historical_source
                .upsert("docs", "one", &[("value", LoroValue::String("v1".into()))])
                .expect("historical v1");
            let historical_snapshot = historical_source
                .export_snapshot()
                .expect("historical snapshot");
            let mut historical_tenant = crate::engine::crdt::tenant_state::TenantCrdtEngine::new(
                TenantId::new(1),
                42,
                nodedb_crdt::ConstraintSet::new(),
            )
            .expect("historical tenant");
            historical_tenant
                .import_snapshot_bytes("docs", &historical_snapshot)
                .expect("historical import");
            historical_tenant
                .version_vector_json("docs")
                .expect("historical version")
        };
        task.request.wal_lsn = Some(crate::types::Lsn::new(73));
        task.wal_lsn = Some(crate::types::Lsn::new(73));
        task.request.plan = nodedb_physical::physical_plan::PhysicalPlan::Crdt(
            nodedb_physical::physical_plan::CrdtOp::ImportSnapshot {
                tenant_id: 1,
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
                bytes: snapshot.clone(),
            },
        );
        let import = core.execute_crdt_import_snapshot(&task, 1, "docs", &snapshot);
        assert_eq!(import.status, Status::Ok);
        assert_eq!(import.read_version_lsn, crate::types::Lsn::new(73));
        assert_eq!(import.watermark_lsn, crate::types::Lsn::new(73));
        let before = core
            .execute_crdt_read(&task, "docs", "one")
            .payload
            .to_vec();
        let dirty_before = core.checkpoint_coordinator.total_dirty_pages();
        let hwm_before = core.sync_hwm.clone();

        let restore = core.execute_crdt_restore(&task, "docs", "one", &historical_json);
        assert_eq!(restore.status, Status::Ok);
        assert!(
            !restore.payload.is_empty(),
            "restore must produce a forward delta"
        );
        assert_eq!(
            core.execute_crdt_read(&task, "docs", "one")
                .payload
                .to_vec(),
            before
        );
        assert_eq!(
            core.checkpoint_coordinator.total_dirty_pages(),
            dirty_before
        );
        assert_eq!(core.sync_hwm, hwm_before);
    }

    #[test]
    fn preview_missing_collection_returns_typed_post_image_without_residue() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _request_tx, _response_rx) = make_core_with_dir(dir.path());
        let task = task();
        let delta = snapshot_delta("docs", "one", "value");
        let dirty_before = core.checkpoint_coordinator.total_dirty_pages();

        let response = preview_response(&mut core, &task, "docs", "one", &delta);
        assert_eq!(response.status, Status::Ok);
        let result: CrdtPreviewResult =
            zerompk::from_msgpack(response.payload.as_bytes()).expect("typed preview response");
        let post_image: Option<Value> =
            zerompk::from_msgpack(&result.post_image_msgpack).expect("typed post image");
        let Some(Value::Object(fields)) = post_image else {
            panic!("preview must return the target object");
        };
        assert_eq!(fields.get("value"), Some(&Value::String("value".into())));
        assert!(result.imported_ops > 0);
        assert!(
            core.crdt_engines.is_empty(),
            "preview must not create an engine"
        );
        assert!(core.sync_hwm.is_empty(), "preview must not advance HWM");
        assert_eq!(
            core.checkpoint_coordinator.total_dirty_pages(),
            dirty_before
        );
        assert_eq!(
            core.sparse
                .get(
                    DatabaseId::DEFAULT.as_u64(),
                    1,
                    "docs",
                    &nodedb_types::StorageKey::for_surrogate(nodedb_types::Surrogate::new(1)),
                )
                .expect("sparse read"),
            None,
            "preview must not materialize sparse state"
        );
    }

    #[test]
    fn preview_rejections_leave_no_engine_or_checkpoint_residue() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _request_tx, _response_rx) = make_core_with_dir(dir.path());
        let task = task();
        let valid = snapshot_delta("docs", "one", "value");
        let cases = [
            ("malformed", b"not-loro".as_slice(), "docs", "one"),
            ("wrong target", valid.as_slice(), "docs", "other"),
        ];

        for (name, delta, collection, row) in cases {
            let response = preview_response(&mut core, &task, collection, row, delta);
            assert_eq!(response.status, Status::Error, "{name}");
            assert!(matches!(
                response.error_code.as_deref(),
                Some(ErrorCode::RejectedPrevalidation { .. })
            ));
            assert!(core.crdt_engines.is_empty(), "{name} created an engine");
            assert!(core.sync_hwm.is_empty(), "{name} changed HWM");
            assert_eq!(
                core.checkpoint_coordinator.total_dirty_pages(),
                0,
                "{name} dirtied checkpoint"
            );
        }

        let over_byte = vec![0; nodedb_crdt::CrdtDeltaPreviewLimits::default().max_delta_bytes + 1];
        let response = preview_response(&mut core, &task, "docs", "one", &over_byte);
        assert_eq!(response.status, Status::Error);
        assert!(core.crdt_engines.is_empty());
        assert!(core.sync_hwm.is_empty());
        assert_eq!(core.checkpoint_coordinator.total_dirty_pages(), 0);

        let source = nodedb_crdt::CrdtState::new(43).expect("source state");
        source
            .upsert(
                "docs",
                "one",
                &[("value", LoroValue::String("base".into()))],
            )
            .expect("base write");
        let _snapshot = source.export_snapshot().expect("commit base state");
        let base_version = source.oplog_version_vector();
        source
            .set_fields(
                "docs",
                "one",
                &[("value", LoroValue::String("next".into()))],
            )
            .expect("dependent update");
        let pending = source
            .export_updates_since(&base_version)
            .expect("incremental delta");
        let response = preview_response(&mut core, &task, "docs", "one", &pending);
        assert_eq!(response.status, Status::Error, "pending dependency");
        assert!(core.crdt_engines.is_empty());
        assert!(core.sync_hwm.is_empty());
        assert_eq!(core.checkpoint_coordinator.total_dirty_pages(), 0);

        let over_op_source = nodedb_crdt::CrdtState::new(44).expect("over-op source");
        for index in 0..=nodedb_crdt::CrdtDeltaPreviewLimits::default().max_imported_ops {
            let field = format!("f{index}");
            over_op_source
                .set_fields(
                    "docs",
                    "one",
                    &[(field.as_str(), LoroValue::I64(index as i64))],
                )
                .expect("source operation");
        }
        let over_op = over_op_source.export_snapshot().expect("over-op snapshot");
        let response = preview_response(&mut core, &task, "docs", "one", &over_op);
        assert_eq!(response.status, Status::Error, "over operation limit");
        assert!(core.crdt_engines.is_empty());
        assert!(core.sync_hwm.is_empty());
        assert_eq!(core.checkpoint_coordinator.total_dirty_pages(), 0);
    }

    #[test]
    fn stale_sync_frontier_does_not_consume_a_newer_producer_epoch() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _request_tx, _response_rx) = make_core_with_dir(dir.path());
        let task = task();
        let delta = snapshot_delta("docs", "one", "value");
        let provenance = SyncProvenance {
            producer_id: 900,
            epoch: 7,
            stream_id: 11,
            seq: 1,
        };
        let response = core.execute_crdt_apply(
            &task,
            super::super::crdt_apply::CrdtApplyParams {
                collection: "docs",
                document_id: "one",
                delta: &delta,
                surrogate: Surrogate::new(77),
                peer_id: 42,
                provenance: Some(&provenance),
                constraint_version_required: 1,
                expected_frontier_digest: Some([0xFF; 32]),
                auth_user_id: 0,
                auth_device_id: 0,
                auth_seq_no: 0,
                delta_signature: [0; 32],
                signing_required: false,
            },
        );

        assert_eq!(response.status, Status::Error);
        assert!(matches!(
            response.error_code.as_deref(),
            Some(ErrorCode::CrdtFrontierMismatch { .. })
        ));
        assert!(core.crdt_engines.is_empty());
        assert!(core.producer_epoch_floor.is_empty());
        assert!(core.sync_hwm.is_empty());
        assert_eq!(core.checkpoint_coordinator.total_dirty_pages(), 0);
        assert_eq!(
            core.sparse
                .get(
                    DatabaseId::DEFAULT.as_u64(),
                    1,
                    "docs",
                    &nodedb_types::StorageKey::for_surrogate(nodedb_types::Surrogate::new(0x4d)),
                )
                .expect("sparse read"),
            None
        );
    }

    #[test]
    fn matching_frontier_applies_and_stale_frontier_is_side_effect_free() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _request_tx, _response_rx) = make_core_with_dir(dir.path());
        let mut task = task();
        task.request.wal_lsn = Some(crate::types::Lsn::new(91));
        task.wal_lsn = Some(crate::types::Lsn::new(91));
        let delta = snapshot_delta("docs", "one", "value");
        let preview = preview_response(&mut core, &task, "docs", "one", &delta);
        let preview: CrdtPreviewResult =
            zerompk::from_msgpack(preview.payload.as_bytes()).expect("preview result");
        let surrogate = Surrogate::new(77);
        task.request.plan = nodedb_physical::physical_plan::PhysicalPlan::Crdt(
            nodedb_physical::physical_plan::CrdtOp::Apply {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
                document_id: "one".into(),
                delta: delta.clone(),
                peer_id: 42,
                mutation_id: 1,
                surrogate,
                provenance: None,
                constraint_version_required: 0,
                expected_frontier_digest: Some(preview.frontier_digest),
            },
        );

        let applied = core.execute_crdt_apply(
            &task,
            super::super::crdt_apply::CrdtApplyParams {
                collection: "docs",
                document_id: "one",
                delta: &delta,
                surrogate,
                peer_id: 42,
                provenance: None,
                constraint_version_required: 0,
                expected_frontier_digest: Some(preview.frontier_digest),
                auth_user_id: 0,
                auth_device_id: 0,
                auth_seq_no: 0,
                delta_signature: [0; 32],
                signing_required: false,
            },
        );
        assert_eq!(applied.status, Status::Ok);
        assert_eq!(applied.read_version_lsn, crate::types::Lsn::new(91));
        assert_eq!(applied.watermark_lsn, crate::types::Lsn::new(91));
        assert!(
            core.crdt_engines
                .get(&(DatabaseId::DEFAULT, TenantId::new(1)))
                .and_then(|engine| engine.read_row("docs", "one"))
                .is_some()
        );
        let doc_id = nodedb_types::StorageKey::for_surrogate(surrogate);
        assert!(
            core.sparse
                .get(DatabaseId::DEFAULT.as_u64(), 1, "docs", &doc_id)
                .expect("sparse read")
                .is_some()
        );

        let dirty_before = core.checkpoint_coordinator.total_dirty_pages();
        let hwm_before = core.sync_hwm.clone();
        let sparse_before = core
            .sparse
            .get(DatabaseId::DEFAULT.as_u64(), 1, "docs", &doc_id)
            .expect("sparse read");
        let stale = core.execute_crdt_apply(
            &task,
            super::super::crdt_apply::CrdtApplyParams {
                collection: "docs",
                document_id: "one",
                delta: &delta,
                surrogate,
                peer_id: 42,
                provenance: None,
                constraint_version_required: 0,
                expected_frontier_digest: Some(preview.frontier_digest),
                auth_user_id: 0,
                auth_device_id: 0,
                auth_seq_no: 0,
                delta_signature: [0; 32],
                signing_required: false,
            },
        );
        assert_eq!(stale.status, Status::Error);
        assert!(matches!(
            stale.error_code.as_deref(),
            Some(ErrorCode::CrdtFrontierMismatch { .. })
        ));
        assert_eq!(
            core.checkpoint_coordinator.total_dirty_pages(),
            dirty_before
        );
        assert_eq!(core.sync_hwm, hwm_before);
        assert_eq!(
            core.sparse
                .get(DatabaseId::DEFAULT.as_u64(), 1, "docs", &doc_id)
                .expect("sparse read"),
            sparse_before
        );

        let legacy = core.execute_crdt_apply(
            &task,
            super::super::crdt_apply::CrdtApplyParams {
                collection: "docs",
                document_id: "one",
                delta: &delta,
                surrogate,
                peer_id: 42,
                provenance: None,
                constraint_version_required: 0,
                expected_frontier_digest: None,
                auth_user_id: 0,
                auth_device_id: 0,
                auth_seq_no: 0,
                delta_signature: [0; 32],
                signing_required: false,
            },
        );
        assert_eq!(
            legacy.status,
            Status::Ok,
            "legacy unfenced apply remains supported"
        );
    }
}
