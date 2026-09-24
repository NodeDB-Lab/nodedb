// SPDX-License-Identifier: BUSL-1.1

//! Panic-safe CRDT gate for transaction-batch apply.

use std::panic::{AssertUnwindSafe, catch_unwind};

use tracing::error;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::data::panic_payload::panic_payload_to_string;
use crate::types::TenantId;

use super::batch::CrdtDelta;
use super::undo::UndoEntry;

/// A CRDT collection's state before a transaction starts importing its deltas.
struct CrdtCollectionPreimage {
    collection: String,
    snapshot: Option<Vec<u8>>,
}

/// Complete CRDT state required to restore a transaction's pre-image.
///
/// Keeping the rollback scope together prevents callers from pairing a
/// pre-image with the wrong tenant or database during failure handling.
struct CrdtRollbackScope {
    database_id: crate::types::DatabaseId,
    tenant_id: TenantId,
    engine_existed_before: bool,
    preimages: Vec<CrdtCollectionPreimage>,
}

/// One failed collection replacement during CRDT transaction rollback.
///
/// The original engine error is converted at the boundary so every failed
/// collection can be reported together rather than aborting restoration at the
/// first failure.
#[derive(Debug)]
struct CrdtCollectionRestoreFailure {
    collection: String,
    detail: String,
}

/// Typed aggregate of every CRDT collection that could not be restored.
///
/// A rollback failure is fatal, but restoration must still be attempted for
/// all pre-images so the error reports the complete shard-state risk.
#[derive(Debug)]
struct CrdtRestoreFailures {
    failures: Vec<CrdtCollectionRestoreFailure>,
}

impl std::fmt::Display for CrdtRestoreFailures {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (index, failure) in self.failures.iter().enumerate() {
            if index > 0 {
                formatter.write_str("; ")?;
            }
            write!(
                formatter,
                "collection {} could not be restored from its pre-image: {}",
                failure.collection, failure.detail
            )?;
        }
        Ok(())
    }
}

impl CoreLoop {
    /// Roll back an uncommitted batch failure and surface accounting/restore
    /// mismatches as `RollbackFailed` rather than reporting a false abort.
    pub(super) fn rollback_transaction_failure(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        undo_log: Vec<UndoEntry>,
        mut response: Response,
    ) -> Response {
        let undo_len = undo_log.len();
        let rollback = catch_unwind(AssertUnwindSafe(|| {
            self.rollback_undo_log(task.request.database_id.as_u64(), tid, undo_log)
        }));
        let failure = match rollback {
            Ok(Ok(())) => None,
            Ok(Err((entry_index, detail))) => Some((entry_index, detail)),
            Err(payload) => Some((
                undo_len,
                format!(
                    "panic during transaction rollback: {}",
                    panic_payload_to_string(payload.as_ref())
                ),
            )),
        };
        if let Some((entry_index, detail)) = failure {
            error!(
                core = self.core_id,
                entry_index,
                detail = %detail,
                "transaction rollback failed after a gate error or panic; shard state unknown"
            );
            response.error_code = Some(Box::new(ErrorCode::RollbackFailed {
                entry_index,
                detail,
            }));
        }
        response
    }

    /// Apply buffered CRDT deltas and roll back every prior forward write on a
    /// CRDT error or panic. CRDT imports are also restored exactly: Loro import
    /// merges, so an already imported delta must be replaced from a snapshot,
    /// not merely imported again.
    pub(super) fn apply_crdt_deltas_or_rollback(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        undo_log: Vec<UndoEntry>,
        crdt_deltas: Vec<CrdtDelta>,
    ) -> Result<Vec<UndoEntry>, Response> {
        let database_id = task.request.database_id;
        let tenant_id = TenantId::new(tid);
        let engine_key = (database_id, tenant_id);
        let engine_existed_before = self.crdt_engines.contains_key(&engine_key);
        let preimages = match catch_unwind(AssertUnwindSafe(|| {
            self.capture_crdt_preimages(task, tenant_id, &crdt_deltas)
        })) {
            Ok(Ok(preimages)) => preimages,
            Ok(Err(response)) => {
                if !engine_existed_before {
                    self.crdt_engines.remove(&engine_key);
                }
                return Err(self.rollback_transaction_failure(task, tid, undo_log, response));
            }
            Err(payload) => {
                if !engine_existed_before {
                    self.crdt_engines.remove(&engine_key);
                }
                let response = self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!(
                            "panic while capturing CRDT transaction pre-images: {}",
                            panic_payload_to_string(payload.as_ref())
                        ),
                    },
                );
                return Err(self.rollback_transaction_failure(task, tid, undo_log, response));
            }
        };

        let response = match catch_unwind(AssertUnwindSafe(|| {
            self.apply_crdt_deltas(task, tid, crdt_deltas)
        })) {
            Ok(None) => return Ok(undo_log),
            Ok(Some(response)) => response,
            Err(payload) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!(
                        "panic during CRDT transaction gate: {}",
                        panic_payload_to_string(payload.as_ref())
                    ),
                },
            ),
        };

        Err(self.rollback_crdt_transaction_failure(
            task,
            tid,
            undo_log,
            response,
            CrdtRollbackScope {
                database_id,
                tenant_id,
                engine_existed_before,
                preimages,
            },
        ))
    }

    fn capture_crdt_preimages(
        &self,
        task: &ExecutionTask,
        tenant_id: TenantId,
        crdt_deltas: &[CrdtDelta],
    ) -> Result<Vec<CrdtCollectionPreimage>, Response> {
        // Capture must be read-only. In particular, do not use
        // `get_crdt_engine` here: creating an empty engine before every
        // pre-commit gate would itself be an avoidable transactional side
        // effect. The apply path creates it only after capture succeeds.
        let engine = self
            .crdt_engines
            .get(&(task.request.database_id, tenant_id));
        let mut seen = std::collections::HashSet::with_capacity(crdt_deltas.len());
        let mut preimages = Vec::with_capacity(crdt_deltas.len());
        for (_, _, collection) in crdt_deltas {
            if !seen.insert(collection.as_str()) {
                continue;
            }
            let snapshot = match engine
                .map(|engine| engine.export_snapshot_bytes(collection))
                .transpose()
            {
                Ok(snapshot) => snapshot.flatten(),
                Err(error) => {
                    return Err(self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!(
                                "CRDT pre-image export failed for collection {collection}: {error}"
                            ),
                        },
                    ));
                }
            };
            preimages.push(CrdtCollectionPreimage {
                collection: collection.clone(),
                snapshot,
            });
        }
        Ok(preimages)
    }

    fn rollback_crdt_transaction_failure(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        undo_log: Vec<UndoEntry>,
        response: Response,
        rollback_scope: CrdtRollbackScope,
    ) -> Response {
        let crdt_restore = match catch_unwind(AssertUnwindSafe(|| {
            self.restore_crdt_preimages(rollback_scope)
        })) {
            Ok(result) => result,
            Err(payload) => Err(CrdtRestoreFailures {
                failures: vec![CrdtCollectionRestoreFailure {
                    collection: "<transaction-rollback>".into(),
                    detail: format!(
                        "panic while restoring CRDT pre-images: {}",
                        panic_payload_to_string(payload.as_ref())
                    ),
                }],
            }),
        };
        let mut response = self.rollback_transaction_failure(task, tid, undo_log, response);
        if let Err(restore_failures) = crdt_restore {
            error!(
                core = self.core_id,
                detail = %restore_failures,
                "CRDT rollback restore failed; shard state unknown"
            );
            let (entry_index, detail) = match response.error_code.as_deref() {
                Some(ErrorCode::RollbackFailed {
                    entry_index,
                    detail,
                }) => (
                    *entry_index,
                    format!(
                        "forward undo rollback failed at entry {entry_index}: {detail}; \
                         CRDT pre-image restoration also failed: {restore_failures}"
                    ),
                ),
                _ => (
                    0,
                    format!("CRDT pre-image restoration failed: {restore_failures}"),
                ),
            };
            response.error_code = Some(Box::new(ErrorCode::RollbackFailed {
                entry_index,
                detail,
            }));
        }
        response
    }

    fn restore_crdt_preimages(
        &mut self,
        rollback_scope: CrdtRollbackScope,
    ) -> Result<(), CrdtRestoreFailures> {
        let CrdtRollbackScope {
            database_id,
            tenant_id,
            engine_existed_before,
            preimages,
        } = rollback_scope;
        let engine_key = (database_id, tenant_id);
        if !engine_existed_before {
            self.crdt_engines.remove(&engine_key);
            return Ok(());
        }
        let Some(engine) = self.crdt_engines.get_mut(&engine_key) else {
            return Err(CrdtRestoreFailures {
                failures: vec![CrdtCollectionRestoreFailure {
                    collection: "<tenant-engine>".into(),
                    detail: "CRDT engine disappeared while rolling back a transaction".into(),
                }],
            });
        };

        let mut failures = Vec::new();
        for CrdtCollectionPreimage {
            collection,
            snapshot,
        } in preimages
        {
            // A single malformed/corrupt pre-image must not keep later
            // collections from being restored. The outer gate also catches
            // panics, but only this per-collection boundary preserves the
            // aggregate failure contract on a panic.
            match catch_unwind(AssertUnwindSafe(|| {
                engine.restore_collection_snapshot(&collection, snapshot.as_deref())
            })) {
                Ok(Ok(())) => {}
                Ok(Err(error)) => failures.push(CrdtCollectionRestoreFailure {
                    collection,
                    detail: error.to_string(),
                }),
                Err(payload) => failures.push(CrdtCollectionRestoreFailure {
                    collection,
                    detail: format!(
                        "panic while restoring CRDT pre-image: {}",
                        panic_payload_to_string(payload.as_ref())
                    ),
                }),
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(CrdtRestoreFailures { failures })
        }
    }
}

#[cfg(test)]
mod tests {
    use loro::LoroValue;
    use nodedb_crdt::state::CrdtState;

    use crate::bridge::envelope::{PhysicalPlan, Status};
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};
    use crate::types::{DatabaseId, TenantId};
    use nodedb_physical::physical_plan::{CrdtOp, TimeseriesOp};
    use nodedb_types::QualifiedCollection;

    fn row_delta(peer: u64, row_id: &str) -> Vec<u8> {
        let state = CrdtState::new(peer).expect("CRDT state");
        state
            .upsert(
                "crdt",
                row_id,
                &[("value", LoroValue::String(row_id.into()))],
            )
            .expect("row upsert");
        state.export_snapshot().expect("delta snapshot")
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn crdt_gate_panic_restores_deferred_timeseries_preimage() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let task = make_default_task();
        let plans = [
            PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "metrics"),
                payload: b"metrics value=1i 1000000000\n".to_vec(),
                format: "ilp".into(),
                wal_lsn: None,
                surrogates: Vec::new(),
                provenance: None,
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
                returning: None,
                rls_filters: Vec::new(),
            }),
            PhysicalPlan::Crdt(CrdtOp::Apply {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "crdt"),
                document_id: "row".into(),
                delta: Vec::new(),
                peer_id: 1,
                mutation_id: 1,
                surrogate: nodedb_types::Surrogate::ZERO,
                provenance: None,
                constraint_version_required: 0,
                expected_frontier_digest: None,
            }),
        ];
        let _fail = crate::fail_point::FailGuard::install(
            "transaction_batch::between_crdt_delta",
            crate::fail_point::FailAction::Panic,
        );

        let response = core.execute_transaction_batch(&task, 1, &plans, &[], None);

        assert_eq!(response.status, Status::Error);
        assert!(
            !core.columnar_memtables.contains_key(&(
                crate::types::DatabaseId::DEFAULT,
                TenantId::new(1),
                "metrics".to_string(),
            )),
            "a CRDT-gate panic must remove the deferred timeseries collection"
        );
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn panic_between_subplans_rolls_back_the_first_timeseries_ingest_completely() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let task = make_default_task();
        let tenant_id = task.request.tenant_id;
        let key = (task.request.database_id, tenant_id, "metrics".to_string());
        let plans = [
            PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "metrics"),
                payload: b"metrics value=1i 1000000000\\n".to_vec(),
                format: "ilp".into(),
                wal_lsn: None,
                surrogates: Vec::new(),
                provenance: None,
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
                returning: None,
                rls_filters: Vec::new(),
            }),
            PhysicalPlan::Crdt(CrdtOp::Apply {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "crdt"),
                document_id: "after-timeseries".into(),
                delta: Vec::new(),
                peer_id: 1,
                mutation_id: 1,
                surrogate: nodedb_types::Surrogate::ZERO,
                provenance: None,
                constraint_version_required: 0,
                expected_frontier_digest: None,
            }),
        ];
        let _fail = crate::fail_point::FailGuard::install(
            "transaction_batch::between_subapply",
            crate::fail_point::FailAction::Panic,
        );

        let response = core.execute_transaction_batch(&task, tenant_id.as_u64(), &plans, &[], None);

        assert_eq!(response.status, Status::Error);
        assert!(!core.columnar_memtables.contains_key(&key));
        assert!(!core.ts_last_value_caches.contains_key(&key));
        assert!(!core.ts_max_ingested_lsn.contains_key(&key));
        assert!(!core.ts_registries.contains_key(&key));
        assert!(!core.columnar_flushed_surrogates.contains_key(&key));
        assert!(core.last_ts_ingest.is_none());
    }

    #[test]
    fn crdt_error_restores_a_previously_applied_delta() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let task = make_default_task();
        let tenant_id = task.request.tenant_id;
        let original = row_delta(1, "before");
        core.get_crdt_engine(task.request.database_id, tenant_id)
            .expect("CRDT engine")
            .apply_committed_delta("crdt", &original)
            .expect("seed CRDT state");

        let result = core.apply_crdt_deltas_or_rollback(
            &task,
            tenant_id.as_u64(),
            Vec::new(),
            vec![
                (row_delta(2, "during"), 2, "crdt".to_string()),
                (b"not a valid Loro delta".to_vec(), 3, "crdt".to_string()),
            ],
        );

        assert!(
            result.is_err(),
            "the invalid second delta must fail the gate"
        );
        let engine = core
            .get_crdt_engine(task.request.database_id, tenant_id)
            .expect("CRDT engine after rollback");
        assert!(engine.row_exists("crdt", "before"));
        assert!(!engine.row_exists("crdt", "during"));
    }

    #[test]
    fn failed_batch_removes_crdt_engine_created_for_earlier_delta() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let task = make_default_task();
        let plans = [
            PhysicalPlan::Crdt(CrdtOp::Apply {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "crdt"),
                document_id: "during".into(),
                delta: row_delta(2, "during"),
                peer_id: 2,
                mutation_id: 2,
                surrogate: nodedb_types::Surrogate::ZERO,
                provenance: None,
                constraint_version_required: 0,
                expected_frontier_digest: None,
            }),
            PhysicalPlan::Crdt(CrdtOp::Apply {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "crdt"),
                document_id: "bad".into(),
                delta: b"not a valid Loro delta".to_vec(),
                peer_id: 3,
                mutation_id: 3,
                surrogate: nodedb_types::Surrogate::ZERO,
                provenance: None,
                constraint_version_required: 0,
                expected_frontier_digest: None,
            }),
        ];

        let response = core.execute_transaction_batch(&task, 1, &plans, &[], None);

        assert_eq!(response.status, Status::Error);
        assert!(
            !core
                .crdt_engines
                .contains_key(&(crate::types::DatabaseId::DEFAULT, TenantId::new(1),)),
            "an aborted first CRDT batch must restore the absent-engine pre-image"
        );
    }

    #[test]
    fn batch_crdt_error_restores_earlier_delta_and_forward_timeseries_write() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let task = make_default_task();
        let tenant_id = task.request.tenant_id;
        let original = row_delta(1, "before");
        core.get_crdt_engine(task.request.database_id, tenant_id)
            .expect("CRDT engine")
            .apply_committed_delta("crdt", &original)
            .expect("seed CRDT state");
        let plans = [
            PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "metrics"),
                payload: b"metrics value=1i 1000000000\n".to_vec(),
                format: "ilp".into(),
                wal_lsn: None,
                surrogates: Vec::new(),
                provenance: None,
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
                returning: None,
                rls_filters: Vec::new(),
            }),
            PhysicalPlan::Crdt(CrdtOp::Apply {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "crdt"),
                document_id: "during".into(),
                delta: row_delta(2, "during"),
                peer_id: 2,
                mutation_id: 2,
                surrogate: nodedb_types::Surrogate::ZERO,
                provenance: None,
                constraint_version_required: 0,
                expected_frontier_digest: None,
            }),
            PhysicalPlan::Crdt(CrdtOp::Apply {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "crdt"),
                document_id: "bad".into(),
                delta: b"not a valid Loro delta".to_vec(),
                peer_id: 3,
                mutation_id: 3,
                surrogate: nodedb_types::Surrogate::ZERO,
                provenance: None,
                constraint_version_required: 0,
                expected_frontier_digest: None,
            }),
        ];

        let response = core.execute_transaction_batch(&task, tenant_id.as_u64(), &plans, &[], None);

        assert_eq!(response.status, Status::Error);
        assert!(
            !core.columnar_memtables.contains_key(&(
                crate::types::DatabaseId::DEFAULT,
                tenant_id,
                "metrics".to_string(),
            )),
            "the forward timeseries write must roll back with a later CRDT failure"
        );
        let engine = core
            .get_crdt_engine(task.request.database_id, tenant_id)
            .expect("CRDT engine after rollback");
        assert!(engine.row_exists("crdt", "before"));
        assert!(!engine.row_exists("crdt", "during"));
    }

    #[test]
    fn restore_attempts_every_preimage_after_an_earlier_failure() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let task = make_default_task();
        let tenant_id = task.request.tenant_id;
        core.get_crdt_engine(task.request.database_id, tenant_id)
            .expect("CRDT engine")
            .apply_committed_delta("removed", &row_delta(1, "before"))
            .expect("seed CRDT state");

        let result = core.restore_crdt_preimages(super::CrdtRollbackScope {
            database_id: task.request.database_id,
            tenant_id,
            engine_existed_before: true,
            preimages: vec![
                super::CrdtCollectionPreimage {
                    collection: "broken".into(),
                    snapshot: Some(b"not a Loro snapshot".to_vec()),
                },
                super::CrdtCollectionPreimage {
                    collection: "removed".into(),
                    snapshot: None,
                },
            ],
        });

        let failures = result.expect_err("an invalid snapshot must fail restore");
        assert_eq!(failures.failures.len(), 1);
        assert_eq!(failures.failures[0].collection, "broken");
        assert!(
            core.get_crdt_engine(task.request.database_id, tenant_id)
                .expect("CRDT engine")
                .export_snapshot_bytes("removed")
                .expect("snapshot export")
                .is_none(),
            "a later preimage must still be restored after an earlier failure"
        );
    }

    #[cfg(feature = "failpoints")]
    #[test]
    fn crdt_panic_after_import_restores_the_exact_preimage() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let task = make_default_task();
        let tenant_id = task.request.tenant_id;
        let original = row_delta(1, "before");
        core.get_crdt_engine(task.request.database_id, tenant_id)
            .expect("CRDT engine")
            .apply_committed_delta("crdt", &original)
            .expect("seed CRDT state");
        let _fail = crate::fail_point::FailGuard::install(
            "transaction_batch::after_crdt_delta",
            crate::fail_point::FailAction::Panic,
        );

        let result = core.apply_crdt_deltas_or_rollback(
            &task,
            tenant_id.as_u64(),
            Vec::new(),
            vec![(row_delta(2, "during"), 2, "crdt".to_string())],
        );

        assert!(result.is_err(), "the post-import panic must fail the gate");
        let engine = core
            .get_crdt_engine(task.request.database_id, tenant_id)
            .expect("CRDT engine after rollback");
        assert!(engine.row_exists("crdt", "before"));
        assert!(!engine.row_exists("crdt", "during"));
    }
}
