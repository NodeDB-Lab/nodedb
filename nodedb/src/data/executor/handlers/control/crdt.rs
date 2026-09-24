// SPDX-License-Identifier: BUSL-1.1

//! CRDT operation handlers: read, versioned read, version vector, delta export,
//! restore, compact, and import snapshot. The delta-apply handler lives in the
//! sibling `crdt_apply` module.

use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    pub(in crate::data::executor) fn execute_crdt_read(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        document_id: &str,
    ) -> Response {
        debug!(core = self.core_id, %collection, %document_id, "crdt read");
        let tenant_id = task.request.tenant_id;
        let engine = match self.get_crdt_engine(task.request.database_id, tenant_id) {
            Ok(e) => e,
            Err(e) => {
                warn!(core = self.core_id, error = %e, "failed to create CRDT engine");
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        match engine.read_snapshot(collection, document_id) {
            Ok(Some(snapshot)) => self.response_with_payload(task, snapshot),
            Ok(None) => self.response_error(task, ErrorCode::NotFound),
            Err(e) => {
                warn!(core = self.core_id, error = %e, "crdt read snapshot failed");
                self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                )
            }
        }
    }

    /// Read a CRDT document at a historical version.
    pub(in crate::data::executor) fn execute_crdt_read_at_version(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        document_id: &str,
        version_vector_json: &str,
    ) -> Response {
        debug!(core = self.core_id, %collection, %document_id, "crdt read at version");
        let tenant_id = task.request.tenant_id;
        let engine = match self.get_crdt_engine(task.request.database_id, tenant_id) {
            Ok(e) => e,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        match engine.read_at_version_json(collection, document_id, version_vector_json) {
            Ok(Some(json_bytes)) => self.response_with_payload(task, json_bytes),
            Ok(None) => self.response_error(task, ErrorCode::NotFound),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Get the current CRDT version vector.
    pub(in crate::data::executor) fn execute_crdt_get_version_vector(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
    ) -> Response {
        let tenant_id = task.request.tenant_id;
        let engine = match self.get_crdt_engine(task.request.database_id, tenant_id) {
            Ok(e) => e,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        match engine.version_vector_json(collection) {
            Ok(json) => self.response_with_payload(task, json.into_bytes()),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Export CRDT delta from a version to current.
    pub(in crate::data::executor) fn execute_crdt_export_delta(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        from_version_json: &str,
    ) -> Response {
        let tenant_id = task.request.tenant_id;
        let engine = match self.get_crdt_engine(task.request.database_id, tenant_id) {
            Ok(e) => e,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        match engine.export_delta(collection, from_version_json) {
            Ok(delta) => self.response_with_payload(task, delta),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Generate a CRDT forward restore delta from a historical version.
    pub(in crate::data::executor) fn execute_crdt_restore(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        document_id: &str,
        target_version_json: &str,
    ) -> Response {
        debug!(core = self.core_id, %collection, %document_id, "crdt restore");
        let tenant_id = task.request.tenant_id;
        let engine = match self.get_crdt_engine(task.request.database_id, tenant_id) {
            Ok(e) => e,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        match engine.preview_restore_to_version(collection, document_id, target_version_json) {
            Ok(delta) => self.response_with_payload(task, delta),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Compact CRDT history at a specific version.
    pub(in crate::data::executor) fn execute_crdt_compact(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        target_version_json: &str,
    ) -> Response {
        debug!(core = self.core_id, "crdt compact at version");
        let tenant_id = task.request.tenant_id;
        let engine = match self.get_crdt_engine(task.request.database_id, tenant_id) {
            Ok(e) => e,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        match engine.compact_at_version(collection, target_version_json) {
            Ok(()) => self.response_ok(task),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// Import a per-collection Loro snapshot into the tenant CRDT engine.
    ///
    /// The durable RESTORE re-issue path replicates this through Raft so every
    /// replica of the data group lands the same snapshot. `import_snapshot_bytes`
    /// is a monotonic, idempotent, commutative Loro merge, so applying the same
    /// bytes on every replica converges deterministically — there is no sync
    /// idempotency gate and no per-document surrogate to bind.
    pub(in crate::data::executor) fn execute_crdt_import_snapshot(
        &mut self,
        task: &ExecutionTask,
        tenant_id: u64,
        collection: &str,
        bytes: &[u8],
    ) -> Response {
        let tid = crate::types::TenantId::new(tenant_id);
        debug!(core = self.core_id, %tid, %collection, "crdt import snapshot");
        let engine = match self.get_crdt_engine(task.request.database_id, tid) {
            Ok(e) => e,
            Err(e) => {
                warn!(core = self.core_id, error = %e, "failed to create CRDT engine");
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };
        match engine.apply_committed_delta_validated(
            collection,
            bytes,
            nodedb_types::Surrogate::ZERO,
            "",
            0,
        ) {
            crate::engine::crdt::tenant_state::ValidatedApplyOutcome::Clean { .. } => {
                self.checkpoint_coordinator.mark_dirty("crdt", 1);
                self.note_collection_write_lsn(task, collection);
                self.response_ok(task)
            }
            crate::engine::crdt::tenant_state::ValidatedApplyOutcome::Rejected(reason) => {
                warn!(core = self.core_id, %reason, "crdt snapshot rejected by constraints");
                // Nothing applied, so the record is cancelled and replay never
                // reaches this rejection. Its dead-letter entry is stored first.
                let code = match self.store_crdt_dead_letter(
                    task.request.database_id,
                    tid,
                    task.wal_lsn(),
                ) {
                    Ok(()) => crate::data::executor::core_loop::crdt_rejection(
                        collection, "snapshot", &reason,
                    ),
                    Err(error) => ErrorCode::Internal {
                        detail: format!(
                            "CRDT snapshot for {collection} violates {reason}, and its \
                             dead-letter entry could not be stored: {error}"
                        ),
                    },
                };
                self.response_error(task, code)
            }
            crate::engine::crdt::tenant_state::ValidatedApplyOutcome::Malformed => {
                warn!(core = self.core_id, "crdt snapshot import was malformed");
                self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: "malformed CRDT snapshot".into(),
                    },
                )
            }
            crate::engine::crdt::tenant_state::ValidatedApplyOutcome::PendingDependencies => {
                // A snapshot restore must apply in full: operations left
                // causally pending mean the collection was NOT restored, so
                // surface a failure rather than reporting a partial success.
                warn!(
                    core = self.core_id,
                    "crdt snapshot import left operations causally pending"
                );
                self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: "CRDT snapshot import left operations causally pending".into(),
                    },
                )
            }
        }
    }

    /// Read the current HWM for a `(producer_id, stream_id)` pair without
    /// advancing it. Returns `0` when no frame from this producer has been
    /// committed on this stream yet.
    pub(in crate::data::executor) fn sync_hwm_value(
        &self,
        producer_id: u64,
        stream_id: u64,
    ) -> u64 {
        *self.sync_hwm.get(&(producer_id, stream_id)).unwrap_or(&0)
    }
}
