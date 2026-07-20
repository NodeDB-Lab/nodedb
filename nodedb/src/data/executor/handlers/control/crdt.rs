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

    /// Restore a CRDT document to a historical version.
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
        match engine.restore_to_version(collection, document_id, target_version_json) {
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
        match engine.import_snapshot_bytes(collection, bytes) {
            Ok(()) => {
                self.checkpoint_coordinator.mark_dirty("crdt", 1);
                self.response_ok(task)
            }
            Err(e) => {
                warn!(core = self.core_id, error = %e, "crdt import snapshot failed");
                self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
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
