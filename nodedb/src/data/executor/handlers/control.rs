//! Control operation handlers: WalAppend, Cancel, SetCollectionPolicy,
//! RangeScan, CrdtRead, CrdtApply, Checkpoint.

use tracing::{debug, info, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::types::RequestId;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    pub(in crate::data::executor) fn execute_wal_append(
        &self,
        task: &ExecutionTask,
        payload: &[u8],
    ) -> Response {
        debug!(core = self.core_id, len = payload.len(), "wal append");
        self.response_ok(task)
    }

    pub(in crate::data::executor) fn execute_cancel(
        &mut self,
        task: &ExecutionTask,
        target_request_id: RequestId,
    ) -> Response {
        debug!(core = self.core_id, %target_request_id, "cancel");
        if let Some(pos) = self
            .task_queue
            .iter()
            .position(|t| t.request_id() == target_request_id)
        {
            self.task_queue.remove(pos);
        }
        self.response_ok(task)
    }

    pub(in crate::data::executor) fn execute_set_collection_policy(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        policy_json: &str,
    ) -> Response {
        debug!(core = self.core_id, %collection, "set collection policy");
        let tenant_id = task.request.tenant_id;
        let engine = match self.get_crdt_engine(tenant_id) {
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
        match engine.set_collection_policy(collection, policy_json) {
            Ok(()) => self.response_ok(task),
            Err(e) => {
                warn!(core = self.core_id, error = %e, "set collection policy failed");
                self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                )
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_range_scan(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        field: &str,
        lower: Option<&[u8]>,
        upper: Option<&[u8]>,
        limit: usize,
    ) -> Response {
        debug!(core = self.core_id, %collection, %field, limit, "range scan");
        match self
            .sparse
            .range_scan(tid, collection, field, lower, upper, limit)
        {
            Ok(results) => match super::super::response_codec::encode(&results) {
                Ok(payload) => self.response_with_payload(task, payload),
                Err(e) => {
                    warn!(core = self.core_id, error = %e, "range scan serialization failed");
                    self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    )
                }
            },
            Err(e) => {
                warn!(core = self.core_id, error = %e, "sparse range scan failed");
                self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                )
            }
        }
    }

    pub(in crate::data::executor) fn execute_crdt_read(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        document_id: &str,
    ) -> Response {
        debug!(core = self.core_id, %collection, %document_id, "crdt read");
        let tenant_id = task.request.tenant_id;
        let engine = match self.get_crdt_engine(tenant_id) {
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

    /// Execute a snapshot creation request: export all engine state as bytes.
    ///
    /// Returns the serialized `CoreSnapshot` as the response payload.
    /// The Control Plane collects these from all cores and writes to disk.
    pub(in crate::data::executor) fn execute_create_snapshot(
        &self,
        task: &ExecutionTask,
    ) -> Response {
        match self.export_snapshot() {
            Ok(snapshot) => match snapshot.to_bytes() {
                Ok(bytes) => {
                    info!(
                        core = self.core_id,
                        watermark = snapshot.watermark,
                        documents = snapshot.sparse_documents.len(),
                        vectors = snapshot.hnsw_indexes.len(),
                        size_bytes = bytes.len(),
                        "snapshot exported"
                    );
                    self.response_with_payload(task, bytes)
                }
                Err(e) => {
                    warn!(core = self.core_id, error = %e, "snapshot serialization failed");
                    self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    )
                }
            },
            Err(e) => {
                warn!(core = self.core_id, error = %e, "snapshot export failed");
                self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                )
            }
        }
    }

    /// Execute a coordinated checkpoint: flush all engine state to disk
    /// and return this core's checkpoint LSN.
    ///
    /// 1. Checkpoint vector indexes (HNSW segments → disk files).
    /// 2. Export CRDT snapshots (Loro docs → disk files).
    /// 3. redb sparse engine is already ACID — no action needed.
    /// 4. CSR is rebuilt from redb edge store on startup — no action needed.
    /// 5. Return the core's watermark LSN as the checkpoint point.
    pub(in crate::data::executor) fn execute_checkpoint(
        &mut self,
        task: &ExecutionTask,
    ) -> Response {
        let checkpoint_lsn = self.watermark.as_u64();

        // 1. Flush vector indexes to disk.
        let vectors_checkpointed = self.checkpoint_vector_indexes();

        // 2. Flush CRDT snapshots to disk.
        let crdts_checkpointed = self.checkpoint_crdt_engines();

        // 3. Compact CSR write buffers into dense arrays for clean state.
        self.csr.compact();

        // 4. Record completed flushes in the checkpoint coordinator
        //    and advance the checkpoint LSN for WAL truncation safety.
        self.checkpoint_coordinator
            .record_flush("vector", vectors_checkpointed);
        self.checkpoint_coordinator
            .record_flush("crdt", crdts_checkpointed);
        self.checkpoint_coordinator
            .complete_checkpoint(checkpoint_lsn);

        info!(
            core = self.core_id,
            checkpoint_lsn,
            vectors_checkpointed,
            crdts_checkpointed,
            dirty_pages = self.checkpoint_coordinator.total_dirty_pages(),
            "core checkpoint complete"
        );

        // Return the checkpoint LSN as the response payload.
        let payload = checkpoint_lsn.to_le_bytes().to_vec();
        self.response_with_payload(task, payload)
    }

    /// Checkpoint all CRDT tenant engines to disk.
    ///
    /// Each tenant's Loro state is exported as a snapshot and written to
    /// `{data_dir}/crdt-ckpt/tenant-{id}.ckpt` with atomic temp+rename.
    ///
    /// Called from both `control.rs` (explicit checkpoint command) and
    /// `compact.rs` (periodic maintenance via `maybe_run_maintenance`).
    pub(in crate::data::executor) fn checkpoint_crdt_engines(&self) -> usize {
        if self.crdt_engines.is_empty() {
            return 0;
        }

        let ckpt_dir = self.data_dir.join("crdt-ckpt");
        if std::fs::create_dir_all(&ckpt_dir).is_err() {
            warn!(core = self.core_id, "failed to create CRDT checkpoint dir");
            return 0;
        }

        let mut checkpointed = 0;
        for (tenant_id, engine) in &self.crdt_engines {
            match engine.export_snapshot_bytes() {
                Ok(snapshot) => {
                    if snapshot.is_empty() {
                        continue;
                    }
                    let ckpt_path = ckpt_dir.join(format!("tenant-{tenant_id}.ckpt"));
                    let tmp_path = ckpt_dir.join(format!("tenant-{tenant_id}.ckpt.tmp"));
                    if std::fs::write(&tmp_path, &snapshot).is_ok()
                        && std::fs::rename(&tmp_path, &ckpt_path).is_ok()
                    {
                        checkpointed += 1;
                    }
                }
                Err(e) => {
                    warn!(
                        core = self.core_id,
                        tenant = tenant_id.as_u32(),
                        error = %e,
                        "CRDT checkpoint export failed"
                    );
                }
            }
        }

        if checkpointed > 0 {
            info!(
                core = self.core_id,
                checkpointed,
                total = self.crdt_engines.len(),
                "CRDT engines checkpointed"
            );
        }
        checkpointed
    }

    pub(in crate::data::executor) fn execute_crdt_apply(
        &mut self,
        task: &ExecutionTask,
        delta: &[u8],
    ) -> Response {
        let tenant_id = task.request.tenant_id;
        let engine = match self.get_crdt_engine(tenant_id) {
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
        match engine.apply_committed_delta(delta) {
            Ok(()) => {
                self.checkpoint_coordinator.mark_dirty("crdt", 1);
                self.response_ok(task)
            }
            Err(e) => {
                warn!(core = self.core_id, error = %e, "crdt apply failed");
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
