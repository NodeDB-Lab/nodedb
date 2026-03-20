//! Control operation handlers: WalAppend, Cancel, SetCollectionPolicy,
//! RangeScan, CrdtRead, CrdtApply.

use tracing::{debug, warn};

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
        let engine = self.get_crdt_engine(tenant_id);
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
        let engine = self.get_crdt_engine(tenant_id);
        match engine.read_snapshot(collection, document_id) {
            Some(snapshot) => self.response_with_payload(task, snapshot),
            None => self.response_error(task, ErrorCode::NotFound),
        }
    }

    pub(in crate::data::executor) fn execute_crdt_apply(
        &mut self,
        task: &ExecutionTask,
        delta: &[u8],
    ) -> Response {
        let tenant_id = task.request.tenant_id;
        let engine = self.get_crdt_engine(tenant_id);
        match engine.apply_committed_delta(delta) {
            Ok(()) => self.response_ok(task),
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
