// SPDX-License-Identifier: BUSL-1.1

//! Read-only resolve pass for a governed `GraphOp::EdgeDelete`. Reads the
//! edge's stored property object, decides the policy against it, and writes
//! nothing. Success lets the Control Plane propose the delete with a
//! decided check; a refusal matches the direct delete's error.

use nodedb_physical::physical_plan::GraphOp;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::TenantId;

impl CoreLoop {
    /// Decide the wrapped delete's policy against the edge's stored image.
    pub(in crate::data::executor) fn execute_edge_delete_resolve(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        inner: &GraphOp,
    ) -> Response {
        let GraphOp::EdgeDelete {
            collection,
            src_id,
            label,
            dst_id,
            rls_write_check,
            ..
        } = inner
        else {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "graph resolve pass wraps a plan that is not an edge delete".into(),
                },
            );
        };

        let stored = self
            .edge_store
            .get_edge(
                task.request.database_id.as_u64(),
                TenantId::new(tid),
                collection.as_str(),
                src_id,
                label,
                dst_id,
            )
            .ok()
            .flatten();

        match crate::data::executor::handlers::rls_write_gate::admit_edge_properties(
            rls_write_check,
            stored.as_deref(),
            tid,
            collection.as_str(),
        ) {
            // Same count semantics as `execute_edge_delete`: 1 when a live
            // edge exists to remove, 0 when it is already absent.
            Ok(()) => self.response_affected(task, u64::from(stored.is_some())),
            Err(error) => self.response_error(task, error),
        }
    }
}
