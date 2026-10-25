// SPDX-License-Identifier: BUSL-1.1

//! Dispatch for `DocumentOp::Register`, the DDL op that installs a document
//! collection's config on this core.

use crate::bridge::envelope::{ErrorCode, Response};
use nodedb_physical::physical_plan::DocumentOp;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::document::write::RegisterDocumentCollectionParams;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Install the collection config a `Register` op carries.
    pub(super) fn dispatch_register(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        op: &DocumentOp,
    ) -> Response {
        let DocumentOp::Register {
            collection,
            indexes,
            crdt_enabled,
            storage_mode,
            enforcement,
            bitemporal,
            conflict_policy,
            timeseries,
            vector_primary,
            vector_fields,
            declared_columns,
            declared_key,
            incarnation,
        } = op
        else {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "dispatch_register: plan is not Register".into(),
                },
            );
        };
        self.execute_register_document_collection(
            task,
            RegisterDocumentCollectionParams {
                tid,
                collection: collection.as_str(),
                indexes,
                crdt_enabled: *crdt_enabled,
                storage_mode,
                enforcement,
                bitemporal: *bitemporal,
                conflict_policy: conflict_policy.as_deref(),
                timeseries: timeseries.as_deref(),
                vector_primary: vector_primary.as_deref(),
                vector_fields,
                declared_columns,
                declared_key: declared_key.as_deref(),
                incarnation: *incarnation,
            },
        )
    }
}
