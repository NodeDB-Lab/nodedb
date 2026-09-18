// SPDX-License-Identifier: BUSL-1.1

//! KV dispatch for the cursor `Scan` op. Split from `dispatch.rs` by op
//! family; fills the handler's params from the op's fields exactly as the
//! single match arm did.

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::KvOp;

impl CoreLoop {
    /// Run a `Scan`: cursor-paginated read with optional filters and sort.
    pub(super) fn dispatch_kv_scan(
        &mut self,
        task: &ExecutionTask,
        did: u64,
        tid: u64,
        op: &KvOp,
    ) -> Response {
        let KvOp::Scan {
            collection,
            cursor,
            count,
            filters,
            projection,
            computed_columns,
            match_pattern,
            sort_keys,
            surrogate_ceiling,
        } = op
        else {
            return self.response_error(
                task,
                ErrorCode::Internal {
                    detail: "dispatch_kv_scan: plan is not Scan".into(),
                },
            );
        };
        self.execute_kv_scan(
            task,
            super::scan::KvScanHandlerParams {
                did,
                tid,
                collection: collection.as_str(),
                cursor,
                count: *count,
                match_pattern: match_pattern.as_deref(),
                filters,
                projection,
                computed_columns_bytes: computed_columns,
                sort_keys,
                surrogate_ceiling: *surrogate_ceiling,
            },
        )
    }
}
