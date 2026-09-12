// SPDX-License-Identifier: BUSL-1.1

//! MERGE RESOLVE pass: classify without writing, returning the NOT-MATCHED
//! insert rows for Control-Plane surrogate assignment.

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

use super::super::merge::MergeParams;

impl CoreLoop {
    /// RESOLVE pass: return ALL resolved arms without writing.
    ///
    /// Response payload is a msgpack 3-tuple `(updates, deletes, inserts)`:
    /// - `updates`: `Vec<(doc_id, surrogate_u32, body_msgpack,
    ///   old_body_msgpack)>` — the existing target row's storage key, its
    ///   registered surrogate (always present — every matched row is
    ///   storage-keyed), the post-update body, and the row's PRE-image
    ///   (which is what lets the Control Plane resolve BOTH sides of a
    ///   materialized-sum join-key rewrite).
    /// - `deletes`: `Vec<(doc_id, Option<surrogate_u32>, body_msgpack)>` — the
    ///   existing target row's storage key, its registered surrogate, and the
    ///   pre-delete body (so the CP can extract its primary key).
    /// - `inserts`: `Vec<(join_key, body_msgpack)>` — the NOT-MATCHED rows, for
    ///   which the orchestrator assigns a fresh, registered surrogate.
    ///
    /// The autocommit orchestrator consumes only `inserts`; the in-transaction
    /// COMMIT expander rewrites all three arms into concrete point ops.
    pub(in crate::data::executor) fn execute_merge_resolve(
        &mut self,
        task: &ExecutionTask,
        tid: u64,
        params: MergeParams<'_>,
    ) -> Response {
        let plan = match self.collect_merge_plan(
            task.request.database_id.as_u64(),
            tid,
            task.request.txn_id,
            &params,
        ) {
            Ok(p) => p,
            Err(e) => return self.response_error(task, e),
        };
        let updates: Vec<crate::query::ResolvedUpdateRowWire> = plan
            .updates
            .into_iter()
            .map(|u| {
                (
                    u.key.to_string(),
                    u.key.surrogate().as_u32(),
                    u.body,
                    u.old_body,
                )
            })
            .collect();
        let deletes: Vec<(String, Option<u32>, Vec<u8>)> = plan
            .deletes
            .into_iter()
            .map(|d| (d.key.to_string(), Some(d.key.surrogate().as_u32()), d.body))
            .collect();
        let inserts: Vec<(String, Vec<u8>)> = plan
            .inserts
            .into_iter()
            .map(|i| (i.join_key, i.body))
            .collect();
        match zerompk::to_msgpack_vec(&(updates, deletes, inserts)) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("merge resolve encode: {e}"),
                },
            ),
        }
    }
}
