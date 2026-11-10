// SPDX-License-Identifier: BUSL-1.1

//! `RETURNING` for KV writes.

use super::project::encode_stored_rows_payload;
use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::returning_stored_docs;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::ReturningSpec;

/// A KV row a write path hands back to a `RETURNING` projection: the raw key
/// bytes paired with the exact value bytes stored under them.
pub(in crate::data::executor) type KvStoredRow<'a> = (&'a [u8], &'a [u8]);

impl CoreLoop {
    /// Build a KV write's `RETURNING` response from the rows it just stored.
    /// Row shape comes from
    /// [`crate::data::executor::scan_normalize::kv_row_to_doc`], the same
    /// helper KV scans use, so it is byte-identical to what `SELECT` on that
    /// key produces. A body that fails to decode fails the statement rather
    /// than substitute a bare `{key}` that hides an unreadable stored value.
    pub(in crate::data::executor) fn kv_stored_returning_response(
        &self,
        task: &ExecutionTask,
        spec: &ReturningSpec,
        rls_filters: &[u8],
        rows: &[KvStoredRow<'_>],
    ) -> Response {
        match kv_stored_rows_payload(spec, rls_filters, rows, &self.crs_context()) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, e),
        }
    }
}

/// The `RowsPayload` blob a KV write's `RETURNING` clause projects. Split
/// from [`CoreLoop::kv_stored_returning_response`] so the resolve-before-
/// propose path can decide the same payload without holding a `Response`.
pub(in crate::data::executor) fn kv_stored_rows_payload(
    spec: &ReturningSpec,
    rls_filters: &[u8],
    rows: &[KvStoredRow<'_>],
    crs: &nodedb_crs::CrsContext<'_>,
) -> crate::Result<Vec<u8>> {
    let docs = returning_stored_docs::kv_stored_docs(rows)?;
    encode_stored_rows_payload(spec, rls_filters, &docs, crs)
}
