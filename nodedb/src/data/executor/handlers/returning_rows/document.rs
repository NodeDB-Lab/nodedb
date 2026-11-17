// SPDX-License-Identifier: BUSL-1.1

//! `RETURNING` for document and CRDT writes: the stored post-images of the
//! rows a write just landed.

use super::project::build_rows_payload;
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::returning_stored_docs;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::RowIdentity;
use nodedb_physical::physical_plan::ReturningSpec;
use nodedb_types::columnar::StrictSchema;

/// Rows a write path hands back to a `RETURNING` projection: the row's
/// client-visible identity paired with the exact bytes stored for it.
pub(in crate::data::executor) type StoredRow<'a> = (&'a RowIdentity, &'a [u8]);

impl CoreLoop {
    /// Prepare fallible projection before the row transaction commits.
    pub(in crate::data::executor) fn prepare_stored_returning_response(
        &self,
        task: &ExecutionTask,
        spec: &ReturningSpec,
        rls_filters: &[u8],
        strict_schema: Option<&StrictSchema>,
        identity_column: &str,
        rows: &[StoredRow<'_>],
    ) -> crate::Result<Response> {
        let payload = build_stored_rows_payload(
            spec,
            rls_filters,
            strict_schema,
            identity_column,
            rows,
            &self.crs_context(),
        )?;
        Ok(self.response_with_payload(task, payload))
    }

    /// Build this task's `RETURNING` response from the rows it just stored.
    /// The single exit every insert-family handler uses, so the read gate and
    /// decode can never be applied on one path and skipped on another.
    /// `identity_column` is the column each row renders its identity under,
    /// per `CoreLoop::identity_column`.
    pub(in crate::data::executor) fn stored_returning_response(
        &self,
        task: &ExecutionTask,
        spec: &ReturningSpec,
        rls_filters: &[u8],
        strict_schema: Option<&StrictSchema>,
        identity_column: &str,
        rows: &[StoredRow<'_>],
    ) -> Response {
        match build_stored_rows_payload(
            spec,
            rls_filters,
            strict_schema,
            identity_column,
            rows,
            &self.crs_context(),
        ) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("RETURNING encode: {e}"),
                },
            ),
        }
    }
}

/// Project the STORED post-images of freshly written rows into a
/// `RowsPayload` — the write paths hold the exact bytes handed to storage,
/// so `RETURNING` reports what landed rather than echoing the submitted
/// body. `strict_schema` must be `Some` exactly when the collection stores
/// Binary Tuples; a decode failure fails the statement, never a bare `{id}`.
pub(in crate::data::executor) fn build_stored_rows_payload(
    spec: &ReturningSpec,
    rls_filters: &[u8],
    strict_schema: Option<&StrictSchema>,
    identity_column: &str,
    rows: &[StoredRow<'_>],
    crs: &nodedb_crs::CrsContext<'_>,
) -> crate::Result<Vec<u8>> {
    let docs = returning_stored_docs::stored_docs(strict_schema, identity_column, rows)?;
    build_rows_payload(spec, rls_filters, &docs, crs)
}
