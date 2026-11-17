// SPDX-License-Identifier: BUSL-1.1

//! `RETURNING` for vector-primary writes.

use super::project::encode_stored_rows_payload;
use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::returning_stored_docs;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::ReturningSpec;

/// The columns a vector-primary `RETURNING` row renders its identity and its
/// vector under.
#[derive(Clone, Copy)]
pub(in crate::data::executor) struct VectorRowColumns<'a> {
    pub identity: &'a str,
    pub vector: &'a str,
}

/// A vector-primary row a write path hands back to a `RETURNING`
/// projection: its sparse-store key, the sidecar bytes stored (or removed)
/// under it, and the vector its node carries (or carried).
pub(in crate::data::executor) type VectorStoredRow<'a> =
    (&'a nodedb_types::StorageKey, &'a [u8], &'a [f32]);

impl CoreLoop {
    /// Build a vector-primary write's `RETURNING` response from the sidecars
    /// it stored or removed, via
    /// [`returning_stored_docs::vector_stored_docs`], the same
    /// converter `SELECT` uses. The sidecar is `zerompk` TAGGED bytes an
    /// ordinary document decode misreads (`"alice"` comes back as
    /// `[4,"alice"]`), so the format must be this literal, not re-decided.
    /// `columns` names the columns each row renders its identity and its
    /// vector under, as a `SELECT` renders them.
    pub(in crate::data::executor) fn vector_stored_returning_response(
        &self,
        task: &ExecutionTask,
        spec: &ReturningSpec,
        rls_filters: &[u8],
        columns: VectorRowColumns<'_>,
        rows: &[VectorStoredRow<'_>],
    ) -> Response {
        match vector_stored_rows_payload(spec, rls_filters, columns, rows, &self.crs_context()) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, e),
        }
    }
}

/// The `RowsPayload` blob a vector-primary write's `RETURNING` clause
/// projects. Split from [`CoreLoop::vector_stored_returning_response`] so the
/// resolve-before-propose path decides the same payload without holding a
/// `Response`.
pub(in crate::data::executor) fn vector_stored_rows_payload(
    spec: &ReturningSpec,
    rls_filters: &[u8],
    columns: VectorRowColumns<'_>,
    rows: &[VectorStoredRow<'_>],
    crs: &nodedb_crs::CrsContext<'_>,
) -> crate::Result<Vec<u8>> {
    let docs = returning_stored_docs::vector_stored_docs(columns, rows)?;
    encode_stored_rows_payload(spec, rls_filters, &docs, crs)
}
