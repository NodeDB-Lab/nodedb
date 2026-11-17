// SPDX-License-Identifier: BUSL-1.1

//! `RETURNING` for timeseries ingests.

use super::project::build_rejecting_rows_payload;
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec::IngestRejection;
use crate::data::executor::task::ExecutionTask;
use crate::util::rmpv_value::rmpv_to_value;
use nodedb_physical::physical_plan::ReturningSpec;
use nodedb_types::Value;

impl CoreLoop {
    /// Build a timeseries ingest's `RETURNING` response from the points it
    /// stored. `rows` are already `raw_scan::emit_memtable_rows_at` output, so
    /// a point renders exactly as `SELECT` would, `NaN`-as-NULL included; this
    /// only re-keys the shape, no per-cell decisions of its own. A
    /// `rejection` travels beside the rows, and the Control Plane raises it
    /// as the rejected-lines notice.
    pub(in crate::data::executor) fn timeseries_stored_returning_response(
        &self,
        task: &ExecutionTask,
        spec: &ReturningSpec,
        rls_filters: &[u8],
        rows: &[rmpv::Value],
        rejection: Option<IngestRejection>,
    ) -> Response {
        let docs: Vec<Value> = rows.iter().map(rmpv_to_value).collect();
        match build_rejecting_rows_payload(spec, rls_filters, &docs, rejection, &self.crs_context())
        {
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
