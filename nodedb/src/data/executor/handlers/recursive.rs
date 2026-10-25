// SPDX-License-Identifier: BUSL-1.1

//! Recursive CTE handler: iterative fixed-point execution.
//!
//! Executes the base query once to seed the working table, then
//! repeatedly joins the collection against the working table via
//! the `join_link` until no new rows are produced (fixed point).
//! Passing `max_iterations` before the fixed point is an error.

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

use super::recursive_iterate::{RecursionError, RecursionSpec, RecursiveRow, iterate};

/// Parameters for [`CoreLoop::execute_recursive_scan`].
pub(in crate::data::executor) struct RecursiveScanParams<'a> {
    pub tid: u64,
    pub collection: &'a str,
    pub base_filters: &'a [u8],
    pub recursive_filters: &'a [u8],
    pub join_link: Option<&'a (String, String)>,
    pub max_iterations: usize,
    pub distinct: bool,
    pub limit: usize,
}

impl CoreLoop {
    /// Execute a recursive CTE scan.
    ///
    /// Algorithm:
    /// 1. Seed: scan collection with base_filters → working_table
    /// 2. Loop: for each row in collection, check if `join_link.0` value
    ///    matches any `join_link.1` value in the working_table → new matches
    /// 3. New matches become the working table for the next iteration
    /// 4. Accumulate all results
    /// 5. Repeat until no new rows; past `max_iterations`, fail
    pub(in crate::data::executor) fn execute_recursive_scan(
        &mut self,
        task: &ExecutionTask,
        params: RecursiveScanParams<'_>,
    ) -> Response {
        let RecursiveScanParams {
            tid,
            collection,
            base_filters,
            recursive_filters,
            join_link,
            max_iterations,
            distinct,
            limit,
        } = params;
        // Scan-quiesce gate.
        let _scan_guard = match self.acquire_scan_guard(task, tid, collection) {
            Ok(g) => g,
            Err(resp) => return resp,
        };

        // Every row of the collection is read under the scan byte budget; a
        // collection past it fails loudly, never as a prefix.
        let budget = self.query_tuning.max_scan_result_bytes;
        let scan_limit = super::scan_budget::fetch_limit_for(usize::MAX, 0, budget);

        // Parse filter predicates.
        let base_preds: Vec<ScanFilter> = if base_filters.is_empty() {
            Vec::new()
        } else {
            match zerompk::from_msgpack(base_filters) {
                Ok(p) => p,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("base filter deserialization failed: {e}"),
                        },
                    );
                }
            }
        };
        let recursive_preds: Vec<ScanFilter> = if recursive_filters.is_empty() {
            Vec::new()
        } else {
            match zerompk::from_msgpack(recursive_filters) {
                Ok(p) => p,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("recursive filter deserialization failed: {e}"),
                        },
                    );
                }
            }
        };

        // The stored bodies' encoding comes from the collection's registered
        // kind, resolved once here: a two-way Strict/Document decision made
        // inline would leave a vector-primary sidecar read as a document, whose
        // tagged values yield `[4,"alice"]` where the CTE expects `alice`.
        let body_format = self.sparse_body_format(
            task.request.database_id,
            crate::types::TenantId::new(tid),
            collection,
        );

        // Scan all documents once (used for both base and recursive steps),
        // each at its current version.
        let all_docs = match self.current_rows(
            task.request.database_id.as_u64(),
            tid,
            collection,
            scan_limit,
        ) {
            Ok(d) => d,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("recursive scan failed: {e}"),
                    },
                );
            }
        };

        // Convert raw stored bytes to the msgpack form the CTE steps compare
        // on, once. A row that lacks its identity column carries its identity
        // only in the storage key, so the row image injects it under that
        // column before any predicate runs. A declared-key row already holds
        // its key and gains no `id`.
        let identity_column =
            self.identity_column(task.request.database_id.as_u64(), tid, collection);
        let rows: Vec<RecursiveRow> = all_docs
            .iter()
            .map(|(doc_id, value)| RecursiveRow {
                id: doc_id.to_string(),
                body: crate::data::executor::scan_normalize::sparse_row_to_doc(
                    doc_id,
                    value,
                    body_format.as_format_ref(),
                    &identity_column,
                )
                .1,
            })
            .collect();
        let scanned_bytes = rows.iter().fold(0usize, |total, row| {
            total
                .saturating_add(row.body.len())
                .saturating_add(row.id.len())
        });
        if super::scan_budget::budget_exceeded(scanned_bytes, budget) {
            return self.response_error(task, ErrorCode::ResourcesExhausted);
        }

        let spec = RecursionSpec {
            base_preds: &base_preds,
            recursive_preds: &recursive_preds,
            join_link,
            max_iterations,
            distinct,
            limit,
        };
        let results = match iterate(&rows, &spec, &self.crs_context()) {
            Ok(results) => results,
            Err(RecursionError::Eval(e)) => return self.response_error(task, ErrorCode::from(e)),
            Err(RecursionError::Decode(detail)) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: format!("recursive CTE row of '{collection}': {detail}"),
                    },
                );
            }
            Err(RecursionError::DepthExceeded { max_depth }) => {
                return self.response_error(
                    task,
                    ErrorCode::RecursionDepthExceeded {
                        cte_name: format!("recursive scan of {collection}"),
                        max_depth,
                    },
                );
            }
        };

        tracing::debug!(
            core = self.core_id,
            %collection,
            total = results.len(),
            "recursive CTE: iteration complete"
        );

        // Build raw msgpack array from collected msgpack rows.
        let mut payload = Vec::with_capacity(results.iter().map(|r| r.len()).sum::<usize>() + 8);
        nodedb_query::msgpack_scan::write_array_header(&mut payload, results.len());
        for row in &results {
            payload.extend_from_slice(row);
        }
        self.response_with_payload(task, payload)
    }
}
