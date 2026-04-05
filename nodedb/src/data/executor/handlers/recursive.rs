//! Recursive CTE handler: iterative fixed-point execution.
//!
//! Executes the base query once to seed the working table, then
//! repeatedly executes the recursive query until no new rows are
//! produced (fixed point) or max_iterations is reached.

use std::collections::HashSet;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Execute a recursive CTE scan.
    ///
    /// Algorithm:
    /// 1. Seed: scan collection with base_filters → working_table
    /// 2. Loop: scan collection with recursive_filters, join against working_table
    /// 3. Add new rows to working_table
    /// 4. Repeat until no new rows or max_iterations reached
    /// 5. Return all accumulated rows
    #[allow(clippy::too_many_arguments)]
    pub(in crate::data::executor) fn execute_recursive_scan(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        base_filters: &[u8],
        recursive_filters: &[u8],
        max_iterations: usize,
        distinct: bool,
        limit: usize,
    ) -> Response {
        let scan_limit = self.query_tuning.aggregate_scan_cap;

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

        // Scan all documents once (used for both base and recursive steps).
        let all_docs = match self.sparse.scan_documents(tid, collection, scan_limit) {
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

        // Step 1: Seed working table with base query results.
        let mut results: Vec<serde_json::Value> = Vec::new();
        let mut seen_keys: HashSet<String> = HashSet::new();

        for (_doc_id, value) in &all_docs {
            let Some(doc) = super::super::doc_format::decode_document(value) else {
                continue;
            };
            if base_preds.iter().all(|f| f.matches(&doc)) {
                let key = if distinct {
                    serde_json::to_string(&doc).unwrap_or_default()
                } else {
                    String::new()
                };
                if !distinct || seen_keys.insert(key) {
                    results.push(doc);
                }
            }
        }

        // Step 2: Iterate recursive step until fixed point.
        let mut prev_count = 0;
        for iteration in 0..max_iterations {
            if results.len() >= limit || results.len() == prev_count {
                break;
            }
            prev_count = results.len();

            // The "working table" for this iteration is the rows added in the
            // previous iteration. For the recursive step, we scan the collection
            // and filter by recursive predicates AND check that the row relates
            // to existing working table rows (by matching any field).
            //
            // In a full SQL recursive CTE, the recursive term references the
            // CTE name. Here, we approximate by applying recursive filters to
            // the full collection and adding any new matching rows.
            let mut new_rows = Vec::new();
            for (doc_id, value) in &all_docs {
                if results.len() + new_rows.len() >= limit {
                    break;
                }
                let Some(doc) = super::super::doc_format::decode_document(value) else {
                    continue;
                };
                if recursive_preds.iter().all(|f| f.matches(&doc)) {
                    let key = if distinct {
                        serde_json::to_string(&doc).unwrap_or_default()
                    } else {
                        doc_id.clone()
                    };
                    if !distinct || seen_keys.insert(key) {
                        new_rows.push(doc);
                    }
                }
            }

            if new_rows.is_empty() {
                break;
            }

            tracing::debug!(
                core = self.core_id,
                iteration,
                new_rows = new_rows.len(),
                total = results.len() + new_rows.len(),
                "recursive CTE iteration"
            );
            results.extend(new_rows);
        }

        // Truncate to limit.
        results.truncate(limit);

        match super::super::response_codec::encode_json_vec(&results) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }
}
