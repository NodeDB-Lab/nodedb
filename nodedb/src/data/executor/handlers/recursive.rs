// SPDX-License-Identifier: BUSL-1.1

//! Recursive CTE handler: iterative fixed-point execution.
//!
//! Executes the base query once to seed the working table, then
//! repeatedly joins the collection against the working table via
//! the `join_link` until no new rows are produced (fixed point)
//! or `max_iterations` is reached.

use std::collections::HashSet;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

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
    /// 5. Repeat until no new rows or max_iterations reached
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

        // The stored bodies' encoding comes from the collection's registered
        // kind, resolved once here: a two-way Strict/Document decision made
        // inline would leave a vector-primary sidecar read as a document, whose
        // tagged values yield `[4,"alice"]` where the CTE expects `alice`.
        let body_format = self.sparse_body_format(
            task.request.database_id,
            crate::types::TenantId::new(tid),
            collection,
        );

        // Scan all documents once (used for both base and recursive steps).
        let all_docs = match self.sparse.scan_documents(
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
        // on. A schemaless row with no declared `id` field carries its
        // identity only in the storage key, never in the body, so the row
        // image must inject it before any predicate runs — otherwise
        // `id IS NULL` and RETURNING rows both lose the identity.
        let to_msgpack = |doc_id: &nodedb_types::StorageKey, value: &[u8]| -> Vec<u8> {
            crate::data::executor::scan_normalize::sparse_row_to_doc(
                doc_id,
                value,
                body_format.as_format_ref(),
            )
            .1
        };

        // Step 1: Seed working table with base query results.
        let mut results: Vec<Vec<u8>> = Vec::new();
        let mut seen_keys: HashSet<String> = HashSet::new();

        tracing::debug!(
            core = self.core_id,
            %collection,
            all_docs = all_docs.len(),
            base_preds = base_preds.len(),
            ?join_link,
            "recursive CTE: starting seed"
        );

        for (doc_id, value) in &all_docs {
            let mp = to_msgpack(doc_id, value);
            match ScanFilter::all_match_binary(&base_preds, &mp) {
                Ok(true) => {}
                Ok(false) => continue,
                Err(_e) => {
                    return self.response_error(task, ErrorCode::DivisionByZero);
                }
            }
            let key = if distinct {
                nodedb_types::msgpack_to_json_string(&mp).unwrap_or_default()
            } else {
                String::new()
            };
            if !distinct || seen_keys.insert(key) {
                results.push(mp);
            }
        }

        tracing::debug!(
            core = self.core_id,
            seed_count = results.len(),
            "recursive CTE: seed complete"
        );

        // Step 2: Iterate recursive step until fixed point.
        if let Some((collection_field, working_field)) = join_link {
            // Working-table hash-join: each iteration finds collection rows
            // where `collection_field` matches a `working_field` value from
            // the previous iteration's new rows.
            let mut frontier = results.clone();

            for iteration in 0..max_iterations {
                if results.len() >= limit || frontier.is_empty() {
                    break;
                }

                // Build hash set of working_field values from the frontier.
                let frontier_values: HashSet<String> = frontier
                    .iter()
                    .filter_map(|row| extract_field_string(row, working_field))
                    .collect();

                if frontier_values.is_empty() {
                    break;
                }

                let mut new_rows = Vec::new();
                for (doc_id, value) in &all_docs {
                    if results.len() + new_rows.len() >= limit {
                        break;
                    }

                    let mp = to_msgpack(doc_id, value);

                    // Apply recursive filters (WHERE clause from recursive branch).
                    match ScanFilter::all_match_binary(&recursive_preds, &mp) {
                        Ok(true) => {}
                        Ok(false) => continue,
                        Err(_e) => {
                            return self.response_error(task, ErrorCode::DivisionByZero);
                        }
                    }

                    // Check join link: collection_field value must be in frontier.
                    let field_val = match extract_field_string(&mp, collection_field) {
                        Some(v) => v,
                        None => continue,
                    };
                    if !frontier_values.contains(&field_val) {
                        continue;
                    }

                    let key = if distinct {
                        nodedb_types::msgpack_to_json_string(&mp).unwrap_or_default()
                    } else {
                        String::new()
                    };
                    if !distinct || seen_keys.insert(key) {
                        new_rows.push(mp);
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
                    "recursive CTE iteration (join-link)"
                );
                frontier = new_rows.clone();
                results.extend(new_rows);
            }
        } else {
            // No join link — fall back to filter-only iteration (original behavior).
            let mut prev_count = 0;
            for iteration in 0..max_iterations {
                if results.len() >= limit || results.len() == prev_count {
                    break;
                }
                prev_count = results.len();

                let mut new_rows = Vec::new();
                for (doc_id, value) in &all_docs {
                    if results.len() + new_rows.len() >= limit {
                        break;
                    }
                    let mp = to_msgpack(doc_id, value);
                    match ScanFilter::all_match_binary(&recursive_preds, &mp) {
                        Ok(true) => {}
                        Ok(false) => continue,
                        Err(_e) => {
                            return self.response_error(task, ErrorCode::DivisionByZero);
                        }
                    }
                    let key = if distinct {
                        nodedb_types::msgpack_to_json_string(&mp).unwrap_or_default()
                    } else {
                        doc_id.to_string()
                    };
                    if !distinct || seen_keys.insert(key) {
                        new_rows.push(mp);
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
                    "recursive CTE iteration (filter-only)"
                );
                results.extend(new_rows);
            }
        }

        tracing::debug!(
            core = self.core_id,
            total = results.len(),
            "recursive CTE: iteration complete"
        );

        // Truncate to limit.
        results.truncate(limit);

        // Build raw msgpack array from collected msgpack rows.
        let mut payload = Vec::with_capacity(results.iter().map(|r| r.len()).sum::<usize>() + 8);
        nodedb_query::msgpack_scan::write_array_header(&mut payload, results.len());
        for row in &results {
            payload.extend_from_slice(row);
        }
        self.response_with_payload(task, payload)
    }
}

/// Extract a field value from a msgpack document as a string for hash lookup.
fn extract_field_string(msgpack_doc: &[u8], field_name: &str) -> Option<String> {
    let value = nodedb_types::value_from_msgpack(msgpack_doc).ok()?;
    match &value {
        nodedb_types::Value::Object(map) => {
            let v = map.get(field_name)?;
            match v {
                nodedb_types::Value::String(s) => Some(s.clone()),
                nodedb_types::Value::Integer(i) => Some(i.to_string()),
                nodedb_types::Value::Float(f) => Some(f.to_string()),
                nodedb_types::Value::Bool(b) => Some(b.to_string()),
                _ => Some(format!("{v:?}")),
            }
        }
        _ => None,
    }
}
