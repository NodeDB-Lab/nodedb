// SPDX-License-Identifier: BUSL-1.1

//! Nested-loop join execution.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;

use super::NestedLoopJoinParams;
use super::merge_join_docs_binary;

impl CoreLoop {
    /// Nested loop join: O(N×M) fallback for non-equi joins, theta joins,
    /// and cross joins where hash join can't operate.
    ///
    /// For each left row, iterates all right rows and evaluates the join
    /// condition. Supports inner/left/right/full join types.
    pub(in crate::data::executor) fn execute_nested_loop_join(
        &mut self,
        p: NestedLoopJoinParams<'_>,
    ) -> Response {
        let NestedLoopJoinParams {
            task,
            tid,
            left_collection,
            right_collection,
            condition,
            join_type,
            limit,
            left_rls_filters,
            right_rls_filters,
        } = p;
        debug!(
            core = self.core_id,
            %left_collection,
            %right_collection,
            %join_type,
            "nested loop join"
        );

        // Derive a finite fetch-ceiling from the per-query byte budget so
        // the underlying KV scan never calls Vec::with_capacity(usize::MAX).
        // For an unbounded join this evaluates to budget_bytes / 16 + 1
        // (floored at 1000). The post-materialisation byte-budget guards below
        // are the authoritative overflow check; this ceiling is a pre-fetch hint.
        let budget = self.query_tuning.max_scan_result_bytes;
        let scan_limit =
            crate::data::executor::handlers::scan_budget::fetch_limit_for(usize::MAX, 0, budget);

        let left_docs = match self.scan_collection_with_rls(
            task.request.database_id.as_u64(),
            tid,
            left_collection,
            scan_limit,
            left_rls_filters,
        ) {
            Ok(d) => d,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        // Memory-budget guard on the probe side (left). Checked immediately
        // after materialisation so an over-budget left input surfaces the error
        // before the right side is scanned. A budget of 0 disables the check
        // (treated as unlimited), matching the scan-budget convention.
        if let Some(err) = self.join_side_over_budget(task, &left_docs, budget) {
            return err;
        }

        let right_docs = match self.scan_collection_with_rls(
            task.request.database_id.as_u64(),
            tid,
            right_collection,
            scan_limit,
            right_rls_filters,
        ) {
            Ok(d) => d,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                );
            }
        };

        // Memory-budget guard on the inner side (right). Same convention as the
        // left-side guard above. A budget of 0 disables the check.
        if let Some(err) = self.join_side_over_budget(task, &right_docs, budget) {
            return err;
        }

        // Parse join condition predicates.
        let predicates: Vec<crate::bridge::scan_filter::ScanFilter> = if condition.is_empty() {
            Vec::new() // Cross join — no condition.
        } else {
            match zerompk::from_msgpack(condition) {
                Ok(f) => f,
                Err(e) => {
                    tracing::warn!(core = self.core_id, error = %e, "malformed join condition");
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("join condition deserialization: {e}"),
                        },
                    );
                }
            }
        };

        let is_left = join_type == "left" || join_type == "full";
        let is_right = join_type == "right" || join_type == "full";

        // Bound the emitted output. An explicit user `LIMIT n`
        // (`limit != usize::MAX`) is honored exactly. A no-LIMIT join
        // (`usize::MAX`) must NOT silently truncate at a default cap: bound its
        // output by the per-query byte budget instead, deriving a budget
        // row-ceiling (`+1` to detect overflow) and surfacing a deterministic
        // `ResourcesExhausted` if it is filled. A budget of 0 = unlimited.
        let (probe_limit, enforce_output_budget) = if limit != usize::MAX {
            (limit, false)
        } else if budget == 0 {
            (usize::MAX, false)
        } else {
            (
                crate::data::executor::handlers::scan_budget::fetch_limit_for(
                    usize::MAX,
                    0,
                    budget,
                ),
                true,
            )
        };

        let mut right_matched: Vec<bool> = vec![false; right_docs.len()];
        let mut results = Vec::new();

        for (_, left_bytes) in &left_docs {
            if results.len() >= probe_limit {
                break;
            }

            let mut left_matched = false;
            for (ri, (_, right_bytes)) in right_docs.iter().enumerate() {
                if results.len() >= probe_limit {
                    break;
                }

                // Evaluate condition against merged row (binary).
                // Nested loop conditions reference prefixed fields (e.g., "left.id"),
                // so we must merge before evaluating.
                let passes = if predicates.is_empty() {
                    true // Cross join.
                } else {
                    let merged = merge_join_docs_binary(
                        left_bytes,
                        Some(right_bytes),
                        left_collection,
                        right_collection,
                    );
                    match crate::bridge::scan_filter::ScanFilter::all_match_binary(
                        &predicates,
                        &merged,
                    ) {
                        Ok(b) => b,
                        Err(_e) => {
                            return self.response_error(task, ErrorCode::DivisionByZero);
                        }
                    }
                };

                if passes {
                    left_matched = true;
                    right_matched[ri] = true;
                    results.push(merge_join_docs_binary(
                        left_bytes,
                        Some(right_bytes),
                        left_collection,
                        right_collection,
                    ));
                }
            }

            // LEFT/FULL: emit unmatched left rows.
            if !left_matched && is_left {
                results.push(merge_join_docs_binary(
                    left_bytes,
                    None,
                    left_collection,
                    right_collection,
                ));
            }
        }

        // RIGHT/FULL: emit unmatched right rows.
        if is_right {
            for (ri, (_, right_bytes)) in right_docs.iter().enumerate() {
                if results.len() >= probe_limit {
                    break;
                }
                if !right_matched[ri] {
                    results.push(merge_join_docs_binary(
                        &[],
                        Some(right_bytes),
                        "",
                        right_collection,
                    ));
                }
            }
        }

        // No-LIMIT join whose output filled the budget-derived ceiling: the
        // result exceeds the byte budget, so surface a deterministic error
        // rather than silently dropping the excess rows.
        if enforce_output_budget && results.len() >= probe_limit {
            return self.response_error(task, ErrorCode::ResourcesExhausted);
        }

        let payload = super::super::super::response_codec::encode_binary_rows(&results);
        self.response_with_payload(task, payload)
    }
}
