// SPDX-License-Identifier: BUSL-1.1

//! Sort-merge join execution.

use tracing::debug;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use nodedb_query::msgpack_scan;

use super::{SortMergeJoinParams, compare_preextracted, merge_join_docs_binary};

/// Collect consecutive index positions in `indices[start..]` sharing the same key as `indices[start]`.
fn collect_key_group_preextracted(
    docs: &[(String, Vec<u8>)],
    indices: &[usize],
    key_ranges: &[Vec<(usize, usize)>],
    start: usize,
) -> usize {
    let pivot_idx = indices[start];
    let mut end = start + 1;
    while end < indices.len() {
        let cur_idx = indices[end];
        let ord = compare_preextracted(
            &docs[pivot_idx].1,
            &key_ranges[pivot_idx],
            &docs[cur_idx].1,
            &key_ranges[cur_idx],
        );
        if ord != std::cmp::Ordering::Equal {
            break;
        }
        end += 1;
    }
    end
}

impl CoreLoop {
    pub(in crate::data::executor) fn execute_sort_merge_join(
        &mut self,
        p: SortMergeJoinParams<'_>,
    ) -> Response {
        let SortMergeJoinParams {
            task,
            tid,
            left_collection,
            right_collection,
            on,
            join_type,
            limit,
            pre_sorted,
            left_rls_filters,
            right_rls_filters,
        } = p;
        debug!(
            core = self.core_id,
            %left_collection,
            %right_collection,
            keys = on.len(),
            %join_type,
            pre_sorted,
            "sort-merge join"
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

        // Memory-budget guard on the left side. Checked immediately after
        // materialisation so an over-budget left input surfaces the error before
        // the right side is scanned. A budget of 0 disables the check (treated
        // as unlimited), matching the scan-budget convention.
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

        // Memory-budget guard on the right side. Same convention as the
        // left-side guard above. A budget of 0 disables the check.
        if let Some(err) = self.join_side_over_budget(task, &right_docs, budget) {
            return err;
        }

        let left_keys: Vec<&str> = on.iter().map(|(l, _)| l.as_str()).collect();
        let right_keys: Vec<&str> = on.iter().map(|(_, r)| r.as_str()).collect();

        // Pre-extract key ranges once: O(n·keys·fields) instead of O(n·log(n)·keys·fields).
        let left_key_ranges: Vec<Vec<(usize, usize)>> = left_docs
            .iter()
            .map(|(_, bytes)| {
                left_keys
                    .iter()
                    .map(|k| msgpack_scan::extract_field(bytes, 0, k).unwrap_or((0, 0)))
                    .collect()
            })
            .collect();
        let right_key_ranges: Vec<Vec<(usize, usize)>> = right_docs
            .iter()
            .map(|(_, bytes)| {
                right_keys
                    .iter()
                    .map(|k| msgpack_scan::extract_field(bytes, 0, k).unwrap_or((0, 0)))
                    .collect()
            })
            .collect();

        let mut left_indices: Vec<usize> = (0..left_docs.len()).collect();
        let mut right_indices: Vec<usize> = (0..right_docs.len()).collect();
        if !pre_sorted {
            left_indices.sort_unstable_by(|&a, &b| {
                compare_preextracted(
                    &left_docs[a].1,
                    &left_key_ranges[a],
                    &left_docs[b].1,
                    &left_key_ranges[b],
                )
            });
            right_indices.sort_unstable_by(|&a, &b| {
                compare_preextracted(
                    &right_docs[a].1,
                    &right_key_ranges[a],
                    &right_docs[b].1,
                    &right_key_ranges[b],
                )
            });
        }

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

        let mut results: Vec<Vec<u8>> = Vec::new();
        let mut li = 0usize;
        let mut ri = 0usize;
        let mut right_matched: Vec<bool> = vec![false; right_docs.len()];
        while li < left_indices.len() && ri < right_indices.len() {
            if results.len() >= probe_limit {
                break;
            }
            let left_idx = left_indices[li];
            let right_idx = right_indices[ri];
            let ord = compare_preextracted(
                &left_docs[left_idx].1,
                &left_key_ranges[left_idx],
                &right_docs[right_idx].1,
                &right_key_ranges[right_idx],
            );
            match ord {
                std::cmp::Ordering::Less => {
                    if is_left {
                        results.push(merge_join_docs_binary(
                            &left_docs[left_idx].1,
                            None,
                            left_collection,
                            right_collection,
                        ));
                    }
                    li += 1;
                }
                std::cmp::Ordering::Greater => {
                    if is_right {
                        right_matched[right_idx] = true;
                        results.push(merge_join_docs_binary(
                            &[],
                            Some(&right_docs[right_idx].1),
                            "",
                            right_collection,
                        ));
                    }
                    ri += 1;
                }
                std::cmp::Ordering::Equal => {
                    let left_end = collect_key_group_preextracted(
                        &left_docs,
                        &left_indices,
                        &left_key_ranges,
                        li,
                    );
                    let right_end = collect_key_group_preextracted(
                        &right_docs,
                        &right_indices,
                        &right_key_ranges,
                        ri,
                    );

                    'outer: for &lj_idx in &left_indices[li..left_end] {
                        for &rj_idx in &right_indices[ri..right_end] {
                            if results.len() >= probe_limit {
                                break 'outer;
                            }
                            right_matched[rj_idx] = true;
                            results.push(merge_join_docs_binary(
                                &left_docs[lj_idx].1,
                                Some(&right_docs[rj_idx].1),
                                left_collection,
                                right_collection,
                            ));
                        }
                    }

                    li = left_end;
                    ri = right_end;
                }
            }
        }

        if is_left {
            while li < left_indices.len() && results.len() < probe_limit {
                let left_idx = left_indices[li];
                results.push(merge_join_docs_binary(
                    &left_docs[left_idx].1,
                    None,
                    left_collection,
                    right_collection,
                ));
                li += 1;
            }
        }

        if is_right {
            while ri < right_indices.len() && results.len() < probe_limit {
                let right_idx = right_indices[ri];
                if !right_matched[right_idx] {
                    results.push(merge_join_docs_binary(
                        &[],
                        Some(&right_docs[right_idx].1),
                        "",
                        right_collection,
                    ));
                }
                ri += 1;
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
