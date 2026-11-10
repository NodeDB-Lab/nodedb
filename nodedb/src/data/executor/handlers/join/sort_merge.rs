// SPDX-License-Identifier: BUSL-1.1

//! Sort-merge join execution.

use tracing::debug;

use super::merge_pairs::{
    CompositeTuples, LeftOnlyMode, MergeSpec, Pair, merge_pairs, rows_match_keys,
};
use super::null_aware_tuple::KeyTuple;
use super::side_rows::JoinSideSource;
use super::support::row_qualifier;
use super::{SortMergeJoinParams, merge_join_docs_binary};
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;

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
            left_input,
            right_input,
            system_time,
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

        let left_docs = match self.join_side_rows(
            task,
            tid,
            JoinSideSource {
                collection: left_collection,
                input: left_input,
                rls_filters: left_rls_filters,
                system_time,
            },
            scan_limit,
        ) {
            Ok(d) => d,
            Err(resp) => return *resp,
        };

        // Memory-budget guard on the left side. Checked immediately after
        // materialisation so an over-budget left input surfaces the error before
        // the right side is scanned. A budget of 0 disables the check (treated
        // as unlimited), matching the scan-budget convention.
        if let Some(err) = self.join_side_over_budget(task, &left_docs, budget) {
            return err;
        }

        let right_docs = match self.join_side_rows(
            task,
            tid,
            JoinSideSource {
                collection: right_collection,
                input: right_input,
                rls_filters: right_rls_filters,
                system_time,
            },
            scan_limit,
        ) {
            Ok(d) => d,
            Err(resp) => return *resp,
        };

        // Memory-budget guard on the right side. Same convention as the
        // left-side guard above. A budget of 0 disables the check.
        if let Some(err) = self.join_side_over_budget(task, &right_docs, budget) {
            return err;
        }

        let left_keys: Vec<&str> = on.iter().map(|(l, _)| l.as_str()).collect();
        let right_keys: Vec<&str> = on.iter().map(|(_, r)| r.as_str()).collect();

        // Read canonical match keys once: O(n·keys·fields) instead of
        // O(n·log(n)·keys·fields).
        let left_match_keys = match rows_match_keys(&left_docs, &left_keys) {
            Ok(keys) => keys,
            Err(error) => return self.response_error(task, ErrorCode::from(error)),
        };
        let right_match_keys = match rows_match_keys(&right_docs, &right_keys) {
            Ok(keys) => keys,
            Err(error) => return self.response_error(task, ErrorCode::from(error)),
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

        // Each side's columns are qualified with its bare collection name,
        // the name SQL qualifies them with.
        let database_id = task.request.database_id;
        let left_qualifier = row_qualifier(left_collection, database_id);
        let right_qualifier = row_qualifier(right_collection, database_id);

        let left_only = LeftOnlyMode::from_join_type(join_type).is_some();
        // A composite `NOT IN` compares row-wise, so it needs each side's
        // key tuples rather than its match keys.
        let composite = if LeftOnlyMode::from_join_type(join_type)
            == Some(LeftOnlyMode::AntiNullAware)
            && left_keys.len() > 1
        {
            let tuples = |docs: &[(String, Vec<u8>)], keys: &[&str]| {
                docs.iter()
                    .map(|(_, bytes)| KeyTuple::of_row(bytes, keys))
                    .collect::<Result<Vec<_>, _>>()
            };
            match (
                tuples(&left_docs, &left_keys),
                tuples(&right_docs, &right_keys),
            ) {
                (Ok(left), Ok(right)) => Some(CompositeTuples { left, right }),
                (Err(error), _) | (_, Err(error)) => {
                    return self.response_error(task, ErrorCode::from(error));
                }
            }
        } else {
            None
        };
        let pairs = merge_pairs(
            &left_match_keys,
            &right_match_keys,
            &MergeSpec {
                left_only: LeftOnlyMode::from_join_type(join_type),
                is_left,
                is_right,
                pre_sorted,
                limit: probe_limit,
                composite,
            },
        );
        let mut results: Vec<Vec<u8>> = Vec::with_capacity(pairs.len());
        for pair in pairs {
            let merged = match pair {
                Pair::Matched(l, r) => merge_join_docs_binary(
                    &left_docs[l].1,
                    Some(&right_docs[r].1),
                    left_qualifier,
                    right_qualifier,
                ),
                // A semi or anti row carries only the left side's columns, as
                // the hash join emits it.
                Pair::LeftOnly(l) if left_only => {
                    merge_join_docs_binary(&left_docs[l].1, None, left_qualifier, "")
                }
                Pair::LeftOnly(l) => {
                    merge_join_docs_binary(&left_docs[l].1, None, left_qualifier, right_qualifier)
                }
                Pair::RightOnly(r) => {
                    merge_join_docs_binary(&[], Some(&right_docs[r].1), "", right_qualifier)
                }
            };
            match merged {
                Ok(row) => results.push(row),
                Err(error) => return self.response_error(task, ErrorCode::from(error)),
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
