// SPDX-License-Identifier: BUSL-1.1

//! `aggregate_over_docs`: orchestrate accumulate + finalize over an
//! already-materialized doc set, layering the per-shard result cache on top.

use super::super::cache_key::aggregate_cache_key;
use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::{AggregateSpec, GroupKeySpec};

/// Borrowed/owned inputs to [`CoreLoop::aggregate_over_docs`]: the
/// already-materialized doc set plus the GROUP BY / aggregate / filter /
/// sort specs needed to accumulate and finalize it.
pub(in crate::data::executor) struct AggregateOverDocsParams<'a> {
    pub task: &'a ExecutionTask,
    pub collection: &'a str,
    pub cache_tid: Option<u64>,
    pub docs: Vec<(String, Vec<u8>)>,
    pub group_by: &'a [GroupKeySpec],
    pub aggregates: &'a [AggregateSpec],
    pub filters: &'a [u8],
    pub having: &'a [u8],
    pub limit: usize,
    pub sub_group_by: &'a [String],
    pub sub_aggregates: &'a [AggregateSpec],
    pub sort_keys: &'a [(String, bool)],
}

impl CoreLoop {
    /// Streaming aggregation over an already-materialized set of `(doc_id,
    /// msgpack_bytes)` rows.
    ///
    /// Shared by the per-shard scan path (`docs` from `scan_collection`) and
    /// the input-sourced catalog path (`docs` decoded from a sub-plan
    /// Response). Documents are processed one at a time; per-group
    /// accumulators hold only the derived scalar / approximate state needed
    /// for the final result — no raw document bytes are retained. Memory is
    /// O(num_groups × num_aggregates) instead of O(all_docs).
    ///
    /// WHERE filters, GROUP BY, sub-groups, HAVING, ORDER BY, and LIMIT are
    /// applied identically regardless of the row source.
    ///
    /// `cache_tid` controls the aggregate result cache: `Some(tid)` writes the
    /// result keyed on `(tid, collection, ...)` (the per-shard scan path);
    /// `None` skips caching (the input-sourced catalog path — catalog rows are
    /// identity-scoped, so caching them across identities would be incorrect).
    ///
    /// The accumulate and finalize phases are factored into `accumulate_groups`
    /// and `finalize_groups` respectively, so the distributed-shuffle producer
    /// and consumer can reuse each half without duplicating logic.
    pub(in crate::data::executor) fn aggregate_over_docs(
        &mut self,
        params: AggregateOverDocsParams<'_>,
    ) -> Response {
        let AggregateOverDocsParams {
            task,
            collection,
            cache_tid,
            docs,
            group_by,
            aggregates,
            filters,
            having,
            limit,
            sub_group_by,
            sub_aggregates,
            sort_keys,
        } = params;

        let (groups, sub_groups) =
            match self.accumulate_groups(super::accumulate::AccumulateGroupsParams {
                docs: &docs,
                group_by,
                aggregates,
                filters,
                sub_group_by,
                sub_aggregates,
            }) {
                Ok(g) => g,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    );
                }
            };

        match self.finalize_groups(super::finalize::FinalizeGroupsParams {
            groups,
            sub_groups,
            group_by,
            aggregates,
            having,
            limit,
            sub_group_by,
            sub_aggregates,
            sort_keys,
        }) {
            Ok(payload) => {
                if let Some(tid) = cache_tid
                    && filters.is_empty()
                    && having.is_empty()
                {
                    let cache_key = aggregate_cache_key(
                        task.request.database_id.as_u64(),
                        tid,
                        collection,
                        group_by,
                        aggregates,
                        sub_group_by,
                        sub_aggregates,
                    );
                    if self.aggregate_cache.len() < 256 {
                        self.aggregate_cache.insert(cache_key, payload.clone());
                    }
                }
                self.response_with_payload(task, payload)
            }
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }
}
