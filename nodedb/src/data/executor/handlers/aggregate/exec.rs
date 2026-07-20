// SPDX-License-Identifier: BUSL-1.1

//! Aggregate dispatch: input-sourced (catalog) routing, grouping-set expansion,
//! and the per-shard fast paths (result cache, index-backed COUNT, native
//! columnar aggregation) before falling back to streaming aggregation.

use tracing::debug;

use super::cache_key::{aggregate_cache_key, legacy_aggregate_pairs};
use super::rows::{apply_user_aliases_to_rows, sort_aggregated_rows};
use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::{AggregateSpec, GroupKeySpec};
use nodedb_query::agg_key::canonical_agg_key;
use nodedb_query::msgpack_scan;

/// Borrowed inputs to [`CoreLoop::execute_aggregate`]: the target collection
/// (or input sub-plan), GROUP BY / aggregate / filter / HAVING / sub-group /
/// grouping-set / sort specs for one aggregate dispatch.
pub(in crate::data::executor) struct AggregateExecInputs<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub collection: &'a str,
    pub input: Option<&'a nodedb_physical::physical_plan::PhysicalPlan>,
    pub group_by: &'a [GroupKeySpec],
    pub aggregates: &'a [AggregateSpec],
    pub filters: &'a [u8],
    pub having: &'a [u8],
    pub limit: usize,
    pub sub_group_by: &'a [String],
    pub sub_aggregates: &'a [AggregateSpec],
    pub grouping_sets: &'a [Vec<u32>],
    pub sort_keys: &'a [(String, bool)],
}

impl CoreLoop {
    pub(in crate::data::executor) fn execute_aggregate(
        &mut self,
        inputs: AggregateExecInputs<'_>,
    ) -> Response {
        let AggregateExecInputs {
            task,
            tid,
            collection,
            input,
            group_by,
            aggregates,
            filters,
            having,
            limit,
            sub_group_by,
            sub_aggregates,
            grouping_sets,
            sort_keys,
        } = inputs;

        debug!(core = self.core_id, %collection, has_input = input.is_some(), group_fields = group_by.len(), aggs = aggregates.len(), "aggregate");

        // Input-sourced aggregate (catalog): the rows come from executing the
        // sub-plan (a coordinator-materialized `ProviderScan`), not from a
        // per-shard collection scan. Decode the sub-plan rows and aggregate
        // over them using the same streaming logic, then short-circuit before
        // the per-shard fast paths (cache / index-backed / columnar memtable),
        // none of which apply to coordinator-local catalog data.
        if let Some(sub_plan) = input {
            let sub_response = self.execute_plan(task, sub_plan);
            // Empty / undecodable payload → aggregate over zero rows, the same as
            // a per-shard scan that matched nothing. Feeding an empty doc set
            // through the shared path keeps behavior identical to the scan path
            // rather than surfacing the sub-plan Response (which may be a
            // non-row payload).
            let docs =
                crate::data::executor::response_codec::decode_response_to_docs(&sub_response)
                    .unwrap_or_default();
            return self.aggregate_over_docs(
                super::streaming::over_docs::AggregateOverDocsParams {
                    task,
                    collection,
                    cache_tid: None,
                    docs,
                    group_by,
                    aggregates,
                    filters,
                    having,
                    limit,
                    sub_group_by,
                    sub_aggregates,
                    sort_keys,
                },
            );
        }

        // ROLLUP / CUBE / GROUPING SETS path: union results from each set.
        if !grouping_sets.is_empty() {
            // Physical column names for the group-key fields, used by the
            // paths that key on raw column names (grouping-set expansion,
            // native columnar aggregation). For a bare column this is the
            // same string the spec emits under, so those paths behave
            // identically.
            let group_fields: Vec<String> =
                group_by.iter().filter_map(|s| s.field.clone()).collect();
            return super::super::grouping_sets_exec::execute_grouping_sets(
                self,
                super::super::grouping_sets_exec::GroupingSetsParams {
                    task,
                    tid,
                    collection,
                    group_by: &group_fields,
                    aggregates,
                    filters,
                    having,
                    limit,
                    grouping_sets,
                },
            );
        }

        // Fast path: incremental aggregate cache.
        if filters.is_empty() && having.is_empty() {
            let cache_key = aggregate_cache_key(
                task.request.database_id.as_u64(),
                tid,
                collection,
                group_by,
                aggregates,
                sub_group_by,
                sub_aggregates,
            );
            if let Some(cached) = self.aggregate_cache.get(&cache_key) {
                debug!(core = self.core_id, %collection, "aggregate cache hit");
                return self.response_with_payload(task, cached.clone());
            }
        }

        // Fast path: index-backed COUNT/GROUP BY. A computed group key
        // (`field: None`) has no physical column to scan an index on, so this
        // path is skipped for it and aggregation falls through to the streaming
        // (expression-evaluating) path below.
        if group_by.len() == 1
            && group_by.iter().all(|s| s.field.is_some())
            && filters.is_empty()
            && having.is_empty()
            && aggregates.len() == 1
            && aggregates[0].expr.is_none()
            && aggregates[0].function == "count"
            && let Some(field) = group_by[0].field.as_deref()
        {
            let out_name = group_by[0].output_name.as_str();
            if let Ok(groups) = self.sparse.scan_index_groups(
                task.request.database_id.as_u64(),
                tid,
                collection,
                field,
            ) && !groups.is_empty()
            {
                let mut payload_buf = Vec::with_capacity(groups.len() * 64);
                let row_count = groups.len().min(limit);
                let count_key = aggregates[0]
                    .user_alias
                    .clone()
                    .unwrap_or_else(|| canonical_agg_key("count", "*"));
                msgpack_scan::write_array_header(&mut payload_buf, row_count);
                for (value, count) in groups.into_iter().take(limit) {
                    msgpack_scan::write_map_header(&mut payload_buf, 2);
                    msgpack_scan::write_kv_str(&mut payload_buf, out_name, &value);
                    msgpack_scan::write_kv_i64(&mut payload_buf, &count_key, count as i64);
                }
                return match Ok::<Vec<u8>, crate::Error>(payload_buf) {
                    Ok(payload) => self.response_with_payload(task, payload),
                    Err(e) => self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: e.to_string(),
                        },
                    ),
                };
            }
        }

        let scan_limit = self.query_tuning.aggregate_scan_cap;

        let mt_key = (
            task.request.database_id,
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        let columnar_mt = self
            .columnar_memtables
            .get(&mt_key)
            .filter(|mt| !mt.is_empty());

        // Fast path: native columnar aggregation. This path keys on physical
        // column names and has no expression-evaluation capability, so a
        // computed group key (`field: None`) diverts it to the streaming path
        // below rather than silently mis-grouping on the dropped key.
        if let Some(mt) = columnar_mt.filter(|_| {
            sub_group_by.is_empty()
                && sub_aggregates.is_empty()
                && group_by.iter().all(|s| s.field.is_some())
        }) {
            let filter_predicates: Vec<ScanFilter> = if filters.is_empty() {
                Vec::new()
            } else {
                match zerompk::from_msgpack(filters) {
                    Ok(f) => f,
                    Err(e) => {
                        tracing::warn!(core = self.core_id, error = %e, "filter predicate deserialization failed");
                        Vec::new()
                    }
                }
            };

            // Physical column names for the group-key fields, used by the
            // paths that key on raw column names (grouping-set expansion,
            // native columnar aggregation). For a bare column this is the
            // same string the spec emits under, so those paths behave
            // identically.
            let group_fields: Vec<String> =
                group_by.iter().filter_map(|s| s.field.clone()).collect();
            let legacy_aggs = legacy_aggregate_pairs(aggregates);
            let columnar_spill_dir = self
                .data_dir
                .join("groupby-spill")
                .join(format!("core-{}-columnar", self.core_id));
            let columnar_spill_cap = self.query_tuning.groupby_max_groups_in_mem;
            if let Some(mut agg_result) = legacy_aggs.and_then(|pairs| {
                super::super::columnar_agg::try_columnar_aggregate(
                    &super::super::columnar_agg::ColumnarAggParams {
                        mt,
                        group_by: &group_fields,
                        aggregates: &pairs,
                        filters: &filter_predicates,
                        limit,
                        scan_limit,
                        spill_dir: &columnar_spill_dir,
                        spill_cap: columnar_spill_cap,
                        governor: self.governor.clone(),
                        db: task.request.database_id,
                        tenant: task.request.tenant_id,
                    },
                )
            }) {
                if !having.is_empty() {
                    let having_predicates: Vec<ScanFilter> = match zerompk::from_msgpack(having) {
                        Ok(h) => h,
                        Err(e) => {
                            tracing::warn!(core = self.core_id, error = %e, "having predicate deserialization failed");
                            Vec::new()
                        }
                    };
                    if !having_predicates.is_empty() {
                        agg_result.rows.retain(|row| {
                            let mp = nodedb_types::json_to_msgpack_or_empty(row);
                            having_predicates.iter().all(|f| f.matches_binary(&mp))
                        });
                    }
                }

                apply_user_aliases_to_rows(&mut agg_result.rows, aggregates);
                // Post-aggregate ORDER BY: sort the finalised group rows
                // before truncating to LIMIT so the visible top-N
                // reflects the requested sort, not hash-map iteration
                // order.
                sort_aggregated_rows(&mut agg_result.rows, sort_keys);
                agg_result.rows.truncate(limit);

                return match crate::data::executor::response_codec::encode_json_vec(
                    &agg_result.rows,
                ) {
                    Ok(payload) => {
                        if filters.is_empty() && having.is_empty() {
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
                };
            }
        }

        // ── Streaming aggregation (per-shard collection scan) ──────────────
        // Bitemporal collections keep every write on the versioned sparse
        // table; the plain `scan_collection` reads the non-versioned namespace
        // and would return zero rows (making COUNT/SUM/etc. see nothing). Route
        // the current-state scan to the versioned table for those collections.
        // Both paths return the same normalized `(doc_id, msgpack)` shape, so
        // the downstream aggregation is identical. Non-bitemporal collections
        // keep the exact `scan_collection` path unchanged.
        let scan_result = if self.is_bitemporal(task.request.database_id.as_u64(), tid, collection)
        {
            self.scan_collection_versioned_current(
                task.request.database_id.as_u64(),
                tid,
                collection,
                scan_limit,
            )
        } else {
            self.scan_collection(
                task.request.database_id.as_u64(),
                tid,
                collection,
                scan_limit,
            )
        };
        let docs = match scan_result {
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
        self.aggregate_over_docs(super::streaming::over_docs::AggregateOverDocsParams {
            task,
            collection,
            cache_tid: Some(tid),
            docs,
            group_by,
            aggregates,
            filters,
            having,
            limit,
            sub_group_by,
            sub_aggregates,
            sort_keys,
        })
    }
}
