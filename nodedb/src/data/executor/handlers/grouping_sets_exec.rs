// SPDX-License-Identifier: BUSL-1.1

//! ROLLUP / CUBE / GROUPING SETS execution.
//!
//! For each grouping set we run the existing single-set aggregation path,
//! NULL-fill columns that are absent from that set, inject a `__grouping_id`
//! bitmask (bit i = 1 when group_by[i] is present), then union all result rows.
//!
//! The `GROUPING(col)` function evaluates to `(__grouping_id >> i) & 1 ^ 1`
//! where `i` is the canonical index of `col` — 0 when the column is a real
//! aggregated value, 1 when it was NULL-filled for this set.

use std::collections::HashMap;

use sonic_rs;

use super::accum::GroupState;
use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::{AggregateSpec, GroupKeySpec};
use nodedb_query::msgpack_scan;

/// Hidden column name carrying the grouping bitmask for `GROUPING()` support.
pub const GROUPING_ID_COL: &str = "__grouping_id";

/// Borrowed inputs to [`execute_grouping_sets`]: the target collection, the
/// GROUP BY / aggregate / filter / HAVING specs, and the expanded grouping
/// sets to union results over.
pub(super) struct GroupingSetsParams<'a> {
    pub task: &'a ExecutionTask,
    pub tid: u64,
    pub collection: &'a str,
    pub group_by: &'a [String],
    pub aggregates: &'a [AggregateSpec],
    pub filters: &'a [u8],
    pub having: &'a [u8],
    pub limit: usize,
    pub grouping_sets: &'a [Vec<u32>],
}

/// Execute a multi-set aggregate (ROLLUP / CUBE / GROUPING SETS).
///
/// Scans the collection once per grouping set, computes aggregates using only
/// the keys active in that set, NULL-fills the absent keys, and unions rows.
pub(super) fn execute_grouping_sets(
    core: &mut CoreLoop,
    params: GroupingSetsParams<'_>,
) -> Response {
    let GroupingSetsParams {
        task,
        tid,
        collection,
        group_by,
        aggregates,
        filters,
        having,
        limit,
        grouping_sets,
    } = params;

    let filter_predicates: Vec<ScanFilter> = if filters.is_empty() {
        Vec::new()
    } else {
        match zerompk::from_msgpack(filters) {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!(error = %e, "grouping sets: filter deserialization failed");
                Vec::new()
            }
        }
    };

    let having_predicates: Vec<ScanFilter> = if having.is_empty() {
        Vec::new()
    } else {
        match zerompk::from_msgpack(having) {
            Ok(f) => f,
            Err(e) => {
                tracing::warn!(error = %e, "grouping sets: HAVING deserialization failed");
                Vec::new()
            }
        }
    };

    let scan_limit = core.query_tuning.aggregate_scan_cap;
    let use_field_index = !filter_predicates.is_empty() || !group_by.is_empty();

    // Scan documents once, collect them all into memory.
    // This is necessary because we need to run multiple passes (one per set).
    // Memory is bounded by `scan_limit` from query tuning.
    let docs_result = core.scan_collection(
        task.request.database_id.as_u64(),
        tid,
        collection,
        scan_limit,
    );
    let owned_docs: Vec<Vec<u8>> = match docs_result {
        Ok(docs) => docs.into_iter().map(|(_, v)| v.to_vec()).collect(),
        Err(e) => {
            return core.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            );
        }
    };

    // Separate real aggregates from GROUPING() pseudo-aggregates.
    // GroupState only accumulates real aggregates; GROUPING is computed
    // post-finalization from the per-set bitmask.
    let real_aggregates: Vec<&AggregateSpec> = aggregates
        .iter()
        .filter(|a| a.function != "grouping")
        .collect();
    let real_agg_slice: Vec<AggregateSpec> = real_aggregates.iter().map(|a| (*a).clone()).collect();
    let grouping_aggs: Vec<&AggregateSpec> = aggregates
        .iter()
        .filter(|a| a.function == "grouping")
        .collect();

    let mut all_rows: Vec<serde_json::Value> = Vec::new();

    for set in grouping_sets {
        // Build the active key list for this set.
        let active_keys: Vec<&String> = set
            .iter()
            .filter_map(|&idx| group_by.get(idx as usize))
            .collect();

        // Grouping bitmask: bit i set means group_by[i] IS present in this set.
        let grouping_id: u64 = set.iter().fold(0u64, |acc, &i| acc | (1u64 << i));

        // Bare-column specs for the active keys, so this set's group key uses
        // the identical `build_group_key` encoding as the single-set path.
        let active_specs: Vec<GroupKeySpec> = active_keys
            .iter()
            .map(|s| GroupKeySpec::column(s.as_str()))
            .collect();

        // Aggregate over the documents using only active keys and real aggregates.
        let mut groups: HashMap<String, GroupState> = HashMap::new();

        for raw in &owned_docs {
            // Apply WHERE filters.
            if !filter_predicates.is_empty() {
                if use_field_index {
                    let idx = msgpack_scan::FieldIndex::build(raw, 0)
                        .unwrap_or_else(msgpack_scan::FieldIndex::empty);
                    match ScanFilter::all_match_binary_indexed(&filter_predicates, raw, &idx) {
                        Ok(true) => {}
                        Ok(false) => continue,
                        Err(e) => {
                            return core.response_error(task, ErrorCode::from(e));
                        }
                    }
                } else {
                    match ScanFilter::all_match_binary(&filter_predicates, raw) {
                        Ok(true) => {}
                        Ok(false) => continue,
                        Err(e) => {
                            return core.response_error(task, ErrorCode::from(e));
                        }
                    }
                }
            }

            let group_key = match msgpack_scan::build_group_key(raw, &active_specs) {
                Ok(k) => k,
                Err(e) => return core.response_error(task, ErrorCode::from(e)),
            };
            if let Err(e) = groups
                .entry(group_key)
                .or_insert_with(|| GroupState::new(&real_agg_slice))
                .feed(&real_agg_slice, raw)
            {
                return core.response_error(task, ErrorCode::from(e));
            }
        }

        // For an empty grouping set (grand-total), produce one aggregate row
        // even when no documents exist in the collection.
        if active_keys.is_empty() && groups.is_empty() {
            let mut grand = GroupState::new(&real_agg_slice);
            for raw in &owned_docs {
                match ScanFilter::all_match_binary(&filter_predicates, raw) {
                    Ok(true) => {
                        if let Err(e) = grand.feed(&real_agg_slice, raw) {
                            return core.response_error(task, ErrorCode::from(e));
                        }
                    }
                    Ok(false) => {}
                    Err(e) => {
                        return core.response_error(task, ErrorCode::from(e));
                    }
                }
            }
            groups.insert(String::new(), grand);
        }

        for (group_key, state) in groups {
            let mut row = serde_json::Map::new();

            // Parse the group key (JSON array of active key values).
            let active_values: Vec<serde_json::Value> =
                if active_keys.is_empty() || group_key.is_empty() {
                    Vec::new()
                } else {
                    sonic_rs::from_str::<Vec<serde_json::Value>>(&group_key).unwrap_or_default()
                };

            // Emit all canonical keys: active ones get their value, absent ones get NULL.
            for (i, key) in group_by.iter().enumerate() {
                let present = set.contains(&(i as u32));
                if present {
                    // Find position of this key in active_keys.
                    let pos = active_keys
                        .iter()
                        .position(|k| *k == key)
                        .unwrap_or(usize::MAX);
                    let val = active_values
                        .get(pos)
                        .cloned()
                        .unwrap_or(serde_json::Value::Null);
                    row.insert(key.clone(), val);
                } else {
                    row.insert(key.clone(), serde_json::Value::Null);
                }
            }

            // Real aggregate results.
            let finalized = match state.finalize(&real_agg_slice) {
                Ok(finalized) => finalized,
                Err(e) => return core.response_error(task, ErrorCode::from(e)),
            };
            for (alias, val) in finalized {
                row.insert(alias, val.into());
            }

            // GROUPING(col) pseudo-aggregates: compute from the bitmask.
            // `agg.field` encodes the canonical key index as a decimal string.
            // Returns 0 if the key is present (real value), 1 if absent (NULL-filled).
            for agg in &grouping_aggs {
                let col_idx: u64 = agg.field.parse().unwrap_or(u64::MAX);
                let bit_present = (grouping_id >> col_idx) & 1;
                let grouping_val = 1u64 ^ bit_present;
                let output_alias = agg.user_alias.as_deref().unwrap_or(&agg.alias);
                row.insert(
                    output_alias.to_string(),
                    serde_json::Value::Number(serde_json::Number::from(grouping_val)),
                );
            }

            // Hidden grouping bitmask column — available for downstream use.
            row.insert(
                GROUPING_ID_COL.to_string(),
                serde_json::Value::Number(serde_json::Number::from(grouping_id)),
            );

            all_rows.push(serde_json::Value::Object(row));
        }
    }

    // Apply HAVING.
    if !having_predicates.is_empty() {
        // `Vec::retain`'s closure must return `bool`, so an evaluation error
        // in a HAVING predicate is captured via this side-channel and checked
        // once the retain finishes.
        let predicate_err: std::cell::RefCell<Option<nodedb_query::EvalError>> =
            std::cell::RefCell::new(None);
        all_rows.retain(|row| {
            if predicate_err.borrow().is_some() {
                return true;
            }
            let mp = nodedb_types::json_to_msgpack_or_empty(row);
            match ScanFilter::all_match_binary(&having_predicates, &mp) {
                Ok(keep) => keep,
                Err(e) => {
                    predicate_err.replace(Some(e));
                    true
                }
            }
        });
        if let Some(e) = predicate_err.take() {
            return core.response_error(task, ErrorCode::from(e));
        }
    }

    // Apply user aliases for real aggregates (grouping aliases were applied above).
    apply_user_aliases(&mut all_rows, &real_agg_slice);

    all_rows.truncate(limit);

    match super::super::response_codec::encode_json_vec_as_msgpack(&all_rows) {
        Ok(payload) => core.response_with_payload(task, payload),
        Err(e) => core.response_error(
            task,
            ErrorCode::Internal {
                detail: e.to_string(),
            },
        ),
    }
}

fn apply_user_aliases(rows: &mut [serde_json::Value], aggregates: &[AggregateSpec]) {
    let renames: Vec<(&str, &str)> = aggregates
        .iter()
        .filter_map(|agg| {
            agg.user_alias
                .as_deref()
                .filter(|alias| *alias != agg.alias)
                .map(|alias| (agg.alias.as_str(), alias))
        })
        .collect();

    if renames.is_empty() {
        return;
    }

    for row in rows {
        if let Some(obj) = row.as_object_mut() {
            for (from, to) in &renames {
                if let Some(value) = obj.remove(*from) {
                    obj.insert((*to).to_string(), value);
                }
            }
        }
    }
}
