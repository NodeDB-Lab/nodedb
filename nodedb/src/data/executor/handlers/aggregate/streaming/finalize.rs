// SPDX-License-Identifier: BUSL-1.1

//! Finalize phase: turn consolidated per-group `GroupState`s into the encoded
//! Response payload.
//!
//! Split out of `aggregate_over_docs` so the distributed-shuffle consumer
//! (`ShuffleAggregateConsume`) can reuse the identical finalize tail —
//! row build, HAVING, user-alias renaming, ORDER BY, LIMIT, encode — after it
//! has merged partial states from every producer.

use std::collections::HashMap;

use super::super::rows::{apply_user_aliases_to_rows, sort_aggregated_rows};
use crate::bridge::scan_filter::ScanFilter;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::accum::GroupState;
use nodedb_physical::physical_plan::{AggregateSpec, GroupKeySpec};

/// Borrowed/owned inputs to [`CoreLoop::finalize_groups`]: the consolidated
/// per-group state maps plus the GROUP BY / aggregate / HAVING / sort specs
/// needed to turn them into an encoded response payload.
pub(in crate::data::executor) struct FinalizeGroupsParams<'a> {
    pub groups: HashMap<String, GroupState>,
    pub sub_groups: HashMap<String, HashMap<String, GroupState>>,
    pub group_by: &'a [GroupKeySpec],
    pub aggregates: &'a [AggregateSpec],
    pub having: &'a [u8],
    pub limit: usize,
    pub sub_group_by: &'a [String],
    pub sub_aggregates: &'a [AggregateSpec],
    pub sort_keys: &'a [nodedb_physical::physical_plan::SortKeySpec],
}

impl CoreLoop {
    /// Finalize consolidated per-group states into an encoded response payload.
    ///
    /// Mirrors the tail of `aggregate_over_docs`: each `GroupState` is finalized
    /// into a row (the GROUP BY columns parsed back from the JSON-array group
    /// key plus the aggregate outputs), HAVING is applied, user aliases are
    /// renamed, the rows are sorted by `sort_keys`, truncated to `limit`, and
    /// encoded as a MessagePack array.
    ///
    /// `sub_groups` carries the optional sub-aggregation map (empty for plain
    /// GROUP BY, including the distributed-shuffle path).
    pub(in crate::data::executor) fn finalize_groups(
        &self,
        params: FinalizeGroupsParams<'_>,
    ) -> crate::Result<Vec<u8>> {
        let FinalizeGroupsParams {
            mut groups,
            mut sub_groups,
            group_by,
            aggregates,
            having,
            limit,
            sub_group_by,
            sub_aggregates,
            sort_keys,
        } = params;
        let need_sub = !sub_group_by.is_empty() && !sub_aggregates.is_empty();

        // A scalar aggregate (no GROUP BY) over zero input rows must still
        // emit exactly one row (COUNT -> 0, SUM/AVG/MIN/MAX -> NULL) — SQL
        // semantics for aggregates without a GROUP BY clause. A `GROUP BY`
        // aggregate over zero rows correctly stays at zero groups/rows, so
        // this only fires when there is no GROUP BY at all.
        //
        // This is the single seed site shared by both the local aggregate
        // path (`aggregate_over_docs`) and the distributed-shuffle
        // coordinator (`execute_shuffle_aggregate`, which merges every
        // shard's partial state by group key before calling here). Seeding
        // here — after the merge, not per-shard — guarantees exactly one
        // final row: if any shard actually matched rows, `groups` already
        // holds the real merged state and this branch never fires; only a
        // fully empty merge (every shard matched nothing) synthesizes the
        // one identity row.
        if group_by.is_empty() && groups.is_empty() {
            groups.insert("__all__".to_string(), GroupState::new(aggregates));
        }

        let mut results: Vec<serde_json::Value> = Vec::new();

        for (group_key, state) in groups {
            let mut row = serde_json::Map::new();

            if !group_by.is_empty()
                && let Ok(parts) = sonic_rs::from_str::<Vec<serde_json::Value>>(&group_key)
            {
                // One positional slot per key that contributed to the key bytes
                // (a bare `field` or a computed `expr`); emit each under its
                // `output_name` — for a computed key this is the evaluated
                // value that `build_group_key` folded into the key.
                let mut part_idx = 0usize;
                for spec in group_by {
                    if spec.field.is_none() && spec.expr.is_none() {
                        continue;
                    }
                    let val = parts
                        .get(part_idx)
                        .cloned()
                        .unwrap_or(serde_json::Value::Null);
                    row.insert(spec.output_name.clone(), val);
                    part_idx += 1;
                }
            }

            for (alias, val) in state.finalize(aggregates)? {
                let json_val: serde_json::Value = val.into();
                row.insert(alias, json_val);
            }

            if need_sub {
                let sub_map = sub_groups.remove(&group_key).unwrap_or_default();
                let mut sub_results: Vec<serde_json::Value> = Vec::new();
                for (sub_key, sub_state) in sub_map {
                    let mut sub_row = serde_json::Map::new();
                    if let Ok(parts) = sonic_rs::from_str::<Vec<serde_json::Value>>(&sub_key) {
                        for (i, field) in sub_group_by.iter().enumerate() {
                            let val = parts.get(i).cloned().unwrap_or(serde_json::Value::Null);
                            sub_row.insert(field.clone(), val);
                        }
                    }
                    for (alias, val) in sub_state.finalize(sub_aggregates)? {
                        let json_val: serde_json::Value = val.into();
                        sub_row.insert(alias, json_val);
                    }
                    let mut sub_value = serde_json::Value::Object(sub_row);
                    apply_user_aliases_to_rows(
                        std::slice::from_mut(&mut sub_value),
                        sub_aggregates,
                    );
                    sub_results.push(sub_value);
                }
                row.insert(
                    "sub_groups".to_string(),
                    serde_json::Value::Array(sub_results),
                );
            }

            results.push(serde_json::Value::Object(row));
        }

        if !having.is_empty() {
            let having_predicates: Vec<ScanFilter> = match zerompk::from_msgpack(having) {
                Ok(f) => f,
                Err(e) => {
                    tracing::warn!(
                        core = self.core_id,
                        error = %e,
                        "HAVING predicate deserialization failed (schemaless)"
                    );
                    Vec::new()
                }
            };
            if !having_predicates.is_empty() {
                // `Vec::retain`'s closure must return `bool`, so an evaluation
                // error in a HAVING predicate is captured via this side-channel
                // and checked once the retain finishes.
                let predicate_err: std::cell::RefCell<Option<nodedb_query::EvalError>> =
                    std::cell::RefCell::new(None);
                results.retain(|row| {
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
                    return Err(crate::Error::from(e));
                }
            }
        }

        apply_user_aliases_to_rows(&mut results, aggregates);
        // Post-aggregate ORDER BY: sort group rows before
        // truncating so LIMIT picks the requested top-N.
        sort_aggregated_rows(&mut results, sort_keys)?;
        results.truncate(limit);

        crate::data::executor::response_codec::encode_json_vec_as_msgpack(&results)
    }
}
