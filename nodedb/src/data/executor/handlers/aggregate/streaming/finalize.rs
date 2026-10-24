// SPDX-License-Identifier: BUSL-1.1

//! Finalize phase: turn consolidated per-group `GroupState`s into the encoded
//! Response payload.
//!
//! Split out of `aggregate_over_docs` so the distributed-shuffle consumer
//! (`ShuffleAggregateConsume`) can reuse the identical finalize tail —
//! row build, HAVING, user-alias renaming, ORDER BY, LIMIT, encode — after it
//! has merged partial states from every producer.

use std::collections::HashMap;

use nodedb_types::Value;

use super::super::rows::{apply_user_aliases_to_rows, retain_having, sort_aggregated_rows};
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
            groups.insert("__all__".to_string(), GroupState::new(aggregates)?);
        }

        let mut results: Vec<Value> = Vec::new();

        for (group_key, state) in groups {
            let mut row: HashMap<String, Value> = HashMap::new();

            if !group_by.is_empty() {
                let parts =
                    sonic_rs::from_str::<Vec<serde_json::Value>>(&group_key).map_err(|e| {
                        crate::Error::Codec {
                            detail: format!("aggregate group key decode: {e}"),
                        }
                    })?;
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
                        .map_or(Value::Null, Value::from);
                    row.insert(spec.output_name.clone(), val);
                    part_idx += 1;
                }
            }

            for (alias, val) in state.finalize(aggregates)? {
                row.insert(alias, val);
            }

            if need_sub {
                let sub_map = sub_groups.remove(&group_key).unwrap_or_default();
                let mut sub_results: Vec<Value> = Vec::new();
                for (sub_key, sub_state) in sub_map {
                    let mut sub_row: HashMap<String, Value> = HashMap::new();
                    if let Ok(parts) = sonic_rs::from_str::<Vec<serde_json::Value>>(&sub_key) {
                        for (i, field) in sub_group_by.iter().enumerate() {
                            let val = parts.get(i).cloned().map_or(Value::Null, Value::from);
                            sub_row.insert(field.clone(), val);
                        }
                    }
                    for (alias, val) in sub_state.finalize(sub_aggregates)? {
                        sub_row.insert(alias, val);
                    }
                    let mut sub_value = Value::Object(sub_row);
                    apply_user_aliases_to_rows(
                        std::slice::from_mut(&mut sub_value),
                        sub_aggregates,
                    );
                    sub_results.push(sub_value);
                }
                row.insert("sub_groups".to_string(), Value::Array(sub_results));
            }

            results.push(Value::Object(row));
        }

        let having_predicates =
            crate::bridge::scan_filter::decode_scan_filters(having, "aggregate HAVING")?;
        let crs = self.crs_context();
        retain_having(&mut results, &having_predicates, &crs)?;

        apply_user_aliases_to_rows(&mut results, aggregates);
        // Post-aggregate ORDER BY: sort group rows before
        // truncating so LIMIT picks the requested top-N.
        sort_aggregated_rows(&mut results, sort_keys, &crs)?;
        results.truncate(limit);

        crate::data::executor::response_codec::encode_value_vec(&results)
    }
}
