// SPDX-License-Identifier: BUSL-1.1

//! ROLLUP / CUBE / GROUPING SETS execution.
//!
//! The WHERE predicates are applied once. Then each grouping set groups the
//! surviving rows by the keys active in that set, NULL-fills the keys absent
//! from it, and adds a `__grouping_id` bitmask (bit i = 1 when group_by[i] is
//! present). The rows of every set are unioned, then HAVING, ORDER BY and
//! LIMIT apply to the union.
//!
//! `GROUPING(col)` evaluates to `((__grouping_id >> i) & 1) ^ 1`, where `i`
//! is the index of `col` in the GROUP BY list: 0 when the column holds a real
//! grouped value, 1 when it was NULL-filled for this set.

use std::collections::HashMap;

use nodedb_types::Value;

use super::accum::GroupState;
use super::aggregate::rows::{apply_user_aliases_to_rows, retain_having, sort_aggregated_rows};
use crate::bridge::envelope::{ErrorCode, Response};
use crate::bridge::scan_filter::{ScanFilter, decode_scan_filters};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use nodedb_physical::physical_plan::{AggregateSpec, GroupKeySpec, SortKeySpec};
use nodedb_query::msgpack_scan;

/// Hidden column name carrying the grouping bitmask for `GROUPING()` support.
pub const GROUPING_ID_COL: &str = "__grouping_id";

/// The widest GROUP BY list a grouping bitmask can describe.
const MAX_GROUPING_KEYS: u32 = u64::BITS;

/// Inputs to [`execute_grouping_sets`]: the rows to aggregate, the GROUP BY /
/// aggregate / filter / HAVING / sort specs, and the expanded grouping sets.
pub(super) struct GroupingSetsParams<'a> {
    pub task: &'a ExecutionTask,
    pub docs: Vec<(String, Vec<u8>)>,
    pub group_by: &'a [GroupKeySpec],
    pub aggregates: &'a [AggregateSpec],
    pub filters: &'a [u8],
    pub having: &'a [u8],
    pub limit: usize,
    pub grouping_sets: &'a [Vec<u32>],
    pub sort_keys: &'a [SortKeySpec],
}

/// Execute a multi-set aggregate (ROLLUP / CUBE / GROUPING SETS).
pub(super) fn execute_grouping_sets(core: &CoreLoop, params: GroupingSetsParams<'_>) -> Response {
    let task = params.task;
    let crs = core.crs_context();
    match grouping_set_rows(params, &crs) {
        Ok(payload) => core.response_with_payload(task, payload),
        Err(e) => core.response_error(task, ErrorCode::from(e)),
    }
}

/// Aggregate every grouping set and encode the union as a MessagePack array.
fn grouping_set_rows(
    params: GroupingSetsParams<'_>,
    crs: &nodedb_crs::CrsContext<'_>,
) -> crate::Result<Vec<u8>> {
    let GroupingSetsParams {
        task: _,
        docs,
        group_by,
        aggregates,
        filters,
        having,
        limit,
        grouping_sets,
        sort_keys,
    } = params;

    let filter_predicates = decode_scan_filters(filters, "grouping sets WHERE filter")?;
    let having_predicates = decode_scan_filters(having, "grouping sets HAVING")?;

    // GroupState accumulates real aggregates only. GROUPING() is computed
    // from each set's bitmask after finalization.
    let (grouping_aggs, real_aggs): (Vec<AggregateSpec>, Vec<AggregateSpec>) = aggregates
        .iter()
        .cloned()
        .partition(|a| a.function == "grouping");
    let grouping_cols = grouping_aggs
        .iter()
        .map(|agg| grouping_column(agg, group_by.len()))
        .collect::<crate::Result<Vec<u32>>>()?;

    let rows = matching_rows(&docs, &filter_predicates, crs)?;

    let mut all_rows: Vec<Value> = Vec::new();
    for set in grouping_sets {
        let grouping_id = grouping_bitmask(set, group_by.len())?;
        // The active keys in GROUP BY order, so the group key's slots follow
        // the canonical key order whatever order the set lists them in.
        let mut active: Vec<usize> = set.iter().map(|&i| i as usize).collect();
        active.sort_unstable();
        active.dedup();
        let active_specs: Vec<GroupKeySpec> = active.iter().map(|&i| group_by[i].clone()).collect();

        let mut groups: HashMap<String, GroupState> = HashMap::new();
        for raw in &rows {
            let key = msgpack_scan::build_group_key(raw, &active_specs, crs)?;
            let state = match groups.entry(key) {
                std::collections::hash_map::Entry::Occupied(slot) => slot.into_mut(),
                std::collections::hash_map::Entry::Vacant(slot) => {
                    slot.insert(GroupState::new(&real_aggs)?)
                }
            };
            state.feed(&real_aggs, raw, crs)?;
        }
        // The grand-total set yields one row even over zero input rows.
        if active.is_empty() && groups.is_empty() {
            groups.insert(String::new(), GroupState::new(&real_aggs)?);
        }

        for (group_key, state) in groups {
            let mut row: HashMap<String, Value> = HashMap::new();
            let values = active_key_values(&group_key, &active_specs)?;
            for spec in group_by {
                row.insert(spec.output_name.clone(), Value::Null);
            }
            for (spec, value) in active_specs.iter().zip(values) {
                row.insert(spec.output_name.clone(), value);
            }
            for (alias, value) in state.finalize(&real_aggs)? {
                row.insert(alias, value);
            }
            for (agg, &col) in grouping_aggs.iter().zip(&grouping_cols) {
                let present = (grouping_id >> col) & 1;
                let output = agg.user_alias.as_deref().unwrap_or(&agg.alias);
                row.insert(output.to_string(), Value::from_u64(present ^ 1));
            }
            row.insert(GROUPING_ID_COL.to_string(), Value::from_u64(grouping_id));
            all_rows.push(Value::Object(row));
        }
    }

    // HAVING is WHERE-shaped: an evaluation error fails the statement.
    retain_having(&mut all_rows, &having_predicates, crs)?;
    // GROUPING() outputs already carry their user aliases.
    apply_user_aliases_to_rows(&mut all_rows, &real_aggs);
    sort_aggregated_rows(&mut all_rows, sort_keys, crs)?;
    all_rows.truncate(limit);

    super::super::response_codec::encode_value_vec(&all_rows)
}

/// The rows the WHERE predicates admit.
fn matching_rows<'d>(
    docs: &'d [(String, Vec<u8>)],
    filters: &[ScanFilter],
    crs: &nodedb_crs::CrsContext<'_>,
) -> crate::Result<Vec<&'d [u8]>> {
    let mut rows = Vec::with_capacity(docs.len());
    for (_, raw) in docs {
        if ScanFilter::all_match_binary(filters, raw, crs)? {
            rows.push(raw.as_slice());
        }
    }
    Ok(rows)
}

/// The presence bitmask of one grouping set: bit i is set when group_by[i]
/// is in the set.
fn grouping_bitmask(set: &[u32], key_count: usize) -> crate::Result<u64> {
    set.iter().try_fold(0u64, |acc, &i| {
        if i >= MAX_GROUPING_KEYS || i as usize >= key_count {
            return Err(crate::Error::PlanError {
                detail: format!(
                    "grouping set names GROUP BY key {i}, but the GROUP BY list has {key_count} \
                     keys"
                ),
            });
        }
        Ok(acc | (1u64 << i))
    })
}

/// The GROUP BY index a `GROUPING(col)` pseudo-aggregate names. The planner
/// writes it into `field` as a decimal index.
fn grouping_column(agg: &AggregateSpec, key_count: usize) -> crate::Result<u32> {
    let index: u32 = agg.field.parse().map_err(|_| crate::Error::PlanError {
        detail: format!(
            "GROUPING() argument '{}' is not a GROUP BY key index",
            agg.field
        ),
    })?;
    if index >= MAX_GROUPING_KEYS || index as usize >= key_count {
        return Err(crate::Error::PlanError {
            detail: format!(
                "GROUPING() names GROUP BY key {index}, but the GROUP BY list has {key_count} keys"
            ),
        });
    }
    Ok(index)
}

/// The grouped values of one group key, one per active key spec.
///
/// `build_group_key` writes one slot per spec that has a column or an
/// expression, so a spec with neither takes NULL. A key that does not parse
/// is an error.
fn active_key_values(group_key: &str, active_specs: &[GroupKeySpec]) -> crate::Result<Vec<Value>> {
    if active_specs.is_empty() {
        return Ok(Vec::new());
    }
    let parts: Vec<serde_json::Value> =
        sonic_rs::from_str(group_key).map_err(|e| crate::Error::Codec {
            detail: format!("grouping set group key decode: {e}"),
        })?;
    let mut parts = parts.into_iter();
    let mut values = Vec::with_capacity(active_specs.len());
    for spec in active_specs {
        if spec.field.is_none() && spec.expr.is_none() {
            values.push(Value::Null);
            continue;
        }
        let part = parts.next().ok_or(crate::Error::Codec {
            detail: format!("grouping set group key '{group_key}' has too few values"),
        })?;
        values.push(Value::from(part));
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(function: &str, field: &str) -> AggregateSpec {
        AggregateSpec {
            function: function.to_string(),
            alias: format!("{function}({field})"),
            user_alias: None,
            field: field.to_string(),
            expr: None,
            param: None,
        }
    }

    #[test]
    fn bitmask_sets_one_bit_per_key() {
        assert_eq!(grouping_bitmask(&[0, 2], 3).expect("valid"), 0b101);
        assert_eq!(grouping_bitmask(&[], 3).expect("valid"), 0);
    }

    #[test]
    fn bitmask_refuses_out_of_range_key() {
        assert!(grouping_bitmask(&[3], 3).is_err());
        assert!(grouping_bitmask(&[64], 100).is_err());
    }

    #[test]
    fn grouping_column_refuses_bad_index() {
        assert_eq!(
            grouping_column(&spec("grouping", "1"), 2).expect("valid"),
            1
        );
        assert!(grouping_column(&spec("grouping", "x"), 2).is_err());
        assert!(grouping_column(&spec("grouping", "2"), 2).is_err());
    }

    #[test]
    fn key_values_follow_the_specs() {
        let specs = vec![GroupKeySpec::column("a"), GroupKeySpec::column("b")];
        let values = active_key_values(r#"["x",3]"#, &specs).expect("parses");
        assert_eq!(values.len(), 2);
        assert!(matches!(&values[0], Value::String(s) if s == "x"));
    }

    #[test]
    fn unparsable_key_errors() {
        let specs = vec![GroupKeySpec::column("a")];
        assert!(active_key_values("not json", &specs).is_err());
        assert!(active_key_values("[]", &specs).is_err());
    }
}
