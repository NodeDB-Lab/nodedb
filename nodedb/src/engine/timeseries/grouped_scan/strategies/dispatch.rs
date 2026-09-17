// SPDX-License-Identifier: BUSL-1.1

//! Strategy selection: picks the grouping strategy for a scan from its
//! GROUP BY shape, symbol cardinality, and row count, then resolves the
//! integer keys to strings.

use super::super::super::columnar_memtable::{ColumnData, ColumnType};
use super::super::types::{GroupedAggResult, ResolvedSchema};
use super::bucket::aggregate_with_bucket;
use super::hashed::{
    aggregate_direct_index, aggregate_hash_generic, aggregate_hash_u32, aggregate_no_group,
    aggregate_two_level,
};
use super::keys::resolve_group_key;

const DIRECT_INDEX_MAX_CARDINALITY: u32 = 65536;

/// Row-level scan inputs shared by every grouping strategy: the resolved
/// schema, per-column data, the row-selection bitmask, row count, and
/// aggregate count. Bundled so the top-level dispatch/bucket entry points
/// stay within clippy's argument budget.
#[derive(Clone, Copy)]
pub(in crate::engine::timeseries::grouped_scan) struct GroupedScanInputs<'a> {
    pub resolved: &'a ResolvedSchema,
    pub columns: &'a [Option<&'a ColumnData>],
    pub mask: &'a [u64],
    pub row_count: usize,
    pub num_aggs: usize,
}

pub(in crate::engine::timeseries::grouped_scan) fn dispatch_grouping<'a>(
    inputs: GroupedScanInputs<'a>,
    group_by: &[String],
    sym_lookup: &dyn Fn(usize) -> Option<&'a nodedb_types::timeseries::SymbolDictionary>,
    timestamps: Option<&[i64]>,
    bucket_interval_ms: i64,
) -> GroupedAggResult {
    let GroupedScanInputs {
        resolved,
        columns,
        mask,
        row_count,
        num_aggs,
    } = inputs;
    let has_bucket = bucket_interval_ms > 0 && timestamps.is_some();

    if has_bucket && let Some(ts) = timestamps {
        return aggregate_with_bucket(inputs, ts, bucket_interval_ms, sym_lookup);
    }

    let local_groups = if group_by.is_empty() {
        aggregate_no_group(resolved, columns, mask, row_count, num_aggs)
    } else if group_by.len() == 1 && resolved.group_cols[0].1 == ColumnType::Symbol {
        let (col_idx, _) = resolved.group_cols[0];
        if let Some(data) = columns[col_idx] {
            let sym_ids = data.as_symbols();
            let cardinality = sym_lookup(col_idx).map(|d| d.len() as u32).unwrap_or(0);
            if cardinality <= DIRECT_INDEX_MAX_CARDINALITY && cardinality > 0 {
                aggregate_direct_index(
                    resolved,
                    columns,
                    mask,
                    row_count,
                    num_aggs,
                    sym_ids,
                    cardinality,
                )
            } else {
                aggregate_hash_u32(resolved, columns, mask, row_count, num_aggs, sym_ids)
            }
        } else {
            aggregate_hash_generic(resolved, columns, mask, row_count, num_aggs)
        }
    } else if row_count > 100_000 {
        aggregate_two_level(resolved, columns, mask, row_count, num_aggs)
    } else {
        aggregate_hash_generic(resolved, columns, mask, row_count, num_aggs)
    };

    // Resolve integer keys to strings.
    let mut result = GroupedAggResult::new(num_aggs);
    for (int_key, accums) in local_groups {
        let str_key = resolve_group_key(&int_key, resolved, sym_lookup);
        result.merge_group(str_key, &accums);
    }
    result
}
