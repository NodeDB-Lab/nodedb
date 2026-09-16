// SPDX-License-Identifier: BUSL-1.1

//! Time-bucket grouping: rows keyed by `time_bucket(ts)` plus the group
//! columns, resolved to `"bucket_ts\0group1\0group2"` string keys.

use rustc_hash::FxHashMap;

use super::super::super::columnar_agg::AggAccum;
use super::super::types::{GroupedAggResult, accumulate_row, for_each_set_bit};
use super::dispatch::GroupedScanInputs;
use super::keys::{key_part, push_key_part};

/// Time-bucket aggregation with integer keys.
///
/// Packs (bucket_ts, group_key_parts...) into a `Vec<u64>`:
/// - `parts[0]` = bucket_ts as u64
/// - `parts[1..]` = group column values (symbol IDs, i64, f64 bits)
///
/// Resolves to string keys with "bucket_ts\0group1\0group2" format
/// so `emit_grouped_results` can parse them.
pub(super) fn aggregate_with_bucket<'a>(
    inputs: GroupedScanInputs<'a>,
    timestamps: &[i64],
    bucket_interval_ms: i64,
    sym_lookup: &dyn Fn(usize) -> Option<&'a nodedb_types::timeseries::SymbolDictionary>,
) -> GroupedAggResult {
    let GroupedScanInputs {
        resolved,
        columns,
        mask,
        row_count,
        num_aggs,
    } = inputs;
    let key_len = 1 + resolved.group_cols.len(); // bucket + group columns

    let mut groups: FxHashMap<Vec<u64>, Vec<AggAccum>> = FxHashMap::default();

    for_each_set_bit(mask, row_count, |row_idx| {
        let bucket =
            super::super::super::time_bucket::time_bucket(bucket_interval_ms, timestamps[row_idx]);

        let mut key = Vec::with_capacity(key_len);
        key.push(bucket as u64);

        // Pack group-by columns as integers.
        for &(col_idx, ty) in &resolved.group_cols {
            key.push(key_part(ty, columns[col_idx], row_idx));
        }

        let accums = groups
            .entry(key)
            .or_insert_with(|| (0..num_aggs).map(|_| AggAccum::default()).collect());
        accumulate_row(accums, resolved, columns, row_idx);
    });

    // Resolve integer keys to string keys: "bucket_ts\0group1\0group2"
    let mut result = GroupedAggResult::new(num_aggs);
    for (key_parts, accums) in groups {
        let bucket_ts = key_parts[0] as i64;
        let mut str_key = bucket_ts.to_string();

        for (i, &part) in key_parts[1..].iter().enumerate() {
            str_key.push('\0');
            if i < resolved.group_cols.len() {
                let (col_idx, ty) = resolved.group_cols[i];
                push_key_part(&mut str_key, col_idx, ty, part, sym_lookup);
            }
        }

        result.merge_group(str_key, &accums);
    }

    result
}
