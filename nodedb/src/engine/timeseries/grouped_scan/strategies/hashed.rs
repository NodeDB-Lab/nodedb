// SPDX-License-Identifier: BUSL-1.1

//! Grouping strategies over integer keys: no-group, direct-index, FxHash
//! on a single symbol column, generic multi-column hash, and two-level
//! bucketed hash for high cardinality.

use rustc_hash::FxHashMap;

use super::super::super::columnar_agg::AggAccum;
use super::super::super::columnar_memtable::ColumnData;
use super::super::types::{ResolvedSchema, accumulate_row, for_each_set_bit};
use super::keys::{IntGroupKey, build_generic_key, fx_hash_key};

pub(super) fn aggregate_no_group(
    resolved: &ResolvedSchema,
    columns: &[Option<&ColumnData>],
    mask: &[u64],
    row_count: usize,
    num_aggs: usize,
) -> Vec<(IntGroupKey, Vec<AggAccum>)> {
    let mut accums: Vec<AggAccum> = (0..num_aggs).map(|_| AggAccum::default()).collect();
    for_each_set_bit(mask, row_count, |row_idx| {
        accumulate_row(&mut accums, resolved, columns, row_idx);
    });
    vec![(IntGroupKey::None, accums)]
}

pub(super) fn aggregate_direct_index(
    resolved: &ResolvedSchema,
    columns: &[Option<&ColumnData>],
    mask: &[u64],
    row_count: usize,
    num_aggs: usize,
    sym_ids: &[u32],
    cardinality: u32,
) -> Vec<(IntGroupKey, Vec<AggAccum>)> {
    let card = cardinality as usize;
    let mut table: Vec<Vec<AggAccum>> = (0..card)
        .map(|_| (0..num_aggs).map(|_| AggAccum::default()).collect())
        .collect();

    for_each_set_bit(mask, row_count, |row_idx| {
        let id = sym_ids[row_idx] as usize;
        if id < card {
            accumulate_row(&mut table[id], resolved, columns, row_idx);
        }
    });

    table
        .into_iter()
        .enumerate()
        .filter(|(_, accums)| accums.iter().any(|a| a.count > 0))
        .map(|(id, accums)| (IntGroupKey::SingleU32(id as u32), accums))
        .collect()
}

pub(super) fn aggregate_hash_u32(
    resolved: &ResolvedSchema,
    columns: &[Option<&ColumnData>],
    mask: &[u64],
    row_count: usize,
    num_aggs: usize,
    sym_ids: &[u32],
) -> Vec<(IntGroupKey, Vec<AggAccum>)> {
    let mut groups: FxHashMap<u32, Vec<AggAccum>> = FxHashMap::default();

    for_each_set_bit(mask, row_count, |row_idx| {
        let id = sym_ids[row_idx];
        let accums = groups
            .entry(id)
            .or_insert_with(|| (0..num_aggs).map(|_| AggAccum::default()).collect());
        accumulate_row(accums, resolved, columns, row_idx);
    });

    groups
        .into_iter()
        .map(|(id, accums)| (IntGroupKey::SingleU32(id), accums))
        .collect()
}

pub(super) fn aggregate_hash_generic(
    resolved: &ResolvedSchema,
    columns: &[Option<&ColumnData>],
    mask: &[u64],
    row_count: usize,
    num_aggs: usize,
) -> Vec<(IntGroupKey, Vec<AggAccum>)> {
    let mut groups: FxHashMap<Vec<u64>, Vec<AggAccum>> = FxHashMap::default();

    for_each_set_bit(mask, row_count, |row_idx| {
        let key = build_generic_key(resolved, columns, row_idx);
        let accums = groups
            .entry(key)
            .or_insert_with(|| (0..num_aggs).map(|_| AggAccum::default()).collect());
        accumulate_row(accums, resolved, columns, row_idx);
    });

    groups
        .into_iter()
        .map(|(key, accums)| (IntGroupKey::Multi(key), accums))
        .collect()
}

/// Two-level aggregation for high-cardinality GROUP BY (2M+ keys).
///
/// Phase 1: Partition rows into buckets by hash prefix (top 8 bits → 256 buckets).
/// Phase 2: Aggregate each bucket independently (small HashMap, cache-friendly).
/// Phase 3: Flatten all buckets into the final result.
pub(super) fn aggregate_two_level(
    resolved: &ResolvedSchema,
    columns: &[Option<&ColumnData>],
    mask: &[u64],
    row_count: usize,
    num_aggs: usize,
) -> Vec<(IntGroupKey, Vec<AggAccum>)> {
    const NUM_BUCKETS: usize = 256;

    // Phase 1: Build key for each row, hash it, and partition into buckets by top 8 bits.
    // The key is stored alongside the row index so Phase 2 does not recompute it.
    let mut buckets: Vec<Vec<(usize, Vec<u64>)>> = (0..NUM_BUCKETS).map(|_| Vec::new()).collect();
    for_each_set_bit(mask, row_count, |row_idx| {
        let key = build_generic_key(resolved, columns, row_idx);
        let hash = fx_hash_key(&key);
        let bucket = (hash >> 56) as usize;
        buckets[bucket].push((row_idx, key));
    });

    // Phase 2 + 3: Aggregate each bucket independently and flatten.
    let mut all_results: Vec<(IntGroupKey, Vec<AggAccum>)> = Vec::new();
    for bucket_rows in buckets {
        if bucket_rows.is_empty() {
            continue;
        }
        let mut groups: FxHashMap<Vec<u64>, Vec<AggAccum>> = FxHashMap::default();
        for (row_idx, key) in bucket_rows {
            let accums = groups
                .entry(key)
                .or_insert_with(|| (0..num_aggs).map(|_| AggAccum::default()).collect());
            accumulate_row(accums, resolved, columns, row_idx);
        }
        all_results.extend(groups.into_iter().map(|(k, a)| (IntGroupKey::Multi(k), a)));
    }

    all_results
}

#[cfg(test)]
mod tests {
    use super::super::super::super::columnar_memtable::ColumnType;
    use super::super::super::types::AggColInfo;
    use super::*;

    fn make_resolved_count(col_types: &[(usize, ColumnType)]) -> ResolvedSchema {
        ResolvedSchema {
            group_cols: col_types.to_vec(),
            agg_cols: vec![AggColInfo::CountStar],
            ts_idx: 0,
        }
    }

    /// Build a full-set mask for `row_count` rows.
    fn full_mask(row_count: usize) -> Vec<u64> {
        let words = row_count.div_ceil(64);
        let mut mask = vec![u64::MAX; words];
        let rem = row_count % 64;
        if rem != 0 {
            *mask.last_mut().unwrap() = (1u64 << rem) - 1;
        }
        mask
    }

    /// Collect aggregation results into a sorted Vec of (key, count) for comparison.
    fn collect_counts(results: Vec<(IntGroupKey, Vec<AggAccum>)>) -> Vec<(Vec<u64>, u64)> {
        let mut out: Vec<(Vec<u64>, u64)> = results
            .into_iter()
            .map(|(key, accums)| {
                let k = match key {
                    IntGroupKey::Multi(v) => v,
                    IntGroupKey::SingleU32(v) => vec![v as u64],
                    IntGroupKey::None => vec![],
                };
                let count = accums.first().map(|a| a.count).unwrap_or(0);
                (k, count)
            })
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    #[test]
    fn two_level_matches_generic_multi_column() {
        // Build two Int64 columns with 8 distinct combinations repeated many times.
        let n = 200_000usize;
        let col_a: Vec<i64> = (0..n).map(|i| (i % 4) as i64).collect();
        let col_b: Vec<i64> = (0..n).map(|i| (i % 2) as i64).collect();

        let data_a = ColumnData::Int64(col_a);
        let data_b = ColumnData::Int64(col_b);
        let columns: Vec<Option<&ColumnData>> = vec![Some(&data_a), Some(&data_b)];

        let resolved = make_resolved_count(&[(0, ColumnType::Int64), (1, ColumnType::Int64)]);
        let mask = full_mask(n);

        // num_aggs = 1 so AggAccum::count tracks row membership.
        let generic = aggregate_hash_generic(&resolved, &columns, &mask, n, 1);
        let two_level = aggregate_two_level(&resolved, &columns, &mask, n, 1);

        assert_eq!(
            collect_counts(generic),
            collect_counts(two_level),
            "two-level and generic must produce identical group counts"
        );
    }
}
