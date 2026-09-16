// SPDX-License-Identifier: BUSL-1.1

//! SIMD bitmask filter evaluation for columnar data.
//!
//! Evaluates `ScanFilter` predicates directly on typed column vectors,
//! returning packed `Vec<u64>` bitmasks. Uses SIMD kernels from
//! `nodedb_query::simd_filter` for numeric and symbol comparisons.

use nodedb_query::simd_filter;

use super::columnar_memtable::{ColumnData, ColumnType};
use crate::bridge::scan_filter::ScanFilter;

/// Evaluate ScanFilter predicates on columnar data, returning a bitmask.
///
/// Bit *i* is set iff row *i* passes ALL filters.
/// Returns `None` for unsupported patterns (OR clauses, contains, etc.).
pub fn eval_filters_to_bitmask<'a>(
    filters: &[ScanFilter],
    schema: &[(String, ColumnType)],
    columns: &[Option<&'a ColumnData>],
    sym_lookup: &dyn Fn(usize) -> Option<&'a nodedb_types::timeseries::SymbolDictionary>,
    row_count: usize,
) -> Option<Vec<u64>> {
    let rt = simd_filter::filter_runtime();
    let mut mask = simd_filter::bitmask_all(row_count);

    for f in filters {
        if f.op == nodedb_query::scan_filter::FilterOp::MatchAll {
            continue;
        }
        if !f.clauses.is_empty() {
            return None;
        }

        let col_pos = schema.iter().position(|(n, _)| n == &f.field)?;
        let (_, col_type) = &schema[col_pos];
        let col_data = columns[col_pos]?;

        let filter_mask = match col_type {
            ColumnType::Float64 => {
                let fv = f.value.as_f64()?;
                let vals = col_data.as_f64();
                let slice = &vals[..row_count.min(vals.len())];
                match f.op.as_str() {
                    "gt" => (rt.gt_f64)(slice, fv),
                    "gte" => (rt.gte_f64)(slice, fv),
                    "lt" => (rt.lt_f64)(slice, fv),
                    "lte" => (rt.lte_f64)(slice, fv),
                    "eq" => {
                        let a = (rt.gte_f64)(slice, fv - f64::EPSILON);
                        let b = (rt.lte_f64)(slice, fv + f64::EPSILON);
                        simd_filter::bitmask_and(&a, &b)
                    }
                    "ne" => {
                        let a = (rt.gte_f64)(slice, fv - f64::EPSILON);
                        let b = (rt.lte_f64)(slice, fv + f64::EPSILON);
                        simd_filter::bitmask_not(&simd_filter::bitmask_and(&a, &b), row_count)
                    }
                    _ => return None,
                }
            }
            ColumnType::Int64 | ColumnType::Timestamp(_) => {
                let fv = f.value.as_i64()?;
                let vals = if *col_type == ColumnType::Int64 {
                    col_data.as_i64()
                } else {
                    col_data.as_timestamps()
                };
                let slice = &vals[..row_count.min(vals.len())];
                match f.op.as_str() {
                    "gt" => (rt.gt_i64)(slice, fv),
                    "gte" => (rt.gte_i64)(slice, fv),
                    "lt" => (rt.lt_i64)(slice, fv),
                    "lte" => (rt.lte_i64)(slice, fv),
                    "eq" => {
                        let a = (rt.gte_i64)(slice, fv);
                        let b = (rt.lte_i64)(slice, fv);
                        simd_filter::bitmask_and(&a, &b)
                    }
                    "ne" => {
                        let a = (rt.gte_i64)(slice, fv);
                        let b = (rt.lte_i64)(slice, fv);
                        simd_filter::bitmask_not(&simd_filter::bitmask_and(&a, &b), row_count)
                    }
                    _ => return None,
                }
            }
            ColumnType::Symbol => {
                let filter_str = f.value.as_str()?;
                let dict = sym_lookup(col_pos)?;
                let sym_ids = col_data.as_symbols();
                let slice = &sym_ids[..row_count.min(sym_ids.len())];
                match f.op.as_str() {
                    "eq" => {
                        if let Some(target_id) = dict.get_id(filter_str) {
                            (rt.eq_u32)(slice, target_id)
                        } else {
                            vec![0u64; simd_filter::words_for(row_count)]
                        }
                    }
                    "ne" => {
                        if let Some(target_id) = dict.get_id(filter_str) {
                            (rt.ne_u32)(slice, target_id)
                        } else {
                            simd_filter::bitmask_all(row_count)
                        }
                    }
                    _ => return None,
                }
            }
        };

        mask = simd_filter::bitmask_and(&mask, &filter_mask);
    }

    Some(mask)
}

/// Apply sparse index block-level skip to a bitmask.
///
/// Clears bits for rows in blocks that don't survive the sparse index
/// filter (time range + predicate pushdown). This is cheaper than
/// not loading those blocks' data in the first place, but avoids
/// processing rows that can be skipped by metadata alone.
pub fn apply_sparse_skip(
    mask: &mut [u64],
    sparse_idx: &super::sparse_index::SparseIndex,
    time_range: (i64, i64),
    row_count: usize,
) {
    let surviving = sparse_idx.filter_blocks(time_range.0, time_range.1, &[]);

    // Build a set of surviving block indices for O(1) lookup.
    let total_blocks = sparse_idx.block_count();
    let mut block_alive = vec![false; total_blocks];
    for &bi in &surviving {
        if bi < total_blocks {
            block_alive[bi] = true;
        }
    }

    // Clear bits for non-surviving blocks.
    for (bi, &alive) in block_alive.iter().enumerate() {
        if alive {
            continue;
        }
        let (start, end) = sparse_idx.block_row_range(bi);
        let end = end.min(row_count);
        for row in start..end {
            let word_idx = row / 64;
            let bit_idx = row % 64;
            if word_idx < mask.len() {
                mask[word_idx] &= !(1u64 << bit_idx);
            }
        }
    }
}
