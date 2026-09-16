// SPDX-License-Identifier: BUSL-1.1

//! Filter evaluation on columnar data vectors.
//!
//! Provides sparse, dense, and SIMD-bitmask filter evaluation paths.

use super::ColumnarSource;
use super::dict::{eval_dict_filter_bitmask, eval_dict_filter_sparse};
use crate::bridge::scan_filter::{FilterOp, ScanFilter};
use crate::engine::timeseries::columnar_memtable::{ColumnData, ColumnType};

/// Evaluate filters on a sparse set of row indices (e.g., after time-range pruning).
///
/// Returns a bitmask aligned with `indices` — `mask[i]` is true if
/// `indices[i]` passes all filters. Returns `None` if any filter can't
/// be evaluated on columnar data (caller should fall back to JSON-based
/// evaluation).
pub(crate) fn eval_filters_sparse(
    src: &dyn ColumnarSource,
    filters: &[ScanFilter],
    indices: &[u32],
) -> Option<Vec<bool>> {
    let mut mask = vec![true; indices.len()];

    for f in filters {
        if f.op == FilterOp::MatchAll {
            continue;
        }
        if !f.clauses.is_empty() {
            return None;
        }

        let (col_pos, col_type, col_data) = src.resolve_column(&f.field)?;

        // DictEncoded intercepts before ColumnType dispatch — the column type
        // may be Symbol but the storage is already dictionary-encoded.
        if let ColumnData::DictEncoded {
            ids,
            dictionary,
            reverse,
            valid,
        } = col_data
        {
            eval_dict_filter_sparse(ids, dictionary, reverse, valid, f, indices, &mut mask)?;
            continue;
        }

        match col_type {
            ColumnType::Float64 => {
                let fv = f.value.as_f64()?;
                if let ColumnData::Float64(vals) = col_data {
                    for (mi, &idx) in indices.iter().enumerate() {
                        if !mask[mi] {
                            continue;
                        }
                        mask[mi] = eval_numeric_f64(vals[idx as usize], f.op.as_str(), fv)?;
                    }
                }
            }
            ColumnType::Int64 => {
                let fv = f.value.as_i64()?;
                if let ColumnData::Int64(vals) = col_data {
                    for (mi, &idx) in indices.iter().enumerate() {
                        if !mask[mi] {
                            continue;
                        }
                        mask[mi] = eval_numeric_i64(vals[idx as usize], f.op.as_str(), fv)?;
                    }
                }
            }
            ColumnType::Timestamp(_) => {
                let fv = nodedb_query::scan_filter::value_as_timestamp_ms(&f.value)?;
                if let ColumnData::Timestamp(vals) = col_data {
                    for (mi, &idx) in indices.iter().enumerate() {
                        if !mask[mi] {
                            continue;
                        }
                        mask[mi] = eval_numeric_i64(vals[idx as usize], f.op.as_str(), fv)?;
                    }
                }
            }
            ColumnType::Symbol => {
                eval_symbol_filter(src, col_pos, col_data, f, indices, &mut mask)?;
            }
        }
    }

    Some(mask)
}

/// Evaluate filters on a contiguous row range `0..row_count`.
pub(crate) fn eval_filters_dense(
    src: &dyn ColumnarSource,
    filters: &[ScanFilter],
    row_count: usize,
) -> Option<Vec<bool>> {
    let mut mask = vec![true; row_count];

    for f in filters {
        if f.op == FilterOp::MatchAll {
            continue;
        }
        if !f.clauses.is_empty() {
            return None;
        }

        let (col_pos, col_type, col_data) = src.resolve_column(&f.field)?;

        // DictEncoded intercepts before ColumnType dispatch.
        if let ColumnData::DictEncoded {
            ids,
            dictionary,
            reverse,
            valid,
        } = col_data
        {
            let indices: Vec<u32> = (0..row_count as u32).collect();
            eval_dict_filter_sparse(ids, dictionary, reverse, valid, f, &indices, &mut mask)?;
            continue;
        }

        match col_type {
            ColumnType::Float64 => {
                let fv = f.value.as_f64()?;
                if let ColumnData::Float64(vals) = col_data {
                    for i in 0..row_count {
                        if !mask[i] {
                            continue;
                        }
                        mask[i] = eval_numeric_f64(vals[i], f.op.as_str(), fv)?;
                    }
                }
            }
            ColumnType::Int64 => {
                let fv = f.value.as_i64()?;
                if let ColumnData::Int64(vals) = col_data {
                    for i in 0..row_count {
                        if !mask[i] {
                            continue;
                        }
                        mask[i] = eval_numeric_i64(vals[i], f.op.as_str(), fv)?;
                    }
                }
            }
            ColumnType::Timestamp(_) => {
                let fv = nodedb_query::scan_filter::value_as_timestamp_ms(&f.value)?;
                if let ColumnData::Timestamp(vals) = col_data {
                    for i in 0..row_count {
                        if !mask[i] {
                            continue;
                        }
                        mask[i] = eval_numeric_i64(vals[i], f.op.as_str(), fv)?;
                    }
                }
            }
            ColumnType::Symbol => {
                let indices: Vec<u32> = (0..row_count as u32).collect();
                eval_symbol_filter(src, col_pos, col_data, f, &indices, &mut mask)?;
            }
        }
    }

    Some(mask)
}

/// Apply passing indices from a bitmask.
pub(crate) fn apply_mask(indices: &[u32], mask: &[bool]) -> Vec<u32> {
    indices
        .iter()
        .zip(mask.iter())
        .filter(|&(_, pass)| *pass)
        .map(|(idx, _)| *idx)
        .collect()
}

// ---------------------------------------------------------------------------
// SIMD bitmask filter evaluation (Vec<u64>, 1 bit per row)
// ---------------------------------------------------------------------------

/// Evaluate filters on a contiguous row range `0..row_count`, returning a
/// packed `Vec<u64>` bitmask (bit *i* set = row *i* passes all filters).
///
/// Uses SIMD kernels for numeric and symbol equality comparisons.
/// Returns `None` for filter patterns that can't be evaluated (OR clauses,
/// unsupported operators) — caller should fall back to `eval_filters_dense`.
pub(crate) fn eval_filters_bitmask(
    src: &dyn ColumnarSource,
    filters: &[ScanFilter],
    row_count: usize,
) -> Option<Vec<u64>> {
    use nodedb_query::simd_filter::{self, filter_runtime};

    let rt = filter_runtime();
    let mut mask = simd_filter::bitmask_all(row_count);

    for f in filters {
        if f.op == FilterOp::MatchAll {
            continue;
        }
        if !f.clauses.is_empty() {
            return None; // OR clauses not supported in bitmask path
        }

        let (col_pos, col_type, col_data) = src.resolve_column(&f.field)?;

        // DictEncoded: use SIMD eq_u32/ne_u32 on IDs, scalar scan for contains.
        if let ColumnData::DictEncoded {
            ids,
            dictionary,
            reverse,
            valid,
        } = col_data
        {
            let dict_mask =
                eval_dict_filter_bitmask(ids, dictionary, reverse, valid, f, row_count, rt)?;
            mask = simd_filter::bitmask_and(&mask, &dict_mask);
            continue;
        }

        let filter_mask = match col_type {
            ColumnType::Float64 => {
                let fv = f.value.as_f64()?;
                let ColumnData::Float64(vals) = col_data else {
                    return None;
                };
                let slice = &vals[..row_count.min(vals.len())];
                match f.op.as_str() {
                    "gt" => (rt.gt_f64)(slice, fv),
                    "gte" => (rt.gte_f64)(slice, fv),
                    "lt" => (rt.lt_f64)(slice, fv),
                    "lte" => (rt.lte_f64)(slice, fv),
                    "eq" => {
                        // f64 equality — use epsilon range.
                        let lo = fv - f64::EPSILON;
                        let hi = fv + f64::EPSILON;
                        let a = (rt.gte_f64)(slice, lo);
                        let b = (rt.lte_f64)(slice, hi);
                        simd_filter::bitmask_and(&a, &b)
                    }
                    "ne" => {
                        let lo = fv - f64::EPSILON;
                        let hi = fv + f64::EPSILON;
                        let a = (rt.gte_f64)(slice, lo);
                        let b = (rt.lte_f64)(slice, hi);
                        let eq = simd_filter::bitmask_and(&a, &b);
                        simd_filter::bitmask_not(&eq, row_count)
                    }
                    _ => return None,
                }
            }
            ColumnType::Int64 => {
                let fv = f.value.as_i64()?;
                let ColumnData::Int64(vals) = col_data else {
                    return None;
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
                        let eq = simd_filter::bitmask_and(&a, &b);
                        simd_filter::bitmask_not(&eq, row_count)
                    }
                    _ => return None,
                }
            }
            ColumnType::Timestamp(_) => {
                let fv = nodedb_query::scan_filter::value_as_timestamp_ms(&f.value)?;
                let ColumnData::Timestamp(vals) = col_data else {
                    return None;
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
                        let eq = simd_filter::bitmask_and(&a, &b);
                        simd_filter::bitmask_not(&eq, row_count)
                    }
                    _ => return None,
                }
            }
            ColumnType::Symbol => {
                let filter_str = f.value.as_str()?;
                let dict = src.symbol_dict(col_pos)?;
                let ColumnData::Symbol(ids) = col_data else {
                    return None;
                };
                let slice = &ids[..row_count.min(ids.len())];
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

#[inline]
fn eval_numeric_f64(v: f64, op: &str, fv: f64) -> Option<bool> {
    Some(match op {
        "gt" => v > fv,
        "gte" => v >= fv,
        "lt" => v < fv,
        "lte" => v <= fv,
        "eq" => (v - fv).abs() < f64::EPSILON,
        "ne" => (v - fv).abs() >= f64::EPSILON,
        _ => return None,
    })
}

#[inline]
fn eval_numeric_i64(v: i64, op: &str, fv: i64) -> Option<bool> {
    Some(match op {
        "gt" => v > fv,
        "gte" => v >= fv,
        "lt" => v < fv,
        "lte" => v <= fv,
        "eq" => v == fv,
        "ne" => v != fv,
        _ => return None,
    })
}

fn eval_symbol_filter(
    src: &dyn ColumnarSource,
    col_pos: usize,
    col_data: &ColumnData,
    f: &ScanFilter,
    indices: &[u32],
    mask: &mut [bool],
) -> Option<()> {
    let filter_str = f.value.as_str()?;
    let dict = src.symbol_dict(col_pos)?;
    let ColumnData::Symbol(ids) = col_data else {
        return None;
    };

    match f.op.as_str() {
        "eq" => {
            if let Some(target_id) = dict.get_id(filter_str) {
                for (mi, &idx) in indices.iter().enumerate() {
                    if mask[mi] {
                        mask[mi] = ids[idx as usize] == target_id;
                    }
                }
            } else {
                for m in mask.iter_mut() {
                    *m = false;
                }
            }
        }
        "ne" => {
            if let Some(target_id) = dict.get_id(filter_str) {
                for (mi, &idx) in indices.iter().enumerate() {
                    if mask[mi] {
                        mask[mi] = ids[idx as usize] != target_id;
                    }
                }
            }
        }
        "contains" => {
            let matching_ids: std::collections::HashSet<u32> = (0..dict.len() as u32)
                .filter(|&id| dict.get(id).is_some_and(|s| s.contains(filter_str)))
                .collect();
            for (mi, &idx) in indices.iter().enumerate() {
                if mask[mi] {
                    mask[mi] = matching_ids.contains(&ids[idx as usize]);
                }
            }
        }
        _ => return None,
    }

    Some(())
}
