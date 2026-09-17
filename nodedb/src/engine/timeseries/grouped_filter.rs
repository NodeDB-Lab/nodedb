// SPDX-License-Identifier: BUSL-1.1

//! SIMD bitmask filter evaluation for columnar data.
//!
//! Evaluates `ScanFilter` predicates directly on typed column vectors,
//! returning packed `Vec<u64>` bitmasks. Uses SIMD kernels from
//! `nodedb_query::simd_filter` for numeric and symbol comparisons.

use nodedb_query::scan_filter::FilterOp;
use nodedb_query::simd_filter;

use super::columnar_memtable::{ColumnData, ColumnType};
use crate::bridge::scan_filter::ScanFilter;

/// A predicate the grouped scan cannot lower onto typed column vectors.
///
/// The grouped scan evaluates predicates only as SIMD bitmasks, so a shape
/// it cannot lower fails the aggregate instead of aggregating rows the
/// predicate never excluded. The message names the predicate and why.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("predicate `{field} {op}` cannot be evaluated by the timeseries grouped scan: {reason}")]
pub struct UnsupportedPredicate {
    pub field: String,
    pub op: &'static str,
    pub reason: &'static str,
}

impl UnsupportedPredicate {
    fn new(f: &ScanFilter, reason: &'static str) -> Self {
        Self {
            field: f.field.clone(),
            op: f.op.as_str(),
            reason,
        }
    }
}

/// Evaluate ScanFilter predicates on columnar data, returning a bitmask.
///
/// Bit *i* is set iff row *i* passes ALL filters. Supported shapes: a
/// single `(column, op, literal)` comparison with `eq`/`ne` on symbol
/// columns and `eq`/`ne`/`gt`/`gte`/`lt`/`lte` on numeric and time
/// columns. Every other shape (OR clauses, expressions, `in`, `like`, an
/// unknown column, a literal of the wrong type) is an
/// [`UnsupportedPredicate`] error.
pub fn eval_filters_to_bitmask<'a>(
    filters: &[ScanFilter],
    schema: &[(String, ColumnType)],
    columns: &[Option<&'a ColumnData>],
    sym_lookup: &dyn Fn(usize) -> Option<&'a nodedb_types::timeseries::SymbolDictionary>,
    row_count: usize,
) -> Result<Vec<u64>, UnsupportedPredicate> {
    let rt = simd_filter::filter_runtime();
    let mut mask = simd_filter::bitmask_all(row_count);

    for f in filters {
        if f.op == FilterOp::MatchAll {
            continue;
        }
        if !f.clauses.is_empty() {
            return Err(UnsupportedPredicate::new(f, "OR clauses"));
        }
        if f.expr.is_some() {
            return Err(UnsupportedPredicate::new(f, "expression predicate"));
        }

        let col_pos = schema
            .iter()
            .position(|(n, _)| n == &f.field)
            .ok_or(UnsupportedPredicate::new(f, "column not in the schema"))?;
        let (_, col_type) = &schema[col_pos];
        let col_data =
            columns[col_pos].ok_or(UnsupportedPredicate::new(f, "column data not loaded"))?;

        let filter_mask = match col_type {
            ColumnType::Float64 => {
                let fv = f
                    .value
                    .as_f64()
                    .ok_or(UnsupportedPredicate::new(f, "literal is not a float"))?;
                let vals = col_data.as_f64();
                let slice = &vals[..row_count.min(vals.len())];
                match f.op {
                    FilterOp::Gt => (rt.gt_f64)(slice, fv),
                    FilterOp::Gte => (rt.gte_f64)(slice, fv),
                    FilterOp::Lt => (rt.lt_f64)(slice, fv),
                    FilterOp::Lte => (rt.lte_f64)(slice, fv),
                    FilterOp::Eq => {
                        let a = (rt.gte_f64)(slice, fv - f64::EPSILON);
                        let b = (rt.lte_f64)(slice, fv + f64::EPSILON);
                        simd_filter::bitmask_and(&a, &b)
                    }
                    FilterOp::Ne => {
                        let a = (rt.gte_f64)(slice, fv - f64::EPSILON);
                        let b = (rt.lte_f64)(slice, fv + f64::EPSILON);
                        simd_filter::bitmask_not(&simd_filter::bitmask_and(&a, &b), row_count)
                    }
                    _ => {
                        return Err(UnsupportedPredicate::new(f, "operator on a float column"));
                    }
                }
            }
            ColumnType::Int64 => {
                let fv = f
                    .value
                    .as_i64()
                    .ok_or(UnsupportedPredicate::new(f, "literal is not an integer"))?;
                i64_column_mask(rt, f, col_data.as_i64(), fv, row_count)?
            }
            ColumnType::Timestamp(kind) => {
                // A time column stores epoch milliseconds; its literal lowers
                // to that unit by the column's kind (an instant for a declared
                // TIMESTAMP, an integer for a millisecond column).
                let fv = kind
                    .literal_ms(&f.value)
                    .ok_or(UnsupportedPredicate::new(f, "literal is not an instant"))?;
                i64_column_mask(rt, f, col_data.as_timestamps(), fv, row_count)?
            }
            ColumnType::Symbol => {
                let filter_str = f
                    .value
                    .as_str()
                    .ok_or(UnsupportedPredicate::new(f, "literal is not a string"))?;
                let dict = sym_lookup(col_pos)
                    .ok_or(UnsupportedPredicate::new(f, "symbol dictionary not loaded"))?;
                let sym_ids = col_data.as_symbols();
                let slice = &sym_ids[..row_count.min(sym_ids.len())];
                match f.op {
                    FilterOp::Eq => {
                        if let Some(target_id) = dict.get_id(filter_str) {
                            (rt.eq_u32)(slice, target_id)
                        } else {
                            vec![0u64; simd_filter::words_for(row_count)]
                        }
                    }
                    FilterOp::Ne => {
                        if let Some(target_id) = dict.get_id(filter_str) {
                            (rt.ne_u32)(slice, target_id)
                        } else {
                            simd_filter::bitmask_all(row_count)
                        }
                    }
                    _ => {
                        return Err(UnsupportedPredicate::new(f, "operator on a symbol column"));
                    }
                }
            }
        };

        mask = simd_filter::bitmask_and(&mask, &filter_mask);
    }

    Ok(mask)
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

/// The bitmask of rows in an `i64` column (integer or time) that satisfy
/// `f.op` against the lowered literal `fv`.
fn i64_column_mask(
    rt: &simd_filter::FilterSimdRuntime,
    f: &ScanFilter,
    vals: &[i64],
    fv: i64,
    row_count: usize,
) -> Result<Vec<u64>, UnsupportedPredicate> {
    let slice = &vals[..row_count.min(vals.len())];
    Ok(match f.op {
        FilterOp::Gt => (rt.gt_i64)(slice, fv),
        FilterOp::Gte => (rt.gte_i64)(slice, fv),
        FilterOp::Lt => (rt.lt_i64)(slice, fv),
        FilterOp::Lte => (rt.lte_i64)(slice, fv),
        FilterOp::Eq => {
            let a = (rt.gte_i64)(slice, fv);
            let b = (rt.lte_i64)(slice, fv);
            simd_filter::bitmask_and(&a, &b)
        }
        FilterOp::Ne => {
            let a = (rt.gte_i64)(slice, fv);
            let b = (rt.lte_i64)(slice, fv);
            simd_filter::bitmask_not(&simd_filter::bitmask_and(&a, &b), row_count)
        }
        _ => {
            return Err(UnsupportedPredicate::new(
                f,
                "operator on an integer or time column",
            ));
        }
    })
}
