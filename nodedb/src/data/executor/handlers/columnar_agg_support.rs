// SPDX-License-Identifier: BUSL-1.1

//! Supporting types and low-level routines for columnar aggregation.

use crate::engine::timeseries::columnar_memtable::{ColumnData, ColumnType, ColumnarMemtable};

/// Iterate over every set bit in a packed `u64` bitmask, calling `f(row_idx)`.
///
/// Skips all-zero words and uses hardware `TZCNT` to locate set bits, which is
/// faster than scanning a `Vec<bool>` when the filter selectivity is low.
#[inline]
pub(in crate::data::executor::handlers) fn for_each_set_bit(
    mask: &[u64],
    row_count: usize,
    mut f: impl FnMut(usize),
) {
    for (word_idx, &word) in mask.iter().enumerate() {
        if word == 0 {
            continue;
        }
        let base = word_idx * 64;
        let mut bits = word;
        while bits != 0 {
            let bit_pos = bits.trailing_zeros() as usize;
            let row_idx = base + bit_pos;
            if row_idx < row_count {
                f(row_idx);
            }
            bits &= bits - 1; // clear lowest set bit
        }
    }
}

/// Accumulator for running aggregate computation per group.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(in crate::data::executor::handlers) struct AggAccum {
    pub count: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
}

impl AggAccum {
    pub(in crate::data::executor::handlers) fn new() -> Self {
        Self {
            count: 0,
            sum: 0.0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
        }
    }

    pub(in crate::data::executor::handlers) fn feed(&mut self, val: f64) {
        self.count += 1;
        self.sum += val;
        if val < self.min {
            self.min = val;
        }
        if val > self.max {
            self.max = val;
        }
    }

    pub(in crate::data::executor::handlers) fn feed_count_only(&mut self) {
        self.count += 1;
    }
}

/// A group key composed of symbol IDs (for Symbol columns) or raw i64/f64
/// values (for numeric group-by columns). Avoids string allocation entirely.
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub(in crate::data::executor::handlers) enum GroupKeyPart {
    SymbolId(u32),
    Int64(i64),
    /// f64 stored as bits for Eq/Hash (NaN-safe: all NaNs compare equal).
    Float64Bits(u64),
    Null,
}

/// Packed group key for multi-column GROUP BY.
pub(in crate::data::executor::handlers) type GroupKey = Vec<GroupKeyPart>;

/// Extract a group key part from a column at a given row index.
pub(in crate::data::executor::handlers) fn extract_group_key_part(
    col_type: &ColumnType,
    col_data: &ColumnData,
    row_idx: usize,
) -> GroupKeyPart {
    match col_type {
        ColumnType::Symbol => {
            if let ColumnData::Symbol(ids) = col_data {
                GroupKeyPart::SymbolId(ids[row_idx])
            } else {
                GroupKeyPart::Null
            }
        }
        ColumnType::Int64 => {
            if let ColumnData::Int64(vals) = col_data {
                GroupKeyPart::Int64(vals[row_idx])
            } else {
                GroupKeyPart::Null
            }
        }
        ColumnType::Float64 => {
            if let ColumnData::Float64(vals) = col_data {
                GroupKeyPart::Float64Bits(vals[row_idx].to_bits())
            } else {
                GroupKeyPart::Null
            }
        }
        ColumnType::Timestamp(_) => {
            if let ColumnData::Timestamp(vals) = col_data {
                GroupKeyPart::Int64(vals[row_idx])
            } else {
                GroupKeyPart::Null
            }
        }
    }
}

/// Resolve a group key part to a serde_json::Value for output.
pub(in crate::data::executor::handlers) fn resolve_key_part(
    mt: &ColumnarMemtable,
    col_idx: usize,
    part: &GroupKeyPart,
) -> serde_json::Value {
    match part {
        GroupKeyPart::SymbolId(id) => mt
            .symbol_dict(col_idx)
            .and_then(|dict| dict.get(*id))
            .map(|s| serde_json::Value::String(s.to_string()))
            .unwrap_or(serde_json::Value::Null),
        GroupKeyPart::Int64(v) => serde_json::Value::Number(serde_json::Number::from(*v)),
        GroupKeyPart::Float64Bits(bits) => {
            let v = f64::from_bits(*bits);
            serde_json::Number::from_f64(v)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null)
        }
        GroupKeyPart::Null => serde_json::Value::Null,
    }
}

/// Parameters for dense-symbol GROUP BY aggregation.
pub(in crate::data::executor::handlers) struct DenseSymbolParams<'a> {
    pub(in crate::data::executor::handlers) mt: &'a ColumnarMemtable,
    pub(in crate::data::executor::handlers) group_col_idx: usize,
    pub(in crate::data::executor::handlers) agg_col_data: &'a [Option<(usize, &'a ColumnData)>],
    pub(in crate::data::executor::handlers) aggregates: &'a [(String, String)],
    pub(in crate::data::executor::handlers) bitmask: Option<&'a [u64]>,
    pub(in crate::data::executor::handlers) bool_mask: Option<&'a [bool]>,
    pub(in crate::data::executor::handlers) row_count: usize,
    pub(in crate::data::executor::handlers) cardinality: usize,
}

/// Dense-array GROUP BY for a single Symbol column with cardinality ≤ 65536.
///
/// Indexes accumulators directly by symbol ID, avoiding HashMap entirely.
/// Returns `(sym_id, accumulators)` for every non-empty group.
pub(in crate::data::executor::handlers) fn aggregate_dense_symbol(
    p: &DenseSymbolParams<'_>,
) -> Vec<(u32, Vec<AggAccum>)> {
    let num_aggs = p.aggregates.len();
    let ids = match p.mt.column(p.group_col_idx) {
        ColumnData::Symbol(v) => v,
        _ => return Vec::new(),
    };

    // Allocate one accumulator vector per possible symbol ID.
    let mut table: Vec<Vec<AggAccum>> = (0..p.cardinality)
        .map(|_| (0..num_aggs).map(|_| AggAccum::new()).collect())
        .collect();

    let accumulate = |row_idx: usize, table: &mut Vec<Vec<AggAccum>>| {
        let sym_id = ids[row_idx] as usize;
        if sym_id >= p.cardinality {
            return;
        }
        let accums = &mut table[sym_id];
        for (agg_idx, (op, _)) in p.aggregates.iter().enumerate() {
            match &p.agg_col_data[agg_idx] {
                None => accums[agg_idx].feed_count_only(),
                Some((_, col_data)) => {
                    let val = match col_data {
                        ColumnData::Float64(vals) => vals[row_idx],
                        ColumnData::Int64(vals) => vals[row_idx] as f64,
                        ColumnData::Timestamp(vals) => vals[row_idx] as f64,
                        _ => return,
                    };
                    if op == "count" {
                        accums[agg_idx].feed_count_only();
                    } else {
                        accums[agg_idx].feed(val);
                    }
                }
            }
        }
    };

    if let Some(bm) = p.bitmask {
        for_each_set_bit(bm, p.row_count, |row_idx| accumulate(row_idx, &mut table));
    } else if let Some(mask) = p.bool_mask {
        for (row_idx, &passes) in mask.iter().enumerate().take(p.row_count) {
            if passes {
                accumulate(row_idx, &mut table);
            }
        }
    } else {
        for row_idx in 0..p.row_count {
            accumulate(row_idx, &mut table);
        }
    }

    // Collect only non-empty groups.
    table
        .into_iter()
        .enumerate()
        .filter(|(_, accums)| accums.iter().any(|a| a.count > 0))
        .map(|(id, accums)| (id as u32, accums))
        .collect()
}
