// SPDX-License-Identifier: BUSL-1.1

//! Integer group keys: packing a row's group columns into `u64` parts,
//! hashing them, and resolving them back to the typed key emission reads.

use super::super::super::columnar_memtable::{ColumnData, ColumnType};
use super::super::group_key::{GroupCell, GroupKey, key_cell};
use super::super::types::ResolvedSchema;

/// Integer group key for local (per-source) grouping.
#[derive(Clone, PartialEq, Eq, Hash)]
pub(super) enum IntGroupKey {
    None,
    SingleU32(u32),
    Multi(Vec<u64>),
}

/// FxHash-style multiplicative hash over a slice of u64 values.
///
/// Used by `aggregate_two_level` to assign rows to buckets in Phase 1.
#[inline]
pub(super) fn fx_hash_key(key: &[u64]) -> u64 {
    let mut hash: u64 = 0;
    for &v in key {
        hash = hash.wrapping_mul(0x517cc1b727220a95).wrapping_add(v);
    }
    hash
}

/// Pack one group column's cell at `row_idx` into a `u64` key part.
///
/// Symbols and integers pack as their value, floats as their bit pattern,
/// and a time column as its millisecond count. A column whose data is absent
/// or of the wrong shape packs as `u64::MAX`.
#[inline]
pub(super) fn key_part(ty: ColumnType, data: Option<&ColumnData>, row_idx: usize) -> u64 {
    data.map(|data| match ty {
        ColumnType::Symbol | ColumnType::Text => {
            if let ColumnData::Symbol(ids) = data {
                ids[row_idx] as u64
            } else {
                u64::MAX
            }
        }
        ColumnType::Int64 => {
            if let ColumnData::Int64(vals) = data {
                vals[row_idx] as u64
            } else {
                u64::MAX
            }
        }
        ColumnType::Float64 => {
            if let ColumnData::Float64(vals) = data {
                vals[row_idx].to_bits()
            } else {
                u64::MAX
            }
        }
        ColumnType::Timestamp(_) => {
            if let ColumnData::Timestamp(vals) = data {
                vals[row_idx] as u64
            } else {
                u64::MAX
            }
        }
    })
    .unwrap_or(u64::MAX)
}

pub(super) fn build_generic_key(
    resolved: &ResolvedSchema,
    columns: &[Option<&ColumnData>],
    row_idx: usize,
) -> Vec<u64> {
    resolved
        .group_cols
        .iter()
        .map(|&(idx, ty)| key_part(ty, columns[idx], row_idx))
        .collect()
}

/// The key cells of packed GROUP BY `parts`, one per GROUP BY column.
///
/// `columns` is the source's column data: a GROUP BY column the source holds
/// no data for is NULL in every row. See [`key_cell`] for each cell.
pub(super) fn key_cells<'a>(
    parts: &[u64],
    resolved: &ResolvedSchema,
    columns: &[Option<&ColumnData>],
    sym_lookup: &dyn Fn(usize) -> Option<&'a nodedb_types::timeseries::SymbolDictionary>,
) -> Vec<GroupCell> {
    resolved
        .group_cols
        .iter()
        .zip(parts)
        .map(|(&(col_idx, ty), &part)| {
            let present = columns.get(col_idx).is_some_and(Option::is_some);
            key_cell(ty, part, present, sym_lookup(col_idx))
        })
        .collect()
}

/// The typed group key of an integer key, with no time bucket.
pub(super) fn resolve_group_key<'a>(
    key: &IntGroupKey,
    resolved: &ResolvedSchema,
    columns: &[Option<&ColumnData>],
    sym_lookup: &dyn Fn(usize) -> Option<&'a nodedb_types::timeseries::SymbolDictionary>,
) -> GroupKey {
    let cells = match key {
        IntGroupKey::None => Vec::new(),
        IntGroupKey::SingleU32(id) => key_cells(&[u64::from(*id)], resolved, columns, sym_lookup),
        IntGroupKey::Multi(parts) => key_cells(parts, resolved, columns, sym_lookup),
    };
    GroupKey {
        bucket: None,
        cells,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fx_hash_key_deterministic() {
        let key = vec![1u64, 2, 3, 4];
        assert_eq!(fx_hash_key(&key), fx_hash_key(&key));
        assert_ne!(fx_hash_key(&[1, 2]), fx_hash_key(&[2, 1]));
    }
}
