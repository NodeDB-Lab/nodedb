// SPDX-License-Identifier: BUSL-1.1

//! Integer group keys: packing a row's group columns into `u64` parts,
//! hashing them, and resolving them back to the string key emission reads.

use super::super::super::columnar_memtable::{ColumnData, ColumnType};
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
        ColumnType::Symbol => {
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

/// Append the string form of one packed key part for a column of type `ty`.
///
/// A symbol resolves through its dictionary and appends nothing when the id
/// is unknown. Integers and time columns render as the signed integer packed,
/// floats from their bit pattern.
pub(super) fn push_key_part<'a>(
    out: &mut String,
    col_idx: usize,
    ty: ColumnType,
    part: u64,
    sym_lookup: &dyn Fn(usize) -> Option<&'a nodedb_types::timeseries::SymbolDictionary>,
) {
    use std::fmt::Write;
    match ty {
        ColumnType::Symbol => {
            if let Some(dict) = sym_lookup(col_idx)
                && let Some(name) = dict.get(part as u32)
            {
                out.push_str(name);
            }
        }
        ColumnType::Int64 | ColumnType::Timestamp(_) => {
            let _ = write!(out, "{}", part as i64);
        }
        ColumnType::Float64 => {
            let _ = write!(out, "{}", f64::from_bits(part));
        }
    }
}

pub(super) fn resolve_group_key<'a>(
    key: &IntGroupKey,
    resolved: &ResolvedSchema,
    sym_lookup: &dyn Fn(usize) -> Option<&'a nodedb_types::timeseries::SymbolDictionary>,
) -> String {
    match key {
        IntGroupKey::None => String::new(),
        IntGroupKey::SingleU32(id) => {
            let col_idx = resolved.group_cols[0].0;
            if resolved.group_cols[0].1 == ColumnType::Symbol {
                sym_lookup(col_idx)
                    .and_then(|d: &nodedb_types::timeseries::SymbolDictionary| d.get(*id))
                    .unwrap_or("")
                    .to_string()
            } else {
                id.to_string()
            }
        }
        IntGroupKey::Multi(parts) => {
            let mut s = String::with_capacity(parts.len() * 16);
            for (i, &part) in parts.iter().enumerate() {
                if i > 0 {
                    s.push('\0');
                }
                let (col_idx, ty) = resolved.group_cols[i];
                push_key_part(&mut s, col_idx, ty, part, sym_lookup);
            }
            s
        }
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
