// SPDX-License-Identifier: BUSL-1.1

//! The typed key of one group in a grouped timeseries aggregate.
//!
//! A key holds cell values, never symbol IDs: each memtable drain and each
//! partition carries its own symbol dictionary, so one ID names different
//! tags in two sources. A NULL cell is [`GroupCell::Null`], distinct from
//! every value, the empty string included.

use nodedb_types::timeseries::SymbolDictionary;

pub use crate::engine::timeseries::continuous_agg::GroupCell;

use super::super::columnar_memtable::ColumnType;

/// The key of one group: its time bucket when the query buckets, then one
/// cell per GROUP BY column in GROUP BY order.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct GroupKey {
    /// The bucket start in milliseconds. `None` when the query has no time
    /// bucket.
    pub bucket: Option<i64>,
    pub cells: Vec<GroupCell>,
}

/// The key cell of one packed GROUP BY part.
///
/// `present` is false when the source holds no data for the column: every
/// row of that source is NULL in it. A symbol the dictionary does not hold,
/// the NULL symbol ID among them, is NULL. A NaN float is NULL.
pub(super) fn key_cell(
    ty: ColumnType,
    part: u64,
    present: bool,
    dict: Option<&SymbolDictionary>,
) -> GroupCell {
    if !present {
        return GroupCell::Null;
    }
    match ty {
        ColumnType::Symbol | ColumnType::Text => u32::try_from(part)
            .ok()
            .and_then(|id| dict.and_then(|d| d.get(id)))
            .map_or(GroupCell::Null, |text| GroupCell::Text(text.to_owned())),
        // The packed part is the cell's two's-complement bits.
        ColumnType::Int64 | ColumnType::Timestamp(_) => GroupCell::Int(part as i64),
        ColumnType::Float64 => {
            if f64::from_bits(part).is_nan() {
                GroupCell::Null
            } else {
                GroupCell::Float(part)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_and_empty_symbols_differ() {
        let mut dict = SymbolDictionary::new();
        let empty = dict.resolve("", 10).expect("intern");
        assert_eq!(
            key_cell(ColumnType::Symbol, u64::from(empty), true, Some(&dict)),
            GroupCell::Text(String::new())
        );
        assert_eq!(
            key_cell(ColumnType::Symbol, u64::from(u32::MAX), true, Some(&dict)),
            GroupCell::Null
        );
    }

    #[test]
    fn an_absent_column_and_a_nan_are_null() {
        assert_eq!(key_cell(ColumnType::Int64, 0, false, None), GroupCell::Null);
        assert_eq!(
            key_cell(ColumnType::Float64, f64::NAN.to_bits(), true, None),
            GroupCell::Null
        );
        assert_eq!(
            key_cell(ColumnType::Int64, (-1i64) as u64, true, None),
            GroupCell::Int(-1)
        );
    }
}
