// SPDX-License-Identifier: BUSL-1.1

//! GROUP BY key cell of one aggregate bucket.
//!
//! A key holds the cell value, never a symbol ID. A memtable resets its
//! symbol dictionaries at every flush, and each partition carries its own,
//! so one ID names different tags in two drains.

use nodedb_types::Value;
use nodedb_types::timeseries::SymbolDictionary;

use crate::engine::timeseries::columnar_memtable::ColumnData;

/// One GROUP BY cell of a bucket key.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum GroupCell {
    /// No value: an absent column, a null symbol, or a null dictionary cell.
    Null,
    Int(i64),
    /// `f64::to_bits` of the cell. `-0.0` and `0.0` group apart.
    Float(u64),
    Text(String),
    /// A decimal. `1.5` and `1.50` are one key, shown as the first seen.
    Decimal(DecimalCell),
    /// A UUID's 16 bytes.
    Uuid([u8; 16]),
    Bool(bool),
}

impl GroupCell {
    /// Key cell of `row` in `column`. `dict` is the column's symbol
    /// dictionary in the same drain or partition.
    pub fn of(column: Option<&ColumnData>, dict: Option<&SymbolDictionary>, row: usize) -> Self {
        match column {
            Some(ColumnData::Symbol(ids)) => ids
                .get(row)
                .and_then(|id| dict.and_then(|d| d.get(*id)))
                .map_or(Self::Null, |s| Self::Text(s.to_owned())),
            Some(ColumnData::DictEncoded {
                ids,
                dictionary,
                valid,
                ..
            }) => {
                if valid.get(row).copied().unwrap_or(false) {
                    ids.get(row)
                        .and_then(|id| dictionary.get(*id as usize))
                        .map_or(Self::Null, |s| Self::Text(s.clone()))
                } else {
                    Self::Null
                }
            }
            Some(ColumnData::Int64(v)) | Some(ColumnData::Timestamp(v)) => {
                v.get(row).map_or(Self::Null, |x| Self::Int(*x))
            }
            Some(ColumnData::Float64(v)) => {
                v.get(row).map_or(Self::Null, |x| Self::Float(x.to_bits()))
            }
            Some(ColumnData::Decimal(v)) => v
                .get(row)
                .copied()
                .flatten()
                .map_or(Self::Null, Self::decimal),
            Some(ColumnData::Uuid(v)) => {
                v.get(row).copied().flatten().map_or(Self::Null, Self::Uuid)
            }
            Some(ColumnData::Bool(v)) => {
                v.get(row).copied().flatten().map_or(Self::Null, Self::Bool)
            }
            None => Self::Null,
        }
    }

    /// The key cell of a decimal. Equal decimals of different scales share
    /// one key, and the cell shows `d` with its own scale.
    pub fn decimal(d: rust_decimal::Decimal) -> Self {
        Self::Decimal(DecimalCell::new(d))
    }

    /// The cell as a value.
    pub fn to_value(&self) -> Value {
        match self {
            Self::Null => Value::Null,
            Self::Int(i) => Value::Integer(*i),
            Self::Float(bits) => Value::Float(f64::from_bits(*bits)),
            Self::Text(s) => Value::String(s.clone()),
            Self::Decimal(cell) => Value::Decimal(cell.shown()),
            Self::Uuid(bytes) => crate::engine::timeseries::row_cells::uuid_value(Some(*bytes)),
            Self::Bool(b) => Value::Bool(*b),
        }
    }
}

/// A decimal GROUP BY cell. Its identity, equality, hash and order, is the
/// normalized value, so `1.5` and `1.50` are one key. It shows the value it
/// was made from with that value's scale: a group keeps the scale of its
/// first-seen key.
#[derive(Debug, Clone, Copy, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub struct DecimalCell {
    /// `rust_decimal::Decimal::serialize` of the normalized value.
    normalized: [u8; 16],
    /// `rust_decimal::Decimal::serialize` of the value as made.
    shown: [u8; 16],
}

impl DecimalCell {
    pub fn new(d: rust_decimal::Decimal) -> Self {
        Self {
            normalized: d.normalize().serialize(),
            shown: d.serialize(),
        }
    }

    /// The value the cell shows, with the scale it was made with.
    pub fn shown(&self) -> rust_decimal::Decimal {
        rust_decimal::Decimal::deserialize(self.shown)
    }
}

impl PartialEq for DecimalCell {
    fn eq(&self, other: &Self) -> bool {
        self.normalized == other.normalized
    }
}

impl Eq for DecimalCell {}

impl std::hash::Hash for DecimalCell {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.normalized.hash(state);
    }
}

impl PartialOrd for DecimalCell {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DecimalCell {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.normalized.cmp(&other.normalized)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn symbol_cells_resolve_through_their_own_dictionary() {
        let mut first = SymbolDictionary::new();
        first.resolve("a", 10);
        first.resolve("b", 10);
        let mut second = SymbolDictionary::new();
        second.resolve("b", 10);
        second.resolve("a", 10);
        let ids = ColumnData::Symbol(vec![0, 1]);
        assert_eq!(
            GroupCell::of(Some(&ids), Some(&first), 0),
            GroupCell::Text("a".into())
        );
        assert_eq!(
            GroupCell::of(Some(&ids), Some(&second), 0),
            GroupCell::Text("b".into())
        );
    }

    #[test]
    fn null_symbol_and_absent_column_are_null() {
        let dict = SymbolDictionary::new();
        let ids = ColumnData::Symbol(vec![u32::MAX]);
        assert_eq!(GroupCell::of(Some(&ids), Some(&dict), 0), GroupCell::Null);
        assert_eq!(GroupCell::of(None, None, 0), GroupCell::Null);
    }

    #[test]
    fn integer_and_float_cells_key_by_value() {
        let ints = ColumnData::Int64(vec![7, 9]);
        assert_eq!(GroupCell::of(Some(&ints), None, 1), GroupCell::Int(9));
        let floats = ColumnData::Float64(vec![1.5]);
        assert_eq!(
            GroupCell::of(Some(&floats), None, 0).to_value(),
            Value::Float(1.5)
        );
    }

    /// A decimal key is one group across scales and shows the scale it was
    /// first seen with.
    #[test]
    fn decimal_key_keeps_its_first_seen_scale() {
        let dec = |t: &str| rust_decimal::Decimal::from_str_exact(t).expect("decimal");
        let first = GroupCell::decimal(dec("1.50"));
        let later = GroupCell::decimal(dec("1.5"));
        assert_eq!(first, later);
        let hash = |cell: &GroupCell| {
            use std::hash::{Hash, Hasher};
            let mut h = std::collections::hash_map::DefaultHasher::new();
            cell.hash(&mut h);
            h.finish()
        };
        assert_eq!(hash(&first), hash(&later));
        assert_eq!(first.cmp(&later), std::cmp::Ordering::Equal);
        let mut groups = std::collections::HashMap::new();
        groups.entry(vec![first.clone()]).or_insert(0u32);
        *groups.entry(vec![later]).or_insert(0) += 1;
        assert_eq!(groups.len(), 1);
        let (key, _) = groups.iter().next().expect("one group");
        let Value::Decimal(shown) = key[0].to_value() else {
            panic!("a decimal key shows a Decimal");
        };
        assert_eq!(shown.to_string(), "1.50");
        let column = ColumnData::Decimal(vec![Some(dec("2.500"))]);
        let Value::Decimal(shown) = GroupCell::of(Some(&column), None, 0).to_value() else {
            panic!("a decimal key shows a Decimal");
        };
        assert_eq!(shown.to_string(), "2.500");
        let bytes = zerompk::to_msgpack_vec(&first).expect("encode");
        let back: GroupCell = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(back, first);
        assert_eq!(back.to_value(), first.to_value());
    }
}
