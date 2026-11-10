// SPDX-License-Identifier: BUSL-1.1

//! Column storage of the columnar timeseries memtable.

use super::types::{ColumnType, NULL_SYMBOL_ID};

/// A single column of data in the memtable.
#[derive(Debug)]
pub enum ColumnData {
    Timestamp(Vec<i64>),
    Float64(Vec<f64>),
    Int64(Vec<i64>),
    Symbol(Vec<u32>),
    /// Dictionary-encoded string column for low-cardinality tags.
    ///
    /// Predicates operate on compact integer IDs rather than string bytes —
    /// an `O(dict_size * string_len + N)` win over `O(N * string_len)`.
    DictEncoded {
        /// Row-level symbol IDs (index into `dictionary`).
        ids: Vec<u32>,
        /// ID → string value.
        dictionary: Vec<String>,
        /// Reverse lookup: string → ID.
        reverse: std::collections::HashMap<String, u32>,
        /// Validity bitmap: false = null row.
        valid: Vec<bool>,
    },
    /// Exact decimals. `None` is a NULL cell.
    Decimal(Vec<Option<rust_decimal::Decimal>>),
    /// UUIDs as their 16 bytes. `None` is a NULL cell.
    Uuid(Vec<Option<[u8; 16]>>),
    /// Booleans. `None` is a NULL cell.
    Bool(Vec<Option<bool>>),
}

impl ColumnData {
    pub(super) fn new(ty: ColumnType) -> Self {
        match ty {
            ColumnType::Timestamp(_) => Self::Timestamp(Vec::with_capacity(4096)),
            ColumnType::Float64 => Self::Float64(Vec::with_capacity(4096)),
            ColumnType::Int64 => Self::Int64(Vec::with_capacity(4096)),
            ColumnType::Symbol | ColumnType::Text => Self::Symbol(Vec::with_capacity(4096)),
            ColumnType::Decimal(_) => Self::Decimal(Vec::with_capacity(4096)),
            ColumnType::Uuid => Self::Uuid(Vec::with_capacity(4096)),
            ColumnType::Bool => Self::Bool(Vec::with_capacity(4096)),
        }
    }

    /// A column of `ty` holding `rows` NULL cells: NaN for a float, `0` for
    /// an integer or time column, which have no NULL, and the NULL symbol
    /// id for a dictionary column.
    pub fn nulls(ty: ColumnType, rows: usize) -> Self {
        match ty {
            ColumnType::Timestamp(_) => Self::Timestamp(vec![0; rows]),
            ColumnType::Float64 => Self::Float64(vec![f64::NAN; rows]),
            ColumnType::Int64 => Self::Int64(vec![0; rows]),
            ColumnType::Symbol | ColumnType::Text => Self::Symbol(vec![NULL_SYMBOL_ID; rows]),
            ColumnType::Decimal(_) => Self::Decimal(vec![None; rows]),
            ColumnType::Uuid => Self::Uuid(vec![None; rows]),
            ColumnType::Bool => Self::Bool(vec![None; rows]),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Timestamp(v) => v.len(),
            Self::Float64(v) => v.len(),
            Self::Int64(v) => v.len(),
            Self::Symbol(v) => v.len(),
            Self::DictEncoded { ids, .. } => ids.len(),
            Self::Decimal(v) => v.len(),
            Self::Uuid(v) => v.len(),
            Self::Bool(v) => v.len(),
        }
    }

    /// Remove the last row, the rollback of a partly ingested row.
    pub(super) fn pop_row(&mut self) {
        match self {
            Self::Timestamp(v) | Self::Int64(v) => {
                v.pop();
            }
            Self::Float64(v) => {
                v.pop();
            }
            Self::Symbol(v) => {
                v.pop();
            }
            Self::DictEncoded { ids, valid, .. } => {
                ids.pop();
                valid.pop();
            }
            Self::Decimal(v) => {
                v.pop();
            }
            Self::Uuid(v) => {
                v.pop();
            }
            Self::Bool(v) => {
                v.pop();
            }
        }
    }

    /// Keep only the first `n` rows.
    pub(super) fn truncate_rows(&mut self, n: usize) {
        match self {
            Self::Timestamp(v) | Self::Int64(v) => v.truncate(n),
            Self::Float64(v) => v.truncate(n),
            Self::Symbol(v) => v.truncate(n),
            Self::DictEncoded { ids, valid, .. } => {
                ids.truncate(n);
                valid.truncate(n);
            }
            Self::Decimal(v) => v.truncate(n),
            Self::Uuid(v) => v.truncate(n),
            Self::Bool(v) => v.truncate(n),
        }
    }

    /// Remove the first `n` rows and keep the rest.
    pub(super) fn drop_front_rows(&mut self, n: usize) {
        fn drop_front<T>(v: &mut Vec<T>, n: usize) {
            *v = v.split_off(n.min(v.len()));
        }
        match self {
            Self::Timestamp(v) | Self::Int64(v) => drop_front(v, n),
            Self::Float64(v) => drop_front(v, n),
            Self::Symbol(v) => drop_front(v, n),
            Self::DictEncoded { ids, valid, .. } => {
                drop_front(ids, n);
                drop_front(valid, n);
            }
            Self::Decimal(v) => drop_front(v, n),
            Self::Uuid(v) => drop_front(v, n),
            Self::Bool(v) => drop_front(v, n),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Approximate memory usage in bytes.
    pub(super) fn memory_bytes(&self) -> usize {
        match self {
            Self::Timestamp(v) => v.capacity() * 8,
            Self::Float64(v) => v.capacity() * 8,
            Self::Int64(v) => v.capacity() * 8,
            Self::Symbol(v) => v.capacity() * 4,
            Self::DictEncoded {
                ids,
                dictionary,
                reverse,
                valid,
            } => {
                ids.capacity() * 4
                    + dictionary.iter().map(|s| s.len() + 24).sum::<usize>()
                    + reverse.len() * 56 // rough HashMap entry cost
                    + valid.capacity()
            }
            Self::Decimal(v) => v.capacity() * std::mem::size_of::<Option<rust_decimal::Decimal>>(),
            Self::Uuid(v) => v.capacity() * std::mem::size_of::<Option<[u8; 16]>>(),
            Self::Bool(v) => v.capacity() * std::mem::size_of::<Option<bool>>(),
        }
    }

    /// Deep clone the column data.
    pub fn clone_data(&self) -> Self {
        match self {
            Self::Timestamp(v) => Self::Timestamp(v.clone()),
            Self::Float64(v) => Self::Float64(v.clone()),
            Self::Int64(v) => Self::Int64(v.clone()),
            Self::Symbol(v) => Self::Symbol(v.clone()),
            Self::DictEncoded {
                ids,
                dictionary,
                reverse,
                valid,
            } => Self::DictEncoded {
                ids: ids.clone(),
                dictionary: dictionary.clone(),
                reverse: reverse.clone(),
                valid: valid.clone(),
            },
            Self::Decimal(v) => Self::Decimal(v.clone()),
            Self::Uuid(v) => Self::Uuid(v.clone()),
            Self::Bool(v) => Self::Bool(v.clone()),
        }
    }

    /// Get timestamp column as slice. Type mismatch is unreachable in correct usage.
    pub(crate) fn as_timestamps(&self) -> &[i64] {
        match self {
            Self::Timestamp(v) => v,
            _ => unreachable!(
                "invariant: ColumnarMemtable columns are constructed by ColumnData::new(ty) keyed to schema; Timestamp type mismatch is impossible"
            ),
        }
    }

    /// Get f64 column as slice. Type mismatch is unreachable in correct usage.
    pub(crate) fn as_f64(&self) -> &[f64] {
        match self {
            Self::Float64(v) => v,
            _ => unreachable!(
                "invariant: ColumnarMemtable columns are constructed by ColumnData::new(ty) keyed to schema; Float64 type mismatch is impossible"
            ),
        }
    }

    /// Get i64 column as slice. Type mismatch is unreachable in correct usage.
    pub(crate) fn as_i64(&self) -> &[i64] {
        match self {
            Self::Int64(v) => v,
            _ => unreachable!(
                "invariant: ColumnarMemtable columns are constructed by ColumnData::new(ty) keyed to schema; Int64 type mismatch is impossible"
            ),
        }
    }

    /// Get decimal column as slice. Type mismatch is unreachable in correct usage.
    pub(crate) fn as_decimals(&self) -> &[Option<rust_decimal::Decimal>] {
        match self {
            Self::Decimal(v) => v,
            _ => unreachable!(
                "invariant: ColumnarMemtable columns are constructed by ColumnData::new(ty) keyed to schema; Decimal type mismatch is impossible"
            ),
        }
    }

    /// Get UUID column as slice. Type mismatch is unreachable in correct usage.
    pub(crate) fn as_uuids(&self) -> &[Option<[u8; 16]>] {
        match self {
            Self::Uuid(v) => v,
            _ => unreachable!(
                "invariant: ColumnarMemtable columns are constructed by ColumnData::new(ty) keyed to schema; Uuid type mismatch is impossible"
            ),
        }
    }

    /// Get boolean column as slice. Type mismatch is unreachable in correct usage.
    pub(crate) fn as_bools(&self) -> &[Option<bool>] {
        match self {
            Self::Bool(v) => v,
            _ => unreachable!(
                "invariant: ColumnarMemtable columns are constructed by ColumnData::new(ty) keyed to schema; Bool type mismatch is impossible"
            ),
        }
    }

    /// Get symbol column as slice. Type mismatch is unreachable in correct usage.
    pub(crate) fn as_symbols(&self) -> &[u32] {
        match self {
            Self::Symbol(v) => v,
            _ => unreachable!(
                "invariant: ColumnarMemtable columns are constructed by ColumnData::new(ty) keyed to schema; Symbol type mismatch is impossible"
            ),
        }
    }
}
