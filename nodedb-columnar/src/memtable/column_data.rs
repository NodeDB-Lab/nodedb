//! `ColumnData` enum: typed per-column storage with optional validity bitmap.

use nodedb_types::columnar::ColumnType;
use nodedb_types::value::Value;

use crate::error::ColumnarError;

use super::IngestValue;

/// A single column's data in the memtable.
///
/// Each variant stores a contiguous Vec of the appropriate primitive type
/// plus an optional validity bitmap. When `valid` is `None`, all rows are
/// considered valid — this is the fast path for non-nullable columns,
/// eliminating one `Vec::push` per row and improving cache density.
///
/// When `valid` is `Some`, `true` = present, `false` = null.
#[derive(Debug, Clone)]
pub enum ColumnData {
    Int64 {
        values: Vec<i64>,
        valid: Option<Vec<bool>>,
    },
    Float64 {
        values: Vec<f64>,
        valid: Option<Vec<bool>>,
    },
    Bool {
        values: Vec<bool>,
        valid: Option<Vec<bool>>,
    },
    Timestamp {
        values: Vec<i64>,
        valid: Option<Vec<bool>>,
    },
    Decimal {
        /// Stored as 16-byte serialized representations.
        values: Vec<[u8; 16]>,
        valid: Option<Vec<bool>>,
    },
    Uuid {
        /// Stored as 16-byte binary representations.
        values: Vec<[u8; 16]>,
        valid: Option<Vec<bool>>,
    },
    String {
        /// Concatenated string bytes.
        data: Vec<u8>,
        /// Byte offsets: offset[i] is the start of string i, offset[len] is end sentinel.
        offsets: Vec<u32>,
        valid: Option<Vec<bool>>,
    },
    Bytes {
        data: Vec<u8>,
        offsets: Vec<u32>,
        valid: Option<Vec<bool>>,
    },
    Geometry {
        /// Stored as JSON-serialized geometry bytes.
        data: Vec<u8>,
        offsets: Vec<u32>,
        valid: Option<Vec<bool>>,
    },
    Vector {
        /// Packed f32 values: dim floats per row.
        data: Vec<f32>,
        dim: u32,
        valid: Option<Vec<bool>>,
    },
    /// Dictionary-encoded string column: stores u32 symbol IDs + dictionary.
    ///
    /// Low-cardinality string columns (e.g. `qtype`, `rcode`) are converted to
    /// this representation before segment flush. The IDs are delta-encoded as
    /// i64 for compact storage; the dictionary is stored in `ColumnMeta`.
    DictEncoded {
        /// Symbol IDs per row (index into dictionary).
        ids: Vec<u32>,
        /// Dictionary: ID → string value.
        dictionary: Vec<String>,
        /// Reverse lookup: string → ID.
        reverse: std::collections::HashMap<String, u32>,
        valid: Option<Vec<bool>>,
    },
}

impl ColumnData {
    /// Create an empty column for the given type.
    ///
    /// When `nullable` is false, validity bitmap is omitted (`None`) — the fast
    /// path for non-nullable columns that saves one `Vec::push` per row.
    pub(crate) fn new(col_type: &ColumnType, nullable: bool) -> Self {
        let valid = if nullable { Some(Vec::new()) } else { None };
        match col_type {
            ColumnType::Int64 => Self::Int64 {
                values: Vec::new(),
                valid,
            },
            ColumnType::Float64 => Self::Float64 {
                values: Vec::new(),
                valid,
            },
            ColumnType::Bool => Self::Bool {
                values: Vec::new(),
                valid,
            },
            ColumnType::Timestamp => Self::Timestamp {
                values: Vec::new(),
                valid,
            },
            ColumnType::Decimal => Self::Decimal {
                values: Vec::new(),
                valid,
            },
            ColumnType::Uuid => Self::Uuid {
                values: Vec::new(),
                valid,
            },
            ColumnType::String => Self::String {
                data: Vec::new(),
                offsets: vec![0],
                valid,
            },
            ColumnType::Bytes => Self::Bytes {
                data: Vec::new(),
                offsets: vec![0],
                valid,
            },
            ColumnType::Geometry => Self::Geometry {
                data: Vec::new(),
                offsets: vec![0],
                valid,
            },
            ColumnType::Vector(dim) => Self::Vector {
                data: Vec::new(),
                dim: *dim,
                valid,
            },
        }
    }

    /// Number of rows in this column.
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Int64 { values, .. } => values.len(),
            Self::Float64 { values, .. } => values.len(),
            Self::Bool { values, .. } => values.len(),
            Self::Timestamp { values, .. } => values.len(),
            Self::Decimal { values, .. } => values.len(),
            Self::Uuid { values, .. } => values.len(),
            Self::String { offsets, .. } => offsets.len().saturating_sub(1),
            Self::Bytes { offsets, .. } => offsets.len().saturating_sub(1),
            Self::Geometry { offsets, .. } => offsets.len().saturating_sub(1),
            Self::Vector { data, dim, .. } => {
                if *dim == 0 {
                    0
                } else {
                    data.len() / *dim as usize
                }
            }
            Self::DictEncoded { ids, .. } => ids.len(),
        }
    }

    /// Get the validity bitmap, or generate an all-true one for non-nullable columns.
    ///
    /// For segment writing — we always need a validity slice for the block encoder.
    /// Non-nullable columns return a freshly generated all-true vec (cheap, happens
    /// once per flush, not per row).
    pub(crate) fn validity_or_all_true(&self) -> std::borrow::Cow<'_, [bool]> {
        let valid_opt = match self {
            Self::Int64 { valid, .. }
            | Self::Float64 { valid, .. }
            | Self::Bool { valid, .. }
            | Self::Timestamp { valid, .. }
            | Self::Decimal { valid, .. }
            | Self::Uuid { valid, .. }
            | Self::String { valid, .. }
            | Self::Bytes { valid, .. }
            | Self::Geometry { valid, .. }
            | Self::Vector { valid, .. }
            | Self::DictEncoded { valid, .. } => valid,
        };
        match valid_opt {
            Some(v) => std::borrow::Cow::Borrowed(v.as_slice()),
            None => std::borrow::Cow::Owned(vec![true; self.len()]),
        }
    }

    /// Check if a row is null (valid bitmap says false).
    ///
    /// Returns false (not null) for non-nullable columns (no bitmap).
    #[inline]
    fn is_null(&self, row: usize) -> bool {
        let valid_opt = match self {
            Self::Int64 { valid, .. }
            | Self::Float64 { valid, .. }
            | Self::Bool { valid, .. }
            | Self::Timestamp { valid, .. }
            | Self::Decimal { valid, .. }
            | Self::Uuid { valid, .. }
            | Self::String { valid, .. }
            | Self::Bytes { valid, .. }
            | Self::Geometry { valid, .. }
            | Self::Vector { valid, .. }
            | Self::DictEncoded { valid, .. } => valid,
        };
        valid_opt.as_ref().is_some_and(|v| !v[row])
    }

    /// Extract a single row's value as `nodedb_types::Value`.
    pub(crate) fn get_value(&self, row: usize) -> Value {
        if self.is_null(row) {
            return Value::Null;
        }
        match self {
            Self::Int64 { values, .. } => Value::Integer(values[row]),
            Self::Float64 { values, .. } => Value::Float(values[row]),
            Self::Bool { values, .. } => Value::Bool(values[row]),
            Self::Timestamp { values, .. } => Value::DateTime(
                nodedb_types::datetime::NdbDateTime::from_micros(values[row]),
            ),
            Self::Decimal { values, .. } => {
                Value::Decimal(rust_decimal::Decimal::deserialize(values[row]))
            }
            Self::Uuid { values, .. } => {
                Value::Uuid(uuid::Uuid::from_bytes(values[row]).to_string())
            }
            Self::String { data, offsets, .. } => {
                let start = offsets[row] as usize;
                let end = offsets[row + 1] as usize;
                let s = std::str::from_utf8(&data[start..end])
                    .unwrap_or("")
                    .to_string();
                Value::String(s)
            }
            Self::Bytes { data, offsets, .. } => {
                let start = offsets[row] as usize;
                let end = offsets[row + 1] as usize;
                Value::Bytes(data[start..end].to_vec())
            }
            Self::Geometry { data, offsets, .. } => {
                let start = offsets[row] as usize;
                let end = offsets[row + 1] as usize;
                let s = std::str::from_utf8(&data[start..end])
                    .unwrap_or("")
                    .to_string();
                Value::String(s)
            }
            Self::Vector { data, dim, .. } => {
                let d = *dim as usize;
                let start = row * d;
                let floats: Vec<Value> = data[start..start + d]
                    .iter()
                    .map(|&f| Value::Float(f as f64))
                    .collect();
                Value::Array(floats)
            }
            Self::DictEncoded {
                ids, dictionary, ..
            } => {
                let id = ids[row] as usize;
                if id < dictionary.len() {
                    Value::String(dictionary[id].clone())
                } else {
                    Value::Null
                }
            }
        }
    }

    /// Push a validity bit (if the column is nullable).
    #[inline(always)]
    pub(crate) fn push_valid(valid: &mut Option<Vec<bool>>, is_valid: bool) {
        if let Some(v) = valid {
            v.push(is_valid);
        }
    }

    /// Append a value. Returns error if type doesn't match.
    pub(crate) fn push(&mut self, value: &Value, col_name: &str) -> Result<(), ColumnarError> {
        match (self, value) {
            (Self::Int64 { values, valid }, Value::Null) => {
                values.push(0);
                Self::push_valid(valid, false);
            }
            (Self::Float64 { values, valid }, Value::Null) => {
                values.push(0.0);
                Self::push_valid(valid, false);
            }
            (Self::Bool { values, valid }, Value::Null) => {
                values.push(false);
                Self::push_valid(valid, false);
            }
            (Self::Timestamp { values, valid }, Value::Null) => {
                values.push(0);
                Self::push_valid(valid, false);
            }
            (Self::Decimal { values, valid }, Value::Null) => {
                values.push([0u8; 16]);
                Self::push_valid(valid, false);
            }
            (Self::Uuid { values, valid }, Value::Null) => {
                values.push([0u8; 16]);
                Self::push_valid(valid, false);
            }
            (Self::String { offsets, valid, .. }, Value::Null) => {
                offsets.push(*offsets.last().unwrap_or(&0));
                Self::push_valid(valid, false);
            }
            (Self::Bytes { offsets, valid, .. }, Value::Null) => {
                offsets.push(*offsets.last().unwrap_or(&0));
                Self::push_valid(valid, false);
            }
            (Self::Geometry { offsets, valid, .. }, Value::Null) => {
                offsets.push(*offsets.last().unwrap_or(&0));
                Self::push_valid(valid, false);
            }
            (Self::Vector { data, dim, valid }, Value::Null) => {
                data.extend(std::iter::repeat_n(0.0f32, *dim as usize));
                Self::push_valid(valid, false);
            }
            (Self::Int64 { values, valid }, Value::Integer(v)) => {
                values.push(*v);
                Self::push_valid(valid, true);
            }
            (Self::Float64 { values, valid }, Value::Float(v)) => {
                values.push(*v);
                Self::push_valid(valid, true);
            }
            (Self::Float64 { values, valid }, Value::Integer(v)) => {
                values.push(*v as f64);
                Self::push_valid(valid, true);
            }
            (Self::Bool { values, valid }, Value::Bool(v)) => {
                values.push(*v);
                Self::push_valid(valid, true);
            }
            (Self::Timestamp { values, valid }, Value::DateTime(dt)) => {
                values.push(dt.micros);
                Self::push_valid(valid, true);
            }
            (Self::Timestamp { values, valid }, Value::Integer(micros)) => {
                values.push(*micros);
                Self::push_valid(valid, true);
            }
            (Self::Decimal { values, valid }, Value::Decimal(d)) => {
                values.push(d.serialize());
                Self::push_valid(valid, true);
            }
            (Self::Uuid { values, valid }, Value::Uuid(s)) => {
                let bytes = uuid::Uuid::parse_str(s)
                    .map(|u| *u.as_bytes())
                    .unwrap_or([0u8; 16]);
                values.push(bytes);
                Self::push_valid(valid, true);
            }
            (
                Self::String {
                    data,
                    offsets,
                    valid,
                },
                Value::String(s),
            ) => {
                data.extend_from_slice(s.as_bytes());
                offsets.push(data.len() as u32);
                Self::push_valid(valid, true);
            }
            (
                Self::Bytes {
                    data,
                    offsets,
                    valid,
                },
                Value::Bytes(b),
            ) => {
                data.extend_from_slice(b);
                offsets.push(data.len() as u32);
                Self::push_valid(valid, true);
            }
            (
                Self::Geometry {
                    data,
                    offsets,
                    valid,
                },
                Value::Geometry(g),
            ) => {
                if let Ok(json) = sonic_rs::to_vec(g) {
                    data.extend_from_slice(&json);
                }
                offsets.push(data.len() as u32);
                Self::push_valid(valid, true);
            }
            (
                Self::Geometry {
                    data,
                    offsets,
                    valid,
                },
                Value::String(s),
            ) => {
                data.extend_from_slice(s.as_bytes());
                offsets.push(data.len() as u32);
                Self::push_valid(valid, true);
            }
            (Self::Vector { data, dim, valid }, Value::Array(arr)) => {
                let d = *dim as usize;
                for (i, v) in arr.iter().take(d).enumerate() {
                    let f = match v {
                        Value::Float(f) => *f as f32,
                        Value::Integer(n) => *n as f32,
                        _ => 0.0,
                    };
                    if i < d {
                        data.push(f);
                    }
                }
                for _ in arr.len()..d {
                    data.push(0.0);
                }
                Self::push_valid(valid, true);
            }
            (Self::DictEncoded { ids, valid, .. }, Value::Null) => {
                ids.push(0);
                Self::push_valid(valid, false);
            }
            (
                Self::DictEncoded {
                    ids,
                    dictionary,
                    reverse,
                    valid,
                },
                Value::String(s),
            ) => {
                let id = if let Some(&existing) = reverse.get(s.as_str()) {
                    existing
                } else {
                    let new_id = dictionary.len() as u32;
                    dictionary.push(s.clone());
                    reverse.insert(s.clone(), new_id);
                    new_id
                };
                ids.push(id);
                Self::push_valid(valid, true);
            }
            (other, val) => {
                let type_name = match other {
                    Self::Int64 { .. } => "Int64",
                    Self::Float64 { .. } => "Float64",
                    Self::Bool { .. } => "Bool",
                    Self::Timestamp { .. } => "Timestamp",
                    Self::Decimal { .. } => "Decimal",
                    Self::Uuid { .. } => "Uuid",
                    Self::String { .. } => "String",
                    Self::Bytes { .. } => "Bytes",
                    Self::Geometry { .. } => "Geometry",
                    Self::Vector { .. } => "Vector",
                    Self::DictEncoded { .. } => "DictEncoded",
                };
                let _ = val;
                return Err(ColumnarError::TypeMismatch {
                    column: col_name.to_string(),
                    expected: type_name.to_string(),
                });
            }
        }
        Ok(())
    }

    /// Append a borrowed value (zero-copy for strings). Used by `ingest_row_refs`.
    pub(crate) fn push_ref(
        &mut self,
        value: &IngestValue<'_>,
        col_name: &str,
    ) -> Result<(), ColumnarError> {
        match (self, value) {
            (Self::Int64 { values, valid }, IngestValue::Null) => {
                values.push(0);
                Self::push_valid(valid, false);
            }
            (Self::Float64 { values, valid }, IngestValue::Null) => {
                values.push(0.0);
                Self::push_valid(valid, false);
            }
            (Self::Bool { values, valid }, IngestValue::Null) => {
                values.push(false);
                Self::push_valid(valid, false);
            }
            (Self::Timestamp { values, valid }, IngestValue::Null) => {
                values.push(0);
                Self::push_valid(valid, false);
            }
            (Self::String { offsets, valid, .. }, IngestValue::Null) => {
                offsets.push(*offsets.last().unwrap_or(&0));
                Self::push_valid(valid, false);
            }
            (Self::DictEncoded { ids, valid, .. }, IngestValue::Null) => {
                ids.push(0);
                Self::push_valid(valid, false);
            }
            (Self::Int64 { values, valid }, IngestValue::Int64(v)) => {
                values.push(*v);
                Self::push_valid(valid, true);
            }
            (Self::Float64 { values, valid }, IngestValue::Float64(v)) => {
                values.push(*v);
                Self::push_valid(valid, true);
            }
            (Self::Float64 { values, valid }, IngestValue::Int64(v)) => {
                values.push(*v as f64);
                Self::push_valid(valid, true);
            }
            (Self::Bool { values, valid }, IngestValue::Bool(v)) => {
                values.push(*v);
                Self::push_valid(valid, true);
            }
            (Self::Timestamp { values, valid }, IngestValue::Timestamp(v)) => {
                values.push(*v);
                Self::push_valid(valid, true);
            }
            (Self::Timestamp { values, valid }, IngestValue::Int64(v)) => {
                values.push(*v);
                Self::push_valid(valid, true);
            }
            (
                Self::String {
                    data,
                    offsets,
                    valid,
                },
                IngestValue::Str(s),
            ) => {
                data.extend_from_slice(s.as_bytes());
                offsets.push(data.len() as u32);
                Self::push_valid(valid, true);
            }
            (
                Self::DictEncoded {
                    ids,
                    dictionary,
                    reverse,
                    valid,
                },
                IngestValue::Str(s),
            ) => {
                let id = if let Some(&existing) = reverse.get(*s) {
                    existing
                } else {
                    let new_id = dictionary.len() as u32;
                    dictionary.push((*s).to_string());
                    reverse.insert((*s).to_string(), new_id);
                    new_id
                };
                ids.push(id);
                Self::push_valid(valid, true);
            }
            (other, _) => {
                let type_name = match other {
                    Self::Int64 { .. } => "Int64",
                    Self::Float64 { .. } => "Float64",
                    Self::Bool { .. } => "Bool",
                    Self::Timestamp { .. } => "Timestamp",
                    Self::Decimal { .. } => "Decimal",
                    Self::Uuid { .. } => "Uuid",
                    Self::String { .. } => "String",
                    Self::Bytes { .. } => "Bytes",
                    Self::Geometry { .. } => "Geometry",
                    Self::Vector { .. } => "Vector",
                    Self::DictEncoded { .. } => "DictEncoded",
                };
                return Err(ColumnarError::TypeMismatch {
                    column: col_name.to_string(),
                    expected: type_name.to_string(),
                });
            }
        }
        Ok(())
    }

    /// Backfill a column with null/default values for existing rows.
    pub(crate) fn backfill_nulls(&mut self, count: usize) {
        match self {
            Self::Int64 { values, valid } => {
                values.extend(std::iter::repeat_n(0i64, count));
                if let Some(v) = valid {
                    v.extend(std::iter::repeat_n(false, count));
                }
            }
            Self::Float64 { values, valid } => {
                values.extend(std::iter::repeat_n(f64::NAN, count));
                if let Some(v) = valid {
                    v.extend(std::iter::repeat_n(false, count));
                }
            }
            Self::Bool { values, valid } => {
                values.extend(std::iter::repeat_n(false, count));
                if let Some(v) = valid {
                    v.extend(std::iter::repeat_n(false, count));
                }
            }
            Self::Timestamp { values, valid } => {
                values.extend(std::iter::repeat_n(0i64, count));
                if let Some(v) = valid {
                    v.extend(std::iter::repeat_n(false, count));
                }
            }
            Self::Decimal { values, valid } => {
                values.extend(std::iter::repeat_n([0u8; 16], count));
                if let Some(v) = valid {
                    v.extend(std::iter::repeat_n(false, count));
                }
            }
            Self::Uuid { values, valid } => {
                values.extend(std::iter::repeat_n([0u8; 16], count));
                if let Some(v) = valid {
                    v.extend(std::iter::repeat_n(false, count));
                }
            }
            Self::String { offsets, valid, .. } => {
                let last = *offsets.last().unwrap_or(&0);
                offsets.extend(std::iter::repeat_n(last, count));
                if let Some(v) = valid {
                    v.extend(std::iter::repeat_n(false, count));
                }
            }
            Self::Bytes { offsets, valid, .. } => {
                let last = *offsets.last().unwrap_or(&0);
                offsets.extend(std::iter::repeat_n(last, count));
                if let Some(v) = valid {
                    v.extend(std::iter::repeat_n(false, count));
                }
            }
            Self::Geometry { offsets, valid, .. } => {
                let last = *offsets.last().unwrap_or(&0);
                offsets.extend(std::iter::repeat_n(last, count));
                if let Some(v) = valid {
                    v.extend(std::iter::repeat_n(false, count));
                }
            }
            Self::Vector { data, dim, valid } => {
                data.extend(std::iter::repeat_n(0.0f32, *dim as usize * count));
                if let Some(v) = valid {
                    v.extend(std::iter::repeat_n(false, count));
                }
            }
            Self::DictEncoded { ids, valid, .. } => {
                ids.extend(std::iter::repeat_n(0u32, count));
                if let Some(v) = valid {
                    v.extend(std::iter::repeat_n(false, count));
                }
            }
        }
    }
}

/// Maximum cardinality for automatic dictionary encoding.
pub const DICT_ENCODE_MAX_CARDINALITY: u32 = 1024;

impl ColumnData {
    /// Attempt to convert a `String` column to `DictEncoded`.
    pub fn try_dict_encode(col: &ColumnData, max_cardinality: u32) -> Option<ColumnData> {
        let (data, offsets, valid) = match col {
            ColumnData::String {
                data,
                offsets,
                valid,
            } => (data, offsets, valid),
            _ => return None,
        };

        let row_count = col.len();
        let mut dictionary: Vec<String> = Vec::new();
        let mut reverse: std::collections::HashMap<String, u32> = std::collections::HashMap::new();
        let mut ids: Vec<u32> = Vec::with_capacity(row_count);

        for i in 0..row_count {
            if valid.as_ref().is_some_and(|v| !v[i]) {
                ids.push(0);
                continue;
            }
            let start = offsets[i] as usize;
            let end = offsets[i + 1] as usize;
            let s = match std::str::from_utf8(&data[start..end]) {
                Ok(s) => s,
                Err(_) => return None,
            };
            let id = if let Some(&existing) = reverse.get(s) {
                existing
            } else {
                if dictionary.len() as u32 >= max_cardinality {
                    return None;
                }
                let new_id = dictionary.len() as u32;
                dictionary.push(s.to_string());
                reverse.insert(s.to_string(), new_id);
                new_id
            };
            ids.push(id);
        }

        Some(ColumnData::DictEncoded {
            ids,
            dictionary,
            reverse,
            valid: valid.clone(),
        })
    }
}
