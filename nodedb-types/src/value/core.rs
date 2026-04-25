//! Dynamic value type for document fields and SQL parameters.
//!
//! Covers the value types needed for AI agent workloads: strings, numbers,
//! booleans, binary blobs (embeddings), arrays, and nested objects.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::array_cell::ArrayCell;
use crate::datetime::{NdbDateTime, NdbDuration};
use crate::geometry::Geometry;

/// A dynamic value that can represent any field type in a document
/// or any parameter in a SQL query.
///
/// Serialized with `#[serde(untagged)]` so that JSON output uses plain
/// JSON types (`"string"`, `1`, `true`, `null`, `[…]`, `{…}`) rather than
/// the externally-tagged form (`{"String":"…"}`, `{"Integer":1}`, etc.).
/// MessagePack (de)serialization is handled by custom `ToMessagePack` /
/// `FromMessagePack` impls and is unaffected by this attribute.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(untagged)]
pub enum Value {
    #[default]
    /// SQL NULL / missing value.
    Null,
    /// Boolean.
    Bool(bool),
    /// Signed 64-bit integer.
    Integer(i64),
    /// 64-bit floating point.
    Float(f64),
    /// UTF-8 string.
    String(String),
    /// Raw bytes (embeddings, serialized blobs).
    Bytes(Vec<u8>),
    /// Ordered array of values.
    Array(Vec<Value>),
    /// Nested key-value object.
    Object(HashMap<String, Value>),
    /// UUID (any version, stored as 36-char hyphenated string).
    Uuid(String),
    /// ULID (26-char Crockford Base32).
    Ulid(String),
    /// UTC timestamp with microsecond precision.
    DateTime(NdbDateTime),
    /// Duration with microsecond precision (signed).
    Duration(NdbDuration),
    /// Arbitrary-precision decimal (financial calculations, exact arithmetic).
    Decimal(rust_decimal::Decimal),
    /// GeoJSON-compatible geometry (Point, LineString, Polygon, etc.).
    Geometry(Geometry),
    /// Ordered set of unique values (auto-deduplicated, maintains insertion order).
    Set(Vec<Value>),
    /// Compiled regex pattern (stored as pattern string).
    Regex(String),
    /// A range of values with optional bounds.
    Range {
        /// Start bound (None = unbounded).
        start: Option<Box<Value>>,
        /// End bound (None = unbounded).
        end: Option<Box<Value>>,
        /// Whether the end bound is inclusive (`..=` vs `..`).
        inclusive: bool,
    },
    /// A typed reference to another record: `table:id`.
    Record {
        /// The table/collection name.
        table: String,
        /// The record's document ID.
        id: String,
    },
    /// One N-dimensional array cell (coords + attrs). Used by the array
    /// engine to carry a single cell across the SQL / wire boundary.
    NdArrayCell(ArrayCell),
}

impl Value {
    /// Returns true if this value is `Null`.
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Try to extract as a string reference.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s),
            _ => None,
        }
    }

    /// Try to extract as i64.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Integer(i) => Some(*i),
            _ => None,
        }
    }

    /// Try to extract as f64.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Float(f) => Some(*f),
            Value::Integer(i) => Some(*i as f64),
            _ => None,
        }
    }

    /// Try to extract as bool.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// Try to extract as byte slice (for embeddings).
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Value::Bytes(b) => Some(b),
            _ => None,
        }
    }

    /// Try to extract as UUID string.
    pub fn as_uuid(&self) -> Option<&str> {
        match self {
            Value::Uuid(s) => Some(s),
            _ => None,
        }
    }

    /// Try to extract as ULID string.
    pub fn as_ulid(&self) -> Option<&str> {
        match self {
            Value::Ulid(s) => Some(s),
            _ => None,
        }
    }

    /// Try to extract as DateTime.
    pub fn as_datetime(&self) -> Option<&NdbDateTime> {
        match self {
            Value::DateTime(dt) => Some(dt),
            _ => None,
        }
    }

    /// Try to extract as Duration.
    pub fn as_duration(&self) -> Option<&NdbDuration> {
        match self {
            Value::Duration(d) => Some(d),
            _ => None,
        }
    }

    /// Try to extract as Decimal.
    pub fn as_decimal(&self) -> Option<&rust_decimal::Decimal> {
        match self {
            Value::Decimal(d) => Some(d),
            _ => None,
        }
    }

    /// Try to extract as Geometry.
    pub fn as_geometry(&self) -> Option<&Geometry> {
        match self {
            Value::Geometry(g) => Some(g),
            _ => None,
        }
    }

    /// Try to extract as a set (deduplicated array).
    pub fn as_set(&self) -> Option<&[Value]> {
        match self {
            Value::Set(s) => Some(s),
            _ => None,
        }
    }

    /// Try to extract as regex pattern string.
    pub fn as_regex(&self) -> Option<&str> {
        match self {
            Value::Regex(r) => Some(r),
            _ => None,
        }
    }

    /// Try to extract as a record reference (table, id).
    pub fn as_record(&self) -> Option<(&str, &str)> {
        match self {
            Value::Record { table, id } => Some((table, id)),
            _ => None,
        }
    }

    /// Return the type name of this value as a string.
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Null => "null",
            Value::Bool(_) => "bool",
            Value::Integer(_) => "int",
            Value::Float(_) => "float",
            Value::String(_) => "string",
            Value::Bytes(_) => "bytes",
            Value::Array(_) => "array",
            Value::Object(_) => "object",
            Value::Uuid(_) => "uuid",
            Value::Ulid(_) => "ulid",
            Value::DateTime(_) => "datetime",
            Value::Duration(_) => "duration",
            Value::Decimal(_) => "decimal",
            Value::Geometry(_) => "geometry",
            Value::Set(_) => "set",
            Value::Regex(_) => "regex",
            Value::Range { .. } => "range",
            Value::Record { .. } => "record",
            Value::NdArrayCell(_) => "ndarray_cell",
        }
    }

    /// Look up a field by name. Returns `None` for non-Object variants.
    pub fn get(&self, field: &str) -> Option<&Value> {
        match self {
            Value::Object(map) => map.get(field),
            _ => None,
        }
    }

    /// Mutable field lookup. Returns `None` for non-Object variants.
    pub fn get_mut(&mut self, field: &str) -> Option<&mut Value> {
        match self {
            Value::Object(map) => map.get_mut(field),
            _ => None,
        }
    }

    /// Try to extract as an object (HashMap reference).
    pub fn as_object(&self) -> Option<&HashMap<String, Value>> {
        match self {
            Value::Object(map) => Some(map),
            _ => None,
        }
    }

    /// Try to extract as a mutable object.
    pub fn as_object_mut(&mut self) -> Option<&mut HashMap<String, Value>> {
        match self {
            Value::Object(map) => Some(map),
            _ => None,
        }
    }

    /// Try to extract as an array slice.
    pub fn as_array(&self) -> Option<&[Value]> {
        match self {
            Value::Array(arr) => Some(arr),
            Value::Set(arr) => Some(arr),
            _ => None,
        }
    }

    /// Get array elements (for IN/array operations).
    pub fn as_array_iter(&self) -> Option<impl Iterator<Item = &Value>> {
        match self {
            Value::Array(arr) | Value::Set(arr) => Some(arr.iter()),
            _ => None,
        }
    }
}

// ─── Convenience conversions ───────────────────────────────────────────

impl From<&str> for Value {
    fn from(s: &str) -> Self {
        Value::String(s.to_owned())
    }
}

impl From<String> for Value {
    fn from(s: String) -> Self {
        Value::String(s)
    }
}

impl From<i64> for Value {
    fn from(i: i64) -> Self {
        Value::Integer(i)
    }
}

impl From<f64> for Value {
    fn from(f: f64) -> Self {
        Value::Float(f)
    }
}

impl From<bool> for Value {
    fn from(b: bool) -> Self {
        Value::Bool(b)
    }
}

impl From<Vec<u8>> for Value {
    fn from(b: Vec<u8>) -> Self {
        Value::Bytes(b)
    }
}

impl From<NdbDateTime> for Value {
    fn from(dt: NdbDateTime) -> Self {
        Value::DateTime(dt)
    }
}

impl From<NdbDuration> for Value {
    fn from(d: NdbDuration) -> Self {
        Value::Duration(d)
    }
}

impl From<rust_decimal::Decimal> for Value {
    fn from(d: rust_decimal::Decimal) -> Self {
        Value::Decimal(d)
    }
}

impl From<Geometry> for Value {
    fn from(g: Geometry) -> Self {
        Value::Geometry(g)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn value_type_checks() {
        assert!(Value::Null.is_null());
        assert!(!Value::Bool(true).is_null());

        assert_eq!(Value::String("hi".into()).as_str(), Some("hi"));
        assert_eq!(Value::Integer(42).as_i64(), Some(42));
        assert_eq!(Value::Float(2.78).as_f64(), Some(2.78));
        assert_eq!(Value::Integer(10).as_f64(), Some(10.0));
        assert_eq!(Value::Bool(true).as_bool(), Some(true));
        assert_eq!(Value::Bytes(vec![1, 2]).as_bytes(), Some(&[1, 2][..]));
    }

    #[test]
    fn from_conversions() {
        let s: Value = "hello".into();
        assert_eq!(s.as_str(), Some("hello"));

        let i: Value = 42i64.into();
        assert_eq!(i.as_i64(), Some(42));

        let f: Value = 2.78f64.into();
        assert_eq!(f.as_f64(), Some(2.78));
    }

    #[test]
    fn nested_value() {
        let nested = Value::Object({
            let mut m = HashMap::new();
            m.insert(
                "inner".into(),
                Value::Array(vec![Value::Integer(1), Value::Integer(2)]),
            );
            m
        });
        assert!(!nested.is_null());
    }
}
