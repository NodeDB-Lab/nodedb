// SPDX-License-Identifier: Apache-2.0

//! Dynamic value type for document fields and SQL parameters.
//!
//! Covers the value types needed for AI agent workloads: strings, numbers,
//! booleans, binary blobs (embeddings), arrays, and nested objects.

use std::collections::HashMap;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::array_cell::ArrayCell;
use crate::datetime::{NdbDateTime, NdbDuration};
use crate::geometry::Geometry;
use crate::json_msgpack::InstantKind;

/// A dynamic value that can represent any field type in a document
/// or any parameter in a SQL query.
///
/// # Serialization policy
///
/// **JSON (`#[serde(untagged)]`) — API-boundary format, documented lossy.**
/// JSON output uses plain JSON types (`"string"`, `1`, `true`, `null`,
/// `[…]`, `{…}`) so that HTTP/pgwire clients see idiomatic JSON without
/// tagged wrappers. This is intentional but lossy for six variants:
///
/// | Variant        | JSON representation | Round-trip loss |
/// |----------------|---------------------|-----------------|
/// | `Uuid(s)`      | `"<s>"`             | decoded as `String` |
/// | `Ulid(s)`      | `"<s>"`             | decoded as `String` |
/// | `Regex(s)`     | `"<s>"`             | decoded as `String` |
/// | `Range {…}`    | `null`              | decoded as `Null` |
/// | `Record {…}`   | `null`              | decoded as `Null` |
/// | `ArrayCell`    | `{"coords":[…], "attrs":[…]}` | decoded as `Object` without type discriminator |
/// | `Vector(v)`    | `[f32, …]` (JSON number array) | decoded as `Array<Float>` |
///
/// **Round-trip through JSON is NOT preserved for these six variants.**
/// JSON is permitted only at the HTTP/pgwire API boundary
/// (`decode_payload_to_json()` in the Control Plane); it must never be
/// used for Data Plane storage, WAL records, or cross-plane messages.
///
/// **zerompk MessagePack (hand-rolled `ToMessagePack`/`FromMessagePack`
/// in `value/msgpack.rs`) — internal transport format, lossless.**
/// Every variant round-trips through MessagePack without loss. All
/// internal paths (Data Plane, SPSC bridge, WAL payloads) MUST use
/// zerompk. JSON is reserved for the API boundary and forbidden in
/// these contexts.
///
/// `#[non_exhaustive]` — new value kinds will be added in future releases
/// (e.g. `Vector`, typed collections). This attribute enforces Rust API
/// hygiene only; the concrete serialization contract is described above.
#[non_exhaustive]
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
    /// UTC timestamp with microsecond precision (timezone-aware).
    DateTime(NdbDateTime),
    /// Naive (local/no-timezone) timestamp with microsecond precision.
    NaiveDateTime(NdbDateTime),
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
    ArrayCell(ArrayCell),
    /// Dense f32 embedding vector (e.g. HNSW, neural embeddings).
    ///
    /// Stored as a reference-counted slice to allow cheap cloning without
    /// copying the underlying floats. Serialized as raw bytes
    /// (`bytemuck::cast_slice`) with zerompk tag 20; JSON serializes as a
    /// plain number array (lossy — decodes as `Array<Float>`, not `Vector`).
    Vector(Arc<[f32]>),
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

    /// Try to extract as DateTime (timezone-aware).
    pub fn as_datetime(&self) -> Option<&NdbDateTime> {
        match self {
            Value::DateTime(dt) => Some(dt),
            _ => None,
        }
    }

    /// Try to extract as NaiveDateTime (no timezone).
    pub fn as_naive_datetime(&self) -> Option<&NdbDateTime> {
        match self {
            Value::NaiveDateTime(dt) => Some(dt),
            _ => None,
        }
    }

    /// Try to extract as an instant of either kind.
    pub fn as_instant(&self) -> Option<(InstantKind, NdbDateTime)> {
        match self {
            Value::DateTime(dt) => Some((InstantKind::Utc, *dt)),
            Value::NaiveDateTime(dt) => Some((InstantKind::Naive, *dt)),
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
            Value::NaiveDateTime(_) => "naive_datetime",
            Value::Duration(_) => "duration",
            Value::Decimal(_) => "decimal",
            Value::Geometry(_) => "geometry",
            Value::Set(_) => "set",
            Value::Regex(_) => "regex",
            Value::Range { .. } => "range",
            Value::Record { .. } => "record",
            Value::ArrayCell(_) => "array_cell",
            Value::Vector(_) => "vector",
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

    /// Construct a `Value::Vector` from any slice of f32 values.
    pub fn vector(floats: impl Into<Arc<[f32]>>) -> Self {
        Value::Vector(floats.into())
    }

    /// Try to extract as an f32 embedding slice.
    pub fn as_vector(&self) -> Option<&[f32]> {
        match self {
            Value::Vector(v) => Some(v),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn naive_datetime_roundtrip_msgpack() {
        let dt = NdbDateTime::from_micros(1_700_000_000_000_000);
        let v = Value::NaiveDateTime(dt);
        let bytes = zerompk::to_msgpack_vec(&v).expect("encode");
        let decoded: Value = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(decoded, v);
    }

    // ── zerompk lossless roundtrips for the six JSON-lossy variants ───────

    #[test]
    fn uuid_roundtrip_msgpack() {
        let v = Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into());
        let bytes = zerompk::to_msgpack_vec(&v).expect("encode");
        let decoded: Value = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(decoded, v);
    }

    #[test]
    fn ulid_roundtrip_msgpack() {
        let v = Value::Ulid("01ARZ3NDEKTSV4RRFFQ69G5FAV".into());
        let bytes = zerompk::to_msgpack_vec(&v).expect("encode");
        let decoded: Value = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(decoded, v);
    }

    #[test]
    fn regex_roundtrip_msgpack() {
        let v = Value::Regex(r"^\d+$".into());
        let bytes = zerompk::to_msgpack_vec(&v).expect("encode");
        let decoded: Value = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(decoded, v);
    }

    #[test]
    fn range_roundtrip_msgpack() {
        let v = Value::Range {
            start: Some(Box::new(Value::Integer(1))),
            end: Some(Box::new(Value::Integer(10))),
            inclusive: false,
        };
        let bytes = zerompk::to_msgpack_vec(&v).expect("encode");
        let decoded: Value = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(decoded, v);
    }

    #[test]
    fn record_roundtrip_msgpack() {
        let v = Value::Record {
            table: "users".into(),
            id: "abc123".into(),
        };
        let bytes = zerompk::to_msgpack_vec(&v).expect("encode");
        let decoded: Value = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(decoded, v);
    }

    #[test]
    fn array_cell_roundtrip_msgpack() {
        use crate::array_cell::ArrayCell;
        let v = Value::ArrayCell(ArrayCell {
            coords: vec![Value::Integer(1), Value::Integer(2)],
            attrs: vec![Value::Float(3.5), Value::String("label".into())],
            system_time: None,
        });
        let bytes = zerompk::to_msgpack_vec(&v).expect("encode");
        let decoded: Value = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(decoded, v);
    }

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

    // ── Value::Vector tests ───────────────────────────────────────────────

    #[test]
    fn vector_roundtrip_msgpack_empty() {
        let v = Value::Vector(Arc::from([] as [f32; 0]));
        let bytes = zerompk::to_msgpack_vec(&v).expect("encode");
        let decoded: Value = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(decoded, v);
    }

    #[test]
    fn vector_roundtrip_msgpack_four_elements() {
        let v: Value = vec![1.0f32, 2.0, 3.0, 4.0].into();
        let bytes = zerompk::to_msgpack_vec(&v).expect("encode");
        let decoded: Value = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(decoded, v);
    }

    #[test]
    fn vector_roundtrip_msgpack_large() {
        let floats: Vec<f32> = (0..1025).map(|i| i as f32 * 0.1).collect();
        let v: Value = floats.into();
        let bytes = zerompk::to_msgpack_vec(&v).expect("encode");
        let decoded: Value = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(decoded, v);
    }

    #[test]
    fn vector_bad_byte_len_rejected() {
        use std::io::Write;
        let mut buf = vec![0x92u8, 20, 0xc4, 3];
        buf.write_all(&[0xAAu8, 0xBB, 0xCC]).unwrap();
        let result: zerompk::Result<Value> = zerompk::from_msgpack(&buf);
        assert!(
            result.is_err(),
            "3-byte payload (not divisible by 4) must be rejected"
        );
    }

    #[test]
    fn vector_json_forward_is_number_array() {
        let v: Value = vec![1.0f32, 2.0, 3.0].into();
        let json = serde_json::Value::from(v);
        assert!(json.is_array(), "Vector must serialize as JSON array");
        let arr = json.as_array().unwrap();
        assert_eq!(arr.len(), 3);
        assert!(arr[0].is_number());
    }

    #[test]
    fn vector_json_reverse_stays_array_of_float() {
        let json = serde_json::json!([1.0, 2.0, 3.0]);
        let v = Value::from(json);
        assert!(
            matches!(v, Value::Array(_)),
            "JSON number array round-trips as Array, not Vector"
        );
    }

    #[test]
    fn vector_display_truncates_after_8() {
        let floats: Vec<f32> = (0..10).map(|i| i as f32).collect();
        let v: Value = floats.into();
        let s = v.to_string();
        assert!(s.contains("10 total"), "display must show total count: {s}");
        assert!(s.contains("…"), "display must contain ellipsis: {s}");
    }

    #[test]
    fn vector_from_conversions() {
        let vec_val: Value = vec![1.0f32, 2.0].into();
        assert!(matches!(vec_val, Value::Vector(_)));

        let arc: Arc<[f32]> = Arc::from([3.0f32, 4.0]);
        let arc_val: Value = arc.into();
        assert!(matches!(arc_val, Value::Vector(_)));

        let slice: &[f32] = &[5.0f32, 6.0];
        let slice_val: Value = slice.into();
        assert!(matches!(slice_val, Value::Vector(_)));

        let helper = Value::vector(vec![7.0f32]);
        assert_eq!(helper.as_vector(), Some([7.0f32].as_slice()));
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
