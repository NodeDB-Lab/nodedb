//! Field extraction from raw MessagePack maps.
//!
//! Given a `&[u8]` containing a MessagePack map, extract the byte range
//! of a value for a given key — without allocating or decoding.

use crate::msgpack_scan::reader::{map_header, skip_value, str_bounds};

/// A byte range `(start, end)` within a MessagePack buffer, pointing to
/// a complete value (tag + payload). Use `read_f64`, `read_i64`, `read_str`
/// etc. with `range.0` as the offset to decode the value.
pub type FieldRange = (usize, usize);

/// Locate the value for `field` in a MessagePack map starting at `offset`.
/// Returns the byte range `(value_start, value_end)` of the matched value.
///
/// Scans map keys sequentially — O(n) in number of keys. For documents
/// with many fields queried repeatedly, see structural indexing (Phase 8).
///
/// # Returns
/// - `Some((start, end))` — the value's byte range (use offset `start` with readers)
/// - `None` — field not found, or buffer is not a valid map
pub fn extract_field(buf: &[u8], offset: usize, field: &str) -> Option<FieldRange> {
    let (count, mut pos) = map_header(buf, offset)?;
    let field_bytes = field.as_bytes();

    for _ in 0..count {
        // Read key string bounds
        let key_match = match str_bounds(buf, pos) {
            Some((start, len)) => buf
                .get(start..start + len)
                .map(|kb| kb == field_bytes)
                .unwrap_or(false),
            None => false,
        };

        // Skip past the key
        pos = skip_value(buf, pos)?;

        if key_match {
            // Found — return the value's byte range
            let value_start = pos;
            let value_end = skip_value(buf, pos)?;
            return Some((value_start, value_end));
        }

        // Skip the value
        pos = skip_value(buf, pos)?;
    }

    None
}

/// Extract a value at a nested path (e.g., `["address", "city"]`).
/// Each segment must be a string key in a nested map.
pub fn extract_path(buf: &[u8], offset: usize, path: &[&str]) -> Option<FieldRange> {
    if path.is_empty() {
        return None;
    }

    let mut current_offset = offset;
    for (i, segment) in path.iter().enumerate() {
        let (value_start, value_end) = extract_field(buf, current_offset, segment)?;
        if i == path.len() - 1 {
            return Some((value_start, value_end));
        }
        // Intermediate segments must be maps — descend into the value
        current_offset = value_start;
    }

    None
}

/// Extract a value using a dot-separated path string (e.g., `"address.city"`).
/// Convenience wrapper over `extract_path`.
pub fn extract_dot_path(buf: &[u8], offset: usize, dot_path: &str) -> Option<FieldRange> {
    let segments: Vec<&str> = dot_path.split('.').collect();
    extract_path(buf, offset, &segments)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::msgpack_scan::reader::{read_f64, read_i64, read_str};
    use serde_json::json;

    fn encode(v: &serde_json::Value) -> Vec<u8> {
        nodedb_types::json_msgpack::json_to_msgpack(v).expect("encode")
    }

    #[test]
    fn extract_integer_field() {
        let buf = encode(&json!({"age": 25}));
        let (start, _end) = extract_field(&buf, 0, "age").unwrap();
        assert_eq!(read_i64(&buf, start), Some(25));
    }

    #[test]
    fn extract_string_field() {
        let buf = encode(&json!({"name": "alice"}));
        let (start, _end) = extract_field(&buf, 0, "name").unwrap();
        assert_eq!(read_str(&buf, start), Some("alice"));
    }

    #[test]
    fn extract_float_field() {
        let buf = encode(&json!({"score": 99.5}));
        let (start, _end) = extract_field(&buf, 0, "score").unwrap();
        assert_eq!(read_f64(&buf, start), Some(99.5));
    }

    #[test]
    fn extract_missing_field() {
        let buf = encode(&json!({"x": 1}));
        assert!(extract_field(&buf, 0, "y").is_none());
    }

    #[test]
    fn extract_multiple_fields() {
        let buf = encode(&json!({"a": 10, "b": 20, "c": 30}));

        let (s, _) = extract_field(&buf, 0, "a").unwrap();
        assert_eq!(read_i64(&buf, s), Some(10));

        let (s, _) = extract_field(&buf, 0, "b").unwrap();
        assert_eq!(read_i64(&buf, s), Some(20));

        let (s, _) = extract_field(&buf, 0, "c").unwrap();
        assert_eq!(read_i64(&buf, s), Some(30));
    }

    #[test]
    fn extract_nested_path() {
        let buf = encode(&json!({"address": {"city": "tokyo"}}));
        let (start, _end) = extract_path(&buf, 0, &["address", "city"]).unwrap();
        assert_eq!(read_str(&buf, start), Some("tokyo"));
    }

    #[test]
    fn extract_dot_path_works() {
        let buf = encode(&json!({"addr": {"zip": "10001"}}));
        let (start, _end) = extract_dot_path(&buf, 0, "addr.zip").unwrap();
        assert_eq!(read_str(&buf, start), Some("10001"));
    }

    #[test]
    fn extract_path_missing_intermediate() {
        let buf = encode(&json!({"x": 1}));
        assert!(extract_path(&buf, 0, &["x", "y"]).is_none());
    }

    #[test]
    fn extract_empty_path() {
        let buf = encode(&json!({}));
        assert!(extract_path(&buf, 0, &[]).is_none());
    }

    #[test]
    fn extract_from_large_map() {
        let mut map = serde_json::Map::new();
        for i in 0..20 {
            map.insert(format!("field_{i}"), json!(i));
        }
        let buf = encode(&serde_json::Value::Object(map));
        let (start, _end) = extract_field(&buf, 0, "field_9").unwrap();
        assert_eq!(read_i64(&buf, start), Some(9));
    }

    #[test]
    fn field_range_spans_entire_value() {
        let buf = encode(&json!({"data": [1, 2, 3]}));
        let (start, end) = extract_field(&buf, 0, "data").unwrap();
        let value_bytes = &buf[start..end];
        assert!(value_bytes.len() > 1);
    }
}
