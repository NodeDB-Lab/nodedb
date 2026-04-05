//! Document format conversion between JSON and MessagePack.
//!
//! Documents enter the system as JSON (from SQL INSERT via DataFusion).
//! They are stored in redb as MessagePack (compact binary, faster to
//! deserialize, supports targeted field extraction).
//!
//! On read, documents are returned as `serde_json::Value` regardless of
//! storage format. During migration, both JSON and MessagePack blobs may
//! coexist in the same redb table — format is detected by inspecting the
//! first byte (MessagePack maps start with 0x80-0x8F for fixmap, 0xDE for
//! map16, 0xDF for map32; JSON objects start with `{` = 0x7B).

/// Convert a document byte blob to `serde_json::Value`.
///
/// Auto-detects the format: MessagePack, JSON, or Binary Tuple.
/// Binary Tuple detection requires knowing the schema — if the bytes
/// don't match MessagePack or JSON, returns `None` (the caller should
/// use `strict_format::binary_tuple_to_json` with the schema if the
/// collection is known to be strict).
pub(super) fn decode_document(bytes: &[u8]) -> Option<serde_json::Value> {
    if bytes.is_empty() {
        return None;
    }

    // Detect MessagePack: maps start with 0x80-0x8F (fixmap), 0xDE (map16), 0xDF (map32).
    let first = bytes[0];
    if (0x80..=0x8F).contains(&first) || first == 0xDE || first == 0xDF {
        // Try MessagePack first.
        if let Ok(val) = nodedb_types::json_from_msgpack(bytes) {
            return Some(val);
        }
    }

    // Fall back to JSON.
    serde_json::from_slice(bytes).ok()

    // Note: Binary Tuple bytes are NOT auto-detected here because decoding
    // requires the schema. For strict collections, callers must check
    // doc_configs.storage_mode and use strict_format::binary_tuple_to_json().
}

/// Extract multiple fields from a MessagePack document without full
/// deserialization.
///
/// Parses the MessagePack blob as a dynamic `rmpv::Value`, extracts the
/// named fields, and converts each to `serde_json::Value`. Faster than
/// `decode_document()` when only a few fields are needed (e.g., GROUP BY
/// keys + aggregate source fields). The blob is parsed only once.
///
/// Returns a vector of `Option<serde_json::Value>` matching `field_names`
/// order. An element is `None` if the field is absent or the blob isn't a map.
pub(super) fn extract_fields(bytes: &[u8], field_names: &[&str]) -> Vec<Option<serde_json::Value>> {
    if bytes.is_empty() || field_names.is_empty() {
        return vec![None; field_names.len()];
    }

    let first = bytes[0];
    if (0x80..=0x8F).contains(&first) || first == 0xDE || first == 0xDF {
        if let Ok(rmpv::Value::Map(pairs)) = rmpv::decode::read_value(&mut &bytes[..]) {
            let mut results = vec![None; field_names.len()];
            for (k, v) in &pairs {
                if let rmpv::Value::String(key) = k
                    && let Some(key_str) = key.as_str()
                    && let Some(idx) = field_names.iter().position(|&n| n == key_str)
                {
                    results[idx] = Some(rmpv_to_json(v));
                }
            }
            return results;
        }
        return vec![None; field_names.len()];
    }

    // JSON fallback.
    if let Ok(doc) = serde_json::from_slice::<serde_json::Value>(bytes) {
        field_names
            .iter()
            .map(|name| doc.get(*name).cloned())
            .collect()
    } else {
        vec![None; field_names.len()]
    }
}

/// Convert an rmpv Value to a serde_json Value.
fn rmpv_to_json(val: &rmpv::Value) -> serde_json::Value {
    match val {
        rmpv::Value::Nil => serde_json::Value::Null,
        rmpv::Value::Boolean(b) => serde_json::Value::Bool(*b),
        rmpv::Value::Integer(i) => {
            if let Some(n) = i.as_i64() {
                serde_json::Value::Number(n.into())
            } else if let Some(n) = i.as_u64() {
                serde_json::Value::Number(n.into())
            } else {
                serde_json::Value::Null
            }
        }
        rmpv::Value::F32(f) => serde_json::Number::from_f64(*f as f64)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        rmpv::Value::F64(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        rmpv::Value::String(s) => serde_json::Value::String(s.as_str().unwrap_or("").to_string()),
        rmpv::Value::Binary(b) => {
            serde_json::Value::String(String::from_utf8_lossy(b).into_owned())
        }
        rmpv::Value::Array(arr) => serde_json::Value::Array(arr.iter().map(rmpv_to_json).collect()),
        rmpv::Value::Map(pairs) => {
            let mut map = serde_json::Map::new();
            for (k, v) in pairs {
                let key = match k {
                    rmpv::Value::String(s) => s.as_str().unwrap_or("").to_string(),
                    other => format!("{other}"),
                };
                map.insert(key, rmpv_to_json(v));
            }
            serde_json::Value::Object(map)
        }
        rmpv::Value::Ext(_, _) => serde_json::Value::Null,
    }
}

/// Encode a JSON value as MessagePack bytes for storage.
///
/// If encoding fails (should not happen for valid `serde_json::Value`),
/// falls back to JSON bytes.
pub(super) fn encode_to_msgpack(value: &serde_json::Value) -> Vec<u8> {
    nodedb_types::json_to_msgpack(value).unwrap_or_else(|_| {
        // Fallback: store as JSON if MessagePack encoding fails.
        serde_json::to_vec(value).unwrap_or_default()
    })
}

/// Convert JSON bytes to MessagePack bytes for storage.
///
/// If the input is already MessagePack, returns it unchanged.
/// If the input is JSON, deserializes and re-encodes as MessagePack.
/// If deserialization fails, returns the original bytes unchanged
/// (the storage engine is format-agnostic).
pub(super) fn json_to_msgpack(bytes: &[u8]) -> Vec<u8> {
    if bytes.is_empty() {
        return bytes.to_vec();
    }

    // Already MessagePack? Return as-is.
    let first = bytes[0];
    if (0x80..=0x8F).contains(&first) || first == 0xDE || first == 0xDF {
        return bytes.to_vec();
    }

    // Try parsing as JSON and converting to MessagePack.
    match serde_json::from_slice::<serde_json::Value>(bytes) {
        Ok(value) => encode_to_msgpack(&value),
        Err(_) => bytes.to_vec(), // Unknown format — store as-is.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn json_roundtrip_through_msgpack() {
        let original = serde_json::json!({"name": "alice", "age": 30, "tags": ["ml", "rust"]});
        let json_bytes = serde_json::to_vec(&original).unwrap();

        // Convert JSON → MessagePack.
        let msgpack_bytes = json_to_msgpack(&json_bytes);
        assert_ne!(
            json_bytes, msgpack_bytes,
            "should convert to different format"
        );

        // Decode from MessagePack.
        let decoded = decode_document(&msgpack_bytes).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn json_input_detected_correctly() {
        let json_bytes = b"{\"x\":1}";
        let decoded = decode_document(json_bytes).unwrap();
        assert_eq!(decoded["x"], 1);
    }

    #[test]
    fn msgpack_input_detected_correctly() {
        let value = serde_json::json!({"key": "value"});
        let msgpack = nodedb_types::json_to_msgpack(&value).unwrap();
        let decoded = decode_document(&msgpack).unwrap();
        assert_eq!(decoded["key"], "value");
    }

    #[test]
    fn already_msgpack_unchanged() {
        let value = serde_json::json!({"a": 1});
        let msgpack = nodedb_types::json_to_msgpack(&value).unwrap();
        let result = json_to_msgpack(&msgpack);
        assert_eq!(result, msgpack, "msgpack should pass through unchanged");
    }

    #[test]
    fn empty_bytes_handled() {
        assert!(decode_document(b"").is_none());
        assert!(json_to_msgpack(b"").is_empty());
    }

    #[test]
    fn extract_multiple_fields() {
        let value = serde_json::json!({"a": 1, "b": "hello", "c": true, "d": null});
        let msgpack = nodedb_types::json_to_msgpack(&value).unwrap();

        let results = extract_fields(&msgpack, &["a", "c", "z"]);
        assert_eq!(results[0], Some(serde_json::json!(1)));
        assert_eq!(results[1], Some(serde_json::json!(true)));
        assert_eq!(results[2], None); // "z" doesn't exist
    }
}
