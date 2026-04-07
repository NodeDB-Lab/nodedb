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

use sonic_rs;

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
    sonic_rs::from_slice(bytes).ok()

    // Note: Binary Tuple bytes are NOT auto-detected here because decoding
    // requires the schema. For strict collections, callers must check
    // doc_configs.storage_mode and use strict_format::binary_tuple_to_json().
}

/// Encode a JSON value as MessagePack bytes for storage.
///
/// If encoding fails (should not happen for valid `serde_json::Value`),
/// falls back to JSON bytes.
pub(super) fn encode_to_msgpack(value: &serde_json::Value) -> Vec<u8> {
    nodedb_types::json_to_msgpack(value).unwrap_or_else(|_| {
        // Fallback: store as JSON if MessagePack encoding fails.
        sonic_rs::to_vec(value).unwrap_or_default()
    })
}

/// Convert JSON bytes to MessagePack bytes for storage.
///
/// If the input is already MessagePack, returns it unchanged.
/// Ensure bytes are in standard MessagePack map format.
///
/// Handles three input formats:
/// - Standard msgpack map (0x80–0x8F / 0xDE / 0xDF): returned as-is.
/// - zerompk `nodedb_types::Value` tagged format: transcoded to standard msgpack map.
/// - JSON bytes: parsed and re-encoded as standard msgpack map.
/// - Unknown: stored as-is (storage engine is format-agnostic).
pub(super) fn json_to_msgpack(bytes: &[u8]) -> Vec<u8> {
    if bytes.is_empty() {
        return bytes.to_vec();
    }

    // Already a standard MessagePack map? Return as-is.
    let first = bytes[0];
    if (0x80..=0x8F).contains(&first) || first == 0xDE || first == 0xDF {
        return bytes.to_vec();
    }

    // Try decoding as nodedb_types::Value (zerompk tagged format) and transcode.
    // Only accept if the result is an Object — zerompk can spuriously "succeed"
    // on JSON bytes by interpreting the first byte (e.g. '{' = 0x7B) as a fixint.
    if let Ok(val @ nodedb_types::Value::Object(_)) = nodedb_types::value_from_msgpack(bytes) {
        let json: serde_json::Value = val.into();
        if let Ok(mp) = nodedb_types::json_to_msgpack(&json) {
            return mp;
        }
    }

    // Try parsing as JSON and converting to MessagePack.
    match sonic_rs::from_slice::<serde_json::Value>(bytes) {
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
}
