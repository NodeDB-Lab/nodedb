//! Response payload serialization for SPSC bridge transport.
//!
//! Replaces `serde_json::to_vec` + `serde_json::json!` on all Data Plane
//! response hot paths. Uses MessagePack (`rmp_serde`) for serialization
//! which is 2-3x faster and 30-50% smaller than JSON.
//!
//! The Control Plane converts MessagePack payloads to JSON text for pgwire
//! clients via `decode_payload_to_json()`.
//!
//! # Why MessagePack, not rkyv?
//!
//! rkyv requires `#[derive(Archive)]` on every response type and fixed
//! schemas. MessagePack is schema-less (like JSON but binary), works with
//! any `serde::Serialize` type, and is already a workspace dependency used
//! for document storage. The serialization speedup over JSON is 2-3x; rkyv
//! would be 5-10x but at the cost of schema rigidity and significant type
//! boilerplate across 13 handler files.

use serde::Serialize;

/// Serialize a response payload as MessagePack bytes.
///
/// Drop-in replacement for `serde_json::to_vec(&value)` in handler code.
/// Returns MessagePack bytes that are 30-50% smaller and 2-3x faster to
/// produce than JSON.
pub(super) fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, String> {
    // Use `to_vec_named` to preserve struct field names as string map keys.
    // Without this, rmp_serde uses integer indices (compact mode) which
    // produces `{0: value}` instead of `{"field": value}` on decode.
    rmp_serde::to_vec_named(value).map_err(|e| format!("response serialization: {e}"))
}

/// Encode a simple `{"key": count}` response (for insert confirmations).
pub(super) fn encode_count(key: &str, count: usize) -> Result<Vec<u8>, String> {
    let mut map = std::collections::BTreeMap::new();
    map.insert(key, count);
    rmp_serde::to_vec_named(&map).map_err(|e| format!("count response serialization: {e}"))
}

/// Decode a MessagePack or JSON payload to a JSON string for pgwire/HTTP output.
///
/// Auto-detects format: if first byte indicates MessagePack, deserializes
/// and re-encodes as JSON text. If already JSON (starts with `[` or `{`),
/// returns as-is.
pub fn decode_payload_to_json(payload: &[u8]) -> String {
    if payload.is_empty() {
        return String::new();
    }

    let first = payload[0];

    // MessagePack detection: arrays (0x90-0x9F, 0xDC, 0xDD) or maps (0x80-0x8F, 0xDE, 0xDF)
    // or other msgpack types that aren't valid JSON start bytes.
    let is_likely_json = first == b'['
        || first == b'{'
        || first == b'"'
        || first.is_ascii_digit()
        || first == b't'
        || first == b'f'
        || first == b'n';

    if is_likely_json {
        // Already JSON — return as-is.
        return String::from_utf8_lossy(payload).into_owned();
    }

    // Try MessagePack → JSON.
    match rmp_serde::from_slice::<serde_json::Value>(payload) {
        Ok(value) => serde_json::to_string(&value)
            .unwrap_or_else(|_| String::from_utf8_lossy(payload).into_owned()),
        Err(_) => String::from_utf8_lossy(payload).into_owned(),
    }
}

/// Intermediate types for response serialization.
/// These implement Serialize for MessagePack encoding without going through
/// `serde_json::Value` heap allocations.

#[derive(Serialize)]
pub(super) struct VectorSearchHit {
    pub id: u32,
    pub distance: f32,
}

#[derive(Serialize)]
pub(super) struct DocumentRow {
    pub id: String,
    pub data: serde_json::Value,
}

#[derive(Serialize)]
pub(super) struct NeighborEntry<'a> {
    pub label: &'a str,
    pub node: &'a str,
}

#[derive(Serialize)]
pub(super) struct SubgraphEdge<'a> {
    pub src: &'a str,
    pub label: &'a str,
    pub dst: &'a str,
}

#[derive(Serialize)]
pub(super) struct GraphRagResult {
    pub node_id: String,
    pub rrf_score: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_rank: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_distance: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hop_distance: Option<usize>,
}

#[derive(Serialize)]
pub(super) struct GraphRagResponse {
    pub results: Vec<GraphRagResult>,
    pub metadata: GraphRagMetadata,
}

#[derive(Serialize)]
pub(super) struct GraphRagMetadata {
    pub vector_candidates: usize,
    pub graph_expanded: usize,
    pub truncated: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_vector_hits() {
        let hits = vec![
            VectorSearchHit {
                id: 1,
                distance: 0.5,
            },
            VectorSearchHit {
                id: 2,
                distance: 0.8,
            },
        ];
        let bytes = encode(&hits).unwrap();
        assert!(!bytes.is_empty());

        // Decode back to verify.
        let json = decode_payload_to_json(&bytes);
        assert!(json.contains("\"id\""));
        assert!(json.contains("\"distance\""));
    }

    #[test]
    fn encode_count_msg() {
        let bytes = encode_count("inserted", 42).unwrap();
        let json = decode_payload_to_json(&bytes);
        assert!(json.contains("\"inserted\""));
        assert!(json.contains("42"));
    }

    #[test]
    fn json_passthrough() {
        let json_str = r#"[{"id":1}]"#;
        let result = decode_payload_to_json(json_str.as_bytes());
        assert_eq!(result, json_str);
    }

    #[test]
    fn msgpack_to_json_roundtrip() {
        let value = serde_json::json!({"key": "value", "num": 42});
        let msgpack = rmp_serde::to_vec(&value).unwrap();
        let json = decode_payload_to_json(&msgpack);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["key"], "value");
        assert_eq!(parsed["num"], 42);
    }
}
