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

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::arrow::record_batch::RecordBatch;
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

/// Encode document rows as Arrow IPC bytes for columnar transport.
///
/// Converts `Vec<(doc_id, serde_json::Value)>` into an Arrow RecordBatch
/// serialized as IPC stream bytes. Schema is inferred from the first row.
/// The Control Plane receives native Arrow batches for DataFusion processing.
///
/// Returns `None` if rows are empty or schema inference fails.
pub fn encode_as_arrow_ipc(
    rows: &[(String, serde_json::Value)],
    projection: &[String],
) -> Option<Vec<u8>> {
    if rows.is_empty() {
        return None;
    }

    // Determine fields from projection or first row.
    let first_obj = rows[0].1.as_object()?;
    let field_names: Vec<&str> = if projection.is_empty() {
        first_obj.keys().map(|k| k.as_str()).collect()
    } else {
        projection.iter().map(|s| s.as_str()).collect()
    };

    if field_names.is_empty() {
        return None;
    }

    // Build schema: id + projected fields.
    let mut fields = vec![Field::new("id", DataType::Utf8, false)];
    for &name in &field_names {
        let dt = first_obj
            .get(name)
            .map(infer_type)
            .unwrap_or(DataType::Utf8);
        fields.push(Field::new(name, dt, true));
    }
    let schema = Arc::new(Schema::new(fields));

    // Build column arrays.
    let mut ids: Vec<String> = Vec::with_capacity(rows.len());
    let mut builders: Vec<ColBuilder> = field_names
        .iter()
        .map(|&name| {
            let dt = first_obj
                .get(name)
                .map(infer_type)
                .unwrap_or(DataType::Utf8);
            ColBuilder::new(dt, rows.len())
        })
        .collect();

    for (doc_id, data) in rows {
        ids.push(doc_id.clone());
        let obj = data.as_object();
        for (i, &name) in field_names.iter().enumerate() {
            match obj.and_then(|o| o.get(name)) {
                Some(v) => builders[i].push(v),
                None => builders[i].push_null(),
            }
        }
    }

    let mut arrays: Vec<ArrayRef> = vec![Arc::new(StringArray::from(ids))];
    for b in builders {
        arrays.push(b.finish());
    }

    let batch = RecordBatch::try_new(schema.clone(), arrays).ok()?;

    // Serialize as Arrow IPC stream.
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, &schema).ok()?;
        writer.write(&batch).ok()?;
        writer.finish().ok()?;
    }
    Some(buf)
}

/// Maximum rows per Arrow batch to prevent OOM.
pub const ARROW_BATCH_MAX_ROWS: usize = 65_536;
/// Maximum bytes per Arrow batch.
pub const ARROW_BATCH_MAX_BYTES: usize = 8 * 1024 * 1024; // 8 MiB

fn infer_type(v: &serde_json::Value) -> DataType {
    match v {
        serde_json::Value::Number(n) if n.is_i64() => DataType::Int64,
        serde_json::Value::Number(_) => DataType::Float64,
        serde_json::Value::Bool(_) => DataType::Boolean,
        _ => DataType::Utf8,
    }
}

enum ColBuilder {
    Str(Vec<Option<String>>),
    I64(Vec<Option<i64>>),
    F64(Vec<Option<f64>>),
}

impl ColBuilder {
    fn new(dt: DataType, cap: usize) -> Self {
        match dt {
            DataType::Int64 => Self::I64(Vec::with_capacity(cap)),
            DataType::Float64 => Self::F64(Vec::with_capacity(cap)),
            _ => Self::Str(Vec::with_capacity(cap)),
        }
    }

    fn push(&mut self, v: &serde_json::Value) {
        match self {
            Self::Str(vec) => vec.push(Some(match v {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            })),
            Self::I64(vec) => vec.push(v.as_i64()),
            Self::F64(vec) => vec.push(v.as_f64()),
        }
    }

    fn push_null(&mut self) {
        match self {
            Self::Str(v) => v.push(None),
            Self::I64(v) => v.push(None),
            Self::F64(v) => v.push(None),
        }
    }

    fn finish(self) -> ArrayRef {
        match self {
            Self::Str(v) => Arc::new(StringArray::from(v)) as ArrayRef,
            Self::I64(v) => Arc::new(Int64Array::from(v)) as ArrayRef,
            Self::F64(v) => Arc::new(Float64Array::from(v)) as ArrayRef,
        }
    }
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub doc_id: Option<String>,
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
pub(super) struct TextSearchHit<'a> {
    pub doc_id: &'a str,
    pub score: f32,
    pub fuzzy: bool,
}

#[derive(Serialize)]
pub(super) struct HybridSearchHit<'a> {
    pub doc_id: &'a str,
    pub rrf_score: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_rank: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text_rank: Option<usize>,
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
    /// Snapshot watermark LSN at the time of query execution.
    /// Consumers can use this to verify they are reading a consistent view.
    pub watermark_lsn: u64,
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
                doc_id: None,
            },
            VectorSearchHit {
                id: 2,
                distance: 0.8,
                doc_id: None,
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
