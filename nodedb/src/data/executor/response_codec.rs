//! Response payload serialization for SPSC bridge transport.
//!
//! Replaces `serde_json::to_vec` + `serde_json::json!` on all Data Plane
//! response hot paths. Uses MessagePack (`zerompk`) for serialization
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

use sonic_rs;
use std::sync::Arc;

use super::msgpack_utils::write_str;

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use serde::Serialize;

/// Serialize a response payload as MessagePack bytes.
///
/// Drop-in replacement for `serde_json::to_vec(&value)` in handler code.
/// Returns MessagePack bytes that are 30-50% smaller and 2-3x faster to
/// produce than JSON.
pub(super) fn encode<T: zerompk::ToMessagePack>(value: &T) -> crate::Result<Vec<u8>> {
    zerompk::to_msgpack_vec(value).map_err(|e| crate::Error::Codec {
        detail: format!("response serialization: {e}"),
    })
}

/// Encode a serde_json::Value payload as MessagePack bytes.
pub(super) fn encode_json(value: &serde_json::Value) -> crate::Result<Vec<u8>> {
    nodedb_types::json_to_msgpack(value).map_err(|e| crate::Error::Codec {
        detail: format!("response serialization: {e}"),
    })
}

/// Encode any `Serialize` type as MessagePack bytes.
///
/// Serializes via serde to an intermediate `serde_json::Value`, then converts
/// to MessagePack. Use `encode()` for types that implement `ToMessagePack`
/// directly (faster, no intermediate).
pub(super) fn encode_serde<T: serde::Serialize>(value: &T) -> crate::Result<Vec<u8>> {
    let json_value = serde_json::to_value(value).map_err(|e| crate::Error::Codec {
        detail: format!("serde serialization: {e}"),
    })?;
    encode_json(&json_value)
}

/// Encode a Vec of serde_json::Value as MessagePack bytes.
pub(super) fn encode_json_vec(values: &[serde_json::Value]) -> crate::Result<Vec<u8>> {
    let wrapped: Vec<nodedb_types::JsonValue> = values
        .iter()
        .map(|v| nodedb_types::JsonValue(v.clone()))
        .collect();
    zerompk::to_msgpack_vec(&wrapped).map_err(|e| crate::Error::Codec {
        detail: format!("response serialization: {e}"),
    })
}

/// Encode a slice of `nodedb_types::Value` as a msgpack array.
///
/// No JSON intermediary — values are serialized directly to standard msgpack.
pub(super) fn encode_value_vec(values: &[nodedb_types::Value]) -> crate::Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(values.len() * 64);
    // Write array header.
    let n = values.len();
    if n <= 15 {
        buf.push(0x90 | n as u8);
    } else if n <= 0xFFFF {
        buf.push(0xDC);
        buf.extend_from_slice(&(n as u16).to_be_bytes());
    } else {
        buf.push(0xDD);
        buf.extend_from_slice(&(n as u32).to_be_bytes());
    }
    for val in values {
        let encoded = nodedb_types::value_to_msgpack(val).map_err(|e| crate::Error::Codec {
            detail: format!("value serialization: {e}"),
        })?;
        buf.extend_from_slice(&encoded);
    }
    Ok(buf)
}

/// Decode a Response payload (msgpack array) back to `(doc_id, msgpack_bytes)` pairs.
///
/// Used for inline sub-plans in multi-way joins: the inner join produces a
/// Response, and the outer join needs its rows as input.
pub(super) fn decode_response_to_docs(
    response: &crate::bridge::envelope::Response,
) -> Option<Vec<(String, Vec<u8>)>> {
    use nodedb_query::msgpack_scan;

    let payload = response.payload.as_ref();
    if payload.is_empty() {
        return None;
    }

    // Payload is a msgpack array of maps.
    let (count, mut offset) = msgpack_scan::array_header(payload, 0)?;
    let mut docs = Vec::with_capacity(count);

    for _ in 0..count {
        let entry_start = offset;
        // Extract "id" field from this map entry.
        let id = msgpack_scan::extract_field(payload, offset, "id")
            .and_then(|(s, _e)| msgpack_scan::read_str(payload, s).map(|s| s.to_string()))
            .unwrap_or_default();
        // Skip past this entire map value to get the next offset.
        let next = msgpack_scan::skip_value(payload, offset)?;
        let entry_bytes = payload[entry_start..next].to_vec();
        docs.push((id, entry_bytes));
        offset = next;
    }

    Some(docs)
}

/// Decode a payload (msgpack array or JSON array) to `(doc_id, msgpack_bytes)` pairs.
///
/// Used for inline hash join: decodes the result of an inner join back to doc pairs.
/// Handles both msgpack (from encode_binary_rows) and JSON (from broadcast_to_all_cores merge).
pub(super) fn decode_response_to_docs_from_bytes(payload: &[u8]) -> Option<Vec<(String, Vec<u8>)>> {
    use nodedb_query::msgpack_scan;

    if payload.is_empty() {
        return None;
    }

    // Try msgpack first.
    if let Some((count, mut offset)) = msgpack_scan::array_header(payload, 0) {
        let mut docs = Vec::with_capacity(count);
        for _ in 0..count {
            let entry_start = offset;
            let id = msgpack_scan::extract_field(payload, offset, "id")
                .and_then(|(s, _e)| msgpack_scan::read_str(payload, s).map(|s| s.to_string()))
                .unwrap_or_default();
            let next = msgpack_scan::skip_value(payload, offset)?;
            let entry_bytes = payload[entry_start..next].to_vec();
            docs.push((id, entry_bytes));
            offset = next;
        }
        return Some(docs);
    }

    // JSON fallback (from broadcast_to_all_cores merge which returns JSON text).
    if payload.first() == Some(&b'[') {
        let arr: Vec<serde_json::Value> = sonic_rs::from_slice(payload).ok()?;
        let mut docs = Vec::with_capacity(arr.len());
        for val in &arr {
            let id = val
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let mp = nodedb_types::json_to_msgpack(val).unwrap_or_default();
            docs.push((id, mp));
        }
        return Some(docs);
    }

    None
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

/// Encode document rows with raw MessagePack passthrough for the data field.
///
/// Each row is `(doc_id, raw_msgpack_bytes)`. The raw bytes are written directly
/// into the output without decoding to `serde_json::Value` first. This eliminates
/// the decode→re-encode cycle that was the main serialization tax on document reads.
///
/// Output format: msgpack array of `{"id": "<doc_id>", "data": <raw_msgpack_value>}`.
pub(super) fn encode_raw_document_rows(rows: &[(String, Vec<u8>)]) -> crate::Result<Vec<u8>> {
    // Pre-estimate capacity: ~32 bytes overhead per row + data sizes.
    let data_size: usize = rows.iter().map(|(id, d)| id.len() + d.len() + 16).sum();
    let mut buf = Vec::with_capacity(data_size + 8);

    // Write array header.
    msgpack_write_array_header(&mut buf, rows.len());

    for (id, data_bytes) in rows {
        // Write map header (2 entries: "id" and "data").
        buf.push(0x82); // fixmap with 2 entries

        // Write "id" key + value.
        write_str(&mut buf, "id");
        write_str(&mut buf, id);

        // Write "data" key.
        write_str(&mut buf, "data");

        // Raw passthrough: write the msgpack bytes directly as the value.
        // These bytes are already a valid msgpack map from storage.
        buf.extend_from_slice(data_bytes);
    }

    Ok(buf)
}

/// Decode concatenated row payloads into `(doc_id, msgpack_data)` pairs.
///
/// Input: zero or more msgpack arrays back-to-back. Elements may be either:
/// - raw scan rows from `encode_raw_document_rows` with `{id, data}` wrappers
/// - plain msgpack rows from aggregate/join paths serialized via `encode_json_vec`
///
/// For wrapped scan rows, the `data` field's raw bytes are extracted. For
/// plain rows, the entire row value is returned as `msgpack_data`.
pub(super) fn decode_raw_scan_to_docs(bytes: &[u8]) -> Vec<(String, Vec<u8>)> {
    use nodedb_query::msgpack_scan;

    let mut results = Vec::new();
    let mut pos = 0;

    while pos < bytes.len() {
        // Read array header.
        let first = bytes[pos];
        let (count, hdr_len) = if (0x90..=0x9f).contains(&first) {
            ((first & 0x0f) as usize, 1)
        } else if first == 0xdc && pos + 3 <= bytes.len() {
            (
                u16::from_be_bytes([bytes[pos + 1], bytes[pos + 2]]) as usize,
                3,
            )
        } else if first == 0xdd && pos + 5 <= bytes.len() {
            (
                u32::from_be_bytes([
                    bytes[pos + 1],
                    bytes[pos + 2],
                    bytes[pos + 3],
                    bytes[pos + 4],
                ]) as usize,
                5,
            )
        } else {
            break;
        };

        let mut inner = pos + hdr_len;
        for _ in 0..count {
            if inner >= bytes.len() {
                break;
            }

            // Scan payloads use {id, data}. Aggregate/join payloads are plain
            // msgpack maps, so fall back to the whole element when "data" is
            // absent.
            let elem_start = inner;
            let elem_end = msgpack_scan::skip_value(bytes, inner).unwrap_or(bytes.len());

            let id = msgpack_scan::extract_field(bytes, elem_start, "id")
                .and_then(|(s, _e)| msgpack_scan::read_value(bytes, s))
                .and_then(|v| match v {
                    nodedb_types::Value::String(s) => Some(s),
                    _ => None,
                })
                .unwrap_or_default();

            let data = msgpack_scan::extract_field(bytes, elem_start, "data")
                .map(|(s, e)| bytes[s..e].to_vec())
                .unwrap_or_else(|| bytes[elem_start..elem_end].to_vec());

            results.push((id, data));

            inner = elem_end;
        }
        pos = inner;
    }

    results
}

/// Encode a list of pre-built binary msgpack rows into a single msgpack array.
///
/// Each row is already a valid msgpack value (typically a map). This just
/// wraps them in an array header and concatenates — zero decode.
pub fn encode_binary_rows(rows: &[Vec<u8>]) -> Vec<u8> {
    let data_size: usize = rows.iter().map(|r| r.len()).sum();
    let mut buf = Vec::with_capacity(data_size + 8);
    msgpack_write_array_header(&mut buf, rows.len());
    for row in rows {
        buf.extend_from_slice(row);
    }
    buf
}

/// Write a msgpack array header.
fn msgpack_write_array_header(buf: &mut Vec<u8>, len: usize) {
    if len < 16 {
        buf.push(0x90 | len as u8);
    } else if len <= u16::MAX as usize {
        buf.push(0xDC);
        buf.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        buf.push(0xDD);
        buf.extend_from_slice(&(len as u32).to_be_bytes());
    }
}

/// Encode a simple `{"key": count}` response (for insert confirmations).
pub(super) fn encode_count(key: &str, count: usize) -> crate::Result<Vec<u8>> {
    let mut map = std::collections::BTreeMap::new();
    map.insert(key, count);
    zerompk::to_msgpack_vec(&map).map_err(|e| crate::Error::Codec {
        detail: format!("count response serialization: {e}"),
    })
}

/// Decode a MessagePack or JSON payload to a JSON string for pgwire/HTTP output.
///
/// Auto-detects format: if first byte indicates MessagePack, transcodes directly
/// to JSON text via streaming transcoder (no intermediate `serde_json::Value`).
/// If already JSON (starts with `[` or `{`), returns as-is.
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

    // Streaming msgpack → JSON transcoder: zero intermediate types.
    nodedb_types::msgpack_to_json_string(payload)
        .unwrap_or_else(|_| String::from_utf8_lossy(payload).into_owned())
}

/// Intermediate types for response serialization.
/// These implement Serialize for MessagePack encoding without going through
/// `serde_json::Value` heap allocations.

#[derive(Serialize, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
pub(super) struct VectorSearchHit {
    pub id: u32,
    pub distance: f32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub doc_id: Option<String>,
    /// Raw msgpack body of the document, populated by the Data Plane only
    /// when `rls_filters` is non-empty so the Control Plane response
    /// translator can evaluate the predicate at the security boundary.
    /// Always `None` for non-RLS queries and stripped before the payload
    /// reaches the client.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub body: Option<Vec<u8>>,
}

#[derive(Serialize, Clone)]
pub(super) struct DocumentRow {
    pub id: String,
    pub data: serde_json::Value,
}

impl zerompk::ToMessagePack for DocumentRow {
    fn write<W: zerompk::Write>(&self, writer: &mut W) -> zerompk::Result<()> {
        writer.write_map_len(2)?;
        writer.write_string("id")?;
        writer.write_string(&self.id)?;
        writer.write_string("data")?;
        nodedb_types::json_msgpack::JsonValue(self.data.clone()).write(writer)
    }
}

#[derive(Serialize, zerompk::ToMessagePack)]
#[msgpack(map)]
pub(super) struct NeighborEntry<'a> {
    pub label: &'a str,
    pub node: &'a str,
}

#[derive(Serialize, zerompk::ToMessagePack)]
#[msgpack(map)]
pub(super) struct NeighborMultiEntry<'a> {
    pub src: &'a str,
    pub label: &'a str,
    pub node: &'a str,
}

#[derive(Serialize, zerompk::ToMessagePack)]
#[msgpack(map)]
pub(super) struct SubgraphEdge<'a> {
    pub src: &'a str,
    pub label: &'a str,
    pub dst: &'a str,
}

#[derive(Serialize, zerompk::ToMessagePack)]
#[msgpack(map)]
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

/// Structured response for `ArrayOp::Slice`.
///
/// Carries the row payload alongside a flag that signals whether the
/// query's `system_as_of` cutoff fell below the oldest tile version on
/// this shard, meaning history was truncated. The shape is identical for
/// both local single-node responses and cluster shard responses.
#[derive(Serialize, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
pub(crate) struct ArraySliceResponse {
    /// Msgpack-encoded row bytes. Each element is one encoded `Value`.
    pub rows_msgpack: Vec<u8>,
    /// True when `system_as_of` is below the oldest tile version and the
    /// response may be incomplete due to horizon truncation.
    pub truncated_before_horizon: bool,
}

/// Structured response for `ArrayOp::Aggregate` (non-partial path).
///
/// Carries the aggregate row payload plus the horizon-truncation flag.
/// Partial responses (cluster fan-out) use a separate encoding to avoid
/// coupling the partial merge protocol to this struct.
#[derive(Serialize, zerompk::ToMessagePack, zerompk::FromMessagePack)]
#[msgpack(map)]
#[allow(dead_code)]
pub(crate) struct ArrayAggregateResponse {
    /// Msgpack-encoded rows.
    pub rows_msgpack: Vec<u8>,
    /// True when the query's `system_as_of` fell below the oldest tile version.
    pub truncated_before_horizon: bool,
}

#[derive(Serialize, zerompk::ToMessagePack)]
#[msgpack(map)]
pub(super) struct TextSearchHit<'a> {
    pub doc_id: &'a str,
    pub score: f32,
    pub fuzzy: bool,
}

#[derive(Serialize, zerompk::ToMessagePack)]
#[msgpack(map)]
pub(super) struct HybridSearchHit<'a> {
    pub doc_id: &'a str,
    pub rrf_score: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vector_rank: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text_rank: Option<usize>,
}

#[derive(Serialize, zerompk::ToMessagePack)]
#[msgpack(map)]
pub(super) struct GraphRagResponse {
    pub results: Vec<GraphRagResult>,
    pub metadata: GraphRagMetadata,
}

#[derive(Serialize, zerompk::ToMessagePack)]
#[msgpack(map)]
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
                body: None,
            },
            VectorSearchHit {
                id: 2,
                distance: 0.8,
                doc_id: None,
                body: None,
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
        let msgpack = nodedb_types::json_to_msgpack(&value).unwrap();
        let json = decode_payload_to_json(&msgpack);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["key"], "value");
        assert_eq!(parsed["num"], 42);
    }

    #[test]
    fn raw_document_rows_roundtrip() {
        let doc1 = serde_json::json!({"name": "alice", "age": 30});
        let doc2 = serde_json::json!({"name": "bob", "age": 25});
        let msgpack1 = nodedb_types::json_to_msgpack(&doc1).unwrap();
        let msgpack2 = nodedb_types::json_to_msgpack(&doc2).unwrap();

        let rows = vec![
            ("doc1".to_string(), msgpack1),
            ("doc2".to_string(), msgpack2),
        ];

        let encoded = encode_raw_document_rows(&rows).unwrap();
        let json = decode_payload_to_json(&encoded);
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0]["id"], "doc1");
        assert_eq!(parsed[0]["data"]["name"], "alice");
        assert_eq!(parsed[0]["data"]["age"], 30);
        assert_eq!(parsed[1]["id"], "doc2");
        assert_eq!(parsed[1]["data"]["name"], "bob");
    }

    #[test]
    fn raw_document_rows_empty() {
        let rows: Vec<(String, Vec<u8>)> = vec![];
        let encoded = encode_raw_document_rows(&rows).unwrap();
        let json = decode_payload_to_json(&encoded);
        let parsed: Vec<serde_json::Value> = serde_json::from_str(&json).unwrap();
        assert!(parsed.is_empty());
    }

    #[test]
    fn decode_raw_scan_to_docs_accepts_plain_rows() {
        let rows = vec![serde_json::json!({"avg_amount": 43.598})];
        let encoded = encode_json_vec(&rows).unwrap();

        let decoded = decode_raw_scan_to_docs(&encoded);

        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].0, "");
        let decoded_json = decode_payload_to_json(&decoded[0].1);
        let parsed: serde_json::Value = serde_json::from_str(&decoded_json).unwrap();
        assert_eq!(parsed["avg_amount"], 43.598);
    }

    #[test]
    fn decode_raw_scan_to_docs_handles_mixed_arrays() {
        let wrapped_doc = serde_json::json!({"name": "alice"});
        let wrapped_rows = vec![(
            "doc1".to_string(),
            nodedb_types::json_to_msgpack(&wrapped_doc).unwrap(),
        )];
        let wrapped = encode_raw_document_rows(&wrapped_rows).unwrap();

        let plain_rows = vec![serde_json::json!({"avg_amount": 43.598})];
        let plain = encode_json_vec(&plain_rows).unwrap();

        let mut combined = wrapped;
        combined.extend_from_slice(&plain);

        let decoded = decode_raw_scan_to_docs(&combined);

        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].0, "doc1");
        assert_eq!(decode_payload_to_json(&decoded[0].1), r#"{"name":"alice"}"#);
        assert_eq!(decoded[1].0, "");
        let parsed: serde_json::Value =
            serde_json::from_str(&decode_payload_to_json(&decoded[1].1)).unwrap();
        assert_eq!(parsed["avg_amount"], 43.598);
    }
}
