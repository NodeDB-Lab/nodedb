// SPDX-License-Identifier: BUSL-1.1

//! Raw-msgpack passthrough encoders and the inverse decoder.
//!
//! The "raw" pattern eliminates the decode→re-encode cycle on document reads
//! by writing storage bytes directly into the response. The decoders read a
//! payload under the row shape its producer emits, which the caller states.

use super::super::msgpack_utils::write_str;
use super::rows::RowContext;

/// Encode document rows with raw MessagePack passthrough for the data field.
///
/// Each row is `(doc_id, raw_msgpack_bytes)`. The raw bytes are written directly
/// into the output without decoding to `serde_json::Value` first. This eliminates
/// the decode→re-encode cycle that was the main serialization tax on document reads.
///
/// Output format: msgpack array of `{"id": "<doc_id>", "data": <raw_msgpack_value>}`.
///
/// Visibility is `pub(crate)` (not Data-Plane-only) so the Control Plane sync
/// layer can re-encode the rows that survive shape-predicate filtering before
/// shipping a snapshot to subscribers.
pub(crate) fn encode_raw_document_rows(rows: &[(String, Vec<u8>)]) -> crate::Result<Vec<u8>> {
    let data_size: usize = rows.iter().map(|(id, d)| id.len() + d.len() + 16).sum();
    let mut buf = Vec::with_capacity(data_size + 8);

    msgpack_write_array_header(&mut buf, rows.len());

    for (id, data_bytes) in rows {
        // Write map header (2 entries: "id" and "data").
        buf.push(0x82); // fixmap with 2 entries

        write_str(&mut buf, "id");
        write_str(&mut buf, id);

        write_str(&mut buf, "data");

        // Raw passthrough: write the msgpack bytes directly as the value.
        // These bytes are already a valid msgpack map from storage.
        buf.extend_from_slice(data_bytes);
    }

    Ok(buf)
}

/// Split a gathered payload into its rows under a caller-stated row shape.
///
/// The payload is zero or more msgpack row arrays back to back, or one bare
/// row (a point read's plain map or native object). Each row yields
/// `(id, body)`:
/// - [`RowContext::ScanEnvelope`]: each row is a `{id, data}` envelope; `id`
///   is the envelope's storage key text and `body` its `data` value.
/// - [`RowContext::Flat`]: each row is the body itself and carries no
///   envelope id, so `id` is empty. A user row whose columns are `id` and
///   `data` stays whole.
///
/// Body bytes keep their encoding, plain or native. A payload in no row
/// shape, or an envelope without its `id` text or `data`, is a codec error.
pub fn scan_rows_in_context(
    bytes: &[u8],
    context: RowContext,
) -> crate::Result<Vec<(String, Vec<u8>)>> {
    use nodedb_query::msgpack_scan::{
        array_header, extract_field, map_header, read_str, skip_value,
    };

    let shape_error = |offset: usize, what: &str| crate::Error::Codec {
        detail: format!(
            "scan_rows_in_context: a {}-byte payload holds {what} at offset {offset}",
            bytes.len()
        ),
    };
    let split = |row: &[u8], offset: usize| -> crate::Result<(String, Vec<u8>)> {
        match context {
            RowContext::Flat => Ok((String::new(), row.to_vec())),
            RowContext::ScanEnvelope => {
                let id = extract_field(row, 0, "id")
                    .and_then(|(start, _)| read_str(row, start))
                    .ok_or_else(|| shape_error(offset, "a scan envelope with no text id"))?;
                let (start, end) = extract_field(row, 0, "data")
                    .ok_or_else(|| shape_error(offset, "a scan envelope with no data"))?;
                let body = row
                    .get(start..end)
                    .ok_or_else(|| shape_error(offset, "a truncated scan envelope"))?;
                Ok((id.to_owned(), body.to_vec()))
            }
        }
    };

    if map_header(bytes, 0).is_some() || super::rows::is_native_row(bytes) {
        if skip_value(bytes, 0) != Some(bytes.len()) {
            return Err(shape_error(0, "trailing bytes after one row"));
        }
        return Ok(vec![split(bytes, 0)?]);
    }

    let mut rows = Vec::new();
    let mut pos = 0;
    while pos < bytes.len() {
        let (count, mut at) =
            array_header(bytes, pos).ok_or_else(|| shape_error(pos, "no row array"))?;
        for _ in 0..count {
            let end = skip_value(bytes, at).ok_or_else(|| shape_error(at, "a truncated row"))?;
            let row = bytes
                .get(at..end)
                .ok_or_else(|| shape_error(at, "a truncated row"))?;
            rows.push(split(row, at)?);
            at = end;
        }
        pos = at;
    }
    Ok(rows)
}

/// Storage→relational boundary under a caller-stated row shape: flatten a
/// gathered payload into one msgpack array of flat rows.
///
/// Rows split as [`scan_rows_in_context`] splits them, and each row's body
/// becomes one flat row: under [`RowContext::ScanEnvelope`] an envelope's
/// `data`, under [`RowContext::Flat`] the row itself, so a user row whose
/// columns are `id` and `data` is never unwrapped. Row bytes keep their
/// encoding, plain or native. A payload in no row shape is a codec error.
pub fn flatten_rows_in_context(bytes: &[u8], context: RowContext) -> crate::Result<Vec<u8>> {
    let rows: Vec<Vec<u8>> = scan_rows_in_context(bytes, context)?
        .into_iter()
        .map(|(_, body)| body)
        .collect();
    Ok(encode_binary_rows(&rows))
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
pub(super) fn msgpack_write_array_header(buf: &mut Vec<u8>, len: usize) {
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

#[cfg(test)]
mod tests {
    use super::super::encode::{decode_payload_to_json, encode_json_vec_as_msgpack};
    use super::*;

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
    fn flat_rows_split_whole() {
        let rows = vec![serde_json::json!({"avg_amount": 43.598})];
        let encoded = encode_json_vec_as_msgpack(&rows).unwrap();

        let decoded = scan_rows_in_context(&encoded, RowContext::Flat).unwrap();

        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].0, "");
        let parsed: serde_json::Value =
            serde_json::from_str(&decode_payload_to_json(&decoded[0].1)).unwrap();
        assert_eq!(parsed["avg_amount"], 43.598);
    }

    /// Envelope arrays back to back split into `(id, data)` pairs.
    #[test]
    fn envelope_rows_split_into_id_and_body() {
        let wrapped_doc = serde_json::json!({"name": "alice"});
        let mut combined = encode_raw_document_rows(&[(
            "doc1".to_string(),
            nodedb_types::json_to_msgpack(&wrapped_doc).unwrap(),
        )])
        .unwrap();
        combined.extend(
            encode_raw_document_rows(&[(
                "doc2".to_string(),
                nodedb_types::json_to_msgpack(&serde_json::json!({"name": "bob"})).unwrap(),
            )])
            .unwrap(),
        );

        let decoded = scan_rows_in_context(&combined, RowContext::ScanEnvelope).unwrap();

        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].0, "doc1");
        assert_eq!(decode_payload_to_json(&decoded[0].1), r#"{"name":"alice"}"#);
        assert_eq!(decoded[1].0, "doc2");
        // An envelope payload read flat stays whole.
        let flat = scan_rows_in_context(&combined, RowContext::Flat).unwrap();
        assert_eq!(flat[0].0, "");
        let whole: serde_json::Value =
            serde_json::from_str(&decode_payload_to_json(&flat[0].1)).unwrap();
        assert_eq!(whole["id"], "doc1");
    }

    /// A flat payload is never unwrapped, even a row whose columns are `id`
    /// and `data`; an envelope payload yields each `data` body, plain or
    /// native, byte for byte. A bare native point-read row is one row.
    #[test]
    fn rows_flatten_under_the_stated_context() {
        use nodedb_types::Value;

        let flat_row = nodedb_types::value_to_msgpack(&Value::Object(
            [
                ("id".to_string(), Value::String("x".into())),
                (
                    "data".to_string(),
                    Value::Object([("k".to_string(), Value::Integer(1))].into()),
                ),
            ]
            .into(),
        ))
        .unwrap();
        let flat = encode_binary_rows(std::slice::from_ref(&flat_row));
        assert_eq!(
            flatten_rows_in_context(&flat, RowContext::Flat).unwrap(),
            flat
        );

        let body =
            Value::Object([("k".to_string(), Value::Decimal("1.5".parse().unwrap()))].into());
        let native = zerompk::to_msgpack_vec(&body).unwrap();
        let envelopes = encode_raw_document_rows(&[("0000000a".into(), native.clone())]).unwrap();
        assert_eq!(
            flatten_rows_in_context(&envelopes, RowContext::ScanEnvelope).unwrap(),
            encode_binary_rows(std::slice::from_ref(&native))
        );
        assert_eq!(
            flatten_rows_in_context(&native, RowContext::Flat).unwrap(),
            encode_binary_rows(std::slice::from_ref(&native))
        );
        assert!(matches!(
            flatten_rows_in_context(&[0x2a], RowContext::Flat),
            Err(crate::Error::Codec { .. })
        ));
    }
}
