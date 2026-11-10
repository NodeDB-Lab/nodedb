// SPDX-License-Identifier: BUSL-1.1

//! JSON text rendering of a row payload for pgwire / HTTP output.
//!
//! One rule decides a typed value's JSON on every path: a native value
//! renders through [`value_to_wire_json`], the renderer HTTP rows and pgwire
//! composite cells already use for a typed `Value`. A Decimal is a JSON
//! string of its exact digits and scale (`"1.50"`), never a JSON number. A
//! UUID is its text. An instant is ISO 8601 text. Bytes are unpadded base64.
//! Plain-stored bytes keep their transcoder rendering: a plain map holds no
//! Decimal, only the text a Decimal was written as, which is the same JSON
//! string.

use nodedb_types::ObjectView;

use super::super::encode::decode_payload_to_json;
use super::node::{RowContext, RowNode, classify, holds_native_row, native_value, row_error, span};
use crate::util::wire_json::value_to_wire_json;

/// Decode a row payload to JSON text under `context`.
///
/// A payload with no native object at a row position renders exactly as
/// [`decode_payload_to_json`] renders it. A native object that does not
/// decode fails the call.
pub fn decode_row_payload_to_json(payload: &[u8], context: RowContext) -> crate::Result<String> {
    if !holds_native_row(payload, context) {
        return Ok(decode_payload_to_json(payload));
    }
    let (value, end) = json_row(payload, 0, 0, context)?;
    if end != payload.len() {
        return Err(row_error(
            payload.len(),
            format!(
                "{} trailing bytes after the row payload",
                payload.len() - end
            ),
        ));
    }
    serde_json::to_string(&value).map_err(|e| row_error(payload.len(), e))
}

/// Render a document body as JSON when it is a native object.
///
/// `None` when the body is not a native object: the caller renders it with
/// its plain decoder. A native object renders through [`value_to_wire_json`].
/// A native object that does not decode fails the call.
pub fn render_native_body_json(body: &[u8]) -> crate::Result<Option<serde_json::Value>> {
    if nodedb_query::msgpack_scan::array_header(body, 0).is_none() {
        return Ok(None);
    }
    Ok(Some(value_to_wire_json(&native_value(body)?)))
}

/// Decode a stored document body to the JSON tree its plain form reads as.
///
/// For internal consumers whose logic reads a JSON tree: a native body reads
/// exactly as the plain body of the same values does (a Decimal is its text,
/// an instant its ISO 8601 text), so a lock key, a hash or a filter computed
/// from either form agrees. A plain map reads as `json_from_msgpack` reads
/// it. A body that does not decode fails the call.
pub fn decode_body_plain_json(body: &[u8]) -> crate::Result<serde_json::Value> {
    if nodedb_query::msgpack_scan::array_header(body, 0).is_none() {
        return nodedb_types::json_from_msgpack(body).map_err(|e| row_error(body.len(), e));
    }
    let value = native_value(body)?;
    let plain = nodedb_types::value_to_msgpack(&value).map_err(|e| row_error(body.len(), e))?;
    nodedb_types::json_from_msgpack(&plain).map_err(|e| row_error(body.len(), e))
}

/// Render the value at `pos` as JSON. Returns the value and where it ends.
fn json_row(
    bytes: &[u8],
    pos: usize,
    depth: usize,
    context: RowContext,
) -> crate::Result<(serde_json::Value, usize)> {
    match classify(bytes, pos, depth, context) {
        RowNode::Plain { end } => Ok((plain_json(span(bytes, pos, end))?, end)),
        RowNode::Native { end } => Ok((
            value_to_wire_json(&native_value(span(bytes, pos, end))?),
            end,
        )),
        RowNode::Envelope { end } => {
            let envelope = span(bytes, pos, end);
            let view = ObjectView::new(envelope).map_err(|e| row_error(envelope.len(), e))?;
            let mut fields = serde_json::Map::new();
            for entry in view.fields() {
                let (key, value) = entry.map_err(|e| row_error(envelope.len(), e))?;
                let value = if key == "data" {
                    value_to_wire_json(&native_value(value)?)
                } else {
                    plain_json(value)?
                };
                fields.insert(key.to_owned(), value);
            }
            Ok((serde_json::Value::Object(fields), end))
        }
        RowNode::List { count, first } => {
            let mut rows = Vec::with_capacity(count.min(bytes.len()));
            let mut at = first;
            for _ in 0..count {
                if at >= bytes.len() {
                    return Err(row_error(
                        bytes.len(),
                        format!("a row array of {count} rows ends after {} rows", rows.len()),
                    ));
                }
                let (row, end) = json_row(bytes, at, depth + 1, context)?;
                rows.push(row);
                at = end;
            }
            Ok((serde_json::Value::Array(rows), at))
        }
    }
}

/// Plain msgpack bytes as the transcoder renders them.
fn plain_json(bytes: &[u8]) -> crate::Result<serde_json::Value> {
    let text =
        nodedb_types::msgpack_to_json_string(bytes).map_err(|e| row_error(bytes.len(), e))?;
    sonic_rs::from_str(&text).map_err(|e| row_error(bytes.len(), e))
}

#[cfg(test)]
mod tests {
    use super::super::node::fixtures::{
        envelope, id_data_row, malformed_native, native_body, plain_body, typed_row,
    };
    use super::*;
    use crate::data::executor::response_codec::encode_binary_rows;

    fn json(text: &str) -> serde_json::Value {
        serde_json::from_str(text).unwrap()
    }

    /// A native body renders as `value_to_wire_json` renders its typed value:
    /// the same JSON HTTP rows and pgwire composite cells give it.
    #[test]
    fn a_native_row_renders_as_the_typed_renderer_renders_it() {
        let rendered = json(
            &decode_row_payload_to_json(&envelope(native_body()), RowContext::ScanEnvelope)
                .unwrap(),
        );
        let data = &rendered[0]["data"];
        assert_eq!(data, &value_to_wire_json(&typed_row()));
        assert_eq!(data["price"], "123.45");
        assert_eq!(data["uid"], "550e8400-e29b-41d4-a716-446655440000");
        assert_eq!(data["seen"], "2020-03-05T10:00:00.000000Z");
        assert_eq!(data["address"]["city"], "Oslo");
        assert_eq!(data["tags"], serde_json::json!([7, {"k": 1}]));
        assert_eq!(rendered[0]["id"], "0000000a");

        let flat = json(
            &decode_row_payload_to_json(&encode_binary_rows(&[native_body()]), RowContext::Flat)
                .unwrap(),
        );
        assert_eq!(flat[0], value_to_wire_json(&typed_row()));
        assert_eq!(
            render_native_body_json(&native_body()).unwrap(),
            Some(value_to_wire_json(&typed_row()))
        );
    }

    /// A plain body keeps its transcoder rendering: a Decimal written plain
    /// is its text.
    #[test]
    fn a_plain_row_renders_unchanged() {
        for context in [RowContext::Flat, RowContext::ScanEnvelope] {
            let plain = envelope(plain_body());
            assert_eq!(
                decode_row_payload_to_json(&plain, context).unwrap(),
                decode_payload_to_json(&plain)
            );
            assert_eq!(
                json(&decode_payload_to_json(&plain))[0]["data"]["price"],
                "123.45"
            );
        }
        assert_eq!(render_native_body_json(&plain_body()).unwrap(), None);
        let text = br#"[{"id":1}]"#;
        assert_eq!(
            decode_row_payload_to_json(text, RowContext::Flat).unwrap(),
            r#"[{"id":1}]"#
        );
    }

    /// A DECIMAL renders as one JSON string of its exact digits and scale,
    /// from a plain row and a native row alike, including digits an f64
    /// cannot hold.
    #[test]
    fn a_decimal_renders_as_its_exact_text_from_either_row() {
        use nodedb_types::Value;

        for text in ["1.50", "12345678901234567.89", "-0.001"] {
            let row =
                Value::Object([("v".to_owned(), Value::Decimal(text.parse().unwrap()))].into());
            let native = encode_binary_rows(&[zerompk::to_msgpack_vec(&row).unwrap()]);
            let plain = encode_binary_rows(&[nodedb_types::value_to_msgpack(&row).unwrap()]);
            let native = json(&decode_row_payload_to_json(&native, RowContext::Flat).unwrap());
            let plain = json(&decode_row_payload_to_json(&plain, RowContext::Flat).unwrap());
            assert_eq!(native[0]["v"], serde_json::Value::String(text.into()));
            assert_eq!(native, plain, "{text}");
        }
    }

    /// A user row with the columns `id` and `data` renders unchanged.
    #[test]
    fn a_flat_id_data_row_renders_unchanged() {
        let payload = id_data_row();
        let rendered = decode_row_payload_to_json(&payload, RowContext::Flat).unwrap();
        assert_eq!(rendered, decode_payload_to_json(&payload));
        assert_eq!(json(&rendered)[0]["data"], serde_json::json!([7, {"k": 1}]));
    }

    /// An internal consumer reads a native body as the plain body of the same
    /// values reads.
    #[test]
    fn a_native_body_reads_as_its_plain_json() {
        let plain = nodedb_types::json_from_msgpack(&plain_body()).unwrap();
        assert_eq!(decode_body_plain_json(&native_body()).unwrap(), plain);
        assert_eq!(decode_body_plain_json(&plain_body()).unwrap(), plain);
        assert_eq!(plain["price"], "123.45");
        assert!(decode_body_plain_json(&malformed_native()).is_err());
    }

    #[test]
    fn a_malformed_native_row_is_an_error() {
        assert!(matches!(
            decode_row_payload_to_json(&envelope(malformed_native()), RowContext::ScanEnvelope),
            Err(crate::Error::Serialization { .. })
        ));
        assert!(matches!(
            decode_row_payload_to_json(
                &encode_binary_rows(&[malformed_native()]),
                RowContext::Flat
            ),
            Err(crate::Error::Serialization { .. })
        ));
        assert!(render_native_body_json(&malformed_native()).is_err());
    }
}
