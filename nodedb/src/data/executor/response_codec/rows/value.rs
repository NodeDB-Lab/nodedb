// SPDX-License-Identifier: BUSL-1.1

//! Typed decoding of a row payload: native objects keep their field types.

use std::collections::HashMap;

use nodedb_types::{ObjectView, Value};

use super::super::encode::decode_payload_value;
use super::node::{RowContext, RowNode, classify, holds_native_row, native_value, row_error, span};

/// Decode a row payload to a typed `Value`, keeping native field types.
///
/// `context` is the row shape of the payload's producer. A payload with no
/// native object at a row position decodes through [`decode_payload_value`],
/// unchanged. A native object that does not decode fails the call with a
/// serialization error.
pub fn decode_row_payload_value(payload: &[u8], context: RowContext) -> crate::Result<Value> {
    if !holds_native_row(payload, context) {
        return decode_payload_value(payload);
    }
    let (value, end) = decode_row(payload, 0, 0, context)?;
    if end != payload.len() {
        return Err(row_error(
            payload.len(),
            format!(
                "{} trailing bytes after the row payload",
                payload.len() - end
            ),
        ));
    }
    Ok(value)
}

/// Decode one row body, plain or native, to a typed object.
///
/// The body is an object context. A plain map decodes as
/// `value_from_msgpack` reads it, JSON text as [`decode_payload_value`]
/// reads it, and a native object keeps its field types. A body that does not
/// decode fails the call.
pub fn decode_row_body(body: &[u8]) -> crate::Result<Value> {
    if nodedb_query::msgpack_scan::array_header(body, 0).is_some() {
        return native_value(body);
    }
    decode_payload_value(body)
}

/// Decode the value at `pos` with each native row as a typed object.
/// Returns the value and where it ends.
fn decode_row(
    bytes: &[u8],
    pos: usize,
    depth: usize,
    context: RowContext,
) -> crate::Result<(Value, usize)> {
    match classify(bytes, pos, depth, context) {
        RowNode::Plain { end } => {
            let value = nodedb_types::value_from_msgpack(span(bytes, pos, end))
                .map_err(|e| row_error(bytes.len(), e))?;
            Ok((value, end))
        }
        RowNode::Native { end } => Ok((native_value(span(bytes, pos, end))?, end)),
        RowNode::Envelope { end } => {
            let envelope = span(bytes, pos, end);
            let view = ObjectView::new(envelope).map_err(|e| row_error(envelope.len(), e))?;
            let mut fields = HashMap::with_capacity(2);
            for entry in view.fields() {
                let (key, value) = entry.map_err(|e| row_error(envelope.len(), e))?;
                let value = if key == "data" {
                    native_value(value)?
                } else {
                    view.decode_value(value)
                        .map_err(|e| row_error(envelope.len(), e))?
                };
                fields.insert(key.to_owned(), value);
            }
            Ok((Value::Object(fields), end))
        }
        RowNode::List { count, first } => {
            // Every row takes at least one byte of what follows the header.
            let remaining = bytes.len().saturating_sub(first);
            let capacity = nodedb_types::decode_bounds::checked_decode_capacity(
                count,
                std::mem::size_of::<Value>(),
                remaining,
                1,
                count,
                usize::MAX,
            )
            .ok_or_else(|| {
                row_error(
                    bytes.len(),
                    format!("a row array declares {count} rows in {remaining} bytes"),
                )
            })?;
            let mut rows = Vec::with_capacity(capacity);
            let mut at = first;
            for _ in 0..count {
                if at >= bytes.len() {
                    return Err(row_error(
                        bytes.len(),
                        format!("a row array of {count} rows ends after {} rows", rows.len()),
                    ));
                }
                let (row, end) = decode_row(bytes, at, depth + 1, context)?;
                rows.push(row);
                at = end;
            }
            Ok((Value::Array(rows), at))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::node::fixtures::{
        envelope, id_data_row, malformed_native, native_body, object, plain_body, typed_row,
    };
    use super::*;
    use crate::data::executor::response_codec::encode_binary_rows;

    #[test]
    fn a_native_envelope_decodes_to_its_typed_value() {
        let decoded =
            decode_row_payload_value(&envelope(native_body()), RowContext::ScanEnvelope).unwrap();
        let expected = Value::Array(vec![object(vec![
            ("id", Value::String("0000000a".into())),
            ("data", typed_row()),
        ])]);
        assert_eq!(decoded, expected);
    }

    #[test]
    fn a_native_flat_row_decodes_to_its_typed_value() {
        let payload = encode_binary_rows(&[native_body()]);
        assert_eq!(
            decode_row_payload_value(&payload, RowContext::Flat).unwrap(),
            Value::Array(vec![typed_row()])
        );
        assert_eq!(decode_row_body(&native_body()).unwrap(), typed_row());
    }

    /// A flat row with columns `id` and `data` is never read as an envelope,
    /// even when `data` is an array shaped like a native object, and even
    /// when that array is a valid native object.
    #[test]
    fn a_flat_id_data_row_decodes_unchanged() {
        let payload = id_data_row();
        assert_eq!(
            decode_row_payload_value(&payload, RowContext::Flat).unwrap(),
            decode_payload_value(&payload).unwrap()
        );
        let valid_native = object(vec![
            ("id", Value::String("x".into())),
            (
                "data",
                Value::Array(vec![Value::Integer(7), object(vec![])]),
            ),
        ]);
        let payload = nodedb_types::value_to_msgpack(&Value::Array(vec![valid_native])).unwrap();
        assert_eq!(
            decode_row_payload_value(&payload, RowContext::Flat).unwrap(),
            decode_payload_value(&payload).unwrap()
        );
    }

    #[test]
    fn a_plain_payload_decodes_unchanged() {
        for context in [RowContext::Flat, RowContext::ScanEnvelope] {
            for plain in [envelope(plain_body()), encode_binary_rows(&[plain_body()])] {
                assert_eq!(
                    decode_row_payload_value(&plain, context).unwrap(),
                    decode_payload_value(&plain).unwrap()
                );
            }
        }
    }

    #[test]
    fn a_malformed_native_row_is_an_error() {
        for (payload, context) in [
            (envelope(malformed_native()), RowContext::ScanEnvelope),
            (encode_binary_rows(&[malformed_native()]), RowContext::Flat),
            (malformed_native(), RowContext::Flat),
        ] {
            assert!(matches!(
                decode_row_payload_value(&payload, context),
                Err(crate::Error::Serialization { .. })
            ));
        }
        assert!(decode_row_body(&malformed_native()).is_err());
    }
}
