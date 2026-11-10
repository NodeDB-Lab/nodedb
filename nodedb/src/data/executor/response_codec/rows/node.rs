// SPDX-License-Identifier: BUSL-1.1

//! Row positions of a Data-Plane row payload, under a caller-stated
//! [`RowContext`].
//!
//! A row payload is one row, or an array of rows. Row arrays can nest. The
//! producer of the payload fixes its row shape, and the caller states it:
//!
//! - [`RowContext::Flat`]: each row is a body, a plain map or a native object
//!   `[7, {field: <tagged value>}]`.
//! - [`RowContext::ScanEnvelope`]: each row is a `{id, data}` scan envelope,
//!   and its `data` value is a body.
//!
//! These row positions are the only object contexts. A field value is never
//! reinterpreted: a plain array field stays an array, and in a flat row a
//! column named `data` is an ordinary column.

use nodedb_query::msgpack_scan::{array_header, map_header, read_i64, read_str, skip_value};
use nodedb_types::Value;

use super::super::encode::looks_like_json;

/// The row shape a payload's producer emits. Derived from the plan that
/// produced the payload, never from the row bytes or column names.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RowContext {
    /// Each row is a body: a plain map or a native object.
    Flat,
    /// Each row is a `{id, data}` scan envelope whose `data` is a body.
    ScanEnvelope,
}

/// The tag of a native object: the `Value::Object` variant tag.
const NATIVE_OBJECT_TAG: i64 = 7;

/// Row arrays nest a few levels at most. Deeper bytes are read as plain.
const MAX_ROW_DEPTH: usize = 32;

/// One value at a row position.
pub(super) enum RowNode {
    /// Bytes with no native object at a row position, up to `end`.
    Plain { end: usize },
    /// A native object, up to `end`.
    Native { end: usize },
    /// A scan envelope whose `data` value is a native object, up to `end`.
    Envelope { end: usize },
    /// An array of rows: `count` elements from `first`.
    List { count: usize, first: usize },
}

/// Classify the value at `pos`. Bytes that do not parse are plain to the end
/// of the buffer, so the downstream decoder reports them as it always has.
pub(super) fn classify(bytes: &[u8], pos: usize, depth: usize, context: RowContext) -> RowNode {
    let Some(end) = skip_value(bytes, pos) else {
        return RowNode::Plain { end: bytes.len() };
    };
    if depth >= MAX_ROW_DEPTH {
        return RowNode::Plain { end };
    }
    if is_native_object(bytes, pos) {
        return RowNode::Native { end };
    }
    if let Some((count, first)) = array_header(bytes, pos) {
        return RowNode::List { count, first };
    }
    match context {
        RowContext::Flat => RowNode::Plain { end },
        RowContext::ScanEnvelope => match envelope_data(bytes, pos) {
            Some(data) if is_native_object(bytes, data.0) => RowNode::Envelope { end },
            Some(_) | None => RowNode::Plain { end },
        },
    }
}

/// Whether any top-level value of `payload` holds a native object at a row
/// position. JSON text holds none.
pub(super) fn holds_native_row(payload: &[u8], context: RowContext) -> bool {
    if payload.first().copied().is_some_and(looks_like_json) {
        return false;
    }
    let mut at = 0;
    while at < payload.len() {
        let (found, end) = scan(payload, at, 0, context);
        if found {
            return true;
        }
        if end <= at {
            return false;
        }
        at = end;
    }
    false
}

/// Whether the value at `pos` holds a native object at a row position, and
/// where the scan stopped.
fn scan(bytes: &[u8], pos: usize, depth: usize, context: RowContext) -> (bool, usize) {
    match classify(bytes, pos, depth, context) {
        RowNode::Plain { end } => (false, end),
        RowNode::Native { end } | RowNode::Envelope { end } => (true, end),
        RowNode::List { count, first } => {
            let mut at = first;
            for _ in 0..count {
                if at >= bytes.len() {
                    break;
                }
                let (found, end) = scan(bytes, at, depth + 1, context);
                if found {
                    return (true, end);
                }
                at = end;
            }
            (false, at)
        }
    }
}

/// Whether `bytes` is exactly one native object at a row position: a
/// two-element array of the integer 7 and a map, with nothing after it. A
/// row array never holds a scalar, so no row array has this shape.
pub fn is_native_row(bytes: &[u8]) -> bool {
    is_native_object(bytes, 0) && skip_value(bytes, 0) == Some(bytes.len())
}

/// Whether the value at `pos` is a two-element array of the integer 7 and a
/// map: a native object. Only a row position reads it so.
fn is_native_object(bytes: &[u8], pos: usize) -> bool {
    let Some((2, tag)) = array_header(bytes, pos) else {
        return false;
    };
    read_i64(bytes, tag) == Some(NATIVE_OBJECT_TAG)
        && skip_value(bytes, tag).is_some_and(|map| map_header(bytes, map).is_some())
}

/// The span of the `data` value of the scan envelope at `pos`.
fn envelope_data(bytes: &[u8], pos: usize) -> Option<(usize, usize)> {
    let (count, mut key) = map_header(bytes, pos)?;
    for _ in 0..count {
        let value = skip_value(bytes, key)?;
        let end = skip_value(bytes, value)?;
        if read_str(bytes, key)? == "data" {
            return Some((value, end));
        }
        key = end;
    }
    None
}

/// Decode one native object, keeping its field types.
pub(super) fn native_value(bytes: &[u8]) -> crate::Result<Value> {
    nodedb_types::decode_object(bytes).map_err(|e| row_error(bytes.len(), e))
}

/// The bytes from `from` to `to`. Every caller passes offsets `skip_value`
/// returned, which lie inside `bytes`.
pub(super) fn span(bytes: &[u8], from: usize, to: usize) -> &[u8] {
    bytes.get(from..to).unwrap_or_default()
}

/// The error for row bytes of `len` bytes that do not decode.
pub(super) fn row_error(len: usize, detail: impl std::fmt::Display) -> crate::Error {
    crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!(
            "row bytes ({len} bytes) do not decode: {detail}. \
             A stored row is corrupt; rewrite or restore it"
        ),
    }
}

#[cfg(test)]
pub(super) mod fixtures {
    //! Rows shared by the decoder tests.

    use nodedb_types::{NdbDateTime, Value};

    use crate::data::executor::response_codec::encode_raw_document_rows;

    pub(crate) fn object(pairs: Vec<(&str, Value)>) -> Value {
        Value::Object(pairs.into_iter().map(|(k, v)| (k.to_owned(), v)).collect())
    }

    /// A row with native-only types, a nested object, and a plain array
    /// field shaped like a native object.
    pub(crate) fn typed_row() -> Value {
        object(vec![
            ("price", Value::Decimal("123.45".parse().unwrap())),
            (
                "uid",
                Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into()),
            ),
            (
                "seen",
                Value::DateTime(NdbDateTime::from_micros(1_583_402_400_000_000)),
            ),
            (
                "address",
                object(vec![("city", Value::String("Oslo".into()))]),
            ),
            (
                "tags",
                Value::Array(vec![
                    Value::Integer(7),
                    object(vec![("k", Value::Integer(1))]),
                ]),
            ),
        ])
    }

    pub(crate) fn native_body() -> Vec<u8> {
        zerompk::to_msgpack_vec(&typed_row()).unwrap()
    }

    pub(crate) fn plain_body() -> Vec<u8> {
        nodedb_types::value_to_msgpack(&typed_row()).unwrap()
    }

    pub(crate) fn envelope(body: Vec<u8>) -> Vec<u8> {
        encode_raw_document_rows(&[("0000000a".to_owned(), body)]).unwrap()
    }

    /// `[7, {"a": [99, 1]}]`: a native object whose field has no valid tag.
    pub(crate) fn malformed_native() -> Vec<u8> {
        vec![0x92, 0x07, 0x81, 0xa1, b'a', 0x92, 0x63, 0x01]
    }

    /// A flat user row with the columns `id` and `data`, whose `data` is a
    /// plain array shaped like a native object.
    pub(crate) fn id_data_row() -> Vec<u8> {
        nodedb_types::value_to_msgpack(&Value::Array(vec![object(vec![
            ("id", Value::String("x".into())),
            (
                "data",
                Value::Array(vec![
                    Value::Integer(7),
                    object(vec![("k", Value::Integer(1))]),
                ]),
            ),
        ])]))
        .unwrap()
    }
}
