// SPDX-License-Identifier: BUSL-1.1

//! The most bytes one `RETURNING` row encodes to, from the list alone.
//!
//! The payload is a `RowsPayload`: a two-key map of the column names and
//! one array of cells per row. For one row of `n` items it is at most
//!
//! ```text
//! envelope(n) + Σ str(name_i) + Σ literal_i + columns × cell
//! ```
//!
//! - `envelope(n)`: the map header, the `columns` and `rows` keys, and the
//!   three array headers.
//! - `str(name_i)`: a column name and its string header.
//! - `literal_i`: a literal item's exact cell, the largest under any
//!   announced type.
//! - `cell`: the most bytes a column cell renders to. A column cell holds a
//!   stored field, an added column's default, a mask or a hash. Its source
//!   is at most `MAX_ROW_IMAGE_BYTES` bytes, so it renders within
//!   [`native_cell_bound`] of that.

use nodedb_types::{
    MAX_ADDED_COLUMN_IMAGE_BYTES, MAX_ROW_IMAGE_BYTES, NativeCell, Value, native_cell_bound,
};

use crate::control::security::redaction::{HASH_REDACTION_TEXT_BYTES, MAX_REDACTION_MASK_BYTES};
use crate::control::server::response_shape::returning::retype_cell;
use crate::control::server::response_shape::types::DdlColType;

/// The bytes of a MessagePack string, array or map header for `len`
/// entries or bytes, by the widths `fix`, 8-bit and wider forms take.
fn header_bytes(len: usize, fix_max: usize, has_8bit: bool) -> usize {
    if len <= fix_max {
        1
    } else if has_8bit && len <= usize::from(u8::MAX) {
        2
    } else if len <= usize::from(u16::MAX) {
        3
    } else {
        5
    }
}

/// The encoded bytes of a string of `len` bytes.
pub(super) fn string_bytes(len: usize) -> usize {
    len.saturating_add(header_bytes(len, 31, true))
}

/// The encoded bytes of an array header for `len` items.
fn array_header_bytes(len: usize) -> usize {
    header_bytes(len, 15, false)
}

/// The bytes of a one-row `RowsPayload` of `columns` cells beyond its names
/// and cells: the two-key map header, the `columns` and `rows` keys, the
/// names array header, the one-row array header and the cell array header.
pub(super) fn envelope_bytes(columns: usize) -> usize {
    let names_array = array_header_bytes(columns);
    1 + string_bytes("columns".len())
        + names_array
        + string_bytes("rows".len())
        + array_header_bytes(1)
        + names_array
}

/// The most bytes a column cell renders to: a stored field, a default or a
/// mask, retyped to its announced type, or a hash.
pub(super) fn column_cell_bytes() -> usize {
    let source = MAX_ROW_IMAGE_BYTES
        .max(MAX_ADDED_COLUMN_IMAGE_BYTES)
        .max(MAX_REDACTION_MASK_BYTES);
    native_cell_bound(source).max(string_bytes(HASH_REDACTION_TEXT_BYTES))
}

/// Every type a cell can be announced as. A literal cell is retyped to its
/// announced type, which the plan does not carry.
const ANNOUNCED_TYPES: [DdlColType; 17] = [
    DdlColType::Text,
    DdlColType::Int8,
    DdlColType::Int4,
    DdlColType::Int2,
    DdlColType::Float8,
    DdlColType::Float4,
    DdlColType::Bool,
    DdlColType::Bytea,
    DdlColType::Json,
    DdlColType::Jsonb,
    DdlColType::Timestamp,
    DdlColType::Timestamptz,
    DdlColType::Varchar,
    DdlColType::Float4Array,
    DdlColType::Float8Array,
    DdlColType::Numeric,
    DdlColType::Uuid,
];

/// The exact bytes a literal's cell renders to, the largest under any
/// announced type. `None` when the literal does not encode.
pub(super) fn literal_cell_bytes(literal: &Value) -> Option<usize> {
    ANNOUNCED_TYPES.iter().try_fold(0usize, |largest, ty| {
        let mut cell = literal.clone();
        retype_cell(*ty, &mut cell);
        let bytes = zerompk::to_msgpack_vec(&NativeCell(cell)).ok()?;
        Some(largest.max(bytes.len()))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::executor::response_codec::RowsPayload;

    /// Every variant is in [`ANNOUNCED_TYPES`]. A new variant fails to
    /// compile here until it is listed.
    #[test]
    fn every_announced_type_is_listed() {
        let index = |ty: DdlColType| match ty {
            DdlColType::Text => 0,
            DdlColType::Int8 => 1,
            DdlColType::Int4 => 2,
            DdlColType::Int2 => 3,
            DdlColType::Float8 => 4,
            DdlColType::Float4 => 5,
            DdlColType::Bool => 6,
            DdlColType::Bytea => 7,
            DdlColType::Json => 8,
            DdlColType::Jsonb => 9,
            DdlColType::Timestamp => 10,
            DdlColType::Timestamptz => 11,
            DdlColType::Varchar => 12,
            DdlColType::Float4Array => 13,
            DdlColType::Float8Array => 14,
            DdlColType::Numeric => 15,
            DdlColType::Uuid => 16,
        };
        for (position, ty) in ANNOUNCED_TYPES.iter().enumerate() {
            assert_eq!(index(*ty), position, "{ty:?}");
        }
    }

    /// The envelope and name bytes match the real encoder exactly.
    #[test]
    fn the_envelope_matches_the_rows_payload_encoder() {
        let long = "n".repeat(40);
        for columns in [0usize, 1, 15, 16, 255, 256, 65_535, 65_536] {
            for name in ["c", long.as_str()] {
                let payload = RowsPayload {
                    columns: vec![name.to_string(); columns],
                    rows: vec![vec![NativeCell(Value::Null); columns]],
                };
                let bytes = zerompk::to_msgpack_vec(&payload).expect("encode");
                assert_eq!(
                    bytes.len(),
                    envelope_bytes(columns) + columns * (string_bytes(name.len()) + 1),
                    "{columns} columns named {} bytes",
                    name.len()
                );
            }
        }
    }

    /// Text retyped to any announced type renders within `G·len + C` of
    /// its bytes, the smallest form stored text takes. A mask is such text.
    #[test]
    fn retyped_text_renders_within_the_growth_bound() {
        let floats = |count: usize| format!("[{}]", vec!["1"; count].join(","));
        let mut texts = vec![
            String::new(),
            "1".into(),
            "-1".into(),
            "1.5".into(),
            "t".into(),
            "[]".into(),
            "2024-03-15".into(),
            "2024-03-15T10:30:00.123456+08:00".into(),
            "550e8400-e29b-41d4-a716-446655440000".into(),
        ];
        texts.extend([1, 2, 15, 16, 65_535, 65_536].map(floats));
        for text in texts {
            for ty in ANNOUNCED_TYPES {
                let mut cell = Value::String(text.clone());
                retype_cell(ty, &mut cell);
                let rendered = zerompk::to_msgpack_vec(&NativeCell(cell)).expect("render");
                assert!(
                    rendered.len() <= native_cell_bound(text.len()),
                    "{ty:?}: {} text bytes render {}",
                    text.len(),
                    rendered.len()
                );
            }
        }
    }

    #[test]
    fn a_literal_cell_is_its_largest_retyped_form() {
        assert_eq!(literal_cell_bytes(&Value::Integer(1)), Some(1));
        assert_eq!(literal_cell_bytes(&Value::Null), Some(1));
        // `[1,1]` is 6 bytes as text and a two-float array under `FLOAT8[]`.
        assert_eq!(
            literal_cell_bytes(&Value::String("[1,1]".into())),
            Some(1 + 2 * 9)
        );
    }

    #[test]
    fn a_column_cell_holds_any_stored_field_mask_or_hash() {
        let cell = column_cell_bytes();
        assert_eq!(cell, native_cell_bound(MAX_ROW_IMAGE_BYTES));
        assert!(cell >= string_bytes(HASH_REDACTION_TEXT_BYTES));
        assert!(cell >= native_cell_bound(MAX_REDACTION_MASK_BYTES));
    }

    /// A stored plain body that holds a map key that is not a string.
    fn key_bomb(depth: usize) -> Vec<u8> {
        let mut key = vec![0x81, 0xc0, 0xc0];
        for _ in 0..depth {
            let mut outer = vec![0x81];
            outer.extend_from_slice(&key);
            outer.push(0xc0);
            key = outer;
        }
        let mut body = vec![0x81, 0xa1, b'k'];
        body.extend_from_slice(&key);
        body
    }

    /// Every body the write paths accept renders one row within the bound
    /// `fits_one_row` proves. A body with a map key that is not a string is
    /// refused before it is stored.
    #[test]
    fn every_accepted_body_renders_within_the_proved_bound() {
        let array_of = |item: &[u8], count: usize| {
            let mut bytes = vec![0xdc];
            bytes.extend_from_slice(&u16::try_from(count).expect("count").to_be_bytes());
            (0..count).for_each(|_| bytes.extend_from_slice(item));
            bytes
        };
        let field = |name: &[u8], value: Vec<u8>| {
            let mut body = vec![0x81, 0xa0 | u8::try_from(name.len()).expect("name")];
            body.extend_from_slice(name);
            body.extend(value);
            body
        };
        let accepted = [
            field(b"f", array_of(&[0xca, 0x3f, 0x80, 0, 0], 4_000)),
            field(b"u", array_of(&[0xcc, 200], 4_000)),
            field(b"s", vec![0xa0]),
            field(b"m", field(b"n", array_of(&[0xc0], 100))),
        ];
        for body in &accepted {
            assert_eq!(nodedb_types::find_body_non_string_key(body), None);
            let Value::Object(fields) = nodedb_types::value_from_msgpack(body).expect("read")
            else {
                panic!("an object");
            };
            let (columns, cells): (Vec<String>, Vec<NativeCell>) = fields
                .into_iter()
                .map(|(name, value)| (name, NativeCell(value)))
                .unzip();
            let bound = envelope_bytes(columns.len())
                + columns
                    .iter()
                    .map(|name| string_bytes(name.len()))
                    .sum::<usize>()
                + columns.len() * native_cell_bound(body.len());
            let payload = RowsPayload {
                columns,
                rows: vec![cells],
            };
            let bytes = zerompk::to_msgpack_vec(&payload).expect("encode");
            assert!(bytes.len() <= bound, "{} bytes over {bound}", bytes.len());
        }
        let refused = [
            key_bomb(0),
            key_bomb(400),
            field(b"a", array_of(&[0x81, 0x01, 0xc0], 100)),
            field(b"k", vec![0x81, 0x91, 0x01, 0xc0]),
        ];
        for body in &refused {
            assert!(nodedb_types::find_body_non_string_key(body).is_some());
        }
    }
}
