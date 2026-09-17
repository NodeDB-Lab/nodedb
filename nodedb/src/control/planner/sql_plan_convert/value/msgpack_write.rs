// SPDX-License-Identifier: BUSL-1.1

//! Standard-msgpack writers for `SqlValue`.
//!
//! These are the *only* msgpack producers used by the DML path — no JSON or
//! zerompk intermediary. Format matches the on-wire layout read by
//! `json_from_msgpack` and the Data Plane row decoders.
//!
//! A typed instant (`SqlValue::Timestamp` / `Timestamptz`) has two
//! spellings, chosen per payload by [`InstantForm`]: ISO 8601 text for the
//! document, KV, and CRDT row decoders, and the `fixext8` instant ext for
//! the timeseries ingest payload, whose decoder takes the typed form.

use nodedb_sql::types::SqlValue;
use nodedb_types::{InstantKind, write_instant};

/// How a typed instant is written into a row payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InstantForm {
    /// ISO 8601 text, decoded by the Data Plane's text-coercion path.
    Iso8601,
    /// The ten-byte `fixext8` instant ext, decoded as a typed instant.
    Ext,
}

/// A row map with instants as ISO 8601 text.
pub(crate) fn row_to_msgpack(row: &[(String, SqlValue)]) -> crate::Result<Vec<u8>> {
    row_to_msgpack_with(row, InstantForm::Iso8601)
}

/// A row map with instants in the given form.
pub(crate) fn row_to_msgpack_with(
    row: &[(String, SqlValue)],
    instants: InstantForm,
) -> crate::Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(row.len() * 32);
    write_msgpack_map_header(&mut buf, row.len());
    for (key, val) in row {
        write_msgpack_str(&mut buf, key);
        write_msgpack_value_with(&mut buf, val, instants);
    }
    Ok(buf)
}

pub(crate) fn write_msgpack_map_header(buf: &mut Vec<u8>, len: usize) {
    if len < 16 {
        buf.push(0x80 | len as u8);
    } else if len <= u16::MAX as usize {
        buf.push(0xDE);
        buf.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        buf.push(0xDF);
        buf.extend_from_slice(&(len as u32).to_be_bytes());
    }
}

pub(crate) fn write_msgpack_array_header(buf: &mut Vec<u8>, len: usize) {
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

pub(crate) fn write_msgpack_str(buf: &mut Vec<u8>, s: &str) {
    let bytes = s.as_bytes();
    let len = bytes.len();
    if len < 32 {
        buf.push(0xA0 | len as u8);
    } else if len <= u8::MAX as usize {
        buf.push(0xD9);
        buf.push(len as u8);
    } else if len <= u16::MAX as usize {
        buf.push(0xDA);
        buf.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        buf.push(0xDB);
        buf.extend_from_slice(&(len as u32).to_be_bytes());
    }
    buf.extend_from_slice(bytes);
}

/// Write one value with instants as ISO 8601 text.
pub(crate) fn write_msgpack_value(buf: &mut Vec<u8>, val: &SqlValue) {
    write_msgpack_value_with(buf, val, InstantForm::Iso8601)
}

/// Write one value with instants in the given form.
pub(crate) fn write_msgpack_value_with(buf: &mut Vec<u8>, val: &SqlValue, instants: InstantForm) {
    match val {
        SqlValue::Null => buf.push(0xC0),
        SqlValue::Bool(true) => buf.push(0xC3),
        SqlValue::Bool(false) => buf.push(0xC2),
        SqlValue::Int(i) => write_msgpack_int(buf, *i),
        SqlValue::Float(f) => {
            buf.push(0xCB);
            buf.extend_from_slice(&f.to_be_bytes());
        }
        SqlValue::Decimal(d) => {
            // Write as msgpack string of the decimal's canonical text representation.
            // The Data Plane strict encoder accepts Value::String for Decimal columns
            // and coerces via rust_decimal::Decimal::from_str. This matches the
            // contract of write_msgpack_value: it produces standard msgpack that
            // json_from_msgpack / value_from_msgpack can decode without loss.
            write_msgpack_str(buf, &d.to_string());
        }
        SqlValue::String(s) => write_msgpack_str(buf, s),
        SqlValue::Array(arr) => {
            write_msgpack_array_header(buf, arr.len());
            for item in arr {
                write_msgpack_value_with(buf, item, instants);
            }
        }
        SqlValue::Bytes(b) => write_msgpack_bin(buf, b),
        SqlValue::Timestamp(dt) => match instants {
            InstantForm::Iso8601 => write_msgpack_str(buf, &dt.to_iso8601()),
            InstantForm::Ext => write_instant(buf, InstantKind::Naive, dt.micros),
        },
        SqlValue::Timestamptz(dt) => match instants {
            InstantForm::Iso8601 => write_msgpack_str(buf, &dt.to_iso8601()),
            InstantForm::Ext => write_instant(buf, InstantKind::Utc, dt.micros),
        },
    }
}

fn write_msgpack_int(buf: &mut Vec<u8>, i: i64) {
    if (0..=127).contains(&i) {
        buf.push(i as u8);
    } else if (-32..0).contains(&i) {
        buf.push(i as u8); // negative fixint
    } else if i >= i8::MIN as i64 && i <= i8::MAX as i64 {
        buf.push(0xD0);
        buf.push(i as i8 as u8);
    } else if i >= i16::MIN as i64 && i <= i16::MAX as i64 {
        buf.push(0xD1);
        buf.extend_from_slice(&(i as i16).to_be_bytes());
    } else if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
        buf.push(0xD2);
        buf.extend_from_slice(&(i as i32).to_be_bytes());
    } else {
        buf.push(0xD3);
        buf.extend_from_slice(&i.to_be_bytes());
    }
}

fn write_msgpack_bin(buf: &mut Vec<u8>, b: &[u8]) {
    let len = b.len();
    if len <= u8::MAX as usize {
        buf.push(0xC4);
        buf.push(len as u8);
    } else if len <= u16::MAX as usize {
        buf.push(0xC5);
        buf.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        buf.push(0xC6);
        buf.extend_from_slice(&(len as u32).to_be_bytes());
    }
    buf.extend_from_slice(b);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_to_msgpack_produces_standard_format() {
        let row = vec![
            ("count".to_string(), SqlValue::Int(1)),
            ("label".to_string(), SqlValue::String("homepage".into())),
        ];
        let bytes = row_to_msgpack(&row).unwrap();
        assert_eq!(
            bytes[0], 0x82,
            "expected fixmap(2), got 0x{:02X}. bytes={bytes:?}",
            bytes[0]
        );
        let json = nodedb_types::json_from_msgpack(&bytes).unwrap();
        let obj = json.as_object().unwrap();
        assert_eq!(obj["count"], 1);
        assert_eq!(obj["label"], "homepage");
    }

    #[test]
    fn write_msgpack_value_int() {
        let mut buf = Vec::new();
        write_msgpack_value(&mut buf, &SqlValue::Int(42));
        assert_eq!(buf, vec![42]);
    }

    #[test]
    fn write_msgpack_value_string() {
        let mut buf = Vec::new();
        write_msgpack_value(&mut buf, &SqlValue::String("hi".into()));
        assert_eq!(buf, vec![0xA2, b'h', b'i']);
    }

    /// `2020-03-05T10:00:00Z` in epoch microseconds.
    const EARLY_MICROS: i64 = 1_583_402_400_000_000;

    /// The default form spells an instant as ISO 8601 text.
    #[test]
    fn an_instant_is_iso8601_text_by_default() {
        let dt = nodedb_types::NdbDateTime::from_micros(EARLY_MICROS);
        for val in [SqlValue::Timestamp(dt), SqlValue::Timestamptz(dt)] {
            let mut buf = Vec::new();
            write_msgpack_value(&mut buf, &val);
            let mut expected = Vec::new();
            write_msgpack_str(&mut expected, "2020-03-05T10:00:00.000000Z");
            assert_eq!(buf, expected);
        }
    }

    /// The ext form writes the `fixext8` instant, tagged by the SQL type.
    #[test]
    fn an_instant_in_ext_form_is_a_typed_fixext8() {
        let dt = nodedb_types::NdbDateTime::from_micros(EARLY_MICROS);
        let cases = [
            (SqlValue::Timestamp(dt), InstantKind::Naive),
            (SqlValue::Timestamptz(dt), InstantKind::Utc),
        ];
        for (val, kind) in cases {
            let mut buf = Vec::new();
            write_msgpack_value_with(&mut buf, &val, InstantForm::Ext);
            assert_eq!(
                nodedb_types::read_instant(&buf, 0),
                Some((kind, EARLY_MICROS)),
                "{val:?}"
            );
        }
    }
}
