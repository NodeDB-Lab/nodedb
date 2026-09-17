// SPDX-License-Identifier: BUSL-1.1

//! Lightweight msgpack decoder for timeseries ingest rows.
//!
//! Decodes a msgpack array of maps into `Vec<Vec<(String, MsgpackValue)>>`.
//! Supports the value types the planner's row writer and the native `Value`
//! encoder produce for a timeseries row: scalars, strings, and the `fixext8`
//! instant ext (`nodedb_types::read_instant`). A `bin` value decodes as
//! `Null`. Any other ext, array, or map value is an error.

use nodedb_types::json_msgpack::INSTANT_EXT_LEN;
use nodedb_types::read_instant;

pub(super) enum MsgpackValue {
    Int(i64),
    Float(f64),
    Str(String),
    Bool(bool),
    /// A typed instant, as epoch microseconds. The column's declared kind,
    /// not the cell's tag, decides how the engine stores and reads it.
    Instant(i64),
    Null,
}

/// Decode a msgpack array of maps into `Vec<Vec<(String, MsgpackValue)>>`.
pub(super) fn decode_msgpack_rows(
    payload: &[u8],
) -> Result<Vec<Vec<(String, MsgpackValue)>>, &'static str> {
    let mut pos = 0;
    let array_len = read_array_header(payload, &mut pos)?;
    // Array/map cardinalities are peer-controlled msgpack values. Grow after
    // each complete row has been decoded from the bounded payload.
    let mut rows = Vec::new();
    for _ in 0..array_len {
        let map_len = read_map_header(payload, &mut pos)?;
        let mut fields = Vec::new();
        for _ in 0..map_len {
            let key = read_str(payload, &mut pos)?;
            let val = read_value(payload, &mut pos)?;
            fields.push((key, val));
        }
        rows.push(fields);
    }
    Ok(rows)
}

fn read_array_header(buf: &[u8], pos: &mut usize) -> Result<usize, &'static str> {
    let b = *buf.get(*pos).ok_or("unexpected EOF in array header")?;
    *pos += 1;
    if b & 0xF0 == 0x90 {
        Ok((b & 0x0F) as usize)
    } else if b == 0xDC {
        let len = read_be_u16(buf, pos)? as usize;
        Ok(len)
    } else if b == 0xDD {
        let len = read_be_u32(buf, pos)?;
        usize::try_from(len).map_err(|_| "msgpack array length exceeds usize")
    } else {
        Err("expected msgpack array")
    }
}

fn read_map_header(buf: &[u8], pos: &mut usize) -> Result<usize, &'static str> {
    let b = *buf.get(*pos).ok_or("unexpected EOF in map header")?;
    *pos += 1;
    if b & 0xF0 == 0x80 {
        Ok((b & 0x0F) as usize)
    } else if b == 0xDE {
        let len = read_be_u16(buf, pos)? as usize;
        Ok(len)
    } else if b == 0xDF {
        let len = read_be_u32(buf, pos)?;
        usize::try_from(len).map_err(|_| "msgpack map length exceeds usize")
    } else {
        Err("expected msgpack map")
    }
}

fn read_str(buf: &[u8], pos: &mut usize) -> Result<String, &'static str> {
    let b = *buf.get(*pos).ok_or("unexpected EOF in str header")?;
    *pos += 1;
    let len = if b & 0xE0 == 0xA0 {
        (b & 0x1F) as usize
    } else if b == 0xD9 {
        let l = *buf.get(*pos).ok_or("unexpected EOF in str8 len")?;
        *pos += 1;
        l as usize
    } else if b == 0xDA {
        read_be_u16(buf, pos)? as usize
    } else if b == 0xDB {
        read_be_u32(buf, pos)? as usize
    } else {
        return Err("expected msgpack str");
    };
    let end = *pos + len;
    if end > buf.len() {
        return Err("unexpected EOF in str data");
    }
    let s = std::str::from_utf8(&buf[*pos..end]).map_err(|_| "invalid UTF-8 in msgpack str")?;
    *pos = end;
    Ok(s.to_owned())
}

fn read_value(buf: &[u8], pos: &mut usize) -> Result<MsgpackValue, &'static str> {
    let b = *buf.get(*pos).ok_or("unexpected EOF in value")?;
    *pos += 1;
    match b {
        0xC0 => Ok(MsgpackValue::Null),
        0xC2 => Ok(MsgpackValue::Bool(false)),
        0xC3 => Ok(MsgpackValue::Bool(true)),
        // positive fixint
        0x00..=0x7F => Ok(MsgpackValue::Int(b as i64)),
        // negative fixint
        0xE0..=0xFF => Ok(MsgpackValue::Int(b as i8 as i64)),
        // int8
        0xD0 => {
            let v = *buf.get(*pos).ok_or("unexpected EOF in int8")? as i8;
            *pos += 1;
            Ok(MsgpackValue::Int(v as i64))
        }
        // int16
        0xD1 => {
            let v = read_be_i16(buf, pos)?;
            Ok(MsgpackValue::Int(v as i64))
        }
        // int32
        0xD2 => {
            let v = read_be_i32(buf, pos)?;
            Ok(MsgpackValue::Int(v as i64))
        }
        // int64
        0xD3 => {
            let v = read_be_i64(buf, pos)?;
            Ok(MsgpackValue::Int(v))
        }
        // float64
        0xCB => {
            let v = read_be_f64(buf, pos)?;
            Ok(MsgpackValue::Float(v))
        }
        // float32
        0xCA => {
            let bits = read_be_u32(buf, pos)?;
            Ok(MsgpackValue::Float(f32::from_bits(bits) as f64))
        }
        // fixstr
        b if b & 0xE0 == 0xA0 => {
            *pos -= 1;
            read_str(buf, pos).map(MsgpackValue::Str)
        }
        // str8, str16, str32
        0xD9..=0xDB => {
            *pos -= 1;
            read_str(buf, pos).map(MsgpackValue::Str)
        }
        // uint8
        0xCC => {
            let v = *buf.get(*pos).ok_or("unexpected EOF in uint8")?;
            *pos += 1;
            Ok(MsgpackValue::Int(v as i64))
        }
        // uint16
        0xCD => {
            let v = read_be_u16(buf, pos)?;
            Ok(MsgpackValue::Int(v as i64))
        }
        // uint32
        0xCE => {
            let v = read_be_u32(buf, pos)?;
            Ok(MsgpackValue::Int(v as i64))
        }
        // uint64
        0xCF => {
            let bytes = read_bytes::<8>(buf, pos)?;
            Ok(MsgpackValue::Int(u64::from_be_bytes(bytes) as i64))
        }
        // fixext8: an instant of either kind. Any other ext type is
        // unsupported, like every other ext marker.
        0xD7 => {
            let start = *pos - 1;
            if buf.len() < start + INSTANT_EXT_LEN {
                return Err("unexpected EOF in fixext8");
            }
            let (_, micros) =
                read_instant(buf, start).ok_or("unsupported msgpack ext type in timeseries row")?;
            *pos = start + INSTANT_EXT_LEN;
            Ok(MsgpackValue::Instant(micros))
        }
        // Skip bin values; ext/array/map values are unsupported.
        _ => {
            skip_msgpack_value(b, buf, pos)?;
            Ok(MsgpackValue::Null)
        }
    }
}

fn skip_msgpack_value(tag: u8, buf: &[u8], pos: &mut usize) -> Result<(), &'static str> {
    match tag {
        // bin8
        0xC4 => {
            let len = *buf.get(*pos).ok_or("EOF")? as usize;
            *pos += 1 + len;
        }
        // bin16
        0xC5 => {
            let len = read_be_u16(buf, pos)? as usize;
            *pos += len;
        }
        // bin32
        0xC6 => {
            let len = read_be_u32(buf, pos)? as usize;
            *pos += len;
        }
        _ => return Err("unsupported msgpack type in timeseries row"),
    }
    Ok(())
}

fn read_bytes<const N: usize>(buf: &[u8], pos: &mut usize) -> Result<[u8; N], &'static str> {
    let end = *pos + N;
    if end > buf.len() {
        return Err("unexpected EOF");
    }
    let mut arr = [0u8; N];
    arr.copy_from_slice(&buf[*pos..end]);
    *pos = end;
    Ok(arr)
}

fn read_be_u16(buf: &[u8], pos: &mut usize) -> Result<u16, &'static str> {
    Ok(u16::from_be_bytes(read_bytes::<2>(buf, pos)?))
}

fn read_be_u32(buf: &[u8], pos: &mut usize) -> Result<u32, &'static str> {
    Ok(u32::from_be_bytes(read_bytes::<4>(buf, pos)?))
}

fn read_be_i16(buf: &[u8], pos: &mut usize) -> Result<i16, &'static str> {
    Ok(i16::from_be_bytes(read_bytes::<2>(buf, pos)?))
}

fn read_be_i32(buf: &[u8], pos: &mut usize) -> Result<i32, &'static str> {
    Ok(i32::from_be_bytes(read_bytes::<4>(buf, pos)?))
}

fn read_be_i64(buf: &[u8], pos: &mut usize) -> Result<i64, &'static str> {
    Ok(i64::from_be_bytes(read_bytes::<8>(buf, pos)?))
}

fn read_be_f64(buf: &[u8], pos: &mut usize) -> Result<f64, &'static str> {
    Ok(f64::from_be_bytes(read_bytes::<8>(buf, pos)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::InstantKind;

    /// `2020-03-05T10:00:00Z` in epoch microseconds.
    const EARLY_MICROS: i64 = 1_583_402_400_000_000;

    /// A row map `{"captured_at": <instant>}` for the given kind.
    fn row_with_instant(kind: InstantKind) -> Vec<u8> {
        let mut payload = vec![0x91, 0x81];
        payload.extend_from_slice(&[0xAB]);
        payload.extend_from_slice(b"captured_at");
        nodedb_types::write_instant(&mut payload, kind, EARLY_MICROS);
        payload
    }

    /// A `fixext8` instant of either kind decodes as the typed instant.
    #[test]
    fn a_fixext8_instant_decodes_as_a_typed_instant() {
        for kind in [InstantKind::Naive, InstantKind::Utc] {
            let rows = decode_msgpack_rows(&row_with_instant(kind)).expect("decode");
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].len(), 1);
            assert_eq!(rows[0][0].0, "captured_at");
            assert!(
                matches!(
                    rows[0][0].1,
                    MsgpackValue::Instant(micros) if micros == EARLY_MICROS
                ),
                "instant of kind {kind:?} must decode typed"
            );
        }
    }

    /// A `fixext8` of a non-instant type is unsupported, like every other ext.
    #[test]
    fn a_fixext8_of_another_type_is_rejected() {
        let mut payload = vec![0x91, 0x81, 0xA1, b'k'];
        payload.extend_from_slice(&[0xD7, 0x09, 0, 0, 0, 0, 0, 0, 0, 1]);
        assert!(matches!(
            decode_msgpack_rows(&payload),
            Err("unsupported msgpack ext type in timeseries row")
        ));
    }

    /// A truncated instant is an EOF error, never a partial value.
    #[test]
    fn a_truncated_instant_is_rejected() {
        let mut payload = row_with_instant(InstantKind::Utc);
        payload.truncate(payload.len() - 1);
        assert!(matches!(
            decode_msgpack_rows(&payload),
            Err("unexpected EOF in fixext8")
        ));
    }

    #[test]
    fn huge_array_count_with_tiny_payload_is_rejected_without_reservation() {
        assert!(matches!(
            decode_msgpack_rows(&[0xDD, 0xFF, 0xFF, 0xFF, 0xFF]),
            Err("unexpected EOF in map header") | Err("msgpack array length exceeds usize")
        ));
    }
}
