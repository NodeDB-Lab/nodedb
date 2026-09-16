// SPDX-License-Identifier: Apache-2.0

//! Msgpack scalar writers for constructing response messages, aggregate results,
//! projections, and other output data — all in raw msgpack without intermediary types.
//!
//! These are the building blocks for zero-serialization response construction.
//! Follow the timeseries ingest pattern: `SqlValue → row_to_msgpack()` at ingress,
//! raw msgpack throughout, `msgpack_to_json_string()` at outermost pgwire/HTTP layer.

use nodedb_types::InstantKind;

/// Write a msgpack map header.
#[inline]
pub fn write_map_header(buf: &mut Vec<u8>, len: usize) {
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

/// Write a msgpack array header.
#[inline]
pub fn write_array_header(buf: &mut Vec<u8>, len: usize) {
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

/// Write a msgpack string (header + UTF-8 bytes).
#[inline]
pub fn write_str(buf: &mut Vec<u8>, s: &str) {
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

/// Write a msgpack integer using the most compact encoding.
#[inline]
pub fn write_i64(buf: &mut Vec<u8>, i: i64) {
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

/// Write a msgpack float64.
#[inline]
pub fn write_f64(buf: &mut Vec<u8>, f: f64) {
    buf.push(0xCB);
    buf.extend_from_slice(&f.to_be_bytes());
}

/// Write a msgpack boolean.
#[inline]
pub fn write_bool(buf: &mut Vec<u8>, b: bool) {
    buf.push(if b { 0xC3 } else { 0xC2 });
}

/// Write a msgpack null.
#[inline]
pub fn write_null(buf: &mut Vec<u8>) {
    buf.push(0xC0);
}

/// Write a typed instant as the ten-byte instant ext (`fixext8` type 1 / 2).
#[inline]
pub fn write_instant(buf: &mut Vec<u8>, kind: InstantKind, micros: i64) {
    nodedb_types::write_instant(buf, kind, micros);
}

/// Write a msgpack binary blob (bin 8/16/32).
#[inline]
pub fn write_bin(buf: &mut Vec<u8>, data: &[u8]) {
    let len = data.len();
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
    buf.extend_from_slice(data);
}

/// Write a key-value pair into a msgpack map being built.
///
/// Caller is responsible for writing the map header first via [`write_map_header`].
#[inline]
pub fn write_kv_str(buf: &mut Vec<u8>, key: &str, value: &str) {
    write_str(buf, key);
    write_str(buf, value);
}

/// Write a key-value pair (string key, i64 value).
#[inline]
pub fn write_kv_i64(buf: &mut Vec<u8>, key: &str, value: i64) {
    write_str(buf, key);
    write_i64(buf, value);
}

/// Write a key-value pair (string key, f64 value).
#[inline]
pub fn write_kv_f64(buf: &mut Vec<u8>, key: &str, value: f64) {
    write_str(buf, key);
    write_f64(buf, value);
}

/// Write a key-value pair (string key, bool value).
#[inline]
pub fn write_kv_bool(buf: &mut Vec<u8>, key: &str, value: bool) {
    write_str(buf, key);
    write_bool(buf, value);
}

/// Write a key-value pair (string key, raw msgpack value bytes).
///
/// The value bytes must be valid msgpack. This enables splicing raw
/// field bytes extracted via `msgpack_scan::extract_field` directly
/// into a new map without decode/re-encode.
#[inline]
pub fn write_kv_raw(buf: &mut Vec<u8>, key: &str, raw_value: &[u8]) {
    write_str(buf, key);
    buf.extend_from_slice(raw_value);
}

/// Write a key-value pair (string key, null value).
#[inline]
pub fn write_kv_null(buf: &mut Vec<u8>, key: &str) {
    write_str(buf, key);
    write_null(buf);
}

/// Write a key-value pair (string key, instant value).
#[inline]
pub fn write_kv_instant(buf: &mut Vec<u8>, key: &str, kind: InstantKind, micros: i64) {
    write_str(buf, key);
    write_instant(buf, kind, micros);
}

/// Inject a string field into a msgpack map without full decode.
///
/// If `value` is a valid msgpack map that already contains `field_name`,
/// the existing value is preserved and the input is returned unchanged —
/// the caller's body is authoritative for its own primary key. If the map
/// does not contain the field, it is prepended. If `value` is not a map,
/// wraps as `{field_name: field_value, "value": raw}`, with `raw` encoded as
/// a msgpack STRING: appending non-msgpack bytes verbatim produces a body a
/// decoder reads as one scalar and silently truncates (`"v1"` becomes the
/// integer 118), which is worse than an error because it reports success.
/// Callers holding a genuine map never reach this branch.
pub fn inject_str_field(value: &[u8], field_name: &str, field_value: &str) -> Vec<u8> {
    if let Some((count, body_start)) = crate::msgpack_scan::reader::map_header(value, 0) {
        if crate::msgpack_scan::extract_field(value, 0, field_name).is_some() {
            return value.to_vec();
        }
        let mut buf = Vec::with_capacity(value.len() + field_name.len() + field_value.len() + 16);
        write_map_header(&mut buf, count + 1);
        write_kv_str(&mut buf, field_name, field_value);
        buf.extend_from_slice(&value[body_start..]);
        buf
    } else {
        let mut buf = Vec::with_capacity(value.len() + field_name.len() + field_value.len() + 16);
        write_map_header(&mut buf, 2);
        write_kv_str(&mut buf, field_name, field_value);
        write_str(&mut buf, "value");
        write_str(&mut buf, &String::from_utf8_lossy(value));
        buf
    }
}

/// Merge field updates into a msgpack map without full decode.
///
/// Takes a base msgpack map and a list of `(field_name, raw_msgpack_value)` updates.
/// Returns a new msgpack map with updated fields replaced and new fields appended.
/// Fields not in `updates` are copied from the original.
pub fn merge_fields(base: &[u8], updates: &[(&str, &[u8])]) -> Vec<u8> {
    use std::collections::HashSet;

    let update_names: HashSet<&str> = updates.iter().map(|(k, _)| *k).collect();

    let (count, body_start) = match crate::msgpack_scan::reader::map_header(base, 0) {
        Some(v) => v,
        None => {
            // Not a valid map — build from updates only.
            let mut buf = Vec::with_capacity(updates.len() * 32);
            write_map_header(&mut buf, updates.len());
            for (k, v) in updates {
                write_str(&mut buf, k);
                buf.extend_from_slice(v);
            }
            return buf;
        }
    };

    // Count fields: existing (not overwritten) + updates.
    let mut kept = 0usize;
    let mut pos = body_start;
    for _ in 0..count {
        let key = crate::msgpack_scan::reader::read_str(base, pos);
        pos = match crate::msgpack_scan::reader::skip_value(base, pos) {
            Some(p) => p,
            None => break,
        };
        pos = match crate::msgpack_scan::reader::skip_value(base, pos) {
            Some(p) => p,
            None => break,
        };
        if let Some(k) = &key {
            if !update_names.contains(&k[..]) {
                kept += 1;
            }
        } else {
            kept += 1;
        }
    }

    let new_count = kept + updates.len();
    let mut buf = Vec::with_capacity(
        base.len()
            + updates
                .iter()
                .map(|(k, v)| k.len() + v.len() + 4)
                .sum::<usize>(),
    );
    write_map_header(&mut buf, new_count);

    // Copy non-overwritten fields from base.
    pos = body_start;
    for _ in 0..count {
        let key_start = pos;
        let key = crate::msgpack_scan::reader::read_str(base, pos);
        pos = match crate::msgpack_scan::reader::skip_value(base, pos) {
            Some(p) => p,
            None => break,
        };
        pos = match crate::msgpack_scan::reader::skip_value(base, pos) {
            Some(p) => p,
            None => break,
        };
        if let Some(k) = &key
            && update_names.contains(&k[..])
        {
            continue; // Will be written from updates.
        }
        // Copy key + value bytes.
        buf.extend_from_slice(&base[key_start..pos]);
    }

    // Write updated/new fields.
    for (k, v) in updates {
        write_str(&mut buf, k);
        buf.extend_from_slice(v);
    }

    buf
}

/// Build a simple msgpack map from string key-value pairs.
pub fn build_str_map(pairs: &[(&str, &str)]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(pairs.len() * 32);
    write_map_header(&mut buf, pairs.len());
    for (k, v) in pairs {
        write_kv_str(&mut buf, k, v);
    }
    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_map() {
        let mut buf = Vec::new();
        write_map_header(&mut buf, 2);
        write_kv_str(&mut buf, "name", "alice");
        write_kv_i64(&mut buf, "age", 30);

        // Verify it's valid msgpack by reading back via reader
        let (count, mut pos) = crate::msgpack_scan::reader::map_header(&buf, 0).unwrap();
        assert_eq!(count, 2);

        let k1 = crate::msgpack_scan::reader::read_str(&buf, pos).unwrap();
        assert_eq!(k1, "name");
        pos = crate::msgpack_scan::reader::skip_value(&buf, pos).unwrap();
        let v1 = crate::msgpack_scan::reader::read_str(&buf, pos).unwrap();
        assert_eq!(v1, "alice");
        pos = crate::msgpack_scan::reader::skip_value(&buf, pos).unwrap();

        let k2 = crate::msgpack_scan::reader::read_str(&buf, pos).unwrap();
        assert_eq!(k2, "age");
        pos = crate::msgpack_scan::reader::skip_value(&buf, pos).unwrap();
        let v2 = crate::msgpack_scan::reader::read_i64(&buf, pos).unwrap();
        assert_eq!(v2, 30);
    }

    #[test]
    fn write_kv_raw_splices_correctly() {
        // Build a value as raw msgpack
        let mut val_buf = Vec::new();
        write_str(&mut val_buf, "hello");

        // Build a map with raw splice
        let mut buf = Vec::new();
        write_map_header(&mut buf, 1);
        write_kv_raw(&mut buf, "greeting", &val_buf);

        // Verify
        let field = crate::msgpack_scan::field::extract_field(&buf, 0, "greeting").unwrap();
        let s = crate::msgpack_scan::reader::read_str(&buf, field.0).unwrap();
        assert_eq!(s, "hello");
    }

    #[test]
    fn compact_integer_encoding() {
        // Positive fixint
        let mut buf = Vec::new();
        write_i64(&mut buf, 42);
        assert_eq!(buf, vec![42]);

        // Negative fixint
        buf.clear();
        write_i64(&mut buf, -1);
        assert_eq!(buf, vec![0xFF]);

        // int8
        buf.clear();
        write_i64(&mut buf, -100);
        assert_eq!(buf, vec![0xD0, (-100i8) as u8]);
    }

    #[test]
    fn instant_cell_reads_back() {
        let mut buf = Vec::new();
        write_map_header(&mut buf, 1);
        write_kv_instant(&mut buf, "ts", InstantKind::Naive, -42);

        let field = crate::msgpack_scan::field::extract_field(&buf, 0, "ts").unwrap();
        assert_eq!(field.1 - field.0, 10);
        assert_eq!(
            crate::msgpack_scan::reader::read_value(&buf, field.0),
            Some(nodedb_types::Value::NaiveDateTime(
                nodedb_types::NdbDateTime::from_micros(-42)
            ))
        );
    }
}
