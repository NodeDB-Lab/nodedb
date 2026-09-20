// SPDX-License-Identifier: Apache-2.0

//! The one shaping rule for a key-value entry's `(key, value)` byte pair.
//!
//! Lives here, beside `inject_str_field`, because BOTH execution planes need
//! it: the Data Plane shapes scan results and a write's `RETURNING` projection,
//! and the Control Plane shapes a single-key `Get` response. It is a pure byte
//! transform — no I/O, no shared state, no runtime — so a single definition
//! crosses that boundary without touching plane separation, which governs
//! tokio/io_uring and shared mutable state rather than pure helpers.
//!
//! One definition is the point. Two copies of this rule already existed and
//! silently diverged: one of them appended raw bytes verbatim into a msgpack
//! map, and every scan path served `value: 118` for a stored `"v1"`.

use crate::msgpack_scan::{
    KvBodyShape, inject_str_field, kv_body_shape, write_map_header, write_str,
};

/// Shape a KV entry into the canonical `{key, value…}` msgpack row.
///
/// Two storage shapes coexist by design, and the difference between them is
/// the whole reason this function exists:
///
/// - **MessagePack map** (typed columns): `key` is injected into the existing
///   map at the binary level, no decode/re-encode roundtrip. A body that
///   already carries its own `key` field keeps it — the stored row is
///   authoritative for its own primary key.
/// - **Raw bytes** (the single-`value` column form, and RESP `SET`): the bytes
///   are NOT msgpack at all, so they are wrapped as a msgpack STRING under
///   `value`. Appending them verbatim instead would hand a msgpack decoder a
///   non-msgpack body — which does not error: it reads the first byte as a
///   scalar and discards the rest, so `"v1"` decodes as the integer 118
///   (ASCII `'v'`). A decoder that succeeds on the wrong format is exactly the
///   failure this wrapping prevents.
///
/// Raw values may be non-UTF-8 (RESP `SET` accepts arbitrary bytes), so the
/// lossy view is taken: a SQL `SELECT` over binary values is already degraded
/// by the pgwire text protocol, and this keeps the output well-formed msgpack
/// rather than merely different-but-still-broken.
///
/// A read-modify-write decodes and re-encodes the body through
/// [`super::kv_body_to_row`] / [`super::row_to_kv_body`], which keep the
/// stored shape.
pub fn kv_row_msgpack(key: &str, value: &[u8]) -> Vec<u8> {
    if kv_body_shape(value) == KvBodyShape::Map {
        return inject_str_field(value, "key", key);
    }
    let mut buf = Vec::with_capacity(value.len() + key.len() + 16);
    write_map_header(&mut buf, 2);
    write_str(&mut buf, "key");
    write_str(&mut buf, key);
    write_str(&mut buf, "value");
    write_str(&mut buf, &String::from_utf8_lossy(value));
    buf
}

#[cfg(test)]
mod tests {
    use super::kv_row_msgpack;
    use crate::msgpack_scan::extract_field;

    fn field_str(mp: &[u8], name: &str) -> Option<String> {
        let (start, _end) = extract_field(mp, 0, name)?;
        crate::msgpack_scan::read_str(mp, start).map(str::to_string)
    }

    #[test]
    fn a_raw_value_is_wrapped_as_a_string_not_appended_verbatim() {
        // Appending verbatim made a decoder read 0x76 as the integer 118 and
        // discard the rest, reporting success. Pin the shape that prevents it.
        let mp = kv_row_msgpack("k1", b"v1");
        assert_eq!(field_str(&mp, "key").as_deref(), Some("k1"));
        assert_eq!(field_str(&mp, "value").as_deref(), Some("v1"));
    }

    #[test]
    fn a_msgpack_map_value_keeps_its_fields_and_gains_the_key() {
        let mut body = Vec::new();
        crate::msgpack_scan::write_map_header(&mut body, 1);
        crate::msgpack_scan::write_str(&mut body, "n");
        crate::msgpack_scan::write_str(&mut body, "7");

        let mp = kv_row_msgpack("k2", &body);
        assert_eq!(field_str(&mp, "key").as_deref(), Some("k2"));
        assert_eq!(field_str(&mp, "n").as_deref(), Some("7"));
    }

    #[test]
    fn a_body_that_already_carries_a_key_field_keeps_its_own() {
        // The stored row is authoritative for its own primary key; overwriting
        // it from the KV key slot would let the two disagree silently.
        let mut body = Vec::new();
        crate::msgpack_scan::write_map_header(&mut body, 1);
        crate::msgpack_scan::write_str(&mut body, "key");
        crate::msgpack_scan::write_str(&mut body, "stored");

        let mp = kv_row_msgpack("slot", &body);
        assert_eq!(field_str(&mp, "key").as_deref(), Some("stored"));
    }

    #[test]
    fn a_non_utf8_raw_value_still_produces_well_formed_msgpack() {
        let mp = kv_row_msgpack("k3", &[0xff, 0xfe]);
        assert_eq!(field_str(&mp, "key").as_deref(), Some("k3"));
        assert!(
            field_str(&mp, "value").is_some(),
            "a lossy string is still a readable msgpack string"
        );
    }
}
