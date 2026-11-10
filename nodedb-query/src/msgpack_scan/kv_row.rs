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

use crate::msgpack_scan::kv_body::{ClassifiedBody, classify_body};
use crate::msgpack_scan::{KvBodyError, inject_str_field, write_map_header, write_str};

/// Shape a KV entry into the canonical `{key, value…}` msgpack row.
///
/// Three storage shapes coexist by design:
///
/// - **MessagePack map** (typed columns): `key` is injected into the existing
///   map at the binary level, no decode/re-encode roundtrip. A body that
///   already carries its own `key` field keeps it — the stored row is
///   authoritative for its own primary key.
/// - **Native tagged object** (typed columns that keep their native types):
///   `key` is added when the body has none, and the row stays native. A body
///   is native only when its object decodes in full ([`super::kv_body_shape`]
///   and this function share that rule), so a native body is never presented
///   as a `value` string.
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
///
/// # Errors
///
/// [`KvBodyError::Encode`] when a native row does not encode back.
pub fn kv_row_msgpack(key: &str, value: &[u8]) -> Result<Vec<u8>, KvBodyError> {
    match classify_body(value) {
        ClassifiedBody::Map => Ok(inject_str_field(value, "key", key)),
        ClassifiedBody::Native(mut fields) => {
            fields
                .entry("key".to_owned())
                .or_insert_with(|| nodedb_types::Value::String(key.to_owned()));
            zerompk::to_msgpack_vec(&nodedb_types::Value::Object(fields))
                .map_err(KvBodyError::Encode)
        }
        ClassifiedBody::Raw => {
            let mut buf = Vec::with_capacity(value.len() + key.len() + 16);
            write_map_header(&mut buf, 2);
            write_str(&mut buf, "key");
            write_str(&mut buf, key);
            write_str(&mut buf, "value");
            write_str(&mut buf, &String::from_utf8_lossy(value));
            Ok(buf)
        }
    }
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
        let mp = kv_row_msgpack("k1", b"v1").expect("row");
        assert_eq!(field_str(&mp, "key").as_deref(), Some("k1"));
        assert_eq!(field_str(&mp, "value").as_deref(), Some("v1"));
    }

    #[test]
    fn a_msgpack_map_value_keeps_its_fields_and_gains_the_key() {
        let mut body = Vec::new();
        crate::msgpack_scan::write_map_header(&mut body, 1);
        crate::msgpack_scan::write_str(&mut body, "n");
        crate::msgpack_scan::write_str(&mut body, "7");

        let mp = kv_row_msgpack("k2", &body).expect("row");
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

        let mp = kv_row_msgpack("slot", &body).expect("row");
        assert_eq!(field_str(&mp, "key").as_deref(), Some("stored"));
    }

    #[test]
    fn a_non_utf8_raw_value_still_produces_well_formed_msgpack() {
        let mp = kv_row_msgpack("k3", &[0xff, 0xfe]).expect("row");
        assert_eq!(field_str(&mp, "key").as_deref(), Some("k3"));
        assert!(
            field_str(&mp, "value").is_some(),
            "a lossy string is still a readable msgpack string"
        );
    }

    /// A native tagged body stays native when the key is injected, keeps its
    /// typed values, and never turns into a `value` string.
    #[test]
    fn a_native_body_stays_native_with_its_types() {
        use nodedb_types::Value;
        let decimal = Value::Decimal("5.25".parse().expect("decimal"));
        let uuid = Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into());
        let body = zerompk::to_msgpack_vec(&Value::Object(
            [
                ("amount".to_owned(), decimal.clone()),
                ("ref".to_owned(), uuid.clone()),
            ]
            .into_iter()
            .collect(),
        ))
        .expect("native body");
        let row = kv_row_msgpack("k4", &body).expect("row");
        let view = nodedb_types::ObjectView::new(&row).expect("an object row");
        assert_eq!(view.format(), nodedb_types::ObjectFormat::Native);
        assert_eq!(
            view.get("key").expect("key"),
            Some(Value::String("k4".into()))
        );
        assert_eq!(view.get("amount").expect("amount"), Some(decimal));
        assert_eq!(view.get("ref").expect("ref"), Some(uuid));
        assert_eq!(view.get("value").expect("value"), None);
    }

    /// A body framed as a native object `[7, {...}]` whose field does not
    /// decode is not native: it is raw bytes on every path. Its shape, its
    /// merge row and its presented row agree.
    #[test]
    fn a_native_body_that_does_not_decode_is_not_classified_native() {
        use crate::msgpack_scan::{KvBodyShape, kv_body_shape, kv_body_to_row};
        use nodedb_types::Value;
        // `[7, {"a": <tagged value with an unknown tag>}]`.
        let body = [0x92, 0x07, 0x81, 0xa1, b'a', 0x92, 0x7f, 0xc0];
        let shape = kv_body_shape(&body);
        assert_ne!(shape, KvBodyShape::Native);
        let (row, read_shape) = kv_body_to_row(&body).expect("raw row");
        assert_eq!(read_shape, shape);
        let presented = kv_row_msgpack("k7", &body).expect("row");
        let view = nodedb_types::ObjectView::new(&presented).expect("an object row");
        assert_eq!(view.format(), nodedb_types::ObjectFormat::Plain);
        assert_eq!(
            view.get("value").expect("value"),
            row.get("value").cloned(),
            "the read path and the merge path see the same row"
        );
        assert!(matches!(row.get("value"), Some(Value::String(_))));
    }

    /// A plain map body stays a plain map.
    #[test]
    fn a_plain_body_stays_plain() {
        let mut body = Vec::new();
        crate::msgpack_scan::write_map_header(&mut body, 1);
        crate::msgpack_scan::write_str(&mut body, "n");
        crate::msgpack_scan::write_str(&mut body, "7");
        let row = kv_row_msgpack("k5", &body).expect("row");
        let view = nodedb_types::ObjectView::new(&row).expect("an object row");
        assert_eq!(view.format(), nodedb_types::ObjectFormat::Plain);
        assert_eq!(field_str(&row, "key").as_deref(), Some("k5"));
    }

    /// A native body round-trips through the read-modify-write helpers in
    /// its own shape, so a merge never re-encodes it plain.
    #[test]
    fn a_native_body_round_trips_through_its_row() {
        use crate::msgpack_scan::{
            KvBodyShape, kv_body_to_row, kv_row_to_body_fields, row_to_kv_body,
        };
        use nodedb_types::Value;
        let body = zerompk::to_msgpack_vec(&Value::Object(
            [(
                "amount".to_owned(),
                Value::Decimal("5.25".parse().expect("decimal")),
            )]
            .into_iter()
            .collect(),
        ))
        .expect("native body");
        let (row, shape) = kv_body_to_row(&body).expect("decode");
        assert_eq!(shape, KvBodyShape::Native);
        assert_eq!(row_to_kv_body(&row, shape).expect("encode"), body);
        let (fields, shape) =
            kv_row_to_body_fields("k6", &kv_row_msgpack("k6", &body).expect("row"))
                .expect("fields");
        assert_eq!(shape, KvBodyShape::Native);
        assert_eq!(
            row_to_kv_body(&Value::Object(fields), shape).expect("encode"),
            body
        );
    }
}
