// SPDX-License-Identifier: BUSL-1.1

//! Document decoding helpers shared by the read paths.
//!
//! Every helper here takes the row's encoding as a REQUIRED parameter. Three
//! encodings share the sparse store — schemaless document bodies (standard
//! MessagePack), strict document bodies (Binary Tuples), and vector-primary
//! metadata sidecars (`zerompk` TAGGED `HashMap<String, Value>`) — and a
//! tagged map and a plain document map are both valid MessagePack maps that
//! begin with the same map header. No inspection of the stored bytes can
//! separate them, so a decoder that sniffs necessarily mis-reads one of them
//! and returns `[4,"alice"]` where the client asked for `alice`.

use crate::data::executor::scan_normalize::sparse_body_to_msgpack;
use crate::data::executor::sparse_body_format::SparseBodyFormatRef;
use crate::data::executor::{doc_format, strict_format};

/// Decode one sparse-store row to `serde_json::Value`, reusing a normalized
/// msgpack image the caller already built for it.
///
/// This is the single implementation of "which decoder does this encoding
/// need"; [`decode_scanned_document`] is the shorthand for callers that hold no
/// image. `normalized` is what
/// [`crate::data::executor::scan_normalize::sparse_body_to_msgpack`] returns for
/// `raw` — a caller that ran an RLS predicate has one in hand, and passing it
/// keeps a vector sidecar from being transcoded a second time here. `None`
/// means "not built", and then it is built only for the encodings whose decoder
/// actually reads it, so a strict row never pays for a msgpack image this
/// function would discard.
///
/// `format` is never derived from `raw`: see the module docs for why the bytes
/// cannot answer the question. The caller resolves it once from the
/// collection's registered kind via
/// [`crate::data::executor::core_loop::CoreLoop::sparse_body_format`].
///
/// The strict arm decodes from the Binary Tuple through `binary_tuple_to_json`
/// rather than from the normalized image, because that is the projection-facing
/// form — it drops the reserved bitemporal bookkeeping columns and applies the
/// strict value→JSON coercions. That is exactly why `raw` stays required
/// alongside `normalized`: the image is not a substitute for the stored bytes,
/// only an alternative source for the encodings that are msgpack-shaped.
///
/// A row that will not decode under its resolved encoding is an error, not a
/// `Null` document: a scan that renders it as `null` puts a row in the result
/// set that no client can tell apart from one whose columns really are null,
/// and the corruption never surfaces anywhere.
pub(in crate::data::executor) fn decode_scanned_row(
    raw: &[u8],
    normalized: Option<&[u8]>,
    format: SparseBodyFormatRef<'_>,
) -> crate::Result<serde_json::Value> {
    match format {
        SparseBodyFormatRef::Strict(schema) => {
            match strict_format::binary_tuple_to_json(raw, schema) {
                Some(doc) => Ok(doc),
                // A row written before the collection became strict is still a
                // schemaless MessagePack body; its own decode error is the one
                // worth reporting when that reading fails too.
                None => doc_format::render_document_json(raw),
            }
        }
        // The image is plain or native. A native image renders as its plain
        // form reads, so both storage forms of one row render alike.
        SparseBodyFormatRef::Document | SparseBodyFormatRef::VectorSidecar => match normalized {
            Some(image) => doc_format::render_document_json(image),
            None => doc_format::render_document_json(&sparse_body_to_msgpack(raw, format)?),
        },
    }
}

/// Decode one sparse-store row to `serde_json::Value` from its stored bytes
/// alone (window functions and the FTS hydration paths, which shape rows as
/// JSON).
///
/// Delegates to [`decode_scanned_row`] with no pre-built image. A caller that
/// has already normalized the row — to evaluate an RLS predicate against the
/// msgpack form, say — should call that directly and hand its image over
/// instead of coming through here, which would derive it a second time.
pub(in crate::data::executor) fn decode_scanned_document(
    value: &[u8],
    format: SparseBodyFormatRef<'_>,
) -> crate::Result<serde_json::Value> {
    decode_scanned_row(value, None, format)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::Value;
    use nodedb_types::columnar::{ColumnDef, ColumnType, StrictSchema};

    #[test]
    fn decode_scanned_document_uses_strict_schema_for_binary_tuple_rows() {
        let schema = StrictSchema {
            columns: vec![
                ColumnDef::required("id", ColumnType::String).with_primary_key(),
                ColumnDef::required("name", ColumnType::String),
                ColumnDef::nullable("age", ColumnType::Int64),
            ],
            version: 1,
            dropped_columns: Vec::new(),
            bitemporal: false,
        };
        let mut map = std::collections::HashMap::new();
        map.insert("id".into(), Value::String("u1".into()));
        map.insert("name".into(), Value::String("Ada".into()));
        map.insert("age".into(), Value::Integer(42));

        let tuple = strict_format::value_to_binary_tuple(&Value::Object(map), &schema, "docs")
            .expect("encode strict tuple");

        let decoded = decode_scanned_document(&tuple, SparseBodyFormatRef::Strict(&schema))
            .expect("strict tuple must decode");

        assert_eq!(
            decoded,
            serde_json::json!({
                "id": "u1",
                "name": "Ada",
                "age": 42
            })
        );
    }

    /// A vector-primary sidecar must decode to its VALUES, not to tag arrays.
    ///
    /// The sidecar is `zerompk::to_msgpack_vec(&HashMap<String, Value>)` — the
    /// tagged form, where `Value::String("alice")` encodes as `[4,"alice"]`.
    /// Its outer container is an ordinary MessagePack map, so the document
    /// decoder ACCEPTS it and yields the tag arrays verbatim; the assertion on
    /// `SparseBodyFormat::Document` below pins that, so the sidecar arm is
    /// shown to be doing the work rather than an incidental byte-level
    /// coincidence.
    #[test]
    fn decode_scanned_document_reads_a_vector_sidecar_as_values_not_tag_arrays() {
        let mut map = std::collections::HashMap::new();
        map.insert("id".to_string(), Value::String("r1".into()));
        map.insert("owner".to_string(), Value::String("alice".into()));
        let tagged = zerompk::to_msgpack_vec(&map).expect("encode tagged sidecar");

        let as_document = decode_scanned_document(&tagged, SparseBodyFormatRef::Document)
            .expect("a tagged map is still a valid msgpack map");
        assert_ne!(
            as_document.get("owner").and_then(|v| v.as_str()),
            Some("alice"),
            "decoding a sidecar as a document body must NOT yield the value — if it \
             does, this test no longer proves the format parameter is load-bearing"
        );

        let decoded = decode_scanned_document(&tagged, SparseBodyFormatRef::VectorSidecar)
            .expect("sidecar must decode");
        assert_eq!(
            decoded.get("owner").and_then(|v| v.as_str()),
            Some("alice"),
            "a sidecar payload column must decode to its value: {decoded:?}"
        );
        assert_eq!(
            decoded.get("id").and_then(|v| v.as_str()),
            Some("r1"),
            "the declared primary key must decode to its value: {decoded:?}"
        );
    }

    /// Handing over an already-normalized image must decode to the SAME value
    /// as letting the decoder derive one.
    ///
    /// The reuse path exists so a row gated by an RLS predicate is transcoded
    /// once rather than twice; if it could diverge from the plain path, a
    /// collection under a policy would render differently from the same
    /// collection without one.
    #[test]
    fn a_reused_normalized_image_decodes_identically_to_a_derived_one() {
        let mut map = std::collections::HashMap::new();
        map.insert("id".to_string(), Value::String("r1".into()));
        map.insert("owner".to_string(), Value::String("alice".into()));
        let tagged = zerompk::to_msgpack_vec(&map).expect("encode tagged sidecar");

        let image = crate::data::executor::scan_normalize::sparse_body_to_msgpack(
            &tagged,
            SparseBodyFormatRef::VectorSidecar,
        )
        .expect("normalize sidecar");

        assert_eq!(
            decode_scanned_row(&tagged, Some(&*image), SparseBodyFormatRef::VectorSidecar)
                .expect("reused image must decode"),
            decode_scanned_document(&tagged, SparseBodyFormatRef::VectorSidecar)
                .expect("derived image must decode"),
        );
    }

    /// An unreadable row is an error, never a `Null` document.
    ///
    /// A scan that renders it as `null` puts a row in the result set that no
    /// client can distinguish from one whose columns really are null, so the
    /// corruption never surfaces anywhere and the row count still looks right.
    #[test]
    fn an_undecodable_row_is_an_error_not_a_null_document() {
        let mut body = nodedb_types::json_to_msgpack(&serde_json::json!({"id": "r1", "n": 7}))
            .expect("encode");
        assert!(
            decode_scanned_document(&body, SparseBodyFormatRef::Document).is_ok(),
            "baseline body must decode"
        );

        body.push(0xC0);
        assert!(
            decode_scanned_document(&body, SparseBodyFormatRef::Document).is_err(),
            "a body with a trailing byte must not decode to Null"
        );
        assert!(
            decode_scanned_row(&body, Some(&body), SparseBodyFormatRef::Document).is_err(),
            "the reused-image path must reject it the same way"
        );
    }

    /// A strict row ignores any image and decodes from the Binary Tuple.
    ///
    /// The strict projection form drops the reserved bitemporal columns, which
    /// the msgpack image keeps, so reading the image here would change the
    /// returned column set.
    #[test]
    fn a_strict_row_decodes_from_the_tuple_even_when_an_image_is_supplied() {
        let schema = StrictSchema {
            columns: vec![
                ColumnDef::required("id", ColumnType::String).with_primary_key(),
                ColumnDef::required("name", ColumnType::String),
            ],
            version: 1,
            dropped_columns: Vec::new(),
            bitemporal: false,
        };
        let mut map = std::collections::HashMap::new();
        map.insert("id".into(), Value::String("u1".into()));
        map.insert("name".into(), Value::String("Ada".into()));
        let tuple = strict_format::value_to_binary_tuple(&Value::Object(map), &schema, "docs")
            .expect("encode strict tuple");

        let image = crate::data::executor::scan_normalize::sparse_body_to_msgpack(
            &tuple,
            SparseBodyFormatRef::Strict(&schema),
        )
        .expect("normalize strict tuple");

        assert_eq!(
            decode_scanned_row(&tuple, Some(&*image), SparseBodyFormatRef::Strict(&schema))
                .expect("strict tuple must decode"),
            serde_json::json!({"id": "u1", "name": "Ada"}),
        );
    }

    /// A native image renders as `value_to_wire_json` renders its typed
    /// value: Decimal as its exact text, UUID and instant as their text, a
    /// nested object as an object, and a plain array field as an array. A
    /// plain image renders unchanged. A malformed native image is an error.
    #[test]
    fn a_native_image_renders_as_its_typed_value() {
        let row = Value::Object(
            [
                ("id".to_owned(), Value::String("r1".into())),
                (
                    "price".to_owned(),
                    Value::Decimal("123.45".parse().unwrap()),
                ),
                (
                    "uid".to_owned(),
                    Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into()),
                ),
                (
                    "seen".to_owned(),
                    Value::DateTime(nodedb_types::NdbDateTime::from_micros(
                        1_583_402_400_000_000,
                    )),
                ),
                (
                    "meta".to_owned(),
                    Value::Object([("k".to_owned(), Value::Integer(1))].into()),
                ),
                (
                    "tags".to_owned(),
                    Value::Array(vec![Value::Integer(7), Value::String("x".into())]),
                ),
            ]
            .into(),
        );
        let native = zerompk::to_msgpack_vec(&row).expect("encode native");
        let plain = nodedb_types::value_to_msgpack(&row).expect("encode plain");

        let rendered = decode_scanned_row(&native, Some(&native), SparseBodyFormatRef::Document)
            .expect("native image renders");
        assert_eq!(rendered, crate::util::wire_json::value_to_wire_json(&row));
        assert_eq!(
            decode_scanned_document(&native, SparseBodyFormatRef::Document)
                .expect("native body renders"),
            rendered
        );
        assert_eq!(rendered["price"], "123.45");
        assert_eq!(rendered["uid"], "550e8400-e29b-41d4-a716-446655440000");
        assert_eq!(rendered["seen"], "2020-03-05T10:00:00.000000Z");
        assert_eq!(rendered["meta"], serde_json::json!({"k": 1}));
        assert_eq!(rendered["tags"], serde_json::json!([7, "x"]));

        let plain_rendered =
            decode_scanned_row(&plain, Some(&plain), SparseBodyFormatRef::Document)
                .expect("plain image renders");
        assert_eq!(
            plain_rendered,
            nodedb_types::json_from_msgpack(&plain).expect("plain json")
        );
        assert_eq!(plain_rendered["price"], "123.45");

        let malformed = [0x92, 0x07, 0x81, 0xa1, b'a', 0x92, 0x63, 0x01];
        assert!(
            decode_scanned_row(&malformed, Some(&malformed), SparseBodyFormatRef::Document)
                .is_err()
        );
    }
}
