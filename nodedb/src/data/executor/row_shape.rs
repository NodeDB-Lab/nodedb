// SPDX-License-Identifier: BUSL-1.1

//! Per-row shape converters shared by every scan path.
//!
//! These take one engine-native row and produce the normalized form the rest
//! of the executor expects — a msgpack map for document-shaped engines, a
//! `Value` for columnar cells. They are free functions with no `CoreLoop`
//! receiver on purpose: the materializing scan, the streaming scan, the
//! per-engine handlers, and a write's `RETURNING` projection all call the SAME
//! converter, so a row a write hands back cannot disagree with a row a read of
//! the same key produces. Every past divergence in this area came from a
//! second copy of one of these rules.

use nodedb_query::msgpack_scan;

use super::sparse_body_format::SparseBodyFormatRef;

/// Convert a single KV engine entry to a `(key, msgpack)` document.
///
/// The shaping rule itself lives in `msgpack_scan::kv_row_msgpack` because the
/// Control Plane's single-key `Get` response needs the identical rule; this
/// wrapper only adds the decoded key string its Data-Plane callers also want.
/// Shared by the materializing scan, the streaming scan, the SQL `SELECT` scan
/// handler, and the `RETURNING` projection on KV writes.
pub(in crate::data::executor) fn kv_row_to_doc(key: &[u8], value: &[u8]) -> (String, Vec<u8>) {
    let key_str = String::from_utf8_lossy(key).to_string();
    let mp = msgpack_scan::kv_row_msgpack(&key_str, value);
    (key_str, mp)
}

/// Normalize one sparse-store row's bytes to a standard msgpack map.
///
/// THE single decision point for how a sparse body is decoded — every reader of
/// a sparse row comes through here, from the `SELECT` scan (via
/// [`sparse_row_to_doc`]) to the vector search body attach to the audit/`AS OF`
/// read, so none of them can drift into its own private notion of the encoding.
/// A caller that finds itself writing `match format { Document => …, Strict =>
/// … }` is re-creating this function badly; pass the format down instead.
///
/// The result BORROWS `raw` whenever no transcode was needed — which is the
/// ordinary case for a schemaless document body, already stored as a standard
/// msgpack map. That is precisely why a caller must NOT special-case
/// `SparseBodyFormatRef::Document` to "skip the copy": there is no copy to skip,
/// and a hand-written carve-out only re-creates the format decision this
/// function exists to own. Call it unconditionally and use the result as
/// `&[u8]`; take `.into_owned()` only where an owned `Vec` is genuinely
/// required.
pub(in crate::data::executor) fn sparse_body_to_msgpack<'a>(
    raw: &'a [u8],
    format: SparseBodyFormatRef<'_>,
) -> std::borrow::Cow<'a, [u8]> {
    match format {
        SparseBodyFormatRef::Strict(schema) => {
            super::strict_format::binary_tuple_to_msgpack(raw, schema)
                .map(std::borrow::Cow::Owned)
                .unwrap_or_else(|| super::doc_format::json_to_msgpack_cow(raw))
        }
        SparseBodyFormatRef::Document => super::doc_format::json_to_msgpack_cow(raw),
        SparseBodyFormatRef::VectorSidecar => super::doc_format::vector_sidecar_to_msgpack_cow(raw),
    }
}

/// Convert a single sparse/document row to a `(id, msgpack)` document.
///
/// Normalizes the body per [`sparse_body_to_msgpack`], then injects the `id`
/// field. Injection is a no-op when the body already carries an `id` — a
/// vector-primary sidecar stores the user's declared primary key, and its
/// sparse key is the internal surrogate-hex, which must not displace it.
///
/// `id` is the row's storage key. When it is a minted key, the client-visible
/// identity is the surrogate's decimal string, not the hex storage key; a
/// non-minted key (a user-supplied or DDL-declared primary key) passes
/// through unchanged. Shared by the materializing scan and the streaming scan
/// so both paths produce byte-identical output.
///
/// `id` comes off a store iterator as a plain `&str`, so this is one of the
/// few sites that reinterprets that shape: a value that parses as 8 lowercase
/// hex characters is a minted storage key, everything else is a legacy or
/// user key taken verbatim.
pub(in crate::data::executor) fn sparse_row_to_doc(
    id: &str,
    raw: &[u8],
    format: SparseBodyFormatRef<'_>,
) -> (String, Vec<u8>) {
    let identity = crate::engine::document::store::identity_of(id);
    let mp = sparse_body_to_msgpack(raw, format);
    let mp = msgpack_scan::inject_str_field(&mp, "id", identity.as_str());
    (id.to_string(), mp)
}

/// Convert a single row from a `DecodedColumn` to a `nodedb_types::value::Value`.
///
/// Returns `Value::Null` if the row index is out of range or the validity bit is false.
pub(in crate::data::executor) fn decoded_col_to_value(
    col: &nodedb_columnar::reader::DecodedColumn,
    row_idx: usize,
) -> nodedb_types::value::Value {
    use nodedb_columnar::reader::DecodedColumn;
    use nodedb_types::value::Value;

    match col {
        DecodedColumn::Int64 { values, valid } => {
            if row_idx < valid.len() && valid[row_idx] {
                Value::Integer(values[row_idx])
            } else {
                Value::Null
            }
        }
        DecodedColumn::Float64 { values, valid } => {
            if row_idx < valid.len() && valid[row_idx] {
                Value::Float(values[row_idx])
            } else {
                Value::Null
            }
        }
        DecodedColumn::Timestamp { values, valid } => {
            if row_idx < valid.len() && valid[row_idx] {
                // Represent as integer microseconds (same as Value::Integer for timestamps).
                Value::Integer(values[row_idx])
            } else {
                Value::Null
            }
        }
        DecodedColumn::Bool { values, valid } => {
            if row_idx < valid.len() && valid[row_idx] {
                Value::Bool(values[row_idx])
            } else {
                Value::Null
            }
        }
        DecodedColumn::Binary {
            data,
            offsets,
            valid,
        } => {
            if row_idx < valid.len() && valid[row_idx] && row_idx + 1 < offsets.len() {
                let start = offsets[row_idx] as usize;
                let end = offsets[row_idx + 1] as usize;
                if start <= end && end <= data.len() {
                    let bytes = &data[start..end];
                    // Best-effort UTF-8 interpretation; fall back to bytes.
                    match std::str::from_utf8(bytes) {
                        Ok(s) => Value::String(s.to_string()),
                        Err(_) => Value::Bytes(bytes.to_vec()),
                    }
                } else {
                    Value::Null
                }
            } else {
                Value::Null
            }
        }
        DecodedColumn::DictEncoded {
            ids,
            dictionary,
            valid,
        } => {
            if row_idx < valid.len() && valid[row_idx] {
                let id = ids[row_idx] as usize;
                if id < dictionary.len() {
                    Value::String(dictionary[id].clone())
                } else {
                    Value::Null
                }
            } else {
                Value::Null
            }
        }
        _ => Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::{kv_row_to_doc, msgpack_scan};

    /// A raw (non-msgpack) KV value must be wrapped as a msgpack STRING, not
    /// appended verbatim.
    ///
    /// Appending it produced a body a msgpack decoder ACCEPTS and misreads:
    /// `b"v1"` starts with `0x76`, a positive fixint, so the value decoded as
    /// the integer 118 and the trailing byte was discarded — no error anywhere.
    /// This pins the byte-level rule that keeps `RETURNING` and `SELECT` in
    /// agreement on the single-`value` KV form, through the Data-Plane entry
    /// point rather than only through the shared shaper's own tests.
    #[test]
    fn a_raw_kv_value_is_wrapped_as_a_string_not_appended_verbatim() {
        let (key, mp) = kv_row_to_doc(b"k1", b"v1");
        assert_eq!(key, "k1");

        let doc = crate::data::executor::doc_format::decode_document(&mp)
            .expect("the wrapped row must decode as msgpack");
        assert_eq!(
            doc.get("value").and_then(|v| v.as_str()),
            Some("v1"),
            "the raw value must survive as its text, not as its first byte: {doc:?}"
        );
        assert_eq!(doc.get("key").and_then(|v| v.as_str()), Some("k1"));
    }

    /// A msgpack-map value keeps its fields and gains `key`.
    #[test]
    fn a_msgpack_map_kv_value_keeps_its_fields() {
        let mut value = Vec::new();
        msgpack_scan::write_map_header(&mut value, 1);
        msgpack_scan::write_str(&mut value, "n");
        msgpack_scan::write_str(&mut value, "7");

        let (_key, mp) = kv_row_to_doc(b"k1", &value);
        let doc = crate::data::executor::doc_format::decode_document(&mp)
            .expect("the injected row must decode as msgpack");
        assert_eq!(doc.get("n").and_then(|v| v.as_str()), Some("7"));
        assert_eq!(doc.get("key").and_then(|v| v.as_str()), Some("k1"));
    }
}
