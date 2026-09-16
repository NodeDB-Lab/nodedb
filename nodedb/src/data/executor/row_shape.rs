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
/// `key` is the row's storage key. The client-visible identity is its
/// surrogate's decimal string, per [`StorageKey::to_identity`]. Shared by the
/// materializing scan and the streaming scan so both paths produce
/// byte-identical output.
pub(in crate::data::executor) fn sparse_row_to_doc(
    key: &nodedb_types::StorageKey,
    raw: &[u8],
    format: SparseBodyFormatRef<'_>,
) -> (String, Vec<u8>) {
    let identity = key.to_identity();
    let mp = sparse_body_to_msgpack(raw, format);
    let mp = msgpack_scan::inject_str_field(&mp, "id", identity.as_str());
    (key.to_string(), mp)
}

/// Convert a single row from a `DecodedColumn` to a `nodedb_types::value::Value`.
///
/// `declared` is the column's declared type from the collection schema. It
/// decides how an eight-byte integer cell is typed, because the segment
/// reader infers a column's physical kind from its codec and decodes every
/// time column as `DecodedColumn::Int64`: a `Timestamp` column yields
/// `Value::NaiveDateTime`, a `Timestamptz` column `Value::DateTime`, both from
/// the epoch microseconds the segment stores. Every other declared type
/// yields the integer stored: a `SystemTimestamp` column is an engine-assigned
/// system-time count, and the bitemporal `_ts_system`, `_ts_valid_from`,
/// `_ts_valid_until` columns are declared `Int64` and hold epoch milliseconds
/// with `i64::MIN` / `i64::MAX` as the unbounded sentinels, which no instant
/// can carry. The live memtable applies the same rule, so a row reads
/// identically before and after a flush.
///
/// Returns `Value::Null` if the row index is out of range or the validity bit is false.
pub(in crate::data::executor) fn decoded_col_to_value(
    col: &nodedb_columnar::reader::DecodedColumn,
    row_idx: usize,
    declared: &nodedb_types::columnar::ColumnType,
) -> nodedb_types::value::Value {
    use nodedb_columnar::reader::DecodedColumn;
    use nodedb_types::value::Value;

    match col {
        DecodedColumn::Int64 { values, valid } | DecodedColumn::Timestamp { values, valid } => {
            if row_idx < valid.len() && valid[row_idx] {
                declared.time_cell(values[row_idx])
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
    }
}

#[cfg(test)]
mod tests {
    use nodedb_columnar::reader::DecodedColumn;
    use nodedb_types::columnar::ColumnType;
    use nodedb_types::value::Value;
    use nodedb_types::{InstantKind, NdbDateTime};

    use super::{decoded_col_to_value, kv_row_to_doc, msgpack_scan};

    const MICROS: i64 = 1_583_402_400_000_000;

    /// A time column as the segment reader decodes it: the reader infers the
    /// physical kind from the codec, so a time column arrives as `Int64`.
    fn time_column() -> DecodedColumn {
        DecodedColumn::Int64 {
            values: vec![MICROS, 0],
            valid: vec![true, false],
        }
    }

    /// A `TIMESTAMP` column reads back as a naive instant, a `TIMESTAMPTZ`
    /// column as a UTC instant, each carrying the stored microseconds,
    /// whether the reader decoded the column as `Int64` or as `Timestamp`.
    #[test]
    fn a_declared_instant_column_reads_back_as_its_instant_variant() {
        let col = time_column();
        assert_eq!(
            decoded_col_to_value(&col, 0, &ColumnType::Timestamp),
            Value::NaiveDateTime(NdbDateTime::from_micros(MICROS))
        );
        assert_eq!(
            decoded_col_to_value(&col, 0, &ColumnType::Timestamptz),
            Value::DateTime(NdbDateTime::from_micros(MICROS))
        );
        assert_eq!(
            decoded_col_to_value(&col, 0, &ColumnType::Timestamp),
            InstantKind::Naive.from_micros(MICROS)
        );
        let typed = DecodedColumn::Timestamp {
            values: vec![MICROS],
            valid: vec![true],
        };
        assert_eq!(
            decoded_col_to_value(&typed, 0, &ColumnType::Timestamptz),
            Value::DateTime(NdbDateTime::from_micros(MICROS))
        );
    }

    /// A system-time column and a duration column share time storage but are
    /// not instants: they read back as the integer stored.
    #[test]
    fn a_non_instant_time_column_reads_back_as_the_integer_stored() {
        let col = time_column();
        assert_eq!(
            decoded_col_to_value(&col, 0, &ColumnType::SystemTimestamp),
            Value::Integer(MICROS)
        );
        assert_eq!(
            decoded_col_to_value(&col, 0, &ColumnType::Duration),
            Value::Integer(MICROS)
        );
    }

    /// The declared type never changes how a non-time column reads, and an
    /// invalid or out-of-range row is NULL for every declared type.
    #[test]
    fn a_null_time_cell_is_null_for_every_declared_type() {
        let col = time_column();
        for ty in [
            ColumnType::Timestamp,
            ColumnType::Timestamptz,
            ColumnType::SystemTimestamp,
        ] {
            assert_eq!(decoded_col_to_value(&col, 1, &ty), Value::Null);
            assert_eq!(decoded_col_to_value(&col, 2, &ty), Value::Null);
        }
        let ints = DecodedColumn::Int64 {
            values: vec![7],
            valid: vec![true],
        };
        assert_eq!(
            decoded_col_to_value(&ints, 0, &ColumnType::Int64),
            Value::Integer(7)
        );
    }

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
