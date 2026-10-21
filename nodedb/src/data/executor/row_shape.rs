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

/// Convert a single sparse/document row to a `(storage key, msgpack)` pair.
///
/// Normalizes the body per [`sparse_body_to_msgpack`], then gives it its
/// identity per [`inject_row_identity`] under `identity_column`, the column
/// `CoreLoop::identity_column` resolves for the collection. Shared by the
/// materializing scan, the streaming scan, and every per-engine read, so all
/// of them produce byte-identical rows.
pub(in crate::data::executor) fn sparse_row_to_doc(
    key: &nodedb_types::StorageKey,
    raw: &[u8],
    format: SparseBodyFormatRef<'_>,
    identity_column: &str,
) -> (String, Vec<u8>) {
    let mp = sparse_body_to_msgpack(raw, format);
    (
        key.to_string(),
        inject_row_identity(&mp, key, identity_column).into_owned(),
    )
}

/// Give a msgpack row body its identity under `identity_column`.
///
/// `identity_column` is the collection's declared key, else `id`. A body that
/// holds that column keeps it unchanged: a declared key is authoritative, and
/// a vector-primary sidecar stores the user's key, so no `id` appears beside
/// it. A body that lacks it gains the identity there: the body's `_rowid`
/// when it carries one, else the storage key's identity per
/// [`StorageKey::to_identity`](nodedb_types::StorageKey::to_identity). A first
/// write sets `_rowid` to the surrogate, and a copy under a new surrogate
/// keeps it. The Control Plane's envelope shaping applies the same rule.
/// The result borrows `body` when it already holds the column.
pub(in crate::data::executor) fn inject_row_identity<'a>(
    body: &'a [u8],
    key: &nodedb_types::StorageKey,
    identity_column: &str,
) -> std::borrow::Cow<'a, [u8]> {
    if msgpack_scan::extract_field(body, 0, identity_column).is_some() {
        return std::borrow::Cow::Borrowed(body);
    }
    let identity = msgpack_scan::extract_field(body, 0, nodedb_types::ROWID_COLUMN)
        .and_then(|(start, _)| msgpack_scan::read_integer(body, start))
        .map(|rowid| rowid.to_string())
        .unwrap_or_else(|| key.to_identity().into_string());
    std::borrow::Cow::Owned(msgpack_scan::inject_str_field(
        body,
        identity_column,
        &identity,
    ))
}

/// Convert a single row from a `DecodedColumn` to a `nodedb_types::value::Value`.
///
/// `declared` is the column's declared type from the collection schema. A
/// segment block records only its physical layout, so the declared type
/// decides the variant, by the live memtable's read rule: a row reads
/// identically before and after a flush. A `UUID` cell is UUID text, a `ULID`
/// cell ULID text, a `VECTOR` cell an array of floats, a `GEOMETRY` cell its
/// stored text, and a `JSON` cell its decoded value. A `Timestamp` column
/// yields `Value::NaiveDateTime` and a `Timestamptz` column `Value::DateTime`,
/// both from the epoch microseconds the segment stores. Every other declared
/// type backed by integer storage yields the integer stored: a
/// `SystemTimestamp` column is an engine-assigned system-time count, and the
/// bitemporal `_ts_system`, `_ts_valid_from`, `_ts_valid_until` columns are
/// declared `Int64`: `_ts_system` holds a system-time ordinal (a columnar
/// read renders its millisecond), and the valid-time columns hold epoch
/// milliseconds with `i64::MIN` / `i64::MAX` as the unbounded sentinels,
/// which no instant can carry.
///
/// Returns `Value::Null` if the row index is out of range or the validity bit
/// is false. Returns `Err` when the cell's bytes do not hold a value of the
/// declared type: the segment is corrupt.
pub(in crate::data::executor) fn decoded_col_to_value(
    col: &nodedb_columnar::reader::DecodedColumn,
    row_idx: usize,
    declared: &nodedb_types::columnar::ColumnType,
) -> crate::Result<nodedb_types::value::Value> {
    nodedb_columnar::reader::decoded_cell_value(col, row_idx, declared).map_err(crate::Error::from)
}

#[cfg(test)]
mod tests {
    use nodedb_columnar::reader::DecodedColumn;
    use nodedb_types::columnar::ColumnType;
    use nodedb_types::value::Value;
    use nodedb_types::{InstantKind, NdbDateTime};

    use super::{SparseBodyFormatRef, kv_row_to_doc, msgpack_scan, sparse_row_to_doc};

    const MICROS: i64 = 1_583_402_400_000_000;

    fn decoded_col_to_value(col: &DecodedColumn, row: usize, ty: &ColumnType) -> Value {
        super::decoded_col_to_value(col, row, ty).expect("decodable cell")
    }

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

    /// A 16-byte identifier cell reads as the text its declared type names,
    /// as on the live memtable, never as raw bytes.
    #[test]
    fn an_identifier_cell_reads_back_as_its_declared_text() {
        let uuid = uuid::Uuid::from_u128(0x67e5_5044_10b1_426f_9247_bb68_0e5f_e0c8);
        let col = DecodedColumn::Binary {
            data: uuid.as_bytes().to_vec(),
            offsets: vec![0, 16],
            valid: vec![true],
        };
        assert_eq!(
            decoded_col_to_value(&col, 0, &ColumnType::Uuid),
            Value::Uuid(uuid.to_string())
        );
        // A ULID is 26 Crockford base32 characters.
        assert!(matches!(
            decoded_col_to_value(&col, 0, &ColumnType::Ulid),
            Value::Ulid(ref text) if text.len() == 26
        ));
        assert!(super::decoded_col_to_value(&col, 0, &ColumnType::Vector(2)).is_err());
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

    /// A schemaless body of `fields`, as msgpack.
    fn body(fields: &[(&str, &str)]) -> Vec<u8> {
        let mut mp = Vec::new();
        msgpack_scan::write_map_header(&mut mp, fields.len());
        for (name, value) in fields {
            msgpack_scan::write_str(&mut mp, name);
            msgpack_scan::write_str(&mut mp, value);
        }
        mp
    }

    /// Shape one schemaless row stored under surrogate 7.
    fn shaped(
        fields: &[(&str, &str)],
        identity_column: &str,
    ) -> std::collections::HashMap<String, Value> {
        let key = nodedb_types::StorageKey::for_surrogate(nodedb_types::Surrogate::new(7));
        let (_, mp) = sparse_row_to_doc(
            &key,
            &body(fields),
            SparseBodyFormatRef::Document,
            identity_column,
        );
        let Ok(Value::Object(row)) = nodedb_types::value_from_msgpack(&mp) else {
            panic!("a shaped row decodes as a map");
        };
        row
    }

    /// A row holding its declared key keeps it and gains no `id`.
    #[test]
    fn a_row_holding_its_declared_key_gains_no_id() {
        let row = shaped(&[("sku", "p1"), ("name", "pen")], "sku");
        assert_eq!(row.get("sku"), Some(&Value::String("p1".into())));
        assert!(!row.contains_key("id"), "{row:?}");
        assert_eq!(row.len(), 2, "{row:?}");
    }

    /// A row lacking its identity column gains the identity there, and only
    /// there.
    #[test]
    fn a_row_lacking_its_identity_column_gains_it_there() {
        let row = shaped(&[("name", "pen")], "sku");
        assert_eq!(row.get("sku"), Some(&Value::String("7".into())));
        assert!(!row.contains_key("id"), "{row:?}");

        let row = shaped(&[("name", "pen")], "id");
        assert_eq!(row.get("id"), Some(&Value::String("7".into())));
    }

    /// A stored `id` stays authoritative under a collection with no declared
    /// key.
    #[test]
    fn a_stored_id_stays_authoritative() {
        let row = shaped(&[("id", "dup"), ("n", "1")], "id");
        assert_eq!(row.get("id"), Some(&Value::String("dup".into())));
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
