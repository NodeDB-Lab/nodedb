// SPDX-License-Identifier: BUSL-1.1

//! Encode a protocol-neutral [`ShapedRows`] (from
//! `response_shape::types`/`response_shape::project`) into a pgwire
//! `Response::Query`.
//!
//! This is the pgwire entrypoint's encoder for the canonical neutral shaping
//! core: the SELECT-read path builds a `ShapedRows` once and every protocol
//! entrypoint (pgwire, native, http) renders it in its own wire format. Here,
//! each cell renders in its column's PostgreSQL text form, driven by the
//! per-column `DdlColType` the shaper threaded through `ShapedRows`:
//! `Float8`/`Float4` go through pgwire's native float encoder (so `0.0` stays
//! `"0.0"`, not `"0"`), `Timestamp`/`Timestamptz` epoch-microsecond cells
//! render as ISO-8601 text, and everything else (`Text`, integers, `Bool`)
//! falls back to `json_value_to_text` — notably `Bool` as `t`/`f`, not
//! `true`/`false`.
//!
//! Each typed cell converts to JSON at this edge through
//! [`value_to_wire_json`] before it is rendered.

use std::sync::Arc;

use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response};
use pgwire::error::PgWireResult;
use pgwire::messages::data::DataRow;

use nodedb_types::NdbDateTime;
use nodedb_types::columnar::IntWidth;

use crate::control::server::response_shape::cell::value_to_wire_json;
use crate::control::server::response_shape::project::json_value_to_text;
use crate::control::server::response_shape::types::{DdlColType, ShapedRow, ShapedRows};

use super::super::ddl_encode::col_type_to_field_with_format;
use super::super::numeric_narrow::{checked_narrow, checked_narrow_f32};

/// Encode one flat row object into a pgwire `DataRow`, using `cell_keys` (in
/// order) to look up cells in `row` and `column_types` (parallel to
/// `cell_keys`) to pick each cell's text rendering.
///
/// `cell_keys` are the per-column unique row-map keys derived from the
/// display names via `response_shape::project::cell_keys` — identical to the
/// display names except where those repeat (`SELECT w.id, b.id`), in which
/// case later duplicates carry a `_n` suffix so both cells survive the map.
///
/// Each cell converts to JSON at this edge. Missing keys and a cell whose
/// JSON is `null` (an explicit `Value::Null`, or a float with no JSON form)
/// both encode as SQL NULL. Every other cell renders per its column type via
/// [`encode_typed_cell`]; a missing/short `column_types` entry defaults to
/// `Text`.
pub(in crate::control::server::pgwire) fn encode_shaped_row(
    schema: &Arc<Vec<FieldInfo>>,
    cell_keys: &[String],
    column_types: &[DdlColType],
    formats: &[FieldFormat],
    row: &ShapedRow,
) -> PgWireResult<DataRow> {
    let mut encoder = DataRowEncoder::new(schema.clone());
    for (idx, name) in cell_keys.iter().enumerate() {
        let ct = column_types.get(idx).copied().unwrap_or(DdlColType::Text);
        let format = formats.get(idx).copied().unwrap_or(FieldFormat::Text);
        match row.get(name).map(value_to_wire_json) {
            None | Some(serde_json::Value::Null) => {
                encoder.encode_field(&None::<&str>)?;
            }
            Some(v) => encode_typed_cell(&mut encoder, ct, format, &v)?,
        }
    }
    Ok(encoder.take_row())
}

/// Encode one non-NULL JSON cell into `encoder` per its column type `ct`.
///
/// `Float8`/`Float4` numeric cells go through pgwire's native float encoder
/// (ryu + `extra_float_digits`) so their text bytes match PostgreSQL exactly;
/// `Timestamp`/`Timestamptz` epoch-microsecond numbers render as ISO-8601
/// text. Any cell whose JSON shape doesn't match the typed arm (e.g. an
/// already-formatted timestamp string) falls back to `json_value_to_text`, as
/// does every other type — `Text`, integers, and `Bool` (`t`/`f`).
fn encode_typed_cell(
    encoder: &mut DataRowEncoder,
    ct: DdlColType,
    format: FieldFormat,
    v: &serde_json::Value,
) -> PgWireResult<()> {
    use serde_json::Value;

    // Binary result format: the column's `FieldInfo` is Binary, so
    // `encode_field` emits the value's binary wire form. Extract the correctly
    // typed scalar from the JSON cell. A type/shape mismatch cannot fall back
    // to the text arms here — the RowDescription already advertises this
    // column's binary type, so a text value under it would be misread by the
    // client; encode SQL NULL for the (well-typed data should never hit this)
    // mismatch instead. Only the feature-supported scalar types reach a Binary
    // format (the resolver downgrades the rest to Text upstream); any other
    // `ct` under Binary falls through to the text arms below.
    if format == FieldFormat::Binary {
        match ct {
            DdlColType::Int8 => return encoder.encode_field(&v.as_i64()),
            // Narrowing casts are fallible, so they are `try_from`, not `as`.
            // A stored value wider than the column's declared width cannot be
            // transmitted under a narrowed OID: the client reads exactly 2 or 4
            // bytes and would silently decode a wrapped number. Writes are
            // range-checked (`nodedb_sql::planner::dml`), so this is
            // unreachable for data written through SQL — but rows predating the
            // declared width, or arriving via a non-SQL ingest path, can still
            // be out of range, and those must surface as an error rather than
            // corrupt a value in flight.
            DdlColType::Int4 => {
                // `as i32` is lossless here: `checked_narrow` has already
                // proved the value is inside `IntWidth::I32`.
                return match checked_narrow(v, IntWidth::I32)? {
                    Some(n) => encoder.encode_field(&(n as i32)),
                    None => encoder.encode_field(&None::<i32>),
                };
            }
            DdlColType::Int2 => {
                return match checked_narrow(v, IntWidth::I16)? {
                    Some(n) => encoder.encode_field(&(n as i16)),
                    None => encoder.encode_field(&None::<i16>),
                };
            }
            DdlColType::Float8 => return encoder.encode_field(&v.as_f64()),
            // Unlike the integer arms above this is not a range *constraint*
            // check: narrowing an f64 rounds rather than wraps, so `1.1`
            // arriving as `1.10000002` is correct PostgreSQL `real` behaviour
            // and never an error. Only overflow-to-infinity is refused.
            DdlColType::Float4 => {
                return match checked_narrow_f32(v)? {
                    Some(f) => encoder.encode_field(&f),
                    None => encoder.encode_field(&None::<f32>),
                };
            }
            DdlColType::Bool => return encoder.encode_field(&v.as_bool()),
            DdlColType::Text | DdlColType::Varchar => {
                // TEXT/VARCHAR binary wire bytes are identical to text bytes,
                // so render any JSON scalar (numbers, bools, strings) to its
                // text form exactly as the text arm does, then emit as binary.
                return encoder.encode_field(&json_value_to_text(v));
            }
            // Feature-blocked / non-scalar types are downgraded to Text by the
            // format resolver and never reach here as Binary; if one somehow
            // does, fall through to the text arms below.
            _ => {}
        }
    }

    match ct {
        DdlColType::Float8 => match v {
            Value::Number(n) => match n.as_f64() {
                Some(f) => encoder.encode_field(&f),
                None => encoder.encode_field(&None::<f64>),
            },
            _ => encoder.encode_field(&json_value_to_text(v)),
        },
        // Same overflow guard as the binary arm: the text rendering of a
        // `real` column must not silently read `Infinity` for a finite stored
        // value either.
        DdlColType::Float4 => match v {
            Value::Number(_) => match checked_narrow_f32(v)? {
                Some(f) => encoder.encode_field(&f),
                None => encoder.encode_field(&None::<f32>),
            },
            _ => encoder.encode_field(&json_value_to_text(v)),
        },
        DdlColType::Timestamp | DdlColType::Timestamptz => match v {
            Value::Number(n) => match n.as_i64() {
                Some(micros) => {
                    encoder.encode_field(&NdbDateTime::from_micros(micros).to_iso8601())
                }
                None => encoder.encode_field(&json_value_to_text(v)),
            },
            _ => encoder.encode_field(&json_value_to_text(v)),
        },
        _ => encoder.encode_field(&json_value_to_text(v)),
    }
}

/// Build a `Response::Query` from a protocol-neutral [`ShapedRows`], plus its
/// carried client-facing notice.
///
/// Unlike `ddl_encode::rows_to_response` (which intentionally drops the
/// notice — the pgwire DDL router never attached one to a `Response::Query`),
/// this path preserves `notice`: the caller is expected to surface it via
/// `sessions.push_notice`.
pub(in crate::control::server::pgwire) fn shaped_query_response(
    shaped: ShapedRows,
    formats: &[FieldFormat],
) -> (Response, Option<String>) {
    // Cells live in the row maps under per-column keys that differ from the
    // display names only when two columns share a name; derived here before
    // the struct is destructured.
    let keys = shaped.cell_keys();
    let ShapedRows {
        columns,
        column_types,
        rows,
        notice,
    } = shaped;

    let fields: Vec<FieldInfo> = columns
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let ct = column_types.get(i).copied().unwrap_or(DdlColType::Text);
            let format = formats.get(i).copied().unwrap_or(FieldFormat::Text);
            col_type_to_field_with_format(name, ct, format)
        })
        .collect();
    let schema = Arc::new(fields);

    let encoded_rows: Vec<PgWireResult<DataRow>> = rows
        .iter()
        .map(|row| encode_shaped_row(&schema, &keys, &column_types, formats, row))
        .collect();

    let response = Response::Query(QueryResponse::new(
        schema,
        futures::stream::iter(encoded_rows),
    ));
    (response, notice)
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;
    use nodedb_types::Value;
    use pgwire::api::results::{QueryResponse, Response};

    use super::shaped_query_response;
    use crate::control::server::response_shape::types::{DdlColType, ShapedRow, ShapedRows};

    /// Drain a `QueryResponse` stream into a `Vec` of `DataRow`s.
    async fn drain(mut qr: QueryResponse) -> Vec<pgwire::messages::data::DataRow> {
        let mut rows = Vec::new();
        while let Some(r) = qr.data_rows.next().await {
            rows.push(r.unwrap());
        }
        rows
    }

    /// Read the text value of field `idx` from a `DataRow`'s raw wire buffer.
    ///
    /// Wire format: 4-byte big-endian length + bytes per field; a negative
    /// length denotes SQL NULL.
    fn field_text(row: &pgwire::messages::data::DataRow, idx: usize) -> Option<String> {
        let data = &row.data;
        let mut offset = 0usize;
        for field_i in 0..=idx {
            if offset + 4 > data.len() {
                return None;
            }
            let len = i32::from_be_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]);
            offset += 4;
            if len < 0 {
                if field_i == idx {
                    return None;
                }
                continue;
            }
            let len = len as usize;
            if offset + len > data.len() {
                return None;
            }
            if field_i == idx {
                return Some(
                    std::str::from_utf8(&data[offset..offset + len])
                        .unwrap()
                        .to_owned(),
                );
            }
            offset += len;
        }
        None
    }

    fn make_shaped(columns: &[&str], rows: Vec<ShapedRow>) -> ShapedRows {
        let columns: Vec<String> = columns.iter().map(|s| s.to_string()).collect();
        let column_types = ShapedRows::text_types(columns.len());
        ShapedRows {
            columns,
            column_types,
            rows,
            notice: None,
        }
    }

    fn obj(pairs: &[(&str, Value)]) -> ShapedRow {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    fn text(s: &str) -> Value {
        Value::String(s.to_string())
    }

    #[tokio::test]
    async fn string_cell_renders_verbatim() {
        let shaped = make_shaped(&["a"], vec![obj(&[("a", text("hello"))])]);
        let (response, notice) = shaped_query_response(shaped, &[]);
        assert!(notice.is_none());
        let Response::Query(qr) = response else {
            panic!("expected Query response");
        };
        let rows = drain(qr).await;
        assert_eq!(field_text(&rows[0], 0).as_deref(), Some("hello"));
    }

    #[tokio::test]
    async fn bool_cells_render_as_t_f_not_true_false() {
        let shaped = make_shaped(
            &["a"],
            vec![
                obj(&[("a", Value::Bool(true))]),
                obj(&[("a", Value::Bool(false))]),
            ],
        );
        let (response, _notice) = shaped_query_response(shaped, &[]);
        let Response::Query(qr) = response else {
            panic!("expected Query response");
        };
        let rows = drain(qr).await;
        assert_eq!(field_text(&rows[0], 0).as_deref(), Some("t"));
        assert_eq!(field_text(&rows[1], 0).as_deref(), Some("f"));
    }

    #[tokio::test]
    async fn number_cells_render_via_to_string() {
        let shaped = make_shaped(
            &["a"],
            vec![
                obj(&[("a", Value::Integer(42))]),
                obj(&[("a", Value::Float(0.0))]),
            ],
        );
        let (response, _notice) = shaped_query_response(shaped, &[]);
        let Response::Query(qr) = response else {
            panic!("expected Query response");
        };
        let rows = drain(qr).await;
        assert_eq!(field_text(&rows[0], 0).as_deref(), Some("42"));
        assert_eq!(field_text(&rows[1], 0).as_deref(), Some("0.0"));
    }

    #[tokio::test]
    async fn null_and_missing_column_both_encode_as_sql_null() {
        let shaped = make_shaped(&["a", "b"], vec![obj(&[("a", Value::Null)])]);
        let (response, _notice) = shaped_query_response(shaped, &[]);
        let Response::Query(qr) = response else {
            panic!("expected Query response");
        };
        let rows = drain(qr).await;
        // "a" was an explicit NULL cell.
        assert_eq!(field_text(&rows[0], 0), None);
        // "b" was entirely absent from the row object.
        assert_eq!(field_text(&rows[0], 1), None);
    }

    #[tokio::test]
    async fn column_order_is_preserved() {
        let shaped = make_shaped(
            &["b", "a"],
            vec![obj(&[("a", text("first")), ("b", text("second"))])],
        );
        let (response, _notice) = shaped_query_response(shaped, &[]);
        let Response::Query(qr) = response else {
            panic!("expected Query response");
        };
        let rows = drain(qr).await;
        assert_eq!(field_text(&rows[0], 0).as_deref(), Some("second"));
        assert_eq!(field_text(&rows[0], 1).as_deref(), Some("first"));
    }

    /// A user `SELECT` of typed columns on the simple-query path must report
    /// the correct RowDescription type OID AND render each cell in that type's
    /// PostgreSQL text form — the two halves that must land together.
    #[tokio::test]
    async fn typed_columns_report_correct_oid_and_text() {
        use pgwire::api::Type;

        let columns: Vec<String> = ["i", "f", "b", "ts"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        let column_types = vec![
            DdlColType::Int8,
            DdlColType::Float8,
            DdlColType::Bool,
            DdlColType::Timestamp,
        ];
        let row = obj(&[
            ("i", Value::Integer(42)),
            // Integral float renders Postgres-style "0" (shortest form) via the
            // native float encoder, not serde's "0.0".
            ("f", Value::Float(0.0)),
            ("b", Value::Bool(true)),
            // Epoch microseconds → ISO-8601 text (0 == Unix epoch).
            ("ts", Value::Integer(0)),
        ]);
        let shaped = ShapedRows {
            columns,
            column_types,
            rows: vec![row],
            notice: None,
        };

        let (response, _notice) = shaped_query_response(shaped, &[]);
        let Response::Query(qr) = response else {
            panic!("expected Query response");
        };
        // RowDescription OIDs are the typed ones, not TEXT.
        let schema = qr.row_schema.clone();
        assert_eq!(schema[0].datatype(), &Type::INT8);
        assert_eq!(schema[1].datatype(), &Type::FLOAT8);
        assert_eq!(schema[2].datatype(), &Type::BOOL);
        assert_eq!(schema[3].datatype(), &Type::TIMESTAMP);

        let rows = drain(qr).await;
        assert_eq!(field_text(&rows[0], 0).as_deref(), Some("42"));
        assert_eq!(field_text(&rows[0], 1).as_deref(), Some("0"));
        assert_eq!(field_text(&rows[0], 2).as_deref(), Some("t"));
        assert_eq!(
            field_text(&rows[0], 3).as_deref(),
            Some("1970-01-01T00:00:00.000000Z")
        );
    }

    /// An instant cell reaches the wire as its ISO-8601 text through the
    /// edge conversion, under a TEXT column and under a TIMESTAMP column
    /// alike, and a byte cell as the transcoder's unpadded base64.
    #[tokio::test]
    async fn instant_and_byte_cells_render_through_the_edge_conversion() {
        let at = nodedb_types::NdbDateTime::from_micros(1_583_402_400_000_000);
        let shaped = shaped_typed(
            &["t", "ts", "blob"],
            vec![DdlColType::Text, DdlColType::Timestamp, DdlColType::Text],
            obj(&[
                ("t", Value::NaiveDateTime(at)),
                ("ts", Value::NaiveDateTime(at)),
                ("blob", Value::Bytes(vec![0, 255, 7])),
            ]),
        );
        let (response, _notice) = shaped_query_response(shaped, &[]);
        let Response::Query(qr) = response else {
            panic!("expected Query response");
        };
        let rows = drain(qr).await;
        assert_eq!(
            field_text(&rows[0], 0).as_deref(),
            Some("2020-03-05T10:00:00.000000Z")
        );
        assert_eq!(
            field_text(&rows[0], 1).as_deref(),
            Some("2020-03-05T10:00:00.000000Z")
        );
        assert_eq!(field_text(&rows[0], 2).as_deref(), Some("AP8H"));
    }

    #[tokio::test]
    async fn notice_is_preserved_not_dropped() {
        let mut shaped = make_shaped(&["a"], vec![obj(&[("a", text("x"))])]);
        shaped.notice = Some("heads up".to_owned());
        let (_response, notice) = shaped_query_response(shaped, &[]);
        assert_eq!(notice.as_deref(), Some("heads up"));
    }

    /// Read the raw bytes of field `idx` from a `DataRow` (or `None` for SQL
    /// NULL), without assuming UTF-8 — used to inspect binary-format cells.
    fn field_bytes(row: &pgwire::messages::data::DataRow, idx: usize) -> Option<Vec<u8>> {
        let data = &row.data;
        let mut offset = 0usize;
        for field_i in 0..=idx {
            if offset + 4 > data.len() {
                return None;
            }
            let len = i32::from_be_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]);
            offset += 4;
            if len < 0 {
                if field_i == idx {
                    return None;
                }
                continue;
            }
            let len = len as usize;
            if offset + len > data.len() {
                return None;
            }
            if field_i == idx {
                return Some(data[offset..offset + len].to_vec());
            }
            offset += len;
        }
        None
    }

    fn shaped_typed(columns: &[&str], column_types: Vec<DdlColType>, row: ShapedRow) -> ShapedRows {
        ShapedRows {
            columns: columns.iter().map(|s| s.to_string()).collect(),
            column_types,
            rows: vec![row],
            notice: None,
        }
    }

    /// A binary-format request for the supported scalar types encodes each
    /// cell in its PostgreSQL binary wire form (big-endian), and the
    /// RowDescription advertises `FieldFormat::Binary`.
    #[tokio::test]
    async fn binary_format_encodes_scalar_wire_bytes() {
        use pgwire::api::results::FieldFormat;

        let shaped = shaped_typed(
            &["i", "f", "b", "t"],
            vec![
                DdlColType::Int8,
                DdlColType::Float8,
                DdlColType::Bool,
                DdlColType::Text,
            ],
            obj(&[
                ("i", Value::Integer(42)),
                ("f", Value::Float(1.5)),
                ("b", Value::Bool(true)),
                ("t", text("hello")),
            ]),
        );
        let formats = vec![FieldFormat::Binary; 4];
        let (response, _notice) = shaped_query_response(shaped, &formats);
        let Response::Query(qr) = response else {
            panic!("expected Query response");
        };
        // RowDescription advertises Binary for every column.
        for f in qr.row_schema.iter() {
            assert_eq!(f.format(), FieldFormat::Binary);
        }
        let rows = drain(qr).await;
        // int8 -> 8-byte big-endian.
        assert_eq!(field_bytes(&rows[0], 0), Some(42i64.to_be_bytes().to_vec()));
        // float8 -> IEEE-754 big-endian bits.
        assert_eq!(
            field_bytes(&rows[0], 1),
            Some(1.5f64.to_be_bytes().to_vec())
        );
        // bool -> single byte 0x01.
        assert_eq!(field_bytes(&rows[0], 2), Some(vec![1u8]));
        // text -> raw UTF-8 bytes (binary text is identical bytes).
        assert_eq!(field_bytes(&rows[0], 3), Some(b"hello".to_vec()));
    }

    /// A per-column format vector: only the columns whose format is Binary are
    /// binary-encoded; the rest stay text. Mirrors an `Individual` Bind.
    #[tokio::test]
    async fn mixed_formats_are_per_column() {
        use pgwire::api::results::FieldFormat;

        let shaped = shaped_typed(
            &["i", "j"],
            vec![DdlColType::Int8, DdlColType::Int8],
            obj(&[("i", Value::Integer(7)), ("j", Value::Integer(9))]),
        );
        let formats = vec![FieldFormat::Binary, FieldFormat::Text];
        let (response, _notice) = shaped_query_response(shaped, &formats);
        let Response::Query(qr) = response else {
            panic!("expected Query response");
        };
        let rows = drain(qr).await;
        // Column 0 binary: 8 raw bytes.
        assert_eq!(field_bytes(&rows[0], 0), Some(7i64.to_be_bytes().to_vec()));
        // Column 1 text: ASCII "9".
        assert_eq!(field_text(&rows[0], 1).as_deref(), Some("9"));
    }
}
