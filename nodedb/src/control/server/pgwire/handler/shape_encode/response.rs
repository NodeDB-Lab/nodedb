// SPDX-License-Identifier: BUSL-1.1

//! Encode a protocol-neutral [`ShapedRows`] (from
//! `response_shape::types`/`response_shape::project`) into a pgwire
//! `Response::Query`.
//!
//! This is the pgwire entrypoint's encoder for the canonical neutral shaping
//! core: the SELECT-read path builds a `ShapedRows` once and every protocol
//! entrypoint (pgwire, native, http) renders it in its own wire format. Here,
//! each typed cell renders per the `DdlColType` the shaper threaded through
//! `ShapedRows`, through the one cell encoder [`encode_cell`].

use std::sync::Arc;

use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response};
use pgwire::error::PgWireResult;
use pgwire::messages::data::DataRow;

use crate::control::server::pgwire::ddl_encode::col_type_to_field_with_format;
use crate::control::server::response_shape::types::{DdlColType, ShapedRow, ShapedRows};

use super::cell::encode_cell;

/// Encode one flat row object into a pgwire `DataRow`, using `cell_keys` (in
/// order) to look up cells in `row` and `column_types` (parallel to
/// `cell_keys`) to pick each cell's rendering.
///
/// `cell_keys` are the per-column unique row-map keys derived from the
/// display names via `response_shape::project::cell_keys` — identical to the
/// display names except where those repeat (`SELECT w.id, b.id`), in which
/// case later duplicates carry a `_n` suffix so both cells survive the map.
///
/// A missing key encodes as SQL NULL. Every present cell renders per its
/// column type via [`encode_cell`]; a missing/short `column_types` entry
/// defaults to `Text`.
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
        match row.get(name) {
            None => encoder.encode_field(&None::<&str>)?,
            Some(v) => encode_cell(&mut encoder, name, ct, format, v)?,
        }
    }
    Ok(encoder.take_row())
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
    use pgwire::api::results::{FieldFormat, QueryResponse, Response};
    use pgwire::error::PgWireError;

    use super::shaped_query_response;
    use crate::control::server::response_shape::types::{DdlColType, ShapedRow, ShapedRows};

    type DataRow = pgwire::messages::data::DataRow;

    /// Drain a `QueryResponse` stream into a `Vec` of per-row results.
    async fn drain_results(mut qr: QueryResponse) -> Vec<Result<DataRow, PgWireError>> {
        let mut rows = Vec::new();
        while let Some(r) = qr.data_rows.next().await {
            rows.push(r);
        }
        rows
    }

    /// Drain a `QueryResponse` stream into a `Vec` of `DataRow`s.
    async fn drain(qr: QueryResponse) -> Vec<DataRow> {
        drain_results(qr)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect()
    }

    /// Read the raw bytes of field `idx` from a `DataRow` (or `None` for SQL
    /// NULL), without assuming UTF-8 — used to inspect binary-format cells.
    ///
    /// Wire format: 4-byte big-endian length + bytes per field; a negative
    /// length denotes SQL NULL.
    fn field_bytes(row: &DataRow, idx: usize) -> Option<Vec<u8>> {
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

    /// Read the text value of field `idx` from a `DataRow`'s raw wire buffer.
    fn field_text(row: &DataRow, idx: usize) -> Option<String> {
        field_bytes(row, idx).map(|b| String::from_utf8(b).unwrap())
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

    fn shaped_typed(columns: &[&str], column_types: Vec<DdlColType>, row: ShapedRow) -> ShapedRows {
        ShapedRows {
            columns: columns.iter().map(|s| s.to_string()).collect(),
            column_types,
            rows: vec![row],
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

    fn query_of(response: Response) -> QueryResponse {
        let Response::Query(qr) = response else {
            panic!("expected Query response");
        };
        qr
    }

    #[tokio::test]
    async fn string_cell_renders_verbatim() {
        let shaped = make_shaped(&["a"], vec![obj(&[("a", text("hello"))])]);
        let (response, notice) = shaped_query_response(shaped, &[]);
        assert!(notice.is_none());
        let rows = drain(query_of(response)).await;
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
        let rows = drain(query_of(response)).await;
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
        let rows = drain(query_of(response)).await;
        assert_eq!(field_text(&rows[0], 0).as_deref(), Some("42"));
        assert_eq!(field_text(&rows[1], 0).as_deref(), Some("0.0"));
    }

    #[tokio::test]
    async fn null_and_missing_column_both_encode_as_sql_null() {
        let shaped = make_shaped(&["a", "b"], vec![obj(&[("a", Value::Null)])]);
        let (response, _notice) = shaped_query_response(shaped, &[]);
        let rows = drain(query_of(response)).await;
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
        let rows = drain(query_of(response)).await;
        assert_eq!(field_text(&rows[0], 0).as_deref(), Some("second"));
        assert_eq!(field_text(&rows[0], 1).as_deref(), Some("first"));
    }

    /// A user `SELECT` of typed columns on the simple-query path must report
    /// the correct RowDescription type OID AND render each cell in that type's
    /// PostgreSQL text form — the two halves that must land together.
    #[tokio::test]
    async fn typed_columns_report_correct_oid_and_text() {
        use pgwire::api::Type;

        let at = nodedb_types::NdbDateTime::from_micros(0);
        let shaped = shaped_typed(
            &["i", "f", "b", "ts"],
            vec![
                DdlColType::Int8,
                DdlColType::Float8,
                DdlColType::Bool,
                DdlColType::Timestamp,
            ],
            obj(&[
                ("i", Value::Integer(42)),
                // Integral float renders Postgres-style "0" (shortest form) via
                // the native float encoder, not serde's "0.0".
                ("f", Value::Float(0.0)),
                ("b", Value::Bool(true)),
                // The Unix epoch as a typed instant → ISO-8601 text.
                ("ts", Value::NaiveDateTime(at)),
            ]),
        );

        let (response, _notice) = shaped_query_response(shaped, &[]);
        let qr = query_of(response);
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

    /// An instant cell renders as its ISO-8601 text under a TEXT column and
    /// under a TIMESTAMP column alike, and a byte cell as unpadded base64.
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
        let rows = drain(query_of(response)).await;
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

    /// An ISO string under a TIMESTAMP column re-parses, so the row renders
    /// the canonical ISO-8601 form, not the stored spelling.
    #[tokio::test]
    async fn iso_text_under_timestamp_renders_canonical_iso8601() {
        let shaped = shaped_typed(
            &["ts"],
            vec![DdlColType::Timestamp],
            obj(&[("ts", text("2020-03-05 10:00:00"))]),
        );
        let (response, _notice) = shaped_query_response(shaped, &[]);
        let rows = drain(query_of(response)).await;
        assert_eq!(
            field_text(&rows[0], 0).as_deref(),
            Some("2020-03-05T10:00:00.000000Z")
        );
    }

    /// An integer under a TIMESTAMP column is a row-encode error naming the
    /// column — never a guessed epoch unit.
    #[tokio::test]
    async fn integer_under_timestamp_column_is_an_error_naming_the_column() {
        let shaped = shaped_typed(
            &["created_at"],
            vec![DdlColType::Timestamp],
            obj(&[("created_at", Value::Integer(1_583_402_400_000_000))]),
        );
        let (response, _notice) = shaped_query_response(shaped, &[]);
        let results = drain_results(query_of(response)).await;
        let err = results
            .into_iter()
            .next()
            .expect("one row")
            .expect_err("an integer under a timestamp column must not encode");
        let PgWireError::UserError(info) = err else {
            panic!("expected a UserError, got {err:?}");
        };
        assert!(
            info.message
                .contains("column \"created_at\" holds an integer where a timestamp is required"),
            "message must name the column, got: {}",
            info.message
        );
    }

    /// Binary `timestamp` cells are the big-endian `i64` of microseconds
    /// since the PostgreSQL epoch (2000-01-01), and the RowDescription
    /// advertises `FieldFormat::Binary`.
    #[tokio::test]
    async fn binary_timestamp_encodes_pg_epoch_micros() {
        let micros = 1_583_402_400_000_000i64;
        let at = nodedb_types::NdbDateTime::from_micros(micros);
        let shaped = shaped_typed(
            &["ts", "tstz"],
            vec![DdlColType::Timestamp, DdlColType::Timestamptz],
            obj(&[
                ("ts", Value::NaiveDateTime(at)),
                ("tstz", Value::DateTime(at)),
            ]),
        );
        let formats = vec![FieldFormat::Binary; 2];
        let (response, _notice) = shaped_query_response(shaped, &formats);
        let qr = query_of(response);
        for f in qr.row_schema.iter() {
            assert_eq!(f.format(), FieldFormat::Binary);
        }
        let rows = drain(qr).await;
        let expected = (micros - 946_684_800_000_000).to_be_bytes().to_vec();
        assert_eq!(field_bytes(&rows[0], 0), Some(expected.clone()));
        assert_eq!(field_bytes(&rows[0], 1), Some(expected));
    }

    #[tokio::test]
    async fn notice_is_preserved_not_dropped() {
        let mut shaped = make_shaped(&["a"], vec![obj(&[("a", text("x"))])]);
        shaped.notice = Some("heads up".to_owned());
        let (_response, notice) = shaped_query_response(shaped, &[]);
        assert_eq!(notice.as_deref(), Some("heads up"));
    }

    /// A binary-format request for the supported scalar types encodes each
    /// cell in its PostgreSQL binary wire form (big-endian), and the
    /// RowDescription advertises `FieldFormat::Binary`.
    #[tokio::test]
    async fn binary_format_encodes_scalar_wire_bytes() {
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
        let qr = query_of(response);
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
        let shaped = shaped_typed(
            &["i", "j"],
            vec![DdlColType::Int8, DdlColType::Int8],
            obj(&[("i", Value::Integer(7)), ("j", Value::Integer(9))]),
        );
        let formats = vec![FieldFormat::Binary, FieldFormat::Text];
        let (response, _notice) = shaped_query_response(shaped, &formats);
        let rows = drain(query_of(response)).await;
        // Column 0 binary: 8 raw bytes.
        assert_eq!(field_bytes(&rows[0], 0), Some(7i64.to_be_bytes().to_vec()));
        // Column 1 text: ASCII "9".
        assert_eq!(field_text(&rows[0], 1).as_deref(), Some("9"));
    }
}
