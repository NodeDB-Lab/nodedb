// SPDX-License-Identifier: BUSL-1.1

//! Shaping for DML `RETURNING` responses.
//!
//! A `RETURNING` payload is a [`RowsPayload`]: the Data Plane's own column list
//! plus typed `Value` cells. For `RETURNING *` that column list is
//! derived from the STORED row, so a schemaless collection can carry fields no
//! catalog column declares — the list is only knowable once the rows exist.
//!
//! # The announced column list wins
//!
//! The extended query protocol answers `Describe` with a RowDescription BEFORE
//! any row exists, and pgwire sends no second RowDescription with the DataRows.
//! The two column lists are therefore decided in different places at different
//! times, and a row-derived list that disagrees with the announced one is not
//! parseable by the client at all ("DataRow field count does not match the
//! number of columns") — the statement fails outright.
//!
//! So whatever was announced is what this shaper is held to: when the caller
//! passes the statement's resolved output columns, the payload's rows are
//! projected onto exactly those columns BY NAME. A column the row does not
//! carry encodes as SQL NULL; a field the row carries that was never announced
//! is not shipped. The field count equals the announced column count *by
//! construction* rather than by coincidence, and every cell stays under the
//! name it was stored with — no padding, no truncation, no re-alignment.
//!
//! The simple-query protocol announces the same list: the planner derives a
//! DML plan's `OutputSchema` from its `RETURNING` column list, so a named
//! clause is held to those columns and their catalog types, and a returned
//! cell renders exactly as the same column renders under `SELECT`.
//!
//! `RETURNING *` announces `is_star` instead, and a star keeps the row-derived
//! list. The concrete columns of a star are only knowable once the rows exist —
//! a schemaless row carries fields no catalog column declares — which is the
//! same answer `SELECT *` gives for the same row.

use nodedb_types::{NativeCell, NdbDateTime, NodeDbError, Value};

use crate::data::executor::response_codec::{RowsPayload, decode_payload_to_json};

use super::compose::kernel::{project_row, redact_rows, single_result_row};
use super::project::cell_keys;
use super::redaction::RedactionCtx;
use super::schema::OutputSchema;
use super::types::{DdlColType, ShapedRow, ShapedRows};

/// Shape a DML-with-`RETURNING` response.
///
/// `projection` carries the columns already announced to the client for this
/// statement, when any were. See the module docs for why they win over the
/// payload's own list.
pub fn shape_returning_rows(
    payload: &[u8],
    projection: Option<&OutputSchema>,
    redaction: Option<RedactionCtx<'_>>,
) -> Result<ShapedRows, NodeDbError> {
    let announced = announced_columns(projection);

    if payload.is_empty() {
        return Ok(match announced {
            Some(schema) => empty_announced(schema),
            None => single_result_column_empty(),
        });
    }

    let rp = match zerompk::from_msgpack::<RowsPayload>(payload) {
        Ok(rp) => rp,
        Err(e) => {
            // Bytes that are not a `RowsPayload` yield no column list at all,
            // so there is none that honours what was announced: a substitute
            // single-column row would be unparseable to the client that holds
            // the RowDescription, which is the very failure this module
            // exists to prevent. Fail the statement instead.
            if announced.is_some() {
                return Err(NodeDbError::serialization(
                    "msgpack",
                    format!(
                        "RETURNING response is not a RowsPayload ({} bytes): {e}",
                        payload.len()
                    ),
                ));
            }
            tracing::warn!(
                error = %e,
                payload_len = payload.len(),
                "ReturningRows msgpack decode failed; falling back to single-column JSON"
            );
            return Ok(single_result_row(decode_payload_to_json(payload)));
        }
    };

    let RowsPayload { columns, rows } = rp;
    let mut rows = rows_keyed_by_column(&columns, rows);
    // `RETURNING` delivers stored column values to the client just as a SELECT
    // does, so the same redaction applies — and it runs on the payload's own
    // names, before any projection renames or drops them.
    redact_rows(redaction.as_ref(), &mut rows);

    let Some(schema) = announced else {
        let column_types = ShapedRows::text_types(columns.len());
        return Ok(ShapedRows::from_rows(columns, column_types, rows));
    };
    Ok(project_onto_announced(schema, &rows))
}

/// The announced output columns, or `None` when the caller announced nothing
/// concrete (no projection, a star, or an empty column list) and the payload's
/// own list therefore stands.
fn announced_columns(projection: Option<&OutputSchema>) -> Option<&OutputSchema> {
    projection.filter(|schema| !schema.is_star && !schema.columns.is_empty())
}

/// Re-key each payload row from positional typed cells to a name-keyed row.
/// A cell keeps its `Value` form: an instant stays an instant, `Value::Null`
/// is SQL NULL, numbers and booleans are themselves. Each protocol renders
/// the cell at its own edge.
fn rows_keyed_by_column(columns: &[String], rows: Vec<Vec<NativeCell>>) -> Vec<ShapedRow> {
    rows.into_iter()
        .map(|row_vals| {
            let mut map = ShapedRow::new();
            for (col, cell) in columns.iter().zip(row_vals) {
                map.insert(col.clone(), cell.0);
            }
            map
        })
        .collect()
}

/// Project name-keyed rows onto the announced columns, in announced order.
///
/// Uses the same `project_row` + `cell_keys` pair the SELECT path uses, so a
/// `RETURNING` result and a `SELECT` result of the same columns are laid out
/// identically for the encoders.
fn project_onto_announced(schema: &OutputSchema, rows: &[ShapedRow]) -> ShapedRows {
    let lookup_keys: Vec<String> = schema
        .columns
        .iter()
        .map(|c| c.lookup_key.clone())
        .collect();
    let display_names: Vec<String> = schema
        .columns
        .iter()
        .map(|c| c.display_name.clone())
        .collect();
    let column_types: Vec<DdlColType> = schema.columns.iter().map(|c| c.ty).collect();
    let keys = cell_keys(&display_names);

    let projected = rows
        .iter()
        .map(|row| {
            let mut out = project_row(row, &lookup_keys, &display_names, &keys);
            for (key, ct) in keys.iter().zip(column_types.iter()) {
                if let Some(cell) = out.get_mut(key) {
                    retype_cell(*ct, cell);
                }
            }
            out
        })
        .collect();

    ShapedRows::from_rows(display_names, column_types, projected)
}

/// Re-read a text `RETURNING` cell as the column's announced type.
///
/// A typed cell is already a number, bool or instant and passes through. A
/// text cell under a numeric, bool or timestamp column is a stored string
/// (a schemaless row can hold `"42"` under a declared `INT`), and the
/// RowDescription this response is held to announces the catalog type — a
/// client that asked for BINARY result format is handed the scalar's wire
/// bytes, which have to come from a number, a bool or an instant, not from
/// the digits of its text form.
///
/// A cell that does not parse as its announced type is left as text. The
/// numeric and bool encoders render such text verbatim; a timestamp column
/// refuses it, because a cell under a timestamp column renders only from an
/// instant.
fn retype_cell(ct: DdlColType, cell: &mut Value) {
    let Value::String(text) = cell else {
        return;
    };
    let retyped = match ct {
        DdlColType::Int8 | DdlColType::Int4 | DdlColType::Int2 => {
            text.parse::<i64>().ok().map(Value::Integer)
        }
        // ISO-8601 text becomes the instant it denotes; an integer string
        // carries no unit and stays text.
        DdlColType::Timestamp => NdbDateTime::parse(text).map(Value::NaiveDateTime),
        DdlColType::Timestamptz => NdbDateTime::parse(text).map(Value::DateTime),
        // A float that parses but is not finite has no JSON form, so it stays
        // text.
        DdlColType::Float8 | DdlColType::Float4 => text
            .parse::<f64>()
            .ok()
            .filter(|f| f.is_finite())
            .map(Value::Float),
        DdlColType::Bool => match text.as_str() {
            "t" | "true" | "TRUE" | "T" => Some(Value::Bool(true)),
            "f" | "false" | "FALSE" | "F" => Some(Value::Bool(false)),
            _ => None,
        },
        DdlColType::Text
        | DdlColType::Varchar
        | DdlColType::Bytea
        | DdlColType::Json
        | DdlColType::Jsonb
        | DdlColType::Float4Array
        | DdlColType::Float8Array => None,
    };
    if let Some(value) = retyped {
        *cell = value;
    }
}

/// The announced columns with no rows — a write that matched nothing still
/// answers with the result set the client was promised, just an empty one.
fn empty_announced(schema: &OutputSchema) -> ShapedRows {
    ShapedRows::from_rows(
        schema
            .columns
            .iter()
            .map(|c| c.display_name.clone())
            .collect(),
        schema.columns.iter().map(|c| c.ty).collect(),
        Vec::new(),
    )
}

/// Single "result" column with zero rows, for an empty payload with nothing
/// announced.
fn single_result_column_empty() -> ShapedRows {
    ShapedRows::from_rows(
        vec!["result".to_string()],
        ShapedRows::text_types(1),
        Vec::new(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::server::response_shape::cell::value_to_wire_json;
    use crate::control::server::response_shape::schema::OutputColumn;

    fn text(s: &str) -> Value {
        Value::String(s.to_string())
    }

    fn typed_payload(columns: &[&str], rows: Vec<Vec<Value>>) -> Vec<u8> {
        let rp = RowsPayload {
            columns: columns.iter().map(|c| (*c).to_string()).collect(),
            rows: rows
                .into_iter()
                .map(|row| row.into_iter().map(NativeCell).collect())
                .collect(),
        };
        zerompk::to_msgpack_vec(&rp).expect("encode RowsPayload")
    }

    /// Text cells, the form a schemaless row's string fields arrive in;
    /// `None` is SQL NULL.
    fn payload(columns: &[&str], rows: &[&[Option<&str>]]) -> Vec<u8> {
        typed_payload(
            columns,
            rows.iter()
                .map(|row| {
                    row.iter()
                        .map(|cell| match cell {
                            Some(text) => Value::String((*text).to_string()),
                            None => Value::Null,
                        })
                        .collect()
                })
                .collect(),
        )
    }

    fn announced(columns: &[(&str, DdlColType)]) -> OutputSchema {
        OutputSchema {
            columns: columns
                .iter()
                .map(|(name, ty)| OutputColumn {
                    display_name: (*name).to_string(),
                    lookup_key: (*name).to_string(),
                    ty: *ty,
                })
                .collect(),
            is_star: false,
        }
    }

    /// Nothing announced: the payload's own column list stands, which is what
    /// the simple-query protocol needs — it sends the RowDescription built
    /// from this same `ShapedRows`.
    #[test]
    fn without_a_projection_the_payload_columns_stand() {
        let bytes = payload(
            &["id", "name", "score"],
            &[&[Some("a"), Some("x"), Some("1")]],
        );
        let shaped = shape_returning_rows(&bytes, None, None).expect("shape");
        assert_eq!(shaped.columns, ["id", "name", "score"]);
        assert_eq!(shaped.rows.len(), 1);
    }

    /// A row carrying a field nobody announced must not widen the result: the
    /// extended-query client already holds a RowDescription without it.
    #[test]
    fn an_undeclared_row_field_is_not_shipped() {
        let bytes = payload(
            &["id", "name", "extra"],
            &[&[Some("a"), Some("x"), Some("surprise")]],
        );
        let schema = announced(&[("id", DdlColType::Text), ("name", DdlColType::Text)]);
        let shaped = shape_returning_rows(&bytes, Some(&schema), None).expect("shape");
        assert_eq!(shaped.columns, ["id", "name"]);
        assert_eq!(shaped.column_types.len(), shaped.columns.len());
        let keys = shaped.cell_keys();
        assert_eq!(shaped.rows[0].len(), keys.len(), "one cell per column");
        assert_eq!(shaped.rows[0]["id"], text("a"));
        assert_eq!(shaped.rows[0]["name"], text("x"));
    }

    /// An announced column the row does not carry is SQL NULL, in its
    /// announced position — never a shifted neighbour.
    #[test]
    fn a_missing_announced_column_is_null_in_place() {
        let bytes = payload(&["id", "score"], &[&[Some("a"), Some("7")]]);
        let schema = announced(&[
            ("id", DdlColType::Text),
            ("name", DdlColType::Text),
            ("score", DdlColType::Int8),
        ]);
        let shaped = shape_returning_rows(&bytes, Some(&schema), None).expect("shape");
        assert_eq!(shaped.columns, ["id", "name", "score"]);
        assert_eq!(shaped.rows[0]["id"], text("a"));
        assert_eq!(shaped.rows[0]["name"], Value::Null);
        assert_eq!(shaped.rows[0]["score"], Value::Integer(7));
    }

    /// Cells are re-read as the announced type so a binary-format request is
    /// encoded from a real scalar rather than from the digits of its text.
    #[test]
    fn cells_are_retyped_to_the_announced_column_type() {
        let bytes = payload(
            &["i", "f", "b", "s"],
            &[&[Some("42"), Some("1.5"), Some("t"), Some("42")]],
        );
        let schema = announced(&[
            ("i", DdlColType::Int8),
            ("f", DdlColType::Float8),
            ("b", DdlColType::Bool),
            ("s", DdlColType::Text),
        ]);
        let shaped = shape_returning_rows(&bytes, Some(&schema), None).expect("shape");
        assert_eq!(shaped.rows[0]["i"], Value::Integer(42));
        assert_eq!(shaped.rows[0]["f"], Value::Float(1.5));
        assert_eq!(shaped.rows[0]["b"], Value::Bool(true));
        // A TEXT column keeps its text even when it looks like a number.
        assert_eq!(shaped.rows[0]["s"], text("42"));
    }

    /// ISO-8601 text under a timestamp column becomes the instant it denotes,
    /// naive under `TIMESTAMP` and UTC under `TIMESTAMPTZ`; an integer string
    /// carries no unit and stays text.
    #[test]
    fn timestamp_text_is_retyped_to_an_instant() {
        let at = NdbDateTime::from_micros(1_583_402_400_000_000);
        let bytes = payload(
            &["ts", "tstz", "digits"],
            &[&[
                Some("2020-03-05 10:00:00"),
                Some("2020-03-05T10:00:00Z"),
                Some("1583402400000000"),
            ]],
        );
        let schema = announced(&[
            ("ts", DdlColType::Timestamp),
            ("tstz", DdlColType::Timestamptz),
            ("digits", DdlColType::Timestamp),
        ]);
        let shaped = shape_returning_rows(&bytes, Some(&schema), None).expect("shape");
        assert_eq!(shaped.rows[0]["ts"], Value::NaiveDateTime(at));
        assert_eq!(shaped.rows[0]["tstz"], Value::DateTime(at));
        assert_eq!(shaped.rows[0]["digits"], text("1583402400000000"));
    }

    /// A typed cell passes through as itself: an integer stays a number under
    /// an INT column, an instant stays an instant (and renders at the edge as
    /// the ISO-8601 text a `SELECT` of the same column renders), and SQL NULL
    /// stays NULL.
    #[test]
    fn typed_cells_reach_the_row_as_themselves() {
        let at = NdbDateTime::from_micros(1_583_402_400_000_000);
        let bytes = typed_payload(
            &["n", "at", "f", "gone"],
            vec![vec![
                Value::Integer(7),
                Value::NaiveDateTime(at),
                Value::Float(1.5),
                Value::Null,
            ]],
        );
        let schema = announced(&[
            ("n", DdlColType::Int8),
            ("at", DdlColType::Timestamp),
            ("f", DdlColType::Float8),
            ("gone", DdlColType::Text),
        ]);
        let shaped = shape_returning_rows(&bytes, Some(&schema), None).expect("shape");
        assert_eq!(shaped.rows[0]["n"], Value::Integer(7));
        assert_eq!(shaped.rows[0]["at"], Value::NaiveDateTime(at));
        assert_eq!(
            value_to_wire_json(&shaped.rows[0]["at"]),
            serde_json::Value::String("2020-03-05T10:00:00.000000Z".into())
        );
        assert_eq!(shaped.rows[0]["f"], Value::Float(1.5));
        assert_eq!(shaped.rows[0]["gone"], Value::Null);
    }

    /// The same instant stays typed when nothing was announced, and renders
    /// as ISO-8601 at the edge, so the simple-query protocol shows what the
    /// extended one does.
    #[test]
    fn an_instant_cell_renders_iso8601_without_a_projection() {
        let at = NdbDateTime::from_micros(1_583_402_400_000_000);
        let bytes = typed_payload(&["at"], vec![vec![Value::DateTime(at)]]);
        let shaped = shape_returning_rows(&bytes, None, None).expect("shape");
        assert_eq!(shaped.rows[0]["at"], Value::DateTime(at));
        assert_eq!(
            value_to_wire_json(&shaped.rows[0]["at"]),
            serde_json::Value::String("2020-03-05T10:00:00.000000Z".into())
        );
    }

    /// A value that does not parse as its announced type stays text rather
    /// than becoming NULL — the encoder renders it verbatim.
    #[test]
    fn an_unparseable_cell_stays_text() {
        let bytes = payload(&["i"], &[&[Some("not a number")]]);
        let schema = announced(&[("i", DdlColType::Int8)]);
        let shaped = shape_returning_rows(&bytes, Some(&schema), None).expect("shape");
        assert_eq!(shaped.rows[0]["i"], text("not a number"));
    }

    /// A write that matched nothing still answers with the announced result
    /// set, empty.
    #[test]
    fn an_empty_payload_keeps_the_announced_columns() {
        let schema = announced(&[("id", DdlColType::Text), ("score", DdlColType::Int8)]);
        let shaped = shape_returning_rows(&[], Some(&schema), None).expect("shape");
        assert_eq!(shaped.columns, ["id", "score"]);
        assert!(shaped.rows.is_empty());
    }

    /// A payload with no rows still reports the announced columns, so the
    /// DataRow count of zero is read against the right RowDescription.
    #[test]
    fn a_rowless_payload_keeps_the_announced_columns() {
        let bytes = payload(&[], &[]);
        let schema = announced(&[("id", DdlColType::Text)]);
        let shaped = shape_returning_rows(&bytes, Some(&schema), None).expect("shape");
        assert_eq!(shaped.columns, ["id"]);
        assert!(shaped.rows.is_empty());
    }

    /// Bytes that are not a `RowsPayload` cannot honour the announced columns,
    /// so the statement fails instead of shipping a substitute column the
    /// client was never told about.
    #[test]
    fn a_malformed_payload_fails_when_columns_were_announced() {
        let schema = announced(&[("id", DdlColType::Text)]);
        assert!(shape_returning_rows(&[0xFF, 0xFE], Some(&schema), None).is_err());
    }

    /// With nothing announced there is no contract to violate, so the legacy
    /// single-column fallback still applies.
    #[test]
    fn a_malformed_payload_falls_back_when_nothing_was_announced() {
        let shaped = shape_returning_rows(&[0xFF, 0xFE], None, None).expect("fallback");
        assert_eq!(shaped.columns, ["result"]);
    }
}
