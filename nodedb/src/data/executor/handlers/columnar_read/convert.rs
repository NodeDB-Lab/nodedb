// SPDX-License-Identifier: BUSL-1.1

//! Value conversions: a decoded columnar row → its projected response
//! object, and timeseries columnar cell → raw msgpack for the timeseries scan
//! path.

use std::collections::HashMap;

use nodedb_types::value::Value;

use crate::data::executor::handlers::columnar_write::render_system_ms;

/// Project a decoded columnar row into the scan's response object: column
/// projection, the forced `_ts_system` audit column, and computed
/// (scalar-expression) columns. Shared by the flushed-segment scan, the base
/// memtable scan loop, the in-transaction overlay merge
/// (`merge_overlay_into_columnar_scan`), and the predicate DML row read, so a
/// staged row's object is built identically to a base row's.
///
/// Every cell is carried as the `Value` the row reader typed it as, so a
/// declared `TIMESTAMP` / `TIMESTAMPTZ` cell is an instant here and is
/// written as the instant ext by `value_to_msgpack`, from the live memtable
/// and from a flushed segment alike. The bitemporal `_ts_system`,
/// `_ts_valid_from`, `_ts_valid_until` columns are declared `Int64` and stay
/// integers, which `i64::MIN` / `i64::MAX` unbounded sentinels need.
/// `_ts_system` stores a system-time ordinal and reads as its epoch
/// millisecond (`render_system_ms`). The valid-time columns hold epoch
/// milliseconds.
///
/// A computed column evaluates over the whole stored row. Its expression can
/// read a column the projection leaves out, as `ST_AsText(loc)` does beside a
/// projected `name`. The output then keeps the projected columns and the
/// computed aliases only. A computed alias that is also projected keeps the
/// projected value, as the document projection does. Any other computed
/// alias carries the evaluated value, even when a stored column shares its
/// name.
pub(in crate::data::executor) fn row_to_projected_value(
    row: &[Value],
    schema: &nodedb_types::columnar::ColumnarSchema,
    projection: &[String],
    computed_cols: &[crate::bridge::expr_eval::ComputedColumn],
    all_versions: bool,
    crs: &nodedb_crs::CrsContext<'_>,
) -> crate::Result<Value> {
    let keep_every_column = projection.is_empty() || !computed_cols.is_empty();
    let mut obj: HashMap<String, Value> = HashMap::with_capacity(schema.columns.len());
    for (i, col_def) in schema.columns.iter().enumerate() {
        let force_system_col =
            all_versions && col_def.name == nodedb_types::columnar::schema::TS_SYSTEM;
        if !keep_every_column && !force_system_col && !projection.iter().any(|p| p == &col_def.name)
        {
            continue;
        }
        if let Some(cell) = row.get(i) {
            obj.insert(col_def.name.clone(), render_system_ms(&col_def.name, cell));
        }
    }
    if !computed_cols.is_empty() {
        let doc_val = Value::Object(obj.clone());
        for cc in computed_cols {
            if projection.iter().any(|p| p == &cc.alias) {
                continue;
            }
            // A computed column is projection-shaped: a division/modulo-by-
            // zero fails the whole scan rather than silently materializing
            // NULL into the response row.
            let v = cc.expr.eval(&doc_val, crs)?;
            obj.insert(cc.alias.clone(), v);
        }
        if !projection.is_empty() {
            obj.retain(|k, _| {
                projection.iter().any(|p| p == k)
                    || computed_cols.iter().any(|cc| &cc.alias == k)
                    || (all_versions && k == nodedb_types::columnar::schema::TS_SYSTEM)
            });
        }
    }
    Ok(Value::Object(obj))
}

/// A decoded columnar row as the object a user reads, holding every stored
/// column (`row_values_to_user_object`).
pub(in crate::data::executor) fn row_to_value(
    row: &[Value],
    schema: &nodedb_types::columnar::ColumnarSchema,
) -> Value {
    crate::data::executor::handlers::columnar_write::row_values_to_user_object(schema, row)
}

/// Write a timeseries columnar memtable cell value directly as msgpack bytes.
///
/// Encodes the column value at the given row index directly into `buf`
/// without intermediate decoding. Used by timeseries raw_scan and aggregate
/// handlers that still use the internal `ColumnarMemtable`.
///
/// A time cell is written as its column's kind says: an instant column
/// yields a typed instant ext, a `Millis` column the integer stored. `Err`
/// when a stored millisecond count overflows the microsecond range an
/// instant carries; the cell is never written wrapped. `Err` when the cell
/// does not read as its declared type, never a written NULL.
pub(in crate::data::executor) fn emit_column_value(
    buf: &mut Vec<u8>,
    mt: &crate::engine::timeseries::columnar_memtable::ColumnarMemtable,
    col_idx: usize,
    col_type: &crate::engine::timeseries::columnar_memtable::ColumnType,
    col_data: &crate::engine::timeseries::columnar_memtable::ColumnData,
    row_idx: usize,
) -> crate::Result<()> {
    use crate::data::executor::handlers::timeseries::cell_read::{TsCell, read_ts_cell};
    let column = mt
        .schema()
        .columns
        .get(col_idx)
        .map_or("", |(name, _)| name.as_str());
    match read_ts_cell(
        col_data,
        *col_type,
        column,
        mt.symbol_dict(col_idx),
        row_idx,
    )? {
        TsCell::Time(kind, millis) => write_time_cell(buf, kind, millis)?,
        TsCell::Float(v) if v.is_finite() => nodedb_query::msgpack_scan::write_f64(buf, v),
        TsCell::Float(_) | TsCell::Null => nodedb_query::msgpack_scan::write_null(buf),
        TsCell::Symbol(s) => nodedb_query::msgpack_scan::write_str(buf, s),
        TsCell::Int(n) => nodedb_query::msgpack_scan::write_i64(buf, n),
    }
    Ok(())
}

/// Epoch microseconds for a stored millisecond count.
///
/// `Err` when `millis * 1000` overflows `i64`: the stored value cannot be
/// expressed as an instant, and wrapping it would hand back a different one.
fn instant_micros(millis: i64) -> crate::Result<i64> {
    nodedb_types::NdbDateTime::from_millis(millis)
        .map(|dt| dt.micros)
        .map_err(|e| crate::Error::Internal {
            detail: format!("timeseries time cell at {millis} ms: {e}"),
        })
}

/// Write a stored millisecond time cell as the value its kind denotes.
pub(in crate::data::executor) fn write_time_cell(
    buf: &mut Vec<u8>,
    kind: crate::engine::timeseries::columnar_memtable::TimeKind,
    millis: i64,
) -> crate::Result<()> {
    use crate::engine::timeseries::columnar_memtable::TimeKind;
    match kind {
        TimeKind::Instant(k) => nodedb_types::write_instant(buf, k, instant_micros(millis)?),
        TimeKind::Millis => nodedb_query::msgpack_scan::write_i64(buf, millis),
    }
    Ok(())
}

/// The rmpv cell for a stored millisecond time value, typed by its kind.
///
/// An instant column yields the ten-byte instant ext, a `Millis` column the
/// integer stored.
pub(in crate::data::executor) fn rmpv_time_cell(
    kind: crate::engine::timeseries::columnar_memtable::TimeKind,
    millis: i64,
) -> crate::Result<rmpv::Value> {
    use crate::engine::timeseries::columnar_memtable::TimeKind;
    Ok(match kind {
        TimeKind::Instant(k) => {
            rmpv::Value::Ext(k.ext_type(), instant_micros(millis)?.to_be_bytes().to_vec())
        }
        TimeKind::Millis => rmpv::Value::Integer(millis.into()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::timeseries::columnar_memtable::{
        ColumnData, ColumnType, ColumnarMemtable, ColumnarMemtableConfig, ColumnarSchema, TimeKind,
    };
    use nodedb_types::InstantKind;

    const MS: i64 = 1_583_402_400_000;

    fn memtable(kind: TimeKind) -> ColumnarMemtable {
        let schema = ColumnarSchema {
            columns: vec![
                ("ts".into(), ColumnType::Timestamp(kind)),
                ("v".into(), ColumnType::Float64),
            ],
            timestamp_idx: 0,
            codecs: vec![nodedb_codec::ColumnCodec::Auto; 2],
        };
        ColumnarMemtable::new(schema, ColumnarMemtableConfig::default())
    }

    fn emit(kind: TimeKind, millis: i64) -> crate::Result<Vec<u8>> {
        let mt = memtable(kind);
        let data = ColumnData::Timestamp(vec![millis]);
        let mut buf = Vec::new();
        emit_column_value(&mut buf, &mt, 0, &ColumnType::Timestamp(kind), &data, 0)?;
        Ok(buf)
    }

    #[test]
    fn an_instant_column_emits_a_typed_instant_ext() {
        let buf = emit(TimeKind::Instant(InstantKind::Naive), MS).expect("emit");
        assert_eq!(
            nodedb_types::read_instant(&buf, 0),
            Some((InstantKind::Naive, MS * 1000))
        );
        let buf = emit(TimeKind::Instant(InstantKind::Utc), MS).expect("emit");
        assert_eq!(
            nodedb_types::read_instant(&buf, 0),
            Some((InstantKind::Utc, MS * 1000))
        );
    }

    #[test]
    fn a_millis_column_emits_the_integer_stored() {
        let buf = emit(TimeKind::Millis, MS).expect("emit");
        let mut expected = Vec::new();
        nodedb_query::msgpack_scan::write_i64(&mut expected, MS);
        assert_eq!(buf, expected);
    }

    /// A projected row carries a declared `TIMESTAMP` cell as the instant the
    /// row reader typed it, and the `_ts_system` audit column as the integer
    /// millisecond of the ordinal it stores.
    #[test]
    fn a_projected_row_keeps_instant_cells_and_integer_system_time() {
        use nodedb_types::NdbDateTime;
        use nodedb_types::columnar::schema::TS_SYSTEM;
        use nodedb_types::columnar::{ColumnDef, ColumnType};

        let schema = nodedb_types::columnar::ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("at", ColumnType::Timestamp),
            ColumnDef::required(TS_SYSTEM, ColumnType::Int64),
        ])
        .expect("valid schema");
        let at = Value::NaiveDateTime(NdbDateTime::from_micros(MS * 1000));
        // An ordinal inside millisecond `MS`.
        let ordinal = MS * nodedb_types::NANOS_PER_MS + 7;
        let row = [Value::Integer(1), at.clone(), Value::Integer(ordinal)];

        let Value::Object(all) = row_to_projected_value(
            &row,
            &schema,
            &[],
            &[],
            false,
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect("project") else {
            panic!("a projected row is an object");
        };
        assert_eq!(all.get("at"), Some(&at));
        assert_eq!(all.get(TS_SYSTEM), Some(&Value::Integer(MS)));

        let projection = ["at".to_string()];
        let Value::Object(audit) = row_to_projected_value(
            &row,
            &schema,
            &projection,
            &[],
            true,
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect("project") else {
            panic!("a projected row is an object");
        };
        assert_eq!(audit.get("at"), Some(&at));
        assert_eq!(
            audit.get(TS_SYSTEM),
            Some(&Value::Integer(MS)),
            "an all-versions read forces the system-time column into the projection"
        );
        assert!(!audit.contains_key("id"));
    }

    /// An `(id, name, city)` schema and one row, for the computed-column
    /// projection tests.
    fn name_city_row() -> (nodedb_types::columnar::ColumnarSchema, [Value; 3]) {
        use nodedb_types::columnar::{ColumnDef, ColumnType};
        let schema = nodedb_types::columnar::ColumnarSchema::new(vec![
            ColumnDef::required("id", ColumnType::Int64).with_primary_key(),
            ColumnDef::required("name", ColumnType::String),
            ColumnDef::required("city", ColumnType::String),
        ])
        .expect("valid schema");
        let row = [
            Value::Integer(1),
            Value::String("ali".into()),
            Value::String("temerloh".into()),
        ];
        (schema, row)
    }

    fn upper(alias: &str, column: &str) -> crate::bridge::expr_eval::ComputedColumn {
        use crate::bridge::expr_eval::{ComputedColumn, SqlExpr};
        ComputedColumn {
            alias: alias.into(),
            expr: SqlExpr::Function {
                name: "upper".into(),
                args: vec![SqlExpr::Column(column.into())],
            },
        }
    }

    /// A computed column reads a stored column the projection leaves out.
    /// The output carries the projected column and the computed alias only.
    #[test]
    fn a_computed_column_reads_a_column_outside_the_projection() {
        let (schema, row) = name_city_row();
        let projection = ["name".to_string()];
        let computed = [upper("upper(city)", "city")];
        let Value::Object(obj) = row_to_projected_value(
            &row,
            &schema,
            &projection,
            &computed,
            false,
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect("project") else {
            panic!("a projected row is an object");
        };
        assert_eq!(obj.get("name"), Some(&Value::String("ali".into())));
        assert_eq!(
            obj.get("upper(city)"),
            Some(&Value::String("TEMERLOH".into())),
            "the computed column must see `city` although only `name` is projected"
        );
        assert!(!obj.contains_key("city"), "an unprojected column stays out");
        assert!(!obj.contains_key("id"), "an unprojected column stays out");
    }

    /// A computed alias that names a stored column carries the evaluated
    /// value, never the stored one.
    #[test]
    fn a_computed_alias_shadowing_a_stored_column_carries_the_computed_value() {
        let (schema, row) = name_city_row();
        let computed = [upper("name", "name")];
        let Value::Object(obj) = row_to_projected_value(
            &row,
            &schema,
            &[],
            &computed,
            false,
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .expect("project") else {
            panic!("a projected row is an object");
        };
        assert_eq!(obj.get("name"), Some(&Value::String("ALI".into())));
    }

    #[test]
    fn a_millisecond_count_past_the_microsecond_range_is_an_error() {
        let err = emit(TimeKind::Instant(InstantKind::Naive), i64::MAX)
            .expect_err("i64::MAX ms cannot be expressed in microseconds");
        assert!(err.to_string().contains("time cell"), "{err}");
        assert!(emit(TimeKind::Millis, i64::MAX).is_ok());
    }
}
