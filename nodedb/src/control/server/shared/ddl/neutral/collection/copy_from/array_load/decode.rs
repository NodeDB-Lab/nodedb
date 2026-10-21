// SPDX-License-Identifier: BUSL-1.1

//! Typed decode and validation of one bulk-load row against an array schema.
//!
//! The columns are the array's dims in schema order, then its attrs in
//! schema order. Each value parses by its declared type and nothing else:
//! no type is guessed from the text. A row is checked for arity, type,
//! NOT NULL and the dim domain before any row of the load is written.

use std::borrow::Cow;

use nodedb_array::types::cell_value::value::CellValue;
use nodedb_sql::catalog::ArrayCatalogView;
use nodedb_sql::types_array::{
    ArrayAttrAst, ArrayAttrLiteral, ArrayAttrType, ArrayCoordLiteral, ArrayDimType,
    ArrayDomainBound, ArrayInsertRow,
};
use nodedb_types::Value;

use crate::control::planner::sql_plan_convert::array_convert::engine_attr_type;

/// A row a load refuses: its 1-based number, SQLSTATE and message.
#[derive(Debug)]
pub(super) struct RowError {
    pub row: usize,
    pub sqlstate: &'static str,
    pub message: String,
}

impl RowError {
    pub(super) fn new(row: usize, sqlstate: &'static str, message: String) -> Self {
        Self {
            row,
            sqlstate,
            message,
        }
    }
}

/// Upper bound of the msgpack bytes of one cell's fixed fields: the
/// six-field array header, the coordinate and attribute array headers, the
/// surrogate, and three timestamps.
const CELL_FIXED_BOUND: usize = 48;

/// Upper bound of the msgpack bytes of one coordinate or attribute value,
/// beyond its string or byte payload. zerompk writes an enum value as a
/// two-element array of the variant name and the payload. The longest name
/// is `TimestampMs`, and an `i64` or `f64` takes 9 bytes.
const SCALAR_BOUND: usize = 24;

/// Check a header record against the schema: dims, then attrs, by name.
pub(super) fn check_header(
    fields: &[Option<Cow<'_, str>>],
    view: &ArrayCatalogView,
    row: usize,
) -> Result<(), RowError> {
    let expected: Vec<&str> = view
        .dims
        .iter()
        .map(|d| d.name.as_str())
        .chain(view.attrs.iter().map(|a| a.name.as_str()))
        .collect();
    if fields.len() != expected.len() {
        return Err(RowError::new(
            row,
            "42703",
            format!(
                "header has {} columns, array '{}' has {} ({})",
                fields.len(),
                view.name,
                expected.len(),
                expected.join(", ")
            ),
        ));
    }
    for (index, (field, want)) in fields.iter().zip(&expected).enumerate() {
        let got = field.as_deref().unwrap_or("");
        if !got.trim().eq_ignore_ascii_case(want) {
            return Err(RowError::new(
                row,
                "42703",
                format!(
                    "header column {} is '{got}', array '{}' has '{want}' there",
                    index + 1,
                    view.name
                ),
            ));
        }
    }
    Ok(())
}

/// Decode one text record into a typed row.
pub(super) fn decode_text_row(
    fields: &[Option<Cow<'_, str>>],
    view: &ArrayCatalogView,
    row: usize,
) -> Result<ArrayInsertRow, RowError> {
    check_arity(fields.len(), view, row)?;
    let (dim_fields, attr_fields) = fields.split_at(view.dims.len());
    let mut coords = Vec::with_capacity(view.dims.len());
    for (field, dim) in dim_fields.iter().zip(&view.dims) {
        let text = field.as_deref().ok_or_else(|| null_dim(&dim.name, row))?;
        let coord = parse_coord(text, dim.dtype)
            .ok_or_else(|| invalid(&dim.name, dim_type_name(dim.dtype), text, row))?;
        check_domain(&coord, &dim.lo, &dim.hi, &dim.name, row)?;
        coords.push(coord);
    }
    let mut attrs = Vec::with_capacity(view.attrs.len());
    for (field, attr) in attr_fields.iter().zip(&view.attrs) {
        let value = match field.as_deref() {
            None if attr.nullable => ArrayAttrLiteral::Null,
            None => return Err(not_null(&attr.name, row)),
            Some(text) => {
                let literal = parse_attr(text, attr.dtype)
                    .ok_or_else(|| invalid(&attr.name, attr.dtype.sql_name(), text, row))?;
                check_width(&literal, attr, row)?;
                literal
            }
        };
        attrs.push(value);
    }
    Ok(ArrayInsertRow { coords, attrs })
}

/// Decode row `index` of native columns into a typed row. `dims` and
/// `attrs` hold one column per dim and per attr, in schema order.
pub(super) fn decode_value_row(
    dims: &[Vec<Value>],
    attrs: &[Vec<Value>],
    index: usize,
    view: &ArrayCatalogView,
) -> Result<ArrayInsertRow, RowError> {
    let row = index + 1;
    let mut coords = Vec::with_capacity(view.dims.len());
    for (column, dim) in dims.iter().zip(&view.dims) {
        let value = column.get(index).unwrap_or(&Value::Null);
        let coord = match (value, dim.dtype) {
            (Value::Null, _) => return Err(null_dim(&dim.name, row)),
            (Value::Integer(n), ArrayDimType::Int64) => ArrayCoordLiteral::Int64(*n),
            (Value::Integer(n), ArrayDimType::TimestampMs) => ArrayCoordLiteral::Int64(*n),
            (Value::Float(f), ArrayDimType::Float64) => ArrayCoordLiteral::Float64(*f),
            (Value::Integer(n), ArrayDimType::Float64) => ArrayCoordLiteral::Float64(*n as f64),
            (Value::String(s), ArrayDimType::String) => ArrayCoordLiteral::String(s.clone()),
            (other, dtype) => {
                return Err(mismatch(&dim.name, dim_type_name(dtype), other, row));
            }
        };
        check_domain(&coord, &dim.lo, &dim.hi, &dim.name, row)?;
        coords.push(coord);
    }
    let mut out = Vec::with_capacity(view.attrs.len());
    for (column, attr) in attrs.iter().zip(&view.attrs) {
        let value = column.get(index).unwrap_or(&Value::Null);
        let literal = match (value, attr.dtype) {
            (Value::Null, _) if attr.nullable => ArrayAttrLiteral::Null,
            (Value::Null, _) => return Err(not_null(&attr.name, row)),
            (Value::Integer(n), dtype) if dtype.is_integer() => ArrayAttrLiteral::Int64(*n),
            (Value::Integer(n), dtype) if dtype.is_float() => ArrayAttrLiteral::Float64(*n as f64),
            (Value::Float(f), dtype) if dtype.is_float() => ArrayAttrLiteral::Float64(*f),
            (Value::String(s), ArrayAttrType::String) => ArrayAttrLiteral::String(s.clone()),
            (Value::Bytes(b), ArrayAttrType::Bytes) => ArrayAttrLiteral::Bytes(b.clone()),
            (other, dtype) => {
                return Err(mismatch(&attr.name, dtype.sql_name(), other, row));
            }
        };
        check_width(&literal, attr, row)?;
        out.push(literal);
    }
    Ok(ArrayInsertRow { coords, attrs: out })
}

/// Upper bound of the msgpack bytes the row's `ArrayPutCell` encodes to.
pub(super) fn wire_bound(row: &ArrayInsertRow) -> usize {
    let coords: usize = row
        .coords
        .iter()
        .map(|c| {
            SCALAR_BOUND
                + match c {
                    ArrayCoordLiteral::String(s) => s.len(),
                    ArrayCoordLiteral::Int64(_) | ArrayCoordLiteral::Float64(_) => 0,
                }
        })
        .sum();
    let attrs: usize = row
        .attrs
        .iter()
        .map(|a| {
            SCALAR_BOUND
                + match a {
                    ArrayAttrLiteral::String(s) => s.len(),
                    ArrayAttrLiteral::Bytes(b) => b.len(),
                    ArrayAttrLiteral::Int64(_)
                    | ArrayAttrLiteral::Float64(_)
                    | ArrayAttrLiteral::Null => 0,
                }
        })
        .sum();
    CELL_FIXED_BOUND + coords + attrs
}

fn check_arity(got: usize, view: &ArrayCatalogView, row: usize) -> Result<(), RowError> {
    let want = view.dims.len() + view.attrs.len();
    if got == want {
        return Ok(());
    }
    Err(RowError::new(
        row,
        "22P02",
        format!(
            "row has {got} columns, array '{}' has {want} ({} dims, {} attrs)",
            view.name,
            view.dims.len(),
            view.attrs.len()
        ),
    ))
}

fn parse_coord(text: &str, dtype: ArrayDimType) -> Option<ArrayCoordLiteral> {
    match dtype {
        ArrayDimType::Int64 | ArrayDimType::TimestampMs => {
            text.trim().parse().ok().map(ArrayCoordLiteral::Int64)
        }
        ArrayDimType::Float64 => text.trim().parse().ok().map(ArrayCoordLiteral::Float64),
        ArrayDimType::String => Some(ArrayCoordLiteral::String(text.to_string())),
    }
}

fn parse_attr(text: &str, dtype: ArrayAttrType) -> Option<ArrayAttrLiteral> {
    match dtype {
        ArrayAttrType::Int64
        | ArrayAttrType::Int8
        | ArrayAttrType::UInt8
        | ArrayAttrType::Int16
        | ArrayAttrType::UInt16
        | ArrayAttrType::Int32
        | ArrayAttrType::UInt32 => text.trim().parse().ok().map(ArrayAttrLiteral::Int64),
        ArrayAttrType::Float64 | ArrayAttrType::Float32 => {
            text.trim().parse().ok().map(ArrayAttrLiteral::Float64)
        }
        ArrayAttrType::String => Some(ArrayAttrLiteral::String(text.to_string())),
        ArrayAttrType::Bytes => parse_bytea_hex(text).map(ArrayAttrLiteral::Bytes),
    }
}

/// A number outside its attr's declared width is `22003`, checked here so
/// the refusal names its row.
fn check_width(
    literal: &ArrayAttrLiteral,
    attr: &ArrayAttrAst,
    row: usize,
) -> Result<(), RowError> {
    let dtype = engine_attr_type(attr.dtype);
    let narrowed = match literal {
        ArrayAttrLiteral::Int64(n) => CellValue::from_int_literal(*n, dtype),
        ArrayAttrLiteral::Float64(f) => CellValue::from_float_literal(*f, dtype),
        ArrayAttrLiteral::String(_) | ArrayAttrLiteral::Bytes(_) | ArrayAttrLiteral::Null => {
            return Ok(());
        }
    };
    match narrowed {
        Ok(_) => Ok(()),
        Err(e) if e.is_out_of_range() => Err(RowError::new(
            row,
            "22003",
            format!("attr '{}': {e}", attr.name),
        )),
        Err(e) => Err(RowError::new(
            row,
            "42804",
            format!("attr '{}': {e}", attr.name),
        )),
    }
}

/// PostgreSQL `bytea` hex form: `\x` then pairs of hex digits.
fn parse_bytea_hex(text: &str) -> Option<Vec<u8>> {
    let hex = text.trim().strip_prefix("\\x")?;
    if !hex.len().is_multiple_of(2) {
        return None;
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(hex.get(i..i + 2)?, 16).ok())
        .collect()
}

/// A coordinate outside its dim's inclusive domain is `22003`. A bound of
/// another type than the coordinate never holds it.
fn check_domain(
    coord: &ArrayCoordLiteral,
    lo: &ArrayDomainBound,
    hi: &ArrayDomainBound,
    dim: &str,
    row: usize,
) -> Result<(), RowError> {
    let inside = match (coord, lo, hi) {
        (
            ArrayCoordLiteral::Int64(v),
            ArrayDomainBound::Int64(lo) | ArrayDomainBound::TimestampMs(lo),
            ArrayDomainBound::Int64(hi) | ArrayDomainBound::TimestampMs(hi),
        ) => lo <= v && v <= hi,
        (
            ArrayCoordLiteral::Float64(v),
            ArrayDomainBound::Float64(lo),
            ArrayDomainBound::Float64(hi),
        ) => *lo <= *v && *v <= *hi,
        (
            ArrayCoordLiteral::String(v),
            ArrayDomainBound::String(lo),
            ArrayDomainBound::String(hi),
        ) => lo <= v && v <= hi,
        _ => false,
    };
    if inside {
        return Ok(());
    }
    Err(RowError::new(
        row,
        "22003",
        format!(
            "coordinate {} for dim '{dim}' is outside its domain [{}..{}]",
            coord_text(coord),
            bound_text(lo),
            bound_text(hi)
        ),
    ))
}

fn null_dim(dim: &str, row: usize) -> RowError {
    RowError::new(row, "23502", format!("dim '{dim}' cannot be NULL"))
}

fn not_null(attr: &str, row: usize) -> RowError {
    RowError::new(row, "23502", format!("attr '{attr}' is NOT NULL"))
}

fn invalid(column: &str, dtype: &str, text: &str, row: usize) -> RowError {
    RowError::new(
        row,
        "22P02",
        format!("invalid input for {dtype} column '{column}': '{text}'"),
    )
}

fn mismatch(column: &str, dtype: &str, value: &Value, row: usize) -> RowError {
    RowError::new(
        row,
        "22P02",
        format!("column '{column}' is {dtype}, the cell holds {value:?}"),
    )
}

fn dim_type_name(dtype: ArrayDimType) -> &'static str {
    match dtype {
        ArrayDimType::Int64 => "INT64",
        ArrayDimType::Float64 => "FLOAT64",
        ArrayDimType::TimestampMs => "TIMESTAMP",
        ArrayDimType::String => "STRING",
    }
}

fn coord_text(coord: &ArrayCoordLiteral) -> String {
    match coord {
        ArrayCoordLiteral::Int64(v) => v.to_string(),
        ArrayCoordLiteral::Float64(v) => v.to_string(),
        ArrayCoordLiteral::String(v) => format!("'{v}'"),
    }
}

pub(super) fn bound_text(bound: &ArrayDomainBound) -> String {
    match bound {
        ArrayDomainBound::Int64(v) | ArrayDomainBound::TimestampMs(v) => v.to_string(),
        ArrayDomainBound::Float64(v) => v.to_string(),
        ArrayDomainBound::String(v) => format!("'{v}'"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_sql::types_array::{ArrayAttrAst, ArrayDimAst};

    fn view() -> ArrayCatalogView {
        ArrayCatalogView {
            name: "scene".into(),
            georef: None,
            dims: vec![ArrayDimAst {
                name: "y".into(),
                dtype: ArrayDimType::Int64,
                lo: ArrayDomainBound::Int64(0),
                hi: ArrayDomainBound::Int64(255),
            }],
            attrs: vec![
                ArrayAttrAst {
                    name: "b1".into(),
                    dtype: ArrayAttrType::Float64,
                    nullable: false,
                    meta: Default::default(),
                },
                ArrayAttrAst {
                    name: "label".into(),
                    dtype: ArrayAttrType::String,
                    nullable: true,
                    meta: Default::default(),
                },
            ],
            tile_extents: vec![64],
        }
    }

    fn fields(values: &[Option<&'static str>]) -> Vec<Option<Cow<'static, str>>> {
        values.iter().map(|v| v.map(Cow::Borrowed)).collect()
    }

    #[test]
    fn a_row_decodes_by_declared_type() {
        let row =
            decode_text_row(&fields(&[Some("7"), Some("1.5"), None]), &view(), 1).expect("decode");
        assert_eq!(row.coords, vec![ArrayCoordLiteral::Int64(7)]);
        assert_eq!(
            row.attrs,
            vec![ArrayAttrLiteral::Float64(1.5), ArrayAttrLiteral::Null]
        );
        assert!(wire_bound(&row) >= CELL_FIXED_BOUND + 3 * SCALAR_BOUND);
    }

    #[test]
    fn refusals_carry_their_sqlstate() {
        let cases: [(&[Option<&'static str>], &str); 4] = [
            (&[Some("7"), Some("1.5")], "22P02"),
            (&[Some("7"), Some("abc"), None], "22P02"),
            (&[Some("7"), None, None], "23502"),
            (&[Some("256"), Some("1.0"), None], "22003"),
        ];
        for (input, sqlstate) in cases {
            let err = decode_text_row(&fields(input), &view(), 9).expect_err("refused");
            assert_eq!(err.sqlstate, sqlstate, "{input:?}");
            assert_eq!(err.row, 9);
        }
    }

    #[test]
    fn a_header_must_name_dims_then_attrs() {
        assert!(check_header(&fields(&[Some("y"), Some("B1"), Some("label")]), &view(), 1).is_ok());
        let err = check_header(&fields(&[Some("y"), Some("label"), Some("b1")]), &view(), 1)
            .expect_err("mismatch");
        assert_eq!(err.sqlstate, "42703");
    }

    #[test]
    fn native_cells_decode_and_check_types() {
        let dims = vec![vec![Value::Integer(3), Value::Integer(4)]];
        let attrs = vec![
            vec![Value::Float(0.5), Value::String("x".into())],
            vec![Value::Null, Value::Null],
        ];
        let row = decode_value_row(&dims, &attrs, 0, &view()).expect("decode");
        assert_eq!(row.coords, vec![ArrayCoordLiteral::Int64(3)]);
        let err = decode_value_row(&dims, &attrs, 1, &view()).expect_err("string in float");
        assert_eq!(err.sqlstate, "22P02");
        assert_eq!(err.row, 2);
    }

    fn narrow_view() -> ArrayCatalogView {
        let attr = |name: &str, dtype| ArrayAttrAst {
            name: name.into(),
            dtype,
            nullable: true,
            meta: Default::default(),
        };
        ArrayCatalogView {
            name: "scene".into(),
            georef: None,
            dims: vec![ArrayDimAst {
                name: "y".into(),
                dtype: ArrayDimType::Int64,
                lo: ArrayDomainBound::Int64(0),
                hi: ArrayDomainBound::Int64(255),
            }],
            attrs: vec![
                attr("q", ArrayAttrType::UInt8),
                attr("b1", ArrayAttrType::UInt16),
                attr("t", ArrayAttrType::Int16),
                attr("ndvi", ArrayAttrType::Float32),
            ],
            tile_extents: vec![64],
        }
    }

    /// Text and native rows narrow by declared width: an in-range value
    /// loads, and one past the width is `22003` with its row number.
    #[test]
    fn narrow_attrs_load_in_range_and_refuse_past_it() {
        let ok = decode_text_row(
            &fields(&[
                Some("1"),
                Some("255"),
                Some("65535"),
                Some("-32768"),
                Some("0.1"),
            ]),
            &narrow_view(),
            1,
        )
        .expect("in range");
        assert_eq!(ok.attrs[1], ArrayAttrLiteral::Int64(65535));
        let cases: [&[Option<&'static str>]; 5] = [
            &[Some("1"), Some("256"), None, None, None],
            &[Some("1"), Some("-1"), None, None, None],
            &[Some("1"), None, Some("65536"), None, None],
            &[Some("1"), None, None, Some("32768"), None],
            &[Some("1"), None, None, None, Some("1e39")],
        ];
        for input in cases {
            let err = decode_text_row(&fields(input), &narrow_view(), 4).expect_err("out of range");
            assert_eq!(err.sqlstate, "22003", "{input:?}");
            assert_eq!(err.row, 4);
        }
        let float_into_int = decode_text_row(
            &fields(&[Some("1"), Some("1.5"), None, None, None]),
            &narrow_view(),
            2,
        )
        .expect_err("float text into UINT8");
        assert_eq!(float_into_int.sqlstate, "22P02");

        let dims = vec![vec![Value::Integer(3)]];
        let attrs = vec![
            vec![Value::Integer(300)],
            vec![Value::Null],
            vec![Value::Null],
            vec![Value::Float(0.5)],
        ];
        let err = decode_value_row(&dims, &attrs, 0, &narrow_view()).expect_err("300 > UINT8");
        assert_eq!(err.sqlstate, "22003");
    }

    #[test]
    fn bytea_hex_parses() {
        assert_eq!(parse_bytea_hex("\\x0aFF"), Some(vec![0x0a, 0xff]));
        assert_eq!(parse_bytea_hex("0aff"), None);
    }
}
