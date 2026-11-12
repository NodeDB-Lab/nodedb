// SPDX-License-Identifier: BUSL-1.1

//! Integer cells checked at the width their column announces.
//!
//! Every integer is an `i64` or an exact decimal at run time, and the width
//! lives in the announced type. An exact total can pass the `i64` range: a
//! `SUM` over `smallint` or `integer` input totals exactly and turns
//! decimal there, on every aggregate path. PostgreSQL types that `SUM`
//! `bigint` and raises "bigint out of range" past it, so a cell under an
//! integer column is checked here, after shaping and before any protocol
//! encodes it. A `SUM` over `bigint` is announced `numeric` and keeps its
//! decimal.

use nodedb_query::cast::IntWidth;
use nodedb_types::Value;

use super::types::{DdlColType, ShapedRow};

/// Fit each cell of `rows` under an integer column to that column's width.
///
/// `keys` and `column_types` are parallel. An integer cell in range stays.
/// A whole decimal in range becomes the integer. A cell past the range is
/// 22003 with PostgreSQL's message for the width ("bigint out of range").
/// Every other cell, and every cell of another column type, is unchanged.
pub(crate) fn fit_integer_cells(
    keys: &[String],
    column_types: &[DdlColType],
    rows: &mut [ShapedRow],
) -> crate::Result<()> {
    let integer_columns: Vec<(&String, IntWidth)> = keys
        .iter()
        .zip(column_types)
        .filter_map(|(key, ty)| int_width(*ty).map(|width| (key, width)))
        .collect();
    if integer_columns.is_empty() {
        return Ok(());
    }
    for row in rows {
        for (key, width) in &integer_columns {
            if let Some(cell) = row.get_mut(key.as_str()) {
                fit_cell(*width, cell)?;
            }
        }
    }
    Ok(())
}

/// The width of an integer column type. `None` for every other type.
fn int_width(ty: DdlColType) -> Option<IntWidth> {
    match ty {
        DdlColType::Int2 => Some(IntWidth::Int2),
        DdlColType::Int4 => Some(IntWidth::Int4),
        DdlColType::Int8 => Some(IntWidth::Int8),
        _ => None,
    }
}

fn fit_cell(width: IntWidth, cell: &mut Value) -> crate::Result<()> {
    match cell {
        Value::Integer(i) => {
            width.fit(*i)?;
        }
        Value::Decimal(d) if d.fract().is_zero() => {
            *cell = Value::Integer(nodedb_query::cast::numeric_to_int(*d, width)?);
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::*;

    fn rows_of(cells: Vec<Value>) -> Vec<ShapedRow> {
        cells
            .into_iter()
            .map(|cell| [("s".to_string(), cell)].into_iter().collect())
            .collect()
    }

    fn fit(ty: DdlColType, cells: Vec<Value>) -> crate::Result<Vec<Value>> {
        let mut rows = rows_of(cells);
        fit_integer_cells(&["s".to_string()], &[ty], &mut rows)?;
        Ok(rows.into_iter().filter_map(|mut r| r.remove("s")).collect())
    }

    #[test]
    fn a_bigint_cell_past_the_range_is_out_of_range() {
        let past = Decimal::from(i64::MAX) + Decimal::ONE;
        let err = fit(DdlColType::Int8, vec![Value::Decimal(past)]).unwrap_err();
        assert!(
            matches!(&err, crate::Error::NumericValueOutOfRange { detail } if detail == "bigint out of range"),
            "{err:?}"
        );
    }

    #[test]
    fn a_whole_decimal_in_range_becomes_an_integer() {
        assert_eq!(
            fit(
                DdlColType::Int8,
                vec![
                    Value::Decimal(Decimal::from(i64::MAX)),
                    Value::Integer(5),
                    Value::Null
                ]
            )
            .unwrap(),
            vec![Value::Integer(i64::MAX), Value::Integer(5), Value::Null]
        );
        assert!(
            fit(
                DdlColType::Int4,
                vec![Value::Integer(i64::from(i32::MAX) + 1)]
            )
            .is_err()
        );
    }

    #[test]
    fn numeric_and_text_columns_keep_their_cells() {
        let past = Decimal::from(i64::MAX) + Decimal::ONE;
        assert_eq!(
            fit(DdlColType::Numeric, vec![Value::Decimal(past)]).unwrap(),
            vec![Value::Decimal(past)]
        );
        assert_eq!(
            fit(DdlColType::Text, vec![Value::Decimal(past)]).unwrap(),
            vec![Value::Decimal(past)]
        );
    }
}
