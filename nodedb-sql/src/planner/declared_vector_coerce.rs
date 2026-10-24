// SPDX-License-Identifier: Apache-2.0

//! Coerce an array literal bound for a declared `VECTOR(dim)` column into an
//! array of floats.
//!
//! A fractional literal resolves to [`SqlValue::Decimal`], and the Origin
//! planner serializes a non-integer decimal as a msgpack string. Left as is,
//! `ARRAY[0.1, 0.2]` reaches every engine as an array of strings. The strict
//! and columnar encoders refuse each string element, and the schemaless
//! engine stores the strings. Converting each element here gives every
//! engine and every write path the same array of floats.
//!
//! An integer, float, or decimal element becomes an `f64`. Any other element
//! is refused with [`SqlError::VectorElementNotNumeric`]. A string element is
//! refused even when its text spells a number, because the client wrote text.
//! A number outside the `f32` range a vector element holds is refused with
//! [`SqlError::FloatOutOfRange`].
//!
//! A value that is not an array passes through. The engines read a vector
//! text literal or raw vector bytes by their own rule. The element count is
//! checked by the engines, which own that error.

use nodedb_types::columnar::ColumnType;
use rust_decimal::prelude::ToPrimitive;

use crate::error::{Result, SqlError};
use crate::types::{ColumnInfo, SqlDataType, SqlValue};

/// The declared type name a vector range error reports.
const VECTOR_TYPE_NAME: &str = "VECTOR";

/// The dimension of `column` when it is declared `VECTOR(dim)`.
///
/// A strict or key-value column carries the dimension in its
/// [`SqlDataType::Vector`]. A schemaless or columnar-family column
/// advertises a vector as text, so its dimension comes from the declared
/// type text in [`ColumnInfo::raw_type`].
pub(crate) fn declared_vector_dim(column: &ColumnInfo) -> Option<usize> {
    if let SqlDataType::Vector(dim) = column.data_type {
        return Some(dim);
    }
    match column
        .raw_type
        .as_deref()
        .and_then(ColumnType::from_declared_type)
    {
        Some(ColumnType::Vector(dim)) => Some(dim as usize),
        _ => None,
    }
}

/// Coerce `value`, bound for the `VECTOR(dim)` column `column`.
///
/// An array becomes an array of [`SqlValue::Float`]. Every other value is
/// returned unchanged.
pub(crate) fn coerce_to_vector(column: &str, value: SqlValue, dim: usize) -> Result<SqlValue> {
    let SqlValue::Array(elements) = value else {
        return Ok(value);
    };
    elements
        .into_iter()
        .map(|element| vector_element(column, element, dim).map(SqlValue::Float))
        .collect::<Result<Vec<_>>>()
        .map(SqlValue::Array)
}

/// One vector element as the `f64` the column stores.
fn vector_element(column: &str, element: SqlValue, dim: usize) -> Result<f64> {
    let number = match element {
        SqlValue::Int(i) => i as f64,
        SqlValue::Float(f) => f,
        SqlValue::Decimal(d) => d.to_f64().ok_or_else(|| SqlError::TypeMismatch {
            detail: format!("column '{column}': '{d}' is not representable as VECTOR({dim})"),
        })?,
        other => {
            return Err(SqlError::VectorElementNotNumeric {
                column: column.to_string(),
                dim,
                element: element_description(&other),
            });
        }
    };
    if !number.is_finite() || (number as f32).is_infinite() {
        return Err(SqlError::FloatOutOfRange {
            column: column.to_string(),
            value: number,
            declared_type: VECTOR_TYPE_NAME,
        });
    }
    Ok(number)
}

/// How a refused element is named: its kind, and its text for a string.
fn element_description(element: &SqlValue) -> String {
    match element {
        SqlValue::String(s) => format!("text '{s}'"),
        SqlValue::Null => "null".to_string(),
        SqlValue::Bool(_) => "a boolean".to_string(),
        SqlValue::Bytes(_) => "bytes".to_string(),
        SqlValue::Array(_) => "a nested array".to_string(),
        SqlValue::Timestamp(_) => "a timestamp".to_string(),
        SqlValue::Timestamptz(_) => "a timestamptz".to_string(),
        SqlValue::Geometry(_) => "a geometry".to_string(),
        SqlValue::Int(_) => "an integer".to_string(),
        SqlValue::Float(_) => "a float".to_string(),
        SqlValue::Decimal(_) => "a decimal".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decimal(text: &str) -> SqlValue {
        SqlValue::Decimal(text.parse().expect("decimal literal"))
    }

    fn column(data_type: SqlDataType, raw_type: Option<&str>) -> ColumnInfo {
        ColumnInfo {
            name: "embedding".to_string(),
            data_type,
            nullable: true,
            is_primary_key: false,
            default: None,
            raw_type: raw_type.map(str::to_string),
            int_width: None,
            float_width: None,
        }
    }

    #[test]
    fn fractional_elements_become_floats() {
        let value = SqlValue::Array(vec![decimal("0.1"), decimal("0.2"), decimal("0.3")]);
        assert_eq!(
            coerce_to_vector("embedding", value, 3).expect("coerces"),
            SqlValue::Array(vec![
                SqlValue::Float(0.1),
                SqlValue::Float(0.2),
                SqlValue::Float(0.3),
            ])
        );
    }

    #[test]
    fn mixed_integer_and_fractional_elements_become_floats() {
        let value = SqlValue::Array(vec![SqlValue::Int(1), decimal("0.5"), SqlValue::Float(2.0)]);
        assert_eq!(
            coerce_to_vector("embedding", value, 3).expect("coerces"),
            SqlValue::Array(vec![
                SqlValue::Float(1.0),
                SqlValue::Float(0.5),
                SqlValue::Float(2.0),
            ])
        );
    }

    #[test]
    fn a_string_element_is_refused_naming_the_column() {
        for text in ["abc", "0.1"] {
            let value = SqlValue::Array(vec![decimal("0.1"), SqlValue::String(text.into())]);
            let error = coerce_to_vector("embedding", value, 2).expect_err("refused");
            assert_eq!(
                error,
                SqlError::VectorElementNotNumeric {
                    column: "embedding".into(),
                    dim: 2,
                    element: format!("text '{text}'"),
                }
            );
        }
    }

    #[test]
    fn null_bool_and_nested_elements_are_refused() {
        for element in [
            SqlValue::Null,
            SqlValue::Bool(true),
            SqlValue::Array(vec![SqlValue::Int(1)]),
        ] {
            let value = SqlValue::Array(vec![element]);
            assert!(matches!(
                coerce_to_vector("embedding", value, 1),
                Err(SqlError::VectorElementNotNumeric { .. })
            ));
        }
    }

    #[test]
    fn an_element_past_the_f32_range_is_refused() {
        let value = SqlValue::Array(vec![SqlValue::Float(1e39)]);
        assert!(matches!(
            coerce_to_vector("embedding", value, 1),
            Err(SqlError::FloatOutOfRange { .. })
        ));
    }

    #[test]
    fn a_non_array_value_passes_through() {
        for value in [
            SqlValue::Null,
            SqlValue::String("[0.1,0.2]".into()),
            SqlValue::Bytes(vec![0; 8]),
        ] {
            assert_eq!(
                coerce_to_vector("embedding", value.clone(), 2).expect("passes"),
                value
            );
        }
    }

    #[test]
    fn the_dimension_comes_from_the_type_or_the_declared_text() {
        assert_eq!(
            declared_vector_dim(&column(SqlDataType::Vector(3), None)),
            Some(3)
        );
        assert_eq!(
            declared_vector_dim(&column(SqlDataType::String, Some("VECTOR(4) NOT NULL"))),
            Some(4)
        );
        assert_eq!(
            declared_vector_dim(&column(SqlDataType::String, Some("TEXT"))),
            None
        );
        assert_eq!(
            declared_vector_dim(&column(SqlDataType::String, None)),
            None
        );
    }
}
