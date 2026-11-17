// SPDX-License-Identifier: BUSL-1.1

//! The cells a line field stores in a DECIMAL, UUID or BOOLEAN column.
//!
//! These columns take only input of their own type, read exactly:
//!
//! - DECIMAL takes an integer field or a string field holding decimal text.
//!   A float field is refused: its text was already rounded to an `f64`
//!   when the line was parsed, so it cannot be stored exactly.
//! - UUID takes a string field holding a UUID.
//! - BOOLEAN takes a boolean field.
//!
//! Anything else is a [`DeclaredCellError`], never a coerced value.

use nodedb_types::Value;
use rust_decimal::Decimal;

use super::columnar_memtable::{ColumnType, ColumnValue, TsDecimalTypmod};
use super::ilp::FieldValue;

/// A field that a DECIMAL, UUID or BOOLEAN column does not take.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DeclaredCellError {
    /// A string field whose text does not parse as the declared type.
    #[error("column '{column}': cannot parse '{text}' as {declared}")]
    InvalidText {
        column: String,
        text: String,
        declared: &'static str,
    },
    /// A field of a kind the declared type does not hold.
    #[error("column '{column}': a {kind} field cannot be stored in a {declared} column{hint}")]
    WrongKind {
        column: String,
        kind: &'static str,
        declared: &'static str,
        hint: &'static str,
    },
    /// A decimal past its column's declared precision: SQLSTATE `22003`,
    /// as the strict and columnar engines answer it.
    #[error("{detail}")]
    OutOfRange { detail: String },
    /// The column's stored typmod is not a typmod: a corrupt schema.
    #[error("{detail}")]
    Internal { detail: String },
}

impl From<DeclaredCellError> for crate::Error {
    fn from(error: DeclaredCellError) -> Self {
        match error {
            DeclaredCellError::InvalidText { .. } => crate::Error::InvalidTextRepresentation {
                detail: error.to_string(),
            },
            DeclaredCellError::WrongKind { .. } => crate::Error::DatatypeMismatch {
                detail: error.to_string(),
            },
            DeclaredCellError::OutOfRange { detail } => {
                crate::Error::NumericValueOutOfRange { detail }
            }
            DeclaredCellError::Internal { detail } => crate::Error::Internal { detail },
        }
    }
}

/// Whether `column_type` takes only exact input of its own type.
pub fn is_declared_exact(column_type: ColumnType) -> bool {
    matches!(
        column_type,
        ColumnType::Decimal(_) | ColumnType::Uuid | ColumnType::Bool
    )
}

/// The cell `value` stores in `column` of `column_type`. `Ok(None)` when
/// `column_type` is not a DECIMAL, UUID or BOOLEAN column.
pub fn declared_cell(
    column: &str,
    column_type: ColumnType,
    value: &FieldValue<'_>,
) -> Result<Option<ColumnValue>, DeclaredCellError> {
    let wrong_kind = |declared: &'static str, hint: &'static str| DeclaredCellError::WrongKind {
        column: column.to_string(),
        kind: field_kind(value),
        declared,
        hint,
    };
    let cell = match column_type {
        ColumnType::Decimal(typmod) => match value {
            FieldValue::Int(i) => {
                ColumnValue::decimal(fit_declared_decimal(column, typmod, Decimal::from(*i))?)
            }
            FieldValue::UInt(u) => {
                ColumnValue::decimal(fit_declared_decimal(column, typmod, Decimal::from(*u))?)
            }
            FieldValue::Str(text) => {
                let d = parse_decimal(text).ok_or_else(|| DeclaredCellError::InvalidText {
                    column: column.to_string(),
                    text: text.to_string(),
                    declared: "DECIMAL",
                })?;
                ColumnValue::decimal(fit_declared_decimal(column, typmod, d)?)
            }
            FieldValue::Float(_) => {
                return Err(wrong_kind(
                    "DECIMAL",
                    "; send the value as decimal text to store it exactly",
                ));
            }
            FieldValue::Bool(_) => return Err(wrong_kind("DECIMAL", "")),
        },
        ColumnType::Uuid => match value {
            FieldValue::Str(text) => match uuid::Uuid::parse_str(text.trim()) {
                Ok(id) => ColumnValue::Uuid(*id.as_bytes()),
                Err(_) => {
                    return Err(DeclaredCellError::InvalidText {
                        column: column.to_string(),
                        text: text.to_string(),
                        declared: "UUID",
                    });
                }
            },
            _ => return Err(wrong_kind("UUID", "")),
        },
        ColumnType::Bool => match value {
            FieldValue::Bool(b) => ColumnValue::Bool(*b),
            _ => return Err(wrong_kind("BOOLEAN", "")),
        },
        ColumnType::Timestamp(_)
        | ColumnType::Float64
        | ColumnType::Int64
        | ColumnType::Symbol
        | ColumnType::Text => return Ok(None),
    };
    Ok(Some(cell))
}

/// The typed value a `Value` cell takes in `column` of `column_type`, as a
/// committed row reads it: a DECIMAL as `Value::Decimal` fitted to its
/// typmod, a UUID as `Value::Uuid` in canonical text, a BOOLEAN as
/// `Value::Bool`. NULL stays NULL. `Ok(None)` when `column_type` is not a
/// DECIMAL, UUID or BOOLEAN column.
///
/// A float bound for a DECIMAL reads as its shortest decimal text, the form
/// the ingest normalization carries it in.
pub fn declared_value(
    column: &str,
    column_type: ColumnType,
    value: &Value,
) -> Result<Option<Value>, DeclaredCellError> {
    let wrong_kind = |declared: &'static str| DeclaredCellError::WrongKind {
        column: column.to_string(),
        kind: value.type_name(),
        declared,
        hint: "",
    };
    let invalid = |text: &str, declared: &'static str| DeclaredCellError::InvalidText {
        column: column.to_string(),
        text: text.to_string(),
        declared,
    };
    if matches!(value, Value::Null) && is_declared_exact(column_type) {
        return Ok(Some(Value::Null));
    }
    let typed = match column_type {
        ColumnType::Decimal(typmod) => {
            let d = match value {
                Value::Decimal(d) => *d,
                Value::Integer(i) => Decimal::from(*i),
                Value::String(text) => {
                    parse_decimal(text).ok_or_else(|| invalid(text, "DECIMAL"))?
                }
                Value::Float(f) => {
                    let text = f.to_string();
                    parse_decimal(&text).ok_or_else(|| invalid(&text, "DECIMAL"))?
                }
                _ => return Err(wrong_kind("DECIMAL")),
            };
            Value::Decimal(fit_declared_decimal(column, typmod, d)?)
        }
        ColumnType::Uuid => match value {
            Value::Uuid(text) | Value::String(text) => match uuid::Uuid::parse_str(text.trim()) {
                Ok(id) => Value::Uuid(id.to_string()),
                Err(_) => return Err(invalid(text, "UUID")),
            },
            _ => return Err(wrong_kind("UUID")),
        },
        ColumnType::Bool => match value {
            Value::Bool(b) => Value::Bool(*b),
            _ => return Err(wrong_kind("BOOLEAN")),
        },
        ColumnType::Timestamp(_)
        | ColumnType::Float64
        | ColumnType::Int64
        | ColumnType::Symbol
        | ColumnType::Text => return Ok(None),
    };
    Ok(Some(typed))
}

/// The memtable cell of a value [`declared_value`] typed. `None` for a NULL
/// or any other value.
pub fn typed_value_cell(value: &Value) -> Option<ColumnValue> {
    match value {
        Value::Decimal(d) => Some(ColumnValue::decimal(*d)),
        Value::Uuid(text) => uuid::Uuid::parse_str(text)
            .ok()
            .map(|id| ColumnValue::Uuid(*id.as_bytes())),
        Value::Bool(b) => Some(ColumnValue::Bool(*b)),
        _ => None,
    }
}

/// `d` fitted to `column`'s declared `DECIMAL(p,s)` typmod, by the one rule
/// the strict and columnar engines apply
/// ([`crate::data::executor::strict_format::coerce_value`]): rounded to `s`
/// fractional digits half away from zero, and refused with SQLSTATE
/// `22003` when it has more than `p - s` integer digits. A plain `DECIMAL`
/// keeps `d` as it is.
pub fn fit_declared_decimal(
    column: &str,
    typmod: Option<TsDecimalTypmod>,
    d: Decimal,
) -> Result<Decimal, DeclaredCellError> {
    let Some(typmod) = typmod else {
        return Ok(d);
    };
    let declared = typmod.typmod().map_err(|e| DeclaredCellError::Internal {
        detail: format!("column '{column}': the stored DECIMAL typmod is invalid: {e}"),
    })?;
    let def = nodedb_types::columnar::ColumnDef::nullable(
        column,
        nodedb_types::columnar::ColumnType::Decimal(Some(declared)),
    );
    match crate::data::executor::strict_format::coerce_value(&Value::Decimal(d), &def) {
        Ok(Value::Decimal(fitted)) => Ok(fitted),
        Ok(other) => Err(DeclaredCellError::Internal {
            detail: format!("column '{column}': DECIMAL coercion returned {other:?}"),
        }),
        Err(crate::Error::NumericValueOutOfRange { detail }) => {
            Err(DeclaredCellError::OutOfRange { detail })
        }
        Err(other) => Err(DeclaredCellError::Internal {
            detail: format!("column '{column}': {other}"),
        }),
    }
}

/// Decimal text read exactly: plain notation, or scientific notation such as
/// `1.5e3`. `None` for text that is not a decimal, or one that does not fit
/// a `Decimal` without rounding.
fn parse_decimal(text: &str) -> Option<Decimal> {
    let text = text.trim();
    if text.contains(['e', 'E']) {
        return Decimal::from_scientific(text).ok();
    }
    Decimal::from_str_exact(text).ok()
}

/// The ILP type name of `value`.
pub fn field_kind(value: &FieldValue<'_>) -> &'static str {
    match value {
        FieldValue::Float(_) => "float",
        FieldValue::Int(_) => "integer",
        FieldValue::UInt(_) => "unsigned integer",
        FieldValue::Bool(_) => "boolean",
        FieldValue::Str(_) => "string",
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use super::*;

    fn text(s: &str) -> FieldValue<'_> {
        FieldValue::Str(Cow::Borrowed(s))
    }

    #[test]
    fn decimal_text_is_stored_exactly() {
        for literal in [
            "0.1000000000000000001",
            "12345678901234567.89",
            "-5",
            "1.50",
        ] {
            let cell =
                declared_cell("p", ColumnType::Decimal(None), &text(literal)).expect("decimal");
            assert_eq!(
                cell,
                Some(ColumnValue::decimal(
                    Decimal::from_str_exact(literal).expect("literal")
                ))
            );
        }
        assert_eq!(
            declared_cell("p", ColumnType::Decimal(None), &FieldValue::Int(7)).expect("int"),
            Some(ColumnValue::decimal(Decimal::from(7)))
        );
    }

    #[test]
    fn invalid_input_is_refused_with_its_type() {
        assert!(matches!(
            declared_cell("p", ColumnType::Decimal(None), &text("abc")),
            Err(DeclaredCellError::InvalidText {
                declared: "DECIMAL",
                ..
            })
        ));
        assert!(matches!(
            declared_cell("p", ColumnType::Decimal(None), &FieldValue::Float(0.1)),
            Err(DeclaredCellError::WrongKind { kind: "float", .. })
        ));
        assert!(matches!(
            declared_cell("d", ColumnType::Uuid, &text("not-a-uuid")),
            Err(DeclaredCellError::InvalidText {
                declared: "UUID",
                ..
            })
        ));
        assert!(matches!(
            declared_cell("d", ColumnType::Uuid, &FieldValue::Int(1)),
            Err(DeclaredCellError::WrongKind { .. })
        ));
        assert!(matches!(
            declared_cell("b", ColumnType::Bool, &text("true")),
            Err(DeclaredCellError::WrongKind { .. })
        ));
        assert!(matches!(
            crate::Error::from(
                declared_cell("p", ColumnType::Decimal(None), &text("abc")).expect_err("refused")
            ),
            crate::Error::InvalidTextRepresentation { .. }
        ));
    }

    #[test]
    fn uuid_and_bool_fields_are_stored() {
        let id = "550E8400-E29B-41D4-A716-446655440000";
        let expected = *uuid::Uuid::parse_str(id).expect("uuid").as_bytes();
        assert_eq!(
            declared_cell("d", ColumnType::Uuid, &text(id)).expect("uuid"),
            Some(ColumnValue::Uuid(expected))
        );
        assert_eq!(
            declared_cell("b", ColumnType::Bool, &FieldValue::Bool(false)).expect("bool"),
            Some(ColumnValue::Bool(false))
        );
        assert_eq!(
            declared_cell("v", ColumnType::Float64, &FieldValue::Float(1.0)).expect("other"),
            None
        );
    }

    /// A line carrying a value its DECIMAL, UUID or BOOLEAN column does not
    /// take is refused whole, and no cell of it is stored. A valid line of
    /// the same batch lands with its exact values.
    #[test]
    fn an_invalid_line_is_refused_before_any_cell_is_stored() {
        use super::super::columnar_memtable::{
            ColumnarMemtable, ColumnarMemtableConfig, ColumnarSchema, TimeKind,
        };
        use super::super::ilp::parse_batch;
        use super::super::ilp_ingest::{ingest_batch, line_type_conflict};

        let schema = ColumnarSchema {
            columns: vec![
                ("timestamp".into(), ColumnType::Timestamp(TimeKind::Millis)),
                ("p".into(), ColumnType::Decimal(None)),
                ("d".into(), ColumnType::Uuid),
                ("ok".into(), ColumnType::Bool),
            ],
            timestamp_idx: 0,
            codecs: vec![nodedb_codec::ColumnCodec::Auto; 4],
        };
        let lines = parse_batch(
            "m p=\"abc\",d=\"550e8400-e29b-41d4-a716-446655440000\",ok=true 1000000000\n\
             m p=\"1.5\",d=\"not-a-uuid\",ok=true 2000000000\n\
             m p=0.1,ok=true 3000000000\n\
             m p=\"0.1000000000000000001\",d=\"550e8400-e29b-41d4-a716-446655440000\",ok=false 4000000000",
        )
        .expect("valid ILP batch")
        .into_lines();
        for line in &lines[..3] {
            assert!(line_type_conflict(&schema, line).is_some());
        }
        assert_eq!(line_type_conflict(&schema, &lines[3]), None);

        let mut mt = ColumnarMemtable::new(schema, ColumnarMemtableConfig::default());
        let mut catalog = nodedb_types::timeseries::SeriesCatalog::new();
        assert_eq!(
            ingest_batch(
                &mut mt,
                nodedb_types::DatabaseId::DEFAULT,
                &lines,
                &mut catalog,
                0
            ),
            (1, 3)
        );
        assert_eq!(mt.row_count(), 1);
        assert_eq!(
            mt.column(1).as_decimals(),
            &[Some(
                Decimal::from_str_exact("0.1000000000000000001").expect("decimal")
            )]
        );
        assert_eq!(mt.column(3).as_bools(), &[Some(false)]);
    }

    /// A `DECIMAL(5,2)` cell fits as a strict or columnar `DECIMAL(5,2)`
    /// column fits it: rounded to two digits half away from zero, and refused
    /// with SQLSTATE `22003` past three integer digits. A line field and a
    /// staged `Value` take the same rule.
    #[test]
    fn a_typmod_fits_as_the_strict_engine_fits_it() {
        use nodedb_types::columnar::{ColumnDef, ColumnType as Strict, DecimalTypmod};

        let typmod = DecimalTypmod::new(5, 2).expect("typmod");
        let ts_type = ColumnType::Decimal(Some(TsDecimalTypmod::of(typmod)));
        let strict = ColumnDef::nullable("p", Strict::Decimal(Some(typmod)));
        for literal in ["1.005", "-1.005", "123.454", "999.994", "7"] {
            let input = Decimal::from_str_exact(literal).expect("literal");
            let expected =
                crate::data::executor::strict_format::coerce_value(&Value::Decimal(input), &strict)
                    .expect("strict fits");
            let Value::Decimal(expected) = expected else {
                panic!("strict returned {expected:?}");
            };
            assert_eq!(
                declared_cell("p", ts_type, &text(literal)).expect("fits"),
                Some(ColumnValue::decimal(expected)),
                "{literal}"
            );
            assert_eq!(
                declared_value("p", ts_type, &Value::String(literal.into())).expect("fits"),
                Some(Value::Decimal(expected)),
                "{literal}"
            );
        }
        assert_eq!(
            declared_cell("p", ts_type, &text("1.005")).expect("fits"),
            Some(ColumnValue::decimal(
                Decimal::from_str_exact("1.01").expect("d")
            ))
        );
        for literal in ["1000", "999.995"] {
            assert!(matches!(
                crate::data::executor::strict_format::coerce_value(
                    &Value::Decimal(Decimal::from_str_exact(literal).expect("literal")),
                    &strict,
                ),
                Err(crate::Error::NumericValueOutOfRange { .. })
            ));
            let refused = declared_cell("p", ts_type, &text(literal)).expect_err("past p");
            assert!(matches!(refused, DeclaredCellError::OutOfRange { .. }));
            assert!(matches!(
                crate::Error::from(refused),
                crate::Error::NumericValueOutOfRange { .. }
            ));
        }
    }
}
