// SPDX-License-Identifier: BUSL-1.1

//! Query streaming MV results as Arrow RecordBatch.
//!
//! Converts the in-memory aggregate state to a RecordBatch.
//!
//! Each aggregate column takes the narrowest Arrow type that holds every
//! value in it exactly:
//!
//! - `Int64` when every value is an `Integer`.
//! - `Decimal128(38, 0)` when every value is an integer, some past `i64`.
//! - `Float64` when the values are numbers and at least one is a float.
//! - `Utf8` (the value's display text) for any other mix, such as a MIN
//!   over text.
//!
//! NULL stays NULL in every type.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Decimal128Array, Float64Array, Int64Array, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use nodedb_types::Value;
use rust_decimal::prelude::ToPrimitive;

use super::state::MvState;

/// Precision of an integer column past `i64`: the widest `Decimal128`.
const DECIMAL_PRECISION: u8 = 38;

/// The Arrow type of one aggregate column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ColumnKind {
    Int64,
    Decimal,
    Float64,
    Utf8,
}

impl ColumnKind {
    fn data_type(self) -> DataType {
        match self {
            Self::Int64 => DataType::Int64,
            Self::Decimal => DataType::Decimal128(DECIMAL_PRECISION, 0),
            Self::Float64 => DataType::Float64,
            Self::Utf8 => DataType::Utf8,
        }
    }
}

/// An integral `Decimal` as its exact `i128`.
fn decimal_integer(d: &rust_decimal::Decimal) -> Option<i128> {
    if d.fract().is_zero() {
        d.trunc().to_i128()
    } else {
        None
    }
}

/// The narrowest kind that holds every value of a column exactly.
fn column_kind<'a>(values: impl Iterator<Item = &'a Value>) -> ColumnKind {
    let mut kind = ColumnKind::Int64;
    for value in values {
        let this = match value {
            Value::Null | Value::Integer(_) => ColumnKind::Int64,
            Value::Decimal(d) if decimal_integer(d).is_some() => ColumnKind::Decimal,
            Value::Float(_) | Value::Decimal(_) => ColumnKind::Float64,
            _ => return ColumnKind::Utf8,
        };
        kind = match (kind, this) {
            (ColumnKind::Float64, _) | (_, ColumnKind::Float64) => ColumnKind::Float64,
            (ColumnKind::Decimal, _) | (_, ColumnKind::Decimal) => ColumnKind::Decimal,
            _ => ColumnKind::Int64,
        };
    }
    kind
}

/// One aggregate column as an Arrow array of `kind`.
fn build_column(values: &[&Value], kind: ColumnKind) -> crate::Result<ArrayRef> {
    Ok(match kind {
        ColumnKind::Int64 => Arc::new(Int64Array::from(
            values
                .iter()
                .map(|v| match v {
                    Value::Integer(i) => Some(*i),
                    _ => None,
                })
                .collect::<Vec<_>>(),
        )),
        ColumnKind::Decimal => Arc::new(
            Decimal128Array::from(
                values
                    .iter()
                    .map(|v| match v {
                        Value::Integer(i) => Some(i128::from(*i)),
                        Value::Decimal(d) => decimal_integer(d),
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            )
            .with_precision_and_scale(DECIMAL_PRECISION, 0)
            .map_err(|e| crate::Error::Serialization {
                format: "arrow".into(),
                detail: format!("streaming MV decimal column: {e}"),
            })?,
        ),
        ColumnKind::Float64 => Arc::new(Float64Array::from(
            values
                .iter()
                .map(|v| match v {
                    Value::Decimal(d) => d.to_f64(),
                    other => other.as_f64(),
                })
                .collect::<Vec<_>>(),
        )),
        ColumnKind::Utf8 => Arc::new(StringArray::from(
            values
                .iter()
                .map(|v| (!v.is_null()).then(|| v.to_string()))
                .collect::<Vec<_>>(),
        )),
    })
}

/// Convert MV state to a RecordBatch.
///
/// Columns: one Utf8 column per GROUP BY key, then one column per aggregate
/// typed by its values, then the `finalized` flag. `None` when the MV has no
/// data yet. Fails when an aggregate total left the exact range, or Arrow
/// refuses a column.
pub fn mv_state_to_record_batch(mv_state: &MvState) -> crate::Result<Option<RecordBatch>> {
    let results = mv_state.read_results_with_status()?;
    if results.is_empty() {
        return Ok(None);
    }

    let num_group_cols = mv_state.group_by_columns.len();
    let mut fields: Vec<Field> = Vec::new();
    let mut columns: Vec<ArrayRef> = Vec::new();

    for (i, col) in mv_state.group_by_columns.iter().enumerate() {
        let values: Vec<String> = results
            .iter()
            .map(|(key, _, _)| {
                key.splitn(num_group_cols, ':')
                    .nth(i)
                    .unwrap_or("")
                    .to_string()
            })
            .collect();
        fields.push(Field::new(col, DataType::Utf8, false));
        columns.push(Arc::new(StringArray::from(values)));
    }

    for (i, agg) in mv_state.aggregates.iter().enumerate() {
        let values: Vec<&Value> = results
            .iter()
            .map(|(_, row, _)| row.get(i).map_or(&Value::Null, |(_, v)| v))
            .collect();
        let kind = column_kind(values.iter().copied());
        fields.push(Field::new(&agg.output_name, kind.data_type(), true));
        columns.push(build_column(&values, kind)?);
    }

    fields.push(Field::new("finalized", DataType::Boolean, false));
    columns.push(Arc::new(BooleanArray::from(
        results.iter().map(|(_, _, f)| *f).collect::<Vec<_>>(),
    )));

    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map(Some)
        .map_err(|e| crate::Error::Serialization {
            format: "arrow".into(),
            detail: format!("streaming MV record batch: {e}"),
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::streaming_mv::state::AggInput;
    use crate::event::streaming_mv::types::{AggDef, AggFunction};
    use arrow::array::Array;

    fn agg(output_name: &str, function: AggFunction) -> AggDef {
        AggDef {
            output_name: output_name.into(),
            function,
            input_expr: "v".into(),
        }
    }

    #[test]
    fn state_to_batch() {
        let state = MvState::new(
            "test".into(),
            vec!["event_type".into()],
            vec![agg("cnt", AggFunction::Count)],
        );

        state.update_with_time("INSERT", &[AggInput::Event], 0);
        state.update_with_time("INSERT", &[AggInput::Event], 0);
        state.update_with_time("DELETE", &[AggInput::Event], 0);

        let batch = mv_state_to_record_batch(&state).unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3); // event_type + cnt + finalized
        assert_eq!(batch.schema().field(0).name(), "event_type");
        assert_eq!(batch.schema().field(1).data_type(), &DataType::Int64);
    }

    #[test]
    fn integer_columns_stay_exact() {
        let state = MvState::new(
            "test".into(),
            vec!["g".into()],
            vec![agg("s", AggFunction::Sum), agg("hi", AggFunction::Max)],
        );
        let big = AggInput::Value(Value::Integer(i64::MAX));
        state.update_with_time("a", &[big.clone(), big.clone()], 0);
        state.update_with_time("a", &[big.clone(), big], 0);
        let small = AggInput::Value(Value::Integer(9_007_199_254_740_993));
        state.update_with_time("b", &[small.clone(), small], 0);

        let batch = mv_state_to_record_batch(&state).unwrap().unwrap();
        let sums = batch
            .column(1)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(sums.value(0), 2 * i128::from(i64::MAX));
        assert_eq!(sums.value(1), 9_007_199_254_740_993);
        let maxes = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(maxes.value(0), i64::MAX);
        assert_eq!(maxes.value(1), 9_007_199_254_740_993);
    }

    #[test]
    fn column_kind_widens_only_as_needed() {
        let i = Value::Integer(1);
        let d = Value::from_u64(u64::MAX);
        let f = Value::Float(0.5);
        let s = Value::String("x".into());
        assert_eq!(
            column_kind([&i, &Value::Null].into_iter()),
            ColumnKind::Int64
        );
        assert_eq!(column_kind([&i, &d].into_iter()), ColumnKind::Decimal);
        assert_eq!(column_kind([&d, &f].into_iter()), ColumnKind::Float64);
        assert_eq!(column_kind([&f, &s].into_iter()), ColumnKind::Utf8);
    }
}
