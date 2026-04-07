//! Query streaming MV results as Arrow RecordBatch.
//!
//! Converts the in-memory aggregate state to a RecordBatch that
//! DataFusion can serve via MemTable.

use std::sync::Arc;

use arrow::array::{Float64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use super::state::MvState;

/// Build the Arrow schema for an MV's results.
///
/// Columns: one Utf8 column per GROUP BY key, then one Float64 column per aggregate.
pub fn mv_result_schema(mv_state: &MvState) -> SchemaRef {
    let mut fields: Vec<Field> = mv_state
        .group_by_columns
        .iter()
        .map(|col| Field::new(col, DataType::Utf8, false))
        .collect();

    for agg in &mv_state.aggregates {
        fields.push(Field::new(&agg.output_name, DataType::Float64, false));
    }

    // Finalization status column.
    fields.push(Field::new("finalized", DataType::Boolean, false));

    Arc::new(Schema::new(fields))
}

/// Convert MV state to a RecordBatch.
///
/// Returns None if the MV has no data yet.
pub fn mv_state_to_record_batch(mv_state: &MvState) -> Option<RecordBatch> {
    let results = mv_state.read_results_with_status();
    if results.is_empty() {
        return None;
    }

    let schema = mv_result_schema(mv_state);
    let num_group_cols = mv_state.group_by_columns.len();
    let num_aggs = mv_state.aggregates.len();

    let mut group_arrays: Vec<Vec<String>> = vec![Vec::new(); num_group_cols];
    let mut agg_arrays: Vec<Vec<f64>> = vec![Vec::new(); num_aggs];
    let mut finalized_array: Vec<bool> = Vec::new();

    for (key, agg_values, finalized) in &results {
        let parts: Vec<&str> = key.splitn(num_group_cols, ':').collect();
        for (i, col_values) in group_arrays.iter_mut().enumerate() {
            col_values.push(parts.get(i).unwrap_or(&"").to_string());
        }

        for (i, agg_col) in agg_arrays.iter_mut().enumerate() {
            let val = agg_values.get(i).map(|(_, v)| *v).unwrap_or(0.0);
            agg_col.push(val);
        }

        finalized_array.push(*finalized);
    }

    let mut columns: Vec<Arc<dyn arrow::array::Array>> = Vec::new();
    for group_col in &group_arrays {
        columns.push(Arc::new(StringArray::from(
            group_col.iter().map(|s| s.as_str()).collect::<Vec<&str>>(),
        )));
    }
    for agg_col in &agg_arrays {
        columns.push(Arc::new(Float64Array::from(agg_col.clone())));
    }
    columns.push(Arc::new(arrow::array::BooleanArray::from(finalized_array)));

    RecordBatch::try_new(schema, columns).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::streaming_mv::types::{AggDef, AggFunction};

    #[test]
    fn schema_reflects_definition() {
        let state = MvState::new(
            "test".into(),
            vec!["event_type".into()],
            vec![
                AggDef {
                    output_name: "cnt".into(),
                    function: AggFunction::Count,
                    input_expr: String::new(),
                },
                AggDef {
                    output_name: "total".into(),
                    function: AggFunction::Sum,
                    input_expr: "amount".into(),
                },
            ],
        );

        let schema = mv_result_schema(&state);
        assert_eq!(schema.fields().len(), 4); // event_type + cnt + total + finalized
        assert_eq!(schema.field(0).name(), "event_type");
        assert_eq!(schema.field(1).name(), "cnt");
        assert_eq!(schema.field(2).name(), "total");
    }

    #[test]
    fn state_to_batch() {
        let state = MvState::new(
            "test".into(),
            vec!["event_type".into()],
            vec![AggDef {
                output_name: "cnt".into(),
                function: AggFunction::Count,
                input_expr: String::new(),
            }],
        );

        state.update("INSERT", &[1.0]);
        state.update("INSERT", &[1.0]);
        state.update("DELETE", &[1.0]);

        let batch = mv_state_to_record_batch(&state).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3); // event_type + cnt + finalized
    }
}
