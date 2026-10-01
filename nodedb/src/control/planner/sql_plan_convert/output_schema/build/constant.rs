// SPDX-License-Identifier: BUSL-1.1

//! Constant-cell output names and wire types.

use crate::control::server::response_shape::schema::{OutputColumn, OutputSchema};
use crate::control::server::response_shape::types::DdlColType;

pub(super) fn constant_schema(
    columns: &[String],
    values: &[nodedb_sql::types_expr::SqlValue],
) -> OutputSchema {
    // The row payload keys each cell by the unique per-column key
    // (`cell_keys`), not the raw display name: two constant columns may
    // share a name (`SELECT nextval('s'), nextval('s')`), and a single
    // object would collapse them. `display_name` keeps the
    // client-facing name; `lookup_key` is the cell key.
    //
    // The type mirrors the cell `convert_constant_result` encodes:
    // `Int`/`Float`/`Bool` keep their typed cell, every other variant
    // (`String`/`Null`/`Decimal`/`Bytes`/`Array`/`Timestamp`/
    // `Timestamptz`) is encoded as text.
    let lookup_keys = crate::control::server::response_shape::project::cell_keys(columns);
    OutputSchema {
        columns: columns
            .iter()
            .zip(lookup_keys)
            .enumerate()
            .map(|(index, (c, lookup_key))| OutputColumn {
                display_name: c.clone(),
                lookup_key,
                ty: constant_cell_type(values.get(index)),
            })
            .collect(),
        is_star: false,
        cp_computed: Vec::new(),
    }
}

/// Wire type of one constant cell. A column with no value (a plan built
/// without values) is `Text`.
fn constant_cell_type(value: Option<&nodedb_sql::types_expr::SqlValue>) -> DdlColType {
    use nodedb_sql::types_expr::SqlValue;
    match value {
        Some(SqlValue::Int(_)) => DdlColType::Int8,
        Some(SqlValue::Float(_)) => DdlColType::Float8,
        Some(SqlValue::Bool(_)) => DdlColType::Bool,
        Some(
            SqlValue::String(_)
            | SqlValue::Null
            | SqlValue::Decimal(_)
            | SqlValue::Bytes(_)
            | SqlValue::Array(_)
            | SqlValue::Timestamp(_)
            | SqlValue::Timestamptz(_),
        )
        | None => DdlColType::Text,
    }
}

#[cfg(test)]
mod tests {
    use super::super::fixtures::NoCatalog;
    use super::super::schema::build_output_schema;
    use crate::control::server::response_shape::types::DdlColType;
    use nodedb_sql::types::SqlPlan;
    #[test]
    fn constant_result_columns_are_typed_from_their_values() {
        use nodedb_sql::types_expr::SqlValue;
        let plans = vec![SqlPlan::ConstantResult {
            columns: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            values: vec![SqlValue::Int(1), SqlValue::String("x".into())],
            volatile: false,
        }];
        let schema =
            build_output_schema(&plans, &NoCatalog, nodedb_types::DatabaseId::DEFAULT, None);
        assert_eq!(schema.columns.len(), 3);
        assert_eq!(schema.columns[0].display_name, "a");
        assert_eq!(schema.columns[0].lookup_key, "a");
        assert_eq!(schema.columns[0].ty, DdlColType::Int8);
        assert_eq!(schema.columns[1].display_name, "b");
        assert_eq!(schema.columns[1].ty, DdlColType::Text);
        // A column without a value keeps its slot and types as text.
        assert_eq!(schema.columns[2].display_name, "c");
        assert_eq!(schema.columns[2].ty, DdlColType::Text);
        assert!(!schema.is_star);
    }
}
