// SPDX-License-Identifier: BUSL-1.1

//! Constant-cell output names and wire types.

use crate::control::server::response_shape::schema::{OutputColumn, OutputSchema};
use crate::control::server::response_shape::types::DdlColType;

pub(super) fn constant_schema(
    columns: &[String],
    values: &[nodedb_sql::types_expr::SqlValue],
    cast_types: &[Option<String>],
) -> OutputSchema {
    // The row payload keys each cell by the unique per-column key
    // (`cell_keys`), not the raw display name: two constant columns may
    // share a name (`SELECT nextval('s'), nextval('s')`), and a single
    // object would collapse them. `display_name` keeps the
    // client-facing name; `lookup_key` is the cell key.
    //
    // The type mirrors the cell `convert_constant_result` encodes:
    // `Int`/`Float`/`Bool`/`Decimal`/`Uuid` keep their typed cell, the
    // other variants are encoded as text. A column whose expression is a
    // `CAST` takes the cast's type when the cell is of that kind, so
    // `CAST(5 AS SMALLINT)` is `smallint` though it folds to an integer.
    let lookup_keys = crate::control::server::response_shape::project::cell_keys(columns);
    OutputSchema {
        columns: columns
            .iter()
            .zip(lookup_keys)
            .enumerate()
            .map(|(index, (c, lookup_key))| OutputColumn {
                display_name: c.clone(),
                lookup_key,
                ty: constant_cell_type(
                    values.get(index),
                    cast_types.get(index).and_then(Option::as_deref),
                ),
            })
            .collect(),
        is_star: false,
        cp_computed: Vec::new(),
        declared_key: None,
    }
}

/// Wire type of one constant cell. `cast_type` is the type its expression
/// casts to, when it is a `CAST`; it types the column when the cell is of
/// that kind, a NULL cell included. A column with no value (a plan built
/// without values) is `Text`.
fn constant_cell_type(
    value: Option<&nodedb_sql::types_expr::SqlValue>,
    cast_type: Option<&str>,
) -> DdlColType {
    use nodedb_sql::types_expr::SqlValue;
    let value_type = value_cell_type(value);
    use crate::control::planner::sql_plan_convert::output_schema_types::cast_type as cast_wire_type;
    let Some(cast) = cast_type.map(cast_wire_type) else {
        return value_type;
    };
    let fits = match cast {
        DdlColType::Int2 | DdlColType::Int4 | DdlColType::Int8 => value_type == DdlColType::Int8,
        DdlColType::Float8 | DdlColType::Bool | DdlColType::Numeric | DdlColType::Uuid => {
            value_type == cast
        }
        _ => false,
    };
    if fits || (matches!(value, Some(SqlValue::Null)) && cast != DdlColType::Text) {
        cast
    } else {
        value_type
    }
}

/// Wire type of one constant cell by its value alone.
fn value_cell_type(value: Option<&nodedb_sql::types_expr::SqlValue>) -> DdlColType {
    use nodedb_sql::types_expr::SqlValue;
    match value {
        Some(SqlValue::Int(_)) => DdlColType::Int8,
        Some(SqlValue::Float(_)) => DdlColType::Float8,
        Some(SqlValue::Bool(_)) => DdlColType::Bool,
        Some(SqlValue::Decimal(_)) => DdlColType::Numeric,
        Some(SqlValue::Uuid(_)) => DdlColType::Uuid,
        Some(
            SqlValue::String(_)
            | SqlValue::Null
            | SqlValue::Bytes(_)
            | SqlValue::Array(_)
            | SqlValue::Object(_)
            | SqlValue::Timestamp(_)
            | SqlValue::Timestamptz(_)
            | SqlValue::Geometry(_)
            | SqlValue::Duration(_),
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
            cast_types: vec![None, None],
            volatile: false,
        }];
        let schema =
            build_output_schema(&plans, &NoCatalog, nodedb_types::DatabaseId::DEFAULT, None)
                .expect("schema derives");
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

    /// A folded `CAST` keeps its target type: `CAST(5 AS SMALLINT)` is a
    /// `smallint` column, and `CAST(NULL AS INTEGER)` an `integer` one.
    #[test]
    fn a_folded_cast_keeps_its_target_type() {
        use nodedb_sql::types_expr::SqlValue;
        let plans = vec![SqlPlan::ConstantResult {
            columns: ["a", "b", "c", "d", "e"].map(String::from).to_vec(),
            values: vec![
                SqlValue::Int(5),
                SqlValue::Null,
                SqlValue::Int(7),
                SqlValue::Decimal(rust_decimal::Decimal::new(15, 1)),
                SqlValue::String("x".into()),
            ],
            cast_types: vec![
                Some("SMALLINT".into()),
                Some("INTEGER".into()),
                None,
                None,
                Some("BIGINT".into()),
            ],
            volatile: false,
        }];
        let schema =
            build_output_schema(&plans, &NoCatalog, nodedb_types::DatabaseId::DEFAULT, None)
                .expect("schema derives");
        let types: Vec<DdlColType> = schema.columns.iter().map(|c| c.ty).collect();
        assert_eq!(
            types,
            vec![
                DdlColType::Int2,
                DdlColType::Int4,
                DdlColType::Int8,
                DdlColType::Numeric,
                // A cell that is not of the cast's kind keeps its own type.
                DdlColType::Text,
            ]
        );
    }
}
