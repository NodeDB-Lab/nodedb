// SPDX-License-Identifier: BUSL-1.1

//! Projection and computed-column application for document scans.
//!
//! A projection key absent from the row maps to SQL NULL. Skipping a missing
//! key would drop the column from the response and break the pgwire
//! RowDescription contract.

use crate::bridge::expr_eval::ComputedColumn;

/// Apply projection and computed columns on raw msgpack bytes.
///
/// Native decoding preserves field types in projected and computed values. A missing
/// projection key is written as NULL. A computed column whose alias is also
/// projected keeps the projected value, so a window alias already on the
/// row is not overwritten.
pub(in crate::data::executor) fn apply_projection_msgpack(
    data: &[u8],
    computed_cols: &[ComputedColumn],
    projection: &[String],
    crs: &nodedb_crs::CrsContext<'_>,
) -> crate::Result<Vec<u8>> {
    if computed_cols.is_empty() && projection.is_empty() {
        return Ok(data.to_vec());
    }

    let doc = crate::data::executor::doc_format::decode_document_value(data)?;
    let mut fields =
        std::collections::HashMap::with_capacity(projection.len() + computed_cols.len());
    for column in projection {
        fields.insert(
            column.clone(),
            doc.get(column)
                .cloned()
                .unwrap_or(nodedb_types::Value::Null),
        );
    }
    for computed in computed_cols {
        if projection.iter().any(|column| column == &computed.alias) {
            continue;
        }
        fields.insert(computed.alias.clone(), computed.expr.eval(&doc, crs)?);
    }
    let buf = zerompk::to_msgpack_vec(&nodedb_types::Value::Object(fields)).map_err(|error| {
        crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("projected document cannot encode: {error}"),
        }
    })?;

    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::expr_eval::SqlExpr;
    use nodedb_types::Value;

    fn row(doc: Value) -> Vec<u8> {
        nodedb_types::value_to_msgpack(&doc).expect("encode row")
    }

    #[test]
    fn tagged_projection_preserves_native_values_and_computed_copies() {
        let decimal = Value::Decimal("123456789.123456789".parse().unwrap());
        let uuid = Value::Uuid("550e8400-e29b-41d4-a716-446655440000".into());
        let row = object(&[("decimal", decimal.clone()), ("uuid", uuid.clone())]);
        let input = zerompk::to_msgpack_vec(&row).unwrap();
        let computed = vec![ComputedColumn {
            alias: "copy".into(),
            expr: SqlExpr::Column("decimal".into()),
        }];
        let output = apply_projection_msgpack(
            &input,
            &computed,
            &["decimal".into(), "uuid".into()],
            &nodedb_crs::CrsContext::builtin_only(),
        )
        .unwrap();
        assert_eq!(
            crate::data::executor::doc_format::decode_document_value(&output).unwrap(),
            object(&[
                ("decimal", decimal.clone()),
                ("uuid", uuid),
                ("copy", decimal)
            ])
        );
    }

    fn object(pairs: &[(&str, Value)]) -> Value {
        Value::Object(
            pairs
                .iter()
                .map(|(k, v)| ((*k).to_string(), v.clone()))
                .collect(),
        )
    }

    fn project(
        doc: Value,
        computed: &[ComputedColumn],
        projection: &[String],
    ) -> crate::Result<Value> {
        let bytes = apply_projection_msgpack(
            &row(doc),
            computed,
            projection,
            &nodedb_crs::CrsContext::builtin_only(),
        )?;
        Ok(
            crate::data::executor::doc_format::decode_document_value(&bytes)
                .expect("decode projected row"),
        )
    }

    #[test]
    fn projection_keeps_base_fields_when_computed_columns_exist() {
        let data = object(&[
            ("id", Value::from("u1")),
            ("name", Value::from("Ada")),
            ("age", Value::Integer(42)),
        ]);
        let computed = vec![ComputedColumn {
            alias: "label".into(),
            expr: SqlExpr::Column("name".into()),
        }];
        let projection = vec!["name".to_string(), "age".to_string()];

        let projected = project(data, &computed, &projection).unwrap();

        assert_eq!(
            projected,
            object(&[
                ("name", Value::from("Ada")),
                ("age", Value::Integer(42)),
                ("label", Value::from("Ada")),
            ])
        );
    }

    #[test]
    fn projection_does_not_overwrite_existing_window_alias() {
        let data = object(&[
            ("name", Value::from("Ada")),
            ("age", Value::Integer(42)),
            ("rn", Value::Integer(1)),
        ]);
        let computed = vec![ComputedColumn {
            alias: "rn".into(),
            expr: SqlExpr::Function {
                name: "row_number".into(),
                args: Vec::new(),
            },
        }];
        let projection = vec!["name".to_string(), "age".to_string(), "rn".to_string()];

        let projected = project(data, &computed, &projection).unwrap();

        assert_eq!(
            projected,
            object(&[
                ("name", Value::from("Ada")),
                ("age", Value::Integer(42)),
                ("rn", Value::Integer(1)),
            ])
        );
    }

    /// `SELECT to_jsonb(*) AS document`: the whole stored row, every field
    /// with its own type, under one alias.
    #[test]
    fn whole_row_computed_column_carries_every_field() {
        let data = object(&[
            ("id", Value::from("u1")),
            ("n", Value::Integer(5)),
            ("ratio", Value::Float(1.5)),
        ]);
        let computed = vec![ComputedColumn {
            alias: "document".into(),
            expr: SqlExpr::Function {
                name: "to_jsonb".into(),
                args: vec![SqlExpr::Column(nodedb_query::expr::WHOLE_ROW_COLUMN.into())],
            },
        }];

        let projected = project(data.clone(), &computed, &[]).unwrap();

        assert_eq!(projected, object(&[("document", data)]));
    }

    #[test]
    fn projection_emits_null_for_missing_keys() {
        let data = object(&[("id", Value::from("u1")), ("score", Value::Float(1.0))]);
        let projection = vec![
            "id".to_string(),
            "score".to_string(),
            "pr_score".to_string(),
        ];

        let projected = project(data, &[], &projection).unwrap();

        assert_eq!(
            projected,
            object(&[
                ("id", Value::from("u1")),
                ("score", Value::Float(1.0)),
                ("pr_score", Value::Null),
            ])
        );
    }

    /// A non-finite window result on the row reaches the projected row as a
    /// float.
    #[test]
    fn projection_keeps_non_finite_floats() {
        let data = object(&[("total", Value::Float(f64::INFINITY))]);
        let projected = project(data, &[], &["total".to_string()]).unwrap();
        assert_eq!(projected, object(&[("total", Value::Float(f64::INFINITY))]));
    }

    /// A computed column that divides by zero fails the projection instead
    /// of silently materializing `NULL`.
    #[test]
    fn projection_computed_column_division_by_zero_errors() {
        use crate::bridge::expr_eval::BinaryOp;
        let data = object(&[("denom", Value::Integer(0))]);
        let computed = vec![ComputedColumn {
            alias: "bad".into(),
            expr: SqlExpr::BinaryOp {
                left: Box::new(SqlExpr::Literal(Value::Integer(10))),
                op: BinaryOp::Div,
                right: Box::new(SqlExpr::Column("denom".into())),
            },
        }];
        let err = project(data, &computed, &[]).unwrap_err();
        assert!(matches!(err, crate::Error::DivisionByZero), "got {err:?}");
    }
}
