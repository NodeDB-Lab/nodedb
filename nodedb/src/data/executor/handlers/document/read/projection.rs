// SPDX-License-Identifier: BUSL-1.1

//! Projection and computed-column application for document scans.
//!
//! A projection key absent from the row maps to SQL NULL. Skipping a missing
//! key would drop the column from the response and break the pgwire
//! RowDescription contract.

use crate::bridge::expr_eval::ComputedColumn;

/// Apply projection and computed columns on raw msgpack bytes.
///
/// For projection-only (no computed columns), uses zero-decode binary field extraction.
/// For computed columns, decodes fields on-demand from msgpack. A missing
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

    // A computed column whose alias is projected writes no entry of its own,
    // so the map header counts only the computed columns that do.
    let written_computed = computed_cols
        .iter()
        .filter(|cc| !projection.iter().any(|p| p == &cc.alias))
        .count();
    let field_count = projection.len() + written_computed;

    let mut buf = Vec::with_capacity(data.len());
    nodedb_query::msgpack_scan::write_map_header(&mut buf, field_count);

    if !projection.is_empty() {
        for col in projection {
            nodedb_query::msgpack_scan::write_str(&mut buf, col);
            if let Some((start, end)) = nodedb_query::msgpack_scan::extract_field(data, 0, col) {
                buf.extend_from_slice(&data[start..end]);
            } else {
                nodedb_query::msgpack_scan::write_null(&mut buf);
            }
        }
    }

    if !computed_cols.is_empty() {
        // A body that does not decode fails the query: evaluating the
        // columns against `Null` would ship NULL for every one of them.
        let doc_val =
            nodedb_types::value_from_msgpack(data).map_err(|e| crate::Error::Serialization {
                format: "msgpack".into(),
                detail: format!("computed columns: stored row does not decode: {e}"),
            })?;
        for cc in computed_cols {
            let already_present = projection.iter().any(|p| p == &cc.alias);
            if already_present {
                continue;
            }
            nodedb_query::msgpack_scan::write_str(&mut buf, &cc.alias);
            // A division/modulo-by-zero in a computed column fails the whole
            // query instead of silently materializing NULL into the
            // response.
            let result = cc.expr.eval(&doc_val, crs)?;
            let encoded = nodedb_types::value_to_msgpack(&result).map_err(|e| {
                crate::Error::Serialization {
                    format: "msgpack".into(),
                    detail: format!("computed column '{}' does not encode: {e}", cc.alias),
                }
            })?;
            buf.extend_from_slice(&encoded);
        }
    }

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
        Ok(nodedb_types::value_from_msgpack(&bytes).expect("decode projected row"))
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
