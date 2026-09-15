// SPDX-License-Identifier: BUSL-1.1

//! Projection and computed-column application for document scans.
//!
//! One codec serves every read path: rows stay MessagePack end to end (see
//! [`apply_projection_msgpack`]). A projection key absent from the row maps to
//! SQL NULL; silently skipping a missing key drops the column from the
//! response and breaks the pgwire RowDescription contract.

use crate::bridge::expr_eval::ComputedColumn;

/// Apply projection and computed columns on raw msgpack bytes.
///
/// For projection-only (no computed columns), uses zero-decode binary field extraction.
/// For computed columns, decodes fields on-demand from msgpack.
pub(in crate::data::executor) fn apply_projection_msgpack(
    data: &[u8],
    computed_cols: &[ComputedColumn],
    projection: &[String],
) -> crate::Result<Vec<u8>> {
    if computed_cols.is_empty() && projection.is_empty() {
        return Ok(data.to_vec());
    }

    let field_count = if projection.is_empty() {
        computed_cols.len()
    } else {
        projection.len() + computed_cols.len()
    };

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
        let doc_val = nodedb_types::value_from_msgpack(data).unwrap_or(nodedb_types::Value::Null);
        for cc in computed_cols {
            let already_present = projection.iter().any(|p| p == &cc.alias);
            if already_present {
                continue;
            }
            nodedb_query::msgpack_scan::write_str(&mut buf, &cc.alias);
            // A division/modulo-by-zero in a computed column fails the whole
            // query instead of silently materializing NULL into the
            // response.
            let result = cc.expr.eval(&doc_val)?;
            if let Ok(mp) = nodedb_types::value_to_msgpack(&result) {
                buf.extend_from_slice(&mp);
            } else {
                nodedb_query::msgpack_scan::write_null(&mut buf);
            }
        }
    }

    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::expr_eval::SqlExpr;
    use std::collections::HashMap;

    fn row(fields: &[(&str, nodedb_types::Value)]) -> Vec<u8> {
        let mut map = HashMap::new();
        for (key, value) in fields {
            map.insert((*key).to_string(), value.clone());
        }
        nodedb_types::value_to_msgpack(&nodedb_types::Value::Object(map)).unwrap()
    }

    fn decoded(bytes: &[u8]) -> HashMap<String, nodedb_types::Value> {
        match nodedb_types::value_from_msgpack(bytes).unwrap() {
            nodedb_types::Value::Object(map) => map,
            other => panic!(
                "expected an object, got the {:?} variant",
                std::mem::discriminant(&other)
            ),
        }
    }

    #[test]
    fn projection_keeps_base_fields_when_computed_columns_exist() {
        let data = row(&[
            ("id", nodedb_types::Value::String("u1".into())),
            ("name", nodedb_types::Value::String("Ada".into())),
            ("age", nodedb_types::Value::Integer(42)),
        ]);
        let computed = vec![ComputedColumn {
            alias: "label".into(),
            expr: SqlExpr::Column("name".into()),
        }];
        let projection = vec!["name".to_string(), "age".to_string()];

        let projected = decoded(&apply_projection_msgpack(&data, &computed, &projection).unwrap());

        assert_eq!(
            projected.len(),
            3,
            "keys: {:?}",
            projected.keys().collect::<Vec<_>>()
        );
        assert_eq!(
            projected.get("name"),
            Some(&nodedb_types::Value::String("Ada".into()))
        );
        assert_eq!(
            projected.get("age"),
            Some(&nodedb_types::Value::Integer(42))
        );
        assert_eq!(
            projected.get("label"),
            Some(&nodedb_types::Value::String("Ada".into()))
        );
    }

    #[test]
    fn projection_emits_null_for_missing_keys() {
        let data = row(&[
            ("id", nodedb_types::Value::String("u1".into())),
            ("score", nodedb_types::Value::Float(1.0)),
        ]);
        let projection = vec![
            "id".to_string(),
            "score".to_string(),
            "pr_score".to_string(),
        ];

        let projected = decoded(&apply_projection_msgpack(&data, &[], &projection).unwrap());

        assert_eq!(
            projected.get("pr_score"),
            Some(&nodedb_types::Value::Null),
            "a missing projection key must be SQL NULL; keys: {:?}",
            projected.keys().collect::<Vec<_>>()
        );
    }

    /// A computed column that divides by zero fails the projection instead
    /// of silently materializing `NULL`.
    #[test]
    fn projection_computed_column_division_by_zero_errors() {
        use crate::bridge::expr_eval::BinaryOp;
        let data = row(&[("denom", nodedb_types::Value::Integer(0))]);
        let computed = vec![ComputedColumn {
            alias: "bad".into(),
            expr: SqlExpr::BinaryOp {
                left: Box::new(SqlExpr::Literal(nodedb_types::Value::Integer(10))),
                op: BinaryOp::Div,
                right: Box::new(SqlExpr::Column("denom".into())),
            },
        }];
        let err = apply_projection_msgpack(&data, &computed, &[]).unwrap_err();
        assert!(matches!(err, crate::Error::DivisionByZero), "got {err:?}");
    }
}
