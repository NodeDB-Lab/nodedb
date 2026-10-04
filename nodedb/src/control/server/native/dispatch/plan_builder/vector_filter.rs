// SPDX-License-Identifier: BUSL-1.1

//! The metadata filter of a native `VectorSearch` request.
//!
//! The client sends a `MetadataFilter` as MessagePack in
//! `TextFields::filters`. The filter lowers to the expression tree the SQL
//! planner builds for the `WHERE` a pgwire client renders from the same
//! filter. It is then planned against the collection by the same column
//! check and literal coercion, and lowered to the same `ScanFilter` bytes.
//! The bytes travel in the plan's residual-filter slot, the slot a SQL
//! `WHERE` fills. The filter therefore matches the same rows on both
//! clients and narrows the candidates before the top-k cut.

use std::sync::Arc;

use nodedb_sql::types::{BinaryOp, SqlExpr, SqlValue, UnaryOp};
use nodedb_types::filter::MetadataFilter;
use nodedb_types::value::Value;

use super::super::DispatchCtx;
use crate::control::planner::catalog_adapter::OriginCatalog;
use crate::control::planner::plan_error_map::map_plan_error;
use crate::control::planner::sql_plan_convert::filter::serialize_filters;

/// The residual-filter bytes for `filter_bytes`, the client's encoded
/// `MetadataFilter`.
///
/// Bytes that do not decode to a `MetadataFilter`, and a filter value no SQL
/// literal can carry, are a `BadRequest`. A column the collection does not
/// have fails the same way the SQL `WHERE` fails.
pub(crate) fn residual_filters(
    ctx: &DispatchCtx<'_>,
    collection: &str,
    filter_bytes: &[u8],
) -> crate::Result<Vec<u8>> {
    let filter = decode_metadata_filter(filter_bytes)?;
    let predicate = metadata_filter_predicate(&filter)?;
    let state = ctx.state;
    let catalog = OriginCatalog::new(
        Arc::clone(&state.credentials),
        state.array_catalog.clone(),
        ctx.tenant_id().as_u64(),
        ctx.database_id(),
        Some(Arc::clone(&state.retention_policy_registry)),
    )
    .with_sequence_registry(Arc::clone(&state.sequence_registry));
    let filters = nodedb_sql::planner::collection_predicate::plan_collection_predicate(
        &catalog, collection, predicate,
    )
    .map_err(|e| map_plan_error(e, ctx.tenant_id()))?;
    serialize_filters(&filters)
}

/// Decode the client's filter bytes as one MessagePack `MetadataFilter`.
fn decode_metadata_filter(bytes: &[u8]) -> crate::Result<MetadataFilter> {
    zerompk::from_msgpack(bytes).map_err(|e| crate::Error::BadRequest {
        detail: format!("vector_search filter is not a MessagePack MetadataFilter: {e}"),
    })
}

/// The `WHERE` expression a pgwire client renders for `filter`:
/// `"<field>" <op> <literal>`, `IN` / `NOT IN` lists, `AND` / `OR` / `NOT`.
/// An empty `AND` is `TRUE` and an empty `OR` is `FALSE`.
fn metadata_filter_predicate(filter: &MetadataFilter) -> crate::Result<SqlExpr> {
    let compare = |field: &str, op: BinaryOp, value: &Value| -> crate::Result<SqlExpr> {
        Ok(SqlExpr::BinaryOp {
            left: Box::new(column(field)),
            op,
            right: Box::new(SqlExpr::Literal(literal(value)?)),
        })
    };
    let in_list = |field: &str, values: &[Value], negated: bool| -> crate::Result<SqlExpr> {
        let list = values
            .iter()
            .map(|v| literal(v).map(SqlExpr::Literal))
            .collect::<crate::Result<Vec<_>>>()?;
        Ok(SqlExpr::InList {
            expr: Box::new(column(field)),
            list,
            negated,
        })
    };
    match filter {
        MetadataFilter::Eq { field, value } => compare(field, BinaryOp::Eq, value),
        MetadataFilter::Ne { field, value } => compare(field, BinaryOp::Ne, value),
        MetadataFilter::Gt { field, value } => compare(field, BinaryOp::Gt, value),
        MetadataFilter::Gte { field, value } => compare(field, BinaryOp::Ge, value),
        MetadataFilter::Lt { field, value } => compare(field, BinaryOp::Lt, value),
        MetadataFilter::Lte { field, value } => compare(field, BinaryOp::Le, value),
        MetadataFilter::In { field, values } => in_list(field, values, false),
        MetadataFilter::NotIn { field, values } => in_list(field, values, true),
        MetadataFilter::And(parts) => fold(parts, BinaryOp::And, true),
        MetadataFilter::Or(parts) => fold(parts, BinaryOp::Or, false),
        MetadataFilter::Not(inner) => Ok(SqlExpr::UnaryOp {
            op: UnaryOp::Not,
            expr: Box::new(metadata_filter_predicate(inner)?),
        }),
        other => Err(crate::Error::BadRequest {
            detail: format!("vector_search filter variant has no WHERE form: {other:?}"),
        }),
    }
}

/// `parts` joined by `op`, or the literal `empty` for no part.
fn fold(parts: &[MetadataFilter], op: BinaryOp, empty: bool) -> crate::Result<SqlExpr> {
    let mut exprs = parts.iter().map(metadata_filter_predicate);
    let Some(first) = exprs.next() else {
        return Ok(SqlExpr::Literal(SqlValue::Bool(empty)));
    };
    exprs.try_fold(first?, |acc, next: crate::Result<SqlExpr>| {
        Ok::<_, crate::Error>(SqlExpr::BinaryOp {
            left: Box::new(acc),
            op,
            right: Box::new(next?),
        })
    })
}

/// A quoted identifier: the field name is kept exactly as sent.
fn column(field: &str) -> SqlExpr {
    SqlExpr::Column {
        table: None,
        name: field.to_string(),
    }
}

/// The SQL literal a pgwire client renders for `value`. Only `NULL`,
/// booleans, integers, floats and strings have a literal form.
fn literal(value: &Value) -> crate::Result<SqlValue> {
    match value {
        Value::Null => Ok(SqlValue::Null),
        Value::Bool(b) => Ok(SqlValue::Bool(*b)),
        Value::Integer(i) => Ok(SqlValue::Int(*i)),
        Value::Float(f) => Ok(SqlValue::Float(*f)),
        Value::String(s) => Ok(SqlValue::String(s.clone())),
        other => Err(crate::Error::BadRequest {
            detail: format!("vector_search filter value has no SQL literal form: {other:?}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lowered(filter: &MetadataFilter) -> String {
        format!(
            "{:?}",
            metadata_filter_predicate(filter).expect("filter lowers")
        )
    }

    #[test]
    fn malformed_bytes_are_bad_request() {
        for bytes in [&b""[..], &[0xc1][..], &b"{\"Eq\":{}}"[..]] {
            let err = decode_metadata_filter(bytes).expect_err("malformed filter bytes");
            assert!(matches!(err, crate::Error::BadRequest { .. }), "{err:?}");
        }
    }

    #[test]
    fn msgpack_filter_decodes() {
        let filter = MetadataFilter::eq("category", Value::String("ai".into()));
        let bytes = zerompk::to_msgpack_vec(&filter).expect("encode");
        assert_eq!(decode_metadata_filter(&bytes).expect("decode"), filter);
    }

    #[test]
    fn comparison_lowers_to_column_op_literal() {
        let expr = metadata_filter_predicate(&MetadataFilter::Gte {
            field: "Score".into(),
            value: Value::Integer(3),
        })
        .expect("lowers");
        match expr {
            SqlExpr::BinaryOp { left, op, right } => {
                assert!(matches!(*left, SqlExpr::Column { ref name, .. } if name == "Score"));
                assert_eq!(op, BinaryOp::Ge);
                assert!(matches!(*right, SqlExpr::Literal(SqlValue::Int(3))));
            }
            other => panic!("expected a comparison, got {other:?}"),
        }
    }

    #[test]
    fn compound_filters_lower_to_logical_operators() {
        let filter = MetadataFilter::Not(Box::new(MetadataFilter::or(vec![
            MetadataFilter::eq("a", Value::Integer(1)),
            MetadataFilter::NotIn {
                field: "b".into(),
                values: vec![Value::String("x".into())],
            },
        ])));
        let text = lowered(&filter);
        assert!(text.starts_with("UnaryOp { op: Not"), "{text}");
        assert!(text.contains("op: Or"), "{text}");
        assert!(text.contains("negated: true"), "{text}");
    }

    #[test]
    fn empty_and_or_are_true_and_false() {
        assert!(matches!(
            metadata_filter_predicate(&MetadataFilter::and(vec![])).expect("lowers"),
            SqlExpr::Literal(SqlValue::Bool(true))
        ));
        assert!(matches!(
            metadata_filter_predicate(&MetadataFilter::or(vec![])).expect("lowers"),
            SqlExpr::Literal(SqlValue::Bool(false))
        ));
    }

    #[test]
    fn value_without_literal_form_is_bad_request() {
        let filter = MetadataFilter::eq("tags", Value::Array(vec![Value::Integer(1)]));
        let err = metadata_filter_predicate(&filter).expect_err("array has no literal");
        assert!(matches!(err, crate::Error::BadRequest { .. }), "{err:?}");
    }
}
