// SPDX-License-Identifier: Apache-2.0

//! Primary-key prefilter of a vector search.
//!
//! A top-level `WHERE pk = v` or `WHERE pk IN (v, ...)` conjunct of a
//! `VectorSearch` names the only rows the search may rank. The pass moves it
//! out of the residual filters into `pk_prefilter`. The executor lowers the
//! keys to a candidate bitmap the index search honors, so the top-k is drawn
//! from those rows even when the nearest vectors lie outside them. Left as a
//! residual filter, the conjunct would run after an over-fetched top-k cut
//! and drop rows the cut never reached.

use nodedb_types::DatabaseId;

use crate::error::Result;
use crate::types::*;

/// Move the primary-key conjuncts of a `VectorSearch` plan's filters into its
/// `pk_prefilter`. Other plans are untouched.
pub(super) fn apply_vector_pk_prefilter(
    plan: &mut SqlPlan,
    catalog: &dyn SqlCatalog,
) -> Result<()> {
    let SqlPlan::VectorSearch {
        collection,
        filters,
        pk_prefilter,
        ..
    } = plan
    else {
        return Ok(());
    };
    let Some(info) = catalog.get_collection(DatabaseId::DEFAULT, collection)? else {
        return Ok(());
    };
    let Some(pk) = info.primary_key.as_deref() else {
        return Ok(());
    };
    let mut keys: Option<Vec<SqlValue>> = pk_prefilter.take();
    let mut kept: Vec<Filter> = Vec::with_capacity(filters.len());
    for filter in filters.drain(..) {
        match filter.expr {
            FilterExpr::Comparison {
                ref field,
                op: CompareOp::Eq,
                ref value,
            } if field == pk => restrict(&mut keys, vec![value.clone()]),
            FilterExpr::InList {
                ref field,
                ref values,
            } if field == pk => restrict(&mut keys, values.clone()),
            FilterExpr::Expr(expr) => {
                let mut residual: Vec<SqlExpr> = Vec::new();
                for conjunct in split_conjuncts(expr) {
                    match pk_keys(&conjunct, pk) {
                        Some(found) => restrict(&mut keys, found),
                        None => residual.push(conjunct),
                    }
                }
                if let Some(expr) = join_conjuncts(residual) {
                    kept.push(Filter {
                        expr: FilterExpr::Expr(expr),
                    });
                }
            }
            other => kept.push(Filter { expr: other }),
        }
    }
    *filters = kept;
    *pk_prefilter = keys;
    Ok(())
}

/// Intersect the admitted keys with `found`. The first restriction admits
/// exactly `found`.
fn restrict(keys: &mut Option<Vec<SqlValue>>, found: Vec<SqlValue>) {
    *keys = Some(match keys.take() {
        None => found,
        Some(admitted) => admitted.into_iter().filter(|k| found.contains(k)).collect(),
    });
}

/// The top-level `AND` operands of `expr`, in order.
fn split_conjuncts(expr: SqlExpr) -> Vec<SqlExpr> {
    match expr {
        SqlExpr::BinaryOp {
            left,
            op: BinaryOp::And,
            right,
        } => {
            let mut out = split_conjuncts(*left);
            out.extend(split_conjuncts(*right));
            out
        }
        other => vec![other],
    }
}

/// `AND` of `conjuncts`, or `None` when there are none.
fn join_conjuncts(conjuncts: Vec<SqlExpr>) -> Option<SqlExpr> {
    conjuncts
        .into_iter()
        .reduce(|left, right| SqlExpr::BinaryOp {
            left: Box::new(left),
            op: BinaryOp::And,
            right: Box::new(right),
        })
}

/// The keys of `pk = literal` or `pk IN (literal, ...)`. `None` for any other
/// predicate, including an `IN` list holding a non-literal.
fn pk_keys(expr: &SqlExpr, pk: &str) -> Option<Vec<SqlValue>> {
    let is_pk = |e: &SqlExpr| matches!(e, SqlExpr::Column { name, .. } if name == pk);
    match expr {
        SqlExpr::BinaryOp {
            left,
            op: BinaryOp::Eq,
            right,
        } => match (left.as_ref(), right.as_ref()) {
            (column, SqlExpr::Literal(value)) | (SqlExpr::Literal(value), column)
                if is_pk(column) =>
            {
                Some(vec![value.clone()])
            }
            _ => None,
        },
        SqlExpr::InList {
            expr,
            list,
            negated: false,
        } if is_pk(expr) => list
            .iter()
            .map(|item| match item {
                SqlExpr::Literal(value) => Some(value.clone()),
                _ => None,
            })
            .collect(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::super::fixtures::plan_select_sql;
    use crate::types::*;

    /// The key prefilter and residual filter count of a vector search plan.
    fn vector_parts(sql: &str) -> (Option<Vec<SqlValue>>, usize) {
        match plan_select_sql(sql) {
            SqlPlan::VectorSearch {
                pk_prefilter,
                filters,
                ..
            } => (pk_prefilter, filters.len()),
            other => panic!("expected VectorSearch, got {other:?}"),
        }
    }

    #[test]
    fn an_id_in_list_becomes_the_prefilter() {
        let (keys, residual) = vector_parts(
            "SELECT id FROM embeddings WHERE id IN ('a', 'b') \
             ORDER BY vector_distance(embedding, [1.0, 0.0]) LIMIT 2",
        );
        assert_eq!(
            keys,
            Some(vec![
                SqlValue::String("a".into()),
                SqlValue::String("b".into())
            ])
        );
        assert_eq!(residual, 0);
    }

    #[test]
    fn other_conjuncts_stay_residual_filters() {
        let (keys, residual) = vector_parts(
            "SELECT id FROM embeddings WHERE tag = 'x' AND id = 'a' \
             ORDER BY vector_distance(embedding, [1.0, 0.0]) LIMIT 2",
        );
        assert_eq!(keys, Some(vec![SqlValue::String("a".into())]));
        assert_eq!(residual, 1);
    }

    #[test]
    fn a_disjunction_with_the_key_is_no_prefilter() {
        let (keys, residual) = vector_parts(
            "SELECT id FROM embeddings WHERE id = 'a' OR tag = 'x' \
             ORDER BY vector_distance(embedding, [1.0, 0.0]) LIMIT 2",
        );
        assert_eq!(keys, None);
        assert_eq!(residual, 1);
    }

    #[test]
    fn two_key_conjuncts_intersect() {
        let (keys, _) = vector_parts(
            "SELECT id FROM embeddings WHERE id IN ('a', 'b') AND id IN ('b', 'c') \
             ORDER BY vector_distance(embedding, [1.0, 0.0]) LIMIT 2",
        );
        assert_eq!(keys, Some(vec![SqlValue::String("b".into())]));
    }

    #[test]
    fn the_remote_client_form_is_a_prefiltered_search_of_the_default_column() {
        let plan = plan_select_sql(
            "SELECT * FROM embeddings WHERE id IN ('a') \
             ORDER BY vector_distance(ARRAY[1.0, 0.0]) LIMIT 2",
        );
        let SqlPlan::VectorSearch {
            field,
            pk_prefilter,
            top_k,
            ..
        } = plan
        else {
            panic!("expected VectorSearch, got {plan:?}");
        };
        assert_eq!(
            field, "",
            "no declared vector column: the collection-level index"
        );
        assert_eq!(pk_prefilter, Some(vec![SqlValue::String("a".into())]));
        assert_eq!(top_k, 2);
    }

    #[test]
    fn no_key_conjunct_is_no_prefilter() {
        let (keys, _) = vector_parts(
            "SELECT id FROM embeddings ORDER BY vector_distance(embedding, [1.0, 0.0]) LIMIT 2",
        );
        assert_eq!(keys, None);
    }
}
