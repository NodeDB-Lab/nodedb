// SPDX-License-Identifier: Apache-2.0

//! Vector-primary payload projection and indexed-filter extraction.

use crate::error::Result;
use crate::types::*;
use nodedb_types::DatabaseId;

/// Returns `true` when every projection item is either:
/// - a plain column reference to the surrogate/PK column (`id` or `document_id`), or
/// - a `vector_distance(...)` function call (any alias).
///
/// Anything else — a payload field, `*`, or an unrecognised expression — returns `false`.
fn is_pure_vector_projection(projection: &[Projection]) -> bool {
    if projection.is_empty() {
        return false;
    }
    for item in projection {
        match item {
            Projection::Column(name) => {
                if !name.eq_ignore_ascii_case("id") && !name.eq_ignore_ascii_case("document_id") {
                    return false;
                }
            }
            Projection::Computed { expr, .. } => {
                // Accept any of the three vector distance function names.
                let SqlExpr::Function { name, .. } = expr else {
                    return false;
                };
                if !name.eq_ignore_ascii_case("vector_distance")
                    && !name.eq_ignore_ascii_case("vector_cosine_distance")
                    && !name.eq_ignore_ascii_case("vector_neg_inner_product")
                {
                    return false;
                }
            }
            // A Control-Plane-computed item is evaluated over the fetched
            // payload, so the payload must be fetched.
            Projection::CpComputed { .. } | Projection::Star | Projection::QualifiedStar(_) => {
                return false;
            }
        }
    }
    true
}

pub(super) fn apply_vector_payload(
    plan: &mut SqlPlan,
    catalog: &dyn SqlCatalog,
    pre_order_by_projection: Option<&[Projection]>,
    pre_order_by_collection: Option<&str>,
) -> Result<()> {
    // After ORDER BY: if we now have a VectorSearch, check whether
    // the collection is vector-primary and the projection is
    // payload-free. If so, set `skip_payload_fetch`.
    if let SqlPlan::VectorSearch {
        collection,
        skip_payload_fetch,
        filters,
        payload_filters,
        ..
    } = plan
    {
        let info = catalog.get_collection(DatabaseId::DEFAULT, collection)?;
        let is_vector_primary = info
            .as_ref()
            .map(|c| c.primary == nodedb_types::PrimaryEngine::Vector)
            .unwrap_or(false);
        if is_vector_primary {
            if let Some(proj) = pre_order_by_projection
                && pre_order_by_collection == Some(collection.as_str())
            {
                *skip_payload_fetch = is_pure_vector_projection(proj);
            }
            if let Some(vp) = info.as_ref().and_then(|c| c.vector_primary.as_ref()) {
                let mut peeled: Vec<SqlPayloadAtom> = Vec::new();
                let is_indexed = |name: &str| {
                    vp.payload_indexes
                        .iter()
                        .any(|(p, _)| p.eq_ignore_ascii_case(name))
                };
                filters.retain(|f| match &f.expr {
                    FilterExpr::Comparison {
                        field,
                        op: CompareOp::Eq,
                        value,
                    } if is_indexed(field) => {
                        peeled.push(SqlPayloadAtom::Eq(field.clone(), value.clone()));
                        false
                    }
                    FilterExpr::InList { field, values } if is_indexed(field) => {
                        peeled.push(SqlPayloadAtom::In(field.clone(), values.clone()));
                        false
                    }
                    FilterExpr::Between { field, low, high } if is_indexed(field) => {
                        peeled.push(SqlPayloadAtom::Range {
                            field: field.clone(),
                            low: Some(low.clone()),
                            low_inclusive: true,
                            high: Some(high.clone()),
                            high_inclusive: true,
                        });
                        false
                    }
                    FilterExpr::Comparison { field, op, value }
                        if matches!(
                            op,
                            CompareOp::Lt | CompareOp::Le | CompareOp::Gt | CompareOp::Ge
                        ) && is_indexed(field) =>
                    {
                        let inclusive = matches!(op, CompareOp::Le | CompareOp::Ge);
                        let upper = matches!(op, CompareOp::Lt | CompareOp::Le);
                        peeled.push(SqlPayloadAtom::Range {
                            field: field.clone(),
                            low: if upper { None } else { Some(value.clone()) },
                            low_inclusive: !upper && inclusive,
                            high: if upper { Some(value.clone()) } else { None },
                            high_inclusive: upper && inclusive,
                        });
                        false
                    }
                    FilterExpr::Expr(SqlExpr::BinaryOp {
                        left,
                        op: BinaryOp::Eq,
                        right,
                    }) => match (&**left, &**right) {
                        (SqlExpr::Column { name, .. }, SqlExpr::Literal(v)) if is_indexed(name) => {
                            peeled.push(SqlPayloadAtom::Eq(name.clone(), v.clone()));
                            false
                        }
                        (SqlExpr::Literal(v), SqlExpr::Column { name, .. }) if is_indexed(name) => {
                            peeled.push(SqlPayloadAtom::Eq(name.clone(), v.clone()));
                            false
                        }
                        _ => true,
                    },
                    FilterExpr::Expr(SqlExpr::InList {
                        expr,
                        list,
                        negated: false,
                    }) => match &**expr {
                        SqlExpr::Column { name, .. } if is_indexed(name) => {
                            let mut lits = Vec::with_capacity(list.len());
                            let all_lit = list.iter().all(|e| {
                                if let SqlExpr::Literal(v) = e {
                                    lits.push(v.clone());
                                    true
                                } else {
                                    false
                                }
                            });
                            if all_lit {
                                peeled.push(SqlPayloadAtom::In(name.clone(), lits));
                                false
                            } else {
                                true
                            }
                        }
                        _ => true,
                    },
                    FilterExpr::Expr(SqlExpr::Between {
                        expr,
                        low,
                        high,
                        negated: false,
                    }) => match (&**expr, &**low, &**high) {
                        (
                            SqlExpr::Column { name, .. },
                            SqlExpr::Literal(lo),
                            SqlExpr::Literal(hi),
                        ) if is_indexed(name) => {
                            peeled.push(SqlPayloadAtom::Range {
                                field: name.clone(),
                                low: Some(lo.clone()),
                                low_inclusive: true,
                                high: Some(hi.clone()),
                                high_inclusive: true,
                            });
                            false
                        }
                        _ => true,
                    },
                    FilterExpr::Expr(SqlExpr::BinaryOp { left, op, right })
                        if matches!(
                            op,
                            BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge
                        ) =>
                    {
                        match (&**left, &**right) {
                            (SqlExpr::Column { name, .. }, SqlExpr::Literal(v))
                                if is_indexed(name) =>
                            {
                                let inclusive = matches!(op, BinaryOp::Le | BinaryOp::Ge);
                                let upper = matches!(op, BinaryOp::Lt | BinaryOp::Le);
                                peeled.push(SqlPayloadAtom::Range {
                                    field: name.clone(),
                                    low: if upper { None } else { Some(v.clone()) },
                                    low_inclusive: !upper && inclusive,
                                    high: if upper { Some(v.clone()) } else { None },
                                    high_inclusive: upper && inclusive,
                                });
                                false
                            }
                            _ => true,
                        }
                    }
                    _ => true,
                });
                *payload_filters = peeled;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::fixtures::plan_select_sql;
    use super::apply_vector_payload;
    use crate::catalog::{SqlCatalog, SqlCatalogError};
    use crate::types::CollectionInfo;

    struct ChangingCatalog;

    impl SqlCatalog for ChangingCatalog {
        fn get_collection(
            &self,
            _: nodedb_types::DatabaseId,
            _: &str,
        ) -> Result<Option<CollectionInfo>, SqlCatalogError> {
            Err(SqlCatalogError::RetryableSchemaChanged {
                descriptor: "collection embeddings".into(),
            })
        }
    }

    #[test]
    fn vector_payload_lookup_preserves_catalog_error() {
        let mut plan = plan_select_sql(
            "SELECT id FROM embeddings ORDER BY vector_distance(embedding, [1.0, 0.0]) LIMIT 5",
        );
        let error = apply_vector_payload(&mut plan, &ChangingCatalog, None, None).unwrap_err();
        assert_eq!(
            error,
            crate::SqlError::from(SqlCatalogError::RetryableSchemaChanged {
                descriptor: "collection embeddings".into(),
            })
        );
    }
}
