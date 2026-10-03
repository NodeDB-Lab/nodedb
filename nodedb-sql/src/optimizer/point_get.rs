// SPDX-License-Identifier: Apache-2.0

//! Detect equality on the primary key → convert Scan to PointGet.

use nodedb_types::DatabaseId;

use crate::catalog::SqlCatalog;
use crate::types::*;

/// If a Scan has a single equality filter on the collection's primary key,
/// convert it to a PointGet.
///
/// The primary key is resolved from the catalog rather than assumed to be the
/// conventional `id` / `document_id` / `key`: a collection created without an
/// explicit `PRIMARY KEY` gets an auto-generated `_rowid` key, so a filter on a
/// regular `id` column is NOT a point lookup and must stay a scan (routing it
/// to PointGet would resolve a surrogate for the wrong key and return zero
/// rows). When the catalog cannot resolve a primary key (unknown collection),
/// fall back to the legacy convention so nothing regresses. A catalog error
/// fails the plan: `RetryableSchemaChanged` must reach the caller.
pub fn optimize(plan: SqlPlan, catalog: &dyn SqlCatalog) -> crate::Result<SqlPlan> {
    match plan {
        SqlPlan::Scan {
            ref collection,
            ref alias,
            ref engine,
            ref filters,
            ref projection,
            ref temporal,
            ..
        } if filters.len() == 1
            && !temporal.is_temporal()
            && !projection.iter().any(|p| {
                matches!(
                    p,
                    Projection::Computed { .. } | Projection::CpComputed { .. }
                )
            }) =>
        {
            let pk = catalog
                .get_collection(DatabaseId::DEFAULT, collection)?
                .and_then(|info| info.primary_key);
            if let Some((key_col, key_val)) = extract_pk_equality(&filters[0], pk.as_deref()) {
                return Ok(SqlPlan::PointGet {
                    collection: collection.clone(),
                    alias: alias.clone(),
                    engine: *engine,
                    key_column: key_col,
                    key_value: key_val,
                    projection: projection.clone(),
                });
            }
            Ok(plan)
        }
        _ => Ok(plan),
    }
}

/// Extract a simple equality filter eligible for the point-get rewrite.
///
/// Only the conventional document-key columns (`id` / `document_id` / `key`)
/// are candidates — this pass deliberately does not promote arbitrary declared
/// primary keys (e.g. `sku`) to point lookups. The refinement over the legacy
/// name-only check: when the catalog resolves a real primary key, a candidate
/// column is eligible only if it actually IS that key. This excludes a regular
/// `id` column on a collection whose real key is the auto-generated `_rowid`
/// (where a point-get would resolve a surrogate for the wrong key and return
/// zero rows) while leaving every previously-optimized case unchanged. When the
/// catalog can't resolve a key, fall back to the name-only convention.
fn extract_pk_equality(filter: &Filter, pk: Option<&str>) -> Option<(String, SqlValue)> {
    let is_pk_column = |col: &str| -> bool {
        let conventional = col == "id" || col == "document_id" || col == "key";
        match pk {
            Some(pk) => conventional && col.eq_ignore_ascii_case(pk),
            None => conventional,
        }
    };
    match &filter.expr {
        FilterExpr::Comparison {
            field,
            op: CompareOp::Eq,
            value,
        } => {
            let f = field.to_lowercase();
            if is_pk_column(&f) {
                Some((f, value.clone()))
            } else {
                None
            }
        }
        FilterExpr::Expr(SqlExpr::BinaryOp {
            left,
            op: BinaryOp::Eq,
            right,
        }) => {
            let col = match left.as_ref() {
                SqlExpr::Column { name, .. } => name.to_lowercase(),
                _ => return None,
            };
            if !is_pk_column(&col) {
                return None;
            }
            let val = match right.as_ref() {
                SqlExpr::Literal(v) => v.clone(),
                _ => return None,
            };
            Some((col, val))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::SqlCatalogError;
    use crate::temporal::TemporalScope;

    struct ChangingCatalog;

    impl SqlCatalog for ChangingCatalog {
        fn get_collection(
            &self,
            _: DatabaseId,
            _: &str,
        ) -> std::result::Result<Option<CollectionInfo>, SqlCatalogError> {
            Err(SqlCatalogError::RetryableSchemaChanged {
                descriptor: "collection users".into(),
            })
        }
    }

    fn id_scan() -> SqlPlan {
        SqlPlan::Scan {
            collection: "users".into(),
            alias: None,
            engine: EngineType::DocumentSchemaless,
            filters: vec![Filter {
                expr: FilterExpr::Comparison {
                    field: "id".into(),
                    op: CompareOp::Eq,
                    value: SqlValue::String("u1".into()),
                },
            }],
            projection: Vec::new(),
            sort_keys: Vec::new(),
            limit: None,
            offset: 0,
            distinct: false,
            window_functions: Vec::new(),
            temporal: TemporalScope::default(),
        }
    }

    #[test]
    fn a_catalog_error_fails_the_point_get_rewrite() {
        let error = optimize(id_scan(), &ChangingCatalog).unwrap_err();
        assert_eq!(
            error,
            crate::SqlError::from(SqlCatalogError::RetryableSchemaChanged {
                descriptor: "collection users".into(),
            })
        );
    }
}
