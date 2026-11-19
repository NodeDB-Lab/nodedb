// SPDX-License-Identifier: Apache-2.0

//! Detect equality on the primary key → convert Scan to PointGet.

use nodedb_types::DatabaseId;

use crate::catalog::SqlCatalog;
use crate::planner::cp_projection::to_cp_computed;
use crate::types::*;

/// If a Scan has a single equality filter on the collection's primary key,
/// convert it to a PointGet.
///
/// The primary key is resolved from the catalog rather than assumed to be the
/// conventional `id` / `document_id` / `key`: a collection created without an
/// explicit `PRIMARY KEY` gets an auto-generated `_rowid` key, so a filter on a
/// regular `id` column is NOT a point lookup and must stay a scan (routing it
/// to PointGet would resolve a surrogate for the wrong key and return zero
/// rows). A declared key of any name is a point lookup: `WHERE sku = 'p1'`
/// reads the one row keyed `p1`, as `WHERE id = 'p1'` does. When the catalog
/// cannot resolve a primary key (unknown collection), the conventional names
/// apply. A catalog error fails the plan: `RetryableSchemaChanged` must reach
/// the caller.
///
/// A point lookup returns the whole stored row and runs no expression. A
/// computed SELECT item over it becomes [`Projection::CpComputed`]: the
/// Control Plane evaluates it once on the returned row. `SELECT to_jsonb(*)
/// ... WHERE <key> = $1` stays a point lookup that way.
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
        } if filters.len() == 1 && !temporal.is_temporal() && has_point_lookup(engine) => {
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
                    projection: projection.iter().cloned().map(to_cp_computed).collect(),
                });
            }
            Ok(plan)
        }
        _ => Ok(plan),
    }
}

/// Whether the engine answers a key equality with a point lookup.
///
/// Timeseries and array refuse point lookups in their engine rules. A key
/// equality on them stays a scan.
fn has_point_lookup(engine: &EngineType) -> bool {
    match engine {
        EngineType::DocumentSchemaless
        | EngineType::DocumentStrict
        | EngineType::KeyValue
        | EngineType::Columnar
        | EngineType::Spatial => true,
        EngineType::Timeseries | EngineType::Array | EngineType::ContinuousAggregate => false,
    }
}

/// Extract a simple equality filter eligible for the point-get rewrite.
///
/// When the catalog resolves the primary key, the candidate column is that
/// key, whatever its name. A regular `id` column on a collection keyed by
/// another column stays a scan: a point lookup would resolve a surrogate for
/// the wrong key and return zero rows. When the catalog resolves no key, the
/// conventional document-key names (`id` / `document_id` / `key`) are the
/// candidates.
fn extract_pk_equality(filter: &Filter, pk: Option<&str>) -> Option<(String, SqlValue)> {
    let (column, value) = match &filter.expr {
        FilterExpr::Comparison {
            field,
            op: CompareOp::Eq,
            value,
        } => (field.as_str(), value),
        FilterExpr::Expr(SqlExpr::BinaryOp {
            left,
            op: BinaryOp::Eq,
            right,
        }) => match (left.as_ref(), right.as_ref()) {
            (SqlExpr::Column { name, .. }, SqlExpr::Literal(value)) => (name.as_str(), value),
            _ => return None,
        },
        _ => return None,
    };
    // `pk = NULL` is unknown and matches no row. A point lookup on the NULL
    // key would read the real key `''`.
    if matches!(value, SqlValue::Null) {
        return None;
    }
    key_column_named(column, pk).map(|key| (key, value.clone()))
}

/// The key column `column` names, spelled as the catalog declares it, or
/// `None` when `column` is not the key.
fn key_column_named(column: &str, pk: Option<&str>) -> Option<String> {
    match pk {
        Some(pk) => column.eq_ignore_ascii_case(pk).then(|| pk.to_string()),
        None => {
            let column = column.to_lowercase();
            matches!(column.as_str(), "id" | "document_id" | "key").then_some(column)
        }
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

    /// A schemaless `users` collection keyed by `id`.
    struct UsersCatalog;

    impl SqlCatalog for UsersCatalog {
        fn get_collection(
            &self,
            _: DatabaseId,
            name: &str,
        ) -> std::result::Result<Option<CollectionInfo>, SqlCatalogError> {
            Ok((name == "users").then(|| CollectionInfo {
                name: "users".to_string(),
                engine: EngineType::DocumentSchemaless,
                columns: Vec::new(),
                primary_key: Some("id".to_string()),
                has_auto_tier: false,
                indexes: Vec::new(),
                bitemporal: false,
                primary: nodedb_types::PrimaryEngine::Document,
                vector_primary: None,
                partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
                open_schema: CollectionInfo::open_schema_for(EngineType::DocumentSchemaless),
                text_indexes: None,
            }))
        }
    }

    #[test]
    fn a_computed_item_over_a_key_lookup_is_control_plane_computed() {
        let SqlPlan::Scan {
            collection,
            alias,
            engine,
            filters,
            sort_keys,
            limit,
            offset,
            distinct,
            window_functions,
            temporal,
            ..
        } = id_scan()
        else {
            panic!("id_scan is a scan");
        };
        let scan = SqlPlan::Scan {
            collection,
            alias,
            engine,
            filters,
            projection: vec![Projection::Computed {
                expr: SqlExpr::Function {
                    name: "to_jsonb".into(),
                    args: vec![SqlExpr::Wildcard],
                    distinct: false,
                },
                alias: "document".into(),
            }],
            sort_keys,
            limit,
            offset,
            distinct,
            window_functions,
            temporal,
        };
        match optimize(scan, &UsersCatalog).unwrap() {
            SqlPlan::PointGet { projection, .. } => assert!(matches!(
                projection.as_slice(),
                [Projection::CpComputed {
                    expr: SqlExpr::Function { name, args, .. },
                    alias,
                }] if name == "to_jsonb"
                    && matches!(args.as_slice(), [SqlExpr::Wildcard])
                    && alias == "document"
            )),
            other => panic!("expected a point lookup, got {other:?}"),
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

    /// A collection keyed by the declared column `Sku`.
    struct SkuCatalog;

    impl SqlCatalog for SkuCatalog {
        fn get_collection(
            &self,
            _: DatabaseId,
            name: &str,
        ) -> std::result::Result<Option<CollectionInfo>, SqlCatalogError> {
            Ok(Some(CollectionInfo {
                name: name.to_string(),
                engine: EngineType::DocumentSchemaless,
                columns: Vec::new(),
                primary_key: Some("Sku".to_string()),
                has_auto_tier: false,
                indexes: Vec::new(),
                bitemporal: false,
                primary: nodedb_types::PrimaryEngine::Document,
                vector_primary: None,
                partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
                open_schema: CollectionInfo::open_schema_for(EngineType::DocumentSchemaless),
                text_indexes: None,
            }))
        }
    }

    fn equality_scan(field: &str, engine: EngineType) -> SqlPlan {
        let SqlPlan::Scan {
            collection,
            alias,
            projection,
            sort_keys,
            limit,
            offset,
            distinct,
            window_functions,
            temporal,
            ..
        } = id_scan()
        else {
            panic!("id_scan is a scan");
        };
        SqlPlan::Scan {
            collection,
            alias,
            engine,
            filters: vec![Filter {
                expr: FilterExpr::Comparison {
                    field: field.into(),
                    op: CompareOp::Eq,
                    value: SqlValue::String("p1".into()),
                },
            }],
            projection,
            sort_keys,
            limit,
            offset,
            distinct,
            window_functions,
            temporal,
        }
    }

    #[test]
    fn a_declared_key_equality_is_a_point_lookup_on_that_key() {
        match optimize(
            equality_scan("sku", EngineType::DocumentSchemaless),
            &SkuCatalog,
        )
        .unwrap()
        {
            SqlPlan::PointGet {
                key_column,
                key_value,
                ..
            } => {
                assert_eq!(key_column, "Sku", "the catalog spelling names the key");
                assert_eq!(key_value, SqlValue::String("p1".into()));
            }
            other => panic!("expected a point lookup, got {other:?}"),
        }
    }

    #[test]
    fn an_id_column_beside_a_declared_key_stays_a_scan() {
        let plan = optimize(
            equality_scan("id", EngineType::DocumentSchemaless),
            &SkuCatalog,
        )
        .unwrap();
        assert!(matches!(plan, SqlPlan::Scan { .. }), "{plan:?}");
    }

    #[test]
    fn a_key_equality_on_an_engine_without_point_lookups_stays_a_scan() {
        for engine in [EngineType::Timeseries, EngineType::Array] {
            let plan = optimize(equality_scan("sku", engine), &SkuCatalog).unwrap();
            assert!(matches!(plan, SqlPlan::Scan { .. }), "{plan:?}");
        }
    }
}
