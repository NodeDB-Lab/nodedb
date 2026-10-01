// SPDX-License-Identifier: BUSL-1.1

//! Catalog and plan fixtures shared by schema responsibility tests.

use nodedb_sql::catalog::SqlCatalog;
use nodedb_sql::types::SqlPlan;
use nodedb_sql::types::query::Projection;
use nodedb_sql::types_expr::SqlExpr;

/// Catalog stub whose `get_collection` is never called by the
/// `ConstantResult` branch under test; only required to satisfy the
/// generic `SqlCatalog` bound on `build_output_schema`.
pub(super) struct NoCatalog;

impl SqlCatalog for NoCatalog {
    fn get_collection(
        &self,
        _database_id: nodedb_types::DatabaseId,
        _name: &str,
    ) -> Result<Option<nodedb_sql::types::CollectionInfo>, nodedb_sql::catalog::SqlCatalogError>
    {
        Ok(None)
    }
}

/// Minimal `Scan` plan against `collection`, used only to exercise
/// recursion (Union/Intersect/Except/Cte); the catalog is `NoCatalog`
/// so every column falls back to `DdlColType::Text`.
pub(super) fn scan_plan(collection: &str, projection: Vec<Projection>) -> SqlPlan {
    SqlPlan::Scan {
        collection: collection.to_string(),
        alias: None,
        engine: nodedb_sql::types::query::EngineType::DocumentSchemaless,
        filters: Vec::new(),
        projection,
        sort_keys: Vec::new(),
        limit: None,
        offset: 0,
        distinct: false,
        window_functions: Vec::new(),
        temporal: nodedb_sql::temporal::TemporalScope::default(),
    }
}

/// Catalog stub exposing a single `metrics` collection with a text
/// `region`, integer `n`, and float `amount` column — used to exercise
/// catalog-backed type resolution for GROUP BY keys, aggregate arguments,
/// and bare-column computed projections.
pub(super) struct TypedCatalog;

impl SqlCatalog for TypedCatalog {
    fn get_collection(
        &self,
        _database_id: nodedb_types::DatabaseId,
        name: &str,
    ) -> Result<Option<nodedb_sql::types::CollectionInfo>, nodedb_sql::catalog::SqlCatalogError>
    {
        use nodedb_sql::types::collection::ColumnInfo;
        use nodedb_sql::types::query::EngineType;
        use nodedb_sql::types_expr::SqlDataType;

        if name != "metrics" {
            return Ok(None);
        }
        let col = |n: &str, t: SqlDataType| ColumnInfo {
            name: n.to_string(),
            data_type: t,
            nullable: true,
            is_primary_key: false,
            default: None,
            raw_type: None,
            int_width: None,
            float_width: None,
        };
        Ok(Some(nodedb_sql::types::CollectionInfo {
            name: "metrics".to_string(),
            engine: EngineType::DocumentStrict,
            columns: vec![
                col("region", SqlDataType::String),
                col("n", SqlDataType::Int64),
                col("amount", SqlDataType::Float64),
            ],
            primary_key: None,
            has_auto_tier: false,
            indexes: Vec::new(),
            bitemporal: false,
            primary: nodedb_types::PrimaryEngine::Document,
            vector_primary: None,
            partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
            open_schema: nodedb_sql::types::CollectionInfo::open_schema_for(
                EngineType::DocumentStrict,
            ),
        }))
    }
}

pub(super) fn agg_expr(
    function: &str,
    args: Vec<SqlExpr>,
    alias: &str,
) -> nodedb_sql::types::query::AggregateExpr {
    nodedb_sql::types::query::AggregateExpr {
        function: function.to_string(),
        args,
        alias: alias.to_string(),
        distinct: false,
        grouping_col_index: None,
    }
}

pub(super) fn metrics_column(name: &str) -> SqlExpr {
    SqlExpr::Column {
        table: None,
        name: name.to_string(),
    }
}
