// SPDX-License-Identifier: BUSL-1.1

//! The column positions of each set-operation branch.
//!
//! `UNION`, `INTERSECT` and `EXCEPT` compare rows by column position. Each
//! branch's positions are the row keys of its output columns in SELECT-list
//! order, read from the same output schema the response announces.
//!
//! A `*` expands into its source's declared column order when that order is
//! known:
//!
//! - A collection that accepts no undeclared column (strict, columnar,
//!   timeseries, KV, continuous aggregate) expands into its declared columns.
//! - A derived table or CTE expands into its body's output columns, when the
//!   body's own positions are known by this same rule.
//!
//! A `*` over a schemaless source has no positional order: its rows carry
//! whatever fields were stored. Such a branch declares no positions, and the
//! merge compares by column name instead (see `set_op_merge::row_key`). A `*`
//! over a join, a lateral join or any other source with no single declared
//! order counts as schemaless too.

use nodedb_sql::SqlError;
use nodedb_sql::catalog::{SqlCatalog, SqlCatalogError};
use nodedb_sql::types::{
    CollectionInfo, CtePlan, DocumentIndexLookupPlan, HybridSearchPlan, HybridSearchTriplePlan,
    RangeScanPlan, RecursiveScanPlan, SqlPlan, TextSearchPlan, TimeseriesScanPlan,
};

use super::build::build_output_schema;
use super::cte_types::CteTypesCatalog;

/// A catalog that resolves no collection, for a converter that holds no
/// catalog. Every `*` it sees is schemaless.
pub struct NoCollectionCatalog;

impl SqlCatalog for NoCollectionCatalog {
    fn get_collection(
        &self,
        _database_id: nodedb_types::DatabaseId,
        _name: &str,
    ) -> Result<Option<CollectionInfo>, SqlCatalogError> {
        Ok(None)
    }
}

/// The row keys of `branch`'s output columns in position order.
///
/// Empty for a branch whose `*` reads a schemaless source: it declares no
/// positions. A catalog lookup error is returned.
pub fn set_op_branch_columns(
    branch: &SqlPlan,
    catalog: &dyn SqlCatalog,
    database_id: nodedb_types::DatabaseId,
) -> Result<Vec<String>, SqlError> {
    let schema = build_output_schema(std::slice::from_ref(branch), catalog, database_id, None)?;
    if schema.is_star && !star_order_known(branch, catalog, database_id, &[])? {
        return Ok(Vec::new());
    }
    Ok(schema.columns.into_iter().map(|c| c.lookup_key).collect())
}

/// Whether the `*` of `plan` expands into a known column order. `ctes`
/// holds, for each CTE definition in scope, whether its body's order is
/// known.
fn star_order_known(
    plan: &SqlPlan,
    catalog: &dyn SqlCatalog,
    database_id: nodedb_types::DatabaseId,
    ctes: &[(String, bool)],
) -> Result<bool, SqlError> {
    if let Some(collection) = star_source(plan) {
        if let Some((_, known)) = ctes.iter().rev().find(|(name, _)| name == collection) {
            return Ok(*known);
        }
        return Ok(catalog
            .get_collection(database_id, collection)?
            .is_some_and(|info| !info.open_schema && !info.columns.is_empty()));
    }
    match plan {
        // A derived table's `*` reads its body's announced columns.
        SqlPlan::Subquery { input, .. } => body_order_known(input, catalog, database_id, ctes),
        SqlPlan::Cte(CtePlan { definitions, outer }) => {
            let mut scope = ctes.to_vec();
            for (index, (name, body)) in definitions.iter().enumerate() {
                // A definition reads only the definitions before it.
                let earlier = CteTypesCatalog::build(catalog, &definitions[..index], database_id)?;
                let known = body_order_known(body, &earlier, database_id, &scope)?;
                scope.push((name.clone(), known));
            }
            let overlay = CteTypesCatalog::build(catalog, definitions, database_id)?;
            star_order_known(outer, &overlay, database_id, &scope)
        }
        // A set operation announces its first branch's columns.
        SqlPlan::Union { inputs, .. } => match inputs.first() {
            Some(first) => star_order_known(first, catalog, database_id, ctes),
            None => Ok(false),
        },
        SqlPlan::Intersect { left, .. } | SqlPlan::Except { left, .. } => {
            star_order_known(left, catalog, database_id, ctes)
        }
        // A join, a lateral join or any other plan has no single declared
        // column order for a `*` to expand into.
        _ => Ok(false),
    }
}

/// Whether every column of `body`'s output has a known position.
fn body_order_known(
    body: &SqlPlan,
    catalog: &dyn SqlCatalog,
    database_id: nodedb_types::DatabaseId,
    ctes: &[(String, bool)],
) -> Result<bool, SqlError> {
    let schema = build_output_schema(std::slice::from_ref(body), catalog, database_id, None)?;
    Ok(!schema.is_star || star_order_known(body, catalog, database_id, ctes)?)
}

/// The collection a single-relation read plan's `*` expands against.
fn star_source(plan: &SqlPlan) -> Option<&str> {
    match plan {
        SqlPlan::Scan { collection, .. }
        | SqlPlan::DocumentIndexLookup(DocumentIndexLookupPlan { collection, .. })
        | SqlPlan::SpatialScan { collection, .. }
        | SqlPlan::TimeseriesScan(TimeseriesScanPlan { collection, .. })
        | SqlPlan::PointGet { collection, .. }
        | SqlPlan::RangeScan(RangeScanPlan { collection, .. })
        | SqlPlan::RecursiveScan(RecursiveScanPlan { collection, .. })
        | SqlPlan::VectorSearch { collection, .. }
        | SqlPlan::MultiVectorSearch { collection, .. }
        | SqlPlan::SparseSearch { collection, .. }
        | SqlPlan::TextSearch(TextSearchPlan { collection, .. })
        | SqlPlan::HybridSearch(HybridSearchPlan { collection, .. })
        | SqlPlan::HybridSearchTriple(HybridSearchTriplePlan { collection, .. }) => {
            Some(collection.as_str())
        }
        _ => None,
    }
}

/// Refuse branches whose declared column counts differ, as SQL does.
/// A schemaless `*` branch declares no count and is not compared.
pub fn check_set_op_arity(columns: &[Vec<String>]) -> Result<(), SqlError> {
    let mut declared = columns.iter().filter(|c| !c.is_empty()).map(Vec::len);
    let Some(first) = declared.next() else {
        return Ok(());
    };
    if declared.any(|count| count != first) {
        return Err(SqlError::Arity {
            detail: "each UNION / INTERSECT / EXCEPT query must have the same number of columns"
                .to_string(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_sql::types::collection::ColumnInfo;
    use nodedb_sql::types::query::{EngineType, Projection};
    use nodedb_sql::types_expr::{SqlDataType, SqlValue};

    const DB: nodedb_types::DatabaseId = nodedb_types::DatabaseId::DEFAULT;

    /// `strict` declares `a, b, c`. `loose` is schemaless.
    struct Catalog;

    impl SqlCatalog for Catalog {
        fn get_collection(
            &self,
            _database_id: nodedb_types::DatabaseId,
            name: &str,
        ) -> Result<Option<CollectionInfo>, SqlCatalogError> {
            let (engine, declared): (EngineType, &[&str]) = match name {
                "strict" => (EngineType::DocumentStrict, &["a", "b", "c"]),
                "loose" => (EngineType::DocumentSchemaless, &[]),
                _ => return Ok(None),
            };
            let open_schema = CollectionInfo::open_schema_for(engine);
            Ok(Some(CollectionInfo {
                name: name.to_string(),
                engine,
                columns: declared
                    .iter()
                    .map(|n| ColumnInfo {
                        name: (*n).to_string(),
                        data_type: SqlDataType::Int64,
                        nullable: true,
                        is_primary_key: false,
                        default: None,
                        raw_type: None,
                        int_width: None,
                        float_width: None,
                    })
                    .collect(),
                primary_key: None,
                has_auto_tier: false,
                indexes: Vec::new(),
                bitemporal: false,
                primary: nodedb_types::PrimaryEngine::Document,
                vector_primary: None,
                partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
                open_schema,
            }))
        }
    }

    fn scan_with(collection: &str, projection: Vec<Projection>) -> SqlPlan {
        SqlPlan::Scan {
            collection: collection.to_string(),
            alias: None,
            engine: EngineType::DocumentSchemaless,
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

    fn scan(collection: &str, columns: &[&str]) -> SqlPlan {
        scan_with(
            collection,
            columns
                .iter()
                .map(|c| Projection::Column((*c).to_string()))
                .collect(),
        )
    }

    fn star(collection: &str) -> SqlPlan {
        scan_with(collection, vec![Projection::Star])
    }

    fn derived(input: SqlPlan) -> SqlPlan {
        SqlPlan::Subquery {
            input: Box::new(input),
            filters: Vec::new(),
            projection: Vec::new(),
            window_functions: Vec::new(),
            sort_keys: Vec::new(),
            offset: 0,
            distinct: false,
            limit: None,
        }
    }

    fn keys(columns: &[&str]) -> Vec<String> {
        columns.iter().map(|c| (*c).to_string()).collect()
    }

    fn columns_of(branch: &SqlPlan) -> Vec<String> {
        set_op_branch_columns(branch, &Catalog, DB).expect("branch columns")
    }

    #[test]
    fn a_branch_names_its_columns_in_select_list_order() {
        assert_eq!(columns_of(&scan("loose", &["b", "a"])), keys(&["b", "a"]));
    }

    /// A `*` over a strict collection expands into its declared order, so
    /// `SELECT * FROM strict UNION SELECT x, y, z FROM loose` compares `a`
    /// with `x`, `b` with `y` and `c` with `z`.
    #[test]
    fn a_strict_star_expands_into_declared_order() {
        let columns = vec![
            columns_of(&star("strict")),
            columns_of(&scan("loose", &["x", "y", "z"])),
        ];
        assert_eq!(columns[0], keys(&["a", "b", "c"]));
        assert!(check_set_op_arity(&columns).is_ok());
    }

    /// A strict `*` has a column count, so a branch of another count is
    /// SQL's arity error.
    #[test]
    fn a_strict_star_of_another_arity_is_refused() {
        let columns = vec![
            columns_of(&star("strict")),
            columns_of(&scan("loose", &["x", "y"])),
        ];
        assert!(matches!(
            check_set_op_arity(&columns),
            Err(SqlError::Arity { .. })
        ));
    }

    /// A `*` over a schemaless collection, or with no catalog, has no order.
    #[test]
    fn a_schemaless_star_declares_no_positions() {
        assert!(columns_of(&star("loose")).is_empty());
        assert!(
            set_op_branch_columns(&star("strict"), &NoCollectionCatalog, DB)
                .expect("branch columns")
                .is_empty()
        );
    }

    /// A derived table's `*` takes its body's positions: known for a body
    /// with a SELECT list or a strict `*`, unknown for a schemaless `*`.
    #[test]
    fn a_derived_table_star_takes_its_body_positions() {
        assert_eq!(
            columns_of(&derived(scan("loose", &["q", "p"]))),
            keys(&["q", "p"])
        );
        assert_eq!(columns_of(&derived(star("strict"))), keys(&["a", "b", "c"]));
        assert!(columns_of(&derived(star("loose"))).is_empty());
    }

    /// A CTE read with `*` takes its definition body's positions.
    #[test]
    fn a_cte_star_takes_its_definition_positions() {
        let cte = |body: SqlPlan| {
            SqlPlan::Cte(CtePlan {
                definitions: vec![("d".to_string(), body)],
                outer: Box::new(star("d")),
            })
        };
        assert_eq!(
            columns_of(&cte(scan("loose", &["q", "p"]))),
            keys(&["q", "p"])
        );
        assert!(columns_of(&cte(star("loose"))).is_empty());
    }

    /// A constant branch (`SELECT 1 UNION SELECT 2`) is positional. Each
    /// cell's key is the per-column key its row is written under, so a
    /// repeated output name keeps a distinct position.
    #[test]
    fn constant_branches_are_positional() {
        let constant = |columns: &[&str]| SqlPlan::ConstantResult {
            columns: keys(columns),
            values: columns.iter().map(|_| SqlValue::Int(1)).collect(),
            cast_types: columns.iter().map(|_| None).collect(),
            volatile: false,
        };
        assert_eq!(columns_of(&constant(&["?column?"])), keys(&["?column?"]));
        assert_eq!(
            columns_of(&constant(&["a", "a"])),
            crate::control::server::response_shape::project::cell_keys(&keys(&["a", "a"]))
        );
    }

    #[test]
    fn branches_of_different_column_counts_are_refused() {
        let two = keys(&["a", "b"]);
        let one = keys(&["x"]);
        assert!(matches!(
            check_set_op_arity(&[two.clone(), one]),
            Err(SqlError::Arity { .. })
        ));
        assert!(check_set_op_arity(&[two.clone(), two.clone()]).is_ok());
        assert!(check_set_op_arity(&[two, Vec::new()]).is_ok());
    }
}
