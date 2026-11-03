// SPDX-License-Identifier: Apache-2.0

//! A read with no LIMIT plans with no row cap.
//!
//! Aggregates, join aggregates, recursive CTEs, and spatial predicates once
//! planned a default row count (`10000` or `1000`) that the executor applied
//! as a silent cut: past it, rows were missing with nothing saying so. Each
//! now plans `usize::MAX`, and the Data Plane bounds the read by its scan
//! byte budget, which fails loudly. An explicit LIMIT still plans its value.

use nodedb_sql::planner::aggregate::AGGREGATE_NO_LIMIT;
use nodedb_sql::types::{CollectionInfo, ColumnInfo, EngineType, SqlDataType, SqlPlan};
use nodedb_sql::{SqlCatalog, SqlCatalogError, plan_sql};
use nodedb_types::DatabaseId;

fn column(name: &str, data_type: SqlDataType, is_primary_key: bool) -> ColumnInfo {
    ColumnInfo {
        name: name.into(),
        data_type,
        nullable: !is_primary_key,
        is_primary_key,
        default: None,
        raw_type: None,
        int_width: None,
        float_width: None,
    }
}

fn collection(name: &str, engine: EngineType, columns: Vec<ColumnInfo>) -> CollectionInfo {
    CollectionInfo {
        name: name.into(),
        engine,
        columns,
        primary_key: Some("id".into()),
        has_auto_tier: false,
        indexes: Vec::new(),
        bitemporal: false,
        primary: nodedb_types::PrimaryEngine::Document,
        vector_primary: None,
        partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
        open_schema: CollectionInfo::open_schema_for(engine),
    }
}

struct Catalog;

impl SqlCatalog for Catalog {
    fn get_collection(
        &self,
        _: DatabaseId,
        name: &str,
    ) -> std::result::Result<Option<CollectionInfo>, SqlCatalogError> {
        Ok(match name {
            "docs" => Some(collection(
                "docs",
                EngineType::DocumentStrict,
                vec![
                    column("id", SqlDataType::String, true),
                    column("g", SqlDataType::String, false),
                    column("parent", SqlDataType::String, false),
                ],
            )),
            "places" => Some(collection(
                "places",
                EngineType::Spatial,
                vec![
                    column("id", SqlDataType::String, true),
                    column("location", SqlDataType::Geometry, false),
                ],
            )),
            _ => None,
        })
    }

    fn lookup_array(&self, _name: &str) -> Option<nodedb_sql::types::ArrayCatalogView> {
        None
    }

    fn array_exists(&self, _name: &str) -> bool {
        false
    }
}

fn plan(sql: &str) -> SqlPlan {
    let mut plans = plan_sql(sql, &Catalog).unwrap_or_else(|e| panic!("plan {sql}: {e}"));
    assert_eq!(plans.len(), 1, "expected one plan for: {sql}");
    plans.pop().expect("one plan")
}

/// The debug form of a plan, searched for the row bound a nested node
/// carries.
fn rendered(sql: &str) -> String {
    format!("{:?}", plan(sql))
}

const UNBOUNDED: &str = "limit: 18446744073709551615";

#[test]
fn an_aggregate_without_limit_plans_every_group() {
    match plan("SELECT g, COUNT(*) FROM docs GROUP BY g") {
        SqlPlan::Aggregate { limit, .. } => assert_eq!(limit, AGGREGATE_NO_LIMIT),
        other => panic!("expected an aggregate, got {other:?}"),
    }
    assert_eq!(AGGREGATE_NO_LIMIT, usize::MAX);
}

#[test]
fn an_aggregate_keeps_an_explicit_limit() {
    match plan("SELECT g, COUNT(*) FROM docs GROUP BY g LIMIT 5") {
        SqlPlan::Aggregate { limit, .. } => assert_eq!(limit, 5),
        other => panic!("expected an aggregate, got {other:?}"),
    }
}

#[test]
fn a_join_aggregate_without_limit_plans_every_group() {
    let text =
        rendered("SELECT a.g, COUNT(*) FROM docs a JOIN docs b ON a.id = b.parent GROUP BY a.g");
    assert!(text.contains("Aggregate"), "{text}");
    assert!(text.contains(UNBOUNDED), "{text}");
    assert!(!text.contains("limit: 10000"), "{text}");
}

#[test]
fn a_recursive_cte_without_limit_plans_every_row() {
    let text = rendered(
        "WITH RECURSIVE tree AS (\
             SELECT * FROM docs WHERE id = 'root' \
             UNION ALL \
             SELECT d.* FROM docs d JOIN tree t ON d.parent = t.id\
         ) SELECT * FROM tree",
    );
    assert!(text.contains("RecursiveScan"), "{text}");
    assert!(text.contains(UNBOUNDED), "{text}");
    assert!(!text.contains("limit: 10000"), "{text}");
}

#[test]
fn a_spatial_predicate_without_limit_plans_every_row() {
    let text =
        rendered("SELECT id FROM places WHERE ST_DWithin(location, ST_Point(-122.4, 37.8), 1000)");
    assert!(text.contains("SpatialScan"), "{text}");
    assert!(text.contains(UNBOUNDED), "{text}");
}

/// A distance that is not a number is refused, never read as zero.
#[test]
fn a_spatial_distance_that_is_not_a_number_is_refused() {
    let result = plan_sql(
        "SELECT id FROM places WHERE ST_DWithin(location, ST_Point(-122.4, 37.8), 'far')",
        &Catalog,
    );
    assert!(result.is_err(), "a non-numeric distance must be refused");
}
