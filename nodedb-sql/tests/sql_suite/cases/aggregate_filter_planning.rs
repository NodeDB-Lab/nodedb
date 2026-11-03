// SPDX-License-Identifier: Apache-2.0

//! `FILTER (WHERE ...)` is folded into the aggregate argument at plan time.
//!
//! A filtered aggregate's argument becomes `CASE WHEN <pred> THEN <arg>
//! END`, and `COUNT(*)` becomes `COUNT(CASE WHEN <pred> THEN 1 END)`. The
//! aggregate keeps the filter text for its output key, so it never shares a
//! key with the same aggregate unfiltered.

use nodedb_sql::planner::agg_naming::aggregate_output_key;
use nodedb_sql::types::{AggregateExpr, CollectionInfo, EngineType, SqlExpr};
use nodedb_sql::{SqlCatalog, SqlCatalogError, SqlPlan, plan_sql};
use nodedb_types::DatabaseId;

struct Catalog;

impl SqlCatalog for Catalog {
    fn get_collection(
        &self,
        _: DatabaseId,
        name: &str,
    ) -> std::result::Result<Option<CollectionInfo>, SqlCatalogError> {
        if name != "d" {
            return Ok(None);
        }
        Ok(Some(CollectionInfo {
            name: name.into(),
            engine: EngineType::DocumentSchemaless,
            columns: Vec::new(),
            primary_key: Some("id".into()),
            has_auto_tier: false,
            indexes: Vec::new(),
            bitemporal: false,
            primary: nodedb_types::PrimaryEngine::Document,
            vector_primary: None,
            partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
            open_schema: CollectionInfo::open_schema_for(EngineType::DocumentSchemaless),
        }))
    }

    fn lookup_array(&self, _name: &str) -> Option<nodedb_sql::types::ArrayCatalogView> {
        None
    }

    fn array_exists(&self, _name: &str) -> bool {
        false
    }
}

fn aggregates(sql: &str) -> Vec<AggregateExpr> {
    let mut plans = plan_sql(sql, &Catalog).unwrap_or_else(|e| panic!("{sql}: {e}"));
    assert_eq!(plans.len(), 1, "expected exactly one plan for: {sql}");
    match plans.pop().expect("one plan") {
        SqlPlan::Aggregate { aggregates, .. } => aggregates,
        other => panic!("expected an aggregate, got {other:?}"),
    }
}

#[test]
fn filter_folds_into_a_case_argument() {
    let aggs = aggregates("SELECT SUM(v) FILTER (WHERE v > 1) FROM d");
    assert_eq!(aggs.len(), 1);
    assert!(
        matches!(aggs[0].args.first(), Some(SqlExpr::Case { .. })),
        "the argument is a CASE: {:?}",
        aggs[0].args
    );
    assert!(aggs[0].filter_text.is_some(), "the filter text is kept");
}

#[test]
fn filtered_count_star_counts_a_case() {
    let aggs = aggregates("SELECT COUNT(*) FILTER (WHERE v > 1) FROM d");
    assert!(
        matches!(aggs[0].args.first(), Some(SqlExpr::Case { .. })),
        "COUNT(*) counts a CASE: {:?}",
        aggs[0].args
    );
}

#[test]
fn filtered_and_unfiltered_keys_differ() {
    let aggs = aggregates(
        "SELECT COUNT(*), COUNT(*) FILTER (WHERE v > 1), COUNT(*) FILTER (WHERE v > 2) FROM d",
    );
    let keys: Vec<String> = aggs.iter().map(aggregate_output_key).collect();
    assert_eq!(keys.len(), 3);
    assert_ne!(keys[0], keys[1], "{keys:?}");
    assert_ne!(keys[1], keys[2], "{keys:?}");
    assert_ne!(keys[0], keys[2], "{keys:?}");
}

#[test]
fn having_binds_to_the_filtered_aggregate() {
    let sql = "SELECT host, COUNT(*) FILTER (WHERE v > 1) AS n FROM d \
               GROUP BY host HAVING COUNT(*) FILTER (WHERE v > 1) > 0";
    let mut plans = plan_sql(sql, &Catalog).unwrap_or_else(|e| panic!("{sql}: {e}"));
    match plans.pop().expect("one plan") {
        SqlPlan::Aggregate {
            aggregates, having, ..
        } => {
            assert_eq!(aggregates.len(), 1, "HAVING reuses the selected aggregate");
            assert_eq!(having.len(), 1, "one HAVING predicate");
        }
        other => panic!("expected an aggregate, got {other:?}"),
    }
}
