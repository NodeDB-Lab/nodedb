// SPDX-License-Identifier: Apache-2.0

//! `HAVING` makes a SELECT an aggregate query, with or without GROUP BY.
//!
//! The clause filters groups, so a plan never drops it. A star beside it has
//! no GROUP BY key to read and is refused as a grouping error.

use nodedb_sql::types::{CollectionInfo, EngineType};
use nodedb_sql::{SqlCatalog, SqlCatalogError, SqlError, SqlPlan, plan_sql};
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
            text_indexes: None,
        }))
    }

    fn lookup_array(&self, _name: &str) -> Option<nodedb_sql::types::ArrayCatalogView> {
        None
    }

    fn array_exists(&self, _name: &str) -> bool {
        false
    }
}

#[test]
fn having_without_group_by_plans_an_aggregate_that_keeps_the_clause() {
    let sql = "SELECT 1 AS one FROM d HAVING COUNT(*) > 1";
    let mut plans = plan_sql(sql, &Catalog).unwrap_or_else(|e| panic!("{sql}: {e}"));
    let plan = plans.pop().expect("one plan");
    let SqlPlan::Subquery { input, .. } = plan else {
        panic!("expected the aggregate under a projection, got {plan:?}");
    };
    match *input {
        SqlPlan::Aggregate { having, .. } => {
            assert_eq!(having.len(), 1, "the HAVING predicate is kept")
        }
        other => panic!("expected an aggregate, got {other:?}"),
    }
}

#[test]
fn star_with_having_and_no_group_by_is_a_grouping_error() {
    let sql = "SELECT * FROM d HAVING COUNT(*) > 1";
    match plan_sql(sql, &Catalog) {
        Err(SqlError::UngroupedColumn { column }) => assert_eq!(column, "*"),
        other => panic!("expected UngroupedColumn, got {other:?}"),
    }
}
