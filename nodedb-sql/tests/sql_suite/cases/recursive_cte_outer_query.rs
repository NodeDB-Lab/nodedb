// SPDX-License-Identifier: Apache-2.0

//! The outer query of a `WITH RECURSIVE` reads the recursion by name, so its
//! SELECT list, WHERE and LIMIT apply to the recursion's rows. Before, the
//! plan was the recursion alone and the outer query was dropped.

use nodedb_sql::types::{CollectionInfo, ColumnInfo, EngineType, Projection, SqlDataType, SqlPlan};
use nodedb_sql::{SqlCatalog, SqlCatalogError, plan_sql};
use nodedb_types::DatabaseId;

fn column(name: &str, is_primary_key: bool) -> ColumnInfo {
    ColumnInfo {
        name: name.into(),
        data_type: SqlDataType::String,
        nullable: !is_primary_key,
        is_primary_key,
        default: None,
        raw_type: None,
        int_width: None,
        float_width: None,
    }
}

struct Catalog;

impl SqlCatalog for Catalog {
    fn get_collection(
        &self,
        _: DatabaseId,
        name: &str,
    ) -> std::result::Result<Option<CollectionInfo>, SqlCatalogError> {
        Ok((name == "docs").then(|| CollectionInfo {
            name: "docs".into(),
            engine: EngineType::DocumentStrict,
            columns: vec![
                column("id", true),
                column("g", false),
                column("parent", false),
            ],
            primary_key: Some("id".into()),
            has_auto_tier: false,
            indexes: Vec::new(),
            bitemporal: false,
            primary: nodedb_types::PrimaryEngine::Document,
            vector_primary: None,
            partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
            open_schema: CollectionInfo::open_schema_for(EngineType::DocumentStrict),
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

const CTE: &str = "WITH RECURSIVE tree AS (\
    SELECT * FROM docs WHERE id = 'root' \
    UNION ALL \
    SELECT d.* FROM docs d JOIN tree t ON d.parent = t.id\
) ";

/// The plan of `CTE` followed by `outer`, split into the recursion and the
/// outer scan.
fn planned(outer: &str) -> (SqlPlan, SqlPlan) {
    let sql = format!("{CTE}{outer}");
    let mut plans = plan_sql(&sql, &Catalog).unwrap_or_else(|e| panic!("plan {sql}: {e}"));
    let SqlPlan::Cte(cte) = plans.pop().expect("one plan") else {
        panic!("expected a CTE plan for: {sql}");
    };
    let (_, recursion) = cte.definitions.into_iter().next().expect("one definition");
    (recursion, *cte.outer)
}

#[test]
fn the_recursion_is_the_definition() {
    let (recursion, _) = planned("SELECT * FROM tree");
    assert!(
        matches!(recursion, SqlPlan::RecursiveScan(_)),
        "{recursion:?}"
    );
}

#[test]
fn the_outer_select_list_is_planned() {
    let (_, outer) = planned("SELECT id FROM tree");
    let SqlPlan::Scan { projection, .. } = outer else {
        panic!("expected an outer scan, got {outer:?}");
    };
    assert_eq!(projection.len(), 1, "{projection:?}");
    assert!(matches!(&projection[0], Projection::Column(name) if name == "id"));
}

#[test]
fn the_outer_where_and_limit_are_planned() {
    let (_, outer) = planned("SELECT id FROM tree WHERE g = 'x' LIMIT 2");
    let SqlPlan::Scan { filters, limit, .. } = outer else {
        panic!("expected an outer scan, got {outer:?}");
    };
    assert_eq!(filters.len(), 1, "{filters:?}");
    assert_eq!(limit, Some(2));
}
