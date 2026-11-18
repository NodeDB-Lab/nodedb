// SPDX-License-Identifier: Apache-2.0

//! A column qualified with its own table, or its alias, plans as the bare
//! column in every predicate shape. A qualified name that reached the filter
//! matched no stored field, so `IN`, `NOT IN`, `BETWEEN` and `LIKE` matched
//! zero rows. A qualifier that names another table is refused in every shape.

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

struct Catalog;

impl SqlCatalog for Catalog {
    fn get_collection(
        &self,
        _: DatabaseId,
        name: &str,
    ) -> std::result::Result<Option<CollectionInfo>, SqlCatalogError> {
        Ok((name == "u1").then(|| CollectionInfo {
            name: "u1".into(),
            engine: EngineType::DocumentStrict,
            columns: vec![
                column("id", SqlDataType::String, true),
                column("n", SqlDataType::Int64, false),
                column("label", SqlDataType::String, false),
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

fn plan(sql: &str) -> SqlPlan {
    let mut plans = plan_sql(sql, &Catalog).unwrap_or_else(|e| panic!("plan {sql}: {e}"));
    assert_eq!(plans.len(), 1, "expected one plan for: {sql}");
    plans.pop().expect("one plan")
}

/// A column the filter reads carries its table when it is still qualified.
fn still_qualified(sql: &str) -> bool {
    format!("{:?}", plan(sql)).contains("table: Some")
}

#[test]
fn an_own_table_qualifier_is_stripped_in_every_predicate_shape() {
    for predicate in [
        "u1.id IN ('a', 'b')",
        "u1.id NOT IN ('a', 'b')",
        "u1.n BETWEEN 1 AND 2",
        "u1.n NOT BETWEEN 1 AND 2",
        "u1.label LIKE 'x%'",
        "u1.label ILIKE 'x%'",
        "upper(u1.label) = 'X'",
    ] {
        let sql = format!("SELECT id FROM u1 WHERE {predicate}");
        assert!(!still_qualified(&sql), "{sql}");
    }
}

#[test]
fn an_alias_qualifier_is_stripped_in_every_predicate_shape() {
    for predicate in [
        "z.id IN ('a', 'b')",
        "z.n BETWEEN 1 AND 2",
        "z.label LIKE 'x%'",
    ] {
        let sql = format!("SELECT z.id FROM u1 AS z WHERE {predicate}");
        assert!(!still_qualified(&sql), "{sql}");
    }
}

#[test]
fn a_foreign_qualifier_is_refused_in_every_predicate_shape() {
    for predicate in [
        "wrong.id IN ('a')",
        "wrong.n BETWEEN 1 AND 2",
        "wrong.label LIKE 'x%'",
        "upper(wrong.label) = 'X'",
    ] {
        let sql = format!("SELECT id FROM u1 WHERE {predicate}");
        let error = plan_sql(&sql, &Catalog).expect_err(&sql).to_string();
        assert!(error.contains("wrong"), "{sql}: {error}");
    }
}
