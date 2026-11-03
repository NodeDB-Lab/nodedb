// SPDX-License-Identifier: Apache-2.0

//! Integration tests: schema-qualified names are rejected at plan time.
//!
//! All tests use `plan_sql()` with a minimal catalog that knows about
//! `users` and `orders`. Every case that uses a schema prefix (`public.X`,
//! `db.public.X`) must produce `SqlError::Unsupported`.

use nodedb_sql::types::{CollectionInfo, EngineType};
use nodedb_sql::{SqlCatalog, SqlCatalogError, SqlError, plan_sql};
use nodedb_types::DatabaseId;

struct Catalog;

impl SqlCatalog for Catalog {
    fn get_collection(
        &self,
        _: DatabaseId,
        name: &str,
    ) -> std::result::Result<Option<CollectionInfo>, SqlCatalogError> {
        let info = match name {
            "users" => Some(CollectionInfo {
                name: "users".into(),
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
            }),
            "orders" => Some(CollectionInfo {
                name: "orders".into(),
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
            }),
            _ => None,
        };
        Ok(info)
    }

    fn lookup_array(&self, _name: &str) -> Option<nodedb_sql::types::ArrayCatalogView> {
        None
    }

    fn array_exists(&self, _name: &str) -> bool {
        false
    }
}

fn is_unsupported(result: nodedb_sql::Result<Vec<nodedb_sql::SqlPlan>>) -> bool {
    matches!(result, Err(SqlError::Unsupported { .. }))
}

// ── FROM clause ───────────────────────────────────────────────────────────────

#[test]
fn select_from_qualified_table_rejected() {
    assert!(
        is_unsupported(plan_sql("SELECT * FROM public.users", &Catalog)),
        "SELECT * FROM public.users should be rejected"
    );
}

// ── Column references ─────────────────────────────────────────────────────────

#[test]
fn select_qualified_column_ref_rejected() {
    assert!(
        is_unsupported(plan_sql("SELECT public.users.id FROM users", &Catalog)),
        "SELECT public.users.id FROM users should be rejected"
    );
}

// ── INSERT ────────────────────────────────────────────────────────────────────

#[test]
fn insert_into_qualified_table_rejected() {
    assert!(
        is_unsupported(plan_sql(
            "INSERT INTO public.users (id, name) VALUES (1, 'Alice')",
            &Catalog
        )),
        "INSERT INTO public.users should be rejected"
    );
}

// ── UPDATE ────────────────────────────────────────────────────────────────────

#[test]
fn update_qualified_table_rejected() {
    assert!(
        is_unsupported(plan_sql(
            "UPDATE public.users SET name = 'Bob' WHERE id = 1",
            &Catalog
        )),
        "UPDATE public.users should be rejected"
    );
}

// ── DELETE ────────────────────────────────────────────────────────────────────

#[test]
fn delete_from_qualified_table_rejected() {
    assert!(
        is_unsupported(plan_sql("DELETE FROM public.users WHERE id = 1", &Catalog)),
        "DELETE FROM public.users should be rejected"
    );
}

// ── JOIN ──────────────────────────────────────────────────────────────────────

#[test]
fn join_qualified_table_rejected() {
    assert!(
        is_unsupported(plan_sql(
            "SELECT * FROM users JOIN public.orders ON users.id = orders.user_id",
            &Catalog
        )),
        "JOIN public.orders should be rejected"
    );
}

// ── CTE ───────────────────────────────────────────────────────────────────────

#[test]
fn cte_with_qualified_table_in_body_rejected() {
    assert!(
        is_unsupported(plan_sql(
            "WITH cte AS (SELECT * FROM users) SELECT * FROM public.cte",
            &Catalog
        )),
        "SELECT * FROM public.cte should be rejected"
    );
}

// ── Subquery ──────────────────────────────────────────────────────────────────

#[test]
fn subquery_with_qualified_column_rejected() {
    assert!(
        is_unsupported(plan_sql("SELECT public.users.id FROM users", &Catalog)),
        "subquery with public.users.id should be rejected"
    );
}
