// SPDX-License-Identifier: Apache-2.0

//! Catalog capability tests for materialized relation column aliases.

use crate::catalog::{SqlCatalog, SqlCatalogError};
use crate::types::CollectionInfo;
use nodedb_types::DatabaseId;

struct DefaultCatalog;

impl SqlCatalog for DefaultCatalog {
    fn get_collection(
        &self,
        _database: DatabaseId,
        name: &str,
    ) -> Result<Option<CollectionInfo>, SqlCatalogError> {
        Ok(matches!(name, "users" | "orders")
            .then(|| crate::resolver::derived::open_subquery_relation(name)))
    }
}

struct RestrictedCatalog;

impl SqlCatalog for RestrictedCatalog {
    fn get_collection(
        &self,
        database: DatabaseId,
        name: &str,
    ) -> Result<Option<CollectionInfo>, SqlCatalogError> {
        DefaultCatalog.get_collection(database, name)
    }

    fn supports_materialized_relation_column_aliases(&self) -> bool {
        false
    }
}

const QUERIES: &[&str] = &[
    "WITH c(x) AS (SELECT 1 AS n) SELECT x FROM c",
    "SELECT x FROM (SELECT 1 AS n) AS d(x)",
    "SELECT a.x FROM (SELECT 1 AS n) AS a(x) JOIN (SELECT 1 AS n) AS b ON a.x = b.n",
    "WITH c AS (SELECT 1 AS n) SELECT x FROM (SELECT n FROM c) AS d(x)",
    "WITH c AS (SELECT x FROM (SELECT 1 AS n) AS d(x)) SELECT x FROM c",
];

#[test]
fn default_catalog_accepts_materialized_relation_column_aliases() {
    for sql in QUERIES {
        crate::plan_sql(sql, &DefaultCatalog).unwrap_or_else(|error| panic!("{sql}: {error}"));
    }
}

#[test]
fn restricted_catalog_refuses_materialized_relation_column_aliases() {
    for sql in QUERIES {
        let error = crate::plan_sql(sql, &RestrictedCatalog).expect_err("unsupported alias list");
        assert!(
            matches!(error, crate::SqlError::Unsupported { .. }),
            "{sql}: {error}"
        );
    }
}

#[test]
fn restricted_catalog_keeps_recursive_column_aliases() {
    crate::plan_sql("WITH RECURSIVE nums(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM nums WHERE n < 3) SELECT n FROM nums", &RestrictedCatalog)
        .expect("recursive value operator carries its column names");
}

#[test]
fn restricted_catalog_keeps_lateral_column_aliases() {
    crate::plan_sql("SELECT u.id FROM users u JOIN LATERAL (SELECT o.id FROM orders o WHERE o.user_id = u.id ORDER BY o.score DESC LIMIT 1) best(id) ON true", &RestrictedCatalog)
        .expect("dedicated lateral path remains available");
}
