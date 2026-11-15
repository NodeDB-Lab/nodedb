// SPDX-License-Identifier: BUSL-1.1

//! A text of several statements that the preprocess rewrote is refused: the
//! rewrite flags describe one statement, so each statement must be planned alone.

use nodedb_sql::types::CollectionInfo;
use nodedb_sql::{SqlCatalog, SqlCatalogError, SqlError};
use nodedb_types::DatabaseId;

/// A catalog with no collection: the guard fires before any lookup.
struct Empty;

impl SqlCatalog for Empty {
    fn get_collection(
        &self,
        _: DatabaseId,
        _: &str,
    ) -> std::result::Result<Option<CollectionInfo>, SqlCatalogError> {
        Ok(None)
    }

    fn lookup_array(&self, _name: &str) -> Option<nodedb_sql::types::ArrayCatalogView> {
        None
    }

    fn array_exists(&self, _name: &str) -> bool {
        false
    }
}

fn refused(sql: &str) -> bool {
    matches!(
        nodedb_sql::plan_sql(sql, &Empty),
        Err(SqlError::Unsupported { .. })
    )
}

#[test]
fn an_upsert_beside_another_statement_is_refused() {
    assert!(refused(
        "UPSERT INTO docs (id, n) VALUES ('a', 1); SELECT 1"
    ));
}

#[test]
fn an_object_literal_beside_another_statement_is_refused() {
    assert!(refused("INSERT INTO docs { id: 'a' }; SELECT 1"));
}

#[test]
fn a_plain_batch_is_not_refused_by_the_guard() {
    assert!(!refused("SELECT 1; SELECT 2"));
}
