//! DDL handlers for strict document collection operations.

use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::error::LiteError;
use crate::query::engine::LiteQueryEngine;
use crate::storage::engine::StorageEngine;

use super::parser::parse_strict_create_sql;

impl<S: StorageEngine> LiteQueryEngine<S> {
    /// Handle: CREATE COLLECTION <name> (<col_defs>) WITH storage = 'strict'
    pub(in crate::query) async fn handle_create_strict(
        &self,
        sql: &str,
    ) -> Result<QueryResult, LiteError> {
        let (name, schema) = parse_strict_create_sql(sql)?;

        // StrictEngine::create_collection is async (uses storage), so we must
        // not hold the std::sync::MutexGuard across the await. Instead, clone
        // the Arc and acquire inside a block_in_place or use a scoped approach.
        // Since StrictEngine methods take &mut self, we need the guard — but we
        // can use block_in_place to avoid Send requirements.
        {
            let mut strict = match self.strict.lock() {
                Ok(s) => s,
                Err(p) => p.into_inner(),
            };
            // create_collection calls storage.batch_write which is async.
            // Use tokio::task::block_in_place + Handle::block_on to call it
            // while holding the sync MutexGuard.
            tokio::task::block_in_place(|| {
                let handle = tokio::runtime::Handle::current();
                handle.block_on(strict.create_collection(&name, schema))
            })?;
        }

        // Register the new collection in the query engine.
        self.register_strict_collection(&name);

        Ok(QueryResult {
            columns: vec!["result".into()],
            rows: vec![vec![Value::String(format!(
                "strict collection '{name}' created"
            ))]],
            rows_affected: 0,
        })
    }

    /// Handle: DROP COLLECTION <name> (for strict collections).
    pub(in crate::query) async fn handle_drop_strict(
        &self,
        name: &str,
    ) -> Result<QueryResult, LiteError> {
        {
            let mut strict = match self.strict.lock() {
                Ok(s) => s,
                Err(p) => p.into_inner(),
            };
            tokio::task::block_in_place(|| {
                let handle = tokio::runtime::Handle::current();
                handle.block_on(strict.drop_collection(name))
            })?;
        }

        Ok(QueryResult {
            columns: vec!["result".into()],
            rows: vec![vec![Value::String(format!(
                "strict collection '{name}' dropped"
            ))]],
            rows_affected: 0,
        })
    }
}
