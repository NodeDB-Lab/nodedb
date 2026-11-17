// SPDX-License-Identifier: Apache-2.0

//! SQL execution and collection lifecycle implementations for `NodeDbRemote`.

use nodedb_types::dropped_collection::DroppedCollection;
use nodedb_types::error::NodeDbResult;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::row_decode::parse_dropped_collection_rows;
use crate::sql_escape::quote_identifier;

use super::core::NodeDbRemote;
use super::sql_params::{param_types, typed_params};

impl NodeDbRemote {
    pub(super) async fn execute_sql_impl(
        &self,
        query: &str,
        params: &[Value],
    ) -> NodeDbResult<QueryResult> {
        // Empty params: a statement whose result columns the server
        // describes at Parse runs extended, so its cells are typed by
        // column. DDL and every other statement with no result shape at
        // Parse runs through `simple_query`, which returns CommandComplete
        // for DDL or text rows in one round-trip. See
        // `query_typed_or_simple`.
        let (columns, rows) = if params.is_empty() {
            self.query_typed_or_simple(query).await?
        } else {
            // Each value binds under the OID of its exact type, so the
            // server decodes it typed.
            let typed = typed_params(params)?;
            let refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = typed
                .iter()
                .map(|(value, _)| {
                    let upcast: &(dyn tokio_postgres::types::ToSql + Sync) = value.as_ref();
                    upcast
                })
                .collect();
            self.query_typed_params(query, &param_types(&typed), &refs)
                .await?
        };

        Ok(QueryResult {
            columns,
            rows,
            rows_affected: 0,
            command: None,
        })
    }

    pub(super) async fn undrop_collection_impl(&self, name: &str) -> NodeDbResult<()> {
        let sql = format!("UNDROP COLLECTION {}", quote_identifier(name));
        self.execute_raw(&sql, &[]).await?;
        Ok(())
    }

    pub(super) async fn drop_collection_purge_impl(&self, name: &str) -> NodeDbResult<()> {
        let sql = format!("DROP COLLECTION {} PURGE", quote_identifier(name));
        self.execute_raw(&sql, &[]).await?;
        Ok(())
    }

    pub(super) async fn list_dropped_collections_impl(
        &self,
    ) -> NodeDbResult<Vec<DroppedCollection>> {
        let sql = "SELECT tenant_id, name, owner, engine_type, \
                   deactivated_at_ns, retention_expires_at_ns \
                   FROM _system.dropped_collections";
        let (_columns, rows) = self.query_raw(sql, &[]).await?;
        parse_dropped_collection_rows(&rows)
    }
}
