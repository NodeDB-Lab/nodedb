// SPDX-License-Identifier: Apache-2.0

//! SQL execution and collection lifecycle implementations for `NodeDbRemote`.

use nodedb_types::dropped_collection::DroppedCollection;
use nodedb_types::error::NodeDbResult;
use nodedb_types::result::QueryResult;
use nodedb_types::value::Value;

use crate::row_decode::parse_dropped_collection_rows;
use crate::sql_escape::quote_identifier;

use super::super::sql::translate_params;
use super::core::NodeDbRemote;

impl NodeDbRemote {
    pub(super) async fn execute_sql_impl(
        &self,
        query: &str,
        params: &[Value],
    ) -> NodeDbResult<QueryResult> {
        // Empty params: route through `simple_query` so DDL works
        // alongside SELECT/DML in the same trait method. The
        // extended-query path (`Client::query`) requires a row
        // description for DDL that the server does not provide,
        // surfacing as a generic `db error`. The simple-query path
        // sends a single Query message and returns CommandComplete
        // for DDL or rows for SELECT in one round-trip.
        let (columns, rows) = if params.is_empty() {
            self.simple_query_raw(query).await?
        } else {
            let translated = translate_params(params)?;
            // Upcast `&(dyn ToSql + Send + Sync)` → `&(dyn ToSql + Sync)`
            // so the slice matches what `Client::query` expects.
            let refs: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = translated
                .iter()
                .map(|b| {
                    let upcast: &(dyn tokio_postgres::types::ToSql + Sync) = b.as_ref();
                    upcast
                })
                .collect();
            self.query_raw(query, &refs).await?
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
