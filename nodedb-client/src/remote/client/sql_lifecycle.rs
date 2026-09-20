// SPDX-License-Identifier: Apache-2.0

//! SQL execution and collection lifecycle implementations for `NodeDbRemote`.

use std::collections::HashMap;

use nodedb_types::dropped_collection::DroppedCollection;
use nodedb_types::error::NodeDbResult;
use nodedb_types::result::{QueryResult, SearchResult};
use nodedb_types::text_search::TextSearchParams;
use nodedb_types::value::Value;

use crate::row_decode::parse_dropped_collection_rows;
use crate::sql_escape::{quote_identifier, quote_string_literal};

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

    pub(super) async fn text_search_impl(
        &self,
        collection: &str,
        field: &str,
        query: &str,
        top_k: usize,
        params: TextSearchParams,
    ) -> NodeDbResult<Vec<SearchResult>> {
        // Server-side FTS query SQL: `text_match(<field>, '<query>')` in
        // a WHERE clause selects matching ids; `bm25_score(<field>,
        // '<query>')` in the SELECT list exposes the score so callers
        // can order/rank. The planner pattern-matches this shape and
        // dispatches `SqlPlan::TextSearch`.
        //
        // `params` (mode, fuzzy, prefix, etc.) is intentionally ignored
        // for now — every supported option is also expressible in the
        // SQL form, but threading them through the DSL string is its
        // own widening. The defaults (Plain query with fuzzy=true) cover
        // the common case the trait's spec calls out.
        let _ = params;
        let coll = quote_identifier(collection);
        let field_quoted = quote_identifier(field);
        let q_lit = quote_string_literal(query);
        let sql = format!(
            "SELECT id, bm25_score({field_quoted}, {q_lit}) AS score \
             FROM {coll} \
             WHERE text_match({field_quoted}, {q_lit}) \
             LIMIT {top_k}"
        );

        let (columns, rows) = self.simple_query_raw(&sql).await?;
        let id_idx = columns.iter().position(|c| c == "id").unwrap_or(0);
        let score_idx = columns.iter().position(|c| c == "score").unwrap_or(1);

        let mut results = Vec::with_capacity(rows.len());
        for row in &rows {
            let id = row
                .get(id_idx)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            // simple_query returns text — score arrives as a stringified
            // float. Parse defensively so a missing/malformed score does
            // not torpedo the whole result set; callers prefer ordered
            // ids with score 0.0 over an Err.
            let score = row
                .get(score_idx)
                .and_then(|v| v.as_str())
                .and_then(|s| s.parse::<f32>().ok())
                .or_else(|| {
                    row.get(score_idx)
                        .and_then(|v| v.as_f64())
                        .map(|f| f as f32)
                })
                .unwrap_or(0.0);

            results.push(SearchResult {
                id,
                node_id: None,
                distance: score,
                metadata: HashMap::new(),
            });
        }
        Ok(results)
    }
}
