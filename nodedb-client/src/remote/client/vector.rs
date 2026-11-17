// SPDX-License-Identifier: Apache-2.0

//! Vector operation implementations for `NodeDbRemote`.

use std::collections::HashSet;

use nodedb_types::document::Document;
use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::filter::MetadataFilter;
use nodedb_types::result::SearchResult;

use crate::remote_parse::format_vector_array;
use crate::row_decode::search_hit::{DISTANCE_COLUMN, ID_COLUMN};
use crate::row_decode::{HitSource, decode_search_hits};
use crate::sql_escape::quote_identifier;

use super::super::sql::{build_vector_search_sql, render_metadata_filter_public};
use super::core::{NodeDbRemote, pg_error_detail};
use super::identity::identity_column_of;
use super::vector_bind::{VectorColumn, vector_upsert};

impl NodeDbRemote {
    pub(super) async fn vector_search_impl(
        &self,
        collection: &str,
        query: &[f32],
        k: usize,
        filter: Option<&MetadataFilter>,
        allowed_ids: Option<&HashSet<String>>,
    ) -> NodeDbResult<Vec<SearchResult>> {
        // An empty allowed set admits no candidate.
        if allowed_ids.is_some_and(HashSet::is_empty) {
            return Ok(Vec::new());
        }
        // The allowed ids restrict the key column, so the server lowers them
        // to the candidate set the index search ranks within.
        let key = match allowed_ids {
            Some(_) => Some(self.identity_column(collection).await?),
            None => None,
        };
        let allowed = key.as_deref().zip(allowed_ids);
        let sql = build_vector_search_sql(collection, query, k, filter, allowed)?;

        let (columns, rows) = self.query_raw(&sql, &[]).await?;
        decode_search_hits(
            &HitSource {
                op: "vector_search",
                collection,
                score_column: DISTANCE_COLUMN,
            },
            &columns,
            &rows,
        )
    }

    pub(super) async fn vector_insert_field_impl(
        &self,
        collection: &str,
        field_name: &str,
        id: &str,
        embedding: &[f32],
        metadata: Option<Document>,
    ) -> NodeDbResult<()> {
        // The vector lands on the column the caller names, not on whichever
        // vector column the planner picks when the column name is omitted.
        self.vector_row_upsert(collection, field_name, id, embedding, metadata)
            .await
    }

    pub(super) async fn vector_search_field_impl(
        &self,
        collection: &str,
        field_name: &str,
        query: &[f32],
        k: usize,
        filter: Option<&MetadataFilter>,
    ) -> NodeDbResult<Vec<SearchResult>> {
        // Field-aware path: use the 2-arg form of `vector_distance` so
        // the planner scopes the HNSW lookup to the named column. The
        // single-arg form `vector_distance(ARRAY[...])` only works on
        // collections that have exactly one vector column.
        let coll = quote_identifier(collection);
        let field = quote_identifier(field_name);
        let vec_lit = format_vector_array(query);
        let where_clause = match filter {
            Some(f) => {
                let rendered = render_metadata_filter_public(f)?;
                format!(" WHERE {rendered}")
            }
            None => String::new(),
        };
        let sql = format!(
            "SELECT {ID_COLUMN}, vector_distance({field}, {vec_lit}) AS {DISTANCE_COLUMN} \
             FROM {coll}{where_clause} \
             ORDER BY vector_distance({field}, {vec_lit}) \
             LIMIT {k}"
        );

        let (columns, rows) = self.query_raw(&sql, &[]).await?;
        decode_search_hits(
            &HitSource {
                op: "vector_search_field",
                collection,
                score_column: DISTANCE_COLUMN,
            },
            &columns,
            &rows,
        )
    }

    pub(super) async fn vector_insert_impl(
        &self,
        collection: &str,
        id: &str,
        embedding: &[f32],
        metadata: Option<Document>,
    ) -> NodeDbResult<()> {
        self.vector_row_upsert(collection, DEFAULT_VECTOR_COLUMN, id, embedding, metadata)
            .await
    }

    /// Upsert the row `id` with `embedding` under `column` and each metadata
    /// field as its own typed column. See [`vector_upsert`].
    async fn vector_row_upsert(
        &self,
        collection: &str,
        column: &str,
        id: &str,
        embedding: &[f32],
        metadata: Option<Document>,
    ) -> NodeDbResult<()> {
        let (describe_columns, describe_rows) = self.describe(collection).await?;
        let key = identity_column_of(collection, &describe_columns, &describe_rows)?;
        let mut row = Document::new(id);
        if let Some(metadata) = metadata {
            row.fields = metadata.fields;
        }
        let vector = VectorColumn {
            name: column,
            embedding,
        };
        let upsert = vector_upsert(collection, &key, &vector, &row)?;
        let failed = |step: &str, e: &tokio_postgres::Error| {
            NodeDbError::storage(format!(
                "vector_insert '{collection}'/'{id}' {step} failed: {}",
                pg_error_detail(e)
            ))
        };
        let client = self.client.lock().await;
        let statement = client
            .prepare_typed(&upsert.sql, &upsert.types())
            .await
            .map_err(|e| failed("prepare", &e))?;
        client
            .execute(&statement, &upsert.values())
            .await
            .map_err(|e| failed("upsert", &e))?;
        Ok(())
    }

    pub(super) async fn vector_delete_impl(&self, collection: &str, id: &str) -> NodeDbResult<()> {
        let key = self.identity_column(collection).await?;
        let sql = format!(
            "DELETE FROM {} WHERE {} = $1",
            quote_identifier(collection),
            quote_identifier(&key)
        );
        self.execute_raw(&sql, &[&id]).await?;
        Ok(())
    }
}

/// The vector column a field-less `vector_insert` writes.
const DEFAULT_VECTOR_COLUMN: &str = "embedding";
