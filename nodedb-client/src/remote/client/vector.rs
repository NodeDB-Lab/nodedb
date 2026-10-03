// SPDX-License-Identifier: Apache-2.0

//! Vector operation implementations for `NodeDbRemote`.

use std::collections::{HashMap, HashSet};

use nodedb_types::document::Document;
use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::filter::MetadataFilter;
use nodedb_types::result::SearchResult;

use crate::remote_parse::format_vector_array;
use crate::sql_escape::quote_identifier;

use super::super::parse::parse_vector_search_json;
use super::super::sql::{build_vector_search_sql, render_metadata_filter_public};
use super::core::NodeDbRemote;

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
        let sql = build_vector_search_sql(collection, query, k, filter, allowed_ids)?;

        let (columns, rows) = self.query_raw(&sql, &[]).await?;

        // The DSL path returns JSON in a single "result" column.
        if columns.len() == 1 && columns[0] == "result" {
            if let Some(row) = rows.first()
                && let Some(nodedb_types::value::Value::String(json_text)) = row.first()
            {
                return parse_vector_search_json(json_text);
            }
            return Ok(Vec::new());
        }

        // Structured result set: id, distance columns.
        let mut results = Vec::with_capacity(rows.len());
        let id_idx = columns.iter().position(|c| c == "id").unwrap_or(0);
        let dist_idx = columns.iter().position(|c| c == "distance").unwrap_or(1);

        for row in &rows {
            let id = row
                .get(id_idx)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let distance = row.get(dist_idx).and_then(|v| v.as_f64()).unwrap_or(0.0) as f32;

            results.push(SearchResult {
                id,
                node_id: None,
                distance,
                metadata: HashMap::new(),
            });
        }

        Ok(results)
    }

    pub(super) async fn vector_insert_field_impl(
        &self,
        collection: &str,
        field_name: &str,
        id: &str,
        embedding: &[f32],
        metadata: Option<Document>,
    ) -> NodeDbResult<()> {
        // Field-aware path: emit `INSERT INTO <coll> (id, <field>[,
        // metadata]) VALUES ($1, ARRAY[...]<, $2>)` so the vector lands
        // on the column named by the trait — not on whichever vector
        // column the planner picks when the column name is omitted.
        let coll = quote_identifier(collection);
        let field = quote_identifier(field_name);
        let vec_lit = format_vector_array(embedding);

        let sql = match metadata {
            Some(_) => {
                format!("INSERT INTO {coll} (id, {field}, metadata) VALUES ($1, {vec_lit}, $2)")
            }
            None => format!("INSERT INTO {coll} (id, {field}) VALUES ($1, {vec_lit})"),
        };

        if let Some(d) = metadata {
            let meta_json = sonic_rs::to_string(&d)
                .map_err(|e| NodeDbError::storage(format!("metadata serialization: {e}")))?;
            self.execute_raw(&sql, &[&id, &meta_json]).await?;
        } else {
            self.execute_raw(&sql, &[&id]).await?;
        }
        Ok(())
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
            "SELECT id, vector_distance({field}, {vec_lit}) AS distance \
             FROM {coll}{where_clause} \
             ORDER BY vector_distance({field}, {vec_lit}) \
             LIMIT {k}"
        );

        let (columns, rows) = self.query_raw(&sql, &[]).await?;
        let id_idx = columns.iter().position(|c| c == "id").unwrap_or(0);
        let dist_idx = columns.iter().position(|c| c == "distance").unwrap_or(1);

        let mut results = Vec::with_capacity(rows.len());
        for row in &rows {
            let id = row
                .get(id_idx)
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let distance = row.get(dist_idx).and_then(|v| v.as_f64()).unwrap_or(0.0) as f32;
            results.push(SearchResult {
                id,
                node_id: None,
                distance,
                metadata: HashMap::new(),
            });
        }
        Ok(results)
    }

    pub(super) async fn vector_insert_impl(
        &self,
        collection: &str,
        id: &str,
        embedding: &[f32],
        metadata: Option<Document>,
    ) -> NodeDbResult<()> {
        let collection = quote_identifier(collection);
        let meta_json = match metadata {
            Some(d) => sonic_rs::to_string(&d)
                .map_err(|e| NodeDbError::storage(format!("metadata serialization: {e}")))?,
            None => "{}".into(),
        };

        let sql = format!(
            "INSERT INTO {collection} (id, embedding, metadata) VALUES ($1, {}, $2::jsonb)",
            format_vector_array(embedding),
        );
        self.execute_raw(&sql, &[&id, &meta_json]).await?;
        Ok(())
    }

    pub(super) async fn vector_delete_impl(&self, collection: &str, id: &str) -> NodeDbResult<()> {
        let collection = quote_identifier(collection);
        let sql = format!("DELETE FROM {collection} WHERE id = $1");
        self.execute_raw(&sql, &[&id]).await?;
        Ok(())
    }
}
