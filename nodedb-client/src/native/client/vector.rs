// SPDX-License-Identifier: Apache-2.0

//! Vector operation implementations for `NativeClient`.

use std::collections::HashSet;

use nodedb_types::document::Document;
use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::filter::MetadataFilter;
use nodedb_types::protocol::{OpCode, TextFields};
use nodedb_types::result::SearchResult;

use super::core::NativeClient;
use crate::native::connection::check_error;
use crate::row_decode::search_hit::DISTANCE_COLUMN;
use crate::row_decode::{HitSource, decode_search_hits};
use crate::sql_escape::{quote_identifier, quote_string_literal};

impl NativeClient {
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
        let request = build_vector_search_request(collection, query, k, filter, allowed_ids)?;
        let mut conn = self.pool.acquire().await?;
        let resp = conn.send(OpCode::VectorSearch, request).await?;
        // An error frame carries no rows; parsed unchecked it would read as
        // "the search matched nothing".
        check_error(&resp)?;
        decode_search_hits(
            &HitSource {
                op: "vector_search",
                collection,
                score_column: DISTANCE_COLUMN,
            },
            resp.columns.as_deref().unwrap_or_default(),
            resp.rows.as_deref().unwrap_or_default(),
        )
    }

    pub(super) async fn vector_insert_impl(
        &self,
        collection: &str,
        id: &str,
        embedding: &[f32],
        metadata: Option<Document>,
    ) -> NodeDbResult<()> {
        // Serialize metadata up front. A serialization failure must
        // propagate — the prior `unwrap_or_else(|_| "{}")` silently
        // replaced caller-supplied metadata with an empty object, which
        // is exactly the silent-drop pattern this client guards against
        // on every other seam (filter bytes, bind params).
        let meta_json = match metadata {
            Some(d) => {
                let obj: std::collections::HashMap<String, nodedb_types::value::Value> = d.fields;
                sonic_rs::to_string(&obj).map_err(|e| {
                    NodeDbError::serialization("json", format!("vector_insert metadata: {e}"))
                })?
            }
            None => "{}".to_string(),
        };
        let sql = format!(
            "INSERT INTO {} (id, embedding, metadata) VALUES ({}, {}, {})",
            quote_identifier(collection),
            quote_string_literal(id),
            format_f32_array(embedding),
            quote_string_literal(&meta_json),
        );
        let mut conn = self.pool.acquire().await?;
        conn.execute_sql(&sql).await?;
        Ok(())
    }

    pub(super) async fn vector_delete_impl(&self, collection: &str, id: &str) -> NodeDbResult<()> {
        let sql = format!(
            "DELETE FROM {} WHERE id = {}",
            quote_identifier(collection),
            quote_string_literal(id),
        );
        let mut conn = self.pool.acquire().await?;
        conn.execute_sql(&sql).await?;
        Ok(())
    }
}

/// Build the `TextFields` payload for an `OpCode::VectorSearch` request.
///
/// A non-`None` `MetadataFilter` travels as MessagePack in
/// `TextFields::filters`. The server plans it as the `WHERE` a pgwire
/// client renders from the same filter, so it narrows the candidates
/// before the top-k cut.
///
/// `allowed_ids` travel sorted in `TextFields::allowed_ids`. The server
/// lowers them to the candidate bitmap the index search honors.
pub(super) fn build_vector_search_request(
    collection: &str,
    query: &[f32],
    k: usize,
    filter: Option<&MetadataFilter>,
    allowed_ids: Option<&HashSet<String>>,
) -> NodeDbResult<TextFields> {
    // Serialization failure here must surface to the caller. Dropping
    // the filter and sending the request anyway would send the query
    // to the server without the caller's predicate — exactly the
    // silent-drop pattern this client guards against.
    let filters_bytes = match filter {
        Some(f) => Some(zerompk::to_msgpack_vec(f).map_err(|e| {
            NodeDbError::serialization("msgpack", format!("vector_search metadata filter: {e}"))
        })?),
        None => None,
    };
    let allowed_ids = allowed_ids.map(|ids| {
        let mut ids: Vec<String> = ids.iter().cloned().collect();
        ids.sort();
        ids
    });
    Ok(TextFields {
        collection: Some(collection.to_string()),
        query_vector: Some(query.to_vec()),
        top_k: Some(k as u32),
        filters: filters_bytes,
        allowed_ids,
        ..Default::default()
    })
}

/// Format a `&[f32]` as a SQL `ARRAY[...]` literal.
pub(super) fn format_f32_array(arr: &[f32]) -> String {
    let inner: Vec<String> = arr.iter().map(|v| format!("{v}")).collect();
    format!("ARRAY[{}]", inner.join(","))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::value::Value;

    // Trait-contract enforcement. A request envelope that omits the
    // caller-supplied filter bytes is the silent-drop pattern these
    // tests guard against — the server would answer without the
    // caller's predicate, returning data from the wrong scope.

    #[test]
    fn vector_search_request_without_filter_omits_filter_bytes() {
        let req = build_vector_search_request("docs", &[0.1, 0.2], 5, None, None)
            .expect("no-filter request must build");
        assert!(req.allowed_ids.is_none(), "no restriction sends no ids");
        assert_eq!(req.collection.as_deref(), Some("docs"));
        assert_eq!(req.query_vector.as_deref(), Some(&[0.1f32, 0.2][..]));
        assert_eq!(req.top_k, Some(5));
        assert!(
            req.filters.is_none(),
            "no-filter case must leave TextFields::filters empty"
        );
    }

    #[test]
    fn vector_search_request_serializes_metadata_filter() {
        let filter = MetadataFilter::eq("category", Value::String("ai".into()));
        let req = build_vector_search_request("docs", &[0.1], 3, Some(&filter), None)
            .expect("derived-Serialize MetadataFilter must encode");
        let bytes = req.filters.expect("non-None filter must produce bytes");
        let decoded: MetadataFilter =
            zerompk::from_msgpack(&bytes).expect("filter bytes are a MessagePack MetadataFilter");
        assert_eq!(decoded, filter);
    }

    #[test]
    fn vector_search_request_carries_allowed_ids_sorted() {
        let ids: HashSet<String> = ["b", "a"].iter().map(|s| s.to_string()).collect();
        let req = build_vector_search_request("docs", &[0.1], 3, None, Some(&ids))
            .expect("allowed ids must encode");
        assert_eq!(
            req.allowed_ids,
            Some(vec!["a".to_string(), "b".to_string()])
        );
    }

    #[test]
    fn format_f32_array_works() {
        let arr = [0.1f32, 0.2, 0.3];
        let s = format_f32_array(&arr);
        assert!(s.starts_with("ARRAY["));
        assert!(s.contains("0.1"));
        assert!(s.ends_with(']'));
    }
}
