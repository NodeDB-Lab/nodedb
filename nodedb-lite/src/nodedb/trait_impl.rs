//! `NodeDb` trait implementation for `NodeDbLite`.
//!
//! This module provides the `#[async_trait] impl NodeDb for NodeDbLite<S>` block,
//! wiring the public database API to the underlying HNSW, CSR, and CRDT engines.

use std::collections::HashMap;

use async_trait::async_trait;
use loro::LoroValue;

use nodedb_client::NodeDb;
use nodedb_types::document::Document;
use nodedb_types::error::{NodeDbError, NodeDbResult};
use nodedb_types::filter::{EdgeFilter, MetadataFilter};
use nodedb_types::id::{EdgeId, NodeId};
use nodedb_types::result::{QueryResult, SearchResult, SubGraph, SubGraphEdge, SubGraphNode};
use nodedb_types::value::Value;

use super::LockExt;
use super::NodeDbLite;
use super::convert::{loro_value_to_document, value_to_loro};
use crate::engine::graph::index::Direction;
use crate::storage::engine::StorageEngine;

/// Internal fields to exclude from metadata by index type.
const INTERNAL_FIELDS_BASE: &[&str] = &["embedding_dim"];
const INTERNAL_FIELDS_NAMED: &[&str] = &["embedding_dim", "__field"];

impl<S: StorageEngine> NodeDbLite<S> {
    /// Shared vector search implementation used by both `vector_search` and `vector_search_field`.
    async fn vector_search_internal(
        &self,
        index_key: &str,
        collection: &str,
        query: &[f32],
        k: usize,
        filter: Option<&MetadataFilter>,
        exclude_fields: &[&str],
    ) -> NodeDbResult<Vec<SearchResult>> {
        // Lazy-load from storage.
        {
            let has_it = self.hnsw_indices.lock_or_recover().contains_key(index_key);
            if !has_it {
                let key = format!("hnsw:{index_key}");
                if let Some(checkpoint) = self
                    .storage
                    .get(nodedb_types::Namespace::Vector, key.as_bytes())
                    .await?
                    && let Some(index) =
                        crate::engine::vector::graph::HnswIndex::from_checkpoint(&checkpoint)
                {
                    tracing::info!(index_key, "lazy-loaded HNSW collection from storage");
                    self.hnsw_indices
                        .lock_or_recover()
                        .insert(index_key.to_string(), index);
                }
            }
        }

        let indices = self.hnsw_indices.lock_or_recover();
        let Some(index) = indices.get(index_key) else {
            return Ok(Vec::new());
        };

        let id_map = self.vector_id_map.lock_or_recover();
        let crdt = self.crdt.lock_or_recover();

        // Pre-filter path: when a metadata filter is present, evaluate it
        // against CRDT docs to build an allowed-set of vector IDs, then pass
        // it to HNSW search_filtered for in-graph filtering (better recall
        // than over-fetch + post-filter).
        // Over-fetch by 3x when filtering to maintain recall after post-filter.
        let fetch_k = if filter.is_some() { k * 3 } else { k };

        // For small collections (< 10K vectors), use pre-filter for better recall.
        // For large collections, over-fetch + post-filter is faster (avoids O(N) scan).
        let collection_size = id_map
            .keys()
            .filter(|key| key.starts_with(index_key))
            .count();

        let raw_results = if let Some(f) = filter
            && collection_size <= 10_000
        {
            let mut allowed = std::collections::HashSet::new();
            for (composite_key, (doc_id, _)) in id_map.iter() {
                if !composite_key.starts_with(index_key) {
                    continue;
                }
                if let Some(loro_val) = crdt.read(collection, doc_id) {
                    let doc = loro_value_to_document(doc_id, &loro_val);
                    let json_doc = serde_json::to_value(&doc.fields).unwrap_or_default();
                    if nodedb_query::metadata_filter::matches_metadata_filter(&json_doc, f)
                        && let Some(vid_str) = composite_key.strip_prefix(&format!("{index_key}:"))
                        && let Ok(vid) = vid_str.parse::<u32>()
                    {
                        allowed.insert(vid);
                    }
                }
            }
            if allowed.is_empty() {
                return Ok(Vec::new());
            }
            index.search_filtered(query, k, self.search_ef, &allowed)
        } else {
            index.search(query, fetch_k, self.search_ef)
        };

        let results: Vec<SearchResult> = raw_results
            .into_iter()
            .filter(|r| !index.is_deleted(r.id))
            .filter_map(|r| {
                let composite_key = format!("{index_key}:{}", r.id);
                let doc_id = id_map
                    .get(&composite_key)
                    .map(|(id, _)| id.clone())
                    .unwrap_or_else(|| r.id.to_string());

                let metadata = if let Some(loro_val) = crdt.read(collection, &doc_id) {
                    let doc = loro_value_to_document(&doc_id, &loro_val);
                    doc.fields
                        .into_iter()
                        .filter(|(k, _)| !exclude_fields.contains(&k.as_str()))
                        .collect::<HashMap<String, Value>>()
                } else {
                    HashMap::new()
                };

                // Post-filter for over-fetch path (large collections).
                if let Some(f) = filter {
                    let json_doc = serde_json::to_value(&metadata).unwrap_or_default();
                    if !nodedb_query::metadata_filter::matches_metadata_filter(&json_doc, f) {
                        return None;
                    }
                }

                Some(SearchResult {
                    id: doc_id,
                    node_id: None,
                    distance: r.distance,
                    metadata,
                })
            })
            .take(k)
            .collect();

        Ok(results)
    }
}

#[async_trait]
impl<S: StorageEngine> NodeDb for NodeDbLite<S> {
    async fn vector_search(
        &self,
        collection: &str,
        query: &[f32],
        k: usize,
        filter: Option<&MetadataFilter>,
    ) -> NodeDbResult<Vec<SearchResult>> {
        self.vector_search_internal(
            collection,
            collection,
            query,
            k,
            filter,
            INTERNAL_FIELDS_BASE,
        )
        .await
    }

    async fn vector_insert(
        &self,
        collection: &str,
        id: &str,
        embedding: &[f32],
        metadata: Option<Document>,
    ) -> NodeDbResult<()> {
        // ── Insert into HNSW ──
        let internal_id = {
            let mut indices = self.hnsw_indices.lock_or_recover();
            let index = Self::ensure_hnsw(&mut indices, collection, embedding.len());
            let id_before = index.len() as u32;
            index
                .insert(embedding.to_vec())
                .map_err(NodeDbError::bad_request)?;
            id_before
        };

        // ── Track ID mapping ──
        {
            let mut id_map = self.vector_id_map.lock_or_recover();
            id_map.insert(
                format!("{collection}:{internal_id}"),
                (id.to_string(), internal_id),
            );
        }

        // ── Record in CRDT ──
        {
            let mut crdt = self.crdt.lock_or_recover();
            let mut fields = vec![("embedding_dim", LoroValue::I64(embedding.len() as i64))];
            if let Some(meta) = &metadata {
                for (k, v) in &meta.fields {
                    fields.push((k.as_str(), value_to_loro(v)));
                }
            }
            crdt.upsert(collection, id, &fields)
                .map_err(NodeDbError::storage)?;
        }

        self.update_memory_stats();
        Ok(())
    }

    async fn vector_delete(&self, collection: &str, id: &str) -> NodeDbResult<()> {
        // Find internal ID from the map.
        let internal_id = {
            let id_map = self.vector_id_map.lock_or_recover();
            id_map
                .iter()
                .find(|(_, (doc_id, _))| doc_id == id)
                .map(|(_, (_, iid))| *iid)
        };

        if let Some(iid) = internal_id {
            let mut indices = self.hnsw_indices.lock_or_recover();
            if let Some(index) = indices.get_mut(collection) {
                index.delete(iid);
            }
        }

        // ── Record in CRDT ──
        {
            let mut crdt = self.crdt.lock_or_recover();
            crdt.delete(collection, id).map_err(NodeDbError::storage)?;
        }

        Ok(())
    }

    async fn graph_traverse(
        &self,
        start: &NodeId,
        depth: u8,
        edge_filter: Option<&EdgeFilter>,
    ) -> NodeDbResult<SubGraph> {
        let csr = self.csr.lock_or_recover();

        // Multi-label filter: use ALL labels from the filter, not just the first.
        let label_strs: Vec<&str> = edge_filter
            .map(|f| f.labels.iter().map(|s| s.as_str()).collect())
            .unwrap_or_default();

        let result = csr.traverse_bfs_with_depth_multi(
            &[start.as_str()],
            &label_strs,
            Direction::Out,
            depth as usize,
        );

        let crdt = self.crdt.lock_or_recover();
        let mut nodes = Vec::with_capacity(result.len());
        let mut edges = Vec::new();

        for (node_name, d) in &result {
            // Populate node properties from CRDT if available.
            let properties = if let Some(loro_val) = crdt.read("__nodes", node_name) {
                let doc = loro_value_to_document(node_name, &loro_val);
                doc.fields
            } else {
                HashMap::new()
            };

            nodes.push(SubGraphNode {
                id: NodeId::new(node_name.clone()),
                depth: *d,
                properties,
            });

            let neighbors = csr.neighbors_multi(node_name, &label_strs, Direction::Out);
            for (label, dst) in &neighbors {
                if result.iter().any(|(n, _)| n == dst) {
                    // Read edge properties from CRDT.
                    let edge_id = EdgeId::from_components(node_name, dst, label);
                    let edge_props = if let Some(loro_val) = crdt.read("__edges", edge_id.as_str())
                    {
                        let doc = loro_value_to_document(edge_id.as_str(), &loro_val);
                        doc.fields
                            .into_iter()
                            .filter(|(k, _)| k != "src" && k != "dst" && k != "label")
                            .collect()
                    } else {
                        HashMap::new()
                    };

                    edges.push(SubGraphEdge {
                        id: edge_id,
                        from: NodeId::new(node_name.clone()),
                        to: NodeId::new(dst.clone()),
                        label: label.clone(),
                        properties: edge_props,
                    });
                }
            }
        }

        Ok(SubGraph { nodes, edges })
    }

    async fn graph_insert_edge(
        &self,
        from: &NodeId,
        to: &NodeId,
        edge_type: &str,
        properties: Option<Document>,
    ) -> NodeDbResult<EdgeId> {
        {
            let mut csr = self.csr.lock_or_recover();
            csr.add_edge(from.as_str(), edge_type, to.as_str());
        }

        // ── Record in CRDT (including properties) ──
        let edge_id = EdgeId::from_components(from.as_str(), to.as_str(), edge_type);
        {
            let mut crdt = self.crdt.lock_or_recover();
            let mut fields: Vec<(&str, LoroValue)> = vec![
                ("src", LoroValue::String(from.as_str().into())),
                ("dst", LoroValue::String(to.as_str().into())),
                ("label", LoroValue::String(edge_type.into())),
            ];

            // Store edge properties alongside the structural fields.
            if let Some(ref props) = properties {
                for (k, v) in &props.fields {
                    fields.push((k.as_str(), value_to_loro(v)));
                }
            }

            crdt.upsert("__edges", edge_id.as_str(), &fields)
                .map_err(NodeDbError::storage)?;
        }

        self.update_memory_stats();
        Ok(edge_id)
    }

    async fn graph_delete_edge(&self, edge_id: &EdgeId) -> NodeDbResult<()> {
        // Parse edge ID: "src--label-->dst"
        let id_str = edge_id.as_str();
        if let Some((src, rest)) = id_str.split_once("--")
            && let Some((label, dst)) = rest.split_once("-->")
        {
            let mut csr = self.csr.lock_or_recover();
            csr.remove_edge(src, label, dst);
        }

        {
            let mut crdt = self.crdt.lock_or_recover();
            crdt.delete("__edges", id_str)
                .map_err(NodeDbError::storage)?;
        }

        Ok(())
    }

    async fn document_get(&self, collection: &str, id: &str) -> NodeDbResult<Option<Document>> {
        let crdt = self.crdt.lock_or_recover();

        let Some(value) = crdt.read(collection, id) else {
            return Ok(None);
        };

        Ok(Some(loro_value_to_document(id, &value)))
    }

    async fn document_put(&self, collection: &str, doc: Document) -> NodeDbResult<()> {
        let mut crdt = self.crdt.lock_or_recover();

        let doc_id = if doc.id.is_empty() {
            nodedb_types::id_gen::uuid_v7()
        } else {
            doc.id.clone()
        };

        let fields: Vec<(&str, LoroValue)> = doc
            .fields
            .iter()
            .map(|(k, v)| (k.as_str(), value_to_loro(v)))
            .collect();

        crdt.upsert(collection, &doc_id, &fields)
            .map_err(NodeDbError::storage)?;
        drop(crdt); // Release lock before text indexing.

        // Update text index incrementally.
        self.index_document_text(collection, &doc_id, &doc.fields);

        Ok(())
    }

    async fn document_delete(&self, collection: &str, id: &str) -> NodeDbResult<()> {
        let mut crdt = self.crdt.lock_or_recover();

        crdt.delete(collection, id).map_err(NodeDbError::storage)?;
        drop(crdt);

        // Remove from text index.
        self.remove_document_text(collection, id);

        Ok(())
    }

    async fn execute_sql(&self, query: &str, _params: &[Value]) -> NodeDbResult<QueryResult> {
        self.query_engine
            .execute_sql(query)
            .await
            .map_err(NodeDbError::storage)
    }

    // ─── Named Vector Fields ─────────────────────────────────────────

    async fn vector_insert_field(
        &self,
        collection: &str,
        field_name: &str,
        id: &str,
        embedding: &[f32],
        metadata: Option<Document>,
    ) -> NodeDbResult<()> {
        // Key: "collection:field" for multi-field HNSW indexes.
        let index_key = if field_name.is_empty() {
            collection.to_string()
        } else {
            format!("{collection}:{field_name}")
        };

        let internal_id = {
            let mut indices = self.hnsw_indices.lock_or_recover();
            let index = Self::ensure_hnsw(&mut indices, &index_key, embedding.len());
            let id_before = index.len() as u32;
            index
                .insert(embedding.to_vec())
                .map_err(NodeDbError::bad_request)?;
            id_before
        };

        {
            let mut id_map = self.vector_id_map.lock_or_recover();
            id_map.insert(
                format!("{index_key}:{internal_id}"),
                (id.to_string(), internal_id),
            );
        }

        {
            let mut crdt = self.crdt.lock_or_recover();
            let mut fields = vec![
                (
                    "embedding_dim",
                    loro::LoroValue::I64(embedding.len() as i64),
                ),
                ("__field", loro::LoroValue::String(field_name.into())),
            ];
            if let Some(meta) = &metadata {
                for (k, v) in &meta.fields {
                    fields.push((k.as_str(), value_to_loro(v)));
                }
            }
            crdt.upsert(collection, id, &fields)
                .map_err(NodeDbError::storage)?;
        }

        self.update_memory_stats();
        Ok(())
    }

    async fn vector_search_field(
        &self,
        collection: &str,
        field_name: &str,
        query: &[f32],
        k: usize,
        filter: Option<&MetadataFilter>,
    ) -> NodeDbResult<Vec<SearchResult>> {
        let index_key = if field_name.is_empty() {
            collection.to_string()
        } else {
            format!("{collection}:{field_name}")
        };
        self.vector_search_internal(
            &index_key,
            collection,
            query,
            k,
            filter,
            INTERNAL_FIELDS_NAMED,
        )
        .await
    }

    // ─── Graph Shortest Path ─────────────────────────────────────────

    async fn graph_shortest_path(
        &self,
        from: &NodeId,
        to: &NodeId,
        max_depth: u8,
        edge_filter: Option<&EdgeFilter>,
    ) -> NodeDbResult<Option<Vec<NodeId>>> {
        let csr = self.csr.lock_or_recover();
        let label_filter = edge_filter
            .and_then(|f| f.labels.first())
            .map(|s| s.as_str());

        let path = csr.shortest_path(from.as_str(), to.as_str(), label_filter, max_depth as usize);

        Ok(path.map(|p| p.into_iter().map(NodeId::new).collect()))
    }

    // ─── Text Search ─────────────────────────────────────────────────

    async fn text_search(
        &self,
        collection: &str,
        query: &str,
        top_k: usize,
    ) -> NodeDbResult<Vec<SearchResult>> {
        use nodedb_query::text_search::{Bm25Params, QueryMode};

        // Use the persistent in-memory inverted index (updated on every document_put/delete).
        let indices = self.text_indices.lock_or_recover();

        let results = if let Some(idx) = indices.get(collection) {
            idx.search(query, top_k, QueryMode::Or, Bm25Params::default())
        } else {
            // No documents indexed yet for this collection.
            Vec::new()
        };
        drop(indices);

        // Populate metadata from CRDT for each result.
        let crdt = self.crdt.lock_or_recover();
        Ok(results
            .into_iter()
            .map(|r| {
                let metadata = if let Some(loro_val) = crdt.read(collection, &r.doc_id) {
                    let doc = loro_value_to_document(&r.doc_id, &loro_val);
                    doc.fields
                } else {
                    HashMap::new()
                };
                SearchResult {
                    id: r.doc_id,
                    node_id: None,
                    distance: 1.0 - (r.score as f32 / 20.0).min(1.0),
                    metadata,
                }
            })
            .collect())
    }
}
