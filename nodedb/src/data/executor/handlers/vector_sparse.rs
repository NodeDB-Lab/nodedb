//! Sparse vector index handlers: insert, search, delete.
//!
//! Operates on `SparseInvertedIndex` instances owned by the CoreLoop.

use tracing::{debug, warn};

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::vector::sparse::SparseInvertedIndex;

impl CoreLoop {
    /// Get or create a sparse inverted index for a collection/field.
    fn get_or_create_sparse_index(
        &mut self,
        tid: u32,
        collection: &str,
        field_name: &str,
    ) -> &mut SparseInvertedIndex {
        let key = Self::sparse_index_key(tid, collection, field_name);
        self.sparse_vector_indexes
            .entry(key)
            .or_insert_with(SparseInvertedIndex::new)
    }

    /// Build the key for sparse vector indexes.
    fn sparse_index_key(tid: u32, collection: &str, field_name: &str) -> String {
        if field_name.is_empty() {
            format!("{tid}:{collection}:_sparse")
        } else {
            format!("{tid}:{collection}:{field_name}")
        }
    }

    /// Insert a sparse vector for a document.
    pub(in crate::data::executor) fn execute_sparse_insert(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        field_name: &str,
        doc_id: &str,
        entries: &[(u32, f32)],
    ) -> Response {
        debug!(
            core = self.core_id,
            %collection,
            %field_name,
            %doc_id,
            nnz = entries.len(),
            "sparse insert"
        );

        let sv = match nodedb_types::SparseVector::from_entries(entries.to_vec()) {
            Ok(sv) => sv,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::RejectedConstraint {
                        constraint: e.to_string(),
                    },
                );
            }
        };

        let index = self.get_or_create_sparse_index(tid, collection, field_name);
        index.insert(doc_id, &sv);
        self.checkpoint_coordinator.mark_dirty("vector", 1);
        self.response_ok(task)
    }

    /// Search the sparse inverted index via dot-product scoring.
    pub(in crate::data::executor) fn execute_sparse_search(
        &self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        field_name: &str,
        query_entries: &[(u32, f32)],
        top_k: usize,
    ) -> Response {
        debug!(
            core = self.core_id,
            %collection,
            %field_name,
            query_nnz = query_entries.len(),
            top_k,
            "sparse search"
        );

        let key = Self::sparse_index_key(tid, collection, field_name);
        let Some(index) = self.sparse_vector_indexes.get(&key) else {
            // No index exists — return empty results (not an error).
            return match super::super::response_codec::encode(&Vec::<
                super::super::response_codec::VectorSearchHit,
            >::new())
            {
                Ok(payload) => self.response_with_payload(task, payload),
                Err(e) => self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                ),
            };
        };

        let query = match nodedb_types::SparseVector::from_entries(query_entries.to_vec()) {
            Ok(sv) => sv,
            Err(e) => {
                return self.response_error(
                    task,
                    ErrorCode::RejectedConstraint {
                        constraint: e.to_string(),
                    },
                );
            }
        };

        let results = crate::engine::vector::sparse::search::dot_product_topk(index, &query, top_k);

        // Convert to VectorSearchHit for unified response codec.
        let hits: Vec<super::super::response_codec::VectorSearchHit> = results
            .iter()
            .map(|r| super::super::response_codec::VectorSearchHit {
                id: r.internal_id,
                distance: r.score,
                doc_id: r.doc_id.clone(),
            })
            .collect();

        match super::super::response_codec::encode(&hits) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => {
                warn!(core = self.core_id, error = %e, "sparse search encode failed");
                self.response_error(
                    task,
                    ErrorCode::Internal {
                        detail: e.to_string(),
                    },
                )
            }
        }
    }

    /// Delete a document from the sparse inverted index.
    pub(in crate::data::executor) fn execute_sparse_delete(
        &mut self,
        task: &ExecutionTask,
        tid: u32,
        collection: &str,
        field_name: &str,
        doc_id: &str,
    ) -> Response {
        debug!(core = self.core_id, %collection, %field_name, %doc_id, "sparse delete");

        let key = Self::sparse_index_key(tid, collection, field_name);
        let Some(index) = self.sparse_vector_indexes.get_mut(&key) else {
            return self.response_error(task, ErrorCode::NotFound);
        };

        if index.delete(doc_id) {
            self.checkpoint_coordinator.mark_dirty("vector", 1);
            self.response_ok(task)
        } else {
            self.response_error(task, ErrorCode::NotFound)
        }
    }
}
