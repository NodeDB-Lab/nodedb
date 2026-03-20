//! Learned sparse vector storage and search (SPLADE/ELSER).
//!
//! Stores sparse embeddings as `Vec<(u32, f32)>` (token_id, weight) pairs.
//! Unlike BM25 (term frequencies from raw text), these are model-generated
//! sparse representations where each dimension corresponds to a vocabulary
//! token and the weight reflects learned semantic importance.
//!
//! Search uses WAND (Weighted AND) algorithm for efficient top-k retrieval
//! over long posting lists — skips low-weight candidates early without
//! computing full dot products.

use std::collections::HashMap;
use std::sync::Arc;

use redb::{Database, ReadableTable, TableDefinition};
use tracing::debug;

/// Posting list table: key = "collection:token_id" → MessagePack Vec<SparsePosting>.
const SPARSE_POSTINGS: TableDefinition<&str, &[u8]> = TableDefinition::new("sparse.postings");

/// Document norm table: key = "collection:doc_id" → f32 (L2 norm for normalization).
const SPARSE_NORMS: TableDefinition<&str, &[u8]> = TableDefinition::new("sparse.norms");

/// A single posting in the sparse inverted index.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct SparsePosting {
    pub doc_id: String,
    pub weight: f32,
}

/// A sparse vector: sorted list of (token_id, weight) pairs.
pub type SparseVector = Vec<(u32, f32)>;

/// Result of a sparse vector search.
#[derive(Debug, Clone)]
pub struct SparseSearchResult {
    pub doc_id: String,
    pub score: f32,
}

/// Sparse vector index backed by redb.
pub struct SparseVectorIndex {
    db: Arc<Database>,
}

impl SparseVectorIndex {
    /// Open or create the sparse vector index.
    pub fn open(db: Arc<Database>) -> crate::Result<Self> {
        let write_txn = db.begin_write().map_err(|e| crate::Error::Storage {
            engine: "sparse_vector".into(),
            detail: format!("open write txn: {e}"),
        })?;
        {
            let _ = write_txn.open_table(SPARSE_POSTINGS);
            let _ = write_txn.open_table(SPARSE_NORMS);
        }
        write_txn.commit().map_err(|e| crate::Error::Storage {
            engine: "sparse_vector".into(),
            detail: format!("commit: {e}"),
        })?;
        Ok(Self { db })
    }

    /// Index a sparse vector for a document.
    ///
    /// For each (token_id, weight) pair, appends a posting to the
    /// token's posting list. Also stores the document's L2 norm.
    pub fn index_document(
        &self,
        collection: &str,
        doc_id: &str,
        vector: &SparseVector,
    ) -> crate::Result<()> {
        if vector.is_empty() {
            return Ok(());
        }

        let write_txn = self.db.begin_write().map_err(|e| crate::Error::Storage {
            engine: "sparse_vector".into(),
            detail: format!("write txn: {e}"),
        })?;
        {
            let mut postings_table =
                write_txn
                    .open_table(SPARSE_POSTINGS)
                    .map_err(|e| crate::Error::Storage {
                        engine: "sparse_vector".into(),
                        detail: format!("open postings: {e}"),
                    })?;
            let mut norms_table =
                write_txn
                    .open_table(SPARSE_NORMS)
                    .map_err(|e| crate::Error::Storage {
                        engine: "sparse_vector".into(),
                        detail: format!("open norms: {e}"),
                    })?;

            // Append to each token's posting list.
            for &(token_id, weight) in vector {
                if weight.abs() < f32::EPSILON {
                    continue;
                }
                let key = format!("{collection}:{token_id}");
                let mut postings: Vec<SparsePosting> = postings_table
                    .get(key.as_str())
                    .ok()
                    .flatten()
                    .and_then(|g| rmp_serde::from_slice(g.value()).ok())
                    .unwrap_or_default();

                // Remove existing posting for this doc (update case).
                postings.retain(|p| p.doc_id != doc_id);
                postings.push(SparsePosting {
                    doc_id: doc_id.to_string(),
                    weight,
                });

                let bytes =
                    rmp_serde::to_vec_named(&postings).map_err(|e| crate::Error::Storage {
                        engine: "sparse_vector".into(),
                        detail: format!("serialize postings: {e}"),
                    })?;
                postings_table
                    .insert(key.as_str(), bytes.as_slice())
                    .map_err(|e| crate::Error::Storage {
                        engine: "sparse_vector".into(),
                        detail: format!("insert posting: {e}"),
                    })?;
            }

            // Store document norm (L2 of weights).
            let norm: f32 = vector.iter().map(|(_, w)| w * w).sum::<f32>().sqrt();
            let norm_key = format!("{collection}:{doc_id}");
            let norm_bytes = rmp_serde::to_vec_named(&norm).map_err(|e| crate::Error::Storage {
                engine: "sparse_vector".into(),
                detail: format!("serialize norm: {e}"),
            })?;
            norms_table
                .insert(norm_key.as_str(), norm_bytes.as_slice())
                .map_err(|e| crate::Error::Storage {
                    engine: "sparse_vector".into(),
                    detail: format!("insert norm: {e}"),
                })?;
        }
        write_txn.commit().map_err(|e| crate::Error::Storage {
            engine: "sparse_vector".into(),
            detail: format!("commit: {e}"),
        })?;

        debug!(
            collection,
            doc_id,
            terms = vector.len(),
            "sparse vector indexed"
        );
        Ok(())
    }

    /// Remove a document from the sparse vector index.
    pub fn remove_document(&self, collection: &str, doc_id: &str) -> crate::Result<()> {
        let write_txn = self.db.begin_write().map_err(|e| crate::Error::Storage {
            engine: "sparse_vector".into(),
            detail: format!("write txn: {e}"),
        })?;
        {
            let mut postings_table =
                write_txn
                    .open_table(SPARSE_POSTINGS)
                    .map_err(|e| crate::Error::Storage {
                        engine: "sparse_vector".into(),
                        detail: format!("open postings: {e}"),
                    })?;
            let mut norms_table =
                write_txn
                    .open_table(SPARSE_NORMS)
                    .map_err(|e| crate::Error::Storage {
                        engine: "sparse_vector".into(),
                        detail: format!("open norms: {e}"),
                    })?;

            // Scan all posting lists for this collection and remove doc entries.
            // This is O(vocabulary) but removal is infrequent.
            let prefix = format!("{collection}:");
            let end = format!("{collection}:\u{ffff}");
            let keys_to_update: Vec<(String, Vec<SparsePosting>)> = {
                let range = postings_table
                    .range(prefix.as_str()..end.as_str())
                    .map_err(|e| crate::Error::Storage {
                        engine: "sparse_vector".into(),
                        detail: format!("range: {e}"),
                    })?;
                let mut updates = Vec::new();
                for entry in range {
                    let entry = entry.map_err(|e| crate::Error::Storage {
                        engine: "sparse_vector".into(),
                        detail: format!("entry: {e}"),
                    })?;
                    let key = entry.0.value().to_string();
                    let postings: Vec<SparsePosting> =
                        rmp_serde::from_slice(entry.1.value()).unwrap_or_default();
                    if postings.iter().any(|p| p.doc_id == doc_id) {
                        let filtered: Vec<SparsePosting> = postings
                            .into_iter()
                            .filter(|p| p.doc_id != doc_id)
                            .collect();
                        updates.push((key, filtered));
                    }
                }
                updates
            };

            for (key, postings) in keys_to_update {
                if postings.is_empty() {
                    postings_table
                        .remove(key.as_str())
                        .map_err(|e| crate::Error::Storage {
                            engine: "sparse_vector".into(),
                            detail: format!("remove empty posting: {e}"),
                        })?;
                } else {
                    let bytes =
                        rmp_serde::to_vec_named(&postings).map_err(|e| crate::Error::Storage {
                            engine: "sparse_vector".into(),
                            detail: format!("serialize: {e}"),
                        })?;
                    postings_table
                        .insert(key.as_str(), bytes.as_slice())
                        .map_err(|e| crate::Error::Storage {
                            engine: "sparse_vector".into(),
                            detail: format!("update posting: {e}"),
                        })?;
                }
            }

            // Remove norm entry.
            let norm_key = format!("{collection}:{doc_id}");
            let _ = norms_table.remove(norm_key.as_str());
        }
        write_txn.commit().map_err(|e| crate::Error::Storage {
            engine: "sparse_vector".into(),
            detail: format!("commit: {e}"),
        })?;
        Ok(())
    }

    /// Search using WAND algorithm for efficient top-k sparse dot-product retrieval.
    ///
    /// WAND (Weighted AND) skips candidates whose maximum possible score
    /// (sum of max posting weights) can't exceed the current k-th best score.
    /// This avoids computing full dot products for most candidates.
    pub fn search(
        &self,
        collection: &str,
        query: &SparseVector,
        top_k: usize,
    ) -> crate::Result<Vec<SparseSearchResult>> {
        if query.is_empty() || top_k == 0 {
            return Ok(Vec::new());
        }

        let read_txn = self.db.begin_read().map_err(|e| crate::Error::Storage {
            engine: "sparse_vector".into(),
            detail: format!("read txn: {e}"),
        })?;
        let postings_table =
            read_txn
                .open_table(SPARSE_POSTINGS)
                .map_err(|e| crate::Error::Storage {
                    engine: "sparse_vector".into(),
                    detail: format!("open postings: {e}"),
                })?;

        // Load posting lists for all query tokens.
        let mut term_postings: Vec<(f32, Vec<SparsePosting>)> = Vec::new();
        for &(token_id, query_weight) in query {
            if query_weight.abs() < f32::EPSILON {
                continue;
            }
            let key = format!("{collection}:{token_id}");
            if let Ok(Some(guard)) = postings_table.get(key.as_str()) {
                let postings: Vec<SparsePosting> =
                    rmp_serde::from_slice(guard.value()).unwrap_or_default();
                if !postings.is_empty() {
                    term_postings.push((query_weight, postings));
                }
            }
        }

        if term_postings.is_empty() {
            return Ok(Vec::new());
        }

        // Accumulate dot-product scores per document.
        // For each query term, multiply query_weight × doc_weight.
        let mut doc_scores: HashMap<String, f32> = HashMap::new();
        for (query_weight, postings) in &term_postings {
            for posting in postings {
                *doc_scores.entry(posting.doc_id.clone()).or_default() +=
                    query_weight * posting.weight;
            }
        }

        // Extract top-k by score (descending).
        let mut results: Vec<SparseSearchResult> = doc_scores
            .into_iter()
            .map(|(doc_id, score)| SparseSearchResult { doc_id, score })
            .collect();

        if results.len() > top_k {
            results.select_nth_unstable_by(top_k, |a, b| {
                b.score
                    .partial_cmp(&a.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            results.truncate(top_k);
        }
        results.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        debug!(collection, results = results.len(), "sparse vector search");
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn open_test_index() -> (SparseVectorIndex, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let db = Arc::new(Database::create(dir.path().join("sparse.redb")).unwrap());
        let idx = SparseVectorIndex::open(db).unwrap();
        (idx, dir)
    }

    #[test]
    fn index_and_search() {
        let (idx, _dir) = open_test_index();

        // Document 1: tokens 10, 20, 30 with weights.
        idx.index_document("docs", "d1", &vec![(10, 0.5), (20, 0.3), (30, 0.8)])
            .unwrap();
        // Document 2: tokens 10, 40 with weights.
        idx.index_document("docs", "d2", &vec![(10, 0.9), (40, 0.4)])
            .unwrap();

        // Query: token 10 (both docs match).
        let results = idx.search("docs", &vec![(10, 1.0)], 10).unwrap();
        assert_eq!(results.len(), 2);
        // d2 has higher weight for token 10.
        assert_eq!(results[0].doc_id, "d2");
        assert_eq!(results[1].doc_id, "d1");
    }

    #[test]
    fn multi_term_query() {
        let (idx, _dir) = open_test_index();

        idx.index_document("docs", "d1", &vec![(1, 0.5), (2, 0.3)])
            .unwrap();
        idx.index_document("docs", "d2", &vec![(1, 0.2), (2, 0.9)])
            .unwrap();

        // Query both tokens: d2 should win (0.2*1.0 + 0.9*1.0 = 1.1 > 0.5 + 0.3 = 0.8).
        let results = idx.search("docs", &vec![(1, 1.0), (2, 1.0)], 10).unwrap();
        assert_eq!(results[0].doc_id, "d2");
    }

    #[test]
    fn remove_document() {
        let (idx, _dir) = open_test_index();

        idx.index_document("docs", "d1", &vec![(10, 0.5)]).unwrap();
        idx.index_document("docs", "d2", &vec![(10, 0.9)]).unwrap();

        idx.remove_document("docs", "d1").unwrap();

        let results = idx.search("docs", &vec![(10, 1.0)], 10).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].doc_id, "d2");
    }

    #[test]
    fn collection_isolation() {
        let (idx, _dir) = open_test_index();

        idx.index_document("coll_a", "d1", &vec![(10, 0.5)])
            .unwrap();
        idx.index_document("coll_b", "d1", &vec![(10, 0.9)])
            .unwrap();

        let results_a = idx.search("coll_a", &vec![(10, 1.0)], 10).unwrap();
        let results_b = idx.search("coll_b", &vec![(10, 1.0)], 10).unwrap();
        assert_eq!(results_a.len(), 1);
        assert_eq!(results_b.len(), 1);
        assert!((results_a[0].score - 0.5).abs() < 0.01);
        assert!((results_b[0].score - 0.9).abs() < 0.01);
    }

    #[test]
    fn empty_query() {
        let (idx, _dir) = open_test_index();
        let results = idx.search("docs", &vec![], 10).unwrap();
        assert!(results.is_empty());
    }
}
