// SPDX-License-Identifier: BUSL-1.1

//! Fold a transaction's staged document writes into a sparse vector search,
//! so a `sparse_score(...)` inside a transaction block scores the block's own
//! writes.
//!
//! A sparse vector is a strict-schema column of a document. The index update
//! is a side effect of the document write, so a staged write has no posting
//! yet. The merge reads each staged body, parses the queried field's
//! literal, and scores it against the query with the dot product the index
//! uses. A staged delete hides the row. Scores are higher-is-better.

use std::collections::HashMap;

use nodedb_types::{SparseVector, Surrogate};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::hybrid_key::HybridFusionKey;
use crate::data::executor::handlers::transaction::overlay::Staged;
use crate::data::executor::response_codec::VectorSearchHit;
use crate::types::{DatabaseId, TenantId, TxnId};

use super::vector_merge::reindex_after_removal;

/// Inputs for [`CoreLoop::merge_sparse_overlay_into_search`].
pub(in crate::data::executor) struct SparseMergeParams<'a> {
    pub txn_id: TxnId,
    pub database_id: DatabaseId,
    pub tid: TenantId,
    pub collection: &'a str,
    /// The sparse column the search ranks.
    pub field_name: &'a str,
    pub query: &'a SparseVector,
    /// The number of hits the merged result keeps.
    pub truncate_to: usize,
}

/// The dot product of `doc` and `query`, or `None` when they share no
/// dimension: the index ranks only documents with a posting the query reads.
fn staged_score(doc: &SparseVector, query: &SparseVector) -> Option<f32> {
    let weights: HashMap<u32, f32> = doc.entries().iter().copied().collect();
    let mut shared = false;
    let mut score = 0.0f32;
    for &(dim, q_weight) in query.entries() {
        if let Some(weight) = weights.get(&dim) {
            shared = true;
            score += q_weight * weight;
        }
    }
    shared.then_some(score)
}

impl CoreLoop {
    /// Merge the staged writes `params` names into `hits`: re-score each
    /// staged put that carries the queried field, remove each staged delete
    /// and each staged put without an overlapping entry, then sort by score
    /// descending and keep `params.truncate_to` hits. A staged hit carries
    /// its body in the shape a base hit's attached body has.
    ///
    /// A staged body that will not decode fails the search: the merge exists
    /// so a transaction sees its own writes.
    pub(in crate::data::executor) fn merge_sparse_overlay_into_search(
        &self,
        params: SparseMergeParams<'_>,
        hits: &mut Vec<VectorSearchHit>,
    ) -> crate::Result<()> {
        let SparseMergeParams {
            txn_id,
            database_id,
            tid,
            collection,
            field_name,
            query,
            truncate_to,
        } = params;
        let coll_key = (database_id, tid, collection.to_string());
        // Read-your-own-writes refreshes the lease (see the reaper).
        self.touch_overlay(txn_id);
        if let Some(overlay) = self.txn_overlays.get(&txn_id) {
            // A staged TRUNCATE hides every base hit; staged puts re-enter below.
            if overlay.is_truncated(&coll_key) {
                hits.clear();
            }
            let format = self.sparse_body_format(database_id, tid, collection);
            let mut seen: HashMap<HybridFusionKey, usize> = hits
                .iter()
                .enumerate()
                .map(|(idx, h)| (h.id, idx))
                .collect();
            for (surrogate, staged) in overlay.iter_for_collection(&coll_key) {
                let key = HybridFusionKey::for_surrogate(Surrogate::new(surrogate));
                let scored = match staged {
                    Staged::Tombstone => None,
                    Staged::Put(body) => {
                        let doc = self.decode_indexed_body(&coll_key, body)?;
                        doc.as_ref()
                            .and_then(|doc| doc.get(field_name))
                            .and_then(serde_json::Value::as_str)
                            .and_then(|literal| SparseVector::parse_literal(literal).ok())
                            .and_then(|vector| staged_score(&vector, query))
                            .map(|score| (score, body))
                    }
                };
                match scored {
                    None => {
                        if let Some(idx) = seen.remove(&key) {
                            hits.remove(idx);
                            reindex_after_removal(&mut seen, idx);
                        }
                    }
                    Some((score, body)) => {
                        let body = crate::data::executor::scan_normalize::sparse_body_to_msgpack(
                            body,
                            format.as_format_ref(),
                        )
                        .into_owned();
                        match seen.get(&key).copied() {
                            Some(idx) => {
                                hits[idx].distance = score;
                                hits[idx].body = Some(body);
                            }
                            None => {
                                seen.insert(key, hits.len());
                                hits.push(VectorSearchHit {
                                    id: key,
                                    distance: score,
                                    doc_id: None,
                                    body: Some(body),
                                });
                            }
                        }
                    }
                }
            }
        }
        hits.sort_by(|a, b| {
            b.distance
                .partial_cmp(&a.distance)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        hits.truncate(truncate_to);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sv(entries: &[(u32, f32)]) -> SparseVector {
        SparseVector::from_entries(entries.to_vec()).expect("sparse vector")
    }

    #[test]
    fn a_staged_vector_scores_its_shared_dimensions_only() {
        let query = sv(&[(1, 2.0), (3, 1.0)]);
        assert_eq!(staged_score(&sv(&[(1, 0.5), (2, 9.0)]), &query), Some(1.0));
        assert_eq!(staged_score(&sv(&[(2, 9.0)]), &query), None);
    }
}
