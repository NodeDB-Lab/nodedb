// SPDX-License-Identifier: BUSL-1.1

//! Fold a transaction's staging overlay into a base full-text search result,
//! so an in-transaction FTS `SEARCH` observes the transaction's own
//! uncommitted document writes (read-your-own-writes for FTS).
//!
//! FTS indexing is not a stageable write in its own right — it is an inline
//! side effect of the document write
//! (`handlers/point/apply_put/core.rs::index_document_in_txn`). There is
//! therefore no staged FTS posting to read at query time. Instead, this
//! merge makes the transaction's already-staged DOCUMENT BODIES (held in
//! [`TxnOverlay`]) searchable by re-tokenizing and BM25-scoring them at
//! query time with the exact same analyzer resolution the forward indexing
//! path uses (`InvertedIndex::analyze_for_collection` — see
//! `IndexDocScope`/`index_document_in_txn`) and the SAME corpus stats
//! (`df` / `total_docs` / `avg_doc_len`) the base
//! search read, via [`InvertedIndex::corpus_stats`] /
//! [`InvertedIndex::term_df`], so a staged doc's score is directly
//! comparable to base-search scores.
//!
//! A collection's per-collection analyzer override
//! (`InvertedIndex::analyze_for_collection`, backed by
//! `FtsIndex::set_collection_analyzer`) is resolved for staged docs exactly
//! the same way the forward indexing path (`index_document_in_txn`)
//! resolves it, so a staged doc is tokenized identically whether it is
//! still staged or already committed — no second, inconsistent tokenization
//! path.

use std::collections::HashMap;

use nodedb_fts::posting::Bm25Params;
use nodedb_fts::search::query_parser::parse_query;
use nodedb_types::Surrogate;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::handlers::transaction::overlay::{Staged, TxnOverlay};
use crate::engine::document::store::surrogate_to_doc_id;
use crate::types::{DatabaseId, TenantId, TxnId};

/// Scope + tuning for one FTS overlay merge: the transaction, the
/// `(database, tenant, collection)` it targets, the raw query text, and the
/// result truncation bound. Bundling these keeps the merge entry points to a
/// small argument count.
pub(in crate::data::executor) struct FtsMergeParams<'a> {
    pub txn_id: TxnId,
    pub database_id: DatabaseId,
    pub tid: TenantId,
    pub collection: &'a str,
    pub query: &'a str,
    pub top_k: usize,
}

impl CoreLoop {
    /// Merge staged writes described by `params` into `base_results`
    /// (surrogate, score, fuzzy triples produced by the base BM25 search),
    /// re-scoring staged puts against the query and removing staged
    /// tombstones, then re-sorting by score descending and truncating to
    /// `params.top_k`.
    ///
    /// No-op when the transaction has no overlay entries for this
    /// collection. A staged put that also appears in `base_results` is
    /// re-scored and replaces the base entry (an in-transaction UPDATE may
    /// have changed whether/how strongly the document matches). A staged
    /// put whose re-tokenized text does not match any positive query term,
    /// or is excluded by a `NOT`/`-` negative term, contributes no entry
    /// (and is removed if it was already present from base — e.g. an
    /// UPDATE that made the document no longer match).
    pub(in crate::data::executor) fn merge_fts_overlay_into_results(
        &self,
        params: FtsMergeParams<'_>,
        base_results: &mut Vec<(Surrogate, f32, bool)>,
    ) {
        let FtsMergeParams {
            txn_id,
            database_id,
            tid,
            collection,
            query,
            top_k,
        } = params;
        let coll_key = (database_id, tid, collection.to_string());
        // Read-your-own-writes refreshes the lease (see the reaper).
        self.touch_overlay(txn_id);
        let Some(overlay) = self.txn_overlays.get(&txn_id) else {
            return;
        };

        let Some((positive_terms, negative_terms)) =
            self.analyze_query_terms(database_id.as_u64(), tid, collection, query)
        else {
            // An invalid query already failed the base search with an
            // error before this merge could run — nothing to fold in.
            return;
        };
        if positive_terms.is_empty() {
            // No positive terms to score staged docs against — but staged
            // tombstones still hide base rows.
            remove_tombstoned(overlay, &coll_key, base_results);
            return;
        }

        let config_key = (database_id, tid, collection.to_string());
        let bm25_params = Bm25Params::default();
        let ctx = self.staged_score_ctx(database_id, tid, collection, &config_key, &bm25_params);

        let mut seen: HashMap<u32, usize> = base_results
            .iter()
            .enumerate()
            .map(|(idx, (s, _, _))| (s.as_u32(), idx))
            .collect();

        for (surrogate, staged) in overlay.iter_for_collection(&coll_key) {
            match staged {
                Staged::Tombstone => {
                    if let Some(idx) = seen.remove(&surrogate) {
                        base_results.remove(idx);
                        reindex_after_removal(&mut seen, idx);
                    }
                }
                Staged::Put(body) => {
                    let score =
                        self.score_staged_fts_doc(&ctx, body, &positive_terms, &negative_terms);
                    match (score, seen.get(&surrogate).copied()) {
                        (Some(s), Some(idx)) => {
                            base_results[idx].1 = s;
                            // Re-scored via the exact analyzed tokenizer, so
                            // this is no longer a fuzzy match regardless of
                            // whether the base entry was fuzzy.
                            base_results[idx].2 = false;
                        }
                        (Some(s), None) => {
                            seen.insert(surrogate, base_results.len());
                            base_results.push((Surrogate::new(surrogate), s, false));
                        }
                        (None, Some(idx)) => {
                            base_results.remove(idx);
                            seen.remove(&surrogate);
                            reindex_after_removal(&mut seen, idx);
                        }
                        (None, None) => {}
                    }
                }
            }
        }

        base_results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        base_results.truncate(top_k);
    }

    /// Phrase-search variant of the overlay merge. Unlike the bag-of-words
    /// BM25 merge, this honours the base phrase search's EXACT proximity
    /// semantics: the phrase's analyzed terms must appear as a CONTIGUOUS,
    /// in-order subsequence of the staged document's analyzed token stream
    /// (adjacency with zero slop — the same `expected_pos = start + offset`
    /// contiguity the durable phrase search enforces on stored postings).
    /// A staged doc is included only if the phrase actually matches
    /// positionally; a staged tombstone removes its base row.
    ///
    /// Positions come from the staged doc's own analyzed token indices — the
    /// forward indexer assigns posting positions the same way (`enumerate`
    /// over `InvertedIndex::analyze_for_collection` output), so no durable
    /// posting list is needed to verify adjacency. Score mirrors the base
    /// phrase formula at
    /// rank 0 (`1 / (1 + earliest_start_pos)`), keeping staged matches
    /// order-comparable with base phrase hits.
    pub(in crate::data::executor) fn merge_fts_phrase_overlay_into_results(
        &self,
        params: FtsMergeParams<'_>,
        terms: &[String],
        base_results: &mut Vec<(Surrogate, f32, bool)>,
    ) {
        let FtsMergeParams {
            txn_id,
            database_id,
            tid,
            collection,
            query: _query,
            top_k,
        } = params;
        let coll_key = (database_id, tid, collection.to_string());
        // Read-your-own-writes refreshes the lease (see the reaper).
        self.touch_overlay(txn_id);
        let Some(overlay) = self.txn_overlays.get(&txn_id) else {
            return;
        };

        // Canonicalize each phrase term through the collection's configured
        // analyzer — the same resolution the base phrase search
        // (`InvertedIndex::phrase_search`) uses — so the contiguity check
        // compares stemmed/normalized tokens on both sides.
        let db_u64 = database_id.as_u64();
        let phrase_terms: Vec<String> = terms
            .iter()
            .map(|t| {
                self.inverted
                    .analyze_for_collection(db_u64, tid, collection, t)
                    .ok()
                    .and_then(|tokens| tokens.into_iter().next())
                    .unwrap_or_else(|| t.clone())
            })
            .collect();

        let config_key = (database_id, tid, collection.to_string());

        let mut seen: HashMap<u32, usize> = base_results
            .iter()
            .enumerate()
            .map(|(idx, (s, _, _))| (s.as_u32(), idx))
            .collect();

        for (surrogate, staged) in overlay.iter_for_collection(&coll_key) {
            match staged {
                Staged::Tombstone => {
                    if let Some(idx) = seen.remove(&surrogate) {
                        base_results.remove(idx);
                        reindex_after_removal(&mut seen, idx);
                    }
                }
                Staged::Put(body) => {
                    let score =
                        self.score_staged_phrase_doc(db_u64, &config_key, body, &phrase_terms);
                    match (score, seen.get(&surrogate).copied()) {
                        (Some(s), Some(idx)) => {
                            base_results[idx].1 = s;
                            base_results[idx].2 = false;
                        }
                        (Some(s), None) => {
                            seen.insert(surrogate, base_results.len());
                            base_results.push((Surrogate::new(surrogate), s, false));
                        }
                        (None, Some(idx)) => {
                            base_results.remove(idx);
                            seen.remove(&surrogate);
                            reindex_after_removal(&mut seen, idx);
                        }
                        (None, None) => {}
                    }
                }
            }
        }

        base_results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        base_results.truncate(top_k);
    }

    /// Staged-doc scoring for `BM25ScoreScan`'s `HashMap<Surrogate, f32>`
    /// score map, which has no top-k / ranked-Vec shape and no fuzzy flag.
    pub(in crate::data::executor) fn merge_fts_overlay_into_score_map(
        &self,
        params: FtsMergeParams<'_>,
        base_results: &mut HashMap<Surrogate, f32>,
    ) {
        let FtsMergeParams {
            txn_id,
            database_id,
            tid,
            collection,
            query,
            top_k: _top_k,
        } = params;
        let coll_key = (database_id, tid, collection.to_string());
        // Read-your-own-writes refreshes the lease (see the reaper).
        self.touch_overlay(txn_id);
        let Some(overlay) = self.txn_overlays.get(&txn_id) else {
            return;
        };
        let Some((positive_terms, negative_terms)) =
            self.analyze_query_terms(database_id.as_u64(), tid, collection, query)
        else {
            return;
        };

        let config_key = (database_id, tid, collection.to_string());
        let bm25_params = Bm25Params::default();
        let ctx = self.staged_score_ctx(database_id, tid, collection, &config_key, &bm25_params);

        for (surrogate, staged) in overlay.iter_for_collection(&coll_key) {
            match staged {
                Staged::Tombstone => {
                    base_results.remove(&Surrogate::new(surrogate));
                }
                Staged::Put(body) if !positive_terms.is_empty() => {
                    match self.score_staged_fts_doc(&ctx, body, &positive_terms, &negative_terms) {
                        Some(s) => {
                            base_results.insert(Surrogate::new(surrogate), s);
                        }
                        None => {
                            base_results.remove(&Surrogate::new(surrogate));
                        }
                    }
                }
                Staged::Put(_) => {
                    base_results.remove(&Surrogate::new(surrogate));
                }
            }
        }
    }

    /// Fold the overlay into `BM25ScoreScan`'s scanned row set
    /// (`(hex_doc_id, body)` pairs) so staged-row MEMBERSHIP is gated on the
    /// FTS match — a staged doc appears as a row ONLY when its surrogate is
    /// in `score_map` (which the FTS score merge already narrowed to matches
    /// and cleared of tombstones / non-matches). This keeps row membership
    /// and scoring from ever disagreeing: a staged tombstone drops its base
    /// row, a staged put that no longer matches (score-map absent) drops
    /// its base row, and a staged put that matches is added with its staged
    /// body. Base rows with no overlay entry are kept untouched (a
    /// non-matching base row still projects with a `null` score, the
    /// existing bm25-scan semantics). Run AFTER
    /// [`merge_fts_overlay_into_score_map`](Self::merge_fts_overlay_into_score_map).
    pub(in crate::data::executor) fn merge_fts_rows_from_score_map(
        &self,
        params: FtsMergeParams<'_>,
        rows: &mut Vec<(String, Vec<u8>)>,
        score_map: &HashMap<Surrogate, f32>,
    ) {
        let FtsMergeParams {
            txn_id,
            database_id,
            tid,
            collection,
            ..
        } = params;
        let coll_key = (database_id, tid, collection.to_string());
        // Read-your-own-writes refreshes the lease (see the reaper).
        self.touch_overlay(txn_id);
        let Some(overlay) = self.txn_overlays.get(&txn_id) else {
            return;
        };

        let mut seen: std::collections::HashSet<u32> = rows
            .iter()
            .filter_map(|(k, _)| u32::from_str_radix(k, 16).ok())
            .collect();

        rows.retain_mut(|(row_key, body)| {
            let Ok(surrogate) = u32::from_str_radix(row_key, 16) else {
                return true;
            };
            match overlay.get(&coll_key, surrogate) {
                Some(Staged::Tombstone) => false,
                Some(Staged::Put(staged_body)) => {
                    *body = staged_body.clone();
                    score_map.contains_key(&Surrogate::new(surrogate))
                }
                None => true,
            }
        });

        for (surrogate, staged) in overlay.iter_for_collection(&coll_key) {
            if seen.contains(&surrogate) {
                continue;
            }
            if let Staged::Put(body) = staged
                && score_map.contains_key(&Surrogate::new(surrogate))
            {
                rows.push((surrogate_to_doc_id(Surrogate::new(surrogate)), body.clone()));
                seen.insert(surrogate);
            }
        }
    }

    /// Parse `query` and analyze its positive/negative terms with the
    /// collection's configured analyzer (`InvertedIndex::analyze_for_collection`
    /// — the same resolution the forward-indexing path and the base search
    /// use), once per merge call (never per staged document — every staged
    /// doc in the merge loop is scored against this same pair of term
    /// lists). Returns `None` when `query` fails to parse (the base search
    /// already surfaced that error).
    fn analyze_query_terms(
        &self,
        database_id: u64,
        tid: TenantId,
        collection: &str,
        query: &str,
    ) -> Option<(Vec<String>, Vec<String>)> {
        let parsed = parse_query(query).ok()?;
        let positive_terms = self
            .inverted
            .analyze_for_collection(database_id, tid, collection, &parsed.positive.join(" "))
            .unwrap_or_default();
        let negative_terms = self
            .inverted
            .analyze_for_collection(database_id, tid, collection, &parsed.negative.join(" "))
            .unwrap_or_default();
        Some((positive_terms, negative_terms))
    }
}

/// Remove every staged tombstone's surrogate from `base_results` when there
/// are no positive query terms to score staged puts against (e.g. an
/// all-stop-word query).
fn remove_tombstoned(
    overlay: &TxnOverlay,
    coll_key: &(DatabaseId, TenantId, String),
    base_results: &mut Vec<(Surrogate, f32, bool)>,
) {
    let tombstoned: std::collections::HashSet<u32> = overlay
        .iter_for_collection(coll_key)
        .filter(|(_, staged)| matches!(staged, Staged::Tombstone))
        .map(|(surrogate, _)| surrogate)
        .collect();
    if tombstoned.is_empty() {
        return;
    }
    base_results.retain(|(s, _, _)| !tombstoned.contains(&s.as_u32()));
}

/// After a `Vec::remove(idx)` shifts every later element left by one, shift
/// every recorded index greater than `idx` in `seen` to match.
fn reindex_after_removal(seen: &mut HashMap<u32, usize>, removed_idx: usize) {
    for idx in seen.values_mut() {
        if *idx > removed_idx {
            *idx -= 1;
        }
    }
}
