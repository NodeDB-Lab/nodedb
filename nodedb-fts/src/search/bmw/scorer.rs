// SPDX-License-Identifier: Apache-2.0

//! Block-Max WAND scoring engine.
//!
//! Two-level skip: WAND pivot selection across terms, then block-level
//! pruning within each term's posting list. Operates on `Surrogate` row
//! identities for zero-allocation scoring.
//!
//! Admission (the allow and deny sets) is checked before a document is
//! scored, so the top-k cut counts only admitted documents. Every term score
//! comes from [`term_score`] and is combined by [`combine`] in term order, the
//! same functions the per-document scorer uses.

use nodedb_types::{Surrogate, SurrogateBitmap};

use crate::bm25;
use crate::posting::Bm25Params;
use crate::search::doc_score::{Combine, combine, term_score};

use super::heap::TopKHeap;
use super::skip_index::TermBlocks;

/// Sentinel returned by an exhausted cursor — sorts after every real surrogate.
const EXHAUSTED: Surrogate = Surrogate(u32::MAX);

/// Inputs of one BMW run.
pub struct BmwInput<'a> {
    /// Posting blocks of each query term.
    pub terms: &'a [TermBlocks],
    /// Query word of each term, parallel to `terms`.
    pub term_groups: &'a [usize],
    /// Number of query words.
    pub groups: usize,
    pub total_docs: u32,
    pub avg_doc_len: f32,
    pub params: &'a Bm25Params,
    pub top_k: usize,
    /// Only these documents are scored. `None` admits every document.
    pub allow: Option<&'a SurrogateBitmap>,
    /// These documents are never scored.
    pub deny: Option<&'a SurrogateBitmap>,
    /// How term contributions combine into a document score. Every mode
    /// scores at most the plain sum, so the BMW upper bounds stay valid.
    pub combine: Combine,
}

/// Per-term iterator state during BMW traversal.
struct TermCursor<'a> {
    /// The term's posting blocks + skip index.
    term: &'a TermBlocks,
    /// Position of the term in the query's term list.
    term_idx: usize,
    /// Current block index within `term.blocks`.
    block_idx: usize,
    /// Current position within the current block's doc_ids.
    pos_in_block: usize,
    /// Precomputed IDF for this term.
    idf: f32,
    /// Term's global max score (for WAND pivot selection).
    max_score: f32,
}

impl<'a> TermCursor<'a> {
    fn new(
        term: &'a TermBlocks,
        term_idx: usize,
        total_docs: u32,
        avg_doc_len: f32,
        params: &Bm25Params,
    ) -> Self {
        let idf = bm25::idf(term.df, total_docs);
        let max_score = bm25::term_max_score(
            term.global_max_tf,
            term.global_min_fieldnorm,
            term.df,
            total_docs,
            avg_doc_len,
            params,
        );
        Self {
            term,
            term_idx,
            block_idx: 0,
            pos_in_block: 0,
            idf,
            max_score,
        }
    }

    /// Current surrogate the cursor points to, or `EXHAUSTED` if past end.
    fn current_doc_id(&self) -> Surrogate {
        if self.block_idx >= self.term.blocks.len() {
            return EXHAUSTED;
        }
        let block = &self.term.blocks[self.block_idx];
        if self.pos_in_block >= block.doc_ids.len() {
            return EXHAUSTED;
        }
        block.doc_ids[self.pos_in_block]
    }

    /// Advance cursor to the first surrogate ≥ `target`.
    fn advance_to(&mut self, target: Surrogate) {
        // Skip entire blocks using the skip index.
        if let Some(blk) = self.term.advance_to_block(target) {
            if blk > self.block_idx {
                self.block_idx = blk;
                self.pos_in_block = 0;
            }
        } else {
            // All blocks exhausted.
            self.block_idx = self.term.blocks.len();
            return;
        }

        // Within the current block, linear scan to target.
        if self.block_idx < self.term.blocks.len() {
            let block = &self.term.blocks[self.block_idx];
            while self.pos_in_block < block.doc_ids.len()
                && block.doc_ids[self.pos_in_block] < target
            {
                self.pos_in_block += 1;
            }
            // If we've exhausted the block, move to next.
            if self.pos_in_block >= block.doc_ids.len() {
                self.block_idx += 1;
                self.pos_in_block = 0;
                // The next block's first doc must be ≥ target
                // (blocks are sorted), so no further scan needed.
            }
        }
    }

    /// Advance cursor past the current doc (to the next doc).
    fn next(&mut self) {
        if self.block_idx >= self.term.blocks.len() {
            return;
        }
        self.pos_in_block += 1;
        let block = &self.term.blocks[self.block_idx];
        if self.pos_in_block >= block.doc_ids.len() {
            self.block_idx += 1;
            self.pos_in_block = 0;
        }
    }

    /// Whether the cursor is exhausted (no more docs).
    fn is_exhausted(&self) -> bool {
        self.block_idx >= self.term.blocks.len()
    }

    /// Compute the block upper bound for the current block.
    fn block_upper_bound(&self, total_docs: u32, avg_doc_len: f32, params: &Bm25Params) -> f32 {
        if self.block_idx >= self.term.blocks.len() {
            return 0.0;
        }
        let meta = &self.term.skip[self.block_idx].meta;
        bm25::bm25_block_upper_bound(
            meta.block_max_tf,
            meta.block_min_fieldnorm,
            self.term.df,
            total_docs,
            avg_doc_len,
            params,
        )
    }

    /// Score the current document.
    fn score_current(&self, avg_doc_len: f32, params: &Bm25Params) -> f32 {
        if self.is_exhausted() {
            return 0.0;
        }
        let block = &self.term.blocks[self.block_idx];
        term_score(
            self.idf,
            block.term_freqs[self.pos_in_block],
            block.fieldnorms[self.pos_in_block],
            avg_doc_len,
            params,
        )
    }
}

/// Whether `doc_id` may be scored.
fn admitted(input: &BmwInput<'_>, doc_id: Surrogate) -> bool {
    input.allow.is_none_or(|bm| bm.contains(doc_id))
        && !input.deny.is_some_and(|bm| bm.contains(doc_id))
}

/// Run BMW scoring across multiple term posting lists.
///
/// Returns the top-k admitted documents by score.
pub fn bmw_score(input: &BmwInput<'_>) -> TopKHeap {
    let mut heap = TopKHeap::new(input.top_k);
    let BmwInput {
        terms,
        term_groups,
        groups,
        total_docs,
        avg_doc_len,
        params,
        ..
    } = *input;

    if input.top_k == 0 || terms.is_empty() || input.allow.is_some_and(|bm| bm.is_empty()) {
        return heap;
    }

    let mut cursors: Vec<TermCursor> = terms
        .iter()
        .enumerate()
        .filter(|(_, tb)| tb.df > 0)
        .map(|(idx, tb)| TermCursor::new(tb, idx, total_docs, avg_doc_len, params))
        .collect();

    if cursors.is_empty() {
        return heap;
    }

    let mut contributions: Vec<Option<f32>> = vec![None; terms.len()];

    loop {
        // Sort cursors by current doc_id (ascending). Exhausted cursors go to the end.
        cursors.sort_by_key(|c| c.current_doc_id());

        // Remove exhausted cursors.
        while cursors.last().is_some_and(|c| c.is_exhausted()) {
            cursors.pop();
        }
        if cursors.is_empty() {
            break;
        }

        let threshold = heap.threshold();

        // WAND pivot selection: accumulate max_scores until sum > threshold.
        let mut pivot_idx = 0;
        let mut accumulated = 0.0f32;
        for (i, cursor) in cursors.iter().enumerate() {
            accumulated += cursor.max_score;
            pivot_idx = i;
            if accumulated > threshold {
                break;
            }
        }

        // If even all terms' max scores can't beat threshold, we're done.
        if accumulated <= threshold {
            break;
        }

        let pivot_doc_id = cursors[pivot_idx].current_doc_id();
        if pivot_doc_id == EXHAUSTED {
            break;
        }

        // Check if all cursors [0..=pivot_idx] point to the same doc_id.
        let first_doc_id = cursors[0].current_doc_id();
        if first_doc_id == pivot_doc_id {
            if admitted(input, pivot_doc_id) {
                // Block-level pruning over every cursor on the pivot doc: a
                // cursor past the pivot index can sit on the same doc and
                // adds to its score.
                let block_upper: f32 = cursors
                    .iter()
                    .filter(|c| c.current_doc_id() == pivot_doc_id)
                    .map(|c| c.block_upper_bound(total_docs, avg_doc_len, params))
                    .sum();

                if block_upper > threshold {
                    contributions.fill(None);
                    for cursor in cursors.iter() {
                        if cursor.current_doc_id() == pivot_doc_id {
                            contributions[cursor.term_idx] =
                                Some(cursor.score_current(avg_doc_len, params));
                        }
                    }
                    if let Some((score, _)) =
                        combine(&contributions, term_groups, groups, input.combine)
                    {
                        heap.insert(score, pivot_doc_id);
                    }
                }
            }

            // Advance all cursors that were at pivot_doc_id.
            for cursor in &mut cursors {
                if cursor.current_doc_id() == pivot_doc_id {
                    cursor.next();
                }
            }
        } else {
            // Not all essential cursors at pivot — advance cursors before pivot to pivot_doc_id.
            for cursor in cursors.iter_mut().take(pivot_idx) {
                if cursor.current_doc_id() < pivot_doc_id {
                    cursor.advance_to(pivot_doc_id);
                }
            }
        }
    }

    heap
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::CompactPosting;
    use crate::codec::smallfloat;

    fn make_term(doc_ids: &[u32], tf: u32) -> TermBlocks {
        let postings: Vec<CompactPosting> = doc_ids
            .iter()
            .map(|&id| CompactPosting {
                doc_id: Surrogate(id),
                term_freq: tf,
                fieldnorm: smallfloat::encode(100),
                positions: vec![0],
            })
            .collect();
        let blocks = crate::block::into_blocks(postings);
        TermBlocks::from_blocks(blocks)
    }

    fn run(
        terms: &[TermBlocks],
        total_docs: u32,
        top_k: usize,
        allow: Option<&SurrogateBitmap>,
        deny: Option<&SurrogateBitmap>,
    ) -> Vec<Surrogate> {
        let params = Bm25Params::default();
        let term_groups: Vec<usize> = (0..terms.len()).collect();
        bmw_score(&BmwInput {
            terms,
            term_groups: &term_groups,
            groups: terms.len(),
            total_docs,
            avg_doc_len: 100.0,
            params: &params,
            top_k,
            allow,
            deny,
            combine: Combine::Sum,
        })
        .into_sorted()
        .iter()
        .map(|d| d.doc_id)
        .collect()
    }

    #[test]
    fn bmw_basic() {
        let term_a = make_term(&[0, 1, 2, 3, 4], 2);
        let term_b = make_term(&[2, 3, 5, 6], 3);
        let top_ids = run(&[term_a, term_b], 100, 3, None, None);
        assert!(!top_ids.is_empty());
        // Docs 2 and 3 match both terms — should score highest.
        assert!(top_ids.contains(&Surrogate(2)));
        assert!(top_ids.contains(&Surrogate(3)));
    }

    #[test]
    fn bmw_single_term() {
        let term = make_term(&[10, 20, 30, 40, 50], 1);
        assert_eq!(run(&[term], 1000, 3, None, None).len(), 3);
    }

    #[test]
    fn bmw_empty_terms() {
        assert!(run(&[], 1000, 10, None, None).is_empty());
    }

    #[test]
    fn bmw_respects_top_k() {
        let ids: Vec<u32> = (0..500).collect();
        let term = make_term(&ids, 1);
        assert_eq!(run(&[term], 1000, 5, None, None).len(), 5);
    }

    #[test]
    fn bmw_large_collection() {
        // Many docs for term_a, few for term_b (rare = high IDF).
        let common_ids: Vec<u32> = (0..1000).collect();
        let rare_ids = vec![50, 200, 500];

        let term_common = make_term(&common_ids, 1);
        let term_rare = make_term(&rare_ids, 5);

        let top_ids = run(&[term_common, term_rare], 10_000, 3, None, None);
        assert_eq!(top_ids.len(), 3);
        // The rare-term docs (50, 200, 500) should dominate due to high IDF + tf.
        assert!(top_ids.contains(&Surrogate(50)));
        assert!(top_ids.contains(&Surrogate(200)));
        assert!(top_ids.contains(&Surrogate(500)));
    }

    #[test]
    fn deny_set_is_excluded_before_the_cut() {
        let term = make_term(&[1, 2, 3, 4, 5], 1);
        let mut deny = SurrogateBitmap::new();
        deny.insert(Surrogate(1));
        deny.insert(Surrogate(2));
        let top_ids = run(&[term], 100, 2, None, Some(&deny));
        assert_eq!(top_ids.len(), 2, "the cut counts only admitted documents");
        assert!(!top_ids.contains(&Surrogate(1)));
        assert!(!top_ids.contains(&Surrogate(2)));
    }

    #[test]
    fn allow_set_restricts_candidates() {
        let term = make_term(&[1, 2, 3, 4, 5], 1);
        let mut allow = SurrogateBitmap::new();
        allow.insert(Surrogate(4));
        assert_eq!(
            run(&[term], 100, 10, Some(&allow), None),
            vec![Surrogate(4)]
        );
    }

    /// The document both terms occur in outranks every single-term document,
    /// whichever cursor the pivot lands on.
    #[test]
    fn two_term_document_wins_top_one() {
        let common: Vec<u32> = (0..300).collect();
        let a = make_term(&common, 1);
        let b = make_term(&[150], 1);
        assert_eq!(run(&[a, b], 300, 1, None, None), vec![Surrogate(150)]);
    }
}
