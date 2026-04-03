//! Phrase proximity boost for BM25 search results.
//!
//! Uses the existing `Posting.positions` data to detect consecutive query
//! tokens at consecutive positions. Applies a multiplicative boost:
//! - Full phrase match (all tokens consecutive): 3x
//! - Partial phrase (some consecutive pairs): scaled between 1x and 3x
//!
//! Reference: oxidoc-search/src/lexical/scoring.rs:118-164

use std::collections::HashMap;

use crate::backend::FtsBackend;
use crate::posting::Posting;

/// Maximum phrase boost multiplier for a full consecutive phrase match.
const FULL_PHRASE_BOOST: f32 = 3.0;

/// Compute a phrase proximity boost for a document given its postings
/// across the query terms.
///
/// `query_tokens` are the analyzed tokens in order. `doc_postings` maps
/// each query token index to the posting for this document (if present).
///
/// Returns a multiplier >= 1.0. Full phrase match returns `FULL_PHRASE_BOOST`.
pub fn phrase_boost(query_tokens: &[String], doc_postings: &HashMap<usize, &Posting>) -> f32 {
    if query_tokens.len() < 2 {
        return 1.0;
    }

    let mut consecutive_pairs = 0u32;
    let total_pairs = (query_tokens.len() - 1) as u32;

    for i in 0..query_tokens.len() - 1 {
        let (Some(posting_a), Some(posting_b)) = (doc_postings.get(&i), doc_postings.get(&(i + 1)))
        else {
            continue;
        };

        // Check if any position in posting_a is immediately followed by
        // a position in posting_b (i.e., pos_b == pos_a + 1).
        if has_consecutive_positions(&posting_a.positions, &posting_b.positions) {
            consecutive_pairs += 1;
        }
    }

    if consecutive_pairs == 0 {
        return 1.0;
    }

    // Scale linearly: 1 pair out of N gives partial boost,
    // all pairs gives full FULL_PHRASE_BOOST.
    let ratio = consecutive_pairs as f32 / total_pairs as f32;
    1.0 + ratio * (FULL_PHRASE_BOOST - 1.0)
}

/// Check if any position in `a` is immediately followed by a position in `b`.
///
/// Both position lists are sorted (ascending), so we can merge-scan in O(n+m).
fn has_consecutive_positions(a: &[u32], b: &[u32]) -> bool {
    let mut i = 0;
    let mut j = 0;

    while i < a.len() && j < b.len() {
        let target = a[i] + 1;
        match b[j].cmp(&target) {
            std::cmp::Ordering::Equal => return true,
            std::cmp::Ordering::Less => j += 1,
            std::cmp::Ordering::Greater => i += 1,
        }
    }

    false
}

/// Collect per-document postings for phrase boost computation.
///
/// Given the query tokens and the posting lists retrieved during BM25 scoring,
/// returns a map from `doc_id` → (token_index → posting).
pub(crate) fn collect_doc_postings<'a, B: FtsBackend>(
    query_tokens: &[String],
    term_postings: &'a [(Vec<Posting>, bool)],
    _backend: &B,
) -> HashMap<String, HashMap<usize, &'a Posting>> {
    let mut doc_map: HashMap<String, HashMap<usize, &Posting>> = HashMap::new();

    for (token_idx, (postings, _is_fuzzy)) in term_postings.iter().enumerate() {
        for posting in postings {
            doc_map
                .entry(posting.doc_id.clone())
                .or_default()
                .insert(token_idx, posting);
        }
    }

    // Only keep documents that have at least 2 matched terms (phrase boost is meaningless for 1).
    if query_tokens.len() >= 2 {
        doc_map.retain(|_, postings| postings.len() >= 2);
    }

    doc_map
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_posting(doc_id: &str, positions: Vec<u32>) -> Posting {
        Posting {
            doc_id: doc_id.to_string(),
            term_freq: positions.len() as u32,
            positions,
        }
    }

    #[test]
    fn full_phrase_match() {
        let tokens = vec!["hello".into(), "world".into()];
        let p0 = make_posting("d1", vec![0, 5]);
        let p1 = make_posting("d1", vec![1, 8]);
        let mut doc = HashMap::new();
        doc.insert(0, &p0);
        doc.insert(1, &p1);

        let boost = phrase_boost(&tokens, &doc);
        assert!((boost - FULL_PHRASE_BOOST).abs() < f32::EPSILON);
    }

    #[test]
    fn no_phrase_match() {
        let tokens = vec!["hello".into(), "world".into()];
        let p0 = make_posting("d1", vec![0]);
        let p1 = make_posting("d1", vec![5]); // Not consecutive with 0
        let mut doc = HashMap::new();
        doc.insert(0, &p0);
        doc.insert(1, &p1);

        let boost = phrase_boost(&tokens, &doc);
        assert!((boost - 1.0).abs() < f32::EPSILON);
    }

    #[test]
    fn partial_phrase_match() {
        let tokens = vec!["the".into(), "quick".into(), "brown".into()];
        let p0 = make_posting("d1", vec![0]);
        let p1 = make_posting("d1", vec![1]); // consecutive with p0
        let p2 = make_posting("d1", vec![5]); // NOT consecutive with p1
        let mut doc = HashMap::new();
        doc.insert(0, &p0);
        doc.insert(1, &p1);
        doc.insert(2, &p2);

        let boost = phrase_boost(&tokens, &doc);
        // 1 pair out of 2 → ratio = 0.5 → boost = 1 + 0.5 * 2 = 2.0
        assert!((boost - 2.0).abs() < f32::EPSILON);
    }

    #[test]
    fn single_token_no_boost() {
        let tokens = vec!["hello".into()];
        let doc: HashMap<usize, &Posting> = HashMap::new();
        assert!((phrase_boost(&tokens, &doc) - 1.0).abs() < f32::EPSILON);
    }

    #[test]
    fn consecutive_positions_merge_scan() {
        assert!(has_consecutive_positions(&[0, 3, 7], &[1, 5, 9])); // 0+1 = 1
        assert!(has_consecutive_positions(&[2, 5, 10], &[3, 8, 11])); // 2+1=3, 10+1=11
        assert!(!has_consecutive_positions(&[0, 5, 10], &[3, 8, 15]));
        assert!(!has_consecutive_positions(&[], &[1, 2]));
        assert!(!has_consecutive_positions(&[0], &[]));
    }
}
