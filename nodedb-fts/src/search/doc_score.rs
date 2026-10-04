// SPDX-License-Identifier: Apache-2.0

//! The one BM25 formula every search path scores a document with.
//!
//! The BMW scorer, the per-document scorer, and the scorer of a
//! transaction's staged documents all call these functions. A document
//! therefore scores the same value whichever path reads it.

use crate::codec::smallfloat;
use crate::posting::Bm25Params;

/// BM25 contribution of one term to one document: `idf` of the term, the
/// term's frequency in the document, and the document's fieldnorm byte.
pub(crate) fn term_score(
    idf: f32,
    tf: u32,
    fieldnorm: u8,
    avg_doc_len: f32,
    params: &Bm25Params,
) -> f32 {
    let doc_len = smallfloat::decode(fieldnorm).max(1);
    let tf_f = tf as f32;
    let dl = doc_len as f32;
    let tf_norm = (tf_f * (params.k1 + 1.0))
        / (tf_f + params.k1 * (1.0 - params.b + params.b * dl / avg_doc_len));
    idf * tf_norm
}

/// How a document's term contributions turn into its score.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Combine {
    /// The plain sum.
    Sum,
    /// The sum scaled by the share of query words the document matches.
    /// The AND-mode fallback ranks partial matches this way.
    Coverage,
}

/// A document's score from its per-term contributions, summed in term order.
///
/// `contributions` and `term_groups` are parallel: one entry per query term,
/// `None` where the term does not occur in the document. Returns the score and
/// the number of distinct query words matched, or `None` when no term occurs.
pub(crate) fn combine(
    contributions: &[Option<f32>],
    term_groups: &[usize],
    groups: usize,
    mode: Combine,
) -> Option<(f32, usize)> {
    let mut score = 0.0f32;
    let mut matched_any = false;
    let mut matched = vec![false; groups];
    for (contribution, group) in contributions.iter().zip(term_groups) {
        if let Some(value) = contribution {
            score += value;
            matched_any = true;
            if let Some(slot) = matched.get_mut(*group) {
                *slot = true;
            }
        }
    }
    if !matched_any {
        return None;
    }
    let matched_groups = matched.iter().filter(|m| **m).count();
    let score = match mode {
        Combine::Sum => score,
        Combine::Coverage => score * (matched_groups as f32 / groups.max(1) as f32),
    };
    Some((score, matched_groups))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn combine_counts_distinct_words() {
        let contributions = [Some(1.0), Some(2.0), None];
        let (score, matched) = combine(&contributions, &[0, 0, 1], 2, Combine::Sum).unwrap();
        assert_eq!(score, 3.0);
        assert_eq!(matched, 1);
    }

    #[test]
    fn coverage_scales_by_matched_share() {
        let contributions = [Some(2.0), None];
        let (score, matched) = combine(&contributions, &[0, 1], 2, Combine::Coverage).unwrap();
        assert_eq!(score, 1.0);
        assert_eq!(matched, 1);
    }

    #[test]
    fn no_contribution_is_no_match() {
        assert!(combine(&[None, None], &[0, 1], 2, Combine::Sum).is_none());
    }

    #[test]
    fn term_score_matches_bm25_formula() {
        let params = Bm25Params::default();
        let fieldnorm = smallfloat::encode(10);
        let idf = crate::bm25::idf(3, 100);
        let expected =
            crate::bm25::bm25_score(2, 3, smallfloat::decode(fieldnorm), 100, 10.0, &params);
        let got = term_score(idf, 2, fieldnorm, 10.0, &params);
        assert!((expected - got).abs() < 1e-6);
    }
}
