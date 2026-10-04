// SPDX-License-Identifier: Apache-2.0

//! Which documents a resolved query matches, decided once per query.
//!
//! AND mode over several words matches documents holding every word. When no
//! admitted document holds every word, the query falls back to OR with
//! coverage-scaled scores. The decision reads only whether any admitted
//! document holds every word, never the top-k cut, so it is the same for
//! every LIMIT and for every reader of the same admitted set.

use nodedb_types::{Surrogate, SurrogateBitmap};

use crate::posting::Bm25Params;
use crate::search::doc_score::{Combine, combine};
use crate::search::query_terms::ResolvedQuery;
use crate::search::staged::{StagedView, staged_contributions};

/// How a resolved query matches documents.
pub(crate) enum MatchMode {
    /// Every word must match. Holds the admitted indexed documents that hold
    /// every word.
    All(SurrogateBitmap),
    /// AND-mode fallback: any word matches, scores scale by word coverage.
    Coverage,
    /// Any word matches.
    Any,
}

impl MatchMode {
    /// How term contributions combine under this mode.
    pub(crate) fn combine(&self) -> Combine {
        match self {
            MatchMode::Coverage => Combine::Coverage,
            MatchMode::All(_) | MatchMode::Any => Combine::Sum,
        }
    }

    /// A document's score under this mode, or `None` when it does not match.
    pub(crate) fn score(
        &self,
        query: &ResolvedQuery,
        contributions: &[Option<f32>],
    ) -> Option<f32> {
        let (score, matched) = combine(
            contributions,
            &query.term_groups,
            query.groups,
            self.combine(),
        )?;
        match self {
            MatchMode::All(_) if matched < query.groups => None,
            MatchMode::All(_) | MatchMode::Coverage | MatchMode::Any => Some(score),
        }
    }
}

/// The staged documents `allow` admits and no negative term excludes, each
/// with its per-term contributions.
pub(crate) fn staged_candidates(
    query: &ResolvedQuery,
    staged: Option<&StagedView>,
    allow: Option<&SurrogateBitmap>,
    params: &Bm25Params,
) -> Vec<(Surrogate, Vec<Option<f32>>)> {
    let Some(view) = staged else {
        return Vec::new();
    };
    view.docs()
        .iter()
        .filter(|doc| allow.is_none_or(|bm| bm.contains(doc.doc_id)))
        .filter_map(|doc| {
            staged_contributions(query, doc, params)
                .map(|contributions| (doc.doc_id, contributions))
        })
        .collect()
}

impl ResolvedQuery {
    /// Decide the match mode. `allow` and `deny` bound the indexed documents
    /// the query reads. `index_visible` is `false` when the transaction
    /// truncated the collection. `staged` are the admitted staged candidates.
    pub(crate) fn match_mode(
        &self,
        index_visible: bool,
        allow: Option<&SurrogateBitmap>,
        deny: &SurrogateBitmap,
        staged: &[(Surrogate, Vec<Option<f32>>)],
    ) -> MatchMode {
        if !self.require_all {
            return MatchMode::Any;
        }
        let all_words = if index_visible {
            self.all_words_docs(allow, deny)
        } else {
            SurrogateBitmap::new()
        };
        let staged_holds_all = staged.iter().any(|(_, contributions)| {
            combine(contributions, &self.term_groups, self.groups, Combine::Sum)
                .is_some_and(|(_, matched)| matched == self.groups)
        });
        if all_words.is_empty() && !staged_holds_all {
            MatchMode::Coverage
        } else {
            MatchMode::All(all_words)
        }
    }

    /// Indexed documents holding every query word, within `allow` and
    /// outside `deny`.
    fn all_words_docs(
        &self,
        allow: Option<&SurrogateBitmap>,
        deny: &SurrogateBitmap,
    ) -> SurrogateBitmap {
        let mut result: Option<SurrogateBitmap> = None;
        for group in 0..self.groups {
            let mut word = SurrogateBitmap::new();
            for (blocks, _) in self
                .blocks
                .iter()
                .zip(&self.term_groups)
                .filter(|(_, g)| **g == group)
            {
                for doc_id in blocks.doc_ids() {
                    word.insert(doc_id);
                }
            }
            let next = match result {
                None => word,
                Some(mut acc) => {
                    acc.intersect_in_place(&word);
                    acc
                }
            };
            let empty = next.is_empty();
            result = Some(next);
            if empty {
                break;
            }
        }
        let mut docs = result.unwrap_or_default();
        if let Some(allow) = allow {
            docs.intersect_in_place(allow);
        }
        docs.difference(deny)
    }
}
