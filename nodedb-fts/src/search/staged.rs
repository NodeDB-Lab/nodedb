// SPDX-License-Identifier: Apache-2.0

//! An open transaction's view over one index.
//!
//! A transaction's writes are not in the index until it commits. A search
//! inside the transaction reads the index with the documents the transaction
//! replaced or removed hidden, and scores the transaction's own documents
//! from their analyzed tokens with the same query semantics and formula as
//! indexed documents. Staged documents score against the index's corpus
//! statistics.

use std::collections::HashMap;

use nodedb_types::{Surrogate, SurrogateBitmap};

use crate::codec::smallfloat;
use crate::posting::Bm25Params;
use crate::search::doc_score::term_score;
use crate::search::query_terms::ResolvedQuery;

/// A document the transaction wrote, with the analyzed tokens of the text
/// it holds in the index, in order.
#[derive(Debug, Clone)]
pub struct StagedDoc {
    pub doc_id: Surrogate,
    pub tokens: Vec<String>,
}

/// The transaction's writes as one index sees them.
#[derive(Debug, Clone)]
pub struct StagedView {
    hidden: SurrogateBitmap,
    hide_all: bool,
    docs: Vec<StagedDoc>,
    by_id: HashMap<Surrogate, usize>,
}

impl StagedView {
    /// `hidden`: indexed documents the transaction replaced or removed.
    /// `hide_all`: the transaction truncated the collection, so no indexed
    /// document counts. `docs`: the transaction's documents that hold text
    /// in this index. A document listed in `docs` is hidden in the index too.
    pub fn new(mut hidden: SurrogateBitmap, hide_all: bool, docs: Vec<StagedDoc>) -> Self {
        let by_id = docs
            .iter()
            .enumerate()
            .map(|(idx, doc)| {
                hidden.insert(doc.doc_id);
                (doc.doc_id, idx)
            })
            .collect();
        Self {
            hidden,
            hide_all,
            docs,
            by_id,
        }
    }

    /// Whether the transaction replaced or removed the indexed `doc_id`.
    pub fn hides(&self, doc_id: Surrogate) -> bool {
        self.hide_all || self.hidden.contains(doc_id)
    }

    /// Whether every indexed document is hidden.
    pub fn hides_all(&self) -> bool {
        self.hide_all
    }

    /// Indexed documents the transaction replaced or removed.
    pub fn hidden(&self) -> &SurrogateBitmap {
        &self.hidden
    }

    /// The transaction's documents that hold text in this index.
    pub fn docs(&self) -> &[StagedDoc] {
        &self.docs
    }

    /// The staged document `doc_id`, when the transaction wrote one that
    /// holds text in this index.
    pub fn doc(&self, doc_id: Surrogate) -> Option<&StagedDoc> {
        self.by_id.get(&doc_id).and_then(|idx| self.docs.get(*idx))
    }

    /// Whether any staged document holds `term`.
    pub(crate) fn holds_term(&self, term: &str) -> bool {
        self.docs.iter().any(|d| d.tokens.iter().any(|t| t == term))
    }

    /// Every distinct token of the staged documents.
    pub(crate) fn vocabulary(&self) -> Vec<&str> {
        let mut terms: Vec<&str> = self
            .docs
            .iter()
            .flat_map(|d| d.tokens.iter().map(String::as_str))
            .collect();
        terms.sort_unstable();
        terms.dedup();
        terms
    }
}

/// Per-term contributions of a staged document to `query`, parallel to the
/// query's terms. `None` when a negative term excludes the document.
pub(crate) fn staged_contributions(
    query: &ResolvedQuery,
    doc: &StagedDoc,
    params: &Bm25Params,
) -> Option<Vec<Option<f32>>> {
    let mut tf: HashMap<&str, u32> = HashMap::new();
    for token in &doc.tokens {
        *tf.entry(token.as_str()).or_insert(0) += 1;
    }
    if query
        .negative_terms
        .iter()
        .any(|term| tf.contains_key(term.as_str()))
    {
        return None;
    }
    let doc_len = u32::try_from(doc.tokens.len()).unwrap_or(u32::MAX);
    let fieldnorm = smallfloat::encode(doc_len);
    Some(
        query
            .texts
            .iter()
            .zip(&query.blocks)
            .map(|(text, blocks)| {
                tf.get(text.as_str()).map(|freq| {
                    term_score(
                        crate::bm25::idf(blocks.df, query.total_docs),
                        *freq,
                        fieldnorm,
                        query.avg_doc_len,
                        params,
                    )
                })
            })
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn staged_docs_are_hidden_in_the_index() {
        let view = StagedView::new(
            SurrogateBitmap::new(),
            false,
            vec![StagedDoc {
                doc_id: Surrogate(7),
                tokens: vec!["rust".into()],
            }],
        );
        assert!(view.hides(Surrogate(7)));
        assert!(!view.hides(Surrogate(8)));
        assert!(view.doc(Surrogate(7)).is_some());
        assert!(view.holds_term("rust"));
        assert!(!view.holds_term("python"));
    }

    #[test]
    fn truncate_hides_every_indexed_document() {
        let view = StagedView::new(SurrogateBitmap::new(), true, Vec::new());
        assert!(view.hides(Surrogate(1)));
        assert!(view.hides_all());
    }
}
