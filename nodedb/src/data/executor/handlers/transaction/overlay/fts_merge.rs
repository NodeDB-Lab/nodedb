// SPDX-License-Identifier: BUSL-1.1

//! An open transaction's staged writes as one inverted index sees them.
//!
//! FTS indexing is an inline side effect of the document write, not a
//! stageable write of its own, so there is no staged posting to read. A
//! full-text read inside the transaction builds a [`StagedView`] instead:
//! every row the transaction staged hides its indexed version, a staged
//! TRUNCATE hides every indexed row, and each staged body that holds text in
//! the index enters the read with its analyzed tokens. The BM25 search, the
//! score columns, and the phrase search all read the transaction's writes
//! through this one view, before their top-k cut.

use nodedb_fts::{IndexScope, StagedDoc, StagedView};
use nodedb_types::{Surrogate, SurrogateBitmap};

use super::fts_score::earliest_contiguous_match;
use super::staged::Staged;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::TenantId;

impl CoreLoop {
    /// The issuing transaction's view of `index`. `None` outside a
    /// transaction, or when the transaction staged nothing for the
    /// collection.
    pub(in crate::data::executor) fn text_staged_view(
        &self,
        task: &ExecutionTask,
        tid: u64,
        index: IndexScope<'_>,
    ) -> crate::Result<Option<StagedView>> {
        let Some(txn_id) = task.request.txn_id else {
            return Ok(None);
        };
        // Read-your-own-writes refreshes the lease (see the reaper).
        self.touch_overlay(txn_id);
        let Some(overlay) = self.txn_overlays.get(&txn_id) else {
            return Ok(None);
        };
        let database_id = task.request.database_id;
        let coll_key = (
            database_id,
            TenantId::new(tid),
            index.collection().to_string(),
        );
        let hide_all = overlay.is_truncated(&coll_key);
        let mut hidden = SurrogateBitmap::new();
        let mut docs = Vec::new();
        for (surrogate, staged) in overlay.iter_for_collection(&coll_key) {
            let doc_id = Surrogate::new(surrogate);
            hidden.insert(doc_id);
            if let Staged::Put(body) = staged
                && let Some(tokens) =
                    self.tokenize_staged_body(database_id.as_u64(), &coll_key, index, body)?
            {
                docs.push(StagedDoc { doc_id, tokens });
            }
        }
        if !hide_all && hidden.is_empty() {
            return Ok(None);
        }
        Ok(Some(StagedView::new(hidden, hide_all, docs)))
    }

    /// Each phrase term analyzed with the collection's analyzer, the same
    /// canonicalization the indexed phrase search applies. A term the
    /// analyzer drops (a stop word) is matched as written.
    pub(in crate::data::executor) fn canonical_phrase_terms(
        &self,
        database_id: u64,
        tid: TenantId,
        collection: &str,
        terms: &[String],
    ) -> crate::Result<Vec<String>> {
        let mut canonical = Vec::with_capacity(terms.len());
        for term in terms {
            let tokens = self
                .inverted
                .analyze_for_collection(database_id, tid, collection, term)?;
            canonical.push(tokens.into_iter().next().unwrap_or_else(|| term.clone()));
        }
        Ok(canonical)
    }
}

/// The staged documents `eligible` admits that hold `phrase` as a
/// contiguous, in-order run of their tokens (zero slop, the adjacency the
/// indexed phrase search enforces). Each scores `1 / (1 + earliest_start)`,
/// the indexed phrase formula at rank 0.
pub(in crate::data::executor) fn staged_phrase_hits(
    staged: &StagedView,
    phrase: &[String],
    eligible: Option<&SurrogateBitmap>,
) -> Vec<(Surrogate, f32, bool)> {
    staged
        .docs()
        .iter()
        .filter(|doc| eligible.is_none_or(|bm| bm.contains(doc.doc_id)))
        .filter_map(|doc| {
            earliest_contiguous_match(&doc.tokens, phrase)
                .map(|start| (doc.doc_id, 1.0 / (1.0 + start as f32), false))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn staged_phrase_hits_respect_adjacency_and_eligibility() {
        let view = StagedView::new(
            SurrogateBitmap::new(),
            false,
            vec![
                StagedDoc {
                    doc_id: Surrogate::new(1),
                    tokens: vec!["quick".into(), "brown".into(), "fox".into()],
                },
                StagedDoc {
                    doc_id: Surrogate::new(2),
                    tokens: vec!["brown".into(), "quick".into(), "fox".into()],
                },
            ],
        );
        let phrase = vec!["brown".to_string(), "fox".to_string()];
        let hits = staged_phrase_hits(&view, &phrase, None);
        assert_eq!(hits, vec![(Surrogate::new(1), 0.5, false)]);

        let mut eligible = SurrogateBitmap::new();
        eligible.insert(Surrogate::new(2));
        assert!(staged_phrase_hits(&view, &phrase, Some(&eligible)).is_empty());
    }
}
