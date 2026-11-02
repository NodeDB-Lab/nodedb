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
//!
//! A read at a system-time cut reads the index through the same view: the
//! documents indexed after the cut are hidden, and the text versions visible
//! at the cut enter with their tokens. A transaction snapshot merges both:
//! the transaction's own writes win over the cut's versions.

use nodedb_fts::{IndexScope, StagedDoc, StagedView};
use nodedb_types::{Surrogate, SurrogateBitmap};

use super::fts_score::earliest_contiguous_match;
use super::staged::Staged;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::TenantId;

impl CoreLoop {
    /// The view of `index` a read at `cut` takes: the issuing transaction's
    /// staged writes when the cut merges them, on top of the index at the
    /// cut's ordinal when it names one. `None` when the read sees the
    /// current index unchanged.
    pub(in crate::data::executor) fn text_staged_view(
        &self,
        task: &ExecutionTask,
        tid: u64,
        index: IndexScope<'_>,
        cut: crate::data::executor::handlers::search_cut::SearchCut,
    ) -> crate::Result<Option<StagedView>> {
        let staged = if cut.merge_overlay {
            self.txn_text_view(task, tid, index)?
        } else {
            None
        };
        let Some(at) = cut.at else {
            return Ok(staged);
        };
        let at_cut = self.inverted.text_cut_view(
            task.request.database_id.as_u64(),
            TenantId::new(tid),
            index,
            at,
        )?;
        Ok(merge_cut_view(staged, at_cut))
    }

    /// The issuing transaction's view of `index`. `None` outside a
    /// transaction, or when the transaction staged nothing for the
    /// collection.
    fn txn_text_view(
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

    /// The phrase `words` as the collection's analyzer tokenizes it: the
    /// token sequence its indexed and staged text carry. A word the analyzer
    /// drops (a stop word) has no token, as in the indexed text. Both the
    /// indexed and the staged phrase match read this one sequence.
    pub(in crate::data::executor) fn analyze_phrase(
        &self,
        database_id: u64,
        tid: TenantId,
        collection: &str,
        words: &[String],
    ) -> crate::Result<Vec<String>> {
        self.inverted
            .analyze_for_collection(database_id, tid, collection, &words.join(" "))
    }
}

/// The view of a read at a cut, with the transaction's `staged` writes on
/// top when it has some. A staged write of a row hides that row's version at
/// the cut, and a staged TRUNCATE hides every version at the cut.
fn merge_cut_view(
    staged: Option<StagedView>,
    at_cut: crate::engine::sparse::inverted::TextCutView,
) -> Option<StagedView> {
    let Some(staged) = staged else {
        if at_cut.hidden.is_empty() && at_cut.docs.is_empty() {
            return None;
        }
        return Some(StagedView::new(at_cut.hidden, false, at_cut.docs));
    };
    if staged.hides_all() {
        return Some(staged);
    }
    let mut hidden = at_cut.hidden;
    hidden.union_in_place(staged.hidden());
    let mut docs: Vec<StagedDoc> = staged.docs().to_vec();
    docs.extend(
        at_cut
            .docs
            .into_iter()
            .filter(|doc| !staged.hides(doc.doc_id)),
    );
    Some(StagedView::new(hidden, false, docs))
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

    /// A read at a cut sees the versions there. A transaction snapshot's own
    /// staged write of a row wins over the row's version at the cut.
    #[test]
    fn a_cut_view_merges_under_the_staged_writes() {
        let doc = |id: u32, word: &str| StagedDoc {
            doc_id: Surrogate::new(id),
            tokens: vec![word.to_string()],
        };
        let mut cut_hidden = SurrogateBitmap::new();
        cut_hidden.insert(Surrogate::new(3));
        let at_cut = crate::engine::sparse::inverted::TextCutView {
            hidden: cut_hidden.clone(),
            docs: vec![doc(1, "old"), doc(2, "kept")],
        };
        let view = merge_cut_view(None, at_cut).expect("a cut view");
        assert!(view.hides(Surrogate::new(3)), "indexed after the cut");
        assert_eq!(view.docs().len(), 2);

        let staged = StagedView::new(SurrogateBitmap::new(), false, vec![doc(1, "mine")]);
        let at_cut = crate::engine::sparse::inverted::TextCutView {
            hidden: cut_hidden,
            docs: vec![doc(1, "old"), doc(2, "kept")],
        };
        let merged = merge_cut_view(Some(staged), at_cut).expect("merged");
        let tokens: Vec<&str> = merged.docs().iter().map(|d| d.tokens[0].as_str()).collect();
        assert_eq!(tokens, vec!["mine", "kept"], "the staged write wins");
        assert!(merged.hides(Surrogate::new(3)));
    }

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
