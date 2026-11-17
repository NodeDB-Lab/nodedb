// SPDX-License-Identifier: Apache-2.0

//! The view of one index at a system-time cut.

use nodedb_types::SurrogateBitmap;

use crate::backend::FtsHistoryRead;
use crate::history::TextCut;
use crate::scope::IndexScope;
use crate::search::staged::{StagedDoc, StagedView};

/// The view of one index at a cut: the indexed documents a read at the cut
/// does not see, and the ended versions it sees in their place.
#[derive(Debug, Default)]
pub struct TextCutView {
    pub hidden: SurrogateBitmap,
    pub docs: Vec<StagedDoc>,
}

impl TextCutView {
    /// Whether the cut reads the index unchanged.
    pub fn is_empty(&self) -> bool {
        self.hidden.is_empty() && self.docs.is_empty()
    }

    /// The view as a staged read: the ended versions compete in the ranking
    /// from their tokens, in place of the documents it hides.
    pub fn into_staged(self) -> StagedView {
        StagedView::new(self.hidden, false, self.docs)
    }
}

/// The view of `index` at `cut`. A live document indexed after the cut is
/// hidden. An ended version the cut sees hides its document's live version
/// and enters with its tokens. Cost is the number of history rows written
/// after the cut.
pub fn cut_view<S: FtsHistoryRead + ?Sized>(
    store: &S,
    database_id: u64,
    tid: u64,
    index: IndexScope<'_>,
    cut: TextCut,
) -> Result<TextCutView, S::Error> {
    let mut view = TextCutView::default();
    if cut.first_after().is_none() {
        return Ok(view);
    }
    for surrogate in store.live_started_after(database_id, tid, index, cut)? {
        view.hidden.insert(surrogate);
    }
    for version in store.ended_seen_at(database_id, tid, index, cut)? {
        view.hidden.insert(version.surrogate);
        view.docs.push(StagedDoc {
            doc_id: version.surrogate,
            tokens: version.tokens,
        });
    }
    Ok(view)
}

#[cfg(test)]
mod tests {
    use nodedb_types::Surrogate;

    use super::*;
    use crate::backend::memory::MemoryBackend;
    use crate::history::test_rows::{end, start};

    const DB: u64 = 0;
    const T: u64 = 1;

    fn view_at(store: &MemoryBackend, at: i64) -> TextCutView {
        cut_view(store, DB, T, IndexScope::document("docs"), TextCut::at(at)).expect("cut view")
    }

    fn doc_ids(view: &TextCutView) -> Vec<u32> {
        let mut ids: Vec<u32> = view.docs.iter().map(|d| d.doc_id.as_u32()).collect();
        ids.sort_unstable();
        ids
    }

    /// Doc 1 indexed at 10, deleted at 20. A cut at 25 neither sees its
    /// version nor needs to hide it: the live index no longer holds it.
    #[test]
    fn a_doc_ended_before_the_cut_is_not_read() {
        let mut store = MemoryBackend::new();
        start(&mut store, 1, 10);
        end(&mut store, 1, 20, &["alpha"]);
        let view = view_at(&store, 25);
        assert!(view.docs.is_empty(), "the ended version is out of reach");
        assert!(!view.hidden.contains(Surrogate::new(1)));
        assert!(view.is_empty());
    }

    /// Doc 1 indexed at 10, updated at 30. A cut at 20 sees the old text in
    /// place of the live one.
    #[test]
    fn a_doc_ended_after_the_cut_is_read_from_its_tokens() {
        let mut store = MemoryBackend::new();
        start(&mut store, 1, 10);
        end(&mut store, 1, 30, &["alpha"]);
        start(&mut store, 1, 30);
        let view = view_at(&store, 20);
        assert_eq!(doc_ids(&view), vec![1]);
        assert_eq!(view.docs[0].tokens, vec!["alpha".to_string()]);
        assert!(
            view.hidden.contains(Surrogate::new(1)),
            "the live text hides"
        );
    }

    /// A document indexed after the cut is hidden and adds no text.
    #[test]
    fn a_doc_indexed_after_the_cut_is_hidden() {
        let mut store = MemoryBackend::new();
        start(&mut store, 2, 40);
        let view = view_at(&store, 20);
        assert!(view.hidden.contains(Surrogate::new(2)));
        assert!(view.docs.is_empty());
    }

    /// The version ended at the cut belongs to the write at the cut: the
    /// cut reads the successor, which the live index holds.
    #[test]
    fn a_version_ended_at_the_cut_is_not_read() {
        let mut store = MemoryBackend::new();
        start(&mut store, 1, 10);
        end(&mut store, 1, 20, &["alpha"]);
        start(&mut store, 1, 20);
        assert!(view_at(&store, 20).is_empty());
        assert_eq!(doc_ids(&view_at(&store, 19)), vec![1]);
    }

    /// Every cut across a document's versions reads exactly one of them:
    /// the versions jointly cover the document's whole history.
    #[test]
    fn the_versions_cover_every_cut_once() {
        let mut store = MemoryBackend::new();
        start(&mut store, 1, 10);
        end(&mut store, 1, 20, &["alpha"]);
        start(&mut store, 1, 20);
        end(&mut store, 1, 30, &["beta"]);
        start(&mut store, 1, 30);
        let read = |at: i64| {
            let view = view_at(&store, at);
            match view.docs.as_slice() {
                [] if view.hidden.contains(Surrogate::new(1)) => "hidden".to_string(),
                [] => "live".to_string(),
                [doc] => doc.tokens.join(" "),
                _ => "two versions".to_string(),
            }
        };
        assert_eq!(read(5), "hidden", "before the first write");
        assert_eq!(read(10), "alpha");
        assert_eq!(read(19), "alpha");
        assert_eq!(read(20), "beta");
        assert_eq!(read(29), "beta");
        assert_eq!(read(30), "live");
    }

    /// The history holds both versions of an updated document: the ended
    /// one with its interval, and the live one with its start.
    #[test]
    fn the_history_holds_every_version() {
        use crate::backend::FtsHistoryRead;

        let mut store = MemoryBackend::new();
        start(&mut store, 1, 10);
        end(&mut store, 1, 20, &["alpha"]);
        start(&mut store, 1, 20);
        let mut ended = Vec::new();
        store
            .scan_ended(&mut |row| ended.push((row.sys_from, row.sys_to)))
            .expect("scan ended");
        let mut live = Vec::new();
        store
            .scan_live(&mut |row| live.push(row.sys_from))
            .expect("scan live");
        assert_eq!(ended, vec![(10, 20)]);
        assert_eq!(live, vec![20]);
    }

    #[test]
    fn the_last_cut_reads_the_live_index() {
        let mut store = MemoryBackend::new();
        start(&mut store, 1, 10);
        assert!(view_at(&store, i64::MAX).is_empty());
    }
}
