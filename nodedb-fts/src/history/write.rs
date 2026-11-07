// SPDX-License-Identifier: Apache-2.0

//! The history bookkeeping of one text write and of its undo.
//!
//! A write indexes a document's text at its system-time ordinal. An update
//! or a delete ends the indexed version at the write's ordinal: the
//! version's analyzed tokens move to the history, so a read at an earlier
//! cut still matches it. The write that indexes the new text records its
//! start. A write at [`UNVERSIONED`] keeps no history.

use crate::backend::FtsHistoryWrite;
use crate::history::{EndedVersion, HistoryDoc, UNVERSIONED};

/// End the indexed version of `doc` at `ordinal`. Runs before the write
/// strips the version's postings. `tokens` reads the version's analyzed
/// tokens, and runs only when a version ends. A re-apply of the write that
/// indexed the version (a WAL replay) ends nothing.
pub fn end_version<S, F>(
    store: &mut S,
    doc: HistoryDoc<'_>,
    ordinal: i64,
    tokens: F,
) -> Result<(), S::Error>
where
    S: FtsHistoryWrite + ?Sized,
    F: FnOnce() -> Result<Vec<String>, S::Error>,
{
    if ordinal == UNVERSIONED {
        return Ok(());
    }
    let HistoryDoc {
        database_id,
        tid,
        index,
        surrogate,
    } = doc;
    let live_from = store.live_from(database_id, tid, index, surrogate)?;
    if live_from == Some(ordinal) {
        return Ok(());
    }
    let tokens = tokens()?;
    store.clear_live_from(database_id, tid, index, surrogate)?;
    store.put_ended(
        database_id,
        tid,
        index,
        &EndedVersion {
            surrogate,
            sys_from: live_from.unwrap_or(UNVERSIONED),
            sys_to: ordinal,
            tokens,
        },
    )
}

/// Record the version of `doc` a write indexed as live from `ordinal`.
pub fn start_version<S: FtsHistoryWrite + ?Sized>(
    store: &mut S,
    doc: HistoryDoc<'_>,
    ordinal: i64,
) -> Result<(), S::Error> {
    if ordinal == UNVERSIONED {
        return Ok(());
    }
    store.set_live_from(doc.database_id, doc.tid, doc.index, doc.surrogate, ordinal)
}

/// The history side of undoing the write of `doc` at one ordinal.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct UndoStep {
    /// Whether the undone write started the live version. The caller strips
    /// that version's postings.
    pub started_here: bool,
    /// The version the undone write ended. The caller indexes its tokens
    /// again, live from its `sys_from`.
    pub restored: Option<EndedVersion>,
}

/// Reverse the history rows the write of `doc` at `ordinal` made: the start
/// it recorded leaves, and the version it ended leaves the history. The
/// caller applies the returned step to the index.
pub fn undo_at<S: FtsHistoryWrite + ?Sized>(
    store: &mut S,
    doc: HistoryDoc<'_>,
    ordinal: i64,
) -> Result<UndoStep, S::Error> {
    let HistoryDoc {
        database_id,
        tid,
        index,
        surrogate,
    } = doc;
    let started_here = store.live_from(database_id, tid, index, surrogate)? == Some(ordinal);
    if started_here {
        store.clear_live_from(database_id, tid, index, surrogate)?;
    }
    let restored = store.take_ended_at(database_id, tid, index, surrogate, ordinal)?;
    Ok(UndoStep {
        started_here,
        restored,
    })
}

#[cfg(test)]
mod tests {
    use nodedb_types::Surrogate;

    use super::*;
    use crate::backend::FtsHistoryRead;
    use crate::backend::memory::MemoryBackend;
    use crate::history::test_rows::{doc, end, start};
    use crate::scope::IndexScope;

    fn ended_count(store: &MemoryBackend) -> usize {
        let mut count = 0;
        store.scan_ended(&mut |_| count += 1).expect("scan");
        count
    }

    #[test]
    fn a_replayed_write_ends_nothing() {
        let mut store = MemoryBackend::new();
        start(&mut store, 1, 10);
        let replay = end_version(&mut store, doc(1), 10, || panic!("a replay reads no tokens"));
        replay.expect("replay");
        assert_eq!(ended_count(&store), 0);
        let live = store
            .live_from(0, 1, IndexScope::document("docs"), Surrogate::new(1))
            .expect("live");
        assert_eq!(live, Some(10));
    }

    #[test]
    fn an_unversioned_write_keeps_no_history() {
        let mut store = MemoryBackend::new();
        start_version(&mut store, doc(1), UNVERSIONED).expect("start");
        end_version(&mut store, doc(1), UNVERSIONED, || Ok(vec!["alpha".into()])).expect("end");
        assert_eq!(ended_count(&store), 0);
        let live = store
            .live_from(0, 1, IndexScope::document("docs"), Surrogate::new(1))
            .expect("live");
        assert_eq!(live, None);
    }

    #[test]
    fn a_version_without_a_start_ends_from_unversioned() {
        let mut store = MemoryBackend::new();
        end(&mut store, 1, 20, &["alpha"]);
        let mut rows = Vec::new();
        store.scan_ended(&mut |row| rows.push(row.to_key())).expect("scan");
        assert_eq!(rows.len(), 1);
        assert_eq!((rows[0].sys_from, rows[0].sys_to), (UNVERSIONED, 20));
    }

    #[test]
    fn an_undo_restores_the_ended_version() {
        let mut store = MemoryBackend::new();
        start(&mut store, 1, 10);
        end(&mut store, 1, 20, &["alpha"]);
        start(&mut store, 1, 20);
        let step = undo_at(&mut store, doc(1), 20).expect("undo");
        assert!(step.started_here);
        let restored = step.restored.expect("the version ended at 20");
        assert_eq!((restored.sys_from, restored.sys_to), (10, 20));
        assert_eq!(restored.tokens, vec!["alpha".to_string()]);
        assert_eq!(ended_count(&store), 0);
        assert_eq!(undo_at(&mut store, doc(1), 20).expect("again"), UndoStep::default());
    }
}
