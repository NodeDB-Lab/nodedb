// SPDX-License-Identifier: Apache-2.0

//! Imports that report which rows of one collection they changed.
//!
//! A caller that keeps a derived index over rows (full-text, spatial) needs
//! the rows an import moved, without a rescan of the collection.
//!
//! The rows come from the operations the import added to the oplog, read the
//! same way as an import's write-set (see [`super::write_set`]). No
//! subscription, second diff, or checkout runs, so a shallow document works
//! like any other and later local writes pay nothing for the tracking.

use std::collections::BTreeSet;

use super::core::CrdtState;
use super::import_admission::ImportAdmission;
use super::write_set::collect_imported_rows;
use crate::error::Result;

/// The outcome of a tracked import and the rows it changed.
///
/// `changed_rows` is filled for every outcome. A blob can apply its ready
/// changes and still return `ImportPendingDependencies`. Derived indexes
/// must follow the rows that did apply.
#[derive(Debug)]
#[must_use]
pub struct TrackedImport {
    /// The import result, as the untracked import returns it.
    pub outcome: Result<ImportAdmission>,
    /// Row ids of the collection an applied operation of the import wrote:
    /// rows added, removed, replaced, or changed in any nested container.
    pub changed_rows: BTreeSet<String>,
}

impl CrdtState {
    /// [`Self::import`] that also reports the rows of `collection` it changed.
    pub fn import_tracked(&self, collection: &str, data: &[u8]) -> TrackedImport {
        self.track_changed_rows(collection, || self.import(data))
    }

    /// [`Self::import_local`] that also reports the rows of `collection` it changed.
    pub fn import_local_tracked(&self, collection: &str, data: &[u8]) -> TrackedImport {
        self.track_changed_rows(collection, || self.import_local(data))
    }

    /// Run `import` and collect the rows of `collection` it wrote.
    fn track_changed_rows(
        &self,
        collection: &str,
        import: impl FnOnce() -> Result<ImportAdmission>,
    ) -> TrackedImport {
        // Shape faults are the write-set's concern. A derived index follows
        // every row the import moved, whatever shape the delta wrote.
        let (outcome, imported) = collect_imported_rows(&self.doc, Some(collection), import);
        TrackedImport {
            outcome,
            changed_rows: imported
                .rows
                .into_iter()
                .map(|(_, row_id)| row_id)
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use loro::{ExportMode, IdSpan, LoroValue};

    use super::CrdtState;
    use crate::error::CrdtError;

    fn put(state: &CrdtState, row: &str, text: &str) {
        state
            .upsert("docs", row, &[("body", LoroValue::from(text))])
            .expect("upsert");
    }

    fn rows(tracked: &super::TrackedImport) -> Vec<&str> {
        tracked.changed_rows.iter().map(String::as_str).collect()
    }

    /// A source with rows `a`, `b`, `c` and a target holding the same state.
    fn synced_pair() -> (CrdtState, CrdtState) {
        let source = CrdtState::new(1).expect("source");
        put(&source, "a", "one");
        put(&source, "b", "two");
        put(&source, "c", "three");
        let target = CrdtState::new(2).expect("target");
        target
            .import(&source.export_snapshot().expect("snapshot"))
            .expect("seed import");
        (source, target)
    }

    #[test]
    fn a_first_import_reports_every_row() {
        let source = CrdtState::new(1).expect("source");
        put(&source, "a", "one");
        put(&source, "b", "two");
        let target = CrdtState::new(2).expect("target");

        let tracked = target.import_tracked("docs", &source.export_snapshot().expect("snapshot"));
        tracked.outcome.as_ref().expect("import");
        assert_eq!(rows(&tracked), vec!["a", "b"]);
    }

    #[test]
    fn a_delta_reports_exactly_the_rows_it_touched() {
        let (source, target) = synced_pair();
        let before = source.oplog_version_vector();
        put(&source, "b", "changed");
        source.delete("docs", "a").expect("delete");
        let delta = source.export_updates_since(&before).expect("delta");

        let tracked = target.import_tracked("docs", &delta);
        tracked.outcome.as_ref().expect("import");
        assert_eq!(rows(&tracked), vec!["a", "b"]);
        assert!(!target.row_exists("docs", "a"));
    }

    #[test]
    fn a_shallow_snapshot_and_a_following_delta_report_their_rows() {
        let mut source = CrdtState::new(1).expect("source");
        put(&source, "a", "one");
        put(&source, "b", "two");
        put(&source, "c", "three");
        source.compact_history().expect("compact");
        let target = CrdtState::new(2).expect("target");

        let tracked =
            target.import_local_tracked("docs", &source.export_snapshot().expect("snapshot"));
        tracked.outcome.as_ref().expect("shallow import");
        assert_eq!(rows(&tracked), vec!["a", "b", "c"]);
        assert!(target.doc.is_shallow());

        let before = source.oplog_version_vector();
        put(&source, "d", "four");
        source.delete("docs", "a").expect("delete");
        let delta = source.export_updates_since(&before).expect("delta");

        let tracked = target.import_tracked("docs", &delta);
        tracked.outcome.as_ref().expect("delta into shallow doc");
        assert_eq!(rows(&tracked), vec!["a", "d"]);
    }

    #[test]
    fn a_shallow_snapshot_into_a_synced_doc_reports_only_rows_it_changed() {
        let (mut source, target) = synced_pair();
        source.compact_history().expect("compact");
        put(&source, "b", "changed");
        let snapshot = source.export_snapshot().expect("snapshot");
        assert!(source.doc.is_shallow());

        let tracked = target.import_tracked("docs", &snapshot);
        tracked.outcome.as_ref().expect("import");
        assert_eq!(rows(&tracked), vec!["b"]);
        assert_eq!(
            target.read_field("docs", "b", "body"),
            Some(LoroValue::from("changed"))
        );
    }

    #[test]
    fn a_nested_container_change_names_its_row() {
        let (source, target) = synced_pair();
        let before = source.oplog_version_vector();
        source
            .list_insert_fields(
                "docs",
                "a",
                "blocks",
                0,
                &[("text".to_owned(), LoroValue::from("block"))],
            )
            .expect("list insert");
        let delta = source.export_updates_since(&before).expect("delta");

        let tracked = target.import_tracked("docs", &delta);
        tracked.outcome.as_ref().expect("import");
        assert_eq!(rows(&tracked), vec!["a"]);
    }

    #[test]
    fn a_row_deleted_and_recreated_in_one_delta_is_reported() {
        let (source, target) = synced_pair();
        let before = source.oplog_version_vector();
        source.delete("docs", "a").expect("delete");
        put(&source, "a", "reborn");
        let delta = source.export_updates_since(&before).expect("delta");

        let tracked = target.import_tracked("docs", &delta);
        tracked.outcome.as_ref().expect("import");
        assert_eq!(rows(&tracked), vec!["a"]);
        assert_eq!(
            target.read_field("docs", "a", "body"),
            Some(LoroValue::from("reborn"))
        );
    }

    #[test]
    fn rows_of_another_collection_are_not_reported() {
        let source = CrdtState::new(1).expect("source");
        put(&source, "a", "one");
        source
            .upsert("other", "x", &[("body", LoroValue::from("elsewhere"))])
            .expect("other upsert");
        let target = CrdtState::new(2).expect("target");

        let tracked = target.import_tracked("docs", &source.export_snapshot().expect("snapshot"));
        tracked.outcome.as_ref().expect("first import");
        assert_eq!(rows(&tracked), vec!["a"]);

        let before = source.oplog_version_vector();
        source
            .upsert("other", "y", &[("body", LoroValue::from("elsewhere"))])
            .expect("other upsert");
        put(&source, "b", "two");
        let delta = source.export_updates_since(&before).expect("delta");

        let tracked = target.import_tracked("docs", &delta);
        tracked.outcome.as_ref().expect("delta");
        assert_eq!(rows(&tracked), vec!["b"]);
    }

    #[test]
    fn an_uncommitted_local_write_is_not_reported_as_imported() {
        let (source, target) = synced_pair();
        put(&target, "local", "mine");
        assert_ne!(target.doc.get_pending_txn_len(), 0);
        let before = source.oplog_version_vector();
        put(&source, "c", "changed");
        let delta = source.export_updates_since(&before).expect("delta");

        let tracked = target.import_tracked("docs", &delta);
        tracked.outcome.as_ref().expect("import");
        assert_eq!(rows(&tracked), vec!["c"]);
        assert!(target.row_exists("docs", "local"));
    }

    #[test]
    fn an_uncommitted_local_write_on_an_empty_doc_is_not_reported() {
        let source = CrdtState::new(1).expect("source");
        put(&source, "a", "one");
        let target = CrdtState::new(2).expect("target");
        put(&target, "local", "mine");

        let tracked = target.import_tracked("docs", &source.export_snapshot().expect("snapshot"));
        tracked.outcome.as_ref().expect("import");
        assert_eq!(rows(&tracked), vec!["a"]);
    }

    #[test]
    fn a_replayed_delta_reports_no_rows() {
        let (source, target) = synced_pair();
        let before = source.oplog_version_vector();
        put(&source, "b", "changed");
        let delta = source.export_updates_since(&before).expect("delta");
        let first = target.import_tracked("docs", &delta);
        first.outcome.as_ref().expect("first import");

        let replay = target.import_tracked("docs", &delta);
        replay.outcome.as_ref().expect("replay");
        assert!(replay.changed_rows.is_empty());
    }

    #[test]
    fn a_tracked_import_leaves_later_local_writes_unchanged() {
        let (source, tracked) = synced_pair();
        let plain = CrdtState::new(2).expect("plain target");
        plain
            .import(&source.export_snapshot().expect("snapshot"))
            .expect("seed import");
        let before = source.oplog_version_vector();
        put(&source, "b", "changed");
        let delta = source.export_updates_since(&before).expect("delta");

        let imported = tracked.import_tracked("docs", &delta);
        imported.outcome.as_ref().expect("tracked import");
        assert_eq!(rows(&imported), vec!["b"]);
        plain.import(&delta).expect("plain import");

        let local_write = |state: &CrdtState| {
            let before = state.oplog_version_vector();
            put(state, "local", "mine");
            state.export_updates_since(&before).expect("local delta")
        };
        assert_eq!(local_write(&tracked), local_write(&plain));
    }

    #[test]
    fn a_partially_pending_import_reports_the_rows_that_applied() {
        let peer1 = CrdtState::new(1).expect("peer 1");
        put(&peer1, "x", "ready");
        let peer3 = CrdtState::new(3).expect("peer 3");
        put(&peer3, "y", "withheld");
        let withheld = peer3.export_snapshot().expect("peer 3 snapshot");

        // The hub's own write to `z` depends on peer 3's history.
        let hub = CrdtState::new(2).expect("hub");
        hub.import(&peer1.export_snapshot().expect("peer 1 snapshot"))
            .expect("hub imports peer 1");
        hub.import(&withheld).expect("hub imports peer 3");
        put(&hub, "z", "dependent");
        hub.doc.commit();
        let vv = hub.oplog_version_vector();
        let end = |peer: u64| vv.get(&peer).copied().unwrap_or(0);
        let blob = hub
            .doc
            .export(ExportMode::updates_in_range(vec![
                IdSpan::new(1, 0, end(1)),
                IdSpan::new(2, 0, end(2)),
            ]))
            .expect("range export");

        let target = CrdtState::new(4).expect("target");
        let tracked = target.import_tracked("docs", &blob);
        assert!(matches!(
            tracked.outcome,
            Err(CrdtError::ImportPendingDependencies)
        ));
        assert_eq!(rows(&tracked), vec!["x"]);
        assert!(target.row_exists("docs", "x"));

        let tracked = target.import_tracked("docs", &withheld);
        tracked.outcome.as_ref().expect("withheld import");
        assert_eq!(rows(&tracked), vec!["y", "z"]);
        assert!(target.row_exists("docs", "z"));
    }
}
