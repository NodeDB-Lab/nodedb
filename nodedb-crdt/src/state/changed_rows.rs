// SPDX-License-Identifier: Apache-2.0

//! Which rows of a collection changed between two versions of the document.
//!
//! A caller that keeps a derived index over rows (full-text, spatial) reads
//! the applied frontiers before an import and asks afterwards which rows
//! moved, instead of rescanning the collection.

use std::collections::BTreeSet;

use loro::event::Diff;
use loro::{ContainerID, Frontiers, Index};

use super::core::CrdtState;
use crate::error::{CrdtError, Result};

/// True when `cid` is the root map of `collection`.
fn is_collection_root(cid: &ContainerID, collection: &str) -> bool {
    matches!(cid, ContainerID::Root { name, .. } if name.as_str() == collection)
}

impl CrdtState {
    /// Frontiers of the applied state. Read before an import and pass to
    /// [`Self::changed_rows_since`] after it.
    pub fn state_frontiers(&self) -> Frontiers {
        self.doc.state_frontiers()
    }

    /// Row ids of `collection` whose applied state differs between `from`
    /// and now: rows added, removed, replaced, or with any field changed.
    ///
    /// A row map sits at `[collection root, Key(row_id), …]` on the path from
    /// the root, so a change to the row map or any container below it names
    /// the row. A change to the root map itself names the row keys it set or
    /// removed. A container with no path left was removed with its row, which
    /// the root map change already names.
    pub fn changed_rows_since(
        &self,
        collection: &str,
        from: &Frontiers,
    ) -> Result<BTreeSet<String>> {
        let to = self.doc.state_frontiers();
        let mut rows = BTreeSet::new();
        if *from == to {
            return Ok(rows);
        }
        let diff = self
            .doc
            .diff(from, &to)
            .map_err(|e| CrdtError::Loro(format!("diff of '{collection}' since import: {e}")))?;
        for (cid, change) in diff.iter() {
            if is_collection_root(cid, collection) {
                if let Diff::Map(delta) = change {
                    rows.extend(delta.updated.keys().map(|k| k.to_string()));
                }
                continue;
            }
            let Some(path) = self.doc.get_path_to_container(cid) else {
                continue;
            };
            if let (Some((root, _)), Some((_, Index::Key(row_id)))) = (path.first(), path.get(1))
                && is_collection_root(root, collection)
            {
                rows.insert(row_id.to_string());
            }
        }
        Ok(rows)
    }
}

#[cfg(test)]
mod tests {
    use loro::LoroValue;

    use super::CrdtState;

    fn put(state: &CrdtState, row: &str, text: &str) {
        state
            .upsert("docs", row, &[("body", LoroValue::from(text))])
            .expect("upsert");
    }

    #[test]
    fn an_import_reports_exactly_the_rows_it_touched() {
        let source = CrdtState::new(1).expect("source");
        put(&source, "a", "one");
        put(&source, "b", "two");
        let target = CrdtState::new(2).expect("target");
        target
            .import(&source.export_snapshot().expect("snapshot"))
            .expect("first import");

        let before_vv = source.doc.oplog_vv();
        put(&source, "b", "three");
        source.delete("docs", "a").expect("delete");
        let delta = source
            .doc
            .export(loro::ExportMode::updates(&before_vv))
            .expect("delta");

        let from = target.state_frontiers();
        target.import(&delta).expect("second import");
        let changed = target.changed_rows_since("docs", &from).expect("diff");
        assert_eq!(changed.into_iter().collect::<Vec<_>>(), vec!["a", "b"]);
    }

    #[test]
    fn no_change_reports_no_rows() {
        let state = CrdtState::new(1).expect("state");
        put(&state, "a", "one");
        let from = state.state_frontiers();
        assert!(
            state
                .changed_rows_since("docs", &from)
                .expect("diff")
                .is_empty()
        );
    }
}
