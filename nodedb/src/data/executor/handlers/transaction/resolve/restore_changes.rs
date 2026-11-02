// SPDX-License-Identifier: BUSL-1.1

//! The change entries a RESTORE batch's columnar keys, vector rows and
//! timeseries rows publish.
//!
//! Each engine names its rows as its live writes name them
//! (`row_changes`):
//! - a columnar or timeseries collection as a whole, one `*` entry per kind
//!   of net change, in insert, update, delete order;
//! - a vector-primary row by its surrogate's identity.
//!
//! A row of a vector index on a document collection publishes nothing: its
//! document row publishes the change. The kind of each row is its net
//! change: whether the destination held it live before the batch, and
//! whether the install leaves it live. A key the destination keeps publishes
//! nothing. The entries ride the batch's redo record, so the live commit and
//! every later read of the record publish the same entries.

use std::collections::BTreeMap;

use crate::wal::{EVERY_ROW, RedoRowChange, RedoRowKind};

/// Which kinds of net change one collection's rows hold.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Kinds {
    insert: bool,
    update: bool,
    delete: bool,
}

/// The change entries of a RESTORE batch's keyed and grouped rows.
#[derive(Debug, Default)]
pub(super) struct RestoreChanges {
    keyed: Vec<RedoRowChange>,
    grouped: BTreeMap<String, Kinds>,
}

impl RestoreChanges {
    /// Note a row of a collection whose change events name it whole.
    pub(super) fn note_grouped(&mut self, collection: &str, kind: RedoRowKind) {
        let kinds = self.grouped.entry(collection.to_owned()).or_default();
        match kind {
            RedoRowKind::Insert => kinds.insert = true,
            RedoRowKind::Update => kinds.update = true,
            RedoRowKind::Delete => kinds.delete = true,
            RedoRowKind::NoChange => {}
        }
    }

    /// Note the row `row` of a collection whose change events name each row.
    pub(super) fn note_row(&mut self, collection: &str, row: String, kind: RedoRowKind) {
        if kind != RedoRowKind::NoChange {
            self.keyed.push(RedoRowChange {
                collection: collection.to_owned(),
                row,
                kind,
            });
        }
    }

    /// The entries: every keyed row in batch order, then each grouped
    /// collection in name order.
    pub(super) fn into_changes(self) -> Vec<RedoRowChange> {
        let mut changes = self.keyed;
        for (collection, kinds) in self.grouped {
            let present = [
                (kinds.insert, RedoRowKind::Insert),
                (kinds.update, RedoRowKind::Update),
                (kinds.delete, RedoRowKind::Delete),
            ];
            for (is_present, kind) in present {
                if is_present {
                    changes.push(RedoRowChange {
                        collection: collection.clone(),
                        row: EVERY_ROW.to_owned(),
                        kind,
                    });
                }
            }
        }
        changes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Keyed rows come first in order. A grouped collection publishes one
    /// `*` entry per kind, in insert, update, delete order. No change
    /// publishes nothing.
    #[test]
    fn entries_follow_the_live_shapes() {
        let mut changes = RestoreChanges::default();
        changes.note_grouped("metrics", RedoRowKind::Delete);
        changes.note_grouped("metrics", RedoRowKind::Insert);
        changes.note_grouped("metrics", RedoRowKind::Insert);
        changes.note_grouped("metrics", RedoRowKind::NoChange);
        changes.note_row("docs", "s1".into(), RedoRowKind::Update);
        changes.note_row("docs", "s2".into(), RedoRowKind::NoChange);
        let entries: Vec<(String, String, RedoRowKind)> = changes
            .into_changes()
            .into_iter()
            .map(|c| (c.collection, c.row, c.kind))
            .collect();
        assert_eq!(
            entries,
            vec![
                ("docs".into(), "s1".into(), RedoRowKind::Update),
                ("metrics".into(), EVERY_ROW.into(), RedoRowKind::Insert),
                ("metrics".into(), EVERY_ROW.into(), RedoRowKind::Delete),
            ]
        );
    }
}
