// SPDX-License-Identifier: BUSL-1.1

//! The change events of a committed transaction: the net change of every row
//! its redo record writes.
//!
//! Every producer of a committed redo record names each row it changes in the
//! record's `row_changes` (see [`crate::wal::redo::row_changes`]): the
//! transaction resolve for every engine, and the restore and clone re-issues
//! for the rows they install. The events come from those entries alone, so an
//! update publishes `Update`, several writes to one row publish once with
//! their net kind, and no kind is inferred from a sub-op's record type.

use nodedb_types::RowIdentity;

use crate::control::change_stream::ChangeOperation;
use crate::wal::{EVERY_ROW, RedoRowChange, RedoRowKind};

use super::extract::{WriteChangeMeta, every_row};

/// One change per row the `TransactionRedo` payload `redo` names. A payload
/// that does not decode yields none: the Data Plane refuses it, so it
/// installs nothing.
pub(super) fn redo_change_meta(redo: &[u8]) -> Vec<WriteChangeMeta> {
    let Ok(record) = crate::wal::RedoRecord::from_bytes(redo) else {
        return Vec::new();
    };
    record
        .row_changes
        .into_iter()
        .filter_map(named_row_change)
        .collect()
}

/// The change a named row publishes. A row with no net change publishes
/// nothing. A chunked redo's row changes publish through it page by page.
pub(super) fn named_row_change(change: RedoRowChange) -> Option<WriteChangeMeta> {
    let operation = match change.kind {
        RedoRowKind::Insert => ChangeOperation::Insert,
        RedoRowKind::Update => ChangeOperation::Update,
        RedoRowKind::Delete => ChangeOperation::Delete,
        RedoRowKind::NoChange => return None,
    };
    let identity = if change.row == EVERY_ROW {
        every_row()
    } else {
        RowIdentity::from_user_key(change.row)
    };
    Some((change.collection, identity, operation))
}

#[cfg(test)]
mod tests {
    use nodedb_types::sync::wire::SyncProvenance;
    use nodedb_wal::record::RecordType;

    use super::*;
    use crate::wal::{RedoRecord, RedoSubRecord};

    fn named(collection: &str, row: &str, kind: RedoRowKind) -> RedoRowChange {
        RedoRowChange {
            collection: collection.to_owned(),
            row: row.to_owned(),
            kind,
        }
    }

    fn record(ops: Vec<RedoSubRecord>, row_changes: Vec<RedoRowChange>) -> Vec<u8> {
        RedoRecord {
            version: 1,
            ops,
            calvin_stamp: None,
            cross_shard_applied: None,
            row_sources: Vec::new(),
            publishes: Vec::new(),
            row_changes,
            user_roles: Vec::new(),
            sum_targets: Vec::new(),
            pins: Default::default(),
        }
        .to_bytes()
        .expect("encode redo")
    }

    fn doc_put(id: &str) -> RedoSubRecord {
        RedoSubRecord {
            record_type: RecordType::Put as u32,
            payload: zerompk::to_msgpack_vec(&(
                "orders",
                id,
                vec![0x80u8],
                None::<SyncProvenance>,
                7u32,
            ))
            .expect("encode"),
        }
    }

    #[test]
    fn named_rows_publish_their_net_kind_once() {
        // The redo holds final images: `o1` inserted then updated, `o2`
        // updated, `o3` updated then deleted, `o4` inserted then deleted.
        let changes = vec![
            named("orders", "o1", RedoRowKind::Insert),
            named("orders", "o2", RedoRowKind::Update),
            named("orders", "o3", RedoRowKind::Delete),
            named("orders", "o4", RedoRowKind::NoChange),
        ];
        let meta = redo_change_meta(&record(vec![doc_put("o1"), doc_put("o2")], changes));
        assert_eq!(
            meta,
            vec![
                (
                    "orders".to_owned(),
                    RowIdentity::from_user_key("o1"),
                    ChangeOperation::Insert
                ),
                (
                    "orders".to_owned(),
                    RowIdentity::from_user_key("o2"),
                    ChangeOperation::Update
                ),
                (
                    "orders".to_owned(),
                    RowIdentity::from_user_key("o3"),
                    ChangeOperation::Delete
                ),
            ]
        );
    }

    #[test]
    fn a_whole_collection_change_names_every_row() {
        let changes = vec![
            named("metrics", EVERY_ROW, RedoRowKind::Delete),
            named("metrics", EVERY_ROW, RedoRowKind::Insert),
        ];
        let meta = redo_change_meta(&record(Vec::new(), changes));
        assert_eq!(
            meta,
            vec![
                ("metrics".to_owned(), every_row(), ChangeOperation::Delete),
                ("metrics".to_owned(), every_row(), ChangeOperation::Insert),
            ]
        );
    }

    #[test]
    fn a_sub_op_no_entry_names_publishes_nothing() {
        // A put the record does not name is never read as an insert.
        let meta = redo_change_meta(&record(vec![doc_put("o1")], Vec::new()));
        assert!(meta.is_empty());
    }

    #[test]
    fn a_malformed_redo_publishes_nothing() {
        assert!(redo_change_meta(b"not a redo").is_empty());
    }
}
