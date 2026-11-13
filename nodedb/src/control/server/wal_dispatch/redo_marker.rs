// SPDX-License-Identifier: BUSL-1.1

//! The WAL payload of a committed redo's `TransactionRedo` record.
//!
//! A stream marker's plan carries the row changes the Control-Plane change
//! stream publishes. The chunk records hold the same changes, and every
//! whole-WAL reader rebuilds the marker from them. So the marker's WAL record
//! holds its reference alone, and stays one small record however many rows
//! the stream changes.

use std::borrow::Cow;

use crate::wal::{RedoRecord, RedoStreamRef};

/// The bytes the WAL record of the redo `redo` holds.
pub(super) fn redo_wal_payload(redo: &[u8]) -> crate::Result<Cow<'_, [u8]>> {
    let mut record = RedoRecord::from_bytes(redo)?;
    if record.row_changes.is_empty() || RedoStreamRef::of_marker(&record)?.is_none() {
        return Ok(Cow::Borrowed(redo));
    }
    record.row_changes = Vec::new();
    Ok(Cow::Owned(record.to_bytes()?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::{RedoRowChange, RedoRowKind, RedoStreamId};

    fn reference() -> RedoStreamRef {
        RedoStreamRef {
            stream: RedoStreamId::Session {
                vshard: 1,
                idempotency_key: 2,
            },
            len: 0,
            chunks: Vec::new(),
            publish_position: None,
            entry: None,
            stamp_floor: None,
        }
    }

    #[test]
    fn a_markers_wal_record_drops_its_row_changes() {
        let change = RedoRowChange {
            collection: "orders".into(),
            row: "o1".into(),
            kind: RedoRowKind::Insert,
        };
        let marker = reference()
            .marker(None, None, vec![change])
            .expect("marker")
            .to_bytes()
            .expect("encode");
        let stored =
            RedoRecord::from_bytes(&redo_wal_payload(&marker).expect("payload")).expect("decode");
        assert!(stored.row_changes.is_empty());
        assert_eq!(
            RedoStreamRef::of_marker(&stored).expect("ref"),
            Some(reference())
        );
    }

    #[test]
    fn an_inline_redo_is_stored_verbatim() {
        let inline = RedoRecord {
            version: 1,
            ops: Vec::new(),
            calvin_stamp: None,
            cross_shard_applied: None,
            row_sources: Vec::new(),
            publishes: Vec::new(),
            row_changes: vec![RedoRowChange {
                collection: "orders".into(),
                row: "o1".into(),
                kind: RedoRowKind::Insert,
            }],
            user_roles: Vec::new(),
        }
        .to_bytes()
        .expect("encode");
        assert!(matches!(
            redo_wal_payload(&inline).expect("payload"),
            Cow::Borrowed(_)
        ));
    }
}
