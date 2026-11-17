// SPDX-License-Identifier: BUSL-1.1

//! Self-describing WAL payload for CRDT list-op records
//! (`CrdtOp::ListInsert` / `ListDelete` / `ListMove`).
//!
//! These ops mutate a nested `LoroMovableList` inside a row's `LoroMap`,
//! live in the Data Plane. The Data Plane never appends to the WAL, and the
//! delta cannot be computed in the Control Plane (it has no `LoroDoc`), so
//! this record carries the **intent** — collection, document, list path,
//! operation kind, and position(s) — rather than a Loro delta. Replay
//! re-executes the exact same live handler
//! (`execute_crdt_list_insert` / `_delete` / `_move`) that ran when the
//! write was first accepted, exactly the "log the predicate, re-apply
//! deterministically" contract `ColumnarDmlWalRecord` uses for
//! `ColumnarOp::Update` / `Delete`.
//!
//! Deliberately NOT `RecordType::CrdtDelta`: that record type's replay
//! contract is idempotent, commutative Loro import with no LSN gate. List
//! ops are position-based — re-applying the same insert/delete/move twice
//! does not converge to the same state — so mixing them into `CrdtDelta`'s
//! replay path would violate that documented invariant.
//!
//! Both the writer (`control/server/wal_dispatch/core.rs`) and the reader
//! (`data/executor/wal_replay/crdt_list.rs`) live in this crate and use this
//! single enum, encoded/decoded with `zerompk`, so there is exactly one
//! unambiguous decode path.
//!
//! Each variant carries exactly the fields its operation needs — no
//! `Option<_>` position fields that a truncated/corrupt record could
//! decode with `None` and have replay silently fall back to position `0`.
//! A record missing `index` (say) simply fails to decode at all, instead
//! of replaying as "insert/delete at position 0" and silently diverging
//! replica state from what was actually written.
//!
//! Each variant carries the parent document's bound `surrogate`. The writer
//! refuses `Surrogate::ZERO`, and replay refuses a record that carries it.

use serde::{Deserialize, Serialize};

/// WAL payload for a `RecordType::CrdtListOp` record.
///
/// Tagged by variant name (zerompk's default enum tag), each variant an
/// array of exactly its own required fields in declaration order — no
/// `Option<_>` fields that could decode into a wrong-but-valid state.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub(crate) enum CrdtListOpWalRecord {
    Insert {
        collection: String,
        document_id: String,
        /// The parent document's bound surrogate, never `Surrogate::ZERO`.
        surrogate: u32,
        list_path: String,
        index: u64,
        /// The inserted block's fields: a native object body, see
        /// `nodedb_physical::physical_plan::crdt::fields_body`.
        fields: Vec<u8>,
    },
    Delete {
        collection: String,
        document_id: String,
        /// The parent document's bound surrogate, never `Surrogate::ZERO`.
        surrogate: u32,
        list_path: String,
        index: u64,
    },
    Move {
        collection: String,
        document_id: String,
        /// The parent document's bound surrogate, never `Surrogate::ZERO`.
        surrogate: u32,
        list_path: String,
        from_index: u64,
        to_index: u64,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_insert() {
        let rec = CrdtListOpWalRecord::Insert {
            collection: "notes".to_string(),
            document_id: "doc1".to_string(),
            surrogate: 7,
            list_path: "blocks".to_string(),
            index: 2,
            fields: nodedb_physical::physical_plan::crdt::encode_fields_body(
                &std::collections::HashMap::from([(
                    "type".to_string(),
                    nodedb_types::Value::String("text".into()),
                )]),
            )
            .expect("block fields encode"),
        };
        let bytes = zerompk::to_msgpack_vec(&rec).expect("encode");
        let decoded: CrdtListOpWalRecord = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(decoded, rec);
    }

    #[test]
    fn round_trips_delete() {
        let rec = CrdtListOpWalRecord::Delete {
            collection: "notes".to_string(),
            document_id: "doc1".to_string(),
            surrogate: 7,
            list_path: "blocks".to_string(),
            index: 5,
        };
        let bytes = zerompk::to_msgpack_vec(&rec).expect("encode");
        let decoded: CrdtListOpWalRecord = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(decoded, rec);
    }

    #[test]
    fn round_trips_move() {
        let rec = CrdtListOpWalRecord::Move {
            collection: "notes".to_string(),
            document_id: "doc1".to_string(),
            surrogate: 7,
            list_path: "blocks".to_string(),
            from_index: 0,
            to_index: 3,
        };
        let bytes = zerompk::to_msgpack_vec(&rec).expect("encode");
        let decoded: CrdtListOpWalRecord = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(decoded, rec);
    }

    /// Proves the fix: `from_index` and `to_index` are distinct required
    /// fields, not `Option<u64>` slots that a malformed record could
    /// decode with both missing (and replay would then silently treat as
    /// `0`). A `Move` record's two indices round-trip distinctly and can
    /// never collapse to the same value.
    #[test]
    fn move_round_trips_distinct_indices_without_collapsing_to_zero() {
        let rec = CrdtListOpWalRecord::Move {
            collection: "notes".to_string(),
            document_id: "doc1".to_string(),
            surrogate: 7,
            list_path: "blocks".to_string(),
            from_index: 3,
            to_index: 1,
        };
        let bytes = zerompk::to_msgpack_vec(&rec).expect("encode");
        let decoded: CrdtListOpWalRecord = zerompk::from_msgpack(&bytes).expect("decode");
        match decoded {
            CrdtListOpWalRecord::Move {
                from_index,
                to_index,
                ..
            } => {
                assert_eq!(from_index, 3, "from_index must survive the round trip");
                assert_eq!(to_index, 1, "to_index must survive the round trip");
                assert_ne!(
                    from_index, to_index,
                    "distinct indices must never collapse to the same value"
                );
                assert_ne!(from_index, 0, "from_index must not collapse to 0");
                assert_ne!(to_index, 0, "to_index must not collapse to 0");
            }
            other => panic!("expected Move variant, got {other:?}"),
        }
    }
}
