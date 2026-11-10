// SPDX-License-Identifier: BUSL-1.1

//! Self-describing WAL payload for CRDT document-row records
//! (`CrdtOp::DocUpsert` / `DocDelete`).
//!
//! These ops mutate a top-level `LoroMap` row and live in the Data Plane. The
//! Data Plane never appends to the WAL, and the delta cannot be computed in the
//! Control Plane (it has no `LoroDoc`), so this record carries the **intent** —
//! collection, document, surrogate, fields, and (for upsert) the partial flag —
//! rather than a Loro delta. Replay re-executes the exact same live handler
//! (`execute_crdt_doc_upsert` / `_delete`) that ran when the write was first
//! accepted, exactly the "log the predicate, re-apply deterministically"
//! contract `CrdtListOpWalRecord` uses for the block-list ops.
//!
//! Deliberately NOT `RecordType::CrdtDelta`: that record type's replay contract
//! is idempotent, commutative Loro import with no LSN gate. `DocUpsert` with
//! `partial = false` prunes absent keys (a full-projection replace), so its
//! effect is order-sensitive and does not fit `CrdtDelta`'s replay path.
//!
//! Each variant carries exactly the fields its operation needs — no
//! `Option<_>` fields that a truncated/corrupt record could decode with a
//! wrong-but-valid state.

use serde::{Deserialize, Serialize};

/// WAL payload for a `RecordType::CrdtDocOp` record.
///
/// `surrogate` is stored as the wire-stable `u32` (`Surrogate::as_u32` /
/// `Surrogate::new`), matching the `CrdtListOpWalRecord` precedent of keeping
/// the record decoupled from the `Surrogate` newtype.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub(crate) enum CrdtDocOpWalRecord {
    Upsert {
        collection: String,
        document_id: String,
        surrogate: u32,
        /// The row's fields: a native object body, see
        /// `nodedb_physical::physical_plan::crdt::fields_body`.
        fields: Vec<u8>,
        /// `false` = INSERT / full replace (prune absent keys); `true` =
        /// UPDATE SET partial-merge.
        partial: bool,
    },
    Delete {
        collection: String,
        document_id: String,
        surrogate: u32,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_upsert() {
        let rec = CrdtDocOpWalRecord::Upsert {
            collection: "users".to_string(),
            document_id: "u1".to_string(),
            surrogate: 42,
            fields: nodedb_physical::physical_plan::encode_fields_body(
                &std::collections::HashMap::from([
                    ("a".to_string(), nodedb_types::Value::Integer(1)),
                    (
                        "b".to_string(),
                        nodedb_types::Value::Decimal("12.50".parse().expect("decimal literal")),
                    ),
                ]),
            )
            .expect("fields body encodes"),
            partial: false,
        };
        let bytes = zerompk::to_msgpack_vec(&rec).expect("encode");
        let decoded: CrdtDocOpWalRecord = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(decoded, rec);
    }

    #[test]
    fn round_trips_delete() {
        let rec = CrdtDocOpWalRecord::Delete {
            collection: "users".to_string(),
            document_id: "u1".to_string(),
            surrogate: 7,
        };
        let bytes = zerompk::to_msgpack_vec(&rec).expect("encode");
        let decoded: CrdtDocOpWalRecord = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(decoded, rec);
    }
}
