// SPDX-License-Identifier: Apache-2.0

//! Columnar resolved-row-set DML WAL record payload.
//!
//! `ColumnarOp::ResolvedUpdate` / `ColumnarOp::ResolvedDelete` carry rows the
//! Control Plane already resolved from a predicate and decided against a
//! write policy, so the durable record persists the concrete row images
//! themselves rather than a predicate to re-evaluate — the same reason the
//! Raft replication path (`ColumnarBulkDmlResolved` in
//! `control::wal_replication`) ships resolved rows instead of a predicate:
//! there is no writing identity present at replay to decide a predicate
//! against.
//!
//! Rides `RecordType::TimeseriesBatch`, disambiguated from
//! [`super::ColumnarWalRecord`] and [`super::ColumnarDmlWalRecord`] by
//! `kind = "columnar_resolved_dml"`.

use serde::{Deserialize, Serialize};

/// One row inside a [`ColumnarResolvedDmlWalRecord`].
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct ColumnarResolvedDmlWalRow {
    /// MessagePack-encoded primary key value.
    pub pk_msgpack: Vec<u8>,
    /// MessagePack-encoded full post-image row (`Value::Array` of column
    /// values). Empty for a delete row.
    pub new_row_msgpack: Vec<u8>,
}

/// Map-encoded columnar resolved-row-set DML WAL record.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct ColumnarResolvedDmlWalRecord {
    /// Record kind tag — always `"columnar_resolved_dml"`.
    pub kind: String,
    /// Target collection name.
    pub collection: String,
    /// `true` for `ColumnarOp::ResolvedUpdate`, `false` for
    /// `ColumnarOp::ResolvedDelete`.
    pub is_update: bool,
    /// Every row the Control Plane resolved and the write policy admitted.
    pub rows: Vec<ColumnarResolvedDmlWalRow>,
    /// System-time ordinal every new version an update writes stamps, or at
    /// which a delete closes each removed key on a bitemporal collection,
    /// raised above the key's bound row at apply. Replay stamps what the
    /// live write stamped. `None` for a writer that leaves the stamp to the
    /// apply.
    #[serde(default)]
    #[msgpack(default)]
    pub system_from: Option<i64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::columnar::{ColumnarDmlWalRecord, ColumnarWalRecord};

    #[test]
    fn round_trips_resolved_update() {
        let rec = ColumnarResolvedDmlWalRecord {
            kind: "columnar_resolved_dml".to_string(),
            collection: "events".to_string(),
            is_update: true,
            rows: vec![ColumnarResolvedDmlWalRow {
                pk_msgpack: vec![1, 2, 3],
                new_row_msgpack: vec![4, 5, 6],
            }],
            system_from: None,
        };
        let bytes = zerompk::to_msgpack_vec(&rec).expect("encode");
        let decoded: ColumnarResolvedDmlWalRecord = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(decoded, rec);
    }

    #[test]
    fn round_trips_resolved_delete_with_empty_row() {
        let rec = ColumnarResolvedDmlWalRecord {
            kind: "columnar_resolved_dml".to_string(),
            collection: "events".to_string(),
            is_update: false,
            rows: vec![ColumnarResolvedDmlWalRow {
                pk_msgpack: vec![7],
                new_row_msgpack: Vec::new(),
            }],
            system_from: None,
        };
        let bytes = zerompk::to_msgpack_vec(&rec).expect("encode");
        let decoded: ColumnarResolvedDmlWalRecord = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(decoded, rec);
    }

    #[test]
    fn does_not_collide_with_predicate_dml_or_row_payload_shapes() {
        let rec = ColumnarResolvedDmlWalRecord {
            kind: "columnar_resolved_dml".to_string(),
            collection: "events".to_string(),
            is_update: false,
            rows: vec![ColumnarResolvedDmlWalRow {
                pk_msgpack: vec![1],
                new_row_msgpack: Vec::new(),
            }],
            system_from: None,
        };
        let bytes = zerompk::to_msgpack_vec(&rec).expect("encode");
        assert!(zerompk::from_msgpack::<ColumnarDmlWalRecord>(&bytes).is_err());
        assert!(zerompk::from_msgpack::<ColumnarWalRecord>(&bytes).is_err());
    }
}
