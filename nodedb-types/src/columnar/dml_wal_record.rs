// SPDX-License-Identifier: Apache-2.0

//! Columnar predicate-DML WAL record payload.
//!
//! `ColumnarOp::Update` / `ColumnarOp::Delete` mutate rows matched by a
//! predicate rather than by a specific row image, so there is no per-row
//! post-image to make durable at append time (the matching set is only known
//! once the Data Plane scans current state). This record instead persists the
//! **predicate itself** — collection, filters, and (for `Update`) the field
//! assignments — so crash replay re-executes the exact same mutation through
//! the exact same live handler that ran when the write was first accepted.
//! This mirrors the Raft replication path for the same operations
//! (`ColumnarBulkDml` in `control::wal_replication`), which already commits to
//! "log the predicate, re-apply deterministically" for this exact op pair.
//!
//! Rides `RecordType::TimeseriesBatch`, disambiguated from the row-payload
//! [`super::ColumnarWalRecord`] (`kind = "columnar"`) by `kind =
//! "columnar_dml"` and a disjoint map key set: [`ColumnarWalRecord`] requires
//! a `payload` key with no default, this record never writes one, so neither
//! shape decodes as the other (see `decode_batch_record`'s callers for the
//! decode-order argument).

use serde::{Deserialize, Serialize};

/// Map-encoded columnar predicate-DML WAL record.
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
pub struct ColumnarDmlWalRecord {
    /// Record kind tag — always `"columnar_dml"`.
    pub kind: String,
    /// Target collection name.
    pub collection: String,
    /// `true` for `ColumnarOp::Update`, `false` for `ColumnarOp::Delete`.
    pub is_update: bool,
    /// Serialized `Vec<nodedb_query::scan_filter::ScanFilter>` (MessagePack).
    pub filters: Vec<u8>,
    /// Field assignments for `Update`: `(column_name, msgpack_value_bytes)`.
    /// Always empty for `Delete`.
    pub updates: Vec<(String, Vec<u8>)>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::columnar::ColumnarWalRecord;

    #[test]
    fn round_trips_delete() {
        let rec = ColumnarDmlWalRecord {
            kind: "columnar_dml".to_string(),
            collection: "metrics".to_string(),
            is_update: false,
            filters: vec![1, 2, 3],
            updates: Vec::new(),
        };
        let bytes = zerompk::to_msgpack_vec(&rec).expect("encode");
        let decoded: ColumnarDmlWalRecord = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(decoded, rec);
    }

    #[test]
    fn round_trips_update_with_field_assignments() {
        let rec = ColumnarDmlWalRecord {
            kind: "columnar_dml".to_string(),
            collection: "metrics".to_string(),
            is_update: true,
            filters: vec![4, 5],
            updates: vec![("v".to_string(), vec![9, 9])],
        };
        let bytes = zerompk::to_msgpack_vec(&rec).expect("encode");
        let decoded: ColumnarDmlWalRecord = zerompk::from_msgpack(&bytes).expect("decode");
        assert_eq!(decoded, rec);
    }

    #[test]
    fn does_not_decode_as_row_payload_columnar_wal_record() {
        // A DML record never carries the `payload` key that
        // `ColumnarWalRecord` requires (no default), so it must not decode as
        // that shape — the two kinds must never collide inside
        // `decode_batch_record`.
        let rec = ColumnarDmlWalRecord {
            kind: "columnar_dml".to_string(),
            collection: "metrics".to_string(),
            is_update: false,
            filters: vec![1],
            updates: Vec::new(),
        };
        let bytes = zerompk::to_msgpack_vec(&rec).expect("encode");
        let as_row_record: Result<ColumnarWalRecord, _> = zerompk::from_msgpack(&bytes);
        assert!(
            as_row_record.is_err(),
            "columnar_dml bytes must not decode as the row-payload ColumnarWalRecord shape"
        );
    }

    #[test]
    fn row_payload_record_does_not_decode_as_dml_record() {
        let rec = ColumnarWalRecord {
            kind: "columnar".to_string(),
            collection: "metrics".to_string(),
            payload: vec![1, 2, 3],
            provenance: None,
            surrogates: Vec::new(),
            conflict_policy: Vec::new(),
        };
        let bytes = zerompk::to_msgpack_vec(&rec).expect("encode");
        let as_dml_record: Result<ColumnarDmlWalRecord, _> = zerompk::from_msgpack(&bytes);
        assert!(
            as_dml_record.is_err(),
            "columnar row-payload bytes must not decode as the ColumnarDmlWalRecord shape"
        );
    }
}
