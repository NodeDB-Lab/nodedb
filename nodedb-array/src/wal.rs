//! WAL record types for array CRDT sync operations.
//!
//! [`ArrayWalRecord`] is the durable on-disk representation of every
//! array sync event that must survive process restart. Origin appends
//! one record per inbound op before applying it to engine state;
//! recovery replays the log in order.
//!
//! # HLC byte invariant
//!
//! `hlc_bytes` fields carry the 18-byte layout produced by
//! [`crate::sync::Hlc::to_bytes()`]. Because zerompk does not derive
//! impls for fixed-size `[u8; N]` arrays they are stored as `Vec<u8>`.
//! The receiver must assert `hlc_bytes.len() == 18` and convert via
//! `Hlc::from_bytes(&arr)` where `arr: [u8; 18]`.

use serde::{Deserialize, Serialize};

/// A single record written to the WAL for array CRDT sync events.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub enum ArrayWalRecord {
    /// A single cell op (Put, Delete, or Erase) received from a Lite peer.
    ///
    /// `op_payload` is a zerompk-encoded [`crate::sync::ArrayOp`] produced
    /// by `nodedb_array::sync::op_codec::encode_op`. The WAL stores it
    /// opaque to decouple the WAL crate from schema evolution in `ArrayOp`.
    ///
    /// `hlc_bytes` is the 18-byte HLC of the op (invariant: `len == 18`).
    ApplyOp {
        array: String,
        op_payload: Vec<u8>,
        hlc_bytes: Vec<u8>,
    },

    /// An array schema was updated (Loro CRDT snapshot imported from a peer).
    ///
    /// `loro_snapshot` is the raw bytes from `SchemaDoc::export_snapshot()`.
    /// `schema_hlc_bytes` is the 18-byte HLC of the new schema version
    /// (invariant: `len == 18`).
    SchemaUpdate {
        array: String,
        loro_snapshot: Vec<u8>,
        schema_hlc_bytes: Vec<u8>,
    },

    /// A tile snapshot was applied as the result of a catch-up sync stream.
    ///
    /// `snapshot_hlc_bytes` is the 18-byte HLC covering this snapshot
    /// (invariant: `len == 18`). `chunks` holds the raw tile blobs —
    /// each element is a zerompk-encoded `Vec<ArrayOp>` matching the
    /// payload produced by `nodedb_array::sync::snapshot::encode_snapshot`.
    Snapshot {
        array: String,
        snapshot_hlc_bytes: Vec<u8>,
        coord_range_payload: Vec<u8>,
        chunks: Vec<Vec<u8>>,
    },

    /// GC collapsed all ops below `collapsed_through_hlc_bytes` into a
    /// snapshot. Ops with HLC < this value may be pruned from the op-log.
    ///
    /// `collapsed_through_hlc_bytes` is 18-byte HLC (invariant: `len == 18`).
    GcCollapse {
        array: String,
        collapsed_through_hlc_bytes: Vec<u8>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn zero_hlc() -> Vec<u8> {
        vec![0u8; 18]
    }

    #[test]
    fn apply_op_roundtrip() {
        let record = ArrayWalRecord::ApplyOp {
            array: "temperatures".into(),
            op_payload: vec![1, 2, 3, 4],
            hlc_bytes: zero_hlc(),
        };
        let encoded = zerompk::to_msgpack_vec(&record).expect("encode");
        let decoded: ArrayWalRecord = zerompk::from_msgpack(&encoded).expect("decode");
        assert_eq!(record, decoded);
    }

    #[test]
    fn schema_update_roundtrip() {
        let record = ArrayWalRecord::SchemaUpdate {
            array: "metrics".into(),
            loro_snapshot: vec![10, 20, 30],
            schema_hlc_bytes: zero_hlc(),
        };
        let encoded = zerompk::to_msgpack_vec(&record).expect("encode");
        let decoded: ArrayWalRecord = zerompk::from_msgpack(&encoded).expect("decode");
        assert_eq!(record, decoded);
    }

    #[test]
    fn snapshot_roundtrip() {
        let record = ArrayWalRecord::Snapshot {
            array: "coords".into(),
            snapshot_hlc_bytes: zero_hlc(),
            coord_range_payload: vec![5, 6, 7],
            chunks: vec![vec![1, 2], vec![3, 4]],
        };
        let encoded = zerompk::to_msgpack_vec(&record).expect("encode");
        let decoded: ArrayWalRecord = zerompk::from_msgpack(&encoded).expect("decode");
        assert_eq!(record, decoded);
    }

    #[test]
    fn gc_collapse_roundtrip() {
        let record = ArrayWalRecord::GcCollapse {
            array: "events".into(),
            collapsed_through_hlc_bytes: zero_hlc(),
        };
        let encoded = zerompk::to_msgpack_vec(&record).expect("encode");
        let decoded: ArrayWalRecord = zerompk::from_msgpack(&encoded).expect("decode");
        assert_eq!(record, decoded);
    }

    #[test]
    fn hlc_bytes_len_invariant() {
        // Confirm the invariant: the zero_hlc sentinel is exactly 18 bytes.
        assert_eq!(zero_hlc().len(), 18);
    }
}
