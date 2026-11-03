// SPDX-License-Identifier: Apache-2.0

//! Operation codes and response status for the native binary protocol.

use serde::{Deserialize, Serialize};

// ─── Operation Codes ────────────────────────────────────────────────

/// Operation codes for the native binary protocol.
///
/// Encoded as a single `u8` in both the MessagePack frame and JSON frame
/// (e.g. `{"op":3}` for `Status`). The `#[serde(try_from = "u8", into = "u8")]`
/// attribute makes JSON encoding consistent with the numeric opcode values.
///
/// `#[non_exhaustive]` — new operation codes may be added as engines grow.
/// Wire-side unknown-op handling is covered by `Unknown(u8)`;
/// this attribute adds Rust API hygiene.
#[non_exhaustive]
#[repr(u8)]
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[serde(try_from = "u8", into = "u8")]
#[msgpack(c_enum)]
pub enum OpCode {
    // ── Auth & session ──────────────────────────────────────────
    Auth = 0x01,
    Ping = 0x02,
    /// Report startup/readiness status. Returns the current startup phase
    /// and whether the node is healthy. Does not require authentication.
    Status = 0x03,

    // ── Data operations (direct Data Plane dispatch) ────────────
    PointGet = 0x10,
    PointPut = 0x11,
    PointDelete = 0x12,
    VectorSearch = 0x13,
    RangeScan = 0x14,
    CrdtRead = 0x15,
    CrdtApply = 0x16,
    GraphRagFusion = 0x17,
    AlterCollectionPolicy = 0x18,

    // ── SQL & DDL ───────────────────────────────────────────────
    Sql = 0x20,
    Ddl = 0x21,
    Explain = 0x22,
    CopyFrom = 0x23,

    // ── Session parameters ──────────────────────────────────────
    Set = 0x30,
    Show = 0x31,
    Reset = 0x32,

    // ── Transaction control ─────────────────────────────────────
    Begin = 0x40,
    Commit = 0x41,
    Rollback = 0x42,

    // ── Graph operations (direct Data Plane dispatch) ───────────
    GraphHop = 0x50,
    GraphNeighbors = 0x51,
    GraphPath = 0x52,
    GraphSubgraph = 0x53,
    EdgePut = 0x54,
    EdgeDelete = 0x55,
    GraphAlgo = 0x56,
    GraphMatch = 0x57,

    // ── Spatial operations (direct Data Plane dispatch) ────────
    SpatialScan = 0x19,

    // ── Timeseries operations (direct Data Plane dispatch) ──────
    TimeseriesScan = 0x1A,
    TimeseriesIngest = 0x1B,

    // ── Search operations (direct Data Plane dispatch) ──────────
    TextSearch = 0x60,
    HybridSearch = 0x61,

    // ── Batch operations ────────────────────────────────────────
    VectorBatchInsert = 0x70,
    DocumentBatchInsert = 0x71,

    // ── KV advanced operations ──────────────────────────────────
    KvScan = 0x72,
    KvExpire = 0x73,
    KvPersist = 0x74,
    KvGetTtl = 0x75,
    KvBatchGet = 0x76,
    KvBatchPut = 0x77,
    KvFieldGet = 0x78,
    KvFieldSet = 0x79,

    // ── Document advanced operations ────────────────────────────
    DocumentUpdate = 0x7A,
    DocumentScan = 0x7B,
    DocumentUpsert = 0x7C,
    DocumentBulkUpdate = 0x7D,
    DocumentBulkDelete = 0x7E,

    // ── Vector advanced operations ──────────────────────────────
    VectorInsert = 0x7F,
    VectorMultiSearch = 0x80,
    VectorDelete = 0x81,

    // ── Columnar operations ─────────────────────────────────────
    ColumnarScan = 0x82,
    ColumnarInsert = 0x83,

    // ── Query operations ────────────────────────────────────────
    RecursiveScan = 0x84,

    // ── Document DDL operations ─────────────────────────────────
    DocumentTruncate = 0x85,
    DocumentEstimateCount = 0x86,
    DocumentInsertSelect = 0x87,
    DocumentRegister = 0x88,
    DocumentDropIndex = 0x89,

    // ── KV DDL operations ───────────────────────────────────────
    KvRegisterIndex = 0x8A,
    KvDropIndex = 0x8B,
    KvTruncate = 0x8C,

    // ── Vector DDL operations ───────────────────────────────────
    VectorSetParams = 0x8D,

    // ── KV atomic operations ───────────────────────────────────
    KvIncr = 0x8E,
    KvIncrFloat = 0x8F,
    KvCas = 0x90,
    KvGetSet = 0x91,

    // ── KV sorted index operations ─────────────────────────────
    KvRegisterSortedIndex = 0x92,
    KvDropSortedIndex = 0x93,
    KvSortedIndexRank = 0x94,
    KvSortedIndexTopK = 0x95,
    KvSortedIndexRange = 0x96,
    KvSortedIndexCount = 0x97,
    KvSortedIndexScore = 0x98,

    // ── CRDT list operations ─────────────────────────────────────
    CrdtListInsert = 0x99,
    CrdtListDelete = 0x9A,
    CrdtListMove = 0x9B,

    // ── Array bulk load ──────────────────────────────────────────
    /// Load cells into an array: `collection` names it, and `payload` holds a
    /// zerompk [`ArrayPutBatch`](super::array_batch::ArrayPutBatch).
    ArrayPutBatch = 0x9C,

    // ── Graph edge batches ───────────────────────────────────────
    /// Insert edges as one unit: `collection` names the collection, and
    /// `payload` holds a zerompk [`EdgePutBatch`](super::edge_batch::EdgePutBatch).
    EdgePutBatch = 0x9D,
    /// Delete edges as one unit: `collection` names the collection, and
    /// `payload` holds a zerompk
    /// [`EdgeDeleteBatch`](super::edge_batch::EdgeDeleteBatch).
    EdgeDeleteBatch = 0x9E,
}

impl OpCode {
    /// Returns true if this operation is a write that requires WAL append.
    pub fn is_write(&self) -> bool {
        matches!(
            self,
            OpCode::PointPut
                | OpCode::PointDelete
                | OpCode::CrdtApply
                | OpCode::EdgePut
                | OpCode::EdgeDelete
                | OpCode::VectorBatchInsert
                | OpCode::DocumentBatchInsert
                | OpCode::AlterCollectionPolicy
                | OpCode::TimeseriesIngest
                | OpCode::KvExpire
                | OpCode::KvPersist
                | OpCode::KvBatchPut
                | OpCode::KvFieldSet
                | OpCode::DocumentUpdate
                | OpCode::DocumentUpsert
                | OpCode::DocumentBulkUpdate
                | OpCode::DocumentBulkDelete
                | OpCode::VectorInsert
                | OpCode::VectorDelete
                | OpCode::ColumnarInsert
                | OpCode::DocumentTruncate
                | OpCode::DocumentInsertSelect
                | OpCode::DocumentRegister
                | OpCode::DocumentDropIndex
                | OpCode::KvRegisterIndex
                | OpCode::KvDropIndex
                | OpCode::KvTruncate
                | OpCode::VectorSetParams
                | OpCode::KvIncr
                | OpCode::KvIncrFloat
                | OpCode::KvCas
                | OpCode::KvGetSet
                | OpCode::KvRegisterSortedIndex
                | OpCode::KvDropSortedIndex
                | OpCode::CrdtListInsert
                | OpCode::CrdtListDelete
                | OpCode::CrdtListMove
                | OpCode::ArrayPutBatch
                | OpCode::EdgePutBatch
                | OpCode::EdgeDeleteBatch
        )
    }
}

impl From<OpCode> for u8 {
    fn from(op: OpCode) -> u8 {
        op as u8
    }
}

/// Error returned when decoding an unknown `OpCode` byte from the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("unknown OpCode byte: 0x{0:02X}")]
pub struct UnknownOpCode(pub u8);

impl TryFrom<u8> for OpCode {
    type Error = UnknownOpCode;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(OpCode::Auth),
            0x02 => Ok(OpCode::Ping),
            0x03 => Ok(OpCode::Status),
            0x10 => Ok(OpCode::PointGet),
            0x11 => Ok(OpCode::PointPut),
            0x12 => Ok(OpCode::PointDelete),
            0x13 => Ok(OpCode::VectorSearch),
            0x14 => Ok(OpCode::RangeScan),
            0x15 => Ok(OpCode::CrdtRead),
            0x16 => Ok(OpCode::CrdtApply),
            0x17 => Ok(OpCode::GraphRagFusion),
            0x18 => Ok(OpCode::AlterCollectionPolicy),
            0x19 => Ok(OpCode::SpatialScan),
            0x1A => Ok(OpCode::TimeseriesScan),
            0x1B => Ok(OpCode::TimeseriesIngest),
            0x20 => Ok(OpCode::Sql),
            0x21 => Ok(OpCode::Ddl),
            0x22 => Ok(OpCode::Explain),
            0x23 => Ok(OpCode::CopyFrom),
            0x30 => Ok(OpCode::Set),
            0x31 => Ok(OpCode::Show),
            0x32 => Ok(OpCode::Reset),
            0x40 => Ok(OpCode::Begin),
            0x41 => Ok(OpCode::Commit),
            0x42 => Ok(OpCode::Rollback),
            0x50 => Ok(OpCode::GraphHop),
            0x51 => Ok(OpCode::GraphNeighbors),
            0x52 => Ok(OpCode::GraphPath),
            0x53 => Ok(OpCode::GraphSubgraph),
            0x54 => Ok(OpCode::EdgePut),
            0x55 => Ok(OpCode::EdgeDelete),
            0x56 => Ok(OpCode::GraphAlgo),
            0x57 => Ok(OpCode::GraphMatch),
            0x60 => Ok(OpCode::TextSearch),
            0x61 => Ok(OpCode::HybridSearch),
            0x70 => Ok(OpCode::VectorBatchInsert),
            0x71 => Ok(OpCode::DocumentBatchInsert),
            0x72 => Ok(OpCode::KvScan),
            0x73 => Ok(OpCode::KvExpire),
            0x74 => Ok(OpCode::KvPersist),
            0x75 => Ok(OpCode::KvGetTtl),
            0x76 => Ok(OpCode::KvBatchGet),
            0x77 => Ok(OpCode::KvBatchPut),
            0x78 => Ok(OpCode::KvFieldGet),
            0x79 => Ok(OpCode::KvFieldSet),
            0x7A => Ok(OpCode::DocumentUpdate),
            0x7B => Ok(OpCode::DocumentScan),
            0x7C => Ok(OpCode::DocumentUpsert),
            0x7D => Ok(OpCode::DocumentBulkUpdate),
            0x7E => Ok(OpCode::DocumentBulkDelete),
            0x7F => Ok(OpCode::VectorInsert),
            0x80 => Ok(OpCode::VectorMultiSearch),
            0x81 => Ok(OpCode::VectorDelete),
            0x82 => Ok(OpCode::ColumnarScan),
            0x83 => Ok(OpCode::ColumnarInsert),
            0x84 => Ok(OpCode::RecursiveScan),
            0x85 => Ok(OpCode::DocumentTruncate),
            0x86 => Ok(OpCode::DocumentEstimateCount),
            0x87 => Ok(OpCode::DocumentInsertSelect),
            0x88 => Ok(OpCode::DocumentRegister),
            0x89 => Ok(OpCode::DocumentDropIndex),
            0x8A => Ok(OpCode::KvRegisterIndex),
            0x8B => Ok(OpCode::KvDropIndex),
            0x8C => Ok(OpCode::KvTruncate),
            0x8D => Ok(OpCode::VectorSetParams),
            0x8E => Ok(OpCode::KvIncr),
            0x8F => Ok(OpCode::KvIncrFloat),
            0x90 => Ok(OpCode::KvCas),
            0x91 => Ok(OpCode::KvGetSet),
            0x92 => Ok(OpCode::KvRegisterSortedIndex),
            0x93 => Ok(OpCode::KvDropSortedIndex),
            0x94 => Ok(OpCode::KvSortedIndexRank),
            0x95 => Ok(OpCode::KvSortedIndexTopK),
            0x96 => Ok(OpCode::KvSortedIndexRange),
            0x97 => Ok(OpCode::KvSortedIndexCount),
            0x98 => Ok(OpCode::KvSortedIndexScore),
            0x99 => Ok(OpCode::CrdtListInsert),
            0x9A => Ok(OpCode::CrdtListDelete),
            0x9B => Ok(OpCode::CrdtListMove),
            0x9C => Ok(OpCode::ArrayPutBatch),
            0x9D => Ok(OpCode::EdgePutBatch),
            0x9E => Ok(OpCode::EdgeDeleteBatch),
            other => Err(UnknownOpCode(other)),
        }
    }
}

// ─── Response Status ────────────────────────────────────────────────

/// Status code in response frames.
///
/// `#[non_exhaustive]` — additional status codes (e.g. `Throttled`, `Redirect`)
/// may be added as the protocol evolves.
#[non_exhaustive]
#[repr(u8)]
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(c_enum)]
pub enum ResponseStatus {
    /// Request completed successfully.
    Ok = 0,
    /// Partial result — more chunks follow with the same `seq`.
    Partial = 1,
    /// Request failed — see `error` field.
    Error = 2,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn opcode_repr() {
        assert_eq!(OpCode::Auth as u8, 0x01);
        assert_eq!(OpCode::Sql as u8, 0x20);
        assert_eq!(OpCode::Begin as u8, 0x40);
        assert_eq!(OpCode::GraphHop as u8, 0x50);
        assert_eq!(OpCode::TextSearch as u8, 0x60);
        assert_eq!(OpCode::VectorBatchInsert as u8, 0x70);
    }

    #[test]
    fn opcode_is_write() {
        assert!(OpCode::PointPut.is_write());
        assert!(OpCode::PointDelete.is_write());
        assert!(OpCode::CrdtApply.is_write());
        assert!(OpCode::EdgePut.is_write());
        assert!(!OpCode::PointGet.is_write());
        assert!(!OpCode::Sql.is_write());
        assert!(!OpCode::VectorSearch.is_write());
        assert!(!OpCode::Ping.is_write());
    }

    #[test]
    fn response_status_repr() {
        assert_eq!(ResponseStatus::Ok as u8, 0);
        assert_eq!(ResponseStatus::Partial as u8, 1);
        assert_eq!(ResponseStatus::Error as u8, 2);
    }

    #[test]
    fn crdt_list_opcodes_try_from_u8_roundtrip() {
        assert_eq!(OpCode::try_from(0x99), Ok(OpCode::CrdtListInsert));
        assert_eq!(OpCode::try_from(0x9A), Ok(OpCode::CrdtListDelete));
        assert_eq!(OpCode::try_from(0x9B), Ok(OpCode::CrdtListMove));

        assert_eq!(u8::from(OpCode::CrdtListInsert), 0x99);
        assert_eq!(u8::from(OpCode::CrdtListDelete), 0x9A);
        assert_eq!(u8::from(OpCode::CrdtListMove), 0x9B);
    }

    #[test]
    fn array_put_batch_opcode_roundtrips_and_is_a_write() {
        assert_eq!(OpCode::try_from(0x9C), Ok(OpCode::ArrayPutBatch));
        assert_eq!(u8::from(OpCode::ArrayPutBatch), 0x9C);
        assert!(OpCode::ArrayPutBatch.is_write());
    }

    #[test]
    fn edge_batch_opcodes_roundtrip_and_are_writes() {
        assert_eq!(OpCode::try_from(0x9D), Ok(OpCode::EdgePutBatch));
        assert_eq!(OpCode::try_from(0x9E), Ok(OpCode::EdgeDeleteBatch));
        assert_eq!(u8::from(OpCode::EdgePutBatch), 0x9D);
        assert_eq!(u8::from(OpCode::EdgeDeleteBatch), 0x9E);
        assert!(OpCode::EdgePutBatch.is_write());
        assert!(OpCode::EdgeDeleteBatch.is_write());
    }

    #[test]
    fn crdt_list_opcodes_are_writes() {
        assert!(OpCode::CrdtListInsert.is_write());
        assert!(OpCode::CrdtListDelete.is_write());
        assert!(OpCode::CrdtListMove.is_write());
    }
}
