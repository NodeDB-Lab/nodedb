//! WAL payload definitions for the array engine.
//!
//! Three record types ride on the existing nodedb-wal pipeline:
//!
//! * [`ArrayPutPayload`] — a batch of cell writes (`coord -> attrs`) for a
//!   single array. Batched so the recovery path can rebuild a memtable
//!   without one syscall per cell.
//! * [`ArrayDeletePayload`] — a batch of point deletes for a single array.
//! * [`ArrayFlushPayload`] — emitted *after* the engine has fsync'd a new
//!   segment file. Replay treats it as a watermark: any earlier
//!   `ArrayPut`/`ArrayDelete` whose LSN <= this record's LSN is already
//!   captured in the segment and must not be reapplied.
//!
//! All three are zerompk-encoded — never JSON between planes (CLAUDE.md
//! rule #11). The engine never builds [`nodedb_wal::WalRecord`] itself;
//! it hands payloads to an [`ArrayWalAppender`] which Origin wires to
//! the real group-commit pipeline.

use nodedb_array::types::cell_value::value::CellValue;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_array::types::{ArrayId, TileId};
use serde::{Deserialize, Serialize};

#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ArrayPutCell {
    pub coord: Vec<CoordValue>,
    pub attrs: Vec<CellValue>,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ArrayPutPayload {
    pub array_id: ArrayId,
    pub cells: Vec<ArrayPutCell>,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ArrayDeletePayload {
    pub array_id: ArrayId,
    pub coords: Vec<Vec<CoordValue>>,
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
pub struct ArrayFlushPayload {
    pub array_id: ArrayId,
    /// Segment file name relative to the array's directory (no path
    /// separators). Recovery joins it with the array root.
    pub segment_id: String,
    /// Tile ids that landed in the segment — lets compaction and
    /// debugging cross-check the manifest without re-decoding the file.
    pub tile_ids: Vec<TileId>,
}

/// Persistence trait the engine calls into. Origin implements this with
/// the real group-commit WAL writer; tests use an in-memory variant.
///
/// Returns the assigned LSN so the engine can stamp memtable entries
/// for ordering against subsequent flush watermarks.
pub trait ArrayWalAppender {
    fn append_put(&mut self, payload: &ArrayPutPayload) -> Result<u64, ArrayWalError>;
    fn append_delete(&mut self, payload: &ArrayDeletePayload) -> Result<u64, ArrayWalError>;
    fn append_flush(&mut self, payload: &ArrayFlushPayload) -> Result<u64, ArrayWalError>;
}

#[derive(Debug, thiserror::Error)]
pub enum ArrayWalError {
    #[error("wal append failed: {detail}")]
    Append { detail: String },
    #[error("payload encode failed: {detail}")]
    Encode { detail: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_payload_roundtrip() {
        let p = ArrayPutPayload {
            array_id: ArrayId::new(nodedb_types::TenantId::new(1), "g"),
            cells: vec![ArrayPutCell {
                coord: vec![CoordValue::Int64(1), CoordValue::Int64(2)],
                attrs: vec![CellValue::Int64(99)],
            }],
        };
        let bytes = zerompk::to_msgpack_vec(&p).unwrap();
        let back: ArrayPutPayload = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(p, back);
    }

    #[test]
    fn flush_payload_roundtrip() {
        let p = ArrayFlushPayload {
            array_id: ArrayId::new(nodedb_types::TenantId::new(1), "g"),
            segment_id: "00000001.ndas".into(),
            tile_ids: vec![TileId::snapshot(7)],
        };
        let bytes = zerompk::to_msgpack_vec(&p).unwrap();
        let back: ArrayFlushPayload = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(p, back);
    }
}
