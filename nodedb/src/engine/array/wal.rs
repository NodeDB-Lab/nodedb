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
//! rule #11). LSNs are allocated by the Control Plane WAL writer; the
//! Data Plane just stamps the supplied LSN, so there is no engine-side
//! "appender" trait — these payload types are consumed by recovery and
//! by the WAL record types directly.
//!
//! `ArrayPutPayload` and `ArrayDeletePayload` are wrapped with a leading
//! format-version byte via [`encode_put_with_version`] and
//! [`decode_put_with_version`] / [`encode_delete_with_version`] /
//! [`decode_delete_with_version`]. Any version other than
//! [`ARRAY_WAL_FORMAT_VERSION`] is rejected fail-CLOSED — no zero-fill
//! defaults, no silent upgrades.

use nodedb_array::error::ArrayError;
use nodedb_array::types::cell_value::value::CellValue;
use nodedb_array::types::coord::value::CoordValue;
use nodedb_array::types::{ArrayId, TileId};
use nodedb_types::Surrogate;
use serde::{Deserialize, Serialize};

/// Current on-disk format version for array WAL payloads.
///
/// Increment this when the shape of [`ArrayPutPayload`] or
/// [`ArrayDeletePayload`] changes in a backward-incompatible way.
/// Readers that encounter any other version return
/// [`ArrayError::UnsupportedFormat`] fail-CLOSED.
///
/// Version 3: added `erasure: bool` field to [`ArrayDeleteCell`] to support
/// GDPR erasure as distinct from soft-delete tombstones.
pub const ARRAY_WAL_FORMAT_VERSION: u8 = 3;

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
    /// Control-Plane-allocated global surrogate for this `(array, coord)`.
    /// Recovery and follower replication re-derive it from the catalog
    /// surrogate map; the live INSERT path stamps it here so engine
    /// writers can carry it directly into the memtable / segment.
    pub surrogate: Surrogate,
    /// System-time timestamp (HLC ms) when the write was accepted by the
    /// Control Plane. Monotonically increasing within a given array.
    pub system_from_ms: i64,
    /// Valid-time lower bound (ms). Supplied by the caller; defaults to
    /// the system timestamp for non-bitemporal inserts.
    pub valid_from_ms: i64,
    /// Valid-time upper bound (ms). [`nodedb_types::OPEN_UPPER`] (`i64::MAX`)
    /// means "currently open / no expiry set".
    pub valid_until_ms: i64,
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
pub struct ArrayDeleteCell {
    pub coord: Vec<CoordValue>,
    /// System-time timestamp (HLC ms) when the delete was accepted by the
    /// Control Plane. Used as the tombstone/erasure version key in the memtable.
    pub system_from_ms: i64,
    /// When `true`, writes the GDPR erasure sentinel (`0xFE`) instead of the
    /// soft-delete tombstone sentinel (`0xFF`). Default is `false` (tombstone).
    /// GDPR erasure preserves coordinate existence for audit but removes content,
    /// and is physically stripped from merged segments once outside the
    /// audit-retention horizon.
    pub erasure: bool,
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
    pub cells: Vec<ArrayDeleteCell>,
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

#[derive(Debug, thiserror::Error)]
pub enum ArrayWalError {
    #[error("wal append failed: {detail}")]
    Append { detail: String },
    #[error("payload encode failed: {detail}")]
    Encode { detail: String },
}

/// Encode an [`ArrayPutPayload`] with a leading version byte.
///
/// The resulting bytes are: `[ARRAY_WAL_FORMAT_VERSION, ...msgpack...]`.
pub fn encode_put_with_version(payload: &ArrayPutPayload) -> Result<Vec<u8>, ArrayError> {
    let mut buf = zerompk::to_msgpack_vec(payload).map_err(|e| ArrayError::SegmentCorruption {
        detail: format!("encode ArrayPutPayload: {e}"),
    })?;
    buf.insert(0, ARRAY_WAL_FORMAT_VERSION);
    Ok(buf)
}

/// Decode bytes produced by [`encode_put_with_version`].
///
/// Returns [`ArrayError::UnsupportedFormat`] if the leading version byte
/// is not [`ARRAY_WAL_FORMAT_VERSION`]. Fails CLOSED — no silent upgrade.
pub fn decode_put_with_version(bytes: &[u8]) -> Result<ArrayPutPayload, ArrayError> {
    let (&version, rest) = bytes.split_first().ok_or(ArrayError::SegmentCorruption {
        detail: "ArrayPutPayload: empty WAL record".into(),
    })?;
    if version != ARRAY_WAL_FORMAT_VERSION {
        return Err(ArrayError::UnsupportedFormat { version });
    }
    zerompk::from_msgpack(rest).map_err(|e| ArrayError::SegmentCorruption {
        detail: format!("decode ArrayPutPayload: {e}"),
    })
}

/// Encode an [`ArrayDeletePayload`] with a leading version byte.
pub fn encode_delete_with_version(payload: &ArrayDeletePayload) -> Result<Vec<u8>, ArrayError> {
    let mut buf = zerompk::to_msgpack_vec(payload).map_err(|e| ArrayError::SegmentCorruption {
        detail: format!("encode ArrayDeletePayload: {e}"),
    })?;
    buf.insert(0, ARRAY_WAL_FORMAT_VERSION);
    Ok(buf)
}

/// Decode bytes produced by [`encode_delete_with_version`].
///
/// Returns [`ArrayError::UnsupportedFormat`] if the leading version byte
/// is not [`ARRAY_WAL_FORMAT_VERSION`]. Fails CLOSED — no silent upgrade.
pub fn decode_delete_with_version(bytes: &[u8]) -> Result<ArrayDeletePayload, ArrayError> {
    let (&version, rest) = bytes.split_first().ok_or(ArrayError::SegmentCorruption {
        detail: "ArrayDeletePayload: empty WAL record".into(),
    })?;
    if version != ARRAY_WAL_FORMAT_VERSION {
        return Err(ArrayError::UnsupportedFormat { version });
    }
    zerompk::from_msgpack(rest).map_err(|e| ArrayError::SegmentCorruption {
        detail: format!("decode ArrayDeletePayload: {e}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_put_payload() -> ArrayPutPayload {
        ArrayPutPayload {
            array_id: ArrayId::new(nodedb_types::TenantId::new(1), "g"),
            cells: vec![ArrayPutCell {
                coord: vec![CoordValue::Int64(1), CoordValue::Int64(2)],
                attrs: vec![CellValue::Int64(99)],
                surrogate: Surrogate::ZERO,
                system_from_ms: 1_000,
                valid_from_ms: 1_000,
                valid_until_ms: i64::MAX,
            }],
        }
    }

    #[test]
    fn put_payload_roundtrip() {
        let p = sample_put_payload();
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

    #[test]
    fn wal_record_rejects_old_versions() {
        let p = sample_put_payload();
        let raw = zerompk::to_msgpack_vec(&p).unwrap();

        for bad_version in [0x01u8, 0x02u8] {
            let mut bad = raw.clone();
            bad.insert(0, bad_version);
            let err = decode_put_with_version(&bad).unwrap_err();
            assert!(
                matches!(err, ArrayError::UnsupportedFormat { .. }),
                "version {bad_version}: expected UnsupportedFormat, got {err:?}"
            );
        }
    }

    #[test]
    fn wal_record_v3_roundtrip() {
        let p = sample_put_payload();
        let encoded = encode_put_with_version(&p).unwrap();
        assert_eq!(encoded[0], ARRAY_WAL_FORMAT_VERSION);
        let decoded = decode_put_with_version(&encoded).unwrap();
        assert_eq!(p, decoded);
    }

    #[test]
    fn delete_cell_erasure_flag_roundtrip() {
        let payload = ArrayDeletePayload {
            array_id: ArrayId::new(nodedb_types::TenantId::new(1), "g"),
            cells: vec![ArrayDeleteCell {
                coord: vec![CoordValue::Int64(5), CoordValue::Int64(7)],
                system_from_ms: 9_000,
                erasure: true,
            }],
        };
        let encoded = encode_delete_with_version(&payload).unwrap();
        assert_eq!(encoded[0], ARRAY_WAL_FORMAT_VERSION);
        let decoded = decode_delete_with_version(&encoded).unwrap();
        assert!(decoded.cells[0].erasure);
        assert_eq!(decoded.cells[0].system_from_ms, 9_000);
    }
}
