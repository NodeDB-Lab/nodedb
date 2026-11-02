// SPDX-License-Identifier: BUSL-1.1

//! WAL payload definitions for the array engine.
//!
//! Three record types ride on the existing nodedb-wal pipeline:
//!
//! * [`ArrayPutPayload`] — a batch of cell writes (`coord -> attrs`) for a
//!   single array. Batched so the recovery path can rebuild a memtable
//!   without one syscall per cell.
//! * [`ArrayDeletePayload`] — a batch of point deletes for a single array.
//! * [`ArrayFlushPayload`] — an explicit flush request. Replay applies
//!   nothing for it: the manifest's replay stamp, not this record, decides
//!   which `ArrayPut`/`ArrayDelete` records a segment already holds.
//!
//! All three are zerompk-encoded — JSON is reserved for the API
//! boundary, never used between planes. LSNs are allocated by the
//! Control Plane WAL writer; the
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
use nodedb_types::sync::wire::SyncProvenance;
use serde::{Deserialize, Serialize};

/// On-disk format version for array WAL payloads.
///
/// Readers that encounter any other version return
/// [`ArrayError::UnsupportedFormat`] fail-CLOSED.
pub const ARRAY_WAL_FORMAT_VERSION: u8 = 1;

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
    /// Control-Plane-allocated global surrogate for this `(array, coord)`,
    /// carried into the memtable and segment. Never `Surrogate::ZERO`: a put
    /// with an unbound cell is refused before any cell is written.
    pub surrogate: Surrogate,
    /// The tile-version stamp the engine writes the cell at. A Data Plane
    /// apply (`handle_array_put`, WAL replay) overwrites it with the
    /// system-time ordinal of the apply. A restored cell
    /// (`crate::wal::ArrayRestoreRedo`) keeps it: the value is the
    /// backed-up version's stamp, or the stamp the restore set the version
    /// again at. A direct engine call writes it as given.
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
    /// The vShard whose write stores the cells: the `vshard_id` of the
    /// `ArrayOp::Put`. Every tile version the record writes carries it, on
    /// every replica and every replay.
    pub vshard_id: u32,
    pub cells: Vec<ArrayPutCell>,
    /// Sync provenance for idempotency — `None` for locally-originated writes.
    /// Carried here so the WAL record is self-contained for deduplication
    /// on replay; the field is ignored until the sync ingest path populates it.
    #[serde(default)]
    pub provenance: Option<SyncProvenance>,
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
    /// The tombstone or erasure version stamp. A Data Plane apply overwrites
    /// it with the system-time ordinal of the apply, or keeps a restored
    /// stamp, as for [`ArrayPutCell::system_from_ms`].
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
    /// The vShard whose write stores the tombstones, as
    /// [`ArrayPutPayload::vshard_id`].
    pub vshard_id: u32,
    pub cells: Vec<ArrayDeleteCell>,
    /// Sync provenance for idempotency — `None` for locally-originated writes.
    #[serde(default)]
    pub provenance: Option<SyncProvenance>,
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
    /// Sync provenance for idempotency — `None` for locally-originated flushes.
    #[serde(default)]
    pub provenance: Option<SyncProvenance>,
}

#[derive(Debug, thiserror::Error)]
pub enum ArrayWalError {
    #[error("wal append failed: {detail}")]
    Append { detail: String },
    #[error("payload encode failed: {detail}")]
    Encode { detail: String },
}

/// The msgpack fixarray header of [`ArrayPutPayload`] and
/// [`ArrayDeletePayload`]. zerompk encodes a struct as an array of its
/// fields, and both payloads have four: `array_id`, `vshard_id`, `cells`,
/// `provenance`.
const PAYLOAD_ARRAY_HEADER: u8 = 0x94;

/// Encode an [`ArrayPutPayload`] with a leading version byte.
///
/// The resulting bytes are: `[ARRAY_WAL_FORMAT_VERSION, ...msgpack...]`.
pub fn encode_put_with_version(payload: &ArrayPutPayload) -> Result<Vec<u8>, ArrayError> {
    encode_versioned(payload, "ArrayPutPayload")
}

/// Encode the versioned `ArrayPut` record of `array_id`, placed on
/// `vshard_id`, around `cells_msgpack`, an already-encoded
/// `Vec<ArrayPutCell>`.
///
/// The cell bytes are copied in as they are, never decoded. The result is
/// byte-identical to [`encode_put_with_version`] over the decoded payload,
/// so [`decode_put_with_version`] reads it. The caller passes cell bytes a
/// `Vec<ArrayPutCell>` encoder produced.
pub fn encode_put_spliced(
    array_id: &ArrayId,
    vshard_id: u32,
    cells_msgpack: &[u8],
    provenance: &Option<SyncProvenance>,
) -> Result<Vec<u8>, ArrayError> {
    splice_versioned(
        array_id,
        vshard_id,
        cells_msgpack,
        provenance,
        "ArrayPutPayload",
    )
}

/// [`encode_put_spliced`] for an `ArrayDelete` record around an
/// already-encoded `Vec<ArrayDeleteCell>`. Byte-identical to
/// [`encode_delete_with_version`] over the decoded payload.
pub fn encode_delete_spliced(
    array_id: &ArrayId,
    vshard_id: u32,
    cells_msgpack: &[u8],
    provenance: &Option<SyncProvenance>,
) -> Result<Vec<u8>, ArrayError> {
    splice_versioned(
        array_id,
        vshard_id,
        cells_msgpack,
        provenance,
        "ArrayDeletePayload",
    )
}

/// Write the version byte, then `value`, into one buffer.
fn encode_versioned<T: zerompk::ToMessagePack>(
    value: &T,
    what: &str,
) -> Result<Vec<u8>, ArrayError> {
    let mut buf = vec![ARRAY_WAL_FORMAT_VERSION];
    write_into(&mut buf, value, what)?;
    Ok(buf)
}

fn splice_versioned(
    array_id: &ArrayId,
    vshard_id: u32,
    cells_msgpack: &[u8],
    provenance: &Option<SyncProvenance>,
    what: &str,
) -> Result<Vec<u8>, ArrayError> {
    // The id and provenance are small. The cell bytes dominate the record.
    let mut buf = Vec::with_capacity(cells_msgpack.len() + 128);
    buf.push(ARRAY_WAL_FORMAT_VERSION);
    buf.push(PAYLOAD_ARRAY_HEADER);
    write_into(&mut buf, array_id, what)?;
    write_into(&mut buf, &vshard_id, what)?;
    buf.extend_from_slice(cells_msgpack);
    write_into(&mut buf, provenance, what)?;
    Ok(buf)
}

fn write_into<T: zerompk::ToMessagePack>(
    buf: &mut Vec<u8>,
    value: &T,
    what: &str,
) -> Result<(), ArrayError> {
    zerompk::write_msgpack(buf, value).map_err(|e| ArrayError::SegmentCorruption {
        detail: format!("encode {what}: {e}"),
    })
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
    encode_versioned(payload, "ArrayDeletePayload")
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
            vshard_id: 517,
            cells: vec![ArrayPutCell {
                coord: vec![CoordValue::Int64(1), CoordValue::Int64(2)],
                attrs: vec![CellValue::Int64(99)],
                surrogate: Surrogate::new(1),
                system_from_ms: 1_000,
                valid_from_ms: 1_000,
                valid_until_ms: i64::MAX,
            }],
            provenance: None,
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
            provenance: None,
        };
        let bytes = zerompk::to_msgpack_vec(&p).unwrap();
        let back: ArrayFlushPayload = zerompk::from_msgpack(&bytes).unwrap();
        assert_eq!(p, back);
    }

    #[test]
    fn wal_record_rejects_another_version() {
        let p = sample_put_payload();
        let mut bad = zerompk::to_msgpack_vec(&p).unwrap();
        let bad_version = ARRAY_WAL_FORMAT_VERSION + 1;
        bad.insert(0, bad_version);
        let err = decode_put_with_version(&bad).unwrap_err();
        assert!(
            matches!(err, ArrayError::UnsupportedFormat { version } if version == bad_version),
            "version {bad_version}: expected UnsupportedFormat, got {err:?}"
        );
    }

    #[test]
    fn wal_record_roundtrip() {
        let p = sample_put_payload();
        let encoded = encode_put_with_version(&p).unwrap();
        assert_eq!(encoded[0], ARRAY_WAL_FORMAT_VERSION);
        let decoded = decode_put_with_version(&encoded).unwrap();
        assert_eq!(p, decoded);
    }

    #[test]
    fn spliced_put_matches_the_encoded_payload() {
        let provenances = [
            None,
            Some(SyncProvenance {
                producer_id: 7,
                epoch: 2,
                stream_id: 3,
                seq: 9,
            }),
        ];
        for provenance in provenances {
            let mut p = sample_put_payload();
            p.provenance = provenance;
            let cells = zerompk::to_msgpack_vec(&p.cells).unwrap();
            let spliced =
                encode_put_spliced(&p.array_id, p.vshard_id, &cells, &p.provenance).unwrap();
            assert_eq!(spliced, encode_put_with_version(&p).unwrap());
            assert_eq!(decode_put_with_version(&spliced).unwrap(), p);
        }
    }

    #[test]
    fn spliced_delete_matches_the_encoded_payload() {
        let payload = ArrayDeletePayload {
            array_id: ArrayId::new(nodedb_types::TenantId::new(1), "g"),
            vshard_id: 33,
            cells: vec![ArrayDeleteCell {
                coord: vec![CoordValue::Int64(5), CoordValue::Int64(7)],
                system_from_ms: 9_000,
                erasure: false,
            }],
            provenance: None,
        };
        let cells = zerompk::to_msgpack_vec(&payload.cells).unwrap();
        let spliced = encode_delete_spliced(
            &payload.array_id,
            payload.vshard_id,
            &cells,
            &payload.provenance,
        )
        .unwrap();
        assert_eq!(spliced, encode_delete_with_version(&payload).unwrap());
        assert_eq!(decode_delete_with_version(&spliced).unwrap(), payload);
    }

    #[test]
    fn delete_cell_erasure_flag_roundtrip() {
        let payload = ArrayDeletePayload {
            array_id: ArrayId::new(nodedb_types::TenantId::new(1), "g"),
            vshard_id: 33,
            cells: vec![ArrayDeleteCell {
                coord: vec![CoordValue::Int64(5), CoordValue::Int64(7)],
                system_from_ms: 9_000,
                erasure: true,
            }],
            provenance: None,
        };
        let encoded = encode_delete_with_version(&payload).unwrap();
        assert_eq!(encoded[0], ARRAY_WAL_FORMAT_VERSION);
        let decoded = decode_delete_with_version(&encoded).unwrap();
        assert!(decoded.cells[0].erasure);
        assert_eq!(decoded.cells[0].system_from_ms, 9_000);
    }
}
