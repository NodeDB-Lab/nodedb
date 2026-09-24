// SPDX-License-Identifier: BUSL-1.1

//! Redo-log transaction record: the replayable payload of a
//! [`RecordType::TransactionRedo`](nodedb_wal::record::RecordType::TransactionRedo)
//! WAL record.
//!
//! A `RedoRecord` groups an ordered set of engine-native sub-records — each in
//! the exact payload shape that engine's own per-op WAL record uses — into one
//! durable, atomically-replayable unit. Because every sub-record preserves its
//! own engine `record_type`, replay reconstitutes a `WalRecord` per sub-op and
//! feeds it to that engine's existing replay path with no tag loss.
//!
//! All three structs are map-encoded (`#[msgpack(map)]`) so fields can be added
//! additively: an older serialized record that predates a field decodes it to
//! its default. `version` carries an explicit format generation alongside that
//! field-level tolerance.

use serde::{Deserialize, Serialize};

/// The replayable payload of a `TransactionRedo` WAL record.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct RedoRecord {
    /// Format generation of this record. Bumped when the sub-record encoding
    /// changes in a way field-level defaulting alone cannot express.
    pub version: u16,
    /// The engine-native sub-records, applied in order on replay.
    pub ops: Vec<RedoSubRecord>,
    /// Calvin sequencer stamp.
    ///
    /// `None` for single-shard transactions. `Some(_)` makes this record double
    /// as the Calvin applied-marker for the stamped `(epoch, position)` on
    /// `vshard_id`, so the durable redo record and the sequencer acknowledgement
    /// are one write.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[msgpack(default)]
    pub calvin_stamp: Option<CalvinStamp>,
}

/// One engine-native sub-record within a [`RedoRecord`].
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct RedoSubRecord {
    /// The engine `record_type` this payload belongs to (same discriminant
    /// space as the WAL record header), so replay can reconstitute the exact
    /// per-engine `WalRecord`.
    pub record_type: u32,
    /// The sub-op payload, in that engine's existing per-op WAL record shape —
    /// produced by the same encoders the autocommit path uses.
    pub payload: Vec<u8>,
}

/// Calvin sequencer coordinates that a [`RedoRecord`] may carry to double as an
/// applied-marker, and what the vShard's slice folds. Mirrors
/// `nodedb_wal::CalvinAppliedPayload` in its coordinates.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct CalvinStamp {
    /// Sequencer epoch of the applied transaction.
    pub epoch: u64,
    /// Zero-based position within the epoch batch.
    pub position: u32,
    /// The vshard that applied this transaction.
    pub vshard_id: u32,
    /// Every collection the vShard's slice writes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[msgpack(default)]
    pub collections: Vec<String>,
    /// The materialized-sum targets the slice's document writes fold into,
    /// keyed by source collection. The live install and restart replay both
    /// fold from them at the record's LSN, so no later record carries the
    /// target rows.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[msgpack(default)]
    pub sum_targets: Vec<nodedb_physical::physical_plan::RedoSumTargets>,
}

/// Redo sub-record payload for a graph edge upsert — the payload bytes of a
/// [`RecordType::Put`](nodedb_wal::record::RecordType::Put) sub-record inside a
/// graph `RedoRecord`.
///
/// One shared definition for every encode and decode site so the field set is a
/// compile-time invariant. This replaced a positional tuple that silently
/// drifted in arity across its ~half-dozen encode/decode sites — appending a
/// field there produced runtime `ArrayLengthMismatch` (or a silently-skipped
/// record that lost the write) at whichever site was not updated in lockstep.
///
/// Map-encoded (`#[msgpack(map)]`), keying fields by name so the set can grow
/// additively — the same idiom [`RedoRecord`] uses. `system_from` is
/// `#[msgpack(default)]`, so a record written before that field existed decodes
/// it to `None`, preserving legacy records without a separate fallback path.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct EdgePutRedo {
    pub collection: String,
    pub src_id: String,
    pub label: String,
    pub dst_id: String,
    pub properties: Vec<u8>,
    pub src_surrogate: u32,
    pub dst_surrogate: u32,
    /// Frozen bitemporal `system_from` ordinal for deterministic cross-replica
    /// replay. `None` in legacy records that predate the field.
    #[serde(default)]
    #[msgpack(default)]
    pub system_from: Option<i64>,
}

/// Redo sub-record payload for a graph edge delete — the payload bytes of a
/// [`RecordType::Delete`](nodedb_wal::record::RecordType::Delete) sub-record
/// inside a graph `RedoRecord`. See [`EdgePutRedo`] for why this is a struct
/// rather than a positional tuple.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct EdgeDeleteRedo {
    pub collection: String,
    pub src_id: String,
    pub label: String,
    pub dst_id: String,
    /// Frozen bitemporal `system_from` ordinal for deterministic cross-replica
    /// replay. `None` in legacy records that predate the field.
    #[serde(default)]
    #[msgpack(default)]
    pub system_from: Option<i64>,
}

impl RedoRecord {
    /// Serialize to a zerompk MessagePack payload for WAL append.
    pub fn to_bytes(&self) -> crate::Result<Vec<u8>> {
        zerompk::to_msgpack_vec(self).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("redo record encode: {e}"),
        })
    }

    /// Deserialize from a zerompk MessagePack payload read from the WAL.
    pub fn from_bytes(bytes: &[u8]) -> crate::Result<Self> {
        zerompk::from_msgpack(bytes).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("redo record decode: {e}"),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_ops() -> Vec<RedoSubRecord> {
        vec![
            RedoSubRecord {
                record_type: nodedb_wal::record::RecordType::Put as u32,
                payload: vec![1, 2, 3, 4],
            },
            RedoSubRecord {
                record_type: nodedb_wal::record::RecordType::VectorPut as u32,
                payload: vec![9, 8, 7],
            },
        ]
    }

    #[test]
    fn roundtrip_without_calvin_stamp() {
        let record = RedoRecord {
            version: 1,
            ops: sample_ops(),
            calvin_stamp: None,
        };
        let bytes = record.to_bytes().expect("encode");
        let decoded = RedoRecord::from_bytes(&bytes).expect("decode");
        assert_eq!(decoded, record);
        assert!(decoded.calvin_stamp.is_none());
    }

    #[test]
    fn roundtrip_with_calvin_stamp() {
        let record = RedoRecord {
            version: 1,
            ops: sample_ops(),
            calvin_stamp: Some(CalvinStamp {
                epoch: 42,
                position: 7,
                vshard_id: 3,
                collections: Vec::new(),
                sum_targets: Vec::new(),
            }),
        };
        let bytes = record.to_bytes().expect("encode");
        let decoded = RedoRecord::from_bytes(&bytes).expect("decode");
        assert_eq!(decoded, record);
        let stamp = decoded.calvin_stamp.expect("stamp present");
        assert_eq!(stamp.epoch, 42);
        assert_eq!(stamp.position, 7);
        assert_eq!(stamp.vshard_id, 3);
    }

    /// A record serialized before `calvin_stamp` existed decodes with the field
    /// defaulted to `None`. Mirrors the legacy-bytes test style for `TxClass`.
    #[test]
    fn decodes_legacy_bytes_without_calvin_stamp_field() {
        #[derive(Serialize, zerompk::ToMessagePack)]
        #[msgpack(map)]
        struct LegacyRedoRecord {
            version: u16,
            ops: Vec<RedoSubRecord>,
        }

        let legacy = LegacyRedoRecord {
            version: 1,
            ops: sample_ops(),
        };
        let bytes = zerompk::to_msgpack_vec(&legacy).expect("encode legacy");

        let decoded = RedoRecord::from_bytes(&bytes).expect("decode legacy as RedoRecord");
        assert_eq!(decoded.version, 1);
        assert_eq!(decoded.ops, sample_ops());
        assert!(decoded.calvin_stamp.is_none());
    }
}
