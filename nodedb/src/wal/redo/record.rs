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
    /// The cross-shard trigger request this record applies. Set on the
    /// receiver's commit, so the request's dedup key is durable in the same
    /// record as its writes, and WAL replay restores the key with them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[msgpack(default)]
    pub cross_shard_applied: Option<CrossShardAppliedKey>,
    /// Rows whose writes ran with another source than the record's: the
    /// BEFORE and SYNC AFTER bodies a client statement fired. See
    /// [`super::row_sources`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[msgpack(default)]
    pub row_sources: Vec<super::row_sources::RedoRowSource>,
    /// The `PUBLISH TO` messages the transaction sent. They commit with the
    /// record: the install emits one event per message, WAL replay rebuilds
    /// the same events, and each topic appends each message once.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[msgpack(default)]
    pub publishes: Vec<RedoPublish>,
    /// The net change of every row the record writes, as its change events
    /// name them. See [`super::row_changes`]. The Control-Plane change stream
    /// publishes a committed record's events from these entries alone. Empty
    /// for a record that changes no row a change stream names: an internal
    /// flush, recovery or post-apply record.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[msgpack(default)]
    pub row_changes: Vec<super::row_changes::RedoRowChange>,
}

/// One `PUBLISH TO` message a committed transaction owes its topic.
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
pub struct RedoPublish {
    /// The body that published it, as its retry and DLQ records name it.
    pub owner: String,
    pub database_id: u64,
    pub tenant_id: u64,
    pub topic: String,
    pub payload: String,
    /// The publisher's metadata floor when it found the topic: at or above
    /// the index of the entry that created the topic. A node that does not
    /// know the topic waits until its own metadata apply reaches this index
    /// before it treats the topic as dropped. `0` on a node without a
    /// metadata group.
    #[serde(default)]
    #[msgpack(default)]
    pub metadata_floor: u64,
    /// Where the record that carries the message sits in its replicated log.
    /// The apply that installs the record stamps it, so every replica names
    /// the message alike without a lookup. `None` on a node without Raft,
    /// where the record's local LSN names it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[msgpack(default)]
    pub position: Option<PublishPosition>,
}

/// The replicated position of the record that carries a committed message:
/// its change-feed partition (the vShard), and its data-group entry's
/// `(epoch, log index)`.
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
#[msgpack(map)]
pub struct PublishPosition {
    pub partition: u32,
    pub epoch: u64,
    pub index: u64,
}

/// Identity of one cross-shard trigger request: the source write's position
/// and the body that emitted it. Stable across every re-send of the request.
#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    zerompk::ToMessagePack,
    zerompk::FromMessagePack,
)]
#[msgpack(map)]
pub struct CrossShardAppliedKey {
    pub source_vshard: u32,
    pub source_lsn: u64,
    pub source_sequence: u64,
    pub origin: String,
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

/// Calvin sequencer coordinates a [`RedoRecord`] carries when it installs a
/// committed Calvin slice. The record doubles as the position's applied
/// marker: boot recovery reads the stamp.
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
}

/// Payload of a graph edge upsert: the bytes of an autocommit
/// [`RecordType::Put`](nodedb_wal::record::RecordType::Put) edge record and of
/// a `Put` sub-record inside a graph `RedoRecord`. Both endpoint surrogates are
/// bound, never `Surrogate::ZERO`. [`Self::endpoints`] refuses an unbound one.
///
/// One shared definition serves every encode and decode site, so the field
/// set is a compile-time invariant. Map-encoded (`#[msgpack(map)]`), keying
/// fields by name, the same idiom [`RedoRecord`] uses. A positional tuple or
/// any other shape does not decode as an edge record.
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
    /// replay. `None` on an autocommit record, which installs at its live
    /// ordinal.
    #[serde(default)]
    #[msgpack(default)]
    pub system_from: Option<i64>,
    /// The ordinal the version is applied at, when it differs from
    /// `system_from`: a restored version keeps its historical `system_from`
    /// and is applied at the restore transaction's ordinal. A TRUNCATE cut
    /// compares against it. `None` for every other version.
    #[serde(default)]
    #[msgpack(default)]
    pub applied: Option<i64>,
}

/// Payload of a graph edge delete: the bytes of an autocommit
/// [`RecordType::Delete`](nodedb_wal::record::RecordType::Delete) edge record
/// and of a `Delete` sub-record inside a graph `RedoRecord`. It carries both
/// endpoint surrogates like [`EdgePutRedo`], and neither is `Surrogate::ZERO`.
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
    pub src_surrogate: u32,
    pub dst_surrogate: u32,
    /// Frozen bitemporal `system_from` ordinal for deterministic cross-replica
    /// replay. `None` on an autocommit record, which installs at its live
    /// ordinal.
    #[serde(default)]
    #[msgpack(default)]
    pub system_from: Option<i64>,
    /// The ordinal the tombstone is applied at, when it differs from
    /// `system_from`. See [`EdgePutRedo::applied`].
    #[serde(default)]
    #[msgpack(default)]
    pub applied: Option<i64>,
}

/// Payload of a `GraphEdgeCut` sub-record: one TRUNCATE share's cut of an
/// edge collection. It writes no edge version. Every read hides the versions
/// of `collection` applied below `cut`.
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
pub struct EdgeCutRedo {
    /// The collection, as the Data Plane stores it.
    pub collection: String,
    /// The ordinal of the TRUNCATE's Calvin transaction.
    pub cut: i64,
}

/// Both endpoint surrogates of an edge record, or `None` when either is
/// `Surrogate::ZERO`. An edge record without both identities is refused.
fn bound_endpoints(
    src_surrogate: u32,
    dst_surrogate: u32,
) -> Option<(nodedb_types::Surrogate, nodedb_types::Surrogate)> {
    let src = nodedb_types::Surrogate::new(src_surrogate);
    let dst = nodedb_types::Surrogate::new(dst_surrogate);
    (src != nodedb_types::Surrogate::ZERO && dst != nodedb_types::Surrogate::ZERO)
        .then_some((src, dst))
}

impl EdgePutRedo {
    /// The bound `(src, dst)` surrogates, or `None` when either is unbound.
    pub fn endpoints(&self) -> Option<(nodedb_types::Surrogate, nodedb_types::Surrogate)> {
        bound_endpoints(self.src_surrogate, self.dst_surrogate)
    }
}

impl EdgeDeleteRedo {
    /// The bound `(src, dst)` surrogates, or `None` when either is unbound.
    pub fn endpoints(&self) -> Option<(nodedb_types::Surrogate, nodedb_types::Surrogate)> {
        bound_endpoints(self.src_surrogate, self.dst_surrogate)
    }
}

impl RedoPublish {
    /// Stamp `position` on every message of `publishes`.
    pub fn stamp_all(publishes: &mut [Self], position: PublishPosition) {
        for publish in publishes {
            publish.position = Some(position);
        }
    }

    /// The opaque bytes a Calvin transaction class carries its messages in.
    /// Empty when there are none.
    pub fn encode_all(publishes: &[Self]) -> crate::Result<Vec<u8>> {
        if publishes.is_empty() {
            return Ok(Vec::new());
        }
        zerompk::to_msgpack_vec(&publishes.to_vec()).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("redo publishes encode: {e}"),
        })
    }

    /// The messages [`Self::encode_all`] wrote. Empty bytes hold none.
    pub fn decode_all(bytes: &[u8]) -> crate::Result<Vec<Self>> {
        if bytes.is_empty() {
            return Ok(Vec::new());
        }
        zerompk::from_msgpack(bytes).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("redo publishes decode: {e}"),
        })
    }
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
            cross_shard_applied: None,
            row_sources: Vec::new(),
            publishes: Vec::new(),
            row_changes: Vec::new(),
        };
        let bytes = record.to_bytes().expect("encode");
        let decoded = RedoRecord::from_bytes(&bytes).expect("decode");
        assert_eq!(decoded, record);
        assert!(decoded.calvin_stamp.is_none());
    }

    #[test]
    fn roundtrip_with_publishes() {
        let record = RedoRecord {
            version: 1,
            ops: Vec::new(),
            calvin_stamp: None,
            cross_shard_applied: None,
            row_sources: Vec::new(),
            publishes: vec![RedoPublish {
                owner: "trigger/1/audit".into(),
                database_id: 1,
                tenant_id: 1,
                topic: "orders_feed".into(),
                payload: "created".into(),
                metadata_floor: 12,
                position: Some(PublishPosition {
                    partition: 3,
                    epoch: 0,
                    index: 41,
                }),
            }],
            row_changes: Vec::new(),
        };
        let bytes = record.to_bytes().expect("encode");
        assert_eq!(RedoRecord::from_bytes(&bytes).expect("decode"), record);
        let carried = RedoPublish::encode_all(&record.publishes).expect("encode publishes");
        assert_eq!(
            RedoPublish::decode_all(&carried).expect("decode publishes"),
            record.publishes
        );
        assert!(
            RedoPublish::encode_all(&[])
                .expect("encode none")
                .is_empty()
        );
        assert!(
            RedoPublish::decode_all(&[])
                .expect("decode none")
                .is_empty()
        );
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
            }),
            cross_shard_applied: None,
            row_sources: Vec::new(),
            publishes: Vec::new(),
            row_changes: Vec::new(),
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
        assert!(decoded.cross_shard_applied.is_none());
        assert!(decoded.row_sources.is_empty());
        assert!(decoded.publishes.is_empty());
        assert!(decoded.row_changes.is_empty());
    }

    #[test]
    fn roundtrip_with_row_sources() {
        let record = RedoRecord {
            version: 1,
            ops: sample_ops(),
            calvin_stamp: None,
            cross_shard_applied: None,
            row_sources: vec![super::super::row_sources::RedoRowSource {
                collection: "orders".into(),
                event_source: crate::event::EventSource::Trigger.wal_code(),
                rows: vec!["o-body".into()],
            }],
            publishes: Vec::new(),
            row_changes: Vec::new(),
        };
        let bytes = record.to_bytes().expect("encode");
        assert_eq!(RedoRecord::from_bytes(&bytes).expect("decode"), record);
    }

    #[test]
    fn roundtrip_with_cross_shard_applied_key() {
        let record = RedoRecord {
            version: 1,
            ops: sample_ops(),
            calvin_stamp: None,
            cross_shard_applied: Some(CrossShardAppliedKey {
                source_vshard: 3,
                source_lsn: 100,
                source_sequence: 7,
                origin: "trigger/1/audit".into(),
            }),
            row_sources: Vec::new(),
            publishes: Vec::new(),
            row_changes: Vec::new(),
        };
        let bytes = record.to_bytes().expect("encode");
        assert_eq!(RedoRecord::from_bytes(&bytes).expect("decode"), record);
    }
}
