// SPDX-License-Identifier: BUSL-1.1

//! The stored write set of a journalled write.
//!
//! A Data Plane core stores it beside the write's effects, in the transaction
//! that persists them (see `crate::engine::durability_gate`). Boot journals a
//! stored write set whose group the WAL lacks as that group's parts, exactly
//! as the Control Plane journals a response's write set. When a crash cut the
//! write's origin from the WAL, boot first journals the origin again from
//! the stored [`OriginAppend`].

use serde::{Deserialize, Serialize};

use super::record::{EdgeDeleteRedo, EdgePutRedo};
use crate::bridge::envelope::{EdgeImage, RowEffect, RowVersion, WriteSetEntry};

/// One write's stored write set and the group it belongs to.
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
pub struct WriteSetCapture {
    pub tenant_id: u64,
    pub vshard_id: u32,
    pub database_id: u64,
    /// The collection of every entry without its own.
    pub collection: String,
    /// The group origin LSN.
    pub origin: u64,
    pub entries: Vec<CapturedEntry>,
    pub origin_append: OriginAppend,
}

/// What the write appended before dispatch, in the shape the funnel appends
/// it from.
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
pub struct OriginAppend {
    /// The dispatched plan, encoded as MessagePack.
    pub plan: Vec<u8>,
    /// The idempotency key every record of the write carries.
    pub apply_key: u64,
    /// The WAL code of the write's event source.
    pub event_source: u8,
    /// The commit instant every record carries, when it was known before the
    /// append.
    pub commit_hlc: Option<u64>,
    /// The instant the forward record resolved.
    pub resolved_now_ms: Option<u64>,
    /// `(epoch, group_id, log_index)` of the replicated position whose marker
    /// precedes the origin.
    pub change_position: Option<(u64, u64, u64)>,
}

/// One stored [`WriteSetEntry`].
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
pub struct CapturedEntry {
    pub surrogate: u32,
    pub identity: String,
    pub collection: Option<String>,
    pub effect: CapturedEffect,
}

/// One stored [`RowEffect`].
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
pub enum CapturedEffect {
    Put {
        value: Vec<u8>,
        /// `(sys_from, valid_from, valid_until)`: the system-time ordinal,
        /// then the valid-time bounds in milliseconds.
        version: Option<(i64, i64, i64)>,
    },
    Delete {
        /// The tombstone's system-time ordinal.
        system_from: Option<i64>,
    },
    CancelForward,
    EdgePut(EdgePutRedo),
    EdgeDelete(EdgeDeleteRedo),
}

impl CapturedEntry {
    pub fn from_entry(entry: &WriteSetEntry) -> Self {
        let effect = match &entry.effect {
            RowEffect::Put { value, version } => CapturedEffect::Put {
                value: value.clone(),
                version: version.map(|v| (v.sys_from, v.valid_from_ms, v.valid_until_ms)),
            },
            RowEffect::Delete { system_from } => CapturedEffect::Delete {
                system_from: *system_from,
            },
            RowEffect::CancelForward => CapturedEffect::CancelForward,
            RowEffect::Edge(EdgeImage::Put(put)) => CapturedEffect::EdgePut(put.clone()),
            RowEffect::Edge(EdgeImage::Delete(delete)) => {
                CapturedEffect::EdgeDelete(delete.clone())
            }
        };
        Self {
            surrogate: entry.surrogate,
            identity: entry.identity.as_str().to_string(),
            collection: entry.collection.clone(),
            effect,
        }
    }

    pub fn into_entry(self) -> WriteSetEntry {
        let effect = match self.effect {
            CapturedEffect::Put { value, version } => RowEffect::Put {
                value,
                version: version.map(|(sys_from, valid_from_ms, valid_until_ms)| RowVersion {
                    sys_from,
                    valid_from_ms,
                    valid_until_ms,
                }),
            },
            CapturedEffect::Delete { system_from } => RowEffect::Delete { system_from },
            CapturedEffect::CancelForward => RowEffect::CancelForward,
            CapturedEffect::EdgePut(put) => RowEffect::Edge(EdgeImage::Put(put)),
            CapturedEffect::EdgeDelete(delete) => RowEffect::Edge(EdgeImage::Delete(delete)),
        };
        WriteSetEntry {
            surrogate: self.surrogate,
            identity: nodedb_types::RowIdentity::from_user_key(self.identity),
            effect,
            collection: self.collection,
        }
    }
}

impl WriteSetCapture {
    pub fn to_bytes(&self) -> crate::Result<Vec<u8>> {
        zerompk::to_msgpack_vec(self).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("write set capture encode: {e}"),
        })
    }

    pub fn from_bytes(bytes: &[u8]) -> crate::Result<Self> {
        zerompk::from_msgpack(bytes).map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("write set capture decode: {e}"),
        })
    }

    /// The write set as the response carried it.
    pub fn write_set(&self) -> Vec<WriteSetEntry> {
        self.entries
            .iter()
            .cloned()
            .map(CapturedEntry::into_entry)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_capture_round_trips_every_effect() {
        let identity = nodedb_types::RowIdentity::from_user_key("r1");
        let entries = vec![
            WriteSetEntry::put(1, identity.clone(), b"v".to_vec())
                .versioned(Some(RowVersion::open(40))),
            WriteSetEntry::put(2, identity.clone(), b"w".to_vec())
                .in_collection("other".to_string()),
            WriteSetEntry::cancel_forward(1, identity),
            WriteSetEntry::edge(EdgeImage::Delete(EdgeDeleteRedo {
                collection: "g".into(),
                src_id: "n".into(),
                label: "L".into(),
                dst_id: "m".into(),
                src_surrogate: 3,
                dst_surrogate: 4,
                system_from: Some(9),
                applied: None,
            })),
        ];
        let capture = WriteSetCapture {
            tenant_id: 1,
            vshard_id: 2,
            database_id: 0,
            collection: "orders".into(),
            origin: 77,
            entries: entries.iter().map(CapturedEntry::from_entry).collect(),
            origin_append: OriginAppend {
                plan: vec![0x90],
                apply_key: 5,
                event_source: crate::event::EventSource::User.wal_code(),
                commit_hlc: Some(11),
                resolved_now_ms: None,
                change_position: Some((1, 2, 3)),
            },
        };
        let decoded = WriteSetCapture::from_bytes(&capture.to_bytes().unwrap()).unwrap();
        assert_eq!(decoded, capture);
        let restored = decoded.write_set();
        assert_eq!(restored.len(), entries.len());
        for (restored, original) in restored.iter().zip(&entries) {
            assert_eq!(restored.effect, original.effect);
            assert_eq!(restored.identity, original.identity);
            assert_eq!(restored.collection, original.collection);
            assert_eq!(restored.surrogate, original.surrogate);
        }
    }
}
