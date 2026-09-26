// SPDX-License-Identifier: BUSL-1.1

//! Redo sub-record encoders for restored document rows and graph edges.
//!
//! Each encoder writes the exact payload shape the transaction resolver emits
//! and the replay arms decode, so a restored row installs through the same
//! arm a committed transaction's row does:
//!
//! * document put: `(collection, document_id, value, prov, surrogate)`, plus
//!   `(sys_from_ms, valid_from_ms, valid_until_ms)` for a version of a
//!   `bitemporal=true` collection;
//! * document delete: `(collection, document_id, prov, surrogate, sys_from_ms)`,
//!   a tombstone version of a `bitemporal=true` collection;
//! * edge put and delete: [`EdgePutRedo`] and [`EdgeDeleteRedo`].

use nodedb_types::sync::wire::SyncProvenance;
use nodedb_wal::record::RecordType;

use crate::wal::{EdgeDeleteRedo, EdgePutRedo, RedoSubRecord};

/// The system and valid time a document version was stored at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct VersionStamp {
    pub sys_from_ms: i64,
    pub valid_from_ms: i64,
    pub valid_until_ms: i64,
}

fn encode_error(what: &str, e: impl std::fmt::Display) -> crate::Error {
    crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("restore redo: encode {what}: {e}"),
    }
}

/// A document put. `value` is the row as MessagePack. `stamp` is `Some` for a
/// version of a `bitemporal=true` collection, which installs at that exact
/// system time.
pub(super) fn document_put(
    collection: &str,
    document_id: &str,
    value: Vec<u8>,
    surrogate: u32,
    stamp: Option<VersionStamp>,
) -> crate::Result<RedoSubRecord> {
    let prov: Option<SyncProvenance> = None;
    let payload = match stamp {
        Some(s) => zerompk::to_msgpack_vec(&(
            collection,
            document_id,
            value,
            prov,
            surrogate,
            s.sys_from_ms,
            s.valid_from_ms,
            s.valid_until_ms,
        )),
        None => zerompk::to_msgpack_vec(&(collection, document_id, value, prov, surrogate)),
    }
    .map_err(|e| encode_error("document put", e))?;
    Ok(RedoSubRecord {
        record_type: RecordType::Put as u32,
        payload,
    })
}

/// The tombstone version of a `bitemporal=true` document at `sys_from_ms`.
pub(super) fn document_tombstone(
    collection: &str,
    document_id: &str,
    surrogate: u32,
    sys_from_ms: i64,
) -> crate::Result<RedoSubRecord> {
    let prov: Option<SyncProvenance> = None;
    let payload = zerompk::to_msgpack_vec(&(collection, document_id, prov, surrogate, sys_from_ms))
        .map_err(|e| encode_error("document tombstone", e))?;
    Ok(RedoSubRecord {
        record_type: RecordType::Delete as u32,
        payload,
    })
}

/// One edge version put at its original `system_from`.
pub(super) fn edge_put(put: &EdgePutRedo) -> crate::Result<RedoSubRecord> {
    Ok(RedoSubRecord {
        record_type: RecordType::Put as u32,
        payload: zerompk::to_msgpack_vec(put).map_err(|e| encode_error("edge put", e))?,
    })
}

/// One edge tombstone at its original `system_from`.
pub(super) fn edge_delete(delete: &EdgeDeleteRedo) -> crate::Result<RedoSubRecord> {
    Ok(RedoSubRecord {
        record_type: RecordType::Delete as u32,
        payload: zerompk::to_msgpack_vec(delete).map_err(|e| encode_error("edge delete", e))?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    type BitemporalPut = (
        String,
        String,
        Vec<u8>,
        Option<SyncProvenance>,
        u32,
        i64,
        i64,
        i64,
    );
    type PlainPut = (String, String, Vec<u8>, Option<SyncProvenance>, u32);
    type BitemporalDelete = (String, String, Option<SyncProvenance>, u32, i64);

    #[test]
    fn a_current_row_encodes_the_plain_put_shape() {
        let sub = document_put("users", "u1", vec![0x80], 7, None).unwrap();
        assert_eq!(sub.record_type, RecordType::Put as u32);
        let (collection, id, value, prov, surrogate): PlainPut =
            zerompk::from_msgpack(&sub.payload).unwrap();
        assert_eq!(
            (collection.as_str(), id.as_str(), value, surrogate),
            ("users", "u1", vec![0x80], 7)
        );
        assert!(prov.is_none());
        assert!(zerompk::from_msgpack::<BitemporalPut>(&sub.payload).is_err());
    }

    #[test]
    fn a_version_keeps_its_stamp() {
        let stamp = VersionStamp {
            sys_from_ms: 1_000,
            valid_from_ms: 10,
            valid_until_ms: 20,
        };
        let sub = document_put("ledger", "e1", vec![0x80], 9, Some(stamp)).unwrap();
        let (_, _, _, _, surrogate, sys, vf, vu): BitemporalPut =
            zerompk::from_msgpack(&sub.payload).unwrap();
        assert_eq!((surrogate, sys, vf, vu), (9, 1_000, 10, 20));

        let tomb = document_tombstone("ledger", "e1", 9, 2_000).unwrap();
        assert_eq!(tomb.record_type, RecordType::Delete as u32);
        let (_, id, _, surrogate, sys): BitemporalDelete =
            zerompk::from_msgpack(&tomb.payload).unwrap();
        assert_eq!((id.as_str(), surrogate, sys), ("e1", 9, 2_000));
    }
}
