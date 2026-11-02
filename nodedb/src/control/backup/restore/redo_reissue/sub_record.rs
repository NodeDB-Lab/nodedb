// SPDX-License-Identifier: BUSL-1.1

//! Redo sub-record encoders for restored document rows.
//!
//! Each encoder writes the exact payload shape the transaction resolver emits
//! and the replay arms decode, so a restored row installs through the same
//! arm a committed transaction's row does:
//!
//! * document put: `(collection, document_id, value, prov, surrogate,
//!   sys_from, valid_from_ms, valid_until_ms)`, one version of a row;
//! * document delete: `(collection, document_id, prov, surrogate, sys_from)`,
//!   a tombstone version of a row.
//!
//! A restored edge version travels typed, and its transaction's resolve
//! encodes it with the transaction's ordinal as its applied ordinal.

use nodedb_types::sync::wire::SyncProvenance;
use nodedb_wal::record::RecordType;

use crate::wal::RedoSubRecord;

/// The system-time ordinal and valid time a document version was stored at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct VersionStamp {
    pub sys_from: i64,
    pub valid_from_ms: i64,
    pub valid_until_ms: i64,
}

fn encode_error(what: &str, e: impl std::fmt::Display) -> crate::Error {
    crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("restore redo: encode {what}: {e}"),
    }
}

/// One version of a document. `value` is the row as MessagePack. The
/// version installs at the exact system time `stamp` carries.
pub(super) fn document_put(
    collection: &str,
    document_id: &str,
    value: Vec<u8>,
    surrogate: u32,
    stamp: VersionStamp,
) -> crate::Result<RedoSubRecord> {
    let prov: Option<SyncProvenance> = None;
    let payload = zerompk::to_msgpack_vec(&(
        collection,
        document_id,
        value,
        prov,
        surrogate,
        stamp.sys_from,
        stamp.valid_from_ms,
        stamp.valid_until_ms,
    ))
    .map_err(|e| encode_error("document put", e))?;
    Ok(RedoSubRecord {
        record_type: RecordType::Put as u32,
        payload,
    })
}

/// The tombstone version of a document at `sys_from`.
pub(super) fn document_tombstone(
    collection: &str,
    document_id: &str,
    surrogate: u32,
    sys_from: i64,
) -> crate::Result<RedoSubRecord> {
    let prov: Option<SyncProvenance> = None;
    let payload = zerompk::to_msgpack_vec(&(collection, document_id, prov, surrogate, sys_from))
        .map_err(|e| encode_error("document tombstone", e))?;
    Ok(RedoSubRecord {
        record_type: RecordType::Delete as u32,
        payload,
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
    type BitemporalDelete = (String, String, Option<SyncProvenance>, u32, i64);

    #[test]
    fn a_version_keeps_its_stamp() {
        let stamp = VersionStamp {
            sys_from: 1_000,
            valid_from_ms: 10,
            valid_until_ms: 20,
        };
        let sub = document_put("ledger", "e1", vec![0x80], 9, stamp).unwrap();
        assert_eq!(sub.record_type, RecordType::Put as u32);
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
