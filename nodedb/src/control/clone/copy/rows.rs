// SPDX-License-Identifier: BUSL-1.1

//! One copied row as the redo sub-record that installs it.
//!
//! A copy of a source row into a clone is the same logical version as the
//! source row. Its sub-record carries the source version's system-time
//! ordinal, never a fresh one. Every apply of the record (install, replica,
//! restart replay) writes the same version key, so a snapshot that read the
//! row through the source reads the same row from the clone.
//!
//! Each sub-record carries the clone-copy marker: the apply installs it only
//! where the target holds no version of the row, live or deleted. A write
//! into the clone therefore always wins over a copy. A copy changes no row a
//! clone reader sees, so it names no row change.

use nodedb_types::Surrogate;
use nodedb_types::sync::wire::SyncProvenance;
use nodedb_wal::record::RecordType;

use crate::control::surrogate::CarriedIdentity;
use crate::types::{DocVersionStamp, KvVersionStamp};
use crate::wal::{KvRestoreLive, KvRestoreRedo, RedoSubRecord};

/// One document row to copy into a clone.
pub struct DocCopy {
    /// The row's client identity text.
    pub document_id: String,
    /// The primary-key bytes the target surrogate binds under.
    pub pk_bytes: Vec<u8>,
    pub target_surrogate: Surrogate,
    /// The source version's body.
    pub body: Vec<u8>,
    /// The source version's stamps.
    pub stamp: DocVersionStamp,
}

/// One KV key to copy into a clone.
pub struct KvCopy {
    pub key: Vec<u8>,
    pub target_surrogate: Surrogate,
    /// The source version's value.
    pub value: Vec<u8>,
    /// The source version's ordinal and absolute expiry.
    pub stamp: KvVersionStamp,
}

/// The pieces one copied row adds to a redo record.
pub(super) struct CopyRecordPart {
    pub op: RedoSubRecord,
    pub identity: CarriedIdentity,
}

impl DocCopy {
    /// The stamped document put that installs this row in `stored`, the
    /// clone collection as its engine stores it. `bare` is its catalog name.
    pub(super) fn record_part(self, stored: &str, bare: &str) -> crate::Result<CopyRecordPart> {
        let provenance: Option<SyncProvenance> = None;
        let payload = zerompk::to_msgpack_vec(&(
            stored,
            self.document_id.as_str(),
            self.body,
            provenance,
            self.target_surrogate.as_u32(),
            self.stamp.sys_from,
            self.stamp.valid_from_ms,
            self.stamp.valid_until_ms,
            // A clone copy installs only where the row holds no version.
            true,
        ))
        .map_err(|e| crate::Error::Serialization {
            format: "msgpack".into(),
            detail: format!("clone copy: encode document put: {e}"),
        })?;
        Ok(CopyRecordPart {
            op: RedoSubRecord {
                record_type: RecordType::Put as u32,
                payload,
            },
            identity: CarriedIdentity {
                collection: bare.to_string(),
                pk_bytes: self.pk_bytes,
                surrogate: self.target_surrogate,
            },
        })
    }
}

impl KvCopy {
    /// The restored-key sub-record that installs this key's live row in
    /// `stored` at its source ordinal. `bare` is the catalog name.
    pub(super) fn record_part(self, stored: &str, bare: &str) -> crate::Result<CopyRecordPart> {
        let payload = KvRestoreRedo {
            collection: stored.to_string(),
            key: self.key.clone(),
            surrogate: self.target_surrogate.as_u32(),
            live: Some(KvRestoreLive {
                value: self.value,
                expire_at_ms: self.stamp.expire_at_ms,
                sys_from: self.stamp.sys_from,
            }),
            history: Vec::new(),
            reassert_at: None,
            // A clone copy installs only where the key holds no version.
            clone_copy: true,
        }
        .encode()?;
        Ok(CopyRecordPart {
            op: RedoSubRecord {
                record_type: RecordType::Put as u32,
                payload,
            },
            identity: CarriedIdentity {
                collection: bare.to_string(),
                pk_bytes: self.key,
                surrogate: self.target_surrogate,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::DocumentPutRedo;

    /// A copied document row installs at its source version's stamps.
    #[test]
    fn a_document_copy_keeps_the_source_stamp() {
        let stamp = DocVersionStamp {
            sys_from: 1_793_457_000_000_000_007,
            valid_from_ms: 10,
            valid_until_ms: 20,
        };
        let part = DocCopy {
            document_id: "p1".into(),
            pk_bytes: b"p1".to_vec(),
            target_surrogate: Surrogate::new(9),
            body: vec![0x80],
            stamp,
        }
        .record_part("db1:products", "products")
        .expect("record part");
        let put = DocumentPutRedo::decode(&part.op.payload).expect("a document put");
        let carried = put.stamp.expect("a stamped put");
        assert_eq!(
            (
                carried.sys_from,
                carried.valid_from_ms,
                carried.valid_until_ms
            ),
            (stamp.sys_from, stamp.valid_from_ms, stamp.valid_until_ms)
        );
        assert_eq!(put.surrogate, 9);
        assert!(
            put.clone_copy,
            "a copy installs only where the row holds no version"
        );
        assert_eq!(part.identity.surrogate, Surrogate::new(9));
    }

    /// A copied KV key installs its live row at its source ordinal, with its
    /// source expiry.
    #[test]
    fn a_kv_copy_keeps_the_source_stamp() {
        let stamp = KvVersionStamp {
            sys_from: 1_793_457_000_000_000_011,
            expire_at_ms: 77,
        };
        let part = KvCopy {
            key: b"p1".to_vec(),
            target_surrogate: Surrogate::new(4),
            value: b"v".to_vec(),
            stamp,
        }
        .record_part("db1:products", "products")
        .expect("record part");
        let restored = KvRestoreRedo::decode(&part.op.payload).expect("a restored key");
        let live = restored.live.expect("a live row");
        assert_eq!((live.sys_from, live.expire_at_ms), (stamp.sys_from, 77));
        assert_eq!(live.value, b"v");
        assert!(restored.history.is_empty());
        assert_eq!(restored.reassert_at, None);
        assert!(
            restored.clone_copy,
            "a copy installs only where the key holds no version"
        );
    }
}
