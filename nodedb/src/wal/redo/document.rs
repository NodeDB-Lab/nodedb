// SPDX-License-Identifier: BUSL-1.1

//! The decoders of the document `Put` / `Delete` redo sub-record payloads.
//!
//! Every producer writes one of two shapes per kind:
//!
//! * a put is `(collection, document_id, value, provenance, surrogate)`, or
//!   that tuple plus `(sys_from, valid_from_ms, valid_until_ms)`, or the
//!   stamped tuple plus a trailing `true` for a clone copy;
//! * a delete is `(collection, document_id, provenance, surrogate)`, or that
//!   tuple plus `sys_from`.
//!
//! The stamped form carries the version key the live write decided. Replay
//! installs the row at that exact key. Every write-set image, every
//! transaction redo and every RESTORE row is stamped.
//!
//! The unstamped form is the pre-dispatch record of a point put, insert or
//! delete, and the opening forward record of its write group
//! (`wal_dispatch::document`). Its apply decides the version key, and the
//! stamped write-set image it journals after apply cancels the unstamped
//! record. Restart replay skips an unstamped record, and a committed-redo
//! install refuses one.
//!
//! MessagePack tuples decode only at their exact arity, so each decoder tries
//! the stamped form, then the unstamped one. A KV or graph payload decodes as
//! neither.

use nodedb_types::sync::wire::SyncProvenance;

/// The stamped put tuple.
pub(super) type StampedPut = (
    String,
    String,
    Vec<u8>,
    Option<SyncProvenance>,
    u32,
    i64,
    i64,
    i64,
);
/// The stamped put tuple of a clone copy: the stamped tuple and a trailing
/// `true`. A clone copy installs only where its row holds no version.
type CopyPut = (
    String,
    String,
    Vec<u8>,
    Option<SyncProvenance>,
    u32,
    i64,
    i64,
    i64,
    bool,
);
/// The unstamped put tuple.
type PlainPut = (String, String, Vec<u8>, Option<SyncProvenance>, u32);
/// The stamped delete tuple.
pub(super) type StampedDelete = (String, String, Option<SyncProvenance>, u32, i64);
/// The unstamped delete tuple.
type PlainDelete = (String, String, Option<SyncProvenance>, u32);

/// The version key a stamped put carries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DocumentPutStamp {
    /// The system-time ordinal of the version.
    pub sys_from: i64,
    /// The valid-time lower bound. `i64::MIN` is unbounded.
    pub valid_from_ms: i64,
    /// The valid-time upper bound. `i64::MAX` is unbounded.
    pub valid_until_ms: i64,
}

/// A decoded document put sub-record.
#[derive(Debug, Clone, PartialEq)]
pub struct DocumentPutRedo {
    pub collection: String,
    /// The row's client identity.
    pub document_id: String,
    /// The MessagePack body the write path encodes into storage.
    pub value: Vec<u8>,
    pub provenance: Option<SyncProvenance>,
    /// The row's stable global surrogate.
    pub surrogate: u32,
    /// The version key of a stamped put. `None` for an unstamped one.
    pub stamp: Option<DocumentPutStamp>,
    /// A clone copy of a source row. It installs only where the row holds
    /// no version, live or deleted, so a write into the clone always wins
    /// over a copy.
    pub clone_copy: bool,
}

impl DocumentPutRedo {
    /// Decode `payload` in any form. `None` when it is no document put.
    pub fn decode(payload: &[u8]) -> Option<Self> {
        if let Ok((collection, document_id, value, provenance, surrogate, sys, vf, vu, copy)) =
            zerompk::from_msgpack::<CopyPut>(payload)
        {
            return Some(Self {
                collection,
                document_id,
                value,
                provenance,
                surrogate,
                stamp: Some(DocumentPutStamp {
                    sys_from: sys,
                    valid_from_ms: vf,
                    valid_until_ms: vu,
                }),
                clone_copy: copy,
            });
        }
        if let Ok((collection, document_id, value, provenance, surrogate, sys, vf, vu)) =
            zerompk::from_msgpack::<StampedPut>(payload)
        {
            return Some(Self {
                collection,
                document_id,
                value,
                provenance,
                surrogate,
                stamp: Some(DocumentPutStamp {
                    sys_from: sys,
                    valid_from_ms: vf,
                    valid_until_ms: vu,
                }),
                clone_copy: false,
            });
        }
        zerompk::from_msgpack::<PlainPut>(payload).ok().map(
            |(collection, document_id, value, provenance, surrogate)| Self {
                collection,
                document_id,
                value,
                provenance,
                surrogate,
                stamp: None,
                clone_copy: false,
            },
        )
    }
}

/// A decoded document delete sub-record.
#[derive(Debug, Clone, PartialEq)]
pub struct DocumentDeleteRedo {
    pub collection: String,
    /// The row's client identity.
    pub document_id: String,
    pub provenance: Option<SyncProvenance>,
    /// The row's stable global surrogate.
    pub surrogate: u32,
    /// The system-time ordinal of a stamped tombstone. `None` for an
    /// unstamped one.
    pub sys_from: Option<i64>,
}

impl DocumentDeleteRedo {
    /// Decode `payload` in either form. `None` when it is no document delete.
    pub fn decode(payload: &[u8]) -> Option<Self> {
        if let Ok((collection, document_id, provenance, surrogate, sys)) =
            zerompk::from_msgpack::<StampedDelete>(payload)
        {
            return Some(Self {
                collection,
                document_id,
                provenance,
                surrogate,
                sys_from: Some(sys),
            });
        }
        zerompk::from_msgpack::<PlainDelete>(payload).ok().map(
            |(collection, document_id, provenance, surrogate)| Self {
                collection,
                document_id,
                provenance,
                surrogate,
                sys_from: None,
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_stamped_put_decodes_with_its_version_key() {
        let prov: Option<SyncProvenance> = None;
        let payload =
            zerompk::to_msgpack_vec(&("c", "d1", vec![1u8], prov, 7u32, 42i64, i64::MIN, 9i64))
                .expect("encode");
        let put = DocumentPutRedo::decode(&payload).expect("decodes");
        assert_eq!(put.collection, "c");
        assert_eq!(put.document_id, "d1");
        assert_eq!(put.value, vec![1u8]);
        assert_eq!(put.surrogate, 7);
        assert_eq!(
            put.stamp,
            Some(DocumentPutStamp {
                sys_from: 42,
                valid_from_ms: i64::MIN,
                valid_until_ms: 9,
            })
        );
    }

    #[test]
    fn a_clone_copy_put_decodes_as_a_copy_with_its_version_key() {
        let prov: Option<SyncProvenance> = None;
        let payload = zerompk::to_msgpack_vec(&(
            "c",
            "d1",
            vec![1u8],
            prov,
            7u32,
            42i64,
            i64::MIN,
            i64::MAX,
            true,
        ))
        .expect("encode");
        let put = DocumentPutRedo::decode(&payload).expect("decodes");
        assert!(put.clone_copy);
        assert_eq!(put.stamp.map(|s| s.sys_from), Some(42));
        let plain = zerompk::to_msgpack_vec(&(
            "c",
            "d1",
            vec![1u8],
            None::<SyncProvenance>,
            7u32,
            42i64,
            0i64,
            1i64,
        ))
        .expect("encode");
        assert!(!DocumentPutRedo::decode(&plain).expect("decodes").clone_copy);
    }

    #[test]
    fn an_unstamped_put_decodes_without_a_version_key() {
        let prov: Option<SyncProvenance> = None;
        let payload = zerompk::to_msgpack_vec(&("c", "d1", vec![1u8], prov, 7u32)).expect("encode");
        let put = DocumentPutRedo::decode(&payload).expect("decodes");
        assert_eq!(put.surrogate, 7);
        assert_eq!(put.stamp, None);
    }

    #[test]
    fn both_delete_forms_decode() {
        let none: Option<SyncProvenance> = None;
        let plain = zerompk::to_msgpack_vec(&("c", "d1", none, 7u32)).expect("encode");
        let none: Option<SyncProvenance> = None;
        let stamped = zerompk::to_msgpack_vec(&("c", "d2", none, 8u32, 10i64)).expect("encode");
        let plain = DocumentDeleteRedo::decode(&plain).expect("plain decodes");
        assert_eq!((plain.document_id.as_str(), plain.surrogate), ("d1", 7));
        assert_eq!(plain.sys_from, None);
        let stamped = DocumentDeleteRedo::decode(&stamped).expect("stamped decodes");
        assert_eq!((stamped.document_id.as_str(), stamped.surrogate), ("d2", 8));
        assert_eq!(stamped.sys_from, Some(10));
    }

    #[test]
    fn a_put_and_a_delete_never_decode_as_each_other() {
        let none: Option<SyncProvenance> = None;
        let delete = zerompk::to_msgpack_vec(&("c", "d1", none, 7u32)).expect("encode");
        assert!(DocumentPutRedo::decode(&delete).is_none());
        let none: Option<SyncProvenance> = None;
        let put = zerompk::to_msgpack_vec(&("c", "d1", vec![1u8], none, 7u32)).expect("encode");
        assert!(DocumentDeleteRedo::decode(&put).is_none());
    }
}
