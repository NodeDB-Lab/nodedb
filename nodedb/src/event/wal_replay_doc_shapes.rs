// SPDX-License-Identifier: BUSL-1.1

//! Decoders for the surrogate-carrying document `Put` / `Delete` payloads.
//!
//! A stamped record appends its version key to the plain tuple:
//!
//! * a put is `(collection, document_id, value, provenance, surrogate)`, or
//!   that tuple plus `(sys_from, valid_from_ms, valid_until_ms)`;
//! * a delete is `(collection, document_id, provenance, surrogate)`, or that
//!   tuple plus `sys_from`.
//!
//! `sys_from` is the version's system-time ordinal. An event carries its
//! millisecond, the unit `_ts_system` renders.
//!
//! `crate::wal::DocumentPutRedo` / `crate::wal::DocumentDeleteRedo` decode
//! both forms. This module maps them to the event view.

use crate::wal::{DocumentDeleteRedo, DocumentPutRedo};

/// A decoded document put.
pub(super) struct DocPut {
    pub collection: String,
    pub document_id: String,
    pub value: Vec<u8>,
    /// `(system_time_ms, valid_time_ms)` the record carries: `Some` system
    /// time for a bitemporal record, the millisecond of its system-time
    /// ordinal, `None` for a plain one. A valid time of `i64::MIN` means
    /// unbounded and reads as `None`.
    pub stamps: Option<(i64, Option<i64>)>,
}

/// Decode a surrogate-carrying document put in either form.
pub(super) fn decode_doc_put(payload: &[u8]) -> Option<DocPut> {
    DocumentPutRedo::decode(payload).map(|put| DocPut {
        collection: put.collection,
        document_id: put.document_id,
        value: put.value,
        stamps: put.stamp.map(|stamp| {
            (
                nodedb_types::ordinal_to_ms(stamp.sys_from),
                (stamp.valid_from_ms != i64::MIN).then_some(stamp.valid_from_ms),
            )
        }),
    })
}

/// Decode a surrogate-carrying document delete in either form, as
/// `(collection, document_id)`.
pub(super) fn decode_doc_delete(payload: &[u8]) -> Option<(String, String)> {
    DocumentDeleteRedo::decode(payload).map(|delete| (delete.collection, delete.document_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_types::sync::wire::SyncProvenance;

    #[test]
    fn a_bitemporal_put_decodes_with_its_stamp() {
        let prov: Option<SyncProvenance> = None;
        // An ordinal inside millisecond 10 reads as millisecond 10.
        let ordinal = 10 * nodedb_types::NANOS_PER_MS + 42;
        let payload = zerompk::to_msgpack_vec(&(
            "c",
            "d1",
            vec![1u8],
            prov,
            7u32,
            ordinal,
            i64::MIN,
            i64::MAX,
        ))
        .expect("encode");
        let put = decode_doc_put(&payload).expect("decodes");
        assert_eq!(put.collection, "c");
        assert_eq!(put.document_id, "d1");
        assert_eq!(put.value, vec![1u8]);
        assert_eq!(put.stamps, Some((10, None)));
    }

    #[test]
    fn a_plain_put_decodes_without_a_stamp() {
        let prov: Option<SyncProvenance> = None;
        let payload = zerompk::to_msgpack_vec(&("c", "d1", vec![1u8], prov, 7u32)).expect("encode");
        let put = decode_doc_put(&payload).expect("decodes");
        assert_eq!(put.document_id, "d1");
        assert_eq!(put.stamps, None);
    }

    #[test]
    fn both_delete_forms_decode() {
        let none: Option<SyncProvenance> = None;
        let plain = zerompk::to_msgpack_vec(&("c", "d1", none, 7u32)).expect("encode");
        let none: Option<SyncProvenance> = None;
        let bitemporal = zerompk::to_msgpack_vec(&("c", "d2", none, 7u32, 10i64)).expect("encode");
        assert_eq!(
            decode_doc_delete(&plain),
            Some(("c".to_owned(), "d1".to_owned()))
        );
        assert_eq!(
            decode_doc_delete(&bitemporal),
            Some(("c".to_owned(), "d2".to_owned()))
        );
    }
}
