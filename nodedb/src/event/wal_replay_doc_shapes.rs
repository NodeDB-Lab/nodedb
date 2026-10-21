// SPDX-License-Identifier: BUSL-1.1

//! Decoders for the surrogate-carrying document `Put` / `Delete` payloads.
//!
//! A `bitemporal=true` collection's record appends its version stamp to the
//! plain tuple:
//!
//! * a put is `(collection, document_id, value, provenance, surrogate)`, or
//!   that tuple plus `(sys_from, valid_from_ms, valid_until_ms)`;
//! * a delete is `(collection, document_id, provenance, surrogate)`, or that
//!   tuple plus `sys_from`.
//!
//! `sys_from` is the version's system-time ordinal. An event carries its
//! millisecond, the unit `_ts_system` renders.
//!
//! MessagePack tuples decode only at their exact arity, so each decoder tries
//! the bitemporal form, then the plain one.

use nodedb_types::sync::wire::SyncProvenance;

/// The bitemporal put tuple.
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
/// The plain put tuple.
type PlainPut = (String, String, Vec<u8>, Option<SyncProvenance>, u32);
/// The bitemporal delete tuple.
type BitemporalDelete = (String, String, Option<SyncProvenance>, u32, i64);
/// The plain delete tuple.
type PlainDelete = (String, String, Option<SyncProvenance>, u32);

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
    if let Ok((collection, document_id, value, _prov, _surrogate, sys, valid_from, _until)) =
        zerompk::from_msgpack::<BitemporalPut>(payload)
    {
        return Some(DocPut {
            collection,
            document_id,
            value,
            stamps: Some((
                nodedb_types::ordinal_to_ms(sys),
                (valid_from != i64::MIN).then_some(valid_from),
            )),
        });
    }
    zerompk::from_msgpack::<PlainPut>(payload).ok().map(
        |(collection, document_id, value, _prov, _surrogate)| DocPut {
            collection,
            document_id,
            value,
            stamps: None,
        },
    )
}

/// Decode a surrogate-carrying document delete in either form, as
/// `(collection, document_id)`.
pub(super) fn decode_doc_delete(payload: &[u8]) -> Option<(String, String)> {
    if let Ok((collection, document_id, _prov, _surrogate, _sys)) =
        zerompk::from_msgpack::<BitemporalDelete>(payload)
    {
        return Some((collection, document_id));
    }
    zerompk::from_msgpack::<PlainDelete>(payload)
        .ok()
        .map(|(collection, document_id, _prov, _surrogate)| (collection, document_id))
}

#[cfg(test)]
mod tests {
    use super::*;

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
