// SPDX-License-Identifier: BUSL-1.1

//! Payload parsers for the Event Plane's WAL replay: map a raw `Put` / `Delete`
//! WAL record payload to a [`WriteEvent`].
//!
//! These are the per-record-type payload decoders that `wal_replay::record_to_events`
//! dispatches to. Each `Put` / `Delete` payload may carry one of several arities
//! (document with/without surrogate/provenance, KV point / batch); the parser
//! tries them in most-specific-first order and returns the first that decodes.
//!
//! `wal_replay` parses each `TransactionRedo` sub-op with these same parsers.
//! Every event takes its identity and event source from the [`ReplayScope`].

use std::sync::Arc;

use nodedb_types::RowIdentity;
use nodedb_types::sync::wire::SyncProvenance;
use tracing::warn;

use crate::event::types::{EdgeEndpoints, RecordPosition, RowId, WriteEvent, WriteOp};
use crate::event::wal_replay_doc_shapes::{DocPut, decode_doc_delete, decode_doc_put};
use crate::event::wal_replay_kv_shapes::parse_kv_put_family;
use crate::event::wal_replay_scope::ReplayScope;

/// `(op, new_value, old_value)` for a node-label CDC event — the op tag plus
/// the label-delta payload placed on whichever side its `WriteOp` implies.
type LabelEventFields = (WriteOp, Option<Arc<[u8]>>, Option<Arc<[u8]>>);

/// Whether a `Put` / `Delete` payload is a graph edge record.
pub(super) fn is_edge_record(payload: &[u8]) -> bool {
    zerompk::from_msgpack::<crate::wal::EdgePutRedo>(payload).is_ok()
        || zerompk::from_msgpack::<crate::wal::EdgeDeleteRedo>(payload).is_ok()
}

/// Parse a `RecordType::Put` payload. May be a document put, KV put, or
/// graph edge put — distinguished by the MessagePack structure.
pub(super) fn parse_put_record(
    payload: &[u8],
    scope: &ReplayScope,
    sequence: &mut u64,
) -> Option<WriteEvent> {
    let ReplayScope {
        database_id,
        tenant_id,
        vshard_id,
        lsn,
        sources,
        commit_hlc,
    } = *scope;
    // Try the KV put family first: its payloads open with a discriminator no
    // document or edge shape carries.
    if let Some(event) = parse_kv_put_family(payload, scope, sequence) {
        return Some(event);
    }

    // Try document put with surrogate, plain or bitemporal. The surrogate is
    // consumed by the Data Plane's vector-index replay. The event stream keys
    // on `document_id`, which the writer journals as the row's `RowIdentity`
    // text, so it is wrapped verbatim and never reinterpreted.
    if let Some(DocPut {
        collection,
        document_id,
        value,
        stamps,
    }) = decode_doc_put(payload)
    {
        *sequence += 1;
        let (system_time_ms, valid_time_ms) = match stamps {
            Some((system, valid)) => (Some(system), valid),
            None => crate::event::bitemporal_extract::extract_stamps(Some(&value)),
        };
        return Some(WriteEvent {
            sequence: *sequence,
            collection: Arc::from(collection.as_str()),
            op: WriteOp::Insert,
            row_id: RowId::row(RowIdentity::from_user_key(document_id)),
            lsn,
            record: Some(RecordPosition::first(lsn)),
            database_id,
            tenant_id,
            vshard_id,
            source: sources.document,
            new_value: Some(Arc::from(value.as_slice())),
            old_value: None,
            system_time_ms,
            valid_time_ms,
            user_id: None,
            statement_digest: None,
            commit_hlc,
            image_fault: None,
        });
    }

    // Try document put with provenance (legacy arity): (collection, document_id, value, provenance)
    if let Ok((collection, document_id, value, _prov)) =
        zerompk::from_msgpack::<(String, String, Vec<u8>, Option<SyncProvenance>)>(payload)
    {
        *sequence += 1;
        let (system_time_ms, valid_time_ms) =
            crate::event::bitemporal_extract::extract_stamps(Some(&value));
        return Some(WriteEvent {
            sequence: *sequence,
            collection: Arc::from(collection.as_str()),
            op: WriteOp::Insert,
            row_id: RowId::row(RowIdentity::from_user_key(document_id)),
            lsn,
            record: Some(RecordPosition::first(lsn)),
            database_id,
            tenant_id,
            vshard_id,
            source: sources.document,
            new_value: Some(Arc::from(value.as_slice())),
            old_value: None,
            system_time_ms,
            valid_time_ms,
            user_id: None,
            statement_digest: None,
            commit_hlc,
            image_fault: None,
        });
    }

    // Try document put (legacy arity): (collection, document_id, value)
    if let Ok((collection, document_id, value)) =
        zerompk::from_msgpack::<(String, String, Vec<u8>)>(payload)
    {
        // Distinguish from graph edge put which is (src_id, label, dst_id, props).
        // Document put has exactly 3 elements; edge put has 4.
        // If the third element parsed as Vec<u8> is the actual doc value, this is a doc put.
        *sequence += 1;
        let (system_time_ms, valid_time_ms) =
            crate::event::bitemporal_extract::extract_stamps(Some(&value));
        return Some(WriteEvent {
            sequence: *sequence,
            collection: Arc::from(collection.as_str()),
            op: WriteOp::Insert,
            row_id: RowId::row(RowIdentity::from_user_key(document_id)),
            lsn,
            record: Some(RecordPosition::first(lsn)),
            database_id,
            tenant_id,
            vshard_id,
            source: sources.document,
            new_value: Some(Arc::from(value.as_slice())),
            old_value: None,
            system_time_ms,
            valid_time_ms,
            user_id: None,
            statement_digest: None,
            commit_hlc,
            image_fault: None,
        });
    }

    // Try graph edge put: a map-encoded `EdgePutRedo`, the payload of both the
    // autocommit record and a transaction's redo sub-op. No array-encoded
    // document or KV shape above decodes as a map. An edge record without both
    // endpoint surrogates is refused. `row_id` is the forward emit's
    // `(src,label,dst)` composition, so replay events dedup against forward
    // events, and it carries the record's endpoint surrogates.
    if let Ok(edge) = zerompk::from_msgpack::<crate::wal::EdgePutRedo>(payload) {
        let Some((src_surrogate, dst_surrogate)) = edge.endpoints() else {
            refuse_unbound_edge(lsn, &edge.collection);
            return None;
        };
        let crate::wal::EdgePutRedo {
            collection,
            src_id,
            label,
            dst_id,
            properties,
            ..
        } = edge;
        *sequence += 1;
        let (system_time_ms, valid_time_ms) =
            crate::event::bitemporal_extract::extract_stamps(Some(&properties));
        return Some(WriteEvent {
            sequence: *sequence,
            collection: Arc::from(collection.as_str()),
            op: WriteOp::Insert,
            row_id: RowId::edge(
                EdgeEndpoints {
                    src: src_id,
                    src_surrogate,
                    dst: dst_id,
                    dst_surrogate,
                },
                label,
            ),
            lsn,
            record: Some(RecordPosition::first(lsn)),
            database_id,
            tenant_id,
            vshard_id,
            source: sources.other,
            new_value: Some(Arc::from(properties.as_slice())),
            old_value: None,
            system_time_ms,
            valid_time_ms,
            user_id: None,
            statement_digest: None,
            commit_hlc,
            image_fault: None,
        });
    }

    // Unrecognized Put payload (e.g., KV expire) — skip.
    warn!(
        lsn = lsn.as_u64(),
        payload_len = payload.len(),
        "WAL replay: unrecognized Put payload format, skipping"
    );
    None
}

/// Parse a `RecordType::GraphNodeLabelSet` / `GraphNodeLabelRemove` payload into
/// a CDC [`WriteEvent`] on the nameable node-label stream
/// ([`crate::event::graph_cdc::GRAPH_LABEL_STREAM`]).
///
/// `is_set` distinguishes set (→ [`WriteOp::Insert`], added labels as
/// `new_value`) from remove (→ [`WriteOp::Delete`], removed labels as
/// `old_value`). The payload shape `(node_id, labels)` and the label-delta
/// value encoding are exactly what the forward-path emit produces, so replayed
/// events are byte-identical to forward events and dedup on LSN. A malformed
/// payload is logged and skipped (never a panic).
pub(super) fn parse_graph_node_label_record(
    payload: &[u8],
    is_set: bool,
    scope: &ReplayScope,
    sequence: &mut u64,
) -> Option<WriteEvent> {
    let ReplayScope {
        database_id,
        tenant_id,
        vshard_id,
        lsn,
        sources,
        commit_hlc,
    } = *scope;
    let (node_id, labels) = match zerompk::from_msgpack::<(String, Vec<String>)>(payload) {
        Ok(decoded) => decoded,
        Err(_) => {
            warn!(
                lsn = lsn.as_u64(),
                payload_len = payload.len(),
                "WAL replay: malformed graph node-label payload, skipping"
            );
            return None;
        }
    };
    *sequence += 1;
    let value = crate::event::graph_cdc::graph_label_delta_value(&labels);
    let (op, new_value, old_value): LabelEventFields = if is_set {
        (WriteOp::Insert, Some(Arc::from(value.as_slice())), None)
    } else {
        (WriteOp::Delete, None, Some(Arc::from(value.as_slice())))
    };
    Some(WriteEvent {
        sequence: *sequence,
        collection: Arc::from(crate::event::graph_cdc::GRAPH_LABEL_STREAM),
        op,
        row_id: RowId::row(RowIdentity::from_user_key(node_id)),
        lsn,
        record: Some(RecordPosition::first(lsn)),
        database_id,
        tenant_id,
        vshard_id,
        source: sources.other,
        new_value,
        old_value,
        system_time_ms: None,
        valid_time_ms: None,
        user_id: None,
        statement_digest: None,
        commit_hlc,
        image_fault: None,
    })
}

/// Parse a `RecordType::Delete` payload. May be a document delete or KV delete.
pub(super) fn parse_delete_record(
    payload: &[u8],
    scope: &ReplayScope,
    sequence: &mut u64,
) -> Option<WriteEvent> {
    let ReplayScope {
        database_id,
        tenant_id,
        vshard_id,
        lsn,
        sources,
        commit_hlc,
    } = *scope;
    // Try KV delete: ("kv_delete", collection, keys)
    if let Ok((disc, collection, keys)) =
        zerompk::from_msgpack::<(&str, String, Vec<Vec<u8>>)>(payload)
        && disc == "kv_delete"
    {
        *sequence += 1;
        return Some(WriteEvent {
            sequence: *sequence,
            collection: Arc::from(collection.as_str()),
            op: WriteOp::BulkDelete {
                count: keys.len() as u32,
            },
            row_id: RowId::Batch,
            lsn,
            record: Some(RecordPosition::first(lsn)),
            database_id,
            tenant_id,
            vshard_id,
            source: sources.other,
            new_value: None,
            old_value: None,
            system_time_ms: None,
            valid_time_ms: None,
            user_id: None,
            statement_digest: None,
            commit_hlc,
            image_fault: None,
        });
    }

    // Try document delete with surrogate, plain or bitemporal. PointDelete, a
    // transaction's redo, and the post-apply write-set redo helper emit it;
    // try it before the 3-tuple so a surrogate-carrying record isn't misdecoded.
    if let Some((collection, document_id)) = decode_doc_delete(payload) {
        *sequence += 1;
        return Some(WriteEvent {
            sequence: *sequence,
            collection: Arc::from(collection.as_str()),
            op: WriteOp::Delete,
            row_id: RowId::row(RowIdentity::from_user_key(document_id)),
            lsn,
            record: Some(RecordPosition::first(lsn)),
            database_id,
            tenant_id,
            vshard_id,
            source: sources.document,
            new_value: None,
            old_value: None,
            system_time_ms: None,
            valid_time_ms: None,
            user_id: None,
            statement_digest: None,
            commit_hlc,
            image_fault: None,
        });
    }

    // Try document delete with provenance (older arity): (collection, document_id, provenance)
    if let Ok((collection, document_id, _prov)) =
        zerompk::from_msgpack::<(String, String, Option<SyncProvenance>)>(payload)
    {
        *sequence += 1;
        return Some(WriteEvent {
            sequence: *sequence,
            collection: Arc::from(collection.as_str()),
            op: WriteOp::Delete,
            row_id: RowId::row(RowIdentity::from_user_key(document_id)),
            lsn,
            record: Some(RecordPosition::first(lsn)),
            database_id,
            tenant_id,
            vshard_id,
            source: sources.document,
            new_value: None,
            old_value: None,
            system_time_ms: None,
            valid_time_ms: None,
            user_id: None,
            statement_digest: None,
            commit_hlc,
            image_fault: None,
        });
    }

    // Try document delete (legacy arity): (collection, document_id)
    if let Ok((collection, document_id)) = zerompk::from_msgpack::<(String, String)>(payload) {
        *sequence += 1;
        return Some(WriteEvent {
            sequence: *sequence,
            collection: Arc::from(collection.as_str()),
            op: WriteOp::Delete,
            row_id: RowId::row(RowIdentity::from_user_key(document_id)),
            lsn,
            record: Some(RecordPosition::first(lsn)),
            database_id,
            tenant_id,
            vshard_id,
            source: sources.document,
            new_value: None,
            old_value: None,
            system_time_ms: None,
            valid_time_ms: None,
            user_id: None,
            statement_digest: None,
            commit_hlc,
            image_fault: None,
        });
    }

    // Try graph edge delete: a map-encoded `EdgeDeleteRedo`, as for the put.
    // An edge record without both endpoint surrogates is refused. `row_id`
    // matches the forward emit's `(src,label,dst)` composition and carries
    // the record's endpoint surrogates.
    if let Ok(edge) = zerompk::from_msgpack::<crate::wal::EdgeDeleteRedo>(payload) {
        let Some((src_surrogate, dst_surrogate)) = edge.endpoints() else {
            refuse_unbound_edge(lsn, &edge.collection);
            return None;
        };
        let crate::wal::EdgeDeleteRedo {
            collection,
            src_id,
            label,
            dst_id,
            ..
        } = edge;
        *sequence += 1;
        return Some(WriteEvent {
            sequence: *sequence,
            collection: Arc::from(collection.as_str()),
            op: WriteOp::Delete,
            row_id: RowId::edge(
                EdgeEndpoints {
                    src: src_id,
                    src_surrogate,
                    dst: dst_id,
                    dst_surrogate,
                },
                label,
            ),
            lsn,
            record: Some(RecordPosition::first(lsn)),
            database_id,
            tenant_id,
            vshard_id,
            source: sources.other,
            new_value: None,
            old_value: None,
            system_time_ms: None,
            valid_time_ms: None,
            user_id: None,
            statement_digest: None,
            commit_hlc,
            image_fault: None,
        });
    }

    warn!(
        lsn = lsn.as_u64(),
        payload_len = payload.len(),
        "WAL replay: unrecognized Delete payload format, skipping"
    );
    None
}

/// Log the refusal of an edge record whose endpoint surrogate is unbound.
/// Every edge writer carries both identities, so no event is built from one
/// that lacks them.
fn refuse_unbound_edge(lsn: crate::types::Lsn, collection: &str) {
    warn!(
        lsn = lsn.as_u64(),
        %collection,
        "WAL replay: edge record carries an unbound endpoint surrogate, refused"
    );
}
