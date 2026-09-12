// SPDX-License-Identifier: BUSL-1.1

//! Payload parsers for the Event Plane's WAL replay: map a raw `Put` / `Delete`
//! WAL record payload to a [`WriteEvent`].
//!
//! These are the per-record-type payload decoders that `wal_replay::record_to_events`
//! dispatches to. Each `Put` / `Delete` payload may carry one of several arities
//! (document with/without surrogate/provenance, KV point / batch); the parser
//! tries them in most-specific-first order and returns the first that decodes.
//!
//! A `TransactionRedo` sub-op payload is byte-identical to the corresponding raw
//! per-op WAL record payload, so `wal_replay` reconstitutes each sub-op as a
//! standalone `WalRecord` and routes it back through the same dispatch — reusing
//! these parsers verbatim, with no redo-specific decode path.

use std::sync::Arc;

use nodedb_types::RowIdentity;
use nodedb_types::sync::wire::SyncProvenance;
use tracing::warn;

use crate::event::types::{EventSource, RowId, WriteEvent, WriteOp};
use crate::types::{DatabaseId, Lsn, TenantId, VShardId};

/// `(op, new_value, old_value)` for a node-label CDC event — the op tag plus
/// the label-delta payload placed on whichever side its `WriteOp` implies.
type LabelEventFields = (WriteOp, Option<Arc<[u8]>>, Option<Arc<[u8]>>);

/// The `(collection, key, value)` an event needs out of a `kv_put` record, in
/// whichever of its three decodable arities the record was written.
fn decode_kv_put_event_fields(payload: &[u8]) -> Option<(String, Vec<u8>, Vec<u8>)> {
    if let Ok((disc, collection, key, value, _ttl, _expire, _surrogate)) =
        zerompk::from_msgpack::<(&str, String, Vec<u8>, Vec<u8>, u64, Option<u64>, u32)>(payload)
        && disc == "kv_put"
    {
        return Some((collection, key, value));
    }
    if let Ok((disc, collection, key, value, _ttl, _expire)) =
        zerompk::from_msgpack::<(&str, String, Vec<u8>, Vec<u8>, u64, u64)>(payload)
        && disc == "kv_put"
    {
        return Some((collection, key, value));
    }
    if let Ok((disc, collection, key, value, _ttl)) =
        zerompk::from_msgpack::<(&str, String, Vec<u8>, Vec<u8>, u64)>(payload)
        && disc == "kv_put"
    {
        return Some((collection, key, value));
    }
    None
}

/// The `(collection, entries)` an event needs out of a `kv_batch_put` record,
/// in whichever of its three decodable arities the record was written.
#[allow(clippy::type_complexity)]
fn decode_kv_batch_put_event_fields(payload: &[u8]) -> Option<(String, Vec<(Vec<u8>, Vec<u8>)>)> {
    if let Ok((disc, collection, entries, _ttl, _expire, _surrogates)) = zerompk::from_msgpack::<(
        &str,
        String,
        Vec<(Vec<u8>, Vec<u8>)>,
        u64,
        Option<u64>,
        Vec<u32>,
    )>(payload)
        && disc == "kv_batch_put"
    {
        return Some((collection, entries));
    }
    if let Ok((disc, collection, entries, _ttl, _expire)) =
        zerompk::from_msgpack::<(&str, String, Vec<(Vec<u8>, Vec<u8>)>, u64, u64)>(payload)
        && disc == "kv_batch_put"
    {
        return Some((collection, entries));
    }
    if let Ok((disc, collection, entries, _ttl)) =
        zerompk::from_msgpack::<(&str, String, Vec<(Vec<u8>, Vec<u8>)>, u64)>(payload)
        && disc == "kv_batch_put"
    {
        return Some((collection, entries));
    }
    None
}

/// Parse a `RecordType::Put` payload. May be a document put, KV put, or
/// graph edge put — distinguished by the MessagePack structure.
pub(super) fn parse_put_record(
    payload: &[u8],
    database_id: DatabaseId,
    tenant_id: TenantId,
    vshard_id: VShardId,
    lsn: Lsn,
    sequence: &mut u64,
) -> Option<WriteEvent> {
    // Try KV put first. Three arities decode: the current
    // `("kv_put", collection, key, value, ttl_ms, expire_at_ms, surrogate)` and
    // the two that predate the carried surrogate. The event stream keys on the
    // raw KV key, so only `collection`, `key`, and `value` are read out.
    if let Some((collection, key, value)) = decode_kv_put_event_fields(payload) {
        *sequence += 1;
        let key_str = String::from_utf8_lossy(&key);
        let (system_time_ms, valid_time_ms) =
            crate::event::bitemporal_extract::extract_stamps(Some(&value));
        // AUDIT_DML rows replayed from WAL after a crash carry user_id = None and
        // statement_digest = None; pre-crash audit rows are durable in the catalog.
        // Widening the WAL record format to carry these fields is tracked separately.
        return Some(WriteEvent {
            sequence: *sequence,
            collection: Arc::from(collection.as_str()),
            op: WriteOp::Insert,
            row_id: RowId::row(RowIdentity::from_user_key(key_str.into_owned())),
            lsn,
            database_id,
            tenant_id,
            vshard_id,
            source: EventSource::User,
            new_value: Some(Arc::from(value.as_slice())),
            old_value: None,
            system_time_ms,
            valid_time_ms,
            user_id: None,
            statement_digest: None,
        });
    }

    // Try KV batch put — same three-arity story as the point put above.
    if let Some((collection, entries)) = decode_kv_batch_put_event_fields(payload) {
        // Emit one event for the batch (BulkInsert).
        *sequence += 1;
        return Some(WriteEvent {
            sequence: *sequence,
            collection: Arc::from(collection.as_str()),
            op: WriteOp::BulkInsert {
                count: entries.len() as u32,
            },
            row_id: RowId::Batch,
            lsn,
            database_id,
            tenant_id,
            vshard_id,
            source: EventSource::User,
            new_value: None,
            old_value: None,
            system_time_ms: None,
            valid_time_ms: None,
            user_id: None,
            statement_digest: None,
        });
    }

    // Try document put with surrogate (current arity):
    // (collection, document_id, value, provenance, surrogate_u32). The trailing
    // surrogate is consumed by the Data Plane's vector-index replay. The event
    // stream keys on `document_id`, which the writer journals as the row's
    // `RowIdentity` text, so it is wrapped verbatim and never reinterpreted.
    if let Ok((collection, document_id, value, _prov, _surrogate)) =
        zerompk::from_msgpack::<(String, String, Vec<u8>, Option<SyncProvenance>, u32)>(payload)
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
            database_id,
            tenant_id,
            vshard_id,
            source: EventSource::User,
            new_value: Some(Arc::from(value.as_slice())),
            old_value: None,
            system_time_ms,
            valid_time_ms,
            user_id: None,
            statement_digest: None,
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
            database_id,
            tenant_id,
            vshard_id,
            source: EventSource::User,
            new_value: Some(Arc::from(value.as_slice())),
            old_value: None,
            system_time_ms,
            valid_time_ms,
            user_id: None,
            statement_digest: None,
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
            database_id,
            tenant_id,
            vshard_id,
            source: EventSource::User,
            new_value: Some(Arc::from(value.as_slice())),
            old_value: None,
            system_time_ms,
            valid_time_ms,
            user_id: None,
            statement_digest: None,
        });
    }

    // Try graph edge put: (collection, src_id, label, dst_id, properties).
    // Tried after every document/KV arm above so it only ever sees genuine
    // non-matches: it is distinguished by its 5-field shape whose 3rd/4th
    // fields are strings (label, dst_id) and 5th is a byte blob (properties) —
    // no document-with-surrogate `(.., Vec<u8>, .., u32)` or KV `(.., u64)`
    // shape decodes into it. `row_id` is the same `(src,label,dst)` composition
    // the forward emit uses, so replay events dedup against forward events.
    if let Ok((collection, src_id, label, dst_id, properties)) =
        zerompk::from_msgpack::<(String, String, String, String, Vec<u8>)>(payload)
    {
        *sequence += 1;
        let (system_time_ms, valid_time_ms) =
            crate::event::bitemporal_extract::extract_stamps(Some(&properties));
        return Some(WriteEvent {
            sequence: *sequence,
            collection: Arc::from(collection.as_str()),
            op: WriteOp::Insert,
            row_id: RowId::edge(src_id, label, dst_id),
            lsn,
            database_id,
            tenant_id,
            vshard_id,
            source: EventSource::User,
            new_value: Some(Arc::from(properties.as_slice())),
            old_value: None,
            system_time_ms,
            valid_time_ms,
            user_id: None,
            statement_digest: None,
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
    database_id: DatabaseId,
    tenant_id: TenantId,
    vshard_id: VShardId,
    lsn: Lsn,
    sequence: &mut u64,
) -> Option<WriteEvent> {
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
        database_id,
        tenant_id,
        vshard_id,
        source: EventSource::User,
        new_value,
        old_value,
        system_time_ms: None,
        valid_time_ms: None,
        user_id: None,
        statement_digest: None,
    })
}

/// Parse a `RecordType::Delete` payload. May be a document delete or KV delete.
pub(super) fn parse_delete_record(
    payload: &[u8],
    database_id: DatabaseId,
    tenant_id: TenantId,
    vshard_id: VShardId,
    lsn: Lsn,
    sequence: &mut u64,
) -> Option<WriteEvent> {
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
            database_id,
            tenant_id,
            vshard_id,
            source: EventSource::User,
            new_value: None,
            old_value: None,
            system_time_ms: None,
            valid_time_ms: None,
            user_id: None,
            statement_digest: None,
        });
    }

    // Try document delete with surrogate (redo 4-tuple): (collection, document_id, provenance, surrogate).
    // PointDelete and the post-apply write-set redo helper both emit this shape;
    // try it before the 3-tuple so a surrogate-carrying record isn't misdecoded.
    if let Ok((collection, document_id, _prov, _surrogate)) =
        zerompk::from_msgpack::<(String, String, Option<SyncProvenance>, u32)>(payload)
    {
        *sequence += 1;
        return Some(WriteEvent {
            sequence: *sequence,
            collection: Arc::from(collection.as_str()),
            op: WriteOp::Delete,
            row_id: RowId::row(RowIdentity::from_user_key(document_id)),
            lsn,
            database_id,
            tenant_id,
            vshard_id,
            source: EventSource::User,
            new_value: None,
            old_value: None,
            system_time_ms: None,
            valid_time_ms: None,
            user_id: None,
            statement_digest: None,
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
            database_id,
            tenant_id,
            vshard_id,
            source: EventSource::User,
            new_value: None,
            old_value: None,
            system_time_ms: None,
            valid_time_ms: None,
            user_id: None,
            statement_digest: None,
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
            database_id,
            tenant_id,
            vshard_id,
            source: EventSource::User,
            new_value: None,
            old_value: None,
            system_time_ms: None,
            valid_time_ms: None,
            user_id: None,
            statement_digest: None,
        });
    }

    // Try graph edge delete: (collection, src_id, label, dst_id). Four strings,
    // tried after every document/KV arm above. It cannot collide with
    // document-delete-with-surrogate `(String, String, Option<SyncProvenance>,
    // u32)` (its 4th field is a `u32`, not a string) nor any shorter-arity arm.
    // `row_id` matches the forward emit's `(src,label,dst)` composition.
    if let Ok((collection, src_id, label, dst_id)) =
        zerompk::from_msgpack::<(String, String, String, String)>(payload)
    {
        *sequence += 1;
        return Some(WriteEvent {
            sequence: *sequence,
            collection: Arc::from(collection.as_str()),
            op: WriteOp::Delete,
            row_id: RowId::edge(src_id, label, dst_id),
            lsn,
            database_id,
            tenant_id,
            vshard_id,
            source: EventSource::User,
            new_value: None,
            old_value: None,
            system_time_ms: None,
            valid_time_ms: None,
            user_id: None,
            statement_digest: None,
        });
    }

    warn!(
        lsn = lsn.as_u64(),
        payload_len = payload.len(),
        "WAL replay: unrecognized Delete payload format, skipping"
    );
    None
}
