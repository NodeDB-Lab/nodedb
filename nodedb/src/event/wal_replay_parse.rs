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

use nodedb_types::sync::wire::SyncProvenance;
use tracing::warn;

use crate::event::types::{EventSource, RowId, WriteEvent, WriteOp};
use crate::types::{Lsn, TenantId, VShardId};

/// Parse a `RecordType::Put` payload. May be a document put, KV put, or
/// graph edge put — distinguished by the MessagePack structure.
pub(super) fn parse_put_record(
    payload: &[u8],
    tenant_id: TenantId,
    vshard_id: VShardId,
    lsn: Lsn,
    sequence: &mut u64,
) -> Option<WriteEvent> {
    // Try KV put first: ("kv_put", collection, key, value, ttl_ms)
    if let Ok((disc, collection, key, value, _ttl_ms)) =
        zerompk::from_msgpack::<(&str, String, Vec<u8>, Vec<u8>, u64)>(payload)
        && disc == "kv_put"
    {
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
            row_id: RowId::new(key_str.as_ref()),
            lsn,
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

    // Try KV batch put: ("kv_batch_put", collection, entries, ttl_ms)
    if let Ok((disc, collection, entries, _ttl_ms)) =
        zerompk::from_msgpack::<(&str, String, Vec<(Vec<u8>, Vec<u8>)>, u64)>(payload)
        && disc == "kv_batch_put"
    {
        // Emit one event for the batch (BulkInsert).
        *sequence += 1;
        return Some(WriteEvent {
            sequence: *sequence,
            collection: Arc::from(collection.as_str()),
            op: WriteOp::BulkInsert {
                count: entries.len() as u32,
            },
            row_id: RowId::new("_batch"),
            lsn,
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
    // surrogate is consumed by the Data Plane's vector-index replay; the event
    // stream keys on `document_id`, so it is ignored here.
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
            row_id: RowId::new(document_id.as_str()),
            lsn,
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
            row_id: RowId::new(document_id.as_str()),
            lsn,
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
            row_id: RowId::new(document_id.as_str()),
            lsn,
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

    // Unrecognized Put payload (e.g., graph edge or KV expire) — skip.
    warn!(
        lsn = lsn.as_u64(),
        payload_len = payload.len(),
        "WAL replay: unrecognized Put payload format, skipping"
    );
    None
}

/// Parse a `RecordType::Delete` payload. May be a document delete or KV delete.
pub(super) fn parse_delete_record(
    payload: &[u8],
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
            row_id: RowId::new("_batch"),
            lsn,
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
            row_id: RowId::new(document_id.as_str()),
            lsn,
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
            row_id: RowId::new(document_id.as_str()),
            lsn,
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
            row_id: RowId::new(document_id.as_str()),
            lsn,
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
