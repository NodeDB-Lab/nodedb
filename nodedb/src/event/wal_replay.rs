//! WAL replay: converts WAL records into WriteEvents for Event Plane recovery.
//!
//! On startup or when entering WAL Catchup Mode, the Event Plane reads WAL
//! records from a given LSN forward and reconstructs WriteEvents from the
//! MessagePack-encoded payloads.
//!
//! Each WAL record type has a known payload format (see `wal_dispatch.rs`):
//! - `Put`: `(collection, document_id, value)` for documents,
//!   `("kv_put", collection, key, value, ttl_ms)` for KV,
//!   `(src_id, label, dst_id, props)` for graph edges
//! - `Delete`: `(collection, document_id)` for documents,
//!   `("kv_delete", collection, keys)` for KV
//! - `VectorPut`: `(collection, vector, dim)` — not a document write event
//! - `VectorDelete`: `(collection, vector_id)` — not a document write event
//!
//! The Event Plane only reconstructs events for data-mutating operations
//! (Put, Delete, KV). Vector and CRDT operations are handled by their own
//! replay paths and are not yet emitted as WriteEvents.

use std::sync::Arc;

use nodedb_wal::WalRecord;
use nodedb_wal::record::RecordType;
use tracing::{trace, warn};

use crate::event::types::{EventSource, RowId, WriteEvent, WriteOp};
use crate::types::{Lsn, TenantId, VShardId};
use crate::wal::WalManager;

/// Replay WAL records from `from_lsn` forward and convert to WriteEvents.
///
/// Filters records to only those routed to `core_id` (by vShard % num_cores).
/// Returns events in LSN order, ready to be processed by the consumer.
///
/// `base_sequence` is the starting sequence number for the replayed events
/// (continues from the consumer's last known sequence).
pub fn replay_wal_to_events(
    wal: &WalManager,
    from_lsn: Lsn,
    core_id: usize,
    num_cores: usize,
    base_sequence: u64,
) -> crate::Result<Vec<WriteEvent>> {
    let records = wal.replay_from(from_lsn)?;
    convert_records_to_events(&records, from_lsn, core_id, num_cores, base_sequence)
}

/// Replay WAL records using mmap (tier-2 catchup path).
///
/// Same conversion logic as `replay_wal_to_events` but uses `MmapWalReader`
/// for sealed segments — the kernel manages page residency without pinning
/// slab memory. This is the preferred path for WAL Catchup Mode.
pub fn replay_wal_mmap(
    wal: &WalManager,
    from_lsn: Lsn,
    core_id: usize,
    num_cores: usize,
    base_sequence: u64,
) -> crate::Result<Vec<WriteEvent>> {
    let records = wal.replay_mmap_from(from_lsn)?;
    convert_records_to_events(&records, from_lsn, core_id, num_cores, base_sequence)
}

/// Convert WAL records to WriteEvents, filtering by core affinity.
fn convert_records_to_events(
    records: &[nodedb_wal::WalRecord],
    from_lsn: Lsn,
    core_id: usize,
    num_cores: usize,
    base_sequence: u64,
) -> crate::Result<Vec<WriteEvent>> {
    let mut events = Vec::new();
    let mut sequence = base_sequence;

    for record in records {
        let vshard_id = record.header.vshard_id as usize;
        let target_core = if num_cores > 0 {
            vshard_id % num_cores
        } else {
            0
        };
        if target_core != core_id {
            continue;
        }

        if let Some(event) = record_to_event(record, &mut sequence) {
            events.push(event);
        }
    }

    trace!(
        core_id,
        from_lsn = from_lsn.as_u64(),
        total_records = records.len(),
        events_produced = events.len(),
        "WAL replay to events complete"
    );

    Ok(events)
}

/// Convert a single WAL record to a WriteEvent, or `None` if the record
/// type is not relevant to the Event Plane (e.g., VectorParams, Checkpoint).
fn record_to_event(record: &WalRecord, sequence: &mut u64) -> Option<WriteEvent> {
    let logical_type = record.logical_record_type();
    let record_type = RecordType::from_raw(logical_type)?;

    let tenant_id = TenantId::new(record.header.tenant_id);
    let vshard_id = VShardId::new(record.header.vshard_id);
    let lsn = Lsn::new(record.header.lsn);

    match record_type {
        RecordType::Put => parse_put_record(&record.payload, tenant_id, vshard_id, lsn, sequence),
        RecordType::Delete => {
            parse_delete_record(&record.payload, tenant_id, vshard_id, lsn, sequence)
        }
        // Vector, CRDT, Timeseries, and Checkpoint records are not yet
        // emitted as WriteEvents — they have their own replay paths.
        // They will be wired in when trigger/CDC support needs them.
        RecordType::VectorPut
        | RecordType::VectorDelete
        | RecordType::VectorParams
        | RecordType::CrdtDelta
        | RecordType::TimeseriesBatch
        | RecordType::LogBatch
        | RecordType::Transaction
        | RecordType::Checkpoint
        | RecordType::Noop => None,
    }
}

/// Parse a `RecordType::Put` payload. May be a document put, KV put, or
/// graph edge put — distinguished by the MessagePack structure.
fn parse_put_record(
    payload: &[u8],
    tenant_id: TenantId,
    vshard_id: VShardId,
    lsn: Lsn,
    sequence: &mut u64,
) -> Option<WriteEvent> {
    // Try KV put first: ("kv_put", collection, key, value, ttl_ms)
    if let Ok((disc, collection, key, value, _ttl_ms)) =
        rmp_serde::from_slice::<(&str, String, Vec<u8>, Vec<u8>, u64)>(payload)
        && disc == "kv_put"
    {
        *sequence += 1;
        let key_str = String::from_utf8_lossy(&key);
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
        });
    }

    // Try KV batch put: ("kv_batch_put", collection, entries, ttl_ms)
    if let Ok((disc, collection, entries, _ttl_ms)) =
        rmp_serde::from_slice::<(&str, String, Vec<(Vec<u8>, Vec<u8>)>, u64)>(payload)
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
        });
    }

    // Try document put: (collection, document_id, value)
    if let Ok((collection, document_id, value)) =
        rmp_serde::from_slice::<(String, String, Vec<u8>)>(payload)
    {
        // Distinguish from graph edge put which is (src_id, label, dst_id, props).
        // Document put has exactly 3 elements; edge put has 4.
        // If the third element parsed as Vec<u8> is the actual doc value, this is a doc put.
        *sequence += 1;
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
fn parse_delete_record(
    payload: &[u8],
    tenant_id: TenantId,
    vshard_id: VShardId,
    lsn: Lsn,
    sequence: &mut u64,
) -> Option<WriteEvent> {
    // Try KV delete: ("kv_delete", collection, keys)
    if let Ok((disc, collection, keys)) =
        rmp_serde::from_slice::<(&str, String, Vec<Vec<u8>>)>(payload)
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
        });
    }

    // Try document delete: (collection, document_id)
    if let Ok((collection, document_id)) = rmp_serde::from_slice::<(String, String)>(payload) {
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
        });
    }

    warn!(
        lsn = lsn.as_u64(),
        payload_len = payload.len(),
        "WAL replay: unrecognized Delete payload format, skipping"
    );
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_document_put() {
        let payload = rmp_serde::to_vec(&("orders", "order-1", b"value")).unwrap();
        let record = make_record(RecordType::Put, &payload, 1, 0, 100);
        let mut seq = 0u64;
        let event = record_to_event(&record, &mut seq).unwrap();
        assert_eq!(event.collection.as_ref(), "orders");
        assert_eq!(event.row_id.as_str(), "order-1");
        assert_eq!(event.op, WriteOp::Insert);
        assert_eq!(event.lsn, Lsn::new(100));
        assert_eq!(seq, 1);
    }

    #[test]
    fn parse_document_delete() {
        let payload = rmp_serde::to_vec(&("orders", "order-1")).unwrap();
        let record = make_record(RecordType::Delete, &payload, 1, 0, 101);
        let mut seq = 0u64;
        let event = record_to_event(&record, &mut seq).unwrap();
        assert_eq!(event.op, WriteOp::Delete);
        assert_eq!(event.row_id.as_str(), "order-1");
    }

    #[test]
    fn parse_kv_put() {
        let payload = rmp_serde::to_vec(&("kv_put", "cache", b"key1", b"val1", 0u64)).unwrap();
        let record = make_record(RecordType::Put, &payload, 1, 0, 102);
        let mut seq = 0u64;
        let event = record_to_event(&record, &mut seq).unwrap();
        assert_eq!(event.collection.as_ref(), "cache");
        assert_eq!(event.op, WriteOp::Insert);
    }

    #[test]
    fn parse_kv_delete() {
        let payload = rmp_serde::to_vec(&("kv_delete", "cache", vec![b"key1".to_vec()])).unwrap();
        let record = make_record(RecordType::Delete, &payload, 1, 0, 103);
        let mut seq = 0u64;
        let event = record_to_event(&record, &mut seq).unwrap();
        assert_eq!(event.op, WriteOp::BulkDelete { count: 1 });
    }

    #[test]
    fn vector_records_skipped() {
        let payload = rmp_serde::to_vec(&("vecs", vec![1.0f32, 2.0, 3.0], 3u32)).unwrap();
        let record = make_record(RecordType::VectorPut, &payload, 1, 0, 104);
        let mut seq = 0u64;
        assert!(record_to_event(&record, &mut seq).is_none());
        assert_eq!(seq, 0); // Not incremented.
    }

    #[test]
    fn checkpoint_records_skipped() {
        let record = make_record(RecordType::Checkpoint, &[], 1, 0, 105);
        let mut seq = 0u64;
        assert!(record_to_event(&record, &mut seq).is_none());
    }

    fn make_record(
        rt: RecordType,
        payload: &[u8],
        tenant_id: u32,
        vshard_id: u16,
        lsn: u64,
    ) -> WalRecord {
        WalRecord::new(rt as u16, lsn, tenant_id, vshard_id, payload.to_vec(), None).unwrap()
    }
}
