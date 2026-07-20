// SPDX-License-Identifier: BUSL-1.1

//! WAL replay: converts WAL records into WriteEvents for Event Plane recovery.
//!
//! On startup or when entering WAL Catchup Mode, the Event Plane reads WAL
//! records from a given LSN forward and reconstructs WriteEvents from the
//! MessagePack-encoded payloads.
//!
//! Each WAL record type has a known payload format (see `wal_dispatch.rs`):
//! - `Put`: `(collection, document_id, value)` for documents,
//!   `("kv_put", collection, key, value, ttl_ms)` for KV,
//!   `(collection, src_id, label, dst_id, properties)` for graph edges
//! - `Delete`: `(collection, document_id)` for documents,
//!   `("kv_delete", collection, keys)` for KV,
//!   `(collection, src_id, label, dst_id)` for graph edges
//! - `GraphNodeLabelSet` / `GraphNodeLabelRemove`: `(node_id, labels)` — surface
//!   on the nameable `__graph_node_labels__` CDC stream
//! - `VectorPut`: `(collection, vector, dim)` — not a document write event
//! - `VectorDelete`: `(collection, vector_id)` — not a document write event
//!
//! The Event Plane reconstructs events for data-mutating operations (Put,
//! Delete, KV, graph edges + node labels). A `TransactionRedo` — the durable
//! payload of a Calvin cross-shard commit — is decomposed into one WriteEvent
//! per write sub-op (each sub-op payload is in the same shape as its raw per-op
//! WAL record, so the same parsers apply), so triggers/CDC/change-streams fire
//! on restart. Vector and CRDT operations are handled by their own replay paths
//! and are not yet emitted as WriteEvents.

use nodedb_wal::WalRecord;
use nodedb_wal::record::{RecordType, WalRecordArgs};
use tracing::{trace, warn};

use crate::event::types::WriteEvent;
use crate::event::wal_replay_parse::{
    parse_delete_record, parse_graph_node_label_record, parse_put_record,
};
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

    // Collection tombstones shadow any prior write in the same stream.
    // Extract once, then drop events whose `(tenant, collection, lsn)`
    // is covered.
    let tombstones = nodedb_wal::extract_tombstones(records);

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

        // A single WAL record may expand to multiple WriteEvents: a
        // `TransactionRedo` (Calvin cross-shard commit) decomposes into one event
        // per write sub-op. Raw Put/Delete records still yield at most one.
        for event in record_to_events(record, &mut sequence) {
            if tombstones.is_tombstoned(
                record.header.database_id,
                event.tenant_id.as_u64(),
                &event.collection,
                event.lsn.as_u64(),
            ) {
                continue;
            }
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

/// Convert a single WAL record into its WriteEvents. Most records map to zero
/// (types with no Event-Plane mapping, e.g. VectorParams, Checkpoint) or one
/// (raw Put/Delete). A `TransactionRedo` — the durable payload of a Calvin
/// cross-shard commit — decomposes into one event per write sub-op, so triggers,
/// CDC, and change streams fire on restart exactly as they did on the forward
/// path.
fn record_to_events(record: &WalRecord, sequence: &mut u64) -> Vec<WriteEvent> {
    let logical_type = record.logical_record_type();
    let Some(record_type) = RecordType::from_raw(logical_type) else {
        return Vec::new();
    };

    let tenant_id = TenantId::new(record.header.tenant_id);
    let vshard_id = VShardId::new(record.header.vshard_id);
    let lsn = Lsn::new(record.header.lsn);

    match record_type {
        RecordType::Put => {
            parse_put_record(&record.payload, tenant_id, vshard_id, lsn, sequence)
                .into_iter()
                .collect()
        }
        RecordType::Delete => {
            parse_delete_record(&record.payload, tenant_id, vshard_id, lsn, sequence)
                .into_iter()
                .collect()
        }
        // A Calvin cross-shard commit is durable as a `TransactionRedo` whose
        // sub-ops carry each engine's own per-op payload. Decompose it into the
        // same WriteEvents the forward path emitted, so the effect (triggers/CDC)
        // is not lost on replay. Every emitted event's `lsn` is this redo record's
        // WAL LSN — the Event-Plane watermark keys on it to dedup against the
        // forward-path event (both share this LSN in the same space).
        RecordType::TransactionRedo => decompose_redo_to_events(record, sequence),
        // Graph node-label mutations carry no natural collection (they are
        // tenant-wide), so they surface on the nameable `__graph_node_labels__`
        // CDC stream. The forward-path emit (Data Plane `SetNodeLabels` /
        // `RemoveNodeLabels`) produces the same `(collection, row_id, op, value)`
        // shape, so replayed events dedup against forward events on LSN.
        RecordType::GraphNodeLabelSet => parse_graph_node_label_record(
            &record.payload,
            true,
            tenant_id,
            vshard_id,
            lsn,
            sequence,
        )
        .into_iter()
        .collect(),
        RecordType::GraphNodeLabelRemove => parse_graph_node_label_record(
            &record.payload,
            false,
            tenant_id,
            vshard_id,
            lsn,
            sequence,
        )
        .into_iter()
        .collect(),
        // `CalvinApplied` is a payload-free applied-marker: it records that a
        // sequencer `(epoch, position)` was applied, but carries no writes. Its
        // base writes, if any, ride a separate `TransactionRedo`; a pure-read or
        // CRDT-only commit has no base WriteEvents at all (CRDT effects ride
        // `CrdtDelta` records). Nothing to emit.
        RecordType::CalvinApplied => Vec::new(),
        // The records below carry NO forward-path Data-Plane WriteEvent, so
        // there is nothing for replay to reconstruct. `record_to_events`
        // reconstructs exactly the forward WriteEvent stream the Data Plane
        // emits (Document / KV / Graph — see
        // `data::executor::core_loop::event_emit`), keyed on LSN so replayed
        // events dedup against forward ones. Emitting a WriteEvent for a record
        // the forward path never emitted would fire triggers / audit /
        // CRDT-sync / CDC on WAL replay and snapshot-catchup but NOT on the live
        // write — a recovery-divergence bug. Each group states why it has no
        // forward WriteEvent.
        //
        // Vector / CRDT records replay through their own Data-Plane paths and
        // never rode the WriteEvent stream. Infra records (Checkpoint,
        // Surrogate*, tombstone, anchors, sync HWM, Noop, …) are not row writes.
        RecordType::VectorPut
        | RecordType::VectorDelete
        | RecordType::VectorParams
        | RecordType::VectorDirectUpsert
        | RecordType::MultiVectorPut
        | RecordType::MultiVectorDelete
        | RecordType::CrdtDelta
        // CrdtListOp: position-based list-op intent, replayed by
        // `data::executor::wal_replay::crdt_list`, not the Event Plane's
        // WriteEvent stream.
        | RecordType::CrdtListOp
        // CrdtDocOp: document-row intent, replayed by
        // `data::executor::wal_replay::crdt_doc`, not the Event Plane's
        // WriteEvent stream.
        | RecordType::CrdtDocOp
        | RecordType::LogBatch
        | RecordType::Transaction
        | RecordType::SurrogateAlloc
        | RecordType::SurrogateBind
        | RecordType::Checkpoint
        | RecordType::CollectionTombstoned
        | RecordType::LsnMsAnchor
        | RecordType::TemporalPurge
        // SyncSeqAdvance: emitted by the sync layer; replay HWM reconstruction
        // is wired in the idempotency replay pass, not the Event Plane.
        | RecordType::SyncSeqAdvance
        | RecordType::Noop
        // Timeseries: the `TimeseriesBatch` payload is an opaque compressed
        // samples blob, not a per-row `(collection, row_id, value)`. Its forward
        // CDC rides the Control-Plane change stream (`publish_origin_change_events`,
        // opt-in per collection), never the Data-Plane WriteEvent stream — so
        // there is no forward WriteEvent to reconstruct.
        | RecordType::TimeseriesBatch
        // Array: `ArrayPut` / `ArrayDelete` cells decode per-cell, but the forward
        // path emits no Data-Plane WriteEvent — array CDC rides the Control-Plane
        // change stream (`extract_write_metadata`, keyed `(array_name, "*", op)`).
        // `ArrayFlush` only reorganizes on-disk tiles (no logical cell).
        | RecordType::ArrayPut
        | RecordType::ArrayDelete
        | RecordType::ArrayFlush
        // FTS / Spatial / Sparse-vector: secondary index overlays over a
        // Document/Columnar row that already publishes its own change event on the
        // forward path. The index write emits no separate WriteEvent — one here
        // would double-publish the underlying row (and the spatial geometry blob is
        // zerompk-tagged, not a standard-msgpack pass-through value anyway).
        | RecordType::FtsIndex
        | RecordType::FtsDelete
        | RecordType::SpatialPut
        | RecordType::SpatialDelete
        | RecordType::SparseVectorPut
        | RecordType::SparseVectorDelete => Vec::new(),
    }
}

/// Decompose a `TransactionRedo` record into per-sub-op WriteEvents.
///
/// Each `RedoSubRecord` carries its engine's own `record_type` and a payload in
/// that engine's exact per-op WAL shape (the same encoders the autocommit path
/// uses). We reconstitute each sub-op as a standalone `WalRecord` — stamped with
/// the enclosing redo record's header identity, crucially its LSN — and feed it
/// back through [`record_to_events`]. That reuses the raw Put/Delete parsers
/// verbatim and inherits every current and future event mapping: a sub-op type
/// with no Event-Plane mapping (VectorPut, SpatialPut, …) yields no event and
/// does not touch `sequence`, exactly as its raw counterpart does. Because the
/// reconstituted record carries `record.header.lsn`, every emitted event sets
/// `lsn = record.header.lsn`, satisfying the watermark-dedup requirement.
///
/// A malformed redo payload is logged and skipped (never a panic), mirroring the
/// decode-failure handling in the Data-Plane redo replay path.
fn decompose_redo_to_events(record: &WalRecord, sequence: &mut u64) -> Vec<WriteEvent> {
    let redo = match crate::wal::RedoRecord::from_bytes(&record.payload) {
        Ok(redo) => redo,
        Err(e) => {
            warn!(
                lsn = record.header.lsn,
                error = %e,
                "WAL replay: skipping malformed TransactionRedo payload"
            );
            return Vec::new();
        }
    };

    let mut events = Vec::new();
    for sub in redo.ops {
        let sub_record = match WalRecord::new(WalRecordArgs {
            record_type: sub.record_type,
            // Every sub-op inherits the enclosing redo record's LSN — the
            // watermark-dedup key — and tenant/vshard identity.
            lsn: record.header.lsn,
            tenant_id: record.header.tenant_id,
            vshard_id: record.header.vshard_id,
            database_id: record.header.database_id,
            payload: sub.payload,
            // The enclosing record was already decrypted when read into memory,
            // so sub-payloads are cleartext and never touch disk again.
            encryption_key: None,
            preamble_bytes: None,
        }) {
            Ok(wr) => wr,
            Err(e) => {
                warn!(
                    lsn = record.header.lsn,
                    sub_record_type = sub.record_type,
                    error = %e,
                    "WAL replay: skipping redo sub-record that failed to reconstitute"
                );
                continue;
            }
        };
        events.extend(record_to_events(&sub_record, sequence));
    }
    events
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::types::WriteOp;
    use nodedb_types::sync::wire::SyncProvenance;

    /// Assert a record maps to exactly one event and return it.
    fn one_event(record: &WalRecord, seq: &mut u64) -> WriteEvent {
        let mut events = record_to_events(record, seq);
        assert_eq!(events.len(), 1, "expected exactly one event");
        events.pop().unwrap()
    }

    #[test]
    fn parse_document_put() {
        let payload = zerompk::to_msgpack_vec(&("orders", "order-1", b"value")).unwrap();
        let record = make_record(RecordType::Put, &payload, 1, 0, 100);
        let mut seq = 0u64;
        let event = one_event(&record, &mut seq);
        assert_eq!(event.collection.as_ref(), "orders");
        assert_eq!(event.row_id.as_str(), "order-1");
        assert_eq!(event.op, WriteOp::Insert);
        assert_eq!(event.lsn, Lsn::new(100));
        assert_eq!(seq, 1);
    }

    #[test]
    fn parse_document_delete() {
        let payload = zerompk::to_msgpack_vec(&("orders", "order-1")).unwrap();
        let record = make_record(RecordType::Delete, &payload, 1, 0, 101);
        let mut seq = 0u64;
        let event = one_event(&record, &mut seq);
        assert_eq!(event.op, WriteOp::Delete);
        assert_eq!(event.row_id.as_str(), "order-1");
    }

    #[test]
    fn parse_kv_put() {
        let payload =
            zerompk::to_msgpack_vec(&("kv_put", "cache", b"key1", b"val1", 0u64)).unwrap();
        let record = make_record(RecordType::Put, &payload, 1, 0, 102);
        let mut seq = 0u64;
        let event = one_event(&record, &mut seq);
        assert_eq!(event.collection.as_ref(), "cache");
        assert_eq!(event.op, WriteOp::Insert);
    }

    #[test]
    fn parse_kv_delete() {
        let payload =
            zerompk::to_msgpack_vec(&("kv_delete", "cache", vec![b"key1".to_vec()])).unwrap();
        let record = make_record(RecordType::Delete, &payload, 1, 0, 103);
        let mut seq = 0u64;
        let event = one_event(&record, &mut seq);
        assert_eq!(event.op, WriteOp::BulkDelete { count: 1 });
    }

    #[test]
    fn vector_records_skipped() {
        let payload = zerompk::to_msgpack_vec(&("vecs", vec![1.0f32, 2.0, 3.0], 3u32)).unwrap();
        let record = make_record(RecordType::VectorPut, &payload, 1, 0, 104);
        let mut seq = 0u64;
        assert!(record_to_events(&record, &mut seq).is_empty());
        assert_eq!(seq, 0); // Not incremented.
    }

    #[test]
    fn checkpoint_records_skipped() {
        let record = make_record(RecordType::Checkpoint, &[], 1, 0, 105);
        let mut seq = 0u64;
        assert!(record_to_events(&record, &mut seq).is_empty());
    }

    #[test]
    fn parse_document_put_with_provenance() {
        // New 4-element arity: (collection, document_id, value, Option<SyncProvenance>).
        let provenance: Option<SyncProvenance> = None;
        let payload =
            zerompk::to_msgpack_vec(&("orders", "order-2", b"value2", provenance)).unwrap();
        let record = make_record(RecordType::Put, &payload, 1, 0, 200);
        let mut seq = 0u64;
        let event = one_event(&record, &mut seq);
        assert_eq!(event.collection.as_ref(), "orders");
        assert_eq!(event.row_id.as_str(), "order-2");
        assert_eq!(event.op, WriteOp::Insert);
        assert_eq!(seq, 1);
    }

    #[test]
    fn parse_document_delete_with_provenance() {
        // New 3-element arity: (collection, document_id, Option<SyncProvenance>).
        let provenance: Option<SyncProvenance> = None;
        let payload = zerompk::to_msgpack_vec(&("orders", "order-2", provenance)).unwrap();
        let record = make_record(RecordType::Delete, &payload, 1, 0, 201);
        let mut seq = 0u64;
        let event = one_event(&record, &mut seq);
        assert_eq!(event.op, WriteOp::Delete);
        assert_eq!(event.row_id.as_str(), "order-2");
        assert_eq!(seq, 1);
    }

    /// A `TransactionRedo` (Calvin cross-shard commit) with two write sub-ops —
    /// a document Put and a KV Put — decomposes into two WriteEvents, both
    /// carrying the redo record's WAL LSN (the watermark-dedup key), with the
    /// same collection/op/value mapping the raw Put arms produce.
    #[test]
    fn transaction_redo_decomposes_into_per_op_events() {
        use crate::wal::{RedoRecord, RedoSubRecord};

        let doc_payload = zerompk::to_msgpack_vec(&("orders", "order-9", b"doc-value")).unwrap();
        let kv_payload = zerompk::to_msgpack_vec(&("kv_put", "cache", b"k9", b"v9", 0u64)).unwrap();
        let redo = RedoRecord {
            version: 1,
            ops: vec![
                RedoSubRecord {
                    record_type: RecordType::Put as u32,
                    payload: doc_payload,
                },
                RedoSubRecord {
                    record_type: RecordType::Put as u32,
                    payload: kv_payload,
                },
            ],
            calvin_stamp: None,
        };
        let record = make_record(
            RecordType::TransactionRedo,
            &redo.to_bytes().unwrap(),
            7,
            0,
            300,
        );

        let mut seq = 0u64;
        let events = record_to_events(&record, &mut seq);
        assert_eq!(events.len(), 2, "one event per write sub-op");

        // Both events carry the enclosing redo record's LSN — the requirement
        // that lets the Event-Plane watermark dedup them against forward events.
        assert!(events.iter().all(|e| e.lsn == Lsn::new(300)));
        // And the enclosing tenant identity.
        assert!(events.iter().all(|e| e.tenant_id == TenantId::new(7)));

        // Sub-op 0: document put.
        assert_eq!(events[0].collection.as_ref(), "orders");
        assert_eq!(events[0].row_id.as_str(), "order-9");
        assert_eq!(events[0].op, WriteOp::Insert);
        // Sub-op 1: KV put.
        assert_eq!(events[1].collection.as_ref(), "cache");
        assert_eq!(events[1].op, WriteOp::Insert);

        // Sequence advanced once per emitted event.
        assert_eq!(seq, 2);
    }

    /// A redo whose write sub-op is preceded by a non-event sub-op (VectorPut,
    /// which has no Event-Plane mapping) still emits the write event, and the
    /// non-event sub-op is skipped without consuming a sequence number.
    #[test]
    fn transaction_redo_skips_non_event_sub_ops() {
        use crate::wal::{RedoRecord, RedoSubRecord};

        let vec_payload = zerompk::to_msgpack_vec(&("vecs", vec![1.0f32, 2.0, 3.0], 3u32)).unwrap();
        let doc_payload = zerompk::to_msgpack_vec(&("orders", "order-x", b"v")).unwrap();
        let redo = RedoRecord {
            version: 1,
            ops: vec![
                RedoSubRecord {
                    record_type: RecordType::VectorPut as u32,
                    payload: vec_payload,
                },
                RedoSubRecord {
                    record_type: RecordType::Put as u32,
                    payload: doc_payload,
                },
            ],
            calvin_stamp: None,
        };
        let record = make_record(
            RecordType::TransactionRedo,
            &redo.to_bytes().unwrap(),
            1,
            0,
            301,
        );

        let mut seq = 0u64;
        let events = record_to_events(&record, &mut seq);
        assert_eq!(events.len(), 1, "only the write sub-op emits");
        assert_eq!(events[0].row_id.as_str(), "order-x");
        assert_eq!(events[0].lsn, Lsn::new(301));
        assert_eq!(seq, 1, "the VectorPut sub-op did not consume a sequence");
    }

    /// A `CalvinApplied` payload-free marker emits no events — its base writes,
    /// if any, ride a separate `TransactionRedo`.
    #[test]
    fn calvin_applied_marker_emits_no_events() {
        let record = make_record(RecordType::CalvinApplied, &[], 1, 0, 302);
        let mut seq = 0u64;
        assert!(record_to_events(&record, &mut seq).is_empty());
        assert_eq!(seq, 0);
    }

    /// A malformed `TransactionRedo` payload is skipped (logged, no panic) and
    /// produces no events.
    #[test]
    fn malformed_transaction_redo_skipped() {
        let record = make_record(RecordType::TransactionRedo, &[0xff, 0xff, 0xff], 1, 0, 303);
        let mut seq = 0u64;
        assert!(record_to_events(&record, &mut seq).is_empty());
        assert_eq!(seq, 0);
    }

    #[test]
    fn graph_edge_put_replays_as_insert_event() {
        // Forward WAL shape for an edge put: (collection, src, label, dst, props).
        let props = b"weight=1".to_vec();
        let payload =
            zerompk::to_msgpack_vec(&("knows", "a", "KNOWS", "b", &props)).expect("encode");
        let record = make_record(RecordType::Put, &payload, 3, 0, 400);
        let mut seq = 0u64;
        let event = one_event(&record, &mut seq);
        assert_eq!(
            event.collection.as_ref(),
            "knows",
            "edge event on its collection"
        );
        assert_eq!(
            event.row_id.as_str(),
            crate::event::graph_cdc::edge_row_id("a", "KNOWS", "b").as_str(),
            "row_id is the (src,label,dst) composition"
        );
        assert_eq!(event.op, WriteOp::Insert);
        assert_eq!(event.lsn, Lsn::new(400));
        assert_eq!(
            event.new_value.as_deref(),
            Some(props.as_slice()),
            "edge properties surface as new_value"
        );
    }

    #[test]
    fn graph_edge_delete_replays_as_delete_event() {
        // Forward WAL shape for an edge delete: (collection, src, label, dst).
        let payload = zerompk::to_msgpack_vec(&("knows", "a", "KNOWS", "b")).expect("encode");
        let record = make_record(RecordType::Delete, &payload, 3, 0, 401);
        let mut seq = 0u64;
        let event = one_event(&record, &mut seq);
        assert_eq!(event.collection.as_ref(), "knows");
        assert_eq!(
            event.row_id.as_str(),
            crate::event::graph_cdc::edge_row_id("a", "KNOWS", "b").as_str()
        );
        assert_eq!(event.op, WriteOp::Delete);
        assert!(event.new_value.is_none() && event.old_value.is_none());
    }

    #[test]
    fn graph_node_label_set_replays_on_label_stream() {
        let payload = zerompk::to_msgpack_vec(&("alice", vec!["Person".to_string()])).expect("enc");
        let record = make_record(RecordType::GraphNodeLabelSet, &payload, 5, 0, 500);
        let mut seq = 0u64;
        let event = one_event(&record, &mut seq);
        assert_eq!(
            event.collection.as_ref(),
            crate::event::graph_cdc::GRAPH_LABEL_STREAM,
            "node-label events surface on the nameable stream, not the NUL sentinel"
        );
        assert_eq!(event.row_id.as_str(), "alice");
        assert_eq!(event.op, WriteOp::Insert);
        assert_eq!(event.lsn, Lsn::new(500));
        // new_value carries the added-labels delta.
        let map = crate::event::deserialize_event_payload(
            event.new_value.as_deref().expect("labels delta present"),
        )
        .expect("delta decodes as object");
        let labels: Vec<&str> = map
            .get("labels")
            .and_then(|v| v.as_array())
            .expect("labels array")
            .iter()
            .filter_map(|v| v.as_str())
            .collect();
        assert_eq!(labels, vec!["Person"]);
    }

    #[test]
    fn graph_node_label_remove_replays_as_delete_event() {
        let payload = zerompk::to_msgpack_vec(&("alice", vec!["Person".to_string()])).expect("enc");
        let record = make_record(RecordType::GraphNodeLabelRemove, &payload, 5, 0, 501);
        let mut seq = 0u64;
        let event = one_event(&record, &mut seq);
        assert_eq!(
            event.collection.as_ref(),
            crate::event::graph_cdc::GRAPH_LABEL_STREAM
        );
        assert_eq!(event.row_id.as_str(), "alice");
        assert_eq!(event.op, WriteOp::Delete);
        assert!(
            event.new_value.is_none() && event.old_value.is_some(),
            "removed labels surface as old_value"
        );
    }

    #[test]
    fn malformed_graph_node_label_record_skipped() {
        let record = make_record(RecordType::GraphNodeLabelSet, &[0xff, 0xff], 5, 0, 502);
        let mut seq = 0u64;
        assert!(record_to_events(&record, &mut seq).is_empty());
        assert_eq!(seq, 0, "malformed label payload consumes no sequence");
    }

    /// A `TransactionRedo` (Calvin cross-shard commit) carrying a graph-edge Put
    /// sub-op decomposes into the same edge WriteEvent the forward path emits —
    /// proving replay parity extends through the redo reconstitution path.
    #[test]
    fn transaction_redo_decomposes_graph_edge_put() {
        use crate::wal::{RedoRecord, RedoSubRecord};

        let props = b"p".to_vec();
        let edge_payload =
            zerompk::to_msgpack_vec(&("knows", "a", "KNOWS", "b", &props)).expect("encode");
        let redo = RedoRecord {
            version: 1,
            ops: vec![RedoSubRecord {
                record_type: RecordType::Put as u32,
                payload: edge_payload,
            }],
            calvin_stamp: None,
        };
        let record = make_record(
            RecordType::TransactionRedo,
            &redo.to_bytes().unwrap(),
            9,
            0,
            600,
        );
        let mut seq = 0u64;
        let events = record_to_events(&record, &mut seq);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].collection.as_ref(), "knows");
        assert_eq!(
            events[0].row_id.as_str(),
            crate::event::graph_cdc::edge_row_id("a", "KNOWS", "b").as_str()
        );
        assert_eq!(events[0].op, WriteOp::Insert);
        assert_eq!(events[0].lsn, Lsn::new(600), "sub-op inherits redo LSN");
    }

    // ── Index-engine / batch-engine replay guards ────────────────────────────
    //
    // Timeseries, Array, FTS, Spatial, and Sparse-vector writes emit NO
    // Data-Plane WriteEvent on the forward path (their CDC, where it exists,
    // rides the Control-Plane change stream, not the WAL→WriteEvent replay
    // stream — see the `record_to_events` arms). `record_to_events`
    // reconstructs exactly the forward stream, so a WELL-FORMED record of each
    // of these types must yield zero events and consume no sequence. These
    // tests use realistic payloads (not garbage) so they prove a deliberate
    // skip, not an incidental decode failure, and guard against a future change
    // reintroducing the recovery-divergence bug (events firing on replay but
    // not on the live write).

    #[test]
    fn timeseries_batch_replays_no_write_event() {
        let prov: Option<SyncProvenance> = None;
        let payload = zerompk::to_msgpack_vec(&("timeseries", "metrics", vec![1u8, 2, 3], prov))
            .expect("enc");
        let record = make_record(RecordType::TimeseriesBatch, &payload, 1, 0, 700);
        let mut seq = 0u64;
        assert!(record_to_events(&record, &mut seq).is_empty());
        assert_eq!(seq, 0, "timeseries batch consumes no sequence");
    }

    #[test]
    fn array_put_and_delete_replay_no_write_event() {
        use crate::engine::array::wal::{
            ArrayDeletePayload, ArrayPutPayload, encode_delete_with_version,
            encode_put_with_version,
        };
        use nodedb_array::types::ArrayId;

        let put = ArrayPutPayload {
            array_id: ArrayId::new(nodedb_types::TenantId::new(1), "genome"),
            cells: Vec::new(),
            provenance: None,
        };
        let put_bytes = encode_put_with_version(&put).expect("enc put");
        let put_record = make_record(RecordType::ArrayPut, &put_bytes, 1, 0, 701);
        let mut seq = 0u64;
        assert!(record_to_events(&put_record, &mut seq).is_empty());

        let del = ArrayDeletePayload {
            array_id: ArrayId::new(nodedb_types::TenantId::new(1), "genome"),
            cells: Vec::new(),
            provenance: None,
        };
        let del_bytes = encode_delete_with_version(&del).expect("enc del");
        let del_record = make_record(RecordType::ArrayDelete, &del_bytes, 1, 0, 702);
        assert!(record_to_events(&del_record, &mut seq).is_empty());
        assert_eq!(seq, 0, "array writes consume no sequence");
    }

    #[test]
    fn fts_index_and_delete_replay_no_write_event() {
        use nodedb_wal::record::{FtsDeletePayload, FtsIndexPayload};

        let prov = SyncProvenance {
            producer_id: 1,
            epoch: 2,
            stream_id: 3,
            seq: 4,
        };
        let idx = FtsIndexPayload::new(prov.clone(), "articles", "doc-1", "hello world")
            .to_bytes()
            .expect("enc idx");
        let idx_record = make_record(RecordType::FtsIndex, &idx, 1, 0, 703);
        let mut seq = 0u64;
        assert!(record_to_events(&idx_record, &mut seq).is_empty());

        let del = FtsDeletePayload::new(prov, "articles", "doc-1")
            .to_bytes()
            .expect("enc del");
        let del_record = make_record(RecordType::FtsDelete, &del, 1, 0, 704);
        assert!(record_to_events(&del_record, &mut seq).is_empty());
        assert_eq!(seq, 0, "fts writes consume no sequence");
    }

    #[test]
    fn spatial_put_and_delete_replay_no_write_event() {
        use nodedb_wal::record::{SpatialDeletePayload, SpatialPutPayload};

        let prov = SyncProvenance {
            producer_id: 5,
            epoch: 6,
            stream_id: 7,
            seq: 8,
        };
        let put = SpatialPutPayload::new(prov.clone(), "places", "loc", "poi-1", vec![0xDE, 0xAD])
            .to_bytes()
            .expect("enc put");
        let put_record = make_record(RecordType::SpatialPut, &put, 1, 0, 705);
        let mut seq = 0u64;
        assert!(record_to_events(&put_record, &mut seq).is_empty());

        let del = SpatialDeletePayload::new(prov, "places", "loc", "poi-1")
            .to_bytes()
            .expect("enc del");
        let del_record = make_record(RecordType::SpatialDelete, &del, 1, 0, 706);
        assert!(record_to_events(&del_record, &mut seq).is_empty());
        assert_eq!(seq, 0, "spatial writes consume no sequence");
    }

    #[test]
    fn sparse_vector_put_and_delete_replay_no_write_event() {
        let entries: Vec<(u32, f32)> = vec![(1, 0.5), (7, 0.25)];
        let put =
            zerompk::to_msgpack_vec(&("embeddings", "sparse", "doc-1", entries)).expect("enc");
        let put_record = make_record(RecordType::SparseVectorPut, &put, 1, 0, 707);
        let mut seq = 0u64;
        assert!(record_to_events(&put_record, &mut seq).is_empty());

        let del = zerompk::to_msgpack_vec(&("embeddings", "sparse", "doc-1")).expect("enc");
        let del_record = make_record(RecordType::SparseVectorDelete, &del, 1, 0, 708);
        assert!(record_to_events(&del_record, &mut seq).is_empty());
        assert_eq!(seq, 0, "sparse-vector writes consume no sequence");
    }

    /// A `TransactionRedo` whose sub-op is an index-engine write (SparseVector)
    /// still emits no event — the decompose path routes each sub-op back
    /// through `record_to_events`, inheriting the same no-forward-event skip, so
    /// a transaction-committed index write does not spuriously fire on replay.
    #[test]
    fn transaction_redo_with_index_sub_op_emits_no_event() {
        use crate::wal::{RedoRecord, RedoSubRecord};

        let entries: Vec<(u32, f32)> = vec![(1, 0.5)];
        let sparse_payload =
            zerompk::to_msgpack_vec(&("embeddings", "sparse", "doc-1", entries)).expect("enc");
        let redo = RedoRecord {
            version: 1,
            ops: vec![RedoSubRecord {
                record_type: RecordType::SparseVectorPut as u32,
                payload: sparse_payload,
            }],
            calvin_stamp: None,
        };
        let record = make_record(
            RecordType::TransactionRedo,
            &redo.to_bytes().unwrap(),
            1,
            0,
            709,
        );
        let mut seq = 0u64;
        assert!(record_to_events(&record, &mut seq).is_empty());
        assert_eq!(seq, 0, "index sub-op consumes no sequence on decompose");
    }

    fn make_record(
        rt: RecordType,
        payload: &[u8],
        tenant_id: u64,
        vshard_id: u32,
        lsn: u64,
    ) -> WalRecord {
        WalRecord::new(nodedb_wal::WalRecordArgs {
            record_type: rt as u32,
            lsn,
            tenant_id,
            vshard_id,
            database_id: 0,
            payload: payload.to_vec(),
            encryption_key: None,
            preamble_bytes: None,
        })
        .unwrap()
    }
}
