// SPDX-License-Identifier: BUSL-1.1

use std::sync::Arc;

use super::CoreLoop;

/// Bundled arguments for [`CoreLoop::emit_graph_edge_event`].
pub(in crate::data::executor) struct GraphEdgeEvent<'a> {
    pub collection: &'a str,
    pub src_id: &'a str,
    pub label: &'a str,
    pub dst_id: &'a str,
    pub op: crate::event::WriteOp,
    pub properties: Option<&'a [u8]>,
}

impl CoreLoop {
    /// Convert stored bytes to msgpack for Event Plane consumption.
    ///
    /// For strict collections, the stored format is Binary Tuple which the
    /// Event Plane cannot decode (it lacks the schema). This method converts
    /// Binary Tuple → msgpack so triggers can deserialize the payload.
    /// Returns `None` for schemaless collections (already msgpack).
    pub(in crate::data::executor) fn resolve_event_payload(
        &self,
        database_id: u64,
        tid: u64,
        collection: &str,
        stored_bytes: &[u8],
    ) -> Option<Vec<u8>> {
        let config_key = (
            crate::types::DatabaseId::new(database_id),
            crate::types::TenantId::new(tid),
            collection.to_string(),
        );
        let config = self.doc_configs.get(&config_key)?;
        if let nodedb_physical::physical_plan::StorageMode::Strict { ref schema } =
            config.storage_mode
        {
            crate::data::executor::strict_format::binary_tuple_to_msgpack(stored_bytes, schema)
        } else {
            None
        }
    }

    /// Emit a point write/overwrite/update event derived from the new bytes
    /// produced by the handler and the prior bytes returned from storage.
    ///
    /// Shared by every handler that runs a put-style mutation against a
    /// document engine: PointPut, Upsert (both branches), batched PointPut,
    /// columnar-row overwrite. Each of these knows its *new* bytes and
    /// receives *prior* bytes from the storage API; the Event Plane
    /// payload (`new_value` / `old_value`) is derived from both after
    /// applying the strict→msgpack shim, and the `WriteOp` tag is computed
    /// from their presence.
    pub(in crate::data::executor) fn emit_put_event(
        &mut self,
        task: &super::super::task::ExecutionTask,
        tid: u64,
        collection: &str,
        row_id: &str,
        new_stored: &[u8],
        prior_stored: Option<&[u8]>,
    ) {
        let database_id = task.request.database_id.as_u64();
        let new_converted = self.resolve_event_payload(database_id, tid, collection, new_stored);
        let old_converted =
            prior_stored.and_then(|p| self.resolve_event_payload(database_id, tid, collection, p));
        let old_bytes: Option<&[u8]> = match (prior_stored, old_converted.as_deref()) {
            (Some(_), Some(c)) => Some(c),
            (Some(raw), None) => Some(raw),
            (None, _) => None,
        };
        let op = if old_bytes.is_some() {
            crate::event::WriteOp::Update
        } else {
            crate::event::WriteOp::Insert
        };
        self.emit_write_event(
            task,
            collection,
            op,
            row_id,
            Some(new_converted.as_deref().unwrap_or(new_stored)),
            old_bytes,
        );
    }

    /// Emit a CDC write event for a node-label mutation on the nameable
    /// `__graph_node_labels__` stream ([`crate::event::graph_cdc::GRAPH_LABEL_STREAM`]).
    ///
    /// `SetNodeLabels` maps to [`crate::event::WriteOp::Insert`] with the added
    /// labels as `new_value`; `RemoveNodeLabels` maps to
    /// [`crate::event::WriteOp::Delete`] with the removed labels as `old_value`.
    /// The `row_id` is the (stable) node id. The label delta is serialized by
    /// [`crate::event::graph_cdc::graph_label_delta_value`] — the same encoder the
    /// WAL-replay path uses — so forward and replayed events are byte-identical.
    ///
    /// The node-label write is already durable at its WAL LSN by the time this
    /// runs (the record was appended in the Control Plane before dispatch), so we
    /// advance the core watermark to it — exactly as every other write chokepoint
    /// does via `note_write_lsn` — before emitting. That makes the forward
    /// event's LSN equal the WAL record's LSN the Event-Plane replay uses,
    /// satisfying watermark dedup.
    pub(in crate::data::executor) fn emit_graph_label_event(
        &mut self,
        task: &super::super::task::ExecutionTask,
        node_id: &str,
        labels: &[String],
        op: crate::event::WriteOp,
    ) {
        if let Some(lsn) = task.wal_lsn()
            && lsn > self.watermark
        {
            self.watermark = lsn;
        }
        let value = crate::event::graph_cdc::graph_label_delta_value(labels);
        let stream = crate::event::graph_cdc::GRAPH_LABEL_STREAM;
        let (new_value, old_value): (Option<&[u8]>, Option<&[u8]>) =
            if matches!(op, crate::event::WriteOp::Delete) {
                (None, Some(value.as_slice()))
            } else {
                (Some(value.as_slice()), None)
            };
        self.emit_write_event(task, stream, op, node_id, new_value, old_value);
    }

    /// Emit a CDC write event for a graph edge mutation on the edge's own
    /// `collection`. The stable `row_id` is composed from the `(src, label, dst)`
    /// identity triple via [`crate::event::graph_cdc::edge_row_id`] — the same
    /// composition the WAL-replay path uses.
    ///
    /// The caller MUST have advanced the core watermark to this edge's WAL LSN
    /// (via `note_edge_write_lsn`) before calling, so the forward event's LSN
    /// matches the WAL-replay reconstruction.
    pub(in crate::data::executor) fn emit_graph_edge_event(
        &mut self,
        task: &super::super::task::ExecutionTask,
        edge: GraphEdgeEvent<'_>,
    ) {
        let row_id = crate::event::graph_cdc::edge_row_id(edge.src_id, edge.label, edge.dst_id);
        let (new_value, old_value): (Option<&[u8]>, Option<&[u8]>) =
            if matches!(edge.op, crate::event::WriteOp::Delete) {
                (None, None)
            } else {
                (edge.properties, None)
            };
        self.emit_write_event(
            task,
            edge.collection,
            edge.op,
            &row_id,
            new_value,
            old_value,
        );
    }

    /// Set the Event Plane producer (called after open, before event loop).
    pub fn set_event_producer(&mut self, producer: crate::event::bus::EventProducer) {
        self.event_producer = Some(producer);
    }

    /// Emit a write event to the Event Plane.
    ///
    /// Called after a successful write (PointPut, PointDelete, PointUpdate,
    /// BatchInsert, BulkDelete, atomic KV ops, etc.). The Data Plane NEVER
    /// blocks here — if the ring buffer is full, the event is dropped and
    /// the Event Plane will detect the gap via sequence numbers and replay
    /// from WAL.
    ///
    /// Prefer [`CoreLoop::emit_put_event`] for any handler that performs a
    /// put-style mutation against a document engine — it derives the
    /// Insert/Update tag from the prior bytes returned by storage so the
    /// emit site cannot disagree with what the row actually did. This
    /// lower-level entry point stays for paths where the op is structurally
    /// determined by the operation itself (kv-atomic increment, CAS, plain
    /// delete) rather than by inspecting pre/post state.
    pub(in crate::data::executor) fn emit_write_event(
        &mut self,
        task: &super::super::task::ExecutionTask,
        collection: &str,
        op: crate::event::WriteOp,
        row_id: &str,
        new_value: Option<&[u8]>,
        old_value: Option<&[u8]>,
    ) {
        let producer = match self.event_producer.as_mut() {
            Some(p) => p,
            None => return, // Event Plane not configured.
        };

        self.event_sequence += 1;

        let (system_time_ms, valid_time_ms) =
            crate::event::bitemporal_extract::extract_stamps(new_value.or(old_value));

        let event = crate::event::WriteEvent {
            sequence: self.event_sequence,
            collection: Arc::from(collection),
            op,
            row_id: crate::event::types::RowId::new(row_id),
            lsn: self.watermark,
            tenant_id: task.request.tenant_id,
            vshard_id: task.request.vshard_id,
            source: task.request.event_source,
            new_value: new_value.map(Arc::from),
            old_value: old_value.map(Arc::from),
            system_time_ms,
            valid_time_ms,
            user_id: task.request.user_id.clone(),
            statement_digest: task.request.statement_digest.clone(),
        };

        producer.emit(event);
    }

    /// Emit a heartbeat event to advance the Event Plane's partition watermark.
    ///
    /// Called when no user writes occur for >1 second. The heartbeat carries
    /// the current watermark LSN so the Event Plane can advance its partition
    /// watermark without waiting for user writes.
    pub fn emit_heartbeat(&mut self) {
        let producer = match self.event_producer.as_mut() {
            Some(p) => p,
            None => return,
        };

        self.event_sequence += 1;

        let event = crate::event::WriteEvent {
            sequence: self.event_sequence,
            collection: Arc::from("_heartbeat"),
            op: crate::event::WriteOp::Heartbeat,
            row_id: crate::event::types::RowId::new(""),
            // watermark = last committed LSN. Correct for heartbeats: uncommitted
            // writes should NOT advance the Event Plane's watermark.
            lsn: self.watermark,
            // Default tenant; vshard derived from core_id for partition routing.
            tenant_id: crate::types::TenantId::new(0),
            vshard_id: crate::types::VShardId::new(
                (self.core_id % crate::types::VShardId::COUNT as usize) as u32,
            ),
            source: crate::event::EventSource::User,
            new_value: None,
            old_value: None,
            system_time_ms: None,
            valid_time_ms: None,
            user_id: None,
            statement_digest: None,
        };

        producer.emit(event);
    }
}
