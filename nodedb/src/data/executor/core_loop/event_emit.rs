use std::sync::Arc;

use super::CoreLoop;

impl CoreLoop {
    /// Convert stored bytes to msgpack for Event Plane consumption.
    ///
    /// For strict collections, the stored format is Binary Tuple which the
    /// Event Plane cannot decode (it lacks the schema). This method converts
    /// Binary Tuple → msgpack so triggers can deserialize the payload.
    /// Returns `None` for schemaless collections (already msgpack).
    pub(in crate::data::executor) fn resolve_event_payload(
        &self,
        tid: u32,
        collection: &str,
        stored_bytes: &[u8],
    ) -> Option<Vec<u8>> {
        let config_key = (crate::types::TenantId::new(tid), collection.to_string());
        let config = self.doc_configs.get(&config_key)?;
        if let crate::bridge::physical_plan::StorageMode::Strict { ref schema } =
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
        tid: u32,
        collection: &str,
        row_id: &str,
        new_stored: &[u8],
        prior_stored: Option<&[u8]>,
    ) {
        let new_converted = self.resolve_event_payload(tid, collection, new_stored);
        let old_converted =
            prior_stored.and_then(|p| self.resolve_event_payload(tid, collection, p));
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
                (self.core_id % crate::types::VShardId::COUNT as usize) as u16,
            ),
            source: crate::event::EventSource::User,
            new_value: None,
            old_value: None,
            system_time_ms: None,
            valid_time_ms: None,
        };

        producer.emit(event);
    }
}
