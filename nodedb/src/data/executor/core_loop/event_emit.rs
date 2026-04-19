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

    /// Set the Event Plane producer (called after open, before event loop).
    pub fn set_event_producer(&mut self, producer: crate::event::bus::EventProducer) {
        self.event_producer = Some(producer);
    }

    /// Emit a write event to the Event Plane.
    ///
    /// Called after a successful write (PointPut, PointDelete, PointUpdate,
    /// BatchInsert, BulkDelete, etc.). The Data Plane NEVER blocks here —
    /// if the ring buffer is full, the event is dropped and the Event Plane
    /// will detect the gap via sequence numbers and replay from WAL.
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
        };

        producer.emit(event);
    }
}
