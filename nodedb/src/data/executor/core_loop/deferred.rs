//! Deferred trigger event collection during transaction batches.
//!
//! Accumulates write metadata during `execute_transaction_batch()`.
//! After successful commit, emits these as WriteEvents with
//! `EventSource::Deferred` so the Event Plane fires DEFERRED-mode triggers.

use std::sync::Arc;

use super::CoreLoop;
use crate::event::types::{EventSource, RowId, WriteEvent, WriteOp};

/// A write that occurred during a transaction, pending deferred trigger emission.
pub(in crate::data::executor) struct DeferredWrite {
    pub collection: String,
    pub op: WriteOp,
    pub row_id: String,
    pub new_value: Option<Vec<u8>>,
    pub old_value: Option<Vec<u8>>,
}

impl CoreLoop {
    /// Emit deferred trigger events for a completed transaction batch.
    ///
    /// Called after `execute_transaction_batch()` commits successfully.
    /// Each write in the transaction is emitted as a WriteEvent with
    /// `EventSource::Deferred`, which the Event Plane consumer routes
    /// to DEFERRED-mode triggers.
    pub(in crate::data::executor) fn emit_deferred_events(
        &mut self,
        writes: Vec<DeferredWrite>,
        tenant_id: crate::types::TenantId,
        vshard_id: crate::types::VShardId,
    ) {
        let producer = match self.event_producer.as_mut() {
            Some(p) => p,
            None => return,
        };

        for write in writes {
            self.event_sequence += 1;

            let (system_time_ms, valid_time_ms) = crate::event::bitemporal_extract::extract_stamps(
                write.new_value.as_deref().or(write.old_value.as_deref()),
            );
            let event = WriteEvent {
                sequence: self.event_sequence,
                collection: Arc::from(write.collection.as_str()),
                op: write.op,
                row_id: RowId::new(write.row_id.as_str()),
                lsn: self.watermark,
                tenant_id,
                vshard_id,
                source: EventSource::Deferred,
                new_value: write.new_value.map(|v| Arc::from(v.as_slice())),
                old_value: write.old_value.map(|v| Arc::from(v.as_slice())),
                system_time_ms,
                valid_time_ms,
            };

            producer.emit(event);
        }
    }
}
