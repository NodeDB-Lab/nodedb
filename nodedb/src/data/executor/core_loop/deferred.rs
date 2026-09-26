// SPDX-License-Identifier: BUSL-1.1

//! Deferred trigger events of a committed transaction.
//!
//! The redo install collects the document writes of a committed record and,
//! once the record settled, emits them as WriteEvents. A client transaction's
//! rows carry `EventSource::Deferred`, so the Event Plane fires DEFERRED-mode
//! triggers. Rows of any other source keep that source, so a trigger's own
//! transaction and a restore fire no DEFERRED trigger (see
//! [`committed_row_source`]).

use std::sync::Arc;

use super::CoreLoop;
use crate::engine::document::store::RowIdentity;
use crate::event::types::{EventSource, RowId, WriteEvent, WriteOp};

/// The source a committed record's document-row events carry.
///
/// A client transaction's rows fire DEFERRED-mode triggers, so they carry
/// `Deferred`. Every other source keeps its own: a trigger's transaction
/// does not re-fire triggers, and a restored row fired its triggers when it
/// was first written.
pub(in crate::data::executor) const fn committed_row_source(
    record_source: EventSource,
) -> EventSource {
    match record_source {
        EventSource::User => EventSource::Deferred,
        EventSource::Trigger => EventSource::Trigger,
        EventSource::RaftFollower => EventSource::RaftFollower,
        EventSource::CrdtSync => EventSource::CrdtSync,
        EventSource::Deferred => EventSource::Deferred,
        EventSource::Restore => EventSource::Restore,
    }
}

/// A write that occurred during a transaction, pending deferred trigger emission.
pub(in crate::data::executor) struct DeferredWrite {
    pub collection: String,
    pub op: WriteOp,
    pub identity: RowIdentity,
    pub new_value: Option<Vec<u8>>,
    pub old_value: Option<Vec<u8>>,
}

impl CoreLoop {
    /// Emit the document-row events of a committed transaction.
    ///
    /// Called after a committed redo record installed and settled. Each
    /// write is emitted as a WriteEvent whose source is
    /// [`committed_row_source`] of the record's `record_source`.
    pub(in crate::data::executor) fn emit_deferred_events(
        &mut self,
        writes: Vec<DeferredWrite>,
        record_source: EventSource,
        database_id: crate::types::DatabaseId,
        tenant_id: crate::types::TenantId,
        vshard_id: crate::types::VShardId,
    ) {
        let source = committed_row_source(record_source);
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
                row_id: RowId::row(write.identity),
                lsn: self.watermark,
                // A deferred trigger event repeats a write whose own event
                // already carries the record; catch-up never rebuilds it.
                record: None,
                database_id,
                tenant_id,
                vshard_id,
                source,
                new_value: write.new_value.map(|v| Arc::from(v.as_slice())),
                old_value: write.old_value.map(|v| Arc::from(v.as_slice())),
                system_time_ms,
                valid_time_ms,
                user_id: None,
                statement_digest: None,
            };

            producer.emit(event);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_a_client_transaction_fires_deferred_triggers() {
        assert_eq!(
            committed_row_source(EventSource::User),
            EventSource::Deferred
        );
        assert_eq!(
            committed_row_source(EventSource::Restore),
            EventSource::Restore
        );
        assert_eq!(
            committed_row_source(EventSource::Trigger),
            EventSource::Trigger
        );
        assert_eq!(
            committed_row_source(EventSource::RaftFollower),
            EventSource::RaftFollower
        );
        assert_eq!(
            committed_row_source(EventSource::CrdtSync),
            EventSource::CrdtSync
        );
        assert_eq!(
            committed_row_source(EventSource::Deferred),
            EventSource::Deferred
        );
    }
}
