// SPDX-License-Identifier: BUSL-1.1

//! Deferred trigger events of a committed transaction.
//!
//! The redo install collects the document writes of a committed record and,
//! once the record settled, emits them as WriteEvents. Each write carries the
//! source the install resolved for it: a client transaction's rows carry
//! `EventSource::Deferred`, so the Event Plane fires DEFERRED-mode triggers,
//! and a trigger body's rows carry `Trigger` (see
//! [`EventSource::committed_row_source`]).

use std::sync::Arc;

use super::CoreLoop;
use crate::engine::document::store::RowIdentity;
use crate::event::types::{EventSource, RowId, WriteEvent, WriteOp};

/// A write that occurred during a transaction, pending deferred trigger emission.
pub(in crate::data::executor) struct DeferredWrite {
    pub collection: String,
    pub op: WriteOp,
    pub identity: RowIdentity,
    /// The source the row's event carries.
    pub source: EventSource,
    pub new_value: Option<Vec<u8>>,
    pub old_value: Option<Vec<u8>>,
    /// The image that did not render; its slot above is `None`.
    pub image_fault: Option<crate::event::image_fault::ImageFault>,
}

impl CoreLoop {
    /// Emit the document-row events of a committed transaction.
    ///
    /// Called after a committed redo record installed and settled. Each
    /// write is emitted as a WriteEvent under its own source, dated by the
    /// transaction's `commit_hlc`.
    pub(in crate::data::executor) fn emit_deferred_events(
        &mut self,
        writes: Vec<DeferredWrite>,
        database_id: crate::types::DatabaseId,
        tenant_id: crate::types::TenantId,
        vshard_id: crate::types::VShardId,
        commit_hlc: Option<u64>,
    ) {
        if self.events.producer.is_none() {
            return;
        }

        for write in writes {
            let (system_time_ms, valid_time_ms) = crate::event::bitemporal_extract::extract_stamps(
                write.new_value.as_deref().or(write.old_value.as_deref()),
            );
            let event = WriteEvent {
                // `send_write_event` numbers it.
                sequence: 0,
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
                source: write.source,
                new_value: write.new_value.map(|v| Arc::from(v.as_slice())),
                old_value: write.old_value.map(|v| Arc::from(v.as_slice())),
                system_time_ms,
                valid_time_ms,
                user_id: None,
                statement_digest: None,
                commit_hlc,
                image_fault: write.image_fault,
            };

            self.send_write_event(event);
        }
    }
}
