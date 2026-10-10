// SPDX-License-Identifier: BUSL-1.1

use std::borrow::Cow;
use std::sync::Arc;

use super::CoreLoop;
use super::event_image::{StoredImage, UndecodableImage};
use crate::event::image_fault::ImageFault;

/// Bundled arguments for [`CoreLoop::emit_graph_edge_event`].
pub(in crate::data::executor) struct GraphEdgeEvent<'a> {
    pub collection: &'a str,
    pub src_id: &'a str,
    pub label: &'a str,
    pub dst_id: &'a str,
    /// The endpoints' bound identities, from the plan.
    pub src_surrogate: nodedb_types::Surrogate,
    pub dst_surrogate: nodedb_types::Surrogate,
    pub op: crate::event::WriteOp,
    pub properties: Option<&'a [u8]>,
}

/// Bundled arguments for [`CoreLoop::emit_event_with_row_id_as`].
pub(in crate::data::executor) struct RowWriteEvent<'a> {
    pub collection: &'a str,
    pub op: crate::event::WriteOp,
    pub row_id: crate::event::types::RowId,
    pub new_value: Option<&'a [u8]>,
    pub old_value: Option<&'a [u8]>,
    /// The image that did not render; its slot above is `None`.
    pub image_fault: Option<ImageFault>,
}

impl CoreLoop {
    /// Emit a point write/overwrite/update event derived from the new bytes
    /// produced by the handler and the prior bytes returned from storage.
    ///
    /// Shared by every handler that runs a put-style mutation against a
    /// document engine: PointPut, Upsert (both branches), batched PointPut,
    /// columnar-row overwrite. Each of these knows its *new* bytes and
    /// receives *prior* bytes from the storage API. Both become MessagePack
    /// images through [`Self::stored_event_image`], and the `WriteOp` tag is
    /// computed from the prior's presence.
    ///
    /// A schemaless body that lacks its identity column carries its identity
    /// only in the storage key. The image gains that identity under the
    /// identity column, as every read path injects it via `sparse_row_to_doc`,
    /// so a WHEN filter, CDC, or change stream reads the same row a query
    /// returns. This identity also becomes the `WriteEvent.row_id`.
    ///
    /// A stored strict row that does not decode has no image: its slot is
    /// `None` and the event names the fault.
    pub(in crate::data::executor) fn emit_put_event(
        &mut self,
        task: &super::super::task::ExecutionTask,
        tid: u64,
        collection: &str,
        identity: crate::engine::document::store::RowIdentity,
        new_stored: &[u8],
        prior_stored: Option<&[u8]>,
    ) {
        let database_id = task.request.database_id.as_u64();
        let id = identity.as_str();
        let new_image = self.stored_event_image(database_id, tid, collection, id, new_stored);
        let old_image = prior_stored
            .map(|prior| self.stored_event_image(database_id, tid, collection, id, prior));
        let op = if prior_stored.is_some() {
            crate::event::WriteOp::Update
        } else {
            crate::event::WriteOp::Insert
        };
        let image_fault = ImageFault::of(
            new_image.is_err(),
            matches!(old_image, Some(Err(UndecodableImage))),
        );
        self.emit_event_with_row_id_as(
            task,
            task.request.event_source,
            RowWriteEvent {
                collection,
                op,
                row_id: crate::event::types::RowId::row(identity),
                new_value: new_image.as_deref().ok(),
                old_value: old_image.as_ref().and_then(|image| image.as_deref().ok()),
                image_fault,
            },
        );
    }

    /// Emit a document-row DELETE event to the Event Plane.
    ///
    /// `identity` is the client-visible identity of the deleted row.
    /// `old_stored` is the row as storage held it. A strict Binary Tuple
    /// becomes MessagePack. A stored strict row that does not decode has no
    /// image: the slot is `None` and the event names the fault.
    pub(in crate::data::executor) fn emit_document_delete_event(
        &mut self,
        task: &super::super::task::ExecutionTask,
        tid: u64,
        collection: &str,
        identity: crate::engine::document::store::RowIdentity,
        old_stored: Option<&[u8]>,
    ) {
        let database_id = task.request.database_id.as_u64();
        let old_image = old_stored.map(|stored| {
            match self.resolve_event_payload(database_id, tid, collection, stored) {
                StoredImage::Converted(converted) => Ok(Cow::Owned(converted)),
                StoredImage::AsStored => Ok(Cow::Borrowed(stored)),
                StoredImage::Undecodable => Err(UndecodableImage),
            }
        });
        let image_fault = ImageFault::of(false, matches!(old_image, Some(Err(UndecodableImage))));
        self.emit_event_with_row_id_as(
            task,
            task.request.event_source,
            RowWriteEvent {
                collection,
                op: crate::event::WriteOp::Delete,
                row_id: crate::event::types::RowId::row(identity),
                new_value: None,
                old_value: old_image.as_ref().and_then(|image| image.as_deref().ok()),
                image_fault,
            },
        );
    }

    /// Set the Event Plane producer (called after open, before event loop).
    /// The producer's fail points take this core's scope.
    pub fn set_event_producer(&mut self, producer: crate::event::bus::EventProducer) {
        self.events.producer = Some(producer);
        #[cfg(feature = "failpoints")]
        if let Some(producer) = self.events.producer.as_mut() {
            producer.set_fail_scope(self.fail_scope);
        }
    }

    /// Emit a write event carrying any [`crate::event::types::RowId`].
    ///
    /// Called after a successful write. The Data Plane never blocks here: if
    /// the ring buffer is full, the event is dropped and the Event Plane
    /// detects the gap by sequence number and replays from the WAL. Document
    /// puts and deletes use [`CoreLoop::emit_put_event`] and
    /// [`CoreLoop::emit_document_delete_event`], which render the row image.
    pub(in crate::data::executor) fn emit_event_with_row_id(
        &mut self,
        task: &super::super::task::ExecutionTask,
        collection: &str,
        op: crate::event::WriteOp,
        row_id: crate::event::types::RowId,
        new_value: Option<&[u8]>,
        old_value: Option<&[u8]>,
    ) {
        self.emit_event_with_row_id_as(
            task,
            task.request.event_source,
            RowWriteEvent {
                collection,
                op,
                row_id,
                new_value,
                old_value,
                image_fault: None,
            },
        );
    }

    /// [`Self::emit_event_with_row_id`] under `source` instead of the
    /// request's.
    pub(in crate::data::executor) fn emit_event_with_row_id_as(
        &mut self,
        task: &super::super::task::ExecutionTask,
        source: crate::event::EventSource,
        event: RowWriteEvent<'_>,
    ) {
        let RowWriteEvent {
            collection,
            op,
            row_id,
            new_value,
            old_value,
            image_fault,
        } = event;
        if self.events.producer.is_none() {
            return; // Event Plane not configured.
        }

        let (system_time_ms, valid_time_ms) =
            crate::event::bitemporal_extract::extract_stamps(new_value.or(old_value));

        let event = crate::event::WriteEvent {
            // Assigned when the event is sent, so a held event never leaves
            // a gap in the sequence.
            sequence: 0,
            collection: Arc::from(collection),
            op,
            row_id,
            lsn: self.watermark,
            record: task
                .wal_lsn()
                .map(crate::event::types::RecordPosition::first),
            database_id: task.request.database_id,
            tenant_id: task.request.tenant_id,
            vshard_id: task.request.vshard_id,
            source,
            new_value: new_value.map(Arc::from),
            old_value: old_value.map(Arc::from),
            system_time_ms,
            valid_time_ms,
            user_id: task.request.user_id.clone(),
            statement_digest: task.request.statement_digest.clone(),
            commit_hlc: task.request.commit_hlc,
            image_fault,
        };

        // The install pass of a committed-redo apply holds its events until
        // the whole record landed. A rolled-back install sends none.
        if let Some(scope) = self.redo_apply.scope.as_mut()
            && scope.pass
                == crate::data::executor::handlers::transaction::redo_apply::RedoApplyPass::Install
        {
            scope.pending_events.push(event);
            return;
        }
        self.send_write_event(event);
    }

    /// Number `event` with the next sequence and hand it to the Event Plane.
    /// A journalled task's events wait until its write set is stored (see
    /// `write_set_journal`).
    pub(in crate::data::executor) fn send_write_event(
        &mut self,
        mut event: crate::event::WriteEvent,
    ) {
        if let Some(held) = self.write_set_journal.held_events.as_mut() {
            held.push(event);
            return;
        }
        let Some(producer) = self.events.producer.as_mut() else {
            return;
        };
        self.events.sequence += 1;
        event.sequence = self.events.sequence;
        producer.emit(event);
    }

    /// Emit a heartbeat event to advance the Event Plane's partition watermark.
    ///
    /// Called when no user writes occur for >1 second. The heartbeat carries
    /// the current watermark LSN so the Event Plane can advance its partition
    /// watermark without waiting for user writes.
    pub fn emit_heartbeat(&mut self) {
        let producer = match self.events.producer.as_mut() {
            Some(p) => p,
            None => return,
        };

        self.events.sequence += 1;

        let event = crate::event::WriteEvent {
            sequence: self.events.sequence,
            collection: Arc::from("_heartbeat"),
            op: crate::event::WriteOp::Heartbeat,
            row_id: crate::event::types::RowId::Heartbeat,
            // watermark = last committed LSN. Correct for heartbeats: uncommitted
            // writes should NOT advance the Event Plane's watermark.
            lsn: self.watermark,
            record: None,
            // Heartbeats are synthetic core-liveness markers rather than data writes,
            // so they have no database owner and are excluded from CDC routing.
            database_id: crate::types::DatabaseId::DEFAULT,
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
            commit_hlc: None,
            image_fault: None,
        };

        producer.emit(event);
    }
}
