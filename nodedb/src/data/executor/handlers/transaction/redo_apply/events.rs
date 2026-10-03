// SPDX-License-Identifier: BUSL-1.1

//! Event Plane output of a committed redo apply.
//!
//! The transaction batch emitted three kinds of events, and the apply emits
//! the same set:
//!
//! * one event per document row written (source rows and materialized-sum
//!   targets), carrying the pre-image, under
//!   [`EventSource::committed_row_source`] of the record's source;
//! * one event per KV key written, from the KV write handlers;
//! * one event per graph node-label delta, on the label stream.
//!
//! KV and label rows carry [`EventSource::committed_other_source`]. A row the
//! record lists in its row sources (a trigger body's write) carries that
//! source instead ([`EventSource::committed_row_override`]). A label delta is
//! listed by its node id on the label stream.
//!
//! Graph edges, CRDT rows and timeseries rows emit from the handlers their
//! replay arms call. The install holds those events and gives each its
//! listed source and the record's commit HLC. A timeseries sub-record emits
//! the event images it carries, present only when some Event Plane consumer
//! read its collection at resolve. The index engines
//! (vector, FTS, spatial), arrays, columnar and spatial collections emit no
//! Data-Plane events on either path.
//!
//! After its rows, the record emits one publish event per `PUBLISH TO`
//! message its transaction's bodies sent (see `crate::event::topic::committed`).

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::KvWriteEvent;
use crate::data::executor::core_loop::deferred::DeferredWrite;
use crate::data::executor::core_loop::event_emit::RowWriteEvent;
use crate::data::executor::task::ExecutionTask;
use crate::event::image_fault::ImageFault;
use crate::event::types::RowId;
use crate::event::{EventSource, WriteOp};
use crate::wal::{RedoPublish, RowSourceIndex};

use super::state::AppliedDocWrite;
use super::sub_ops::{RedoKvOp, RedoLabelOp};

/// One KV key a redo record writes, with the value it held before.
pub(super) struct KvPriorImage {
    collection: String,
    key: Vec<u8>,
    /// `Some` for a put, `None` for a delete.
    new_value: Option<Vec<u8>>,
    prior: Option<Vec<u8>>,
}

impl CoreLoop {
    /// Read the current value of every KV key `ops` writes, before the KV arm
    /// overwrites it.
    pub(super) fn kv_prior_images(
        &self,
        database_id: u64,
        tid: u64,
        ops: Vec<RedoKvOp>,
    ) -> Vec<KvPriorImage> {
        let now_ms = self.kv_read_now_ms();
        let mut images = Vec::new();
        for op in ops {
            match op {
                RedoKvOp::Put {
                    collection,
                    key,
                    value,
                } => {
                    let prior = self
                        .kv_engine
                        .get(database_id, tid, &collection, &key, now_ms);
                    images.push(KvPriorImage {
                        collection,
                        key,
                        new_value: Some(value),
                        prior,
                    });
                }
                RedoKvOp::Delete { collection, keys } => {
                    for key in keys {
                        let prior = self
                            .kv_engine
                            .get(database_id, tid, &collection, &key, now_ms);
                        images.push(KvPriorImage {
                            collection: collection.clone(),
                            key,
                            new_value: None,
                            prior,
                        });
                    }
                }
            }
        }
        images
    }

    /// Emit every event the committed record's writes produce. `rows` names
    /// the rows whose writes ran with another source than the record's.
    pub(super) fn emit_committed_redo_events(
        &mut self,
        task: &ExecutionTask,
        doc_writes: Vec<AppliedDocWrite>,
        kv_images: Vec<KvPriorImage>,
        labels: Vec<RedoLabelOp>,
        rows: &RowSourceIndex,
    ) {
        let record_source = task.request.event_source;
        let other_source = record_source.committed_other_source();
        // Every event names the record's LSN; the arms advanced the watermark
        // to it for the rows they versioned, and this covers a record whose
        // writes versioned none.
        if let Some(lsn) = task.wal_lsn()
            && lsn > self.watermark
        {
            self.watermark = lsn;
        }

        for image in kv_images {
            let source = committed_source(
                rows,
                record_source,
                other_source,
                &image.collection,
                &String::from_utf8_lossy(&image.key),
            );
            match image.new_value {
                Some(new_value) => {
                    let op = if image.prior.is_some() {
                        WriteOp::Update
                    } else {
                        WriteOp::Insert
                    };
                    self.emit_kv_write_event_as(
                        task,
                        source,
                        KvWriteEvent {
                            collection: &image.collection,
                            op,
                            key: &image.key,
                            new_stored: Some(new_value.as_slice()),
                            old_stored: image.prior.as_deref(),
                        },
                    );
                }
                // A delete of an absent key removed nothing.
                None if image.prior.is_some() => {
                    self.emit_kv_write_event_as(
                        task,
                        source,
                        KvWriteEvent {
                            collection: &image.collection,
                            op: WriteOp::Delete,
                            key: &image.key,
                            new_stored: None,
                            old_stored: image.prior.as_deref(),
                        },
                    );
                }
                None => {}
            }
        }

        for label in labels {
            let op = if label.is_set {
                WriteOp::Insert
            } else {
                WriteOp::Delete
            };
            let source = committed_source(
                rows,
                record_source,
                other_source,
                crate::event::graph_cdc::GRAPH_LABEL_STREAM,
                &label.node_id,
            );
            self.emit_graph_label_event_as(task, source, &label.node_id, &label.labels, op);
        }

        // Each row carries its post-image and pre-image as the Event Plane
        // reads them, the same as an autocommit write's event: an AFTER INSERT
        // or UPDATE trigger binds NEW, and CDC and views read the new row.
        let database_id = task.request.database_id.as_u64();
        let tid = task.request.tenant_id.as_u64();
        let document_source = record_source.committed_row_source();
        let deferred: Vec<DeferredWrite> = doc_writes
            .into_iter()
            .map(|write| {
                let id = write.identity.as_str();
                let new_value = write.new_body.as_deref().map(|body| {
                    self.body_event_image(database_id, tid, &write.collection, id, body)
                });
                // A stored strict row that does not decode has no image: the
                // event names the fault instead.
                let old_image = write.old_value.as_deref().map(|stored| {
                    self.stored_event_image(database_id, tid, &write.collection, id, stored)
                });
                let image_fault = ImageFault::of(false, matches!(old_image, Some(Err(_))));
                let old_value = old_image.and_then(Result::ok);
                let source =
                    committed_source(rows, record_source, document_source, &write.collection, id);
                DeferredWrite {
                    new_value,
                    old_value,
                    image_fault,
                    source,
                    collection: write.collection,
                    op: write.op,
                    identity: write.identity,
                }
            })
            .collect();
        if !deferred.is_empty() {
            self.emit_deferred_events(
                deferred,
                task.request.database_id,
                task.request.tenant_id,
                task.request.vshard_id,
                task.request.commit_hlc,
            );
        }
    }
}

impl CoreLoop {
    /// Emit one publish event per message the committed record carries, in
    /// record order. WAL catch-up rebuilds the same events from the record.
    pub(super) fn emit_committed_publishes(
        &mut self,
        task: &ExecutionTask,
        publishes: &[RedoPublish],
    ) {
        let source = task.request.event_source.committed_other_source();
        for (ordinal, publish) in (0u32..).zip(publishes) {
            let value = match crate::event::topic::encode_publish(ordinal, publish) {
                Ok(value) => value,
                Err(error) => {
                    tracing::error!(
                        topic = %publish.topic,
                        error = %error,
                        "a committed publish did not encode; no event emitted for it"
                    );
                    continue;
                }
            };
            let stream = crate::event::topic::publish_stream(&publish.topic);
            self.emit_event_with_row_id_as(
                task,
                source,
                RowWriteEvent {
                    collection: &stream,
                    op: WriteOp::Publish,
                    row_id: RowId::Batch,
                    new_value: Some(&value),
                    old_value: None,
                    image_fault: None,
                },
            );
        }
    }
}

/// The source a committed row carries: its row-source group's, when the
/// record lists it, else `default` for its kind of row.
pub(in crate::data::executor) fn committed_source(
    rows: &RowSourceIndex,
    record_source: EventSource,
    default: EventSource,
    collection: &str,
    row: &str,
) -> EventSource {
    match rows.source_of(collection, row) {
        Some(listed) => record_source.committed_row_override(listed),
        None => default,
    }
}
