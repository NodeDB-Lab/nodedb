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

//!
//! Every reader streams from the install's spools (see `batched::spools`),
//! so the events of a large record never sit in RAM at once.

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::KvWriteEvent;
use crate::data::executor::core_loop::deferred::DeferredWrite;
use crate::data::executor::core_loop::event_emit::RowWriteEvent;
use crate::data::executor::handlers::spill::run::BlockReader;
use crate::data::executor::handlers::spill::tiered::{SpillWeight, Spool};
use crate::data::executor::task::ExecutionTask;
use crate::event::image_fault::ImageFault;
use crate::event::types::RowId;
use crate::event::{EventSource, WriteOp};
use crate::wal::RedoPublish;

use crate::data::executor::core_loop::held_events::{EventSpool, HEADER};

use super::batched::{HeldRowSources, SpooledDocWrite};
use super::state::AppliedDocWrite;
use super::sub_ops::{RedoKvOp, RedoLabelOp};

/// Document row events handed to the Event Plane together.
const DOC_EVENT_CHUNK: usize = 256;

/// One KV key a redo record writes, with the value it held before.
#[derive(Debug, Clone, PartialEq, zerompk::ToMessagePack, zerompk::FromMessagePack)]
pub(super) struct KvPriorImage {
    pub(super) collection: String,
    key: Vec<u8>,
    /// `Some` for a put, `None` for a delete.
    new_value: Option<Vec<u8>>,
    prior: Option<Vec<u8>>,
}

impl SpillWeight for KvPriorImage {
    fn spill_weight(&self) -> usize {
        let bytes = |value: &Option<Vec<u8>>| HEADER + value.as_ref().map_or(0, Vec::len);
        self.collection.spill_weight()
            + self.key.spill_weight()
            + bytes(&self.new_value)
            + bytes(&self.prior)
    }
}

/// What the events of a settled record read: its row sources, and the
/// reader of its spilled spools.
pub(super) struct EventRows<'a> {
    pub rows: &'a HeldRowSources,
    pub reader: &'a BlockReader,
}

impl EventRows<'_> {
    /// The source a committed row carries: its row-source group's, when
    /// the record lists it, else `default` for its kind of row.
    fn source(
        &self,
        record_source: EventSource,
        default: EventSource,
        collection: &str,
        row: &str,
    ) -> crate::Result<EventSource> {
        let listed = self.rows.source_of(collection, row, self.reader)?;
        Ok(committed_source(listed, record_source, default))
    }
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
        self.kv_prior_images_at(database_id, tid, ops, self.kv_read_now_ms())
    }

    /// [`Self::kv_prior_images`] read at `now_ms`: a value expired at
    /// `now_ms` reads as absent.
    pub(super) fn kv_prior_images_at(
        &self,
        database_id: u64,
        tid: u64,
        ops: Vec<RedoKvOp>,
        now_ms: u64,
    ) -> Vec<KvPriorImage> {
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

    /// Emit every event the committed record's writes produce, in the
    /// order the spools hold them: KV keys, node labels, then document
    /// rows. `rows` names the rows whose writes ran with another source
    /// than the record's.
    pub(super) fn emit_committed_redo_events(
        &mut self,
        task: &ExecutionTask,
        written: CommittedWrites,
        rows: &EventRows<'_>,
    ) -> crate::Result<()> {
        let CommittedWrites {
            doc_writes,
            kv_priors,
            labels,
        } = written;
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

        kv_priors.drain(|image| {
            let key = String::from_utf8_lossy(&image.key);
            let source = rows.source(record_source, other_source, &image.collection, &key)?;
            self.emit_kv_image_event(task, source, image);
            Ok(())
        })?;

        labels.drain(|label| {
            let op = if label.is_set {
                WriteOp::Insert
            } else {
                WriteOp::Delete
            };
            let source = rows.source(
                record_source,
                other_source,
                crate::event::graph_cdc::GRAPH_LABEL_STREAM,
                &label.node_id,
            )?;
            self.emit_graph_label_event_as(task, source, &label.node_id, &label.labels, op);
            Ok(())
        })?;

        // Each row carries its post-image and pre-image as the Event Plane
        // reads them, the same as an autocommit write's event: an AFTER INSERT
        // or UPDATE trigger binds NEW, and CDC and views read the new row.
        let mut chunk: Vec<DeferredWrite> = Vec::new();
        doc_writes.drain(|write| {
            let write = write.into_write()?;
            if !write.emits_event {
                return Ok(());
            }
            chunk.push(self.doc_write_event(task, write, rows)?);
            if chunk.len() >= DOC_EVENT_CHUNK {
                self.emit_doc_write_events(task, std::mem::take(&mut chunk));
            }
            Ok(())
        })?;
        self.emit_doc_write_events(task, chunk);
        Ok(())
    }

    /// The event of one KV key image.
    fn emit_kv_image_event(
        &mut self,
        task: &ExecutionTask,
        source: EventSource,
        image: KvPriorImage,
    ) {
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

    /// The deferred event of one document row the record wrote.
    fn doc_write_event(
        &self,
        task: &ExecutionTask,
        write: AppliedDocWrite,
        rows: &EventRows<'_>,
    ) -> crate::Result<DeferredWrite> {
        let database_id = task.request.database_id.as_u64();
        let tid = task.request.tenant_id.as_u64();
        let record_source = task.request.event_source;
        let id = write.identity.as_str();
        let (new_value, old_value, image_fault, op) = match write.event {
            Some(event) => {
                let op = if event.old_value.is_some() {
                    WriteOp::Update
                } else {
                    WriteOp::Insert
                };
                (Some(event.new_value), event.old_value, None, op)
            }
            None => {
                // A deleted strict row that does not decode names its image fault.
                let old_image = write.old_value.as_deref().map(|stored| {
                    self.stored_event_image(database_id, tid, &write.collection, id, stored)
                });
                let image_fault = ImageFault::of(false, matches!(old_image, Some(Err(_))));
                (None, old_image.and_then(Result::ok), image_fault, write.op)
            }
        };
        let source = rows.source(
            record_source,
            record_source.committed_row_source(),
            &write.collection,
            id,
        )?;
        Ok(DeferredWrite {
            new_value,
            old_value,
            image_fault,
            source,
            collection: write.collection,
            op,
            identity: write.identity,
        })
    }

    fn emit_doc_write_events(&mut self, task: &ExecutionTask, writes: Vec<DeferredWrite>) {
        if writes.is_empty() {
            return;
        }
        self.emit_deferred_events(
            writes,
            task.request.database_id,
            task.request.tenant_id,
            task.request.vshard_id,
            task.request.commit_hlc,
        );
    }

    /// Emit one publish event per message the committed record carries, in
    /// record order. WAL catch-up rebuilds the same events from the record.
    pub(super) fn emit_committed_publishes(
        &mut self,
        task: &ExecutionTask,
        publishes: Spool<RedoPublish>,
    ) -> crate::Result<()> {
        let source = task.request.event_source.committed_other_source();
        let mut ordinal: u32 = 0;
        publishes.drain(|publish| {
            let this = ordinal;
            let Some(next) = ordinal.checked_add(1) else {
                return Err(crate::Error::Internal {
                    detail: format!(
                        "a committed redo record carries more than {} messages",
                        u32::MAX
                    ),
                });
            };
            ordinal = next;
            let value = match crate::event::topic::encode_publish(this, &publish) {
                Ok(value) => value,
                Err(error) => {
                    tracing::error!(
                        topic = %publish.topic,
                        error = %error,
                        "a committed publish did not encode; no event emitted for it"
                    );
                    return Ok(());
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
            Ok(())
        })
    }

    /// Send the events the install's handlers raised and held back.
    ///
    /// Every one names the record's LSN `lsn`: the install held the
    /// watermark back. A handler stamped each with the record's source: it
    /// takes the committed source of its kind of row, or its row-source
    /// group's. A handler that ran under a replay task stamped no commit HLC
    /// or author: each takes the record's.
    pub(super) fn send_held_events(
        &mut self,
        task: &ExecutionTask,
        events: EventSpool,
        rows: &EventRows<'_>,
        lsn: crate::types::Lsn,
    ) -> crate::Result<()> {
        let record_source = task.request.event_source;
        events.drain(|mut event| {
            event.lsn = lsn;
            if event.commit_hlc.is_none() {
                event.commit_hlc = task.request.commit_hlc;
            }
            if event.user_id.is_none() {
                event.user_id = task.request.user_id.clone();
            }
            if event.statement_digest.is_none() {
                event.statement_digest = task.request.statement_digest.clone();
            }
            event.source = rows.source(
                record_source,
                record_source.committed_other_source(),
                &event.collection,
                event.row_id.as_str(),
            )?;
            self.send_write_event(event);
            Ok(())
        })
    }
}

/// The rows a committed record wrote, as its spools hold them.
pub(super) struct CommittedWrites {
    pub doc_writes: Spool<SpooledDocWrite>,
    pub kv_priors: Spool<KvPriorImage>,
    pub labels: Spool<RedoLabelOp>,
}

/// The source a committed row carries: `listed`, its row-source group's,
/// when the record lists it, else `default` for its kind of row.
pub(in crate::data::executor) fn committed_source(
    listed: Option<EventSource>,
    record_source: EventSource,
    default: EventSource,
) -> EventSource {
    match listed {
        Some(listed) => record_source.committed_row_override(listed),
        None => default,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_kv_prior_image_round_trips() {
        for image in [
            KvPriorImage {
                collection: "cache".into(),
                key: b"k".to_vec(),
                new_value: Some(b"v1".to_vec()),
                prior: Some(b"v0".to_vec()),
            },
            KvPriorImage {
                collection: "cache".into(),
                key: b"gone".to_vec(),
                new_value: None,
                prior: None,
            },
        ] {
            assert!(image.spill_weight() > 0);
            let bytes = zerompk::to_msgpack_vec(&image).expect("encode");
            let back: KvPriorImage = zerompk::from_msgpack(&bytes).expect("decode");
            assert_eq!(back, image);
        }
    }
}
