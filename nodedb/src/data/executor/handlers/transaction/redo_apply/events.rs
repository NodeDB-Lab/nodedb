// SPDX-License-Identifier: BUSL-1.1

//! Event Plane output of a committed redo apply.
//!
//! The transaction batch emitted three kinds of events, and the apply emits
//! the same set:
//!
//! * one `Deferred` event per document row written (source rows and
//!   materialized-sum targets), carrying the pre-image, for DEFERRED triggers;
//! * one event per KV key written, from the KV write handlers;
//! * one event per graph node-label delta, on the label stream.
//!
//! Graph edges and CRDT rows emit from the handlers their replay arms call,
//! and the index engines (vector, FTS, spatial) and the columnar family emit
//! no Data-Plane events on either path.

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::core_loop::deferred::DeferredWrite;
use crate::data::executor::task::ExecutionTask;
use crate::engine::document::store::RowIdentity;
use crate::event::WriteOp;

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
        let now_ms = crate::engine::kv::current_ms();
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

    /// Emit every event the committed record's writes produce.
    pub(super) fn emit_committed_redo_events(
        &mut self,
        task: &ExecutionTask,
        doc_writes: Vec<AppliedDocWrite>,
        kv_images: Vec<KvPriorImage>,
        labels: Vec<RedoLabelOp>,
    ) {
        // Every event names the record's LSN; the arms advanced the watermark
        // to it for the rows they versioned, and this covers a record whose
        // writes versioned none.
        if let Some(lsn) = task.wal_lsn()
            && lsn > self.watermark
        {
            self.watermark = lsn;
        }

        for image in kv_images {
            let identity = RowIdentity::from_user_key(String::from_utf8_lossy(&image.key).as_ref());
            match image.new_value {
                Some(new_value) => {
                    let op = if image.prior.is_some() {
                        WriteOp::Update
                    } else {
                        WriteOp::Insert
                    };
                    self.emit_write_event(
                        task,
                        &image.collection,
                        op,
                        identity,
                        Some(new_value.as_slice()),
                        image.prior.as_deref(),
                    );
                }
                // A delete of an absent key removed nothing.
                None if image.prior.is_some() => {
                    self.emit_write_event(
                        task,
                        &image.collection,
                        WriteOp::Delete,
                        identity,
                        None,
                        None,
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
            self.emit_graph_label_event(task, &label.node_id, &label.labels, op);
        }

        let deferred: Vec<DeferredWrite> = doc_writes
            .into_iter()
            .map(|write| DeferredWrite {
                collection: write.collection,
                op: write.op,
                identity: write.identity,
                new_value: None,
                old_value: write.old_value,
            })
            .collect();
        if !deferred.is_empty() {
            self.emit_deferred_events(
                deferred,
                task.request.database_id,
                task.request.tenant_id,
                task.request.vshard_id,
            );
        }
    }
}
