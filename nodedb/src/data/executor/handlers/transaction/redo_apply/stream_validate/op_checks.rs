// SPDX-License-Identifier: BUSL-1.1

//! The checks one sub-record of a stream passes as the walk reaches it.
//!
//! In whole-record order these are: the stamp raise to the stream's floor,
//! the clone-copy probe, the document row image, the collection checks, the
//! KV row image and the snapshot writes. Each offers its error under its
//! rank, and work whose rank cannot replace the kept error is skipped. The
//! arms see the raised op with the rest of its batch.

use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;
use crate::wal::RedoSubRecord;

use super::super::clone_copy::clone_probe_unreadable;
use super::super::passes::final_refusal;
use super::super::sub_ops::{RedoDocOp, document_op, kv_op};
use super::super::validate::check_kv_row_image;
use super::collection::post_state_failed;
use super::verdict::Rank;
use super::walk::{StreamValidateInput, StreamWalk};

impl CoreLoop {
    /// Check the content's next sub-record, `sub`.
    pub(super) fn check_stream_op(
        &self,
        walk: &mut StreamWalk,
        input: &StreamValidateInput<'_>,
        mut sub: RedoSubRecord,
    ) {
        let op = walk.ops;
        walk.ops += 1;
        // Every version of the content stamps at least the floor, as the
        // installed record does.
        if let Some(floor) = input.reference.stamp_floor
            && walk.verdict.admits(&Rank::Raise(op))
            && let Err(error) = sub.raise_stamp_to(floor)
        {
            walk.verdict
                .offer(Rank::Raise(op), final_refusal(error.into()));
        }
        // Every later check of this op ranks after its clone probe.
        if !walk.verdict.admits(&Rank::CloneProbe(op)) {
            return;
        }
        if let Some(doc) = document_op(&sub) {
            self.check_stream_document(walk, input, op, &doc);
        }
        if let Some(kv) = kv_op(&sub)
            && walk.verdict.admits(&Rank::KvRowImage(op))
            && let Err(error) = check_kv_row_image(&kv)
        {
            walk.verdict
                .offer(Rank::KvRowImage(op), ErrorCode::from(error));
        }
        self.check_stream_writes(walk, input, op, &sub);
        if walk.verdict.admits(&Rank::Record(walk.batch)) {
            walk.batch_ops.push(sub);
        }
        if walk.verdict.admits(&Rank::Local)
            && let Err(error) = walk.post.spill_past_threshold()
        {
            walk.verdict.offer(Rank::Local, post_state_failed(error));
        }
    }

    /// The clone probe, the row image and the collection checks of the
    /// document op `doc`. A clone copy whose row holds a version writes
    /// nothing, so no check judges it.
    fn check_stream_document(
        &self,
        walk: &mut StreamWalk,
        input: &StreamValidateInput<'_>,
        op: u64,
        doc: &RedoDocOp,
    ) {
        let database_id = input.task.request.database_id.as_u64();
        if let RedoDocOp::Put {
            collection,
            surrogate,
            clone_copy: true,
            ..
        } = doc
        {
            match self.doc_row_holds_a_version(database_id, input.tid, collection, *surrogate) {
                Ok(true) => return,
                Ok(false) => {}
                Err(error) => {
                    walk.verdict
                        .offer(Rank::CloneProbe(op), clone_probe_unreadable(error));
                    return;
                }
            }
        }
        if walk.verdict.admits(&Rank::DocRowImage(op))
            && let Err(error) = self.check_doc_row_image(database_id, input.tid, doc)
        {
            walk.verdict
                .offer(Rank::DocRowImage(op), ErrorCode::from(error));
        }
        self.check_stream_collection_op(walk, input, op, doc);
    }
}
