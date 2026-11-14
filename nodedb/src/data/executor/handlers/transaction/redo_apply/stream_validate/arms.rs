// SPDX-License-Identifier: BUSL-1.1

//! The arms' validate pass over a stream, one batch at a time.
//!
//! Each batch's ops form a record that holds only them, with the content's
//! header fields and the marker's Calvin stamp, at the marker's LSN. The
//! pass runs over it through [`ValidateBatches`], which keeps one scope
//! across the batches and ranks their errors as a whole-record pass does.

use crate::data::executor::core_loop::CoreLoop;
use crate::wal::RedoRecord;

use super::super::passes::{RedoTarget, final_refusal, redo_wal_record};
use super::verdict::Rank;
use super::walk::{StreamValidateInput, StreamWalk};

impl CoreLoop {
    /// Run the arms over the ops of the batch the walk ends, and move the
    /// walk to the next batch.
    pub(super) fn validate_stream_arms(
        &mut self,
        walk: &mut StreamWalk,
        input: &StreamValidateInput<'_>,
    ) {
        let batch = walk.batch;
        walk.batch += 1;
        let ops = std::mem::take(&mut walk.batch_ops);
        if ops.is_empty() || !walk.verdict.admits(&Rank::Record(batch)) {
            return;
        }
        let Some(header) = &walk.header else {
            return;
        };
        let sub_records = ops.len();
        let redo = RedoRecord {
            version: header.version,
            ops,
            calvin_stamp: input.marker.calvin_stamp.clone(),
            cross_shard_applied: input.marker.cross_shard_applied.clone(),
            row_sources: Vec::new(),
            publishes: Vec::new(),
            row_changes: Vec::new(),
            user_roles: header.user_roles.clone(),
        };
        let payload = match redo.to_bytes() {
            Ok(payload) => payload,
            Err(error) => {
                walk.verdict
                    .offer(Rank::Record(batch), final_refusal(error.into()));
                return;
            }
        };
        let record = match redo_wal_record(input.task, input.lsn, payload) {
            Ok(record) => record,
            Err(code) => {
                walk.verdict.offer(Rank::Record(batch), code);
                return;
            }
        };
        if !walk.verdict.admits(&Rank::ArmsAbort(batch)) {
            return;
        }
        let Some(batches) = walk.batches.as_mut() else {
            return;
        };
        let target = RedoTarget {
            record: &record,
            sub_records,
            database_id: input.task.request.database_id.as_u64(),
            tid: input.tid,
            origin: input.origin,
        };
        if let Err(refusal) = self.validate_redo_batch(batches, &target) {
            walk.verdict
                .offer(Rank::ArmsAbort(batch), refusal.into_code());
            walk.batches = None;
        }
    }
}
