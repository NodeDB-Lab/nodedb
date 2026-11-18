// SPDX-License-Identifier: BUSL-1.1

//! The arms' validate pass over a stream, one batch at a time.
//!
//! Each batch's ops form a record that holds only them (see
//! `batched::install::batch_record`). The pass runs over it through
//! `ValidateBatches`, which keeps one scope across the batches and ranks
//! their errors as a whole-record pass does.

use crate::bridge::envelope::ErrorCode;
use crate::data::executor::core_loop::CoreLoop;

use super::super::batched::{BatchStamps, batch_wal_record};
use super::super::passes::RedoTarget;
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
        let Ok(batch_index) = u32::try_from(batch) else {
            walk.verdict.offer(
                Rank::Record(batch),
                ErrorCode::RejectedPrevalidation {
                    reason: format!(
                        "the committed redo stream holds more than {} batches",
                        u32::MAX
                    ),
                },
            );
            return;
        };
        let sub_records = ops.len();
        let stamps = BatchStamps::of(input.marker);
        let record = match batch_wal_record(input.task, input.lsn, header, &stamps, ops) {
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
        batches.enter_batch(batch_index);
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
