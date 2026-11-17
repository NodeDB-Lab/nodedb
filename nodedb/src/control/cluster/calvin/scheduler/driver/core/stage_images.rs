// SPDX-License-Identifier: BUSL-1.1

//! Take the `RETURNING` images a stage response carries onto the staged txn.
//!
//! A successful static or active stage answers with the images of every
//! local `RETURNING` plan. The leader keys them by plan index on the txn's
//! [`PendingTxn`](super::super::types::PendingTxn), where the vote reads them.

use super::scheduler::Scheduler;
use crate::bridge::envelope::{Response, Status};
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;
use crate::data::executor::response_codec::StagedReturningImages;

impl Scheduler {
    /// Take the images `response`, the answer to `txn_id`'s stage, carries.
    /// A refused stage carries none.
    ///
    /// Images the scheduler cannot read leave the slice failed. The txn then
    /// votes abort with an internal refusal.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn absorb_stage_images(
        &mut self,
        txn_id: TxnId,
        response: &Response,
    ) {
        if response.status != Status::Ok || response.payload.is_empty() {
            return;
        }
        let Some(pending) = self.pending.get_mut(&txn_id) else {
            return;
        };
        let absorbed = zerompk::from_msgpack::<StagedReturningImages>(response.payload.as_bytes())
            .map_err(|e| format!("the images do not decode: {e}"))
            .and_then(|images| pending.returning.absorb(images));
        if let Err(error) = absorbed {
            self.metrics.record_executor_error();
            tracing::warn!(
                vshard_id = self.vshard_id,
                epoch = txn_id.epoch,
                position = txn_id.position,
                %error,
                "calvin: stage RETURNING images are unreadable; the vote is an abort"
            );
            pending.returning.failed = Some(format!("stage RETURNING images: {error}"));
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::Value;

    use super::*;
    use crate::bridge::envelope::{Payload, StageVote};
    use crate::control::cluster::calvin::scheduler::driver::core::test_support::{
        build_test_scheduler, make_sequenced_txn, staged_pending, staged_response,
    };
    use crate::control::cluster::calvin::scheduler::driver::staged_returning::StagedReturning;
    use crate::data::executor::response_codec::{StagedImageRow, StagedPlanImages};

    fn images(plans: &[u32]) -> Payload {
        let staged = StagedReturningImages {
            plans: plans
                .iter()
                .map(|plan| StagedPlanImages {
                    plan: *plan,
                    collection: "cache".into(),
                    rows: vec![StagedImageRow {
                        key: b"k".to_vec(),
                        ordinal: 0,
                        fields: Value::Object(std::collections::HashMap::from([(
                            "v".to_string(),
                            Value::Integer(i64::from(*plan)),
                        )])),
                    }],
                    rejected: 0,
                })
                .collect(),
        };
        Payload::from_vec(zerompk::to_msgpack_vec(&staged).expect("encode images"))
    }

    fn staged_scheduler(txn_id: TxnId, plan_indexes: Vec<u32>) -> (Scheduler, tempfile::TempDir) {
        let (mut scheduler, dir) = build_test_scheduler(7);
        let mut pending = staged_pending(make_sequenced_txn(txn_id.epoch, txn_id.position), txn_id);
        pending.returning = StagedReturning::of_slice(plan_indexes);
        scheduler.pending.insert(txn_id, pending);
        (scheduler, dir)
    }

    /// Two local `RETURNING` plans, at class indexes 1 and 3, reach the
    /// pending txn as two keyed sets.
    #[tokio::test]
    async fn a_stage_answer_keys_its_images_on_the_pending_txn() {
        let txn_id = TxnId::new(40, 0);
        let (mut scheduler, _dir) = staged_scheduler(txn_id, vec![1, 3]);
        let mut response = staged_response(Status::Ok, Some(StageVote::Commit));
        response.payload = images(&[0, 1]);

        scheduler.absorb_stage_images(txn_id, &response);

        let pending = scheduler.pending.get(&txn_id).expect("pending");
        assert_eq!(
            pending.returning.images.keys().copied().collect::<Vec<_>>(),
            vec![1, 3]
        );
        assert_eq!(
            pending.returning.images[&3][0].identity.operation_index,
            3 << 32
        );
        assert!(scheduler.apply_halt().is_none());
    }

    #[tokio::test]
    async fn an_empty_payload_holds_no_images() {
        let txn_id = TxnId::new(41, 0);
        let (mut scheduler, _dir) = staged_scheduler(txn_id, Vec::new());
        let response = staged_response(Status::Ok, Some(StageVote::Commit));

        scheduler.absorb_stage_images(txn_id, &response);
        let pending = scheduler.pending.get(&txn_id).expect("pending");
        assert!(pending.returning.images.is_empty());
        assert!(pending.returning.failed.is_none());
    }

    /// Images for a plan the slice does not hold, or images that do not
    /// decode, fail the slice. The scheduler does not halt.
    #[tokio::test]
    async fn unreadable_images_fail_the_slice_without_a_halt() {
        let txn_id = TxnId::new(42, 0);
        let (mut scheduler, _dir) = staged_scheduler(txn_id, vec![0]);
        let mut response = staged_response(Status::Ok, Some(StageVote::Commit));
        response.payload = images(&[1]);

        scheduler.absorb_stage_images(txn_id, &response);
        assert!(scheduler.apply_halt().is_none());
        let pending = scheduler.pending.get(&txn_id).expect("pending");
        assert!(pending.returning.failed.is_some());

        let txn_id = TxnId::new(43, 0);
        let (mut scheduler, _dir) = staged_scheduler(txn_id, vec![0]);
        response.payload = Payload::from_vec(vec![0xc1]);
        scheduler.absorb_stage_images(txn_id, &response);
        assert!(scheduler.apply_halt().is_none());
        let pending = scheduler.pending.get(&txn_id).expect("pending");
        assert!(pending.returning.failed.is_some());
    }
}
