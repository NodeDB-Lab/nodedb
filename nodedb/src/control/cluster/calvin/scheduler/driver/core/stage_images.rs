// SPDX-License-Identifier: BUSL-1.1

//! Take the `RETURNING` images a stage response carries onto the staged txn.
//!
//! A successful static or active stage answers with the images of every
//! local `RETURNING` plan. The leader keys them by plan index on the txn's
//! [`PendingTxn`](super::super::types::PendingTxn), where the vote reads them.

use super::halt::{HaltReason, HaltStep};
use super::scheduler::Scheduler;
use crate::bridge::envelope::{Response, Status};
use crate::control::cluster::calvin::scheduler::lock_manager::TxnId;
use crate::data::executor::response_codec::StagedReturningImages;

impl Scheduler {
    /// Take the images `response`, the answer to `txn_id`'s stage, carries.
    /// A refused stage carries none.
    ///
    /// Returns `false` once it halts on images it cannot read. The txn then
    /// casts no vote.
    pub(in crate::control::cluster::calvin::scheduler::driver::core) fn absorb_stage_images(
        &mut self,
        txn_id: TxnId,
        response: &Response,
    ) -> bool {
        if response.status != Status::Ok || response.payload.is_empty() {
            return true;
        }
        let absorbed = zerompk::from_msgpack::<StagedReturningImages>(response.payload.as_bytes())
            .map_err(|e| format!("the images do not decode: {e}"))
            .and_then(|images| match self.pending.get_mut(&txn_id) {
                Some(pending) => pending.returning.absorb(images),
                None => Ok(()),
            });
        match absorbed {
            Ok(()) => true,
            Err(error) => {
                self.metrics.record_executor_error();
                self.halt_apply(
                    txn_id,
                    HaltReason::StageVoteInvalid,
                    HaltStep::Stage,
                    format!("stage RETURNING images: {error}"),
                );
                false
            }
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

        assert!(scheduler.absorb_stage_images(txn_id, &response));

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

        assert!(scheduler.absorb_stage_images(txn_id, &response));
        let pending = scheduler.pending.get(&txn_id).expect("pending");
        assert!(pending.returning.images.is_empty());
    }

    /// Images for a plan the slice does not hold halt the stage. The txn
    /// casts no vote.
    #[tokio::test]
    async fn images_the_slice_cannot_key_halt_the_stage() {
        let txn_id = TxnId::new(42, 0);
        let (mut scheduler, _dir) = staged_scheduler(txn_id, vec![0]);
        let mut response = staged_response(Status::Ok, Some(StageVote::Commit));
        response.payload = images(&[1]);

        assert!(!scheduler.absorb_stage_images(txn_id, &response));
        assert_eq!(
            scheduler.apply_halt().map(|halt| halt.reason),
            Some(HaltReason::StageVoteInvalid)
        );
    }
}
