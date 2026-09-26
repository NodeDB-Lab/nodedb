// SPDX-License-Identifier: BUSL-1.1

//! KV writes from a Lite KV push, behind the sync idempotency gate.
//!
//! A pushed put or delete runs the same handler a SQL write runs. The gate
//! wraps it:
//! - a frame the gate holds back (duplicate, fenced epoch, sequence gap)
//!   applies nothing and answers `SyncNotApplied`, so the funnel cancels the
//!   frame's records and restart replay never applies them;
//! - an applied frame advances the stream's mark and answers the gate's ack
//!   payload;
//! - a frame the row-level-security write policy refuses advances the mark
//!   and answers `SyncRejected`, so the producer's next frame is not a gap.

use nodedb_types::sync::violation::ViolationType;
use nodedb_types::sync::wire::{AckStatus, SyncProvenance};

use super::types::{KvDeleteParams, KvWriteParams};
use crate::bridge::envelope::{ErrorCode, Response, Status, SyncHold};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::sync_gate::SyncAdmit;
use crate::data::executor::task::ExecutionTask;

impl CoreLoop {
    /// Run a pushed KV put behind the sync gate.
    pub(in crate::data::executor) fn execute_kv_sync_put(
        &mut self,
        task: &ExecutionTask,
        params: KvWriteParams<'_>,
        prov: &SyncProvenance,
    ) -> Response {
        if let Some(held) = self.kv_sync_hold(task, prov) {
            return held;
        }
        let response = self.execute_kv_put(task, params);
        self.kv_sync_outcome(task, response, prov)
    }

    /// Run a pushed KV delete behind the sync gate.
    pub(in crate::data::executor) fn execute_kv_sync_delete(
        &mut self,
        task: &ExecutionTask,
        params: KvDeleteParams<'_>,
        prov: &SyncProvenance,
    ) -> Response {
        if let Some(held) = self.kv_sync_hold(task, prov) {
            return held;
        }
        let response = self.execute_kv_delete(task, params);
        self.kv_sync_outcome(task, response, prov)
    }

    /// The `SyncNotApplied` response for a frame the gate holds back, or
    /// `None` when the frame is admitted.
    fn kv_sync_hold(&mut self, task: &ExecutionTask, prov: &SyncProvenance) -> Option<Response> {
        let hold = match self.sync_admit(prov) {
            SyncAdmit::Apply => return None,
            SyncAdmit::Duplicate => SyncHold::Duplicate,
            SyncAdmit::Fenced => SyncHold::Fenced,
            SyncAdmit::Gap { expected } => SyncHold::Gap { expected },
        };
        let applied_seq = self.sync_hwm_value(prov.producer_id, prov.stream_id);
        Some(self.response_error(task, ErrorCode::SyncNotApplied { hold, applied_seq }))
    }

    /// Turn the handler's response into the sync outcome.
    fn kv_sync_outcome(
        &mut self,
        task: &ExecutionTask,
        response: Response,
        prov: &SyncProvenance,
    ) -> Response {
        match response.status {
            Status::Ok | Status::Partial => {
                self.sync_commit(prov);
                self.sync_ack_response(task, AckStatus::Applied, prov.seq)
            }
            Status::Error => {
                let refusing_policy = match response.error_code.as_deref() {
                    Some(ErrorCode::RejectedAuthz { resource }) => Some(resource.clone()),
                    Some(_) | None => None,
                };
                match refusing_policy {
                    Some(policy_name) => self.sync_reject_response(
                        task,
                        ViolationType::RlsPolicyViolation { policy_name },
                        prov,
                    ),
                    None => response,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_types::sync::wire::{SyncAckResult, SyncOutcome};
    use nodedb_types::{DatabaseId, RlsWriteCheck, Surrogate};

    use super::*;
    use crate::data::executor::core_loop::tests::{make_core_with_dir, make_default_task};

    const TID: u64 = 1;

    fn prov(seq: u64) -> SyncProvenance {
        SyncProvenance {
            producer_id: 7,
            epoch: 1,
            stream_id: 42,
            seq,
        }
    }

    fn put<'a>(value: &'a [u8]) -> KvWriteParams<'a> {
        KvWriteParams {
            did: DatabaseId::DEFAULT.as_u64(),
            tid: TID,
            collection: "cfg",
            key: b"k1",
            value,
            ttl_ms: 0,
            surrogate: Surrogate::ZERO,
            returning: None,
            rls_filters: &[],
        }
    }

    fn applied_seq(response: &Response) -> u64 {
        assert_eq!(response.status, Status::Ok);
        let ack: SyncAckResult = zerompk::from_msgpack(&response.payload).expect("sync ack");
        assert_eq!(ack.outcome, SyncOutcome::Ack(AckStatus::Applied));
        ack.applied_seq
    }

    fn hold(response: &Response) -> SyncHold {
        match response.error_code.as_deref() {
            Some(ErrorCode::SyncNotApplied { hold, .. }) => *hold,
            other => panic!("expected SyncNotApplied, got {other:?}"),
        }
    }

    fn stored(core: &CoreLoop) -> Option<Vec<u8>> {
        core.kv_engine.get(
            DatabaseId::DEFAULT.as_u64(),
            TID,
            "cfg",
            b"k1",
            crate::engine::kv::current_ms(),
        )
    }

    #[test]
    fn a_frame_applies_once_and_its_resend_is_a_duplicate() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let task = make_default_task();

        let first = core.execute_kv_sync_put(&task, put(b"v1"), &prov(1));
        assert_eq!(applied_seq(&first), 1);

        let resent = core.execute_kv_sync_put(&task, put(b"v1"), &prov(1));
        assert_eq!(hold(&resent), SyncHold::Duplicate);
        assert_eq!(stored(&core).as_deref(), Some(b"v1".as_slice()));
    }

    #[test]
    fn a_frame_past_a_gap_applies_nothing() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let task = make_default_task();

        let skipped = core.execute_kv_sync_put(&task, put(b"v1"), &prov(3));
        assert_eq!(hold(&skipped), SyncHold::Gap { expected: 1 });
        assert_eq!(stored(&core), None);
    }

    #[test]
    fn a_pushed_delete_removes_the_key_behind_the_gate() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _req, _resp) = make_core_with_dir(dir.path());
        let task = make_default_task();
        assert_eq!(
            applied_seq(&core.execute_kv_sync_put(&task, put(b"v1"), &prov(1))),
            1
        );

        let keys = vec![b"k1".to_vec()];
        let deleted = core.execute_kv_sync_delete(
            &task,
            KvDeleteParams {
                did: DatabaseId::DEFAULT.as_u64(),
                tid: TID,
                collection: "cfg",
                keys: &keys,
                rls_write_check: &RlsWriteCheck::NoPolicyApplies,
                returning: None,
                rls_filters: &[],
            },
            &prov(2),
        );
        assert_eq!(applied_seq(&deleted), 2);
        assert_eq!(stored(&core), None);
    }
}
