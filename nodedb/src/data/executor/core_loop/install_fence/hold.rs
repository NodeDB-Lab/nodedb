// SPDX-License-Identifier: BUSL-1.1

//! The core's install fence: arm, hold, sweep, release.
//!
//! With no fence armed, each of these pays one `None` check and nothing
//! else:
//! - `hold_fenced`, once per task in `poll_one`
//! - `sweep_fence_waiters`, once per tick, after its fail-stop check
//! - `poll_write_batch`, once per batch attempt
//! - `pending_count`
//! - the waiter fallback of `execute_cancel`, when the target is not queued

use std::collections::{BTreeSet, VecDeque};

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::types::RequestId;

use super::{FenceKey, InstallFence, footprint};

impl CoreLoop {
    /// Hold back every task that touches `keys` until
    /// [`Self::release_install_fence`]. A fence already armed gains `keys`
    /// and keeps its waiters.
    pub fn arm_install_fence(&mut self, keys: BTreeSet<FenceKey>) {
        match self.install_fence.as_mut() {
            Some(fence) => fence.keys.extend(keys),
            None => self.install_fence = Some(InstallFence::new(keys)),
        }
    }

    /// Disarm the fence. Its waiters return to the front of the queue in
    /// arrival order.
    pub fn release_install_fence(&mut self) {
        let Some(mut fence) = self.install_fence.take() else {
            return;
        };
        for task in fence.take_waiters().into_iter().rev() {
            self.task_queue.push_front(task);
        }
    }

    /// Whether an install fence is armed.
    pub fn has_install_fence(&self) -> bool {
        self.install_fence.is_some()
    }

    /// Hold `task` when the armed fence stops it. Returns the task when it
    /// proceeds.
    pub(in crate::data::executor) fn hold_fenced(
        &mut self,
        task: ExecutionTask,
    ) -> Option<ExecutionTask> {
        let Some(fence) = self.install_fence.as_mut() else {
            return Some(task);
        };
        let footprint = footprint(&task);
        if fence.admits(&footprint) {
            return Some(task);
        }
        fence.hold(task, footprint);
        None
    }

    /// Answer every waiter past its deadline through the expiry path, and
    /// refuse every waiter of a fail-stopped core. Returns how many tasks
    /// answered.
    pub(in crate::data::executor) fn sweep_fence_waiters(&mut self) -> usize {
        if self.fail_stop.is_stopped() {
            if self.fence_waiter_count() == 0 {
                return 0;
            }
            return self.refuse_queued_while_stopped();
        }
        let Some(fence) = self.install_fence.as_mut() else {
            return 0;
        };
        if !fence.waiting.iter().any(ExecutionTask::is_expired) {
            return 0;
        }
        let (expired, live): (VecDeque<ExecutionTask>, VecDeque<ExecutionTask>) = fence
            .take_waiters()
            .into_iter()
            .partition(ExecutionTask::is_expired);
        fence.waiting = live;
        fence.recompute_blocked();
        let answered = expired.len();
        for task in expired {
            self.run_task(task);
        }
        answered
    }

    /// Remove the waiter of `request_id`. Returns whether one waited.
    pub(in crate::data::executor) fn remove_fence_waiter(&mut self, request_id: RequestId) -> bool {
        self.install_fence
            .as_mut()
            .is_some_and(|fence| fence.remove_waiter(request_id))
    }

    /// Take every waiter, in arrival order. The fence stays armed.
    pub(crate) fn take_fence_waiters(&mut self) -> VecDeque<ExecutionTask> {
        self.install_fence
            .as_mut()
            .map(InstallFence::take_waiters)
            .unwrap_or_default()
    }

    /// How many tasks wait behind the fence.
    pub(in crate::data::executor) fn fence_waiter_count(&self) -> usize {
        self.install_fence
            .as_ref()
            .map_or(0, |fence| fence.waiting.len())
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use nodedb_physical::physical_plan::{DocumentOp, MetaOp};
    use nodedb_types::QualifiedCollection;

    use super::*;
    use crate::bridge::dispatch::{BridgeResponse, JournalGroup};
    use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Priority, Request, Status};
    use crate::data::executor::core_loop::fail_stop::FailStopCause;
    use crate::data::executor::core_loop::tests::{make_core, make_request};
    use crate::types::{DatabaseId, Lsn};

    fn key(name: &str) -> FenceKey {
        FenceKey::Collection {
            db: DatabaseId::DEFAULT,
            name: name.into(),
        }
    }

    fn keys(names: &[&str]) -> BTreeSet<FenceKey> {
        names.iter().map(|name| key(name)).collect()
    }

    fn request(id: u64, plan: PhysicalPlan) -> Request {
        Request {
            request_id: RequestId::new(id),
            deadline: Instant::now() + Duration::from_secs(60),
            ..make_request(plan)
        }
    }

    fn get(collection: &str) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::PointGet {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, collection),
            document_id: "d".into(),
            surrogate: None,
            pk_bytes: Vec::new(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
        })
    }

    fn put(collection: &str) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::PointPut {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, collection),
            document_id: "d".into(),
            value: crate::data::executor::core_loop::tests::doc_value("a", "1"),
            surrogate: nodedb_types::Surrogate::new(1),
            pk_bytes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        })
    }

    fn queue(core: &mut CoreLoop, id: u64, plan: PhysicalPlan) {
        core.task_queue.push(ExecutionTask::new(request(id, plan)));
    }

    fn answered(
        resp_rx: &mut nodedb_bridge::buffer::Consumer<BridgeResponse>,
    ) -> Vec<(u64, Status)> {
        let mut out = Vec::new();
        while let Ok(resp) = resp_rx.try_pop() {
            out.push((resp.inner.request_id.as_u64(), resp.inner.status));
        }
        out
    }

    fn ids(answers: &[(u64, Status)]) -> Vec<u64> {
        answers.iter().map(|(id, _)| *id).collect()
    }

    #[test]
    fn a_read_of_an_untouched_collection_runs() {
        let (mut core, _tx, mut rx, _dir) = make_core();
        core.arm_install_fence(keys(&["a"]));
        queue(&mut core, 1, get("b"));
        core.tick();
        assert_eq!(ids(&answered(&mut rx)), vec![1]);
        assert_eq!(core.fence_waiter_count(), 0);
    }

    #[test]
    fn a_read_of_a_fenced_collection_waits_and_runs_after_release() {
        let (mut core, _tx, mut rx, _dir) = make_core();
        core.arm_install_fence(keys(&["a"]));
        queue(&mut core, 2, get("a"));
        core.tick();
        assert!(answered(&mut rx).is_empty());
        assert_eq!(core.fence_waiter_count(), 1);
        assert_eq!(core.pending_count(), 1);

        core.release_install_fence();
        assert!(!core.has_install_fence());
        core.tick();
        assert_eq!(ids(&answered(&mut rx)), vec![2]);
    }

    #[test]
    fn a_later_write_waits_behind_a_waiting_task_and_release_keeps_arrival_order() {
        let (mut core, _tx, mut rx, _dir) = make_core();
        core.arm_install_fence(keys(&["a"]));
        queue(&mut core, 1, get("a"));
        queue(&mut core, 2, put("b"));
        queue(&mut core, 3, get("b"));
        core.tick();
        // The write of the fenced database waits, and the read of `b` waits
        // behind it.
        assert!(answered(&mut rx).is_empty());
        assert_eq!(core.fence_waiter_count(), 3);

        core.release_install_fence();
        core.tick();
        assert_eq!(ids(&answered(&mut rx)), vec![1, 2, 3]);
    }

    #[test]
    fn a_task_of_another_database_runs_past_the_fence() {
        let (mut core, _tx, mut rx, _dir) = make_core();
        core.arm_install_fence(keys(&["a"]));
        let other = DatabaseId::new(7);
        let plan = PhysicalPlan::Document(DocumentOp::PointGet {
            collection: QualifiedCollection::new(other, "a"),
            document_id: "d".into(),
            surrogate: None,
            pk_bytes: Vec::new(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
        });
        core.task_queue.push(ExecutionTask::new(Request {
            database_id: other,
            ..request(4, plan)
        }));
        core.tick();
        assert_eq!(ids(&answered(&mut rx)), vec![4]);
    }

    #[test]
    fn a_waiting_checkpoint_holds_every_later_task() {
        let (mut core, _tx, mut rx, _dir) = make_core();
        core.arm_install_fence(BTreeSet::new());
        queue(&mut core, 1, PhysicalPlan::Meta(MetaOp::Checkpoint));
        queue(&mut core, 2, get("b"));
        core.tick();
        assert!(answered(&mut rx).is_empty());
        assert_eq!(core.fence_waiter_count(), 2);

        core.release_install_fence();
        core.tick();
        assert_eq!(ids(&answered(&mut rx)), vec![1, 2]);
    }

    #[test]
    fn cancel_reaches_a_waiter() {
        let (mut core, _tx, mut rx, _dir) = make_core();
        core.arm_install_fence(keys(&["a"]));
        let probe = ExecutionTask::new(request(10, get("a")));
        core.restore_journal_group(
            &probe,
            JournalGroup {
                origin: Lsn::new(1),
                collection: "a".into(),
                apply_key: 1,
                commit_hlc: None,
                change_position: None,
            },
        );
        queue(&mut core, 10, get("a"));
        core.tick();
        assert_eq!(core.fence_waiter_count(), 1);

        core.task_queue.push(ExecutionTask::new(Request {
            priority: Priority::Critical,
            ..request(
                99,
                PhysicalPlan::Meta(MetaOp::Cancel {
                    target_request_id: RequestId::new(10),
                }),
            )
        }));
        core.tick();
        assert_eq!(ids(&answered(&mut rx)), vec![99]);
        assert_eq!(core.fence_waiter_count(), 0);
        // The cancelled waiter never runs, so its group is gone.
        assert!(!core.journals_write_set(&probe));

        core.release_install_fence();
        core.tick();
        assert!(answered(&mut rx).is_empty());
    }

    #[test]
    fn an_expired_waiter_answers_that_it_never_started() {
        let (mut core, _tx, mut rx, _dir) = make_core();
        core.arm_install_fence(keys(&["a"]));
        let expired = ExecutionTask::new(Request {
            deadline: Instant::now() - Duration::from_secs(1),
            ..request(5, get("a"))
        });
        assert!(core.hold_fenced(expired).is_none());
        queue(&mut core, 6, get("a"));
        core.tick();

        let resp = rx.try_pop().expect("expiry answer");
        assert_eq!(resp.inner.request_id, RequestId::new(5));
        assert_eq!(resp.inner.status, Status::Error);
        assert_eq!(
            resp.inner.error_code.as_deref(),
            Some(&ErrorCode::ExpiredBeforeExecution)
        );
        assert!(answered(&mut rx).is_empty());
        // The live waiter still waits.
        assert_eq!(core.fence_waiter_count(), 1);
    }

    #[test]
    fn a_fail_stop_refuses_every_waiter() {
        let (mut core, _tx, mut rx, _dir) = make_core();
        core.arm_install_fence(keys(&["a"]));
        queue(&mut core, 7, get("a"));
        core.tick();
        assert_eq!(core.fence_waiter_count(), 1);

        core.fail_stop_core(
            FailStopCause::RollbackFailed,
            "undo entry 0: restore failed",
        );
        assert_eq!(core.tick(), 1);
        let resp = rx.try_pop().expect("refusal");
        assert_eq!(resp.inner.request_id, RequestId::new(7));
        assert!(matches!(
            resp.inner.error_code.as_deref(),
            Some(ErrorCode::CoreFailStopped { .. })
        ));
        assert_eq!(core.fence_waiter_count(), 0);
    }
}
