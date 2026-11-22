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

use super::state::stream_of;
use super::{FenceKey, InstallFence, footprint};

impl CoreLoop {
    /// Hold back every task that touches `keys` until
    /// [`Self::release_install_fence`]. A fence already armed gains `keys`
    /// and keeps its waiters.
    pub(in crate::data::executor) fn arm_install_fence(&mut self, keys: BTreeSet<FenceKey>) {
        match self.install_fence.as_mut() {
            Some(fence) => fence.keys.extend(keys),
            None => self.install_fence = Some(InstallFence::new(keys)),
        }
    }

    /// Disarm the fence. Maintenance that came due while it was armed runs
    /// first, so back-to-back installs never starve it. Its waiters then
    /// return to the front of the queue in arrival order.
    pub(in crate::data::executor) fn release_install_fence(&mut self) {
        let Some(mut fence) = self.install_fence.take() else {
            return;
        };
        if fence.maintenance_owed && !self.fail_stop.is_stopped() {
            self.maybe_run_maintenance();
        }
        for task in fence.take_waiters().into_iter().rev() {
            self.task_queue.push_front(task);
        }
    }

    /// Record that maintenance came due while the fence is armed. A no-op
    /// with no fence armed.
    pub(in crate::data::executor) fn owe_maintenance(&mut self) {
        if let Some(fence) = self.install_fence.as_mut() {
            fence.maintenance_owed = true;
        }
    }

    /// Note an install at `lsn` that holds events. Until the fence releases,
    /// a heartbeat publishes below the lowest such LSN, so no consumer sees
    /// a watermark past an event still to come. A no-op with no fence armed.
    pub(in crate::data::executor) fn hold_event_floor(&mut self, lsn: u64) {
        if let Some(fence) = self.install_fence.as_mut() {
            fence.event_floor = Some(fence.event_floor.map_or(lsn, |floor| floor.min(lsn)));
        }
    }

    /// The LSN a heartbeat publishes: the watermark, capped below the lowest
    /// LSN of an install that holds events.
    pub(in crate::data::executor) fn heartbeat_lsn(&self) -> crate::types::Lsn {
        let floor = self
            .install_fence
            .as_ref()
            .and_then(|fence| fence.event_floor);
        match floor {
            Some(lsn) => crate::types::Lsn::new(self.watermark.as_u64().min(lsn.saturating_sub(1))),
            None => self.watermark,
        }
    }

    /// Whether an install fence is armed.
    pub(crate) fn has_install_fence(&self) -> bool {
        self.install_fence.is_some()
    }

    /// Hold `task` when the armed fence stops it. Returns the task when it
    /// proceeds. The footprint, the stream check and the registry lookups
    /// run only while a fence is armed. The waiter keeps the footprint it
    /// was held by, so a registry change while it waits never changes what
    /// it holds.
    pub(in crate::data::executor) fn hold_fenced(
        &mut self,
        task: ExecutionTask,
    ) -> Option<ExecutionTask> {
        if self.install_fence.is_none() {
            return Some(task);
        }
        let footprint = footprint(&task, &*self);
        let Some(fence) = self.install_fence.as_mut() else {
            return Some(task);
        };
        if fence.admits_task(&footprint, stream_of(&task.request)) {
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
        let expired = fence.take_expired();
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
    fn a_write_of_an_untouched_collection_runs_past_a_waiting_read() {
        let (mut core, _tx, mut rx, _dir) = make_core();
        core.arm_install_fence(keys(&["a"]));
        queue(&mut core, 1, get("a"));
        queue(&mut core, 2, put("b"));
        core.tick();
        assert_eq!(ids(&answered(&mut rx)), vec![2]);
        assert_eq!(core.fence_waiter_count(), 1);

        core.release_install_fence();
        core.tick();
        assert_eq!(ids(&answered(&mut rx)), vec![1]);
    }

    #[test]
    fn a_later_write_waits_behind_a_waiting_write_and_release_keeps_arrival_order() {
        let (mut core, _tx, mut rx, _dir) = make_core();
        core.arm_install_fence(keys(&["a"]));
        queue(&mut core, 1, put("a"));
        queue(&mut core, 2, put("b"));
        queue(&mut core, 3, get("b"));
        core.tick();
        // The write of `b` waits behind the waiting write of its vShard, and
        // the read of `b` waits behind the write of `b`.
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

    /// A client's core read keeps its order: a later task waits behind it.
    #[test]
    fn a_waiting_core_task_holds_every_later_task() {
        let (mut core, _tx, mut rx, _dir) = make_core();
        core.arm_install_fence(BTreeSet::new());
        let list = MetaOp::ListTenantCollections { tenant_id: 1 };
        queue(&mut core, 1, PhysicalPlan::Meta(list));
        queue(&mut core, 2, get("b"));
        queue(&mut core, 3, put("c"));
        core.tick();
        assert!(answered(&mut rx).is_empty());
        assert_eq!(core.fence_waiter_count(), 3);

        core.release_install_fence();
        core.tick();
        assert_eq!(ids(&answered(&mut rx)), vec![1, 2, 3]);
    }

    /// A task of client connection `connection`.
    fn on_connection(connection: u64, id: u64, plan: PhysicalPlan) -> ExecutionTask {
        let connection = crate::control::server::shared::session::ConnectionId::new(connection)
            .expect("non-zero id");
        ExecutionTask::new(Request {
            stream: crate::bridge::envelope::RequestStream::Connection(connection),
            ..request(id, plan)
        })
    }

    /// A client's list waits. A write of another connection runs past it,
    /// and a later write of the list's connection waits. Release keeps the
    /// list's connection in order.
    #[test]
    fn a_waiting_client_list_holds_its_connection_alone() {
        let (mut core, _tx, mut rx, _dir) = make_core();
        core.arm_install_fence(BTreeSet::new());
        let list = MetaOp::ListTenantCollections { tenant_id: 1 };
        core.task_queue
            .push(on_connection(1, 1, PhysicalPlan::Meta(list)));
        core.task_queue.push(on_connection(2, 2, put("c")));
        core.task_queue.push(on_connection(1, 3, put("b")));
        core.tick();
        assert_eq!(ids(&answered(&mut rx)), vec![2]);
        assert_eq!(core.fence_waiter_count(), 2);

        core.release_install_fence();
        core.tick();
        assert_eq!(ids(&answered(&mut rx)), vec![1, 3]);
    }

    /// A read and a write pipelined on one connection keep their order,
    /// though the write touches no fenced key.
    #[test]
    fn a_pipelined_read_and_write_keep_their_connection_order() {
        let (mut core, _tx, mut rx, _dir) = make_core();
        core.arm_install_fence(keys(&["a"]));
        core.task_queue.push(on_connection(1, 1, get("a")));
        core.task_queue.push(on_connection(1, 2, put("b")));
        core.tick();
        assert!(answered(&mut rx).is_empty());
        assert_eq!(core.fence_waiter_count(), 2);

        core.release_install_fence();
        core.tick();
        assert_eq!(ids(&answered(&mut rx)), vec![1, 2]);
    }

    /// A checkpoint keeps no client order: it waits for the release, and a
    /// later write runs ahead of it.
    #[test]
    fn a_waiting_checkpoint_holds_no_later_task() {
        let (mut core, _tx, mut rx, _dir) = make_core();
        core.arm_install_fence(BTreeSet::new());
        queue(&mut core, 1, PhysicalPlan::Meta(MetaOp::Checkpoint));
        queue(&mut core, 2, put("c"));
        core.tick();
        assert_eq!(ids(&answered(&mut rx)), vec![2]);
        assert_eq!(core.fence_waiter_count(), 1);

        core.release_install_fence();
        core.tick();
        assert_eq!(ids(&answered(&mut rx)), vec![1]);
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

    /// A heartbeat never publishes a watermark past an install whose events
    /// are held, and publishes the full watermark once the fence releases.
    #[test]
    fn a_heartbeat_stays_below_an_install_that_holds_events() {
        let (mut core, _tx, _rx, _dir) = make_core();
        let (mut producers, mut consumers) =
            crate::event::bus::create_event_bus_with_capacity(1, 64);
        core.set_event_producer(producers.pop().expect("producer"));
        let mut events = consumers.pop().expect("consumer");
        core.advance_watermark(Lsn::new(100));

        core.emit_heartbeat();
        assert_eq!(events.try_recv().expect("heartbeat").lsn, Lsn::new(100));

        core.arm_install_fence(keys(&["a"]));
        core.hold_event_floor(60);
        core.hold_event_floor(80);
        core.emit_heartbeat();
        assert_eq!(
            events.try_recv().expect("heartbeat").lsn,
            Lsn::new(59),
            "the lowest held LSN caps the heartbeat"
        );

        core.release_install_fence();
        core.emit_heartbeat();
        assert_eq!(events.try_recv().expect("heartbeat").lsn, Lsn::new(100));
    }
}
