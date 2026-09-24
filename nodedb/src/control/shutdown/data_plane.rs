// SPDX-License-Identifier: BUSL-1.1

//! The participant that holds [`ShutdownPhase::DrainingDataPlane`].
//!
//! # What the phase promises
//!
//! Every Data Plane core has stopped accepting new work and has finished what
//! it already had. Until this supervisor reports drained, the bus does not
//! advance, so no later phase — Event Plane drain, watermark persist, final WAL
//! fsync — runs while a core is still executing.
//!
//! # How it keeps the promise
//!
//! [`Dispatcher::begin_data_plane_drain`] closes the enqueue gate under the
//! dispatcher mutex, which is the mutex every enqueue already takes. After that
//! call returns, nothing new can reach a core. The supervisor then polls
//! `Dispatcher::data_plane_pending` until every core owes nothing.
//!
//! The whole drain reads Control-Plane state. The Data Plane keeps the SPSC
//! bridge as its only channel, runs no tokio task, and shares no lock with this
//! path.
//!
//! # What "finished" covers
//!
//! - Requests staged in a core's weighted-fair queue.
//! - Requests sitting in a core's SPSC request ring.
//! - The task a core is executing right now, including its io_uring reads and
//!   writes: every ring the Data Plane owns is submitted and reaped inside the
//!   call that issued it, so a completed task has no outstanding completion.
//! - The response for each of those, routed back to its waiting session.
//!
//! # What it deliberately does not cover
//!
//! - WAL durability. The Control Plane appends a write's WAL record before it
//!   dispatches the request, so no WAL append rides on a Data Plane core.
//!   Making that log durable is [`ShutdownPhase::WalFsync`]'s job.
//! - Core-local maintenance — compaction, tombstone sweeps, periodic vector
//!   checkpoints — and off-core HNSW index builds. None is client-visible work
//!   and each is rebuilt from durable state on restart.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::Notify;
use tracing::{error, info};

use crate::bridge::dispatch::{CorePending, Dispatcher};
use crate::control::state::SharedState;

use super::bus::ShutdownBus;
use super::phase::ShutdownPhase;

/// How often the supervisor re-checks whether the cores still owe work.
const DRAIN_POLL_INTERVAL: Duration = Duration::from_millis(5);

/// Latch that says whether the Data Plane drain has finished.
///
/// The response poller reads it: it must keep routing Data Plane responses
/// while the drain is waiting for them, and stop once the drain is over.
#[derive(Debug, Default)]
pub struct DataPlaneDrain {
    complete: AtomicBool,
    notify: Notify,
}

impl DataPlaneDrain {
    /// A fresh latch, not yet complete.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Whether the drain has finished. True after a clean drain and after an
    /// expired one — in both cases the phase is over.
    pub fn is_complete(&self) -> bool {
        self.complete.load(Ordering::Acquire)
    }

    /// Latch the drain as finished and wake every waiter. Idempotent, so a
    /// second shutdown signal arriving mid-drain reports nothing twice.
    pub fn mark_complete(&self) {
        self.complete.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }

    /// Resolve once the drain has finished.
    pub async fn wait_complete(&self) {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            // Arm before the check so a `mark_complete` landing between the two
            // still wakes this waiter.
            notified.as_mut().enable();
            if self.is_complete() {
                return;
            }
            notified.await;
        }
    }
}

/// Outcome of one Data Plane drain.
#[derive(Debug, Clone)]
pub struct DataPlaneDrainReport {
    /// Cores that still owed work when the drain ended. Empty on a clean drain.
    pub pending: Vec<CorePending>,
    /// How long the drain took.
    pub elapsed: Duration,
    /// True when the drain ended on its deadline rather than on completion.
    pub timed_out: bool,
}

impl DataPlaneDrainReport {
    /// Whether every core finished within the deadline.
    pub fn is_clean(&self) -> bool {
        !self.timed_out && self.pending.is_empty()
    }
}

fn lock_dispatcher(shared: &SharedState) -> std::sync::MutexGuard<'_, Dispatcher> {
    match shared.dispatcher.lock() {
        Ok(guard) => guard,
        Err(poisoned) => {
            error!(
                target: "shutdown",
                "dispatcher mutex poisoned during the data plane drain — recovering"
            );
            poisoned.into_inner()
        }
    }
}

/// Close the enqueue gate, then wait for every core to finish what it holds.
///
/// The wait is bounded by `deadline`. On expiry the supervisor answers every
/// request the cores still hold with an error and lets shutdown proceed: a core
/// wedged on one slow task must not hold the process open.
pub async fn drain_data_plane_cores(
    shared: &SharedState,
    deadline: Duration,
) -> DataPlaneDrainReport {
    let start = Instant::now();
    lock_dispatcher(shared).begin_data_plane_drain();

    loop {
        // Routing responses is what clears the outstanding set, so the drain
        // does it itself rather than depending on the response poller's timing.
        shared.poll_and_route_responses();

        let pending = {
            let dispatcher = lock_dispatcher(shared);
            dispatcher.wake_all_cores();
            dispatcher.data_plane_pending()
        };
        if pending.is_empty() {
            return DataPlaneDrainReport {
                pending,
                elapsed: start.elapsed(),
                timed_out: false,
            };
        }
        if start.elapsed() >= deadline {
            let abandoned = lock_dispatcher(shared).abandon_data_plane_work();
            for response in abandoned {
                shared.tracker.complete(response);
            }
            return DataPlaneDrainReport {
                pending,
                elapsed: start.elapsed(),
                timed_out: true,
            };
        }
        tokio::time::sleep(DRAIN_POLL_INTERVAL).await;
    }
}

/// Register the Data Plane drain barrier and spawn the task that runs it.
///
/// Registration happens synchronously, before this returns, so the bus cannot
/// enter `DrainingDataPlane` without this barrier in place. The task is
/// critical: the bus waits for it past the per-phase budget, because a later
/// phase running while a core still executes is the exact defect the phase
/// exists to prevent. The wait inside the task carries its own deadline, so
/// "critical" never means "unbounded".
pub fn spawn_data_plane_drain_supervisor(
    shared: Arc<SharedState>,
    bus: &ShutdownBus,
    deadline: Duration,
) -> tokio::task::JoinHandle<()> {
    let mut guard =
        bus.register_critical_task(ShutdownPhase::DrainingDataPlane, "data_plane::cores");

    tokio::spawn(async move {
        guard.await_signal().await;
        let report = drain_data_plane_cores(&shared, deadline).await;
        if report.is_clean() {
            info!(
                target: "shutdown",
                duration_ms = report.elapsed.as_millis() as u64,
                "every data plane core finished its in-flight work"
            );
        } else {
            let offenders: Vec<String> = report.pending.iter().map(|p| p.to_string()).collect();
            error!(
                target: "shutdown",
                duration_ms = report.elapsed.as_millis() as u64,
                deadline_ms = deadline.as_millis() as u64,
                offenders = ?offenders,
                "data plane drain deadline expired — the work these cores still held was \
                 failed back to its callers and shutdown proceeds"
            );
        }
        shared.data_plane_drain.mark_complete();
        guard.report_drained();
    })
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::DocumentOp;
    use nodedb_types::QualifiedCollection;

    use super::*;
    use crate::bridge::dispatch::{BridgeResponse, CoreChannelDataSide};
    use crate::bridge::envelope::{
        Admission, ExemptReason, Payload, PhysicalPlan, Priority, Request, Response, Status,
    };
    use crate::control::shutdown::PHASE_BUDGET;
    use crate::types::{DatabaseId, Lsn, ReadConsistency, RequestId, TenantId, TraceId, VShardId};
    use crate::wal::WalManager;

    /// One-core state plus the Data-Plane side of its channel, so a test can
    /// answer a request the way a core would.
    fn test_state() -> (
        Arc<SharedState>,
        Vec<CoreChannelDataSide>,
        tempfile::TempDir,
    ) {
        let directory = tempfile::tempdir().expect("create drain test directory");
        let wal = Arc::new(
            WalManager::open_for_testing(&directory.path().join("drain.wal"))
                .expect("open drain test WAL"),
        );
        let (dispatcher, data_sides) = Dispatcher::new(1, 64);
        let shared = SharedState::new(dispatcher, wal).expect("construct drain test state");
        (shared, data_sides, directory)
    }

    fn read_request(id: u64) -> Request {
        Request {
            request_id: RequestId::new(id),
            tenant_id: TenantId::new(1),
            database_id: DatabaseId::DEFAULT,
            vshard_id: VShardId::new(0),
            plan: PhysicalPlan::Document(DocumentOp::PointGet {
                collection: QualifiedCollection::new(DatabaseId::DEFAULT, "c"),
                document_id: "d".into(),
                surrogate: nodedb_types::Surrogate::ZERO,
                pk_bytes: Vec::new(),
                rls_filters: Vec::new(),
                system_time: nodedb_types::SystemTimeScope::Current,
                valid_at_ms: None,
            }),
            deadline: Instant::now() + Duration::from_secs(30),
            priority: Priority::Normal,
            trace_id: TraceId::ZERO,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id: None,
            wal_lsn: None,
            resolved_now_ms: None,
            admission: Admission::Exempt(ExemptReason::Read),
        }
    }

    fn ok_response(id: u64) -> Response {
        Response {
            request_id: RequestId::new(id),
            status: Status::Ok,
            attempt: 1,
            partial: false,
            payload: Payload::empty(),
            watermark_lsn: Lsn::ZERO,
            error_code: None,
            read_set_valid: None,
            read_version_lsn: Lsn::ZERO,
            write_set: Vec::new(),
        }
    }

    /// Hand the request to the single core and register its waiter, exactly as
    /// a session would.
    fn dispatch_one(shared: &SharedState, id: u64) -> crate::control::ResponseReceiver {
        let rx = shared.tracker.register(RequestId::new(id));
        shared
            .dispatcher
            .lock()
            .expect("dispatcher lock")
            .dispatch(read_request(id))
            .expect("a running dispatcher accepts work");
        rx
    }

    /// Answer as the core would: push the response onto the Data-Plane side of
    /// the SPSC pair.
    fn answer_as_core(data_sides: &mut [CoreChannelDataSide], id: u64) {
        data_sides[0]
            .response_tx
            .try_push(BridgeResponse {
                inner: ok_response(id),
            })
            .expect("core response ring has room");
    }

    #[tokio::test]
    async fn the_drain_waits_for_work_a_core_still_holds_and_ends_when_it_lands() {
        let (shared, mut data_sides, _dir) = test_state();
        let _waiter = dispatch_one(&shared, 1);

        let drain_state = Arc::clone(&shared);
        let drain = tokio::spawn(async move {
            drain_data_plane_cores(&drain_state, Duration::from_secs(30)).await
        });

        tokio::time::sleep(Duration::from_millis(40)).await;
        assert!(
            !drain.is_finished(),
            "the drain must not finish while a core still holds in-flight work"
        );

        answer_as_core(&mut data_sides, 1);
        let report = tokio::time::timeout(Duration::from_secs(5), drain)
            .await
            .expect("the drain ends once the core answers")
            .expect("drain task");
        assert!(report.is_clean(), "clean drain, got {report:?}");
        assert!(report.pending.is_empty());
    }

    #[tokio::test]
    async fn the_drain_ends_on_its_deadline_when_a_core_never_answers() {
        let (shared, _data_sides, _dir) = test_state();
        let mut waiter = dispatch_one(&shared, 7);

        let report = drain_data_plane_cores(&shared, Duration::from_millis(80)).await;
        assert!(report.timed_out, "the bounded wait must fire");
        assert_eq!(report.pending.len(), 1, "one core still owed work");
        assert_eq!(report.pending[0].core_id, 0);

        let answer = waiter
            .try_recv()
            .expect("an abandoned request is answered, not left silent");
        assert_eq!(answer.status, Status::Error);
        assert!(
            shared
                .dispatcher
                .lock()
                .expect("dispatcher lock")
                .data_plane_pending()
                .is_empty(),
            "abandoning clears the pending set"
        );
    }

    #[tokio::test]
    async fn work_submitted_after_the_drain_begins_is_refused_with_an_error() {
        let (shared, _data_sides, _dir) = test_state();

        let report = drain_data_plane_cores(&shared, Duration::from_secs(5)).await;
        assert!(report.is_clean(), "an idle Data Plane drains at once");

        let err = shared
            .dispatcher
            .lock()
            .expect("dispatcher lock")
            .dispatch(read_request(2))
            .expect_err("work arriving after the drain must be refused");
        assert!(
            matches!(err, crate::Error::Dispatch { .. }),
            "the caller gets a dispatch error it can answer with: {err}"
        );
    }

    #[tokio::test]
    async fn the_sequencer_holds_the_phase_until_the_core_finishes() {
        let (shared, mut data_sides, _dir) = test_state();
        let watch = Arc::clone(&shared.shutdown);
        let (bus, mut handle) = ShutdownBus::new(watch);
        let supervisor =
            spawn_data_plane_drain_supervisor(Arc::clone(&shared), &bus, Duration::from_secs(30));

        let _waiter = dispatch_one(&shared, 3);

        bus.initiate();
        handle.await_phase(ShutdownPhase::DrainingDataPlane).await;
        tokio::time::sleep(PHASE_BUDGET + Duration::from_millis(50)).await;

        assert_eq!(
            bus.current_phase(),
            ShutdownPhase::DrainingDataPlane,
            "a core still holding work must block every later phase"
        );
        assert!(
            tokio::time::timeout(
                Duration::from_millis(50),
                handle.await_phase(ShutdownPhase::DrainingEventPlane),
            )
            .await
            .is_err(),
            "the Event Plane must not drain while the Data Plane is still executing"
        );

        answer_as_core(&mut data_sides, 3);
        tokio::time::timeout(
            Duration::from_secs(5),
            handle.await_phase(ShutdownPhase::Closed),
        )
        .await
        .expect("shutdown completes once the core finishes");
        supervisor.await.expect("drain supervisor");
        assert!(shared.data_plane_drain.is_complete());
    }

    #[tokio::test]
    async fn a_second_shutdown_signal_mid_drain_neither_panics_nor_double_reports() {
        let (shared, mut data_sides, _dir) = test_state();
        let (bus, mut handle) = ShutdownBus::new(Arc::clone(&shared.shutdown));
        let supervisor =
            spawn_data_plane_drain_supervisor(Arc::clone(&shared), &bus, Duration::from_secs(30));
        let _waiter = dispatch_one(&shared, 5);

        bus.initiate();
        handle.await_phase(ShutdownPhase::DrainingDataPlane).await;
        // Second signal while the drain is still waiting on the core.
        bus.initiate();
        bus.initiate();
        assert_eq!(bus.current_phase(), ShutdownPhase::DrainingDataPlane);

        answer_as_core(&mut data_sides, 5);
        tokio::time::timeout(
            Duration::from_secs(5),
            handle.await_phase(ShutdownPhase::Closed),
        )
        .await
        .expect("a repeated signal must not stall shutdown");
        supervisor.await.expect("drain supervisor");
    }

    #[tokio::test]
    async fn the_drain_is_idempotent_across_repeated_runs() {
        let (shared, _data_sides, _dir) = test_state();
        let first = drain_data_plane_cores(&shared, Duration::from_secs(5)).await;
        let second = drain_data_plane_cores(&shared, Duration::from_secs(5)).await;
        assert!(first.is_clean());
        assert!(second.is_clean());
    }

    #[tokio::test]
    async fn a_fresh_latch_is_not_complete() {
        let latch = DataPlaneDrain::new();
        assert!(!latch.is_complete());
        assert!(
            tokio::time::timeout(Duration::from_millis(20), latch.wait_complete())
                .await
                .is_err(),
            "waiting on an incomplete drain must not resolve"
        );
    }

    #[tokio::test]
    async fn wait_complete_resolves_once_marked() {
        let latch = DataPlaneDrain::new();
        let waiter = Arc::clone(&latch);
        let task = tokio::spawn(async move { waiter.wait_complete().await });
        tokio::task::yield_now().await;
        assert!(!task.is_finished());

        latch.mark_complete();
        task.await.expect("waiter resolves after mark_complete");
        assert!(latch.is_complete());
    }

    #[tokio::test]
    async fn marking_complete_twice_is_idempotent() {
        let latch = DataPlaneDrain::new();
        latch.mark_complete();
        latch.mark_complete();
        assert!(latch.is_complete());
        latch.wait_complete().await;
    }

    #[tokio::test]
    async fn wait_complete_returns_immediately_when_already_complete() {
        let latch = DataPlaneDrain::new();
        latch.mark_complete();
        tokio::time::timeout(Duration::from_millis(20), latch.wait_complete())
            .await
            .expect("an already-complete drain resolves without waiting");
    }
}
