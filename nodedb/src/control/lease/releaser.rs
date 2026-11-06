// SPDX-License-Identifier: BUSL-1.1

//! Background release of leases that synchronous code gives up.
//!
//! A `Drop`, a metadata applier step, or any other synchronous site never
//! waits for a metadata proposal. It hands the release to [`LeaseReleaser`]
//! instead: a bounded queue owned by `SharedState`. One Control-Plane task
//! drains the queue, proposes each release, and retries a failed one with
//! backoff.
//!
//! A descriptor lease or a DDL preparation lease whose release is never
//! proposed costs latency only. A descriptor lease expires at its
//! `expires_at`, and the renewal loop releases an idle one at expiry. A DDL
//! preparation lease is reclaimed by the metadata leader once its lease window
//! passed. Each such loss files a report, except an idle descriptor lease at
//! shutdown, which the shutdown release and the expiry cover.
//!
//! A drain has no expiry. A drain end that is never proposed leaves its
//! descriptor drained until a restart, so a drain end is never dropped while
//! the task runs. A full queue hands it to the [`DrainEndBacklog`], which needs
//! no queue slot. An end whose attempts all failed files a report and retries
//! until it applies. At shutdown each waiting end gets one bounded attempt,
//! and a failed one files a report.

use std::collections::HashSet;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use nodedb_cluster::{DescriptorId, DrainOwner};
use tokio::sync::mpsc;
use tokio::time::Instant;

use super::drain_end_backlog::{DrainEnd, DrainEndBacklog};
use crate::control::shutdown::{ShutdownPhase, ShutdownReceiver, spawn_loop};
use crate::control::state::SharedState;
use crate::error::Error;

/// Requests the queue holds. A drain end beyond it waits in the backlog, and
/// any other request is dropped to its expiry path.
const QUEUE_CAPACITY: usize = 1024;

/// Proposals one round of a request makes.
const MAX_ATTEMPTS: u32 = 6;

/// Wait before the second attempt. Each later wait doubles it.
const FIRST_BACKOFF: Duration = Duration::from_millis(50);

/// Wait between rounds for a drain end whose attempts all failed.
const DRAIN_END_RETRY_INTERVAL: Duration = Duration::from_secs(5);

/// The time all waiting drain ends share at shutdown. It sits inside the 500 ms
/// a shutdown phase gives this task, so a failed end still files its report.
const SHUTDOWN_DRAIN_END_WAIT: Duration = Duration::from_millis(250);

/// One release the background task proposes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ReleaseRequest {
    /// Release this node's leases on these descriptors that no statement
    /// holds when the release runs.
    UnheldDescriptors(Vec<DescriptorId>),
    /// Abandon the DDL preparation lease `token`: cancel its pending DDL
    /// record, then release it.
    DdlPrepare { token: u64 },
    /// End `owner`'s drain on `descriptor_id`.
    DrainEnd {
        descriptor_id: DescriptorId,
        owner: DrainOwner,
    },
}

/// The bounded queue of releases synchronous code hands off.
pub struct LeaseReleaser {
    tx: mpsc::Sender<ReleaseRequest>,
    rx: Mutex<Option<mpsc::Receiver<ReleaseRequest>>>,
    backlog: Arc<DrainEndBacklog>,
}

impl Default for LeaseReleaser {
    fn default() -> Self {
        let (tx, rx) = mpsc::channel(QUEUE_CAPACITY);
        Self {
            tx,
            rx: Mutex::new(Some(rx)),
            backlog: Arc::new(DrainEndBacklog::default()),
        }
    }
}

impl LeaseReleaser {
    /// Hand `request` to the background task without waiting. See
    /// [`ReleaseQueue::submit`].
    pub(crate) fn submit(&self, request: ReleaseRequest) {
        self.queue().submit(request);
    }

    /// A handle that queues releases into this releaser, for an owner that
    /// outlives no `SharedState` borrow.
    pub(crate) fn queue(&self) -> ReleaseQueue {
        ReleaseQueue {
            tx: self.tx.clone(),
            backlog: Arc::clone(&self.backlog),
        }
    }

    /// The queue's receiving end. The first call takes it, later calls get
    /// `None`.
    pub(super) fn take_receiver(&self) -> Option<mpsc::Receiver<ReleaseRequest>> {
        self.rx.lock().unwrap_or_else(|p| p.into_inner()).take()
    }
}

/// The sending end of a [`LeaseReleaser`].
#[derive(Clone)]
pub(crate) struct ReleaseQueue {
    tx: mpsc::Sender<ReleaseRequest>,
    backlog: Arc<DrainEndBacklog>,
}

impl ReleaseQueue {
    /// Hand `request` to the background task without waiting.
    ///
    /// A full queue moves a drain end to the backlog. It drops any other
    /// request to its expiry path, with a report. Once the task stopped, a
    /// drain end files a report: nothing will propose it.
    pub(crate) fn submit(&self, request: ReleaseRequest) {
        match self.tx.try_send(request) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(ReleaseRequest::DrainEnd {
                descriptor_id,
                owner,
            })) => {
                if let Err((descriptor_id, owner)) = self.backlog.hold((descriptor_id, owner)) {
                    give_up(
                        &ReleaseRequest::DrainEnd {
                            descriptor_id,
                            owner,
                        },
                        &releaser_stopped(),
                    );
                }
            }
            Err(mpsc::error::TrySendError::Full(request)) => give_up(
                &request,
                &Error::Internal {
                    detail: "lease release queue full".into(),
                },
            ),
            Err(mpsc::error::TrySendError::Closed(request)) => dropped_at_shutdown(&request),
        }
    }
}

/// The error of a request no releaser task will propose.
fn releaser_stopped() -> Error {
    Error::Internal {
        detail: "lease releaser stopped".into(),
    }
}

/// Record a request the stopped task will never propose. The shutdown release
/// and the lease's expiry cover an idle descriptor lease, so it files no report.
fn dropped_at_shutdown(request: &ReleaseRequest) {
    match request {
        ReleaseRequest::UnheldDescriptors(_) => {
            tracing::debug!(
                ?request,
                "lease releaser stopped; the lease ends at its expiry"
            );
        }
        ReleaseRequest::DdlPrepare { .. } | ReleaseRequest::DrainEnd { .. } => {
            give_up(request, &releaser_stopped());
        }
    }
}

/// Spawn the task that drains `shared`'s release queue. `start_raft` calls it
/// once the metadata raft handle is installed. A second call spawns nothing.
pub(crate) fn spawn_lease_releaser(shared: &Arc<SharedState>) {
    let Some(mut rx) = shared.lease_runtime.releaser.take_receiver() else {
        tracing::warn!("lease releaser already running; start_raft appears to have run twice");
        return;
    };
    let backlog = Arc::clone(&shared.lease_runtime.releaser.backlog);
    let weak = Arc::downgrade(shared);
    spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "lease_releaser",
        ShutdownPhase::DrainingControlPlane,
        move |mut shutdown| async move {
            let mut failed = FailedDrainEnds::default();
            loop {
                tokio::select! {
                    biased;
                    _ = shutdown.wait_cancelled() => break,
                    _ = backlog.woken() => {
                        end_drains(&weak, backlog.take(), &mut failed, &mut shutdown).await;
                    }
                    request = rx.recv() => {
                        let Some(request) = request else { break };
                        let round = release_round(&weak, &request, &mut shutdown).await;
                        after_round(request, round, &mut failed);
                    }
                    _ = tokio::time::sleep_until(failed.next_retry), if !failed.ends.is_empty() => {
                        let due = std::mem::take(&mut failed.ends);
                        end_drains(&weak, due, &mut failed, &mut shutdown).await;
                    }
                }
            }
            finish_at_shutdown(&weak, rx, &backlog, failed.ends).await;
        },
    );
}

/// Drain ends whose attempts all failed, with the time of their next round.
struct FailedDrainEnds {
    ends: HashSet<DrainEnd>,
    next_retry: Instant,
}

impl Default for FailedDrainEnds {
    fn default() -> Self {
        Self {
            ends: HashSet::new(),
            next_retry: Instant::now(),
        }
    }
}

impl FailedDrainEnds {
    /// Keep `end` for the next round. The first end of a round starts its
    /// timer, so later failures do not push the round back.
    fn add(&mut self, end: DrainEnd) {
        if self.ends.is_empty() {
            self.next_retry = Instant::now() + DRAIN_END_RETRY_INTERVAL;
        }
        self.ends.insert(end);
    }
}

/// How one round of a request ended.
enum Round {
    Applied,
    /// Every attempt failed, and the report is filed.
    Failed,
    /// Shutdown started before the round finished. No report is filed yet.
    Interrupted,
}

/// One round of `request`. A shutdown interrupts it, so the task's last pass
/// runs inside its shutdown budget.
async fn release_round(
    shared: &Weak<SharedState>,
    request: &ReleaseRequest,
    shutdown: &mut ShutdownReceiver,
) -> Round {
    tokio::select! {
        biased;
        _ = shutdown.wait_cancelled() => Round::Interrupted,
        applied = release_with_retry(shared, request) => {
            if applied { Round::Applied } else { Round::Failed }
        }
    }
}

/// Keep a drain end that did not apply for the next round, or for the last
/// pass. A failed request of another kind has its report, and an interrupted
/// one files it here.
fn after_round(request: ReleaseRequest, round: Round, failed: &mut FailedDrainEnds) {
    match (round, request) {
        (Round::Applied, _) => {}
        (
            Round::Failed | Round::Interrupted,
            ReleaseRequest::DrainEnd {
                descriptor_id,
                owner,
            },
        ) => failed.add((descriptor_id, owner)),
        (Round::Failed, _) => {}
        (Round::Interrupted, other) => dropped_at_shutdown(&other),
    }
}

/// One round of each drain end.
async fn end_drains(
    shared: &Weak<SharedState>,
    ends: HashSet<DrainEnd>,
    failed: &mut FailedDrainEnds,
    shutdown: &mut ShutdownReceiver,
) {
    for (descriptor_id, owner) in ends {
        let request = ReleaseRequest::DrainEnd {
            descriptor_id,
            owner,
        };
        let round = release_round(shared, &request, shutdown).await;
        after_round(request, round, failed);
    }
}

/// The task's last pass: close both inputs, then make one bounded attempt at
/// every drain end still waiting. A failed attempt files a report, since
/// nothing proposes that end afterwards.
async fn finish_at_shutdown(
    shared: &Weak<SharedState>,
    mut rx: mpsc::Receiver<ReleaseRequest>,
    backlog: &DrainEndBacklog,
    failed: HashSet<DrainEnd>,
) {
    rx.close();
    let mut ends = backlog.close();
    ends.extend(failed);
    while let Ok(request) = rx.try_recv() {
        match request {
            ReleaseRequest::DrainEnd {
                descriptor_id,
                owner,
            } => {
                ends.insert((descriptor_id, owner));
            }
            other => dropped_at_shutdown(&other),
        }
    }
    let deadline = Instant::now() + SHUTDOWN_DRAIN_END_WAIT;
    for (descriptor_id, owner) in ends {
        let request = ReleaseRequest::DrainEnd {
            descriptor_id,
            owner,
        };
        let Some(state) = shared.upgrade() else {
            give_up(&request, &releaser_stopped());
            continue;
        };
        match tokio::time::timeout_at(deadline, release_once(&state, &request)).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => give_up(&request, &error),
            Err(_) => give_up(&request, &releaser_stopped()),
        }
    }
}

/// Propose `request` until it applies or [`MAX_ATTEMPTS`] failed. `true` when
/// it applied, or when `SharedState` is gone and with it the node's drains.
async fn release_with_retry(shared: &Weak<SharedState>, request: &ReleaseRequest) -> bool {
    let mut backoff = FIRST_BACKOFF;
    for attempt in 1..=MAX_ATTEMPTS {
        let Some(state) = shared.upgrade() else {
            return true;
        };
        match release_once(&state, request).await {
            Ok(()) => return true,
            Err(error) if attempt < MAX_ATTEMPTS => {
                tracing::debug!(?request, attempt, %error, "lease release failed; retrying");
            }
            Err(error) => {
                give_up(request, &error);
                return false;
            }
        }
        drop(state);
        tokio::time::sleep(backoff).await;
        backoff = backoff.saturating_mul(2);
    }
    false
}

/// Record a request that did not apply. A descriptor lease ends at its expiry
/// and the metadata leader reclaims a DDL preparation lease, but a drain has
/// no expiry backstop.
fn give_up(request: &ReleaseRequest, error: &Error) {
    match request {
        ReleaseRequest::UnheldDescriptors(ids) => {
            crate::diag::descriptor_lease_release_dropped(error, ids.len());
        }
        ReleaseRequest::DdlPrepare { token } => {
            crate::diag::ddl_cleanup_not_completed(error, "abandon_lease", *token);
        }
        ReleaseRequest::DrainEnd {
            descriptor_id,
            owner,
        } => crate::diag::descriptor_drain_not_ended(error, descriptor_id, owner),
    }
}

async fn release_once(shared: &SharedState, request: &ReleaseRequest) -> Result<(), Error> {
    match request {
        ReleaseRequest::UnheldDescriptors(ids) => {
            super::release::release_unheld_leases(shared, ids.clone()).await
        }
        ReleaseRequest::DdlPrepare { token } => {
            crate::control::metadata_proposer::release_ddl_prepare_token(shared, *token).await
        }
        ReleaseRequest::DrainEnd {
            descriptor_id,
            owner,
        } => super::end_drain_async(shared, descriptor_id.clone(), owner.clone()).await,
    }
}

#[cfg(test)]
mod tests {
    use nodedb_cluster::DescriptorKind;

    use super::*;

    #[test]
    fn a_full_queue_drops_the_request_without_blocking() {
        let releaser = LeaseReleaser::default();
        for token in 0..QUEUE_CAPACITY as u64 {
            releaser.submit(ReleaseRequest::DdlPrepare { token });
        }
        releaser.submit(ReleaseRequest::DdlPrepare { token: u64::MAX });
        let mut rx = releaser.take_receiver().expect("receiver");
        let mut queued = 0;
        while let Ok(request) = rx.try_recv() {
            assert_ne!(request, ReleaseRequest::DdlPrepare { token: u64::MAX });
            queued += 1;
        }
        assert_eq!(queued, QUEUE_CAPACITY);
        assert!(releaser.take_receiver().is_none());
    }

    fn drain_end(name: &str) -> ReleaseRequest {
        ReleaseRequest::DrainEnd {
            descriptor_id: DescriptorId::new(0, 7, DescriptorKind::Collection, name),
            owner: DrainOwner::Ddl,
        }
    }

    /// A drain has no expiry, so a full queue must not lose its end.
    #[test]
    fn a_full_queue_keeps_a_drain_end_in_the_backlog() {
        let releaser = LeaseReleaser::default();
        for token in 0..QUEUE_CAPACITY as u64 {
            releaser.submit(ReleaseRequest::DdlPrepare { token });
        }

        releaser.submit(drain_end("orders"));
        releaser.submit(drain_end("orders"));

        let held = releaser.backlog.take();
        assert_eq!(held.len(), 1, "one drain holds one entry");
        assert!(held.iter().any(|(id, _)| id.name == "orders"));
    }

    /// A stopped task leaves no end waiting for a sweep that never comes.
    #[test]
    fn a_closed_queue_holds_no_drain_end() {
        let releaser = LeaseReleaser::default();
        drop(releaser.take_receiver());

        releaser.submit(drain_end("orders"));

        assert!(releaser.backlog.take().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_round_keeps_its_first_timer() {
        let mut failed = FailedDrainEnds::default();
        let id = |name: &str| DescriptorId::new(0, 7, DescriptorKind::Collection, name);
        failed.add((id("a"), DrainOwner::Ddl));
        let first = failed.next_retry;

        tokio::time::advance(Duration::from_secs(1)).await;
        failed.add((id("b"), DrainOwner::Ddl));

        assert_eq!(failed.next_retry, first);
        assert_eq!(failed.ends.len(), 2);
    }

    /// The task ends a drain held in the backlog, with no queue slot involved.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_task_ends_a_drain_from_the_backlog() {
        let cluster = crate::control::cluster::test_one_node::boot().await;
        let state = &cluster.state;
        let id = DescriptorId::new(0, 7, DescriptorKind::Collection, "backlogged");
        super::super::drain_for_ddl_async(
            state,
            id.clone(),
            3,
            crate::control::metadata_proposer::DEFAULT_DRAIN_TIMEOUT,
            0,
        )
        .await
        .expect("an unleased descriptor drains at once");
        assert!(state.lease_drain.is_draining(&id, 3));

        state
            .lease_runtime
            .releaser
            .backlog
            .hold((id.clone(), DrainOwner::Ddl))
            .expect("the task runs");

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while state.lease_drain.is_draining(&id, 3) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "a held drain end left its drain in force"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        cluster.shutdown().await;
    }
}
