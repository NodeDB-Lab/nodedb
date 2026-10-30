// SPDX-License-Identifier: BUSL-1.1

//! Background release of leases that synchronous code gives up.
//!
//! A `Drop`, a metadata applier step, or any other synchronous site never
//! waits for a metadata proposal. It hands the release to [`LeaseReleaser`]
//! instead: a bounded queue owned by `SharedState`. One Control-Plane task
//! drains the queue, proposes each release, and retries a failed one with
//! backoff.
//!
//! A release that is never proposed, because the queue is full, every
//! attempt failed, or the node shut down, costs latency only. A descriptor
//! lease expires at its `expires_at`, and the renewal loop releases an idle
//! one at expiry. A DDL preparation lease is reclaimed by the metadata leader
//! once its lease window passed. A drain has no expiry: a drain end that is
//! never proposed leaves its descriptor drained until a later end or a
//! restart, so its failure is recorded where it is detected.

use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use nodedb_cluster::{DescriptorId, DrainOwner};
use tokio::sync::mpsc;

use crate::control::shutdown::{ShutdownPhase, spawn_loop};
use crate::control::state::SharedState;
use crate::error::Error;

/// Requests the queue holds before a new one is dropped to its expiry path.
const QUEUE_CAPACITY: usize = 1024;

/// Proposals one request makes before it is left to its expiry path.
const MAX_ATTEMPTS: u32 = 6;

/// Wait before the second attempt. Each later wait doubles it.
const FIRST_BACKOFF: Duration = Duration::from_millis(50);

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
}

impl Default for LeaseReleaser {
    fn default() -> Self {
        let (tx, rx) = mpsc::channel(QUEUE_CAPACITY);
        Self {
            tx,
            rx: Mutex::new(Some(rx)),
        }
    }
}

impl LeaseReleaser {
    /// Queue `request` without waiting. A full or closed queue drops it and
    /// logs: the lease then ends through its expiry path.
    pub(crate) fn submit(&self, request: ReleaseRequest) {
        self.queue().submit(request);
    }

    /// A handle that queues releases into this releaser, for an owner that
    /// outlives no `SharedState` borrow.
    pub(crate) fn queue(&self) -> ReleaseQueue {
        ReleaseQueue {
            tx: self.tx.clone(),
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
}

impl ReleaseQueue {
    /// Queue `request` without waiting. A full or closed queue drops it and
    /// logs: the lease then ends through its expiry path.
    pub(crate) fn submit(&self, request: ReleaseRequest) {
        match self.tx.try_send(request) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(request)) => tracing::warn!(
                ?request,
                "lease release queue full; the lease ends at its expiry"
            ),
            Err(mpsc::error::TrySendError::Closed(request)) => tracing::warn!(
                ?request,
                "lease release queue closed; the lease ends at its expiry"
            ),
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
    let weak = Arc::downgrade(shared);
    spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "lease_releaser",
        ShutdownPhase::DrainingControlPlane,
        move |mut shutdown| async move {
            loop {
                tokio::select! {
                    biased;
                    _ = shutdown.wait_cancelled() => break,
                    request = rx.recv() => {
                        let Some(request) = request else { break };
                        release_with_retry(&weak, &request).await;
                    }
                }
            }
        },
    );
}

/// Propose `request` until it applies or [`MAX_ATTEMPTS`] failed.
async fn release_with_retry(shared: &Weak<SharedState>, request: &ReleaseRequest) {
    let mut backoff = FIRST_BACKOFF;
    for attempt in 1..=MAX_ATTEMPTS {
        let Some(state) = shared.upgrade() else {
            return;
        };
        match release_once(&state, request).await {
            Ok(()) => return,
            Err(error) if attempt < MAX_ATTEMPTS => {
                tracing::debug!(?request, attempt, %error, "lease release failed; retrying");
            }
            Err(error) => {
                give_up(request, &error);
                return;
            }
        }
        drop(state);
        tokio::time::sleep(backoff).await;
        backoff = backoff.saturating_mul(2);
    }
}

/// Record a request every attempt failed to apply. A descriptor lease ends at
/// its expiry and the metadata leader reclaims a DDL preparation lease, but a
/// drain has no expiry backstop.
fn give_up(request: &ReleaseRequest, error: &Error) {
    match request {
        ReleaseRequest::UnheldDescriptors(_) => tracing::warn!(
            ?request,
            %error,
            "lease release failed on every attempt; the lease ends at its expiry"
        ),
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
}
