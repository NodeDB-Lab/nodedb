// SPDX-License-Identifier: BUSL-1.1

//! The DDL preparation lease: the local lock and the replicated lease that
//! serialize descriptor preparation across the cluster.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use nodedb_cluster::{METADATA_GROUP_ID, MetadataEntry, WaitOutcome, encode_entry};

use crate::control::state::SharedState;
use crate::error::Error;

use super::handle::MetadataRaftHandle;
use super::timeouts::DEFAULT_PROPOSE_TIMEOUT;
use super::wait::wait_applied;

/// How long a proposer waits for the lease. It outlasts the leader's
/// stuck-owner fallback, so a reclaim always frees the lease first.
const DDL_PREPARE_WAIT: Duration =
    super::ddl_owner::DDL_PREPARE_LEASE.saturating_add(Duration::from_secs(10));

fn wall_now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos()
        .min(u64::MAX as u128) as u64
}

/// Poll interval while another token holds the preparation lease.
const DDL_PREPARE_POLL: Duration = Duration::from_millis(10);

/// A fresh preparation-lease token for this node.
fn next_token(shared: &SharedState) -> u64 {
    let sequence = shared
        .metadata_ddl
        .token_seq
        .fetch_add(1, Ordering::Relaxed);
    shared.node_id.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ wall_now_ns().rotate_left(17) ^ sequence
}

fn encode_metadata(entry: &MetadataEntry) -> Result<Vec<u8>, Error> {
    encode_entry(entry).map_err(|e| Error::Config {
        detail: format!("metadata entry encode: {e}"),
    })
}

/// The result of waiting `timeout` for log index `index` to apply.
fn applied_or_error(
    outcome: WaitOutcome,
    index: u64,
    timeout: Duration,
    current: u64,
) -> Result<u64, Error> {
    match outcome {
        WaitOutcome::Reached => Ok(index),
        WaitOutcome::TimedOut => Err(Error::Config {
            detail: format!(
                "metadata propose timed out after {timeout:?} waiting for log index {index} \
                 (current: {current})"
            ),
        }),
        WaitOutcome::GroupGone => Err(Error::Config {
            detail: "metadata group no longer hosted on this node".into(),
        }),
    }
}

pub(super) async fn propose_metadata_and_wait_async(
    shared: &SharedState,
    handle: &dyn MetadataRaftHandle,
    entry: &MetadataEntry,
    timeout: Duration,
) -> Result<u64, Error> {
    let index = handle.propose_async(encode_metadata(entry)?).await?;
    let watcher = shared.applied_index_watcher(METADATA_GROUP_ID);
    let outcome = wait_applied(Arc::clone(&watcher), index, timeout).await?;
    applied_or_error(outcome, index, timeout, watcher.current())
}

/// What the acquire loop does next, from the current lease owner.
enum OwnerStep {
    /// `token` holds the lease.
    Acquired,
    /// No owner: propose the acquire again.
    Retry,
    /// Another token holds the lease: poll again. The metadata leader's
    /// reclaim loop frees a lease whose owner died or got stuck.
    Wait,
}

fn owner_step(shared: &SharedState, token: u64, deadline: Instant) -> Result<OwnerStep, Error> {
    match super::ddl_owner::current_owner(shared) {
        Some(owner) if owner.token == token => Ok(OwnerStep::Acquired),
        None => Ok(OwnerStep::Retry),
        Some(_) if Instant::now() < deadline => Ok(OwnerStep::Wait),
        Some(owner) => Err(Error::Config {
            detail: format!(
                "metadata DDL preparation lease timed out after {DDL_PREPARE_WAIT:?}: node {} \
                 still holds it",
                owner.node_id
            ),
        }),
    }
}

/// Release the preparation lease `token` and await the release's apply here.
/// The background lease releaser calls it for a lease dropped unreleased.
pub(crate) async fn release_ddl_prepare_token(
    shared: &SharedState,
    token: u64,
) -> Result<(), Error> {
    let handle = shared.metadata_raft_handle()?;
    propose_metadata_and_wait_async(
        shared,
        handle.as_ref(),
        &MetadataEntry::DdlPrepareRelease { token },
        DEFAULT_PROPOSE_TIMEOUT,
    )
    .await
    .map(|_| ())
}

/// The metadata-Raft-serialized descriptor preparation lease an async
/// proposer holds. The matching release is itself replicated, so another
/// node cannot stamp from the same prior catalog version until that release
/// has applied.
///
/// [`Self::release`] releases it and awaits the release's apply. A lease
/// dropped unreleased, as when its proposer's future is cancelled, hands its
/// release to the background lease releaser and never blocks. A release that
/// never applies ends when the metadata leader reclaims the lease (see
/// [`super::ddl_reclaim`]).
pub(crate) struct DdlPrepareLease<'a> {
    shared: &'a SharedState,
    handle: &'a dyn MetadataRaftHandle,
    token: u64,
    released: bool,
}

impl DdlPrepareLease<'_> {
    pub(crate) fn token(&self) -> u64 {
        self.token
    }

    /// Release the lease and wait until the release applied here. A failed
    /// release is logged: the metadata leader reclaims the lease once
    /// [`super::ddl_owner::DDL_PREPARE_LEASE`] passed.
    pub(crate) async fn release(mut self) {
        self.released = true;
        if let Err(error) = propose_metadata_and_wait_async(
            self.shared,
            self.handle,
            &MetadataEntry::DdlPrepareRelease { token: self.token },
            DEFAULT_PROPOSE_TIMEOUT,
        )
        .await
        {
            tracing::error!(token = self.token, %error, "metadata DDL lease release failed");
        }
    }
}

impl Drop for DdlPrepareLease<'_> {
    fn drop(&mut self) {
        if !self.released {
            self.shared.lease_runtime.releaser.submit(
                crate::control::lease::releaser::ReleaseRequest::DdlPrepare { token: self.token },
            );
        }
    }
}

/// Take the preparation lease from async code.
pub(crate) async fn acquire_ddl_prepare_lease_async<'a>(
    shared: &'a SharedState,
    handle: &'a dyn MetadataRaftHandle,
) -> Result<DdlPrepareLease<'a>, Error> {
    let token = next_token(shared);
    let deadline = Instant::now() + DDL_PREPARE_WAIT;

    loop {
        propose_metadata_and_wait_async(
            shared,
            handle,
            &MetadataEntry::DdlPrepareAcquire {
                token,
                node_id: shared.node_id,
            },
            DEFAULT_PROPOSE_TIMEOUT,
        )
        .await?;

        loop {
            match owner_step(shared, token, deadline)? {
                OwnerStep::Acquired => {
                    return Ok(DdlPrepareLease {
                        shared,
                        handle,
                        token,
                        released: false,
                    });
                }
                OwnerStep::Retry => break,
                OwnerStep::Wait => tokio::time::sleep(DDL_PREPARE_POLL).await,
            }
        }
    }
}

/// Take the local DDL preparation lock from async code. The guard is `Send`,
/// so the holder can await its post-apply while it holds the lock.
pub(crate) async fn lock_ddl_preparation_async(
    shared: &SharedState,
) -> tokio::sync::MutexGuard<'_, ()> {
    shared.metadata_ddl.lock.lock().await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn owner(shared: &SharedState) -> Option<u64> {
        let current = *shared
            .metadata_ddl
            .owner
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        current.map(|owner| owner.token)
    }

    /// A preparation lease dropped unreleased, as a cancelled DDL drops it,
    /// is released by the background releaser without blocking the drop.
    #[tokio::test]
    async fn a_dropped_preparation_lease_is_released_in_the_background() {
        let cluster = crate::control::cluster::test_one_node::boot().await;
        let state = Arc::clone(&cluster.state);
        let token = {
            let handle = state.metadata_raft_handle().expect("metadata raft handle");
            let lease = acquire_ddl_prepare_lease_async(&state, handle.as_ref())
                .await
                .expect("take the preparation lease");
            assert_eq!(owner(&state), Some(lease.token()));
            lease.token()
        };
        assert_eq!(
            owner(&state),
            Some(token),
            "the drop itself proposes nothing"
        );

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while owner(&state).is_some() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "the background releaser did not release the dropped lease"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        drop(state);
        cluster.shutdown().await;
    }
}
