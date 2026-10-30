// SPDX-License-Identifier: BUSL-1.1

//! The DDL preparation lease: the local lock and the replicated lease that
//! serialize descriptor preparation across the cluster.

use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use nodedb_cluster::{MetadataEntry, encode_entry};

use crate::control::state::SharedState;
use crate::error::Error;

use super::handle::MetadataRaftHandle;
use super::wait::await_metadata_apply;

/// How long a proposer waits for the lease. It outlasts the leader's
/// stuck-owner fallback, so a reclaim always frees the lease first.
const DDL_PREPARE_WAIT: Duration =
    super::ddl_owner::DDL_PREPARE_LEASE.saturating_add(Duration::from_secs(10));

/// Poll interval while another token holds the preparation lease.
const DDL_PREPARE_POLL: Duration = Duration::from_millis(10);

/// A fresh preparation-lease token for this node.
fn next_token(shared: &SharedState) -> u64 {
    let sequence = shared
        .metadata_ddl
        .token_seq
        .fetch_add(1, Ordering::Relaxed);
    shared.node_id.wrapping_mul(0x9e37_79b9_7f4a_7c15)
        ^ nodedb_types::wall_now_ns().rotate_left(17)
        ^ sequence
}

fn encode_metadata(entry: &MetadataEntry) -> Result<Vec<u8>, Error> {
    encode_entry(entry).map_err(|e| Error::Config {
        detail: format!("metadata entry encode: {e}"),
    })
}

/// Propose `entry` and wait until this node applied it, until the statement
/// deadline. Returns its log index.
pub(super) async fn propose_metadata_and_wait_async(
    shared: &SharedState,
    handle: &dyn MetadataRaftHandle,
    entry: &MetadataEntry,
) -> Result<u64, Error> {
    let index = handle.propose_async(encode_metadata(entry)?).await?;
    await_metadata_apply(shared, index).await
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

/// Abandon the preparation lease `token`: cancel any pending DDL record it
/// reserved, then release it, and await both applies here. The background
/// lease releaser calls it for a lease dropped unreleased.
pub(crate) async fn release_ddl_prepare_token(
    shared: &SharedState,
    token: u64,
) -> Result<(), Error> {
    let handle = shared.metadata_raft_handle()?;
    abandon_token(shared, handle.as_ref(), token).await
}

/// Cancel `token`'s pending DDL record, then release `token`'s lease.
///
/// The cancel is proposed even when this node has no record for `token`: a
/// `DdlPendingPropose` whose outcome its proposer never learned can still
/// apply, and it applies while `token` owns the lease. Proposed after it, the
/// cancel applies after it, so the record never outlives the lease. Cancel and
/// release are both no-ops for a token with nothing to cancel or release.
pub(super) async fn abandon_token(
    shared: &SharedState,
    handle: &dyn MetadataRaftHandle,
    token: u64,
) -> Result<(), Error> {
    propose_metadata_and_wait_async(shared, handle, &MetadataEntry::DdlPendingCancel { token })
        .await?;
    propose_metadata_and_wait_async(shared, handle, &MetadataEntry::DdlPrepareRelease { token })
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

    /// Release the lease and wait until the release applied here, until the
    /// statement deadline. A failed release is logged: the metadata leader
    /// reclaims the lease once [`super::ddl_owner::DDL_PREPARE_LEASE`] passed.
    pub(crate) async fn release(mut self) {
        self.released = true;
        match propose_metadata_and_wait_async(
            self.shared,
            self.handle,
            &MetadataEntry::DdlPrepareRelease { token: self.token },
        )
        .await
        {
            Ok(_) => {}
            // Proposed: it applies in log order, ahead of this node's next
            // acquire.
            Err(error @ Error::DeadlineExceeded { .. }) => tracing::warn!(
                token = self.token,
                %error,
                "metadata DDL lease release did not apply here before the statement deadline"
            ),
            Err(error) => {
                tracing::error!(token = self.token, %error, "metadata DDL lease release failed");
            }
        }
    }

    /// Give the lease up after a failure: cancel any pending DDL record the
    /// lease reserved, then release it, and wait until both applied here.
    ///
    /// For every exit that does not finalize. A failed abandon leaves the
    /// lease unreleased, so its drop hands it to the background releaser,
    /// which retries the same two steps.
    pub(crate) async fn abandon(mut self) {
        match abandon_token(self.shared, self.handle, self.token).await {
            Ok(()) => self.released = true,
            Err(error) => tracing::debug!(
                token = self.token,
                %error,
                "metadata DDL lease abandon failed; the background releaser retries it"
            ),
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
    // The guard owns `token` from before its first propose. An acquire whose
    // outcome is unknown (a missed deadline, an unanswered leader forward, a
    // cancelled future) can still apply and make `token` the owner, so every
    // exit that does not return the lease drops this guard and releases the
    // token. A release of a token that never owned the lease applies as a
    // no-op, and it is proposed after every acquire of that token.
    let lease = DdlPrepareLease {
        shared,
        handle,
        token,
        released: false,
    };

    loop {
        propose_metadata_and_wait_async(
            shared,
            handle,
            &MetadataEntry::DdlPrepareAcquire {
                token,
                node_id: shared.node_id,
            },
        )
        .await?;

        loop {
            match owner_step(shared, token, deadline)? {
                OwnerStep::Acquired => return Ok(lease),
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
    use std::sync::Arc;

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

    /// An acquire cancelled at any point, before or after its propose
    /// applied, leaves the lease free well before the leader's reclaim
    /// window: the next acquire gets it at once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_cancelled_acquire_never_strands_the_lease() {
        let cluster = crate::control::cluster::test_one_node::boot().await;
        let state = Arc::clone(&cluster.state);
        let handle = state.metadata_raft_handle().expect("metadata raft handle");
        for cancel_after_us in [0u64, 100, 500, 1_000, 2_000, 5_000, 10_000] {
            let cancelled = tokio::time::timeout(
                Duration::from_micros(cancel_after_us),
                acquire_ddl_prepare_lease_async(&state, handle.as_ref()),
            )
            .await;
            match cancelled {
                Ok(lease) => lease.expect("acquire").release().await,
                Err(_elapsed) => {}
            }
            let lease = tokio::time::timeout(
                Duration::from_secs(5),
                acquire_ddl_prepare_lease_async(&state, handle.as_ref()),
            )
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "an acquire cancelled after {cancel_after_us}us left the lease held; the \
                     next acquire waited for the reclaim"
                )
            })
            .expect("acquire after a cancelled acquire");
            lease.release().await;
        }
        drop(state);
        cluster.shutdown().await;
    }
}
