// SPDX-License-Identifier: BUSL-1.1

//! The descriptor lease drain, proposed and awaited.
//!
//! The flow is the one [`super::drain_propose`] describes. Every wait is
//! awaited: the drain variants' metadata commits apply while the proposer's
//! task yields, and the lease wait sleeps until a hold or lease changes or
//! the timer fires. No runtime flavor loses a worker to it.

use std::time::{Duration, Instant};

use nodedb_cluster::{DescriptorId, DrainOwner, MetadataEntry};

use crate::control::metadata_proposer::wait::await_metadata_apply;
use crate::control::state::SharedState;
use crate::error::Error;

use super::drain_propose::{POLL_INTERVAL, drain_start_entry, drained_or_timed_out, encode_drain};

/// Drain every lease on `id` at `version <= up_to_version` for a `Put*` DDL.
///
/// The drain is owned by [`DrainOwner::Ddl`], so the apply of the DDL's own
/// catalog entry ends it and no other owner's drain.
///
/// Returns `Ok(())` once they have drained. Errors on timeout or propose
/// failure.
///
/// `own_holds` is how many of those refcount units the requesting transaction
/// holds itself — `0` for a caller with no lease scope of its own. A
/// transaction altering a descriptor it also holds a statement-time lease on
/// cannot wait for its own hold: it cannot release that lease until this
/// call returns.
pub async fn drain_for_ddl_async(
    shared: &SharedState,
    id: DescriptorId,
    up_to_version: u64,
    max_wait: Duration,
    own_holds: u32,
) -> Result<(), Error> {
    start_drain(
        shared,
        id,
        DrainOwner::Ddl,
        up_to_version,
        max_wait,
        own_holds,
    )
    .await
}

/// Drain every lease on `id`, at any version, under `owner`.
///
/// For a holder that stops all writes for a span of work rather than for one
/// version bump. Covering every version keeps the drain in force when a DDL
/// bumps the descriptor's version meanwhile. Only [`end_drain_async`] with the
/// same `owner`, or the owner's own implicit clear, ends it.
pub async fn drain_for_owner_async(
    shared: &SharedState,
    id: DescriptorId,
    owner: DrainOwner,
    max_wait: Duration,
) -> Result<(), Error> {
    start_drain(shared, id, owner, u64::MAX, max_wait, 0).await
}

async fn start_drain(
    shared: &SharedState,
    id: DescriptorId,
    owner: DrainOwner,
    up_to_version: u64,
    max_wait: Duration,
    own_holds: u32,
) -> Result<(), Error> {
    // No prior version means no lease can exist.
    if up_to_version == 0 {
        return Ok(());
    }
    if let Err(error) = propose_drain_async(
        shared,
        drain_start_entry(shared, &id, &owner, up_to_version, max_wait),
        "drain_start",
    )
    .await
    {
        // The start may be in the log even when its apply wait failed, and it
        // then applies. Ending a drain that is not active is a no-op.
        return Err(end_failed_drain(shared, id, owner, error).await);
    }

    let deadline = Instant::now() + max_wait;
    let drained = loop {
        // A hold given back or a lease release applied wakes the wait. The
        // timer covers what no event marks: a lease expiry, a holder's death.
        let changed = shared.lease_drain.holds_changed();
        tokio::pin!(changed);
        changed.as_mut().enable();
        match drained_or_timed_out(shared, &id, up_to_version, max_wait, own_holds, deadline) {
            Ok(true) => break Ok(()),
            Ok(false) => {
                tokio::select! {
                    _ = changed.as_mut() => {}
                    _ = tokio::time::sleep(POLL_INTERVAL) => {}
                }
            }
            Err(error) => break Err(error),
        }
    };
    if let Err(error) = drained {
        return Err(end_failed_drain(shared, id, owner, error).await);
    }
    Ok(())
}

/// End the drain a failed start or wait leaves behind. `is_draining` has no
/// expiry backstop, so this explicit propose is the only thing that clears
/// the drain. Its own errors are logged and dropped. Returns `error`, the
/// failure that stopped the drain.
async fn end_failed_drain(
    shared: &SharedState,
    id: DescriptorId,
    owner: DrainOwner,
    error: Error,
) -> Error {
    if let Err(cleanup_err) = end_drain_async(shared, id, owner).await {
        tracing::warn!(
            error = %cleanup_err,
            "descriptor lease drain: cleanup propose failed after a failed drain"
        );
    }
    error
}

/// End `owner`'s drain on `id` on every node, and wait until the end applies
/// on this node. Other owners' drains on `id` stay.
///
/// For a DDL whose own catalog entry never commits, so no implicit clear
/// runs, and for every [`drain_for_owner_async`] holder. Ending a drain that
/// is not active is a no-op on every node.
pub async fn end_drain_async(
    shared: &SharedState,
    id: DescriptorId,
    owner: DrainOwner,
) -> Result<(), Error> {
    propose_drain_async(
        shared,
        MetadataEntry::DescriptorDrainEnd {
            descriptor_id: id,
            owner,
        },
        "drain_end",
    )
    .await
}

/// Propose a drain variant and await its apply on this node, until the
/// statement deadline.
async fn propose_drain_async(
    shared: &SharedState,
    entry: MetadataEntry,
    operation: &'static str,
) -> Result<(), Error> {
    let handle = shared.metadata_raft_handle()?;
    let log_index = handle
        .propose_async(encode_drain(&entry, operation)?)
        .await?;
    await_metadata_apply(shared, log_index).await.map(|_| ())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use nodedb_cluster::DescriptorKind;

    use super::*;
    use crate::control::server::shared::session::{conn_scope, deadline};

    /// A drain start whose apply wait runs out of time is already in the log
    /// and applies. The drain has no expiry, so it must not stay open after
    /// the statement failed.
    #[tokio::test]
    async fn a_drain_start_that_misses_its_deadline_does_not_stay_open() {
        let cluster = crate::control::cluster::test_one_node::boot().await;
        let state = Arc::clone(&cluster.state);
        let id = DescriptorId::new(0, 1, DescriptorKind::Collection, "drain_probe");

        let mut missed = 0;
        for _ in 0..20 {
            let attempt = conn_scope::scoped(async {
                let _scope = deadline::enter(Some(Duration::ZERO), 30);
                drain_for_ddl_async(&state, id.clone(), 1, Duration::from_secs(5), 0).await
            })
            .await;
            match attempt {
                Ok(()) => end_drain_async(&state, id.clone(), DrainOwner::Ddl)
                    .await
                    .expect("end the drain"),
                Err(Error::DeadlineExceeded { .. }) => missed += 1,
                Err(other) => panic!("unexpected drain error: {other}"),
            }
        }
        assert!(
            missed > 0,
            "no drain start missed its deadline, so the test checked nothing"
        );

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while state.lease_drain.is_draining(&id, 1) {
            assert!(
                tokio::time::Instant::now() < deadline,
                "a drain start that missed its deadline left the drain open"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        drop(state);
        cluster.shutdown().await;
    }
}
