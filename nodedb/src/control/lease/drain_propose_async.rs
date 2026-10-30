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

use super::DrainHolds;
use super::drain_propose::{POLL_INTERVAL, drain_start_entry, drained_or_timed_out, encode_drain};

/// Drain every lease on `id` at `version <= up_to_version` for a `Put*` DDL.
///
/// The drain is owned by [`DrainOwner::Ddl`], so the apply of the DDL's own
/// catalog entry ends it and no other owner's drain.
///
/// Returns `Ok(())` once they have drained, and the caller owns the drain
/// from then on. Errors on timeout or propose failure, with the drain ended.
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
    // `is_draining` has no expiry backstop. The hold owns the drain from
    // before its start is proposed, so a start whose outcome is unknown, a
    // timed-out wait, and a cancelled future all end it. Only a drained
    // return hands the drain to the caller.
    let mut hold = DrainHolds::new(shared);
    hold.hold(id.clone(), owner.clone());
    if let Err(error) = propose_drain_async(
        shared,
        drain_start_entry(shared, &id, &owner, up_to_version, max_wait),
        "drain_start",
    )
    .await
    {
        hold.end().await;
        return Err(error);
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
    match drained {
        Ok(()) => {
            hold.keep();
            Ok(())
        }
        Err(error) => {
            hold.end().await;
            Err(error)
        }
    }
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
