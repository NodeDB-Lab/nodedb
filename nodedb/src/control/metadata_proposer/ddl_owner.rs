// SPDX-License-Identifier: BUSL-1.1

//! The owner of the DDL preparation lease, and when the metadata leader can
//! take the lease back.
//!
//! Every replica applies the same `DdlPrepareAcquire` and `DdlPrepareRelease`
//! entries, so every replica agrees on the owner at each log index. A
//! `DdlPrepared`, `DdlPendingPropose` or `DdlPendingFinalize` applies only
//! while its token owns the lease. An entry the old owner proposes after a
//! reclaim applies after the reclaim's release in log order, so it is a no-op
//! on every replica. The token is the fence. The grace below decides when a
//! reclaim stops a live owner's DDL, never whether a stale entry applies.

use std::time::{Duration, Instant};

use crate::control::state::SharedState;

/// How long an owner holds the lease before the metadata leader reclaims it
/// from a live owner. The fallback for an owner that is alive but stuck.
pub(super) const DDL_PREPARE_LEASE: Duration = Duration::from_secs(60);

/// The current owner of the DDL preparation lease on this replica.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DdlPrepareOwner {
    /// The fencing token the owner's prepared entries carry.
    pub token: u64,
    /// The node that proposed the acquire.
    pub node_id: u64,
    /// When this replica applied the acquire, or seeded it at boot. Local and
    /// monotonic: it paces the stuck-owner fallback only.
    pub acquired_at: Instant,
}

/// Why the metadata leader takes the lease from its owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReclaimCause {
    /// The owner held the lease past [`DDL_PREPARE_LEASE`].
    Expired,
    /// The owner's node left the topology.
    OwnerLeft,
    /// The owner's node is SWIM-Dead past `DEAD_HOLDER_LEASE_GRACE`, and the
    /// leader heard no Raft response from it for `DEAD_HOLDER_RAFT_SILENCE`.
    /// By then the owner self-fenced, or it still hears the leader.
    OwnerDead,
}

/// Why this node, as metadata leader, can reclaim the lease `owner` holds,
/// or `None`. Only the leader decides: its Raft contact samples are the ones
/// that show a dead owner silent.
pub(crate) fn reclaim_cause(shared: &SharedState, owner: &DdlPrepareOwner) -> Option<ReclaimCause> {
    if !shared.is_metadata_leader() {
        return None;
    }
    if owner.acquired_at.elapsed() >= DDL_PREPARE_LEASE {
        return Some(ReclaimCause::Expired);
    }
    if owner.node_id == shared.node_id {
        return None;
    }
    if let Some(topology) = shared.cluster_topology.as_ref()
        && !topology
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .contains(owner.node_id)
    {
        return Some(ReclaimCause::OwnerLeft);
    }
    let leader_term = shared
        .lease_runtime
        .metadata_leader_term
        .get()
        .and_then(|term| term());
    shared
        .lease_runtime
        .holder_liveness
        .dead_holder_released(owner.node_id, leader_term, Instant::now())
        .then_some(ReclaimCause::OwnerDead)
}

/// Whether `token` owns the preparation lease on this replica.
pub(crate) fn owns_ddl_lease(shared: &SharedState, token: u64) -> bool {
    shared
        .metadata_ddl
        .owner
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .is_some_and(|owner| owner.token == token)
}

/// The current owner on this replica, if the lease is held.
pub(crate) fn current_owner(shared: &SharedState) -> Option<DdlPrepareOwner> {
    *shared
        .metadata_ddl
        .owner
        .lock()
        .unwrap_or_else(|p| p.into_inner())
}
