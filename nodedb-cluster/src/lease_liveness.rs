// SPDX-License-Identifier: BUSL-1.1

//! Lease-holder liveness fed by SWIM verdicts.
//!
//! The lease GC releases a holder's descriptor leases once the holder is gone.
//! Topology absence is the stable signal, but it lags a crash: the node stays
//! in the topology until the removal path runs, and until then its leases block
//! DDL drains for the whole lease duration. This module carries the faster
//! signal — SWIM's verdict.
//!
//! The set is a lease-specific input only: routing, placement, and rebalancing
//! never read it. `Dead` and `Left` mark a holder; an `Alive` transition
//! revives it, because a verdict is refutable by a higher incarnation and a
//! refuted node is serving again. `Suspect` is deliberately a no-op — it is
//! transient and a suspect holder may still hold its lease.
//!
//! Subscribers run on the detector task and must not block; marking an id in a
//! set is exactly that cost. The proposal side stays in the raft loop, where
//! the GC already proposes releases.

use std::collections::BTreeSet;
use std::sync::{Arc, RwLock};

use nodedb_types::NodeId;

use crate::routing_liveness::NodeIdResolver;
use crate::swim::member::MemberState;
use crate::swim::subscriber::MembershipSubscriber;

/// Node ids whose SWIM verdict is `Dead` or `Left`.
///
/// `Left` is terminal and never revives; `Dead` is cleared by an `Alive`
/// transition for the same node (a refutation at a higher incarnation).
#[derive(Debug, Default)]
pub struct LeaseHolderLiveness {
    gone: RwLock<BTreeSet<u64>>,
}

impl LeaseHolderLiveness {
    pub fn new() -> Self {
        Self::default()
    }

    /// Mark `node_id` as gone: its leases may be released without waiting for
    /// expiry or topology removal.
    pub fn mark_gone(&self, node_id: u64) {
        self.gone
            .write()
            .unwrap_or_else(|poison| poison.into_inner())
            .insert(node_id);
    }

    /// Clear the mark for `node_id`: a refuted verdict means the holder is
    /// serving again and its leased descriptors are live.
    pub fn revive(&self, node_id: u64) {
        self.gone
            .write()
            .unwrap_or_else(|poison| poison.into_inner())
            .remove(&node_id);
    }

    /// Whether `node_id` is currently marked gone.
    pub fn is_gone(&self, node_id: u64) -> bool {
        self.gone
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .contains(&node_id)
    }
}

/// [`MembershipSubscriber`] that feeds [`LeaseHolderLiveness`].
pub struct LeaseHolderLivenessHook {
    liveness: Arc<LeaseHolderLiveness>,
    resolver: NodeIdResolver,
}

impl LeaseHolderLivenessHook {
    pub fn new(liveness: Arc<LeaseHolderLiveness>, resolver: NodeIdResolver) -> Self {
        Self { liveness, resolver }
    }
}

impl MembershipSubscriber for LeaseHolderLivenessHook {
    fn on_state_change(&self, node_id: &NodeId, _old: Option<MemberState>, new: MemberState) {
        let Some(numeric_id) = (self.resolver)(node_id) else {
            // SWIM knows a node the numeric registry does not — a seed
            // placeholder or a learner mid-join. Nothing to mark.
            return;
        };
        match new {
            MemberState::Dead | MemberState::Left => self.liveness.mark_gone(numeric_id),
            MemberState::Alive => self.liveness.revive(numeric_id),
            MemberState::Suspect => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nid(id: &str) -> NodeId {
        NodeId::try_new(id.to_owned()).expect("valid node id fixture")
    }

    #[test]
    fn dead_and_left_mark_then_alive_revives() {
        let liveness = Arc::new(LeaseHolderLiveness::new());
        let hook = LeaseHolderLivenessHook::new(liveness.clone(), Arc::new(|_| Some(7)));

        assert!(!liveness.is_gone(7));
        hook.on_state_change(&nid("n7"), Some(MemberState::Alive), MemberState::Dead);
        assert!(liveness.is_gone(7));
        hook.on_state_change(&nid("n7"), Some(MemberState::Dead), MemberState::Alive);
        assert!(!liveness.is_gone(7));
        hook.on_state_change(&nid("n7"), Some(MemberState::Alive), MemberState::Left);
        assert!(liveness.is_gone(7));
    }

    #[test]
    fn suspect_is_not_a_release_signal() {
        let liveness = Arc::new(LeaseHolderLiveness::new());
        let hook = LeaseHolderLivenessHook::new(liveness.clone(), Arc::new(|_| Some(3)));

        hook.on_state_change(&nid("n3"), Some(MemberState::Alive), MemberState::Suspect);
        assert!(!liveness.is_gone(3));
    }

    #[test]
    fn an_unresolvable_node_is_ignored() {
        let liveness = Arc::new(LeaseHolderLiveness::new());
        let hook = LeaseHolderLivenessHook::new(liveness.clone(), Arc::new(|_| None));

        hook.on_state_change(&nid("seed"), None, MemberState::Dead);
        assert!(!liveness.is_gone(0));
        assert!(!liveness.is_gone(u64::MAX));
    }
}
