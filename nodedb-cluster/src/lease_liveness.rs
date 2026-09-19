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

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use nodedb_types::NodeId;

use crate::routing_liveness::NodeIdResolver;
use crate::swim::member::MemberState;
use crate::swim::subscriber::MembershipSubscriber;

/// Node ids whose SWIM verdict is `Dead` or `Left`.
///
/// The value is the incarnation the verdict landed at, when the detector
/// supplied one: a lease stamped at incarnation `<=` that value was granted
/// before the verdict and may be released; a lease re-acquired later (a
/// restart bumped the incarnation) is not fenced by it. `None` means gone
/// without a fencing value — release unconditionally, the phase-1 behaviour.
///
/// `Left` is terminal and never revives; `Dead` is cleared by an `Alive`
/// transition for the same node (a refutation at a higher incarnation).
#[derive(Debug, Default)]
pub struct LeaseHolderLiveness {
    gone: RwLock<BTreeMap<u64, Option<u64>>>,
}

impl LeaseHolderLiveness {
    pub fn new() -> Self {
        Self::default()
    }

    /// Mark `node_id` as gone without a fencing value: its leases may be
    /// released without waiting for expiry or topology removal.
    pub fn mark_gone(&self, node_id: u64) {
        self.gone
            .write()
            .unwrap_or_else(|poison| poison.into_inner())
            .insert(node_id, None);
    }

    /// Mark `node_id` as gone at `incarnation`: leases stamped at an
    /// incarnation `<=` this value are fenced; later re-acquisitions are not.
    pub fn mark_gone_at(&self, node_id: u64, incarnation: u64) {
        self.gone
            .write()
            .unwrap_or_else(|poison| poison.into_inner())
            .insert(node_id, Some(incarnation));
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
            .contains_key(&node_id)
    }

    /// The incarnation the gone verdict landed at, when one was supplied.
    ///
    /// Callers check [`is_gone`](Self::is_gone) first; `None` then means the
    /// mark carries no fence and the release is unconditional.
    pub fn fence_at(&self, node_id: u64) -> Option<u64> {
        self.gone
            .read()
            .unwrap_or_else(|poison| poison.into_inner())
            .get(&node_id)
            .copied()
            .flatten()
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

/// Build the same SWIM-id → numeric-id resolver the routing hook uses:
/// parse the decimal node id and require it to exist in the live topology.
/// Placeholder entries (`seed:…`) resolve to `None` and are ignored.
pub fn topology_resolver(
    topology: Arc<RwLock<crate::topology::ClusterTopology>>,
) -> NodeIdResolver {
    Arc::new(move |node_id| {
        let topo = topology.read().unwrap_or_else(|poison| poison.into_inner());
        node_id
            .as_str()
            .parse::<u64>()
            .ok()
            .filter(|&id| topo.get_node(id).is_some())
    })
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

    fn on_state_change_with_incarnation(
        &self,
        node_id: &NodeId,
        new: MemberState,
        incarnation: u64,
    ) {
        let Some(numeric_id) = (self.resolver)(node_id) else {
            return;
        };
        match new {
            MemberState::Dead | MemberState::Left => {
                self.liveness.mark_gone_at(numeric_id, incarnation)
            }
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
    fn a_dead_verdict_carries_the_fencing_incarnation() {
        let liveness = Arc::new(LeaseHolderLiveness::new());
        let hook = LeaseHolderLivenessHook::new(liveness.clone(), Arc::new(|_| Some(4)));

        // The detector calls both hooks per transition; the fencing value
        // from the second must win over the fenceless mark from the first.
        hook.on_state_change(&nid("n4"), Some(MemberState::Alive), MemberState::Dead);
        hook.on_state_change_with_incarnation(&nid("n4"), MemberState::Dead, 12);
        assert_eq!(liveness.fence_at(4), Some(12));

        hook.on_state_change(&nid("n4"), Some(MemberState::Dead), MemberState::Alive);
        hook.on_state_change_with_incarnation(&nid("n4"), MemberState::Alive, 13);
        assert!(!liveness.is_gone(4));
        assert_eq!(liveness.fence_at(4), None);
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
