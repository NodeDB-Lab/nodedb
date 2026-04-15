//! Probe target scheduler — implements Lifeguard §4.3's "random
//! permutation" rule: every alive peer must be probed exactly once per
//! epoch, and epochs restart with a freshly shuffled order.
//!
//! The scheduler is purely in-memory, `Send`, and holds no locks — the
//! detector serializes access to it from a single task.

use std::net::SocketAddr;

use nodedb_types::NodeId;
use rand::SeedableRng;
use rand::rngs::SmallRng;
use rand::seq::SliceRandom;

use crate::swim::membership::MembershipList;

/// Round-robin probe target chooser.
#[derive(Debug)]
pub struct ProbeScheduler {
    rng: SmallRng,
    queue: Vec<(NodeId, SocketAddr)>,
}

impl ProbeScheduler {
    /// Construct a scheduler with a non-deterministic seed. Production.
    pub fn new() -> Self {
        Self {
            rng: SmallRng::from_os_rng(),
            queue: Vec::new(),
        }
    }

    /// Construct with a fixed seed for deterministic unit tests.
    pub fn with_seed(seed: u64) -> Self {
        Self {
            rng: SmallRng::seed_from_u64(seed),
            queue: Vec::new(),
        }
    }

    /// Return the next target to probe, or `None` if the cluster has no
    /// alive peers other than ourselves. Excludes the local node and
    /// every non-[`MemberState::Alive`] member.
    pub fn next_target(&mut self, membership: &MembershipList) -> Option<(NodeId, SocketAddr)> {
        if self.queue.is_empty() {
            self.reshuffle(membership);
        }
        self.queue.pop()
    }

    /// Rebuild the queue from the current alive set, then shuffle.
    fn reshuffle(&mut self, membership: &MembershipList) {
        let snap = membership.snapshot();
        let local = membership.local_node_id();
        self.queue = snap
            .alive()
            .filter(|m| m.node_id != *local)
            .map(|m| (m.node_id.clone(), m.addr))
            .collect();
        self.queue.shuffle(&mut self.rng);
    }

    /// Pick `k` indirect probe helpers for `target`, excluding the local
    /// node and `target` itself. Returned order is randomized. Always
    /// returns at most `k` entries; fewer if the alive set is small.
    pub fn pick_helpers(
        &mut self,
        membership: &MembershipList,
        target: &NodeId,
        k: usize,
    ) -> Vec<(NodeId, SocketAddr)> {
        let snap = membership.snapshot();
        let local = membership.local_node_id();
        let mut pool: Vec<(NodeId, SocketAddr)> = snap
            .alive()
            .filter(|m| m.node_id != *local && m.node_id != *target)
            .map(|m| (m.node_id.clone(), m.addr))
            .collect();
        pool.shuffle(&mut self.rng);
        pool.truncate(k);
        pool
    }
}

impl Default for ProbeScheduler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::swim::incarnation::Incarnation;
    use crate::swim::member::MemberState;
    use crate::swim::member::record::MemberUpdate;
    use std::collections::HashSet;
    use std::net::{IpAddr, Ipv4Addr};

    fn addr(p: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), p)
    }

    fn membership_with(peers: &[(&str, MemberState)]) -> MembershipList {
        let list = MembershipList::new_local(NodeId::new("local"), addr(7000), Incarnation::ZERO);
        for (i, (id, state)) in peers.iter().enumerate() {
            list.apply(&MemberUpdate {
                node_id: NodeId::new(*id),
                addr: addr(7001 + i as u16).to_string(),
                state: *state,
                incarnation: Incarnation::new(1),
            });
        }
        list
    }

    #[test]
    fn empty_cluster_returns_none() {
        let mut sched = ProbeScheduler::with_seed(1);
        let list = membership_with(&[]);
        assert!(sched.next_target(&list).is_none());
    }

    #[test]
    fn skips_local_node() {
        let mut sched = ProbeScheduler::with_seed(1);
        let list = membership_with(&[("n1", MemberState::Alive)]);
        let (id, _) = sched.next_target(&list).expect("target");
        assert_eq!(id, NodeId::new("n1"));
    }

    #[test]
    fn skips_non_alive_members() {
        let mut sched = ProbeScheduler::with_seed(1);
        let list = membership_with(&[
            ("n1", MemberState::Suspect),
            ("n2", MemberState::Dead),
            ("n3", MemberState::Alive),
        ]);
        let (id, _) = sched.next_target(&list).expect("target");
        assert_eq!(id, NodeId::new("n3"));
        // Next call reshuffles; still only n3 is eligible.
        let (id, _) = sched.next_target(&list).expect("target");
        assert_eq!(id, NodeId::new("n3"));
    }

    #[test]
    fn exhausts_permutation_then_reshuffles() {
        let mut sched = ProbeScheduler::with_seed(42);
        let list = membership_with(&[
            ("n1", MemberState::Alive),
            ("n2", MemberState::Alive),
            ("n3", MemberState::Alive),
        ]);
        let mut first_epoch = HashSet::new();
        for _ in 0..3 {
            first_epoch.insert(sched.next_target(&list).unwrap().0);
        }
        assert_eq!(first_epoch.len(), 3, "epoch must touch every alive peer");
        // Fourth call triggers reshuffle and produces some peer again.
        assert!(sched.next_target(&list).is_some());
    }

    #[test]
    fn pick_helpers_excludes_target_and_local() {
        let mut sched = ProbeScheduler::with_seed(7);
        let list = membership_with(&[
            ("n1", MemberState::Alive),
            ("n2", MemberState::Alive),
            ("n3", MemberState::Alive),
            ("n4", MemberState::Alive),
        ]);
        let helpers = sched.pick_helpers(&list, &NodeId::new("n2"), 3);
        assert!(helpers.len() <= 3);
        for (id, _) in &helpers {
            assert_ne!(id.as_str(), "n2");
            assert_ne!(id.as_str(), "local");
        }
    }

    #[test]
    fn pick_helpers_caps_at_pool_size() {
        let mut sched = ProbeScheduler::with_seed(7);
        let list = membership_with(&[("n1", MemberState::Alive), ("n2", MemberState::Alive)]);
        let helpers = sched.pick_helpers(&list, &NodeId::new("n2"), 5);
        assert_eq!(helpers.len(), 1); // only n1 is a valid helper
    }

    #[test]
    fn seeded_scheduler_is_deterministic() {
        let list = membership_with(&[
            ("n1", MemberState::Alive),
            ("n2", MemberState::Alive),
            ("n3", MemberState::Alive),
        ]);
        let mut a = ProbeScheduler::with_seed(99);
        let mut b = ProbeScheduler::with_seed(99);
        for _ in 0..5 {
            assert_eq!(a.next_target(&list), b.next_target(&list));
        }
    }
}
