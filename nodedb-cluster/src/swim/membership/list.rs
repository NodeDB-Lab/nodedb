//! In-memory membership table.
//!
//! `MembershipList` is the canonical view of cluster membership from the
//! local node's perspective. It is:
//!
//! * Thread-safe via a single `RwLock<HashMap<NodeId, Member>>`.
//! * Snapshot-able without holding the lock, so downstream consumers
//!   (routing, health, metrics) can iterate without blocking the detector.
//! * Free of any I/O — it only applies [`merge_update`] outcomes to the
//!   stored table and returns the outcome verbatim so the caller can drive
//!   dissemination.
//!
//! The lock is a plain `std::sync::RwLock` (no parking_lot dependency).
//! Read-heavy workloads are well-served because detector probes take only
//! the read guard, while writes are bounded by the number of rumours per
//! probe round (typically a handful).

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::RwLock;
use std::time::Instant;

use nodedb_types::NodeId;

use super::super::incarnation::Incarnation;
use super::super::member::record::MemberUpdate;
use super::super::member::{Member, MemberState};
use super::merge::{MergeOutcome, merge_update};

/// A point-in-time copy of the membership table. Cheap to clone and iterate.
#[derive(Debug, Clone)]
pub struct MembershipSnapshot {
    members: Vec<Member>,
}

impl MembershipSnapshot {
    /// Every member in the snapshot, in unspecified order.
    pub fn iter(&self) -> impl Iterator<Item = &Member> {
        self.members.iter()
    }

    /// Only members in [`MemberState::Alive`].
    pub fn alive(&self) -> impl Iterator<Item = &Member> {
        self.members.iter().filter(|m| m.is_reachable())
    }

    /// Total number of members, including non-reachable ones.
    pub fn len(&self) -> usize {
        self.members.len()
    }

    /// `true` if the snapshot contains zero members.
    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }
}

/// Canonical, mutable membership table shared across the SWIM detector
/// and any read-only consumers (routing, health monitor, `/cluster/debug`).
#[derive(Debug)]
pub struct MembershipList {
    local_node_id: NodeId,
    table: RwLock<HashMap<NodeId, Member>>,
}

impl MembershipList {
    /// Construct a list containing only the local node as `Alive` at the
    /// configured initial incarnation.
    pub fn new_local(local_node_id: NodeId, local_addr: SocketAddr, initial: Incarnation) -> Self {
        let mut table = HashMap::new();
        table.insert(
            local_node_id.clone(),
            Member {
                node_id: local_node_id.clone(),
                addr: local_addr,
                state: MemberState::Alive,
                incarnation: initial,
                last_state_change: Instant::now(),
            },
        );
        Self {
            local_node_id,
            table: RwLock::new(table),
        }
    }

    /// The local node's id.
    pub fn local_node_id(&self) -> &NodeId {
        &self.local_node_id
    }

    /// Number of members currently stored.
    pub fn len(&self) -> usize {
        self.table.read().expect("membership lock poisoned").len()
    }

    /// `true` if the list is empty. Practically never the case — the
    /// local node is always present — but provided for lint symmetry with
    /// [`MembershipList::len`].
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether the list contains only the local node.
    pub fn is_solo(&self) -> bool {
        self.len() <= 1
    }

    /// Take a snapshot of the full table. The returned structure is a
    /// cheap `Vec<Member>` clone — reference to the underlying lock is
    /// released before this function returns.
    pub fn snapshot(&self) -> MembershipSnapshot {
        let guard = self.table.read().expect("membership lock poisoned");
        MembershipSnapshot {
            members: guard.values().cloned().collect(),
        }
    }

    /// Apply a rumour to the table. Returns the merge outcome so the caller
    /// can drive the dissemination queue. On `SelfRefute`, the local
    /// record is updated in place to carry the bumped incarnation before
    /// returning, so the caller only needs to gossip the new record.
    pub fn apply(&self, update: &MemberUpdate) -> MergeOutcome {
        // Malformed address = dropped rumour. We never invent a SocketAddr
        // for a node we don't already know about.
        let parsed_addr = update.parse_addr();

        let mut guard = self.table.write().expect("membership lock poisoned");
        let stored = guard.get(&update.node_id);
        let outcome = merge_update(&self.local_node_id, stored, update);

        match &outcome {
            MergeOutcome::Insert => {
                let Some(addr) = parsed_addr else {
                    return MergeOutcome::Ignore;
                };
                guard.insert(
                    update.node_id.clone(),
                    Member {
                        node_id: update.node_id.clone(),
                        addr,
                        state: update.state,
                        incarnation: update.incarnation,
                        last_state_change: Instant::now(),
                    },
                );
            }
            MergeOutcome::Apply => {
                if let Some(cur) = guard.get_mut(&update.node_id) {
                    cur.state = update.state;
                    cur.incarnation = update.incarnation;
                    if let Some(addr) = parsed_addr {
                        cur.addr = addr;
                    }
                    cur.last_state_change = Instant::now();
                }
            }
            MergeOutcome::SelfRefute { new_incarnation } => {
                let addr = guard
                    .get(&self.local_node_id)
                    .map(|m| m.addr)
                    .or(parsed_addr)
                    .expect("local node must already be registered");
                guard.insert(
                    self.local_node_id.clone(),
                    Member {
                        node_id: self.local_node_id.clone(),
                        addr,
                        state: MemberState::Alive,
                        incarnation: *new_incarnation,
                        last_state_change: Instant::now(),
                    },
                );
            }
            MergeOutcome::Ignore | MergeOutcome::Refute | MergeOutcome::TerminalLeft => {}
        }

        outcome
    }

    /// Look up a single member by id and return a clone. Returns `None`
    /// if the id is unknown.
    pub fn get(&self, node_id: &NodeId) -> Option<Member> {
        self.table
            .read()
            .expect("membership lock poisoned")
            .get(node_id)
            .cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};
    use std::sync::Arc;
    use std::thread;

    fn addr(port: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port)
    }

    fn local() -> MembershipList {
        MembershipList::new_local(NodeId::new("local"), addr(7000), Incarnation::ZERO)
    }

    fn upd(id: &str, state: MemberState, inc: u64, port: u16) -> MemberUpdate {
        MemberUpdate {
            node_id: NodeId::new(id),
            addr: addr(port).to_string(),
            state,
            incarnation: Incarnation::new(inc),
        }
    }

    #[test]
    fn local_member_is_inserted_alive() {
        let list = local();
        assert_eq!(list.len(), 1);
        assert!(list.is_solo());
        let snap = list.snapshot();
        assert_eq!(snap.alive().count(), 1);
    }

    #[test]
    fn insert_new_member() {
        let list = local();
        let out = list.apply(&upd("n1", MemberState::Alive, 0, 7001));
        assert_eq!(out, MergeOutcome::Insert);
        assert_eq!(list.len(), 2);
        assert!(!list.is_solo());
    }

    #[test]
    fn apply_newer_incarnation() {
        let list = local();
        list.apply(&upd("n1", MemberState::Alive, 0, 7001));
        let out = list.apply(&upd("n1", MemberState::Suspect, 1, 7001));
        assert_eq!(out, MergeOutcome::Apply);
        let m = list.get(&NodeId::new("n1")).expect("stored");
        assert_eq!(m.state, MemberState::Suspect);
        assert_eq!(m.incarnation, Incarnation::new(1));
    }

    #[test]
    fn stale_update_leaves_state_untouched() {
        let list = local();
        list.apply(&upd("n1", MemberState::Alive, 5, 7001));
        let out = list.apply(&upd("n1", MemberState::Suspect, 3, 7001));
        assert_eq!(out, MergeOutcome::Refute);
        let m = list.get(&NodeId::new("n1")).expect("stored");
        assert_eq!(m.state, MemberState::Alive);
        assert_eq!(m.incarnation, Incarnation::new(5));
    }

    #[test]
    fn terminal_left_rejects_resurrection() {
        let list = local();
        list.apply(&upd("n1", MemberState::Alive, 0, 7001));
        list.apply(&upd("n1", MemberState::Left, 1, 7001));
        let out = list.apply(&upd("n1", MemberState::Alive, 99, 7001));
        assert_eq!(out, MergeOutcome::TerminalLeft);
        let m = list.get(&NodeId::new("n1")).expect("stored");
        assert_eq!(m.state, MemberState::Left);
    }

    #[test]
    fn self_refute_bumps_local_incarnation() {
        let list = local();
        let out = list.apply(&upd("local", MemberState::Suspect, 3, 7000));
        match out {
            MergeOutcome::SelfRefute { new_incarnation } => {
                assert_eq!(new_incarnation, Incarnation::new(4));
            }
            other => panic!("expected SelfRefute, got {other:?}"),
        }
        let me = list.get(&NodeId::new("local")).expect("stored");
        assert_eq!(me.state, MemberState::Alive);
        assert_eq!(me.incarnation, Incarnation::new(4));
    }

    #[test]
    fn snapshot_is_consistent_under_concurrent_writes() {
        let list = Arc::new(local());
        let writer = {
            let list = Arc::clone(&list);
            thread::spawn(move || {
                for i in 0..500u64 {
                    let id = format!("n{}", i % 20);
                    list.apply(&MemberUpdate {
                        node_id: NodeId::new(id),
                        addr: addr(7000 + (i as u16 % 20)).to_string(),
                        state: MemberState::Alive,
                        incarnation: Incarnation::new(i),
                    });
                }
            })
        };
        // Hammer snapshot() while the writer is running; every snapshot
        // must observe a self-consistent table (no partial inserts, no
        // panics from poisoned locks).
        for _ in 0..500 {
            let snap = list.snapshot();
            for m in snap.iter() {
                // Each cloned member is internally consistent.
                assert_eq!(m.is_reachable(), m.state == MemberState::Alive);
            }
        }
        writer.join().expect("writer thread");
        // After the writer finishes, the local node + up to 20 peers are
        // present.
        assert!(!list.is_empty() && list.len() <= 21);
    }

    #[test]
    fn get_returns_none_for_unknown() {
        let list = local();
        assert!(list.get(&NodeId::new("ghost")).is_none());
    }
}
