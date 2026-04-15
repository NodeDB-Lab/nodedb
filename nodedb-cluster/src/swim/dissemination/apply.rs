//! `apply_and_disseminate` — keep the dissemination queue consistent
//! with every [`MembershipList::apply`] outcome.
//!
//! The rules, per Lifeguard:
//!
//! - `Insert` / `Apply` → enqueue the incoming update (it is now our
//!   new truth, and the rest of the cluster should learn about it).
//! - `SelfRefute { new_incarnation }` → enqueue a refutation `Alive`
//!   update for the local node at the bumped incarnation. This is the
//!   critical path for recovering from a false suspicion.
//! - `Refute` → enqueue the **stored** view of the contested node so
//!   the sender (and anyone it gossips to) learns the newer truth.
//! - `Ignore` / `TerminalLeft` → no-op.

use crate::swim::member::MemberState;
use crate::swim::member::record::MemberUpdate;
use crate::swim::membership::{MembershipList, MergeOutcome};

use super::queue::DisseminationQueue;

/// Apply `update` to `list` and reflect the outcome in `queue`.
/// Returns the raw merge outcome so callers can still branch on it.
pub fn apply_and_disseminate(
    list: &MembershipList,
    queue: &DisseminationQueue,
    update: &MemberUpdate,
) -> MergeOutcome {
    let outcome = list.apply(update);
    match &outcome {
        MergeOutcome::Insert | MergeOutcome::Apply => {
            queue.enqueue(update.clone());
        }
        MergeOutcome::SelfRefute { new_incarnation } => {
            if let Some(local) = list.get(list.local_node_id()) {
                queue.enqueue(MemberUpdate {
                    node_id: local.node_id.clone(),
                    addr: local.addr.to_string(),
                    state: MemberState::Alive,
                    incarnation: *new_incarnation,
                });
            }
        }
        MergeOutcome::Refute => {
            if let Some(stored) = list.get(&update.node_id) {
                queue.enqueue(MemberUpdate::from(&stored));
            }
        }
        MergeOutcome::Ignore | MergeOutcome::TerminalLeft => {}
    }
    outcome
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::swim::incarnation::Incarnation;
    use nodedb_types::NodeId;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn addr(p: u16) -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), p)
    }

    fn local_list() -> MembershipList {
        MembershipList::new_local(NodeId::new("local"), addr(7000), Incarnation::ZERO)
    }

    fn upd(id: &str, state: MemberState, inc: u64) -> MemberUpdate {
        MemberUpdate {
            node_id: NodeId::new(id),
            addr: addr(7001).to_string(),
            state,
            incarnation: Incarnation::new(inc),
        }
    }

    #[test]
    fn insert_enqueues() {
        let list = local_list();
        let q = DisseminationQueue::new();
        let out = apply_and_disseminate(&list, &q, &upd("n1", MemberState::Alive, 0));
        assert_eq!(out, MergeOutcome::Insert);
        assert_eq!(q.len(), 1);
    }

    #[test]
    fn apply_enqueues() {
        let list = local_list();
        let q = DisseminationQueue::new();
        apply_and_disseminate(&list, &q, &upd("n1", MemberState::Alive, 0));
        q.clear();
        let out = apply_and_disseminate(&list, &q, &upd("n1", MemberState::Suspect, 1));
        assert_eq!(out, MergeOutcome::Apply);
        assert_eq!(q.len(), 1);
    }

    #[test]
    fn ignore_leaves_queue_empty() {
        let list = local_list();
        let q = DisseminationQueue::new();
        apply_and_disseminate(&list, &q, &upd("n1", MemberState::Alive, 3));
        q.clear();
        let out = apply_and_disseminate(&list, &q, &upd("n1", MemberState::Alive, 3));
        assert_eq!(out, MergeOutcome::Ignore);
        assert!(q.is_empty());
    }

    #[test]
    fn refute_enqueues_stored_view() {
        let list = local_list();
        let q = DisseminationQueue::new();
        apply_and_disseminate(&list, &q, &upd("n1", MemberState::Alive, 5));
        q.clear();
        let out = apply_and_disseminate(&list, &q, &upd("n1", MemberState::Suspect, 3));
        assert_eq!(out, MergeOutcome::Refute);
        // Queue should now hold the stored (newer) view of n1 at inc=5.
        let taken = q.take_for_message(4, 8);
        assert_eq!(taken.len(), 1);
        assert_eq!(taken[0].incarnation, Incarnation::new(5));
        assert_eq!(taken[0].state, MemberState::Alive);
    }

    #[test]
    fn self_refute_enqueues_bumped_local_update() {
        let list = local_list();
        let q = DisseminationQueue::new();
        let out = apply_and_disseminate(&list, &q, &upd("local", MemberState::Suspect, 3));
        match out {
            MergeOutcome::SelfRefute { new_incarnation } => {
                assert_eq!(new_incarnation, Incarnation::new(4));
            }
            other => panic!("expected SelfRefute, got {other:?}"),
        }
        let taken = q.take_for_message(4, 8);
        assert_eq!(taken.len(), 1);
        assert_eq!(taken[0].node_id.as_str(), "local");
        assert_eq!(taken[0].state, MemberState::Alive);
        assert_eq!(taken[0].incarnation, Incarnation::new(4));
    }

    #[test]
    fn terminal_left_is_noop_on_queue() {
        let list = local_list();
        let q = DisseminationQueue::new();
        apply_and_disseminate(&list, &q, &upd("n1", MemberState::Alive, 0));
        apply_and_disseminate(&list, &q, &upd("n1", MemberState::Left, 1));
        q.clear();
        let out = apply_and_disseminate(&list, &q, &upd("n1", MemberState::Alive, 99));
        assert_eq!(out, MergeOutcome::TerminalLeft);
        assert!(q.is_empty());
    }
}
