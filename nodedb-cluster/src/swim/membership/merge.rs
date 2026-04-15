//! Pure state-merge rule for SWIM rumours.
//!
//! `merge_update` compares a stored [`Member`] against an incoming
//! [`MemberUpdate`] and produces a [`MergeOutcome`] describing what the
//! caller should do. The function is deliberately free of any shared
//! mutable state — the caller is responsible for taking the lock, applying
//! the outcome, and forwarding any rumour to the dissemination queue.
//!
//! ## Merge rule
//!
//! Compare the two `(incarnation, state_precedence)` tuples lexicographically:
//!
//! * If the incoming tuple strictly dominates the stored one → **Apply**.
//! * If the tuples are equal → **Ignore** (no new information).
//! * If the stored tuple strictly dominates → **Refute**: the local view
//!   is newer, so the caller should gossip the stored record back.
//!
//! ## Self-refutation
//!
//! When the `local_node_id` matches the update's node_id **and** the update
//! reports a non-`Alive` state, the local node must refute by bumping its
//! own incarnation past the rumour and re-broadcasting `Alive`. This is
//! reported as [`MergeOutcome::SelfRefute`] — the caller applies the bumped
//! incarnation and re-disseminates.
//!
//! ## Terminal state
//!
//! Once a member enters [`MemberState::Left`], no further updates are
//! accepted regardless of incarnation — `Left` is an explicit graceful
//! departure and the node must rejoin through bootstrap to re-enter the
//! membership list.

use super::super::incarnation::Incarnation;
use super::super::member::record::{Member, MemberUpdate};
use super::super::member::state::MemberState;

use nodedb_types::NodeId;

/// What the caller should do after `merge_update` returns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeOutcome {
    /// No stored record existed; insert the update as a new member.
    Insert,
    /// Update strictly dominates the stored record; overwrite in place.
    Apply,
    /// Update is redundant or stale; drop it silently.
    Ignore,
    /// Update is stale *and* the stored record should be re-gossiped so
    /// the sender can learn the newer value. `merge_update` does not send
    /// anything itself.
    Refute,
    /// The update targets the local node with a non-`Alive` state. The
    /// caller must bump its own incarnation to `new_incarnation` and
    /// broadcast an `Alive` refutation.
    SelfRefute { new_incarnation: Incarnation },
    /// Stored state is [`MemberState::Left`]; update rejected.
    TerminalLeft,
}

/// Compute the merge outcome between `stored` (possibly `None` if the node
/// is previously unknown) and `update`.
///
/// Pure function: does not mutate `stored`. The caller applies the result.
pub fn merge_update(
    local_node_id: &NodeId,
    stored: Option<&Member>,
    update: &MemberUpdate,
) -> MergeOutcome {
    // Self-refutation: a non-Alive rumour about us is always wrong (we're
    // clearly still running). Bump past whatever the rumour claimed and
    // broadcast Alive at the new incarnation.
    if &update.node_id == local_node_id && update.state != MemberState::Alive {
        let local_inc = stored.map(|m| m.incarnation).unwrap_or(Incarnation::ZERO);
        return MergeOutcome::SelfRefute {
            new_incarnation: local_inc.refute(update.incarnation),
        };
    }

    let Some(cur) = stored else {
        return MergeOutcome::Insert;
    };

    if cur.state == MemberState::Left {
        return MergeOutcome::TerminalLeft;
    }

    let cur_key = cur.rumour_key();
    let upd_key = (update.incarnation, update.state.precedence());

    use std::cmp::Ordering::*;
    match upd_key.cmp(&cur_key) {
        Greater => MergeOutcome::Apply,
        Equal => MergeOutcome::Ignore,
        Less => MergeOutcome::Refute,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn addr() -> SocketAddr {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7000)
    }

    fn member(id: &str, state: MemberState, inc: u64) -> Member {
        Member {
            node_id: NodeId::new(id),
            addr: addr(),
            state,
            incarnation: Incarnation::new(inc),
            last_state_change: std::time::Instant::now(),
        }
    }

    fn update(id: &str, state: MemberState, inc: u64) -> MemberUpdate {
        MemberUpdate {
            node_id: NodeId::new(id),
            addr: addr().to_string(),
            state,
            incarnation: Incarnation::new(inc),
        }
    }

    fn me() -> NodeId {
        NodeId::new("local")
    }

    #[test]
    fn unknown_node_is_inserted() {
        let out = merge_update(&me(), None, &update("n1", MemberState::Alive, 0));
        assert_eq!(out, MergeOutcome::Insert);
    }

    #[test]
    fn newer_incarnation_applies() {
        let cur = member("n1", MemberState::Alive, 3);
        let upd = update("n1", MemberState::Alive, 4);
        assert_eq!(merge_update(&me(), Some(&cur), &upd), MergeOutcome::Apply);
    }

    #[test]
    fn older_incarnation_refutes() {
        let cur = member("n1", MemberState::Alive, 5);
        let upd = update("n1", MemberState::Suspect, 3);
        assert_eq!(merge_update(&me(), Some(&cur), &upd), MergeOutcome::Refute);
    }

    #[test]
    fn same_incarnation_higher_precedence_applies() {
        let cur = member("n1", MemberState::Alive, 4);
        let upd = update("n1", MemberState::Suspect, 4);
        assert_eq!(merge_update(&me(), Some(&cur), &upd), MergeOutcome::Apply);
    }

    #[test]
    fn same_incarnation_lower_precedence_refutes() {
        let cur = member("n1", MemberState::Suspect, 4);
        let upd = update("n1", MemberState::Alive, 4);
        assert_eq!(merge_update(&me(), Some(&cur), &upd), MergeOutcome::Refute);
    }

    #[test]
    fn equal_tuples_ignore() {
        let cur = member("n1", MemberState::Alive, 4);
        let upd = update("n1", MemberState::Alive, 4);
        assert_eq!(merge_update(&me(), Some(&cur), &upd), MergeOutcome::Ignore);
    }

    #[test]
    fn left_is_terminal() {
        let cur = member("n1", MemberState::Left, 2);
        let upd = update("n1", MemberState::Alive, 99);
        assert_eq!(
            merge_update(&me(), Some(&cur), &upd),
            MergeOutcome::TerminalLeft
        );
    }

    #[test]
    fn suspect_self_triggers_refutation() {
        let cur = member("local", MemberState::Alive, 7);
        let upd = update("local", MemberState::Suspect, 7);
        match merge_update(&me(), Some(&cur), &upd) {
            MergeOutcome::SelfRefute { new_incarnation } => {
                assert!(new_incarnation > Incarnation::new(7));
            }
            other => panic!("expected SelfRefute, got {other:?}"),
        }
    }

    #[test]
    fn self_refute_without_stored_record() {
        let upd = update("local", MemberState::Dead, 0);
        match merge_update(&me(), None, &upd) {
            MergeOutcome::SelfRefute { new_incarnation } => {
                assert_eq!(new_incarnation, Incarnation::new(1));
            }
            other => panic!("expected SelfRefute, got {other:?}"),
        }
    }

    #[test]
    fn alive_self_update_not_treated_as_refutation() {
        // An `Alive` echo of ourselves is just a confirmation, not a
        // refutation signal. Falls through to the normal path.
        let cur = member("local", MemberState::Alive, 2);
        let upd = update("local", MemberState::Alive, 2);
        assert_eq!(merge_update(&me(), Some(&cur), &upd), MergeOutcome::Ignore);
    }
}
