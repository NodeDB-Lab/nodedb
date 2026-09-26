// SPDX-License-Identifier: BUSL-1.1

//! RESTORE's quorum check: every Raft group the restore reads or writes has a
//! reachable majority before the restore proposes anything.
//!
//! A restore reads every data group's write marks, writes catalog rows
//! through the metadata group, places a cut marker in the sequencer group,
//! and re-issues rows through the data groups. A group with no reachable
//! majority commits nothing, so every step against it waits out its
//! deadline. Checked first, the restore fails at once with the group and the
//! nodes it cannot reach, and nothing of it is applied anywhere.
//!
//! The check reads this node's membership view: the routing table's voters
//! and the topology's active nodes. A majority lost after the check fails the
//! step that needs it at its deadline instead.

use std::collections::BTreeSet;

use crate::Error;
use crate::control::state::SharedState;

/// Fail with [`Error::GroupQuorumUnavailable`] for the first Raft group whose
/// voters have no reachable majority. A node with no cluster routing has no
/// group to check.
pub(super) fn require_quorum(state: &SharedState) -> Result<(), Error> {
    let (Some(routing), Some(topology)) = (
        state.cluster_routing.as_ref(),
        state.cluster_topology.as_ref(),
    ) else {
        return Ok(());
    };
    let active: BTreeSet<u64> = topology
        .read()
        .unwrap_or_else(|p| p.into_inner())
        .active_nodes()
        .iter()
        .map(|node| node.node_id)
        .collect();
    let routing = routing.read().unwrap_or_else(|p| p.into_inner());
    let mut group_ids = routing.group_ids();
    group_ids.sort_unstable();
    for group_id in group_ids {
        let Some(info) = routing.group_info(group_id) else {
            continue;
        };
        if let Some(error) = quorum_error(group_id, &info.members, &active) {
            return Err(error);
        }
    }
    Ok(())
}

/// The error for `group_id` when fewer than a majority of `voters` are in
/// `active`, else `None`.
fn quorum_error(group_id: u64, voters: &[u64], active: &BTreeSet<u64>) -> Option<Error> {
    if voters.is_empty() {
        return None;
    }
    let mut unreachable: Vec<u64> = voters
        .iter()
        .copied()
        .filter(|voter| !active.contains(voter))
        .collect();
    unreachable.sort_unstable();
    let reachable = voters.len() - unreachable.len();
    if reachable * 2 > voters.len() {
        return None;
    }
    let mut voters = voters.to_vec();
    voters.sort_unstable();
    Some(Error::GroupQuorumUnavailable {
        group_id,
        voters,
        unreachable,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_group_with_a_reachable_majority_passes() {
        let active = BTreeSet::from([1, 2]);
        assert!(quorum_error(4, &[1, 2, 3], &active).is_none());
    }

    #[test]
    fn a_group_without_a_reachable_majority_names_its_unreachable_voters() {
        let active = BTreeSet::from([1]);
        match quorum_error(4, &[3, 1, 2], &active) {
            Some(Error::GroupQuorumUnavailable {
                group_id,
                voters,
                unreachable,
            }) => {
                assert_eq!(group_id, 4);
                assert_eq!(voters, vec![1, 2, 3]);
                assert_eq!(unreachable, vec![2, 3]);
            }
            other => panic!("expected GroupQuorumUnavailable, got {other:?}"),
        }
    }

    #[test]
    fn half_of_an_even_voter_set_is_not_a_majority() {
        let active = BTreeSet::from([1, 2]);
        assert!(quorum_error(4, &[1, 2, 3, 4], &active).is_some());
    }
}
