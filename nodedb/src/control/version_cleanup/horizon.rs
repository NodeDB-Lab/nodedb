// SPDX-License-Identifier: BUSL-1.1

//! The cleanup horizon: the system-time ordinal below which no read and no
//! commit will reach, on this node or any replica it shares a group with.
//!
//! Two bounds make it, and the horizon is the lower:
//!
//! - The snapshot board (`SnapshotBoard::horizon`): the oldest transaction
//!   snapshot any live node holds, each node raised to `now - max_hold`.
//! - A closed cut `C` less `max_hold`. The pass closes `C` here and on the
//!   leader of every group this node hosts (`read_cut::close`), so every
//!   entry this node applies afterwards carries an ordinal above `C`.
//!
//! `max_hold` (`NetworkTuning::snapshot_max_hold`) is the snapshot age limit
//! plus the longest statement deadline plus the clock skew bound. Every
//! node runs one value.
//!
//! The horizon never passes a cut a read still uses:
//!
//! - A block statement runs only while its snapshot is at most the age limit
//!   old, and for at most the deadline. A purge in that time stays at least
//!   `max_hold` below the clock, so below the snapshot.
//! - A historical statement reads a non-bitemporal collection only at a cut
//!   within the age limit (`super::history_window`), so the same bound
//!   holds. A bitemporal collection purges at its audit window instead.
//! - With no snapshot open, the board reports the wall clock, and the
//!   horizon stays `max_hold` below a closed cut. No later read takes a cut
//!   below it: a new snapshot or statement cap starts at the clock.
//!
//! It also never passes the snapshot of a commit any replica still applies.
//! Each such commit carries an ordinal `R` above `C`, and every replica
//! refuses one whose snapshot is more than `max_hold` below `R` (the commit
//! check of the Data Plane). So every commit that applies has a snapshot
//! above `C - max_hold`, at or above the horizon, and a replica's purge below
//! the horizon changes none of its verdicts. This holds across a follower
//! (each replica purges from its own pass and checks the same window), a
//! leader change (the closed cut binds every later leader through the
//! group's ordinal floor), and a restart (the window is a pure function of
//! the commit's two ordinals).

use crate::control::cluster::linearizable_read::groups_hosted_here;
use crate::control::read_cut::close::close_group_cuts;
use crate::control::read_cut::{snapshot_max_hold, statement_cap};
use crate::control::server::shared::session::StatementDeadline;
use crate::control::state::SharedState;

/// One computed cleanup horizon and the bounds that made it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CleanupHorizon {
    /// The ordinal every core purges below.
    pub horizon: i64,
    /// The snapshot board's horizon.
    pub board: i64,
    /// The cut closed here and on every hosted group's leader.
    pub closed: i64,
}

/// Compute the cleanup horizon of this node. Closes a cut at the clock on
/// this node and on the leader of every group it hosts, within the default
/// statement deadline.
pub async fn cleanup_horizon(state: &SharedState) -> crate::Result<CleanupHorizon> {
    let max_hold = snapshot_max_hold(state);
    let now_ns = nodedb_types::wall_now_ns_i64();
    let board =
        state
            .read_cut
            .board()
            .horizon(state.node_id, cluster_members(state), now_ns, max_hold);
    let cap = statement_cap(state, nodedb_types::ordinal_to_ms(now_ns)).await?;
    let deadline = StatementDeadline::of_statement(state);
    close_group_cuts(state, &groups_hosted_here(state), cap.cap, deadline).await?;
    let window = i64::try_from(max_hold.as_nanos()).unwrap_or(i64::MAX);
    Ok(horizon_of(board, cap.cap, window))
}

/// The horizon of a board horizon `board` and a closed cut `closed`, with
/// the commit window `window_ns`.
pub fn horizon_of(board: i64, closed: i64, window_ns: i64) -> CleanupHorizon {
    CleanupHorizon {
        horizon: board.min(closed.saturating_sub(window_ns)),
        board,
        closed,
    }
}

/// Every node of the cluster topology, this one included. Empty on a node
/// with no topology.
fn cluster_members(state: &SharedState) -> Vec<u64> {
    state
        .cluster_topology
        .as_ref()
        .map_or_else(Vec::new, |topology| {
            topology
                .read()
                .unwrap_or_else(|p| p.into_inner())
                .all_nodes()
                .map(|node| node.node_id)
                .collect()
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_horizon_is_the_lower_bound() {
        let open = horizon_of(500, 10_000, 1_000);
        assert_eq!(open.horizon, 500, "an open snapshot holds the horizon");
        let idle = horizon_of(10_000, 10_000, 1_000);
        assert_eq!(idle.horizon, 9_000, "the commit window holds it otherwise");
        assert_eq!(horizon_of(5, i64::MIN + 3, 10).horizon, i64::MIN);
    }

    /// With no snapshot open, the horizon stays a full window below the
    /// clock: a statement cap taken now is above it.
    #[tokio::test]
    async fn an_idle_node_keeps_the_horizon_a_window_below_the_clock() {
        let dir = tempfile::tempdir().expect("tempdir");
        let wal = std::sync::Arc::new(
            crate::wal::WalManager::open_for_testing(&dir.path().join("test.wal")).expect("wal"),
        );
        let (dispatcher, _data_sides) = crate::bridge::dispatch::Dispatcher::new(1, 64);
        let state = SharedState::new(dispatcher, wal).expect("state");
        let before = nodedb_types::wall_now_ns_i64();
        let computed = cleanup_horizon(&state).await.expect("horizon");
        let window = i64::try_from(snapshot_max_hold(&state).as_nanos()).expect("window");
        assert!(computed.closed >= before);
        assert_eq!(computed.horizon, computed.closed - window);

        let guard = state.read_cut.snapshots.register(before - window - 5);
        let held = cleanup_horizon(&state).await.expect("horizon");
        assert_eq!(
            held.horizon,
            before - window - 5,
            "an open snapshot holds it"
        );
        drop(guard);
    }
}
