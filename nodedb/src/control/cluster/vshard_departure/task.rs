// SPDX-License-Identifier: BUSL-1.1

//! The background loop that runs the departure pass.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use nodedb_cluster::multi_raft::MultiRaft;

use crate::control::shutdown::{ShutdownPhase, spawn_loop};
use crate::control::state::SharedState;

use super::pass::DeparturePass;

/// How often the departure pass runs. The pass reads the routing table and
/// the residency rows, and purges only when a vShard left.
const PASS_PERIOD: Duration = Duration::from_secs(1);

/// Spawn the departure pass of node `node_id`, whose data groups
/// `multi_raft` mounts. It runs until shutdown.
pub fn spawn_departure_purge(
    shared: &Arc<SharedState>,
    node_id: u64,
    multi_raft: Arc<Mutex<MultiRaft>>,
) {
    let state = Arc::clone(shared);
    spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "vshard_departure_purge",
        ShutdownPhase::DrainingControlPlane,
        move |mut shutdown| async move {
            let mut pass = DeparturePass::default();
            let mut tick = tokio::time::interval(PASS_PERIOD);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    biased;
                    _ = shutdown.wait_cancelled() => break,
                    _ = tick.tick() => pass.run(&state, node_id, &multi_raft).await,
                }
            }
        },
    );
}
