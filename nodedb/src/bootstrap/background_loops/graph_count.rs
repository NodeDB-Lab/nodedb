// SPDX-License-Identifier: BUSL-1.1

//! Background graph count sampler.
//!
//! Every period it sends each local core the vShards this node hosts, and
//! each core moves the graph node and edge gauges by its share's change
//! (`control::metrics::graph_refresh`). A failed pass leaves the gauges at
//! the last count, and the next tick counts again.

use std::sync::Arc;

use tracing::info;

use crate::control::metrics::graph_refresh::{GRAPH_COUNT_PERIOD, refresh_graph_count};
use crate::control::shutdown::{ShutdownPhase, spawn_loop};
use crate::control::state::SharedState;

/// Spawn the graph count sampler.
pub fn spawn_graph_count_sampler(shared: &Arc<SharedState>) {
    let state = Arc::clone(shared);
    spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "graph_count_sampler",
        ShutdownPhase::DrainingControlPlane,
        move |mut shutdown| async move {
            let mut tick = tokio::time::interval(GRAPH_COUNT_PERIOD);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = shutdown.wait_cancelled() => break,
                    _ = tick.tick() => {}
                }
                if shutdown.is_cancelled() {
                    break;
                }
                if let Err(e) = refresh_graph_count(&state).await {
                    tracing::warn!(error = %e, "graph count pass failed");
                }
            }
        },
    );
    info!(
        interval_ms = GRAPH_COUNT_PERIOD.as_millis() as u64,
        "graph count sampler running"
    );
}
