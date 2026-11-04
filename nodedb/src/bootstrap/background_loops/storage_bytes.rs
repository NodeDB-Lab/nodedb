// SPDX-License-Identifier: BUSL-1.1

//! The stored-bytes loop: each period, sample this node's share, then merge
//! every node's share into the cluster total.
//!
//! Each sample reads counters on every core, so it runs on this period and
//! never per write. A failed sample keeps the last published share, and the
//! round still runs on it until it ages out.

use std::sync::Arc;
use std::time::Duration;

use crate::control::metrics::storage::{StorageAggregator, sample_storage};
use crate::control::shutdown::{ShutdownPhase, spawn_loop};
use crate::control::state::SharedState;

/// The sample period `[tuning.maintenance] storage_sample_interval_ms` sets.
pub fn storage_sample_period(interval_ms: u64) -> Duration {
    Duration::from_millis(interval_ms.max(1))
}

/// Spawn the loop that samples stored bytes and aggregates the cluster total
/// every `period`.
pub fn spawn_storage_sampler(shared: &Arc<SharedState>, period: Duration) {
    let state = Arc::clone(shared);
    spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "storage_bytes_sampler",
        ShutdownPhase::DrainingControlPlane,
        move |mut shutdown| async move {
            let mut aggregator = StorageAggregator::new(period);
            let mut tick = tokio::time::interval(period);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = shutdown.wait_cancelled() => break,
                    _ = tick.tick() => {}
                }
                tokio::select! {
                    _ = shutdown.wait_cancelled() => break,
                    _ = storage_round(&state, &mut aggregator) => {}
                }
            }
        },
    );
}

/// One local sample, then one cluster round.
async fn storage_round(state: &SharedState, aggregator: &mut StorageAggregator) {
    if let Err(error) = sample_storage(state).await {
        crate::diag::storage_bytes_sample_failed(&error);
        tracing::warn!(error = %error, "storage bytes sample failed");
    }
    aggregator.run_round(state).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_period_is_the_configured_interval() {
        assert_eq!(storage_sample_period(30_000), Duration::from_secs(30));
        assert_eq!(storage_sample_period(0), Duration::from_millis(1));
    }
}
