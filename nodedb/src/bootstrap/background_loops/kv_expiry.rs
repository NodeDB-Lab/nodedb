// SPDX-License-Identifier: BUSL-1.1

//! Background KV TTL reaper.
//!
//! Every expiry tick it turns the passed TTLs of the vShards this node
//! leads into replicated reaps (`control::kv_expiry`). A failed pass leaves
//! its keys due, and the next tick proposes them again.

use std::sync::Arc;
use std::time::Duration;

use tracing::info;

use crate::ServerConfig;
use crate::control::shutdown::{ShutdownPhase, spawn_loop};
use crate::control::state::SharedState;

/// Spawn the KV TTL reaper.
pub fn spawn_kv_expiry_reaper(shared: &Arc<SharedState>, config: &ServerConfig) {
    let interval = Duration::from_millis(config.tuning.kv.expiry_tick_ms.max(1));
    let limit = u32::try_from(config.tuning.kv.expiry_reap_budget).unwrap_or(u32::MAX);
    let state = Arc::clone(shared);
    spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "kv_expiry_reaper",
        ShutdownPhase::DrainingControlPlane,
        move |mut shutdown| async move {
            let mut tick = tokio::time::interval(interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = shutdown.wait_cancelled() => break,
                    _ = tick.tick() => {}
                }
                if shutdown.is_cancelled() {
                    break;
                }
                match crate::control::kv_expiry::run_reap_pass(&state, limit).await {
                    Ok(pass) => {
                        if let Some(error) = &pass.first_error {
                            tracing::warn!(
                                due = pass.due,
                                proposed = pass.proposed,
                                failed = pass.failed,
                                %error,
                                "kv expiry reap pass finished with errors; failed keys stay due"
                            );
                        } else if pass.proposed > 0 {
                            tracing::debug!(
                                due = pass.due,
                                proposed = pass.proposed,
                                "kv expiry reap pass"
                            );
                        }
                    }
                    Err(e) => tracing::warn!(error = %e, "kv expiry reap pass failed"),
                }
            }
        },
    );
    info!(
        interval_ms = interval.as_millis() as u64,
        limit, "kv expiry reaper running"
    );
}
