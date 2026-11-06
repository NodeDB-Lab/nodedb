// SPDX-License-Identifier: BUSL-1.1

//! Periodic catch-up of `Periodic` continuous aggregates.
//!
//! A `Periodic(ms)` aggregate takes one refresh cut per period slot
//! (`now_ms / ms`). The cut is the same replicated write a REFRESH proposes,
//! so every replica folds the same rows at the same log position. The
//! metadata leader proposes the cuts. It need not replicate the root's data
//! group: `propose_refresh_cut` then sends the cut to that group's leader.
//! A replica applies a slot only when it is above the last slot it applied,
//! so a slot proposed twice, by a leader change or a retry, cuts once.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use nodedb_physical::physical_plan::RefreshCut;

use crate::control::state::SharedState;
use crate::engine::timeseries::continuous_agg::{ContinuousAggregateDef, RefreshPolicy};
use crate::types::{DatabaseId, TenantId};

use super::refresh::{propose_refresh_cut, resolve_root};

/// How often the loop looks for a slot that turned over.
const PERIODIC_SCAN_INTERVAL: Duration = Duration::from_secs(1);

/// `(database_id, tenant_id, name)` → the last slot this node proposed.
type ProposedSlots = HashMap<(u64, u64, String), u64>;

/// Spawn the loop that proposes the periodic refresh cuts.
pub fn spawn_periodic_refresh_loop(shared: Arc<SharedState>) {
    let loop_shared = Arc::clone(&shared);
    crate::control::shutdown::spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "continuous_aggregate_periodic_refresh",
        crate::control::shutdown::ShutdownPhase::DrainingControlPlane,
        move |mut shutdown| async move {
            let mut tick = tokio::time::interval(PERIODIC_SCAN_INTERVAL);
            let mut proposed = ProposedSlots::new();
            loop {
                tokio::select! {
                    _ = shutdown.wait_cancelled() => break,
                    _ = tick.tick() => {}
                }
                if shutdown.is_cancelled() {
                    break;
                }
                if !loop_shared.is_singleton_worker() {
                    // A node that leads again starts from what the replicas
                    // hold, not from what it proposed before.
                    proposed.clear();
                    continue;
                }
                propose_due_slots(&loop_shared, &mut proposed).await;
            }
        },
    );
}

/// Propose the current slot of every `Periodic` aggregate whose slot
/// turned over since this node last proposed it.
async fn propose_due_slots(state: &SharedState, proposed: &mut ProposedSlots) {
    let stored = match state.credentials.catalog().load_all_continuous_aggregates() {
        Ok(stored) => stored,
        Err(e) => {
            tracing::warn!(error = %e, "periodic refresh: catalog read failed");
            return;
        }
    };
    let now_ms = crate::engine::kv::current_ms();
    let mut live = HashMap::new();
    for s in stored {
        // A read-only mirror folds its source's cuts when they arrive through
        // the log. Its own slots start at PROMOTE.
        if state
            .mirror
            .fence
            .is_read_only(DatabaseId::new(s.database_id))
        {
            continue;
        }
        let Ok(def) = zerompk::from_msgpack::<ContinuousAggregateDef>(&s.def_bytes) else {
            continue;
        };
        let RefreshPolicy::Periodic(interval_ms) = def.refresh_policy else {
            continue;
        };
        if interval_ms == 0 {
            continue;
        }
        let slot = now_ms / interval_ms;
        let key = (s.database_id, s.tenant_id, s.name.clone());
        let last = proposed.get(&key).copied().unwrap_or(0);
        live.insert(key.clone(), last);
        if slot <= last {
            continue;
        }
        match propose_slot(state, &s.name, s.source.clone(), &def, slot, s.tenant_id).await {
            Ok(()) => {
                live.insert(key, slot);
            }
            Err(e) => tracing::warn!(
                aggregate = %s.name,
                slot,
                error = %e,
                "periodic refresh cut not proposed; it is retried next scan"
            ),
        }
    }
    *proposed = live;
}

async fn propose_slot(
    state: &SharedState,
    name: &str,
    source: String,
    def: &ContinuousAggregateDef,
    slot: u64,
    tenant_id: u64,
) -> crate::Result<()> {
    let catalog = state.credentials.catalog();
    let root = resolve_root(name, source, |agg| {
        catalog
            .get_continuous_aggregate(def.database_id, tenant_id, agg)
            .map(|s| s.map(|s| s.source))
    })?;
    let cut = RefreshCut {
        aggregate: name.to_string(),
        incarnation: def.incarnation,
        refresh_id: slot,
        periodic: true,
    };
    propose_refresh_cut(
        state,
        TenantId::new(tenant_id),
        DatabaseId::new(def.database_id),
        &root,
        cut,
    )
    .await
}
