// SPDX-License-Identifier: BUSL-1.1

//! The mirror lag monitor.

use std::sync::Arc;
use std::time::Duration;

use nodedb_types::MirrorStatusStep;
use tracing::info;

use crate::control::catalog_entry::CatalogEntry;
use crate::control::metadata_proposer::propose_catalog_entry_async;
use crate::control::shutdown::{ShutdownPhase, spawn_loop};
use crate::control::state::SharedState;

/// Interval between mirror lag samples.
const MIRROR_LAG_INTERVAL: Duration = Duration::from_secs(5);

/// Mirror lag monitor.
///
/// Reads `_system.mirror_cut_state` for every active mirror and updates the
/// `nodedb_database_mirror_lag_ms` metric. Also drives status transitions
/// (Following → Degraded → Disconnected) and clears the metric when a
/// mirror is promoted.
pub fn spawn_mirror_lag_monitor(shared: &Arc<SharedState>) {
    let shared_mirror = Arc::clone(shared);
    spawn_loop(
        &shared.loop_registry,
        &shared.shutdown,
        "mirror_lag_monitor",
        ShutdownPhase::DrainingControlPlane,
        move |mut shutdown| async move {
            let mut tick = tokio::time::interval(MIRROR_LAG_INTERVAL);
            loop {
                tokio::select! {
                    _ = shutdown.wait_cancelled() => break,
                    _ = tick.tick() => {}
                }
                if shutdown.is_cancelled() {
                    break;
                }
                sample_mirror_lag(&shared_mirror).await;
            }
        },
    );
    info!("mirror lag monitor running");
}

/// One lag sample across every mirror database that is not promoted.
///
/// Every node updates its metric. Only the metadata leader proposes a status:
/// the applier runs there, so its receive timestamp is the one that moves. Each
/// step names the status it read, so it cannot undo a later PROMOTE. A proposal
/// that does not land leaves the status, and the next sample proposes it again.
pub async fn sample_mirror_lag(shared: &SharedState) {
    let catalog = shared.credentials.catalog();
    let databases = match catalog.list_databases() {
        Ok(d) => d,
        Err(e) => {
            tracing::warn!(error = %e, "mirror_lag_monitor: catalog list error");
            return;
        }
    };
    for db in databases {
        let Some(origin) = db.mirror_origin.as_ref() else {
            continue;
        };
        // Promoted mirrors are normal writable databases — skip.
        if matches!(origin.status, nodedb_types::MirrorStatus::Promoted) {
            continue;
        }
        // Read the real receive timestamp from the link registry. `None`
        // means no link is registered for this database (the supervisor has
        // not started its applier on this node yet). `update_lag_status`
        // falls back to the applied cut's time in that case, so the
        // disconnect timer still advances.
        let last_received = shared.mirror.links.last_received_ms(db.id);
        let next = crate::control::mirror::update_lag_status(
            catalog,
            db.id,
            &db.name,
            &origin.status,
            last_received,
            &shared.database_metrics,
        );
        let Some(next) = next else {
            continue;
        };
        if !shared.is_singleton_worker() {
            continue;
        }
        let step = CatalogEntry::StepMirrorStatus {
            database_id: db.id.as_u64(),
            step: MirrorStatusStep::Lag {
                from: origin.status.clone(),
                to: next,
            },
        };
        if let Err(error) = propose_catalog_entry_async(shared, &step).await {
            tracing::warn!(
                database = %db.name,
                %error,
                "mirror_lag_monitor: the status step did not land; the next sample retries"
            );
        }
    }
}
