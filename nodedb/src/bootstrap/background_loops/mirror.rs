// SPDX-License-Identifier: BUSL-1.1

//! Mirror database restart decisions and the mirror lag monitor.

use std::sync::Arc;
use std::time::Duration;

use tracing::info;

use crate::control::shutdown::{ShutdownPhase, spawn_loop};
use crate::control::state::SharedState;

/// Interval between mirror lag samples.
const MIRROR_LAG_INTERVAL: Duration = Duration::from_secs(5);

/// Enumerate mirror databases that need their observer link re-established
/// after a server restart, and log the restart decisions.
///
/// This is called once during startup, after [`SharedState`] and the catalog
/// are fully open. Databases with `MirrorStatus::Promoted` are excluded:
/// they are normal writable databases and must NOT attempt to reconnect.
/// The actual link objects are created by the cluster layer when it
/// processes each restart decision; this function only reads the catalog
/// and logs.
pub fn log_mirror_restart_decisions(shared: &Arc<SharedState>) {
    let catalog = shared.credentials.catalog();
    match crate::control::mirror::enumerate_resumable_mirrors(catalog) {
        Ok(decisions) => {
            for d in &decisions {
                tracing::info!(
                    database = %d.database_name,
                    resume_lsn = d.resume_from_lsn,
                    needs_bootstrap = d.needs_bootstrap,
                    "mirror restart: observer link will resume"
                );
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, "mirror restart: failed to enumerate mirrors; skipping");
        }
    }
}

/// Mirror lag monitor.
///
/// Reads `_system.mirror_lag` for every active mirror and updates the
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
                sample_mirror_lag(&shared_mirror);
            }
        },
    );
    info!("mirror lag monitor running");
}

/// One lag sample across every mirror database that is not promoted.
fn sample_mirror_lag(shared: &SharedState) {
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
        // means no link is registered for this database (the cluster layer
        // has not yet (re)established it after restart). `update_lag_status`
        // falls back to the catalog's apply time in that case, so the
        // disconnect timer still advances.
        let last_received = shared.mirror.links.last_received_ms(db.id);
        crate::control::mirror::update_lag_status(
            catalog,
            db.id,
            &db.name,
            &origin.status,
            last_received,
            false,
            &shared.database_metrics,
        );
    }
}
