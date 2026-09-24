// SPDX-License-Identifier: BUSL-1.1

//! Periodic sampler for the per-database metrics that need an outside reader.
//!
//! The `DatabaseMetricsRegistry` setters exist for subsystems that own a
//! value continuously; the ones filled here have no single owning writer, so
//! a sampler reads them on an interval:
//!
//! * `bridge_queue_depth` — summed from every core's dispatch WFQ for the
//!   database, so a bulk loader can read queue pressure before the WFQ
//!   refuses a request.
//!
//! Families whose owning subsystem does not exist yet are deliberately not
//! written, because publishing a constant is worse than publishing nothing:
//!
//! * `connections` — `state.session_registry` has no production registration
//!   caller, so counting from it would publish a hard zero. Attribution needs
//!   the pgwire connection lifecycle (the factory's connection registry knows
//!   the bound database).
//! * `memory_used_bytes` / `storage_used_bytes` — no per-database usage
//!   accessor exists yet (budgets are per database, usage is not).
//! * `wal_commit_latency_p99_us` — WAL commits are not attributed per
//!   database.
//! * `maintenance_cpu_seconds_total` — no per-database CPU accounting exists.

use std::collections::HashMap;

use crate::control::state::SharedState;

/// Sample every database once and update the gauges that have a source.
pub fn sample_once(state: &SharedState) {
    let catalog = state.credentials.catalog();
    let databases = match catalog.list_databases() {
        Ok(databases) => databases,
        Err(e) => {
            tracing::warn!(error = %e, "database metrics sampler: catalog list failed");
            return;
        }
    };

    let depths: HashMap<u64, u64> = {
        let dispatcher = match state.dispatcher.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        databases
            .iter()
            .map(|db| {
                let id = db.id.as_u64();
                (id, dispatcher.db_queue_depth(id))
            })
            .collect()
    };

    for db in databases {
        state
            .database_metrics
            .set_bridge_queue_depth(&db.name, depths.get(&db.id.as_u64()).copied().unwrap_or(0));
    }
}
