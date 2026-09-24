// SPDX-License-Identifier: BUSL-1.1

//! Periodic sampler for the per-database metrics that need an outside reader.
//!
//! The `DatabaseMetricsRegistry` setters exist for subsystems that own a
//! value continuously; the ones filled here have no single owning writer, so
//! a sampler reads them on an interval:
//!
//! * `connections` — the live permit count from the admission registry. The
//!   registry holds an entry once a quota record has been applied for the
//!   database, and a database with no entry stays unpublished: absence is
//!   "unmeasured", never a fabricated zero. Removing a cap keeps the entry and
//!   its live count, so a measured zero still publishes.
//! * `bridge_queue_depth` — summed from every core's dispatch WFQ for the
//!   database, so a bulk loader can read queue pressure before the WFQ
//!   refuses a request.
//!
//! Families whose owning subsystem does not exist yet are deliberately not
//! written, because publishing a constant is worse than publishing nothing:
//!
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

    // Resolve each database's identity the way `SHOW DATABASE USAGE` does
    // before it reads the admission registry: by name, through the catalog.
    let ids: HashMap<String, u64> = databases
        .iter()
        .map(|db| {
            let id = catalog
                .get_database_id_by_name(&db.name)
                .ok()
                .flatten()
                .unwrap_or(db.id);
            (db.name.clone(), id.as_u64())
        })
        .collect();

    // Connections publish first and without touching the dispatcher: a
    // sampler that blocks behind the dispatch poller would freeze every
    // family it fills. A database whose permit entry does not exist stays
    // unpublished; an entry that exists publishes its live count, zero
    // included, because the registry tracks the count even after a cap is
    // removed.
    for db in &databases {
        let id = ids.get(&db.name).copied().unwrap_or_else(|| db.id.as_u64());
        if let Some(live) = state
            .admission_registry
            .database_live_connections(crate::types::DatabaseId::new(id))
        {
            state
                .database_metrics
                .set_connections(&db.name, u64::from(live));
        }
    }

    // Queue depth needs the dispatcher lock, which the response poller holds
    // in a tight loop. `try_lock` keeps the sampler moving: a contended round
    // keeps the previous depth instead of stalling the whole sample.
    let dispatcher = match state.dispatcher.try_lock() {
        Ok(guard) => Some(guard),
        Err(std::sync::TryLockError::Poisoned(poisoned)) => Some(poisoned.into_inner()),
        Err(std::sync::TryLockError::WouldBlock) => None,
    };
    if let Some(dispatcher) = dispatcher {
        for db in &databases {
            let id = ids.get(&db.name).copied().unwrap_or_else(|| db.id.as_u64());
            state
                .database_metrics
                .set_bridge_queue_depth(&db.name, dispatcher.db_queue_depth(id));
        }
    }
}
