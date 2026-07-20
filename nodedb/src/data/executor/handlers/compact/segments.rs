// SPDX-License-Identifier: BUSL-1.1

//! Timeseries L1 segment compaction.
//!
//! Selects mergeable partition groups via the per-collection partition
//! registry, marks them for merge, and purges expired/deleted partitions.
//! The actual merge I/O is owned by the partition registry and the flush
//! pipeline; this handler is only concerned with selection and budget gating.

use tracing::info;

use crate::data::executor::core_loop::CoreLoop;
use nodedb_types::DatabaseId;

impl CoreLoop {
    /// Run L1 segment compaction using the partition registry's merge logic.
    ///
    /// Finds eligible sealed partitions via `find_mergeable`, marks them
    /// for merge, and purges expired/deleted partitions. The actual merge
    /// I/O is handled by the partition registry and flush pipeline.
    ///
    /// Returns `(merged, deferred)` — the count of partitions selected for
    /// merge, and the count of `(tenant, collection)` pairs whose owning
    /// database was over its maintenance CPU budget and was skipped.
    pub(super) fn run_segment_compaction(&mut self, force: bool) -> (usize, usize) {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        let max_per_pass = self.segment_compaction_config.max_segments_per_pass;
        let mut total_merged = 0usize;
        let mut total_deferred = 0usize;

        // Snapshot bitemporal flags per collection up front so the
        // mutable-borrow loop below can query them without re-borrowing `self`.
        let bitemporal_flags: std::collections::HashMap<
            (DatabaseId, crate::types::TenantId, String),
            bool,
        > = self
            .ts_registries
            .keys()
            .map(|(db, tid, col)| {
                let flag = self.is_bitemporal(db.as_u64(), tid.as_u64(), col);
                ((*db, *tid, col.clone()), flag)
            })
            .collect();

        let budget = self.maintenance_budget.clone();
        let core_id = self.core_id;

        for ((db, tid, collection), registry) in &mut self.ts_registries {
            // The owning database is now part of the registry key, so the
            // maintenance-budget lease scopes directly to it — no tenant→db
            // lookup needed.
            let db = *db;

            // Per-(tenant, collection) budget gate. Lease lives across the
            // mark/purge work below — its drop records elapsed wall-clock
            // into the per-database window.
            let _lease = if force {
                None
            } else {
                match budget.as_ref() {
                    None => None,
                    Some(tracker) => match tracker.try_acquire(db, 0.0) {
                        Some(l) => Some(l),
                        None => {
                            total_deferred += 1;
                            tracing::debug!(
                                core = core_id,
                                db = db.as_u64(),
                                collection = %collection,
                                "segment compaction deferred: database over maintenance budget"
                            );
                            continue;
                        }
                    },
                }
            };

            let bitemporal = bitemporal_flags
                .get(&(db, *tid, collection.clone()))
                .copied()
                .unwrap_or(false);

            // Find mergeable partition groups, limited by config.
            let mut groups = registry.find_mergeable(now_ms);
            if !force {
                groups.truncate(max_per_pass);
            }
            for group in &groups {
                for &start_ts in group {
                    registry.mark_merging(start_ts);
                }
                total_merged += group.len();
            }

            // Retention: find and purge expired partitions.
            let expired = registry.find_expired(now_ms, bitemporal);
            for start_ts in &expired {
                registry.mark_deleted(*start_ts);
            }
            let purged = registry.purge_deleted();

            if !groups.is_empty() || !purged.is_empty() {
                info!(
                    core = core_id,
                    collection = %collection,
                    merge_groups = groups.len(),
                    expired = expired.len(),
                    purged = purged.len(),
                    "timeseries partition compaction"
                );
            }

            // Force mode: also compact partitions that wouldn't normally merge.
            if force && total_merged == 0 {
                let sealed_count = registry.sealed_count();
                if sealed_count >= 2 {
                    info!(
                        core = core_id,
                        collection = %collection,
                        sealed_count,
                        "forced compaction: marking sealed partitions for merge"
                    );
                }
            }
        }

        (total_merged, total_deferred)
    }
}
