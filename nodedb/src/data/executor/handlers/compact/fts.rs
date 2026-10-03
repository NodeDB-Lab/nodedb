// SPDX-License-Identifier: BUSL-1.1

//! FTS LSM level compaction, budget-gated against the per-database
//! maintenance CPU tracker.
//!
//! ## Overview
//!
//! Each FTS index uses a tiered LSM: memtable flushes produce L0 segments.
//! When a level accumulates more segments than `max_segments_per_level`
//! (default 8), all segments at that level are merged into one at the next
//! level. Without periodic compaction the L0 count grows without bound,
//! increasing per-query merge cost.
//!
//! ## Budget enforcement
//!
//! Each `(tenant, collection)` is gated against the per-database maintenance
//! CPU budget before compaction work starts. Collections whose database has
//! exhausted its budget for the current minute are deferred and counted in
//! `CompactionStats::fts_deferred`. On the next maintenance cycle the budget
//! window slides and the deferred collections are eligible again.
//!
//! ## Crash safety
//!
//! The merged segment is written and the source segments removed in a single
//! redb write transaction via `InvertedIndex::compact_commit`. A crash
//! mid-compaction leaves the original segments intact; the next maintenance
//! pass detects the same compaction need and retries.
//!
//! ## Failure surfacing
//!
//! Three distinct failure modes are surfaced via `CompactionStats` so
//! operators can distinguish work-done from work-deferred without parsing logs:
//!
//! - `fts_deferred` increments per collection whose lease was rejected by
//!   the budget gate, *or* whose merge aborted on backend / commit failure.
//!   In both cases the original segments are intact and the next maintenance
//!   pass will retry.
//! - `fts_enumeration_failed` is set when the segments-table read txn itself
//!   fails — we couldn't even discover what work was needed.

use std::sync::atomic::{AtomicU64, Ordering};

use nodedb_fts::IndexScope;
use nodedb_fts::backend::FtsBackend as _;
use nodedb_fts::lsm::compaction::{
    CompactError, CompactLevelParams, CompactionConfig, SegmentMeta, compact_level,
    needs_compaction, parse_level, segment_id,
};
use tracing::info;

use crate::data::executor::core_loop::CoreLoop;
use nodedb_types::TenantId;

use super::budget::BudgetGate;

/// Outcome summary for a single FTS compaction pass.
pub(super) struct FtsCompactionOutcome {
    /// Total segments merged across all (tid, collection) pairs.
    pub merged: u64,
    /// Number of (tid, collection) pairs deferred this cycle — either by the
    /// budget gate or by a transient backend/commit failure.
    pub deferred: u64,
    /// `true` iff the segments-table enumeration itself failed and we could
    /// not iterate any candidates this cycle.
    pub enumeration_failed: bool,
}

/// Identifies one `(database, tenant, index)` FTS compaction unit.
///
/// Bundled so `compact_one_fts_collection` stays within the argument budget;
/// `force`, the shared `config`, and the mutable `outcome` accumulator are
/// passed alongside it.
struct FtsCompactionTarget<'a> {
    database_id: u64,
    tid: TenantId,
    index: IndexScope<'a>,
}

impl CoreLoop {
    /// Compact FTS LSM levels for every collection that has accumulated more
    /// segments than `max_segments_per_level`.
    ///
    /// Reads the segment list from redb, evaluates `needs_compaction`, acquires
    /// the maintenance lease, merges, and commits atomically. One level per
    /// collection per call — repeated maintenance ticks drain further levels.
    pub(super) fn run_fts_compaction(&mut self, force: bool) -> FtsCompactionOutcome {
        let config = CompactionConfig::default();
        let mut outcome = FtsCompactionOutcome {
            merged: 0,
            deferred: 0,
            enumeration_failed: false,
        };

        // Enumerate all (tenant, collection) pairs that have at least one
        // segment. This queries the FTS subsystem for what it owns — no
        // external registry. A failure here is distinct from per-collection
        // deferral and is surfaced via `enumeration_failed`.
        let indexes = match self.inverted.list_all_fts_indexes() {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(
                    core = self.core_id,
                    error = %e,
                    "FTS compaction: failed to enumerate collections"
                );
                outcome.enumeration_failed = true;
                return outcome;
            }
        };

        for (database_id, tid, collection, field) in &indexes {
            self.compact_one_fts_collection(
                FtsCompactionTarget {
                    database_id: *database_id,
                    tid: *tid,
                    index: IndexScope::from_key(collection, field),
                },
                force,
                &config,
                &mut outcome,
            );
        }

        outcome
    }

    /// Compact one (tid, collection) pair: list segments, gate against the
    /// budget, merge, commit. Deferral or failure increments
    /// `outcome.deferred`; success increments `outcome.merged`.
    fn compact_one_fts_collection(
        &mut self,
        target: FtsCompactionTarget<'_>,
        force: bool,
        config: &CompactionConfig,
        outcome: &mut FtsCompactionOutcome,
    ) {
        let FtsCompactionTarget {
            database_id,
            tid,
            index,
        } = target;
        let collection = index.collection();
        let db = nodedb_types::DatabaseId::new(database_id);
        let tid_u64 = tid.as_u64();

        // Resolve segment list before acquiring the lease so we only hold
        // the lease across actual merge work, not the read path.
        let segment_ids = match self
            .inverted
            .backend()
            .list_segments(database_id, tid_u64, index)
        {
            Ok(ids) => ids,
            Err(e) => {
                tracing::warn!(
                    core = self.core_id,
                    tid = tid_u64,
                    collection = %collection,
                    error = %e,
                    "FTS compaction: failed to list segments — deferred to next cycle"
                );
                outcome.deferred += 1;
                return;
            }
        };

        if segment_ids.is_empty() {
            return;
        }

        // Build lightweight SegmentMeta from segment IDs. The `size` field
        // is not used by `needs_compaction` or `compact_level`; 0 is correct.
        let segments: Vec<SegmentMeta> = segment_ids
            .iter()
            .map(|id| SegmentMeta {
                segment_id: id.clone(),
                level: parse_level(id),
                size: 0,
            })
            .collect();

        let level = match needs_compaction(&segments, config) {
            Some(l) => l,
            None => return,
        };

        // Budget gate — lease must be held across the merge work so elapsed
        // wall-clock time is recorded into the per-database sliding window.
        let _lease = match self.acquire_maintenance_lease(db, force) {
            BudgetGate::Granted(lease) => lease,
            BudgetGate::Deferred => {
                outcome.deferred += 1;
                tracing::debug!(
                    core = self.core_id,
                    db = db.as_u64(),
                    collection = %collection,
                    "FTS compaction deferred: database over maintenance budget"
                );
                return;
            }
        };

        // `CompactLevelParams::governor` is `&Arc<MemoryGovernor>` in
        // nodedb-fts (a shared engine crate, out of scope for this change).
        let params = CompactLevelParams {
            backend: self.inverted.backend(),
            database_id,
            tid: tid_u64,
            index,
            segments: &segments,
            level,
            governor: &self.governor,
        };

        match compact_level(params) {
            Ok(Some((new_bytes, merged_ids))) => {
                let new_level = level + 1;
                let new_id_num = fts_new_segment_id();
                let new_seg_id = segment_id(new_id_num, new_level);
                let merged_count = merged_ids.len() as u64;

                match self
                    .inverted
                    .compact_commit(crate::engine::sparse::fts_redb::CompactCommit {
                        database_id,
                        tid: tid_u64,
                        index,
                        new_segment_id: &new_seg_id,
                        new_segment_data: &new_bytes,
                        merged_ids: &merged_ids,
                    }) {
                    Ok(()) => {
                        outcome.merged += merged_count;
                        info!(
                            core = self.core_id,
                            tid = tid_u64,
                            collection = %collection,
                            level,
                            new_level,
                            merged = merged_count,
                            "FTS LSM level compaction committed"
                        );
                    }
                    Err(e) => {
                        // The commit txn aborted: the original segments are
                        // intact, so this is a deferral — counted in
                        // `outcome.deferred` so operators can see it.
                        outcome.deferred += 1;
                        tracing::warn!(
                            core = self.core_id,
                            tid = tid_u64,
                            collection = %collection,
                            level,
                            error = %e,
                            "FTS compaction: commit failed, original segments preserved — deferred"
                        );
                    }
                }
            }
            Ok(None) => {
                // Fewer than 2 readable segments — nothing to merge.
            }
            Err(CompactError::Budget(e)) => {
                outcome.deferred += 1;
                tracing::debug!(
                    core = self.core_id,
                    tid = tid_u64,
                    collection = %collection,
                    error = %e,
                    "FTS compaction: memory budget exhausted, deferred"
                );
            }
            Err(CompactError::Backend(e)) => {
                outcome.deferred += 1;
                tracing::warn!(
                    core = self.core_id,
                    tid = tid_u64,
                    collection = %collection,
                    level,
                    error = %e,
                    "FTS compaction: segment read failed, deferred"
                );
            }
        }
        // `_lease` drops here, recording elapsed wall-clock into the budget window.
    }
}

/// Generate a unique numeric segment ID for the output of a compaction.
///
/// The ID is the concatenation of full-resolution UNIX-epoch nanoseconds (low
/// 32 bits, wrapping ~4.3 seconds) and a process-local monotonic counter
/// (high 32 bits). The counter alone is unique within a single process run;
/// mixing in nanos disambiguates IDs across process restarts. Together they
/// give a globally unique 64-bit value at FTS-compaction frequencies — the
/// counter would have to overflow 4 billion within one nanosecond to collide.
fn fts_new_segment_id() -> u64 {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed) & 0xFFFF_FFFF;
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
        & 0xFFFF_FFFF;
    (n << 32) | nanos
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_ids_are_unique_within_process() {
        let mut seen = std::collections::HashSet::new();
        for _ in 0..10_000 {
            assert!(
                seen.insert(fts_new_segment_id()),
                "fts_new_segment_id produced a duplicate within a single process run"
            );
        }
    }
}
