// SPDX-License-Identifier: BUSL-1.1

//! Folding sealed timeseries partitions into continuous aggregates.
//!
//! A flush folds its own partition into the loaded `OnFlush` / `OnSeal`
//! aggregates at once. Everything else folds here, from the partition
//! files, never from the memtable: memtable rows fold at their own flush,
//! so reading them here would count them twice.
//!
//! - The maintenance tick reads images of unloaded aggregates, folds the
//!   partitions an aggregate created or restarted after them lacks, and
//!   folds the partitions pending refresh cuts name (`cagg_cut.rs`). It
//!   reads at most `cagg_fold_partitions_per_tick` partitions per call.
//!   While the core holds a refresh wait or an aggregate read, every
//!   `tick()` runs the same pass (`drive_cagg_waiters`).
//! - Retention folds each expiring partition into every aggregate before it
//!   removes the partition.

use std::collections::HashSet;

use super::partition_read::{TsPartitionColumns, partition_corrupt, read_ts_partition};
use super::paths::ts_collection_dir;
use crate::data::executor::core_loop::CoreLoop;
use crate::engine::timeseries::columnar_memtable::ColumnData;
use crate::engine::timeseries::continuous_agg::{FoldScope, FoldTarget, RefreshSource};
use crate::types::{DatabaseId, TenantId};

/// Read path named in partition corruption reports.
const FOLD_SITE: &str = "continuous_aggregate_fold";

/// Which partitions of the root a fold pass reads.
#[derive(Clone, Copy)]
pub(in crate::data::executor) enum FoldFilter<'a> {
    /// Every partition the registry lists.
    Every,
    /// Only the named partition directories.
    Only(&'a HashSet<String>),
}

/// The root collection a fold pass reads.
#[derive(Clone, Copy)]
pub(in crate::data::executor) struct CaggRoot<'a> {
    pub database_id: u64,
    pub tenant_id: u64,
    pub root: &'a str,
}

/// Outcome of one bounded fold pass.
pub(in crate::data::executor) struct FoldPass {
    /// Partitions read and folded.
    pub folded: usize,
    /// No partition in scope was left unread.
    pub complete: bool,
}

/// The rows of one root partition, read whole for a fold.
pub(in crate::data::executor) struct CaggSourceRows {
    part: TsPartitionColumns,
    columns: Vec<ColumnData>,
}

impl CaggSourceRows {
    /// The rows as a fold reads them.
    pub(in crate::data::executor) fn source(&self) -> RefreshSource<'_> {
        RefreshSource {
            schema: &self.part.schema,
            columns: &self.columns,
            symbol_dicts: &self.part.sym_dicts,
            row_count: self
                .columns
                .get(self.part.schema.timestamp_idx)
                .map_or(0, ColumnData::len),
        }
    }
}

impl CoreLoop {
    /// Read partition `dir` of `target.root` whole.
    pub(in crate::data::executor) fn read_cagg_source(
        &self,
        target: CaggRoot<'_>,
        dir: &str,
    ) -> crate::Result<CaggSourceRows> {
        let part_dir = ts_collection_dir(
            &self.data_dir,
            target.database_id,
            target.tenant_id,
            target.root,
        )
        .join(dir);
        let mut part = read_ts_partition(
            &part_dir,
            FOLD_SITE,
            self.segment_keks.ts_segment_kek.as_ref(),
        )?;
        let columns: Vec<ColumnData> = std::mem::take(&mut part.columns)
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| {
                partition_corrupt(
                    &part_dir,
                    "column",
                    FOLD_SITE,
                    "a schema column did not read",
                )
            })?;
        Ok(CaggSourceRows { part, columns })
    }

    /// Fold the partitions of `root` that an aggregate in `scope` lacks,
    /// reading at most `budget` partitions.
    pub(in crate::data::executor) fn fold_cagg_root(
        &mut self,
        target: CaggRoot<'_>,
        scope: FoldScope,
        filter: FoldFilter<'_>,
        budget: usize,
        now_ms: i64,
    ) -> crate::Result<FoldPass> {
        let CaggRoot {
            database_id,
            tenant_id,
            root,
        } = target;
        if !self.owns_cagg_root(database_id, root) {
            return Ok(FoldPass {
                folded: 0,
                complete: true,
            });
        }
        self.ensure_ts_registry(TenantId::new(tenant_id), DatabaseId::new(database_id), root)?;
        let key = (
            DatabaseId::new(database_id),
            TenantId::new(tenant_id),
            root.to_string(),
        );
        let mgr = &self.continuous_agg_mgr;
        let mut pending: Vec<String> = self
            .ts_registries
            .get(&key)
            .map(|registry| {
                // A partition a TRUNCATE ended holds no current row, so no
                // aggregate folds it.
                registry
                    .current_partitions()
                    .map(|entry| entry.dir_name.clone())
                    .filter(|dir| match filter {
                        FoldFilter::Every => true,
                        FoldFilter::Only(names) => names.contains(dir),
                    })
                    .filter(|dir| mgr.needs_fold(database_id, tenant_id, root, dir, scope))
                    .collect()
            })
            .unwrap_or_default();
        pending.sort_unstable();
        let complete = pending.len() <= budget;
        pending.truncate(budget);

        let mut folded = 0;
        for dir in pending {
            let rows = self.read_cagg_source(target, &dir)?;
            let src = rows.source();
            self.continuous_agg_mgr.fold_into_chain(
                FoldTarget {
                    database_id,
                    tenant_id,
                    root,
                    partition: &dir,
                },
                &src,
                scope,
                now_ms,
            );
            folded += 1;
        }
        Ok(FoldPass { folded, complete })
    }

    /// Maintenance tick body: load images, backfill, and fold pending
    /// refresh cuts, within the per-tick partition budget. A root whose
    /// partitions cannot be read is quarantined: its aggregates turn stale
    /// and take no fold. Refresh waits whose cut completed answer here.
    pub(in crate::data::executor) fn fold_pending_cagg_partitions(&mut self, now_ms: i64) -> usize {
        let mut budget = self.ts_tuning.cagg_fold_partitions_per_tick;
        let mut total = 0;
        let roots = self.continuous_agg_mgr.tick_roots();
        if roots.is_empty() {
            return 0;
        }
        for (database_id, tenant_id, root) in roots {
            if budget == 0 {
                break;
            }
            self.load_cagg_images(database_id, tenant_id, &root);
            let pass = self.fold_cagg_root(
                CaggRoot {
                    database_id,
                    tenant_id,
                    root: &root,
                },
                FoldScope::Tick,
                FoldFilter::Every,
                budget,
                now_ms,
            );
            match pass {
                Ok(pass) => {
                    budget = budget.saturating_sub(pass.folded);
                    total += pass.folded;
                    if pass.complete {
                        self.continuous_agg_mgr
                            .finish_tick(database_id, tenant_id, &root);
                    }
                }
                Err(e) => self.quarantine_cagg_root(database_id, tenant_id, &root, &e),
            }
        }
        self.persist_dirty_cagg_images();
        self.answer_cagg_refresh_waiters();
        self.answer_cagg_read_waiters();
        total
    }

    /// Load the images of `root` and fold every partition its `OnFlush` /
    /// `OnSeal` aggregates lack, without a budget. A read, a retention pass,
    /// and a snapshot capture run this first so they see complete state.
    pub(in crate::data::executor) fn complete_cagg_backfill(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        root: &str,
        now_ms: i64,
    ) -> crate::Result<()> {
        self.load_cagg_images(database_id, tenant_id, root);
        let pass = self.fold_cagg_root(
            CaggRoot {
                database_id,
                tenant_id,
                root,
            },
            FoldScope::AtFlush,
            FoldFilter::Every,
            usize::MAX,
            now_ms,
        );
        if let Err(e) = &pass {
            self.quarantine_cagg_root(database_id, tenant_id, root, e);
        }
        pass.map(|_| ())
    }

    /// Fold the partitions retention is about to remove into every
    /// aggregate rooted at `root`, whatever its refresh policy, and write
    /// the image. A cut aggregate parks the rows no pending cut names.
    /// Retention removes nothing unless this returns `Ok`.
    ///
    /// `dropped_through` is the newest time-key value of an expiring
    /// partition. The image records it before any partition leaves, so a
    /// read at a transaction snapshot never takes a raw read of that range
    /// for complete (`raw_dropped_through`).
    pub(in crate::data::executor) fn fold_expiring_partitions(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        root: &str,
        expiring: &HashSet<String>,
        dropped_through: i64,
        now_ms: i64,
    ) -> crate::Result<()> {
        if self
            .continuous_agg_mgr
            .chain(database_id, tenant_id, root)
            .is_empty()
        {
            return Ok(());
        }
        self.load_cagg_images(database_id, tenant_id, root);
        let pass = self.fold_cagg_root(
            CaggRoot {
                database_id,
                tenant_id,
                root,
            },
            FoldScope::Expiring,
            FoldFilter::Only(expiring),
            usize::MAX,
            now_ms,
        );
        if let Err(e) = &pass {
            self.quarantine_cagg_root(database_id, tenant_id, root, e);
        }
        pass?;
        self.continuous_agg_mgr
            .note_raw_dropped(database_id, tenant_id, root, dropped_through);
        self.persist_cagg_image(database_id, tenant_id, root)
    }

    /// Quarantine `root` after a fold error. A partition corruption already
    /// filed its report where it was found; any other error files one here.
    pub(in crate::data::executor) fn quarantine_cagg_root(
        &mut self,
        database_id: u64,
        tenant_id: u64,
        root: &str,
        error: &crate::Error,
    ) {
        tracing::error!(
            core = self.core_id,
            collection = %root,
            error = %error,
            "continuous aggregate fold failed; its aggregates are marked stale"
        );
        if !matches!(error, crate::Error::SegmentCorrupted { .. }) {
            crate::diag::continuous_aggregate_state_fault(
                error,
                "fold",
                database_id,
                tenant_id,
                root,
            );
        }
        self.continuous_agg_mgr
            .quarantine_root(database_id, tenant_id, root);
    }
}

#[cfg(test)]
mod tests {
    use super::super::cagg_test_support::{
        COLLECTION, ROWS_A, ROWS_B, TENANT, flush, host_def, image_path, ingest, register, task_at,
        totals,
    };
    use crate::bridge::envelope::Status;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::engine::timeseries::continuous_agg::RefreshPolicy;

    const SUM_A: i64 = 111;
    const SUM_AB: i64 = 11_111;

    #[test]
    fn fold_backfills_partitions_flushed_before_register() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        assert_eq!(ingest(&mut core, ROWS_A, 1), Status::Ok);
        flush(&mut core);
        assert_eq!(ingest(&mut core, ROWS_B, 2), Status::Ok);
        flush(&mut core);

        register(&mut core, host_def("by_host", RefreshPolicy::OnFlush));
        assert_eq!(totals(&core, "by_host"), (0, 0));
        core.fold_pending_cagg_partitions(0);
        assert_eq!(totals(&core, "by_host"), (5, SUM_AB));
        core.fold_pending_cagg_partitions(0);
        assert_eq!(
            totals(&core, "by_host"),
            (5, SUM_AB),
            "a partition folds once"
        );
    }

    #[test]
    fn the_fold_budget_spreads_a_backfill_over_ticks() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        core.ts_tuning.cagg_fold_partitions_per_tick = 1;
        assert_eq!(ingest(&mut core, ROWS_A, 1), Status::Ok);
        flush(&mut core);
        assert_eq!(ingest(&mut core, ROWS_B, 2), Status::Ok);
        flush(&mut core);
        register(&mut core, host_def("by_host", RefreshPolicy::OnFlush));

        assert_eq!(core.fold_pending_cagg_partitions(0), 1);
        assert_eq!(core.fold_pending_cagg_partitions(0), 1);
        assert_eq!(core.fold_pending_cagg_partitions(0), 0);
        assert_eq!(totals(&core, "by_host"), (5, SUM_AB));
    }

    #[test]
    fn flush_during_backfill_is_not_double_counted() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        core.ts_tuning.cagg_fold_partitions_per_tick = 1;
        assert_eq!(ingest(&mut core, ROWS_A, 1), Status::Ok);
        flush(&mut core);
        register(&mut core, host_def("by_host", RefreshPolicy::OnFlush));
        core.fold_pending_cagg_partitions(0);
        assert_eq!(ingest(&mut core, ROWS_B, 2), Status::Ok);
        flush(&mut core);
        core.fold_pending_cagg_partitions(0);
        core.fold_pending_cagg_partitions(0);
        assert_eq!(totals(&core, "by_host"), (5, SUM_AB));
    }

    #[test]
    fn restart_refolds_each_partition_exactly_once() {
        let dir = tempfile::tempdir().expect("tempdir");
        {
            let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
            register(&mut core, host_def("by_host", RefreshPolicy::OnFlush));
            core.fold_pending_cagg_partitions(0);
            assert_eq!(ingest(&mut core, ROWS_A, 1), Status::Ok);
            flush(&mut core);
            assert_eq!(totals(&core, "by_host"), (3, SUM_A));
        }
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        core.load_ts_registries().expect("load registries");
        register(&mut core, host_def("by_host", RefreshPolicy::OnFlush));
        core.fold_pending_cagg_partitions(0);
        core.fold_pending_cagg_partitions(0);
        assert_eq!(totals(&core, "by_host"), (3, SUM_A));
    }

    #[test]
    fn crash_before_image_write_refolds_without_double_count() {
        let dir = tempfile::tempdir().expect("tempdir");
        {
            let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
            register(&mut core, host_def("by_host", RefreshPolicy::OnFlush));
            core.fold_pending_cagg_partitions(0);
            assert_eq!(ingest(&mut core, ROWS_A, 1), Status::Ok);
            flush(&mut core);
            core.persist_dirty_cagg_images();
            assert!(image_path(dir.path()).exists());
            assert_eq!(ingest(&mut core, ROWS_B, 2), Status::Ok);
            flush(&mut core);
        }
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        core.load_ts_registries().expect("load registries");
        register(&mut core, host_def("by_host", RefreshPolicy::OnFlush));
        core.fold_pending_cagg_partitions(0);
        assert_eq!(totals(&core, "by_host"), (5, SUM_AB));
    }

    #[test]
    fn recreated_aggregate_ignores_stale_image() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        register(&mut core, host_def("by_host", RefreshPolicy::OnFlush));
        core.fold_pending_cagg_partitions(0);
        assert_eq!(ingest(&mut core, ROWS_A, 1), Status::Ok);
        flush(&mut core);
        core.persist_dirty_cagg_images();

        core.continuous_agg_mgr.unregister(0, TENANT, "by_host");
        let mut recreated = host_def("by_host", RefreshPolicy::OnFlush);
        recreated.incarnation = nodedb_types::Hlc::new(2, 0);
        register(&mut core, recreated);
        core.fold_pending_cagg_partitions(0);
        assert_eq!(totals(&core, "by_host"), (3, SUM_A));
    }

    #[test]
    fn clear_then_install_removes_image() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        register(&mut core, host_def("by_host", RefreshPolicy::OnFlush));
        core.fold_pending_cagg_partitions(0);
        assert_eq!(ingest(&mut core, ROWS_A, 1), Status::Ok);
        flush(&mut core);
        core.persist_dirty_cagg_images();
        assert!(image_path(dir.path()).exists());

        core.clear_collection_all_engines(
            nodedb_types::DatabaseId::DEFAULT,
            crate::types::TenantId::new(TENANT),
            COLLECTION,
            true,
            false,
            true,
        )
        .expect("clear");
        assert!(!image_path(dir.path()).exists());
        assert_eq!(totals(&core, "by_host"), (0, 0));
        assert!(core.continuous_agg_mgr.has_unloaded(0, TENANT, COLLECTION));
    }

    #[test]
    fn truncate_undo_restores_aggregate_from_image() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        register(&mut core, host_def("by_host", RefreshPolicy::OnFlush));
        core.fold_pending_cagg_partitions(0);
        assert_eq!(ingest(&mut core, ROWS_A, 1), Status::Ok);
        flush(&mut core);
        core.persist_dirty_cagg_images();

        let mut undo = Vec::new();
        let resp = core.execute_timeseries_truncate(&task_at(3), COLLECTION, Some(&mut undo));
        assert_eq!(resp.status, Status::Ok);
        assert_eq!(totals(&core, "by_host"), (0, 0));
        core.rollback_undo_log(0, TENANT, undo).expect("rollback");
        core.fold_pending_cagg_partitions(0);
        assert_eq!(totals(&core, "by_host"), (3, SUM_A));
    }

    /// A truncate leaves the aggregates empty. The partition it ended keeps
    /// its rows on disk, and no fold reads it again: after the truncate the
    /// aggregate counts only rows written after it.
    #[test]
    fn a_truncate_leaves_the_aggregate_with_the_later_rows_only() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        register(&mut core, host_def("by_host", RefreshPolicy::OnFlush));
        core.fold_pending_cagg_partitions(0);
        assert_eq!(ingest(&mut core, ROWS_A, 1), Status::Ok);
        flush(&mut core);
        core.persist_dirty_cagg_images();

        let resp = core.execute_timeseries_truncate(&task_at(3), COLLECTION, None);
        assert_eq!(resp.status, Status::Ok);
        let image = crate::engine::timeseries::continuous_agg::decode_image(
            &std::fs::read(image_path(dir.path())).expect("the truncate rewrites the image"),
        )
        .expect("decode");
        assert!(
            image.entries.iter().all(|entry| entry.buckets.is_empty()),
            "the rewritten image holds no bucket"
        );
        core.fold_pending_cagg_partitions(0);
        assert_eq!(
            totals(&core, "by_host"),
            (0, 0),
            "the ended partition is not folded"
        );

        assert_eq!(ingest(&mut core, ROWS_B, 4), Status::Ok);
        flush(&mut core);
        core.fold_pending_cagg_partitions(0);
        assert_eq!(totals(&core, "by_host"), (2, SUM_AB - SUM_A));
    }

    #[test]
    fn unreadable_image_quarantines_the_root() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        assert_eq!(ingest(&mut core, ROWS_A, 1), Status::Ok);
        flush(&mut core);
        std::fs::write(image_path(dir.path()), [0xFF_u8]).expect("write junk image");
        register(&mut core, host_def("by_host", RefreshPolicy::OnFlush));
        core.fold_pending_cagg_partitions(0);
        let def = core
            .continuous_agg_mgr
            .get_definition(0, TENANT, "by_host")
            .expect("registered");
        assert!(def.stale);
        assert_eq!(totals(&core, "by_host"), (0, 0));
    }
}
