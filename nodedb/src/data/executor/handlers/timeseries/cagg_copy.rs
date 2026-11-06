// SPDX-License-Identifier: BUSL-1.1

//! Continuous-aggregate state for a copy that re-issues a root's rows.
//!
//! A source captures the state, and a mirror's bootstrap installs it after the
//! root's rows. See `engine::timeseries::continuous_agg::copy` for the rules.

use std::collections::HashSet;

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;
use crate::engine::timeseries::continuous_agg::{RefreshSource, decode_image, encode_image};
use crate::types::{DatabaseId, TenantId};

use super::cagg_fold::CaggRoot;

impl CoreLoop {
    /// The copy image of `root`, from its encoded image `bytes`. Every applied
    /// cut has folded. The rows of every partition and memtable no aggregate
    /// folded join the image, in the same core turn the capture reads them.
    pub(in crate::data::executor) fn cagg_copy_image(
        &self,
        database_id: u64,
        tenant_id: u64,
        root: &str,
        bytes: &[u8],
    ) -> crate::Result<Vec<u8>> {
        let mut image = decode_image(bytes)?;
        let target = CaggRoot {
            database_id,
            tenant_id,
            root,
        };
        let key = (
            DatabaseId::new(database_id),
            TenantId::new(tenant_id),
            root.to_string(),
        );
        // A partition a TRUNCATE ended holds no current row.
        let current: Vec<String> = self
            .ts_registries
            .get(&key)
            .map(|registry| {
                registry
                    .current_partitions()
                    .map(|entry| entry.dir_name.clone())
                    .collect()
            })
            .unwrap_or_default();
        let mgr = &self.continuous_agg_mgr;
        for entry in &mut image.entries {
            let folded: HashSet<String> = entry.folded.iter().cloned().collect();
            for dir in current.iter().filter(|dir| !folded.contains(*dir)) {
                let rows = self.read_cagg_source(target, dir)?;
                mgr.absorb_unfolded(database_id, tenant_id, entry, &rows.source());
            }
            if let Some(memtable) = self.columnar_memtables.get(&key) {
                let view = memtable.flush_view();
                let src = RefreshSource {
                    schema: view.schema,
                    columns: view.columns,
                    symbol_dicts: view.symbol_dicts,
                    row_count: usize::try_from(view.row_count).unwrap_or(usize::MAX),
                };
                mgr.absorb_unfolded(database_id, tenant_id, entry, &src);
            }
        }
        image.seal_for_copy();
        Ok(encode_image(&image.view())?)
    }

    /// `MetaOp::InstallAggregateImage`.
    pub(in crate::data::executor) fn execute_cagg_copy_install(
        &mut self,
        task: &ExecutionTask,
        root: &str,
        image: &[u8],
    ) -> Response {
        let installed = self.install_cagg_copy_image(
            task.request.database_id,
            task.request.tenant_id,
            root,
            image,
        );
        match installed {
            Ok(()) => self.response_ok(task),
            Err(e) => self.response_error(task, e),
        }
    }

    /// Flush `root`, count every partition it holds as folded, and take the
    /// image's state. The image reaches disk before the apply answers.
    fn install_cagg_copy_image(
        &mut self,
        database_id: DatabaseId,
        tenant_id: TenantId,
        root: &str,
        bytes: &[u8],
    ) -> crate::Result<()> {
        let (db, tid) = (database_id.as_u64(), tenant_id.as_u64());
        if !self.owns_cagg_root(db, root) {
            return Ok(());
        }
        let image = decode_image(bytes)?;
        let now_ms = self.ingest_now_ms();
        self.ensure_ts_registry(tenant_id, database_id, root)?;
        self.flush_ts_collection(tenant_id, database_id, root, now_ms)?;
        let live = self.ts_live_partitions(database_id, tenant_id, root);
        self.continuous_agg_mgr
            .install_copy_image(db, tid, root, image, &live);
        if let Err(e) = self.persist_cagg_image(db, tid, root) {
            self.continuous_agg_mgr.mark_image_dirty(db, tid, root);
            crate::diag::continuous_aggregate_state_fault(&e, "image_write", db, tid, root);
            return Err(e);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::cagg_test_support::{
        COLLECTION, ROWS_A, ROWS_B, TENANT, flush, host_def, ingest, register, task_at, totals,
    };
    use crate::bridge::envelope::Status;
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::engine::timeseries::continuous_agg::RefreshPolicy;
    use nodedb_physical::physical_plan::RefreshCut;

    const SUM_A: i64 = 111;
    const SUM_AB: i64 = 11_111;

    fn apply_cut(core: &mut CoreLoop, refresh_id: u64) {
        let cut = RefreshCut {
            aggregate: "by_host".into(),
            incarnation: host_def("by_host", RefreshPolicy::Manual).incarnation,
            refresh_id,
            periodic: false,
        };
        let response = core.execute_cagg_refresh_cut(&task_at(9), COLLECTION, &cut);
        assert_eq!(response.status, Status::Ok);
        core.fold_pending_cagg_partitions(0);
    }

    /// The capture and the install of one root's image.
    fn copy_of(source: &mut CoreLoop) -> Vec<u8> {
        source
            .complete_cagg_root(0, TENANT, COLLECTION, 0)
            .expect("complete");
        let image = source
            .capture_cagg_image(0, TENANT, COLLECTION, 0)
            .expect("capture");
        source
            .cagg_copy_image(0, TENANT, COLLECTION, &image)
            .expect("copy image")
    }

    fn install(copy: &mut CoreLoop, image: &[u8]) {
        let response = copy.execute_cagg_copy_install(&task_at(10), COLLECTION, image);
        assert_eq!(response.status, Status::Ok);
    }

    /// A manual aggregate on a copy answers what its source answers, and its
    /// next cut takes the rows the source's next cut takes, once each.
    #[test]
    fn a_copy_of_a_manual_aggregate_matches_its_source_at_every_cut() {
        let source_dir = tempfile::tempdir().expect("tempdir");
        let (mut source, _tx, _rx) = make_core_with_dir(source_dir.path());
        register(&mut source, host_def("by_host", RefreshPolicy::Manual));
        source.fold_pending_cagg_partitions(0);
        assert_eq!(ingest(&mut source, ROWS_A, 1), Status::Ok);
        apply_cut(&mut source, 1);
        assert_eq!(ingest(&mut source, ROWS_B, 2), Status::Ok);
        let image = copy_of(&mut source);

        let copy_dir = tempfile::tempdir().expect("tempdir");
        let (mut copy, _copy_tx, _copy_rx) = make_core_with_dir(copy_dir.path());
        register(&mut copy, host_def("by_host", RefreshPolicy::Manual));
        copy.fold_pending_cagg_partitions(0);
        assert_eq!(ingest(&mut copy, ROWS_A, 1), Status::Ok);
        assert_eq!(ingest(&mut copy, ROWS_B, 2), Status::Ok);
        install(&mut copy, &image);
        assert_eq!(totals(&copy, "by_host"), totals(&source, "by_host"));
        assert_eq!(totals(&copy, "by_host").1, SUM_A);

        apply_cut(&mut copy, 2);
        assert_eq!(totals(&copy, "by_host").1, SUM_AB);
    }

    /// A flush-driven aggregate counts its source's memtable rows at the
    /// install. A later fold finds no partition left to take.
    #[test]
    fn a_copy_of_a_flush_driven_aggregate_counts_each_row_once() {
        let source_dir = tempfile::tempdir().expect("tempdir");
        let (mut source, _tx, _rx) = make_core_with_dir(source_dir.path());
        register(&mut source, host_def("by_host", RefreshPolicy::OnFlush));
        source.fold_pending_cagg_partitions(0);
        assert_eq!(ingest(&mut source, ROWS_A, 1), Status::Ok);
        flush(&mut source);
        assert_eq!(ingest(&mut source, ROWS_B, 2), Status::Ok);
        let image = copy_of(&mut source);

        let copy_dir = tempfile::tempdir().expect("tempdir");
        let (mut copy, _copy_tx, _copy_rx) = make_core_with_dir(copy_dir.path());
        register(&mut copy, host_def("by_host", RefreshPolicy::OnFlush));
        copy.fold_pending_cagg_partitions(0);
        assert_eq!(ingest(&mut copy, ROWS_A, 1), Status::Ok);
        assert_eq!(ingest(&mut copy, ROWS_B, 2), Status::Ok);
        install(&mut copy, &image);
        copy.fold_pending_cagg_partitions(0);
        assert_eq!(totals(&copy, "by_host").1, SUM_AB);
    }

    /// An install reaches an aggregate its catalog apply has not registered.
    #[test]
    fn an_install_registers_the_aggregates_its_image_holds() {
        let source_dir = tempfile::tempdir().expect("tempdir");
        let (mut source, _tx, _rx) = make_core_with_dir(source_dir.path());
        register(&mut source, host_def("by_host", RefreshPolicy::Manual));
        source.fold_pending_cagg_partitions(0);
        assert_eq!(ingest(&mut source, ROWS_A, 1), Status::Ok);
        apply_cut(&mut source, 1);
        let image = copy_of(&mut source);

        let copy_dir = tempfile::tempdir().expect("tempdir");
        let (mut copy, _copy_tx, _copy_rx) = make_core_with_dir(copy_dir.path());
        assert_eq!(ingest(&mut copy, ROWS_A, 1), Status::Ok);
        install(&mut copy, &image);
        assert_eq!(totals(&copy, "by_host").1, SUM_A);
    }
}
