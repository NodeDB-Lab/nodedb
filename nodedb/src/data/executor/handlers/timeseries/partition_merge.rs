// SPDX-License-Identifier: BUSL-1.1

//! Merge of the partitions of one timeseries interval, on the core that owns
//! the collection.
//!
//! The maintenance tick merges up to `merge_groups_per_tick` groups that
//! [`merge_groups`] selects; a forced compaction merges every group. A merge
//! reads its sources whole, combines their rows ([`merge_sources`]), stages
//! the merged partition, and commits it with the manifest of `merge_commit`.
//! The rest of the merge (install, continuous aggregate names, source
//! removal) is [`CoreLoop::complete_ts_merge`], which also rolls a committed
//! merge forward when an earlier attempt stopped part way.
//!
//! A group merges only when every continuous aggregate record rooted at the
//! collection names all of its sources or none
//! (`ContinuousAggregateManager::merge_admissible`). Otherwise it waits.

use std::path::Path;

use nodedb_types::timeseries::{PartitionMeta, PartitionState};
use nodedb_wal::crypto::WalEncryptionKey;

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::timeseries_checkpoint::stamp::{
    TsCollectionKey, TsReplayStamp, read_ts_stamp, write_ts_stamp,
};
use crate::engine::timeseries::columnar_memtable::ColumnarSchema;
use crate::engine::timeseries::columnar_segment::encrypt::encrypt_file;
use crate::engine::timeseries::columnar_segment::{ColumnarSegmentReader, ColumnarSegmentWriter};
use crate::engine::timeseries::merge::{
    MergeOutcome, MergeSource, merge_sources, schema_incompatibility,
};
use crate::engine::timeseries::partition_registry::PartitionEntry;

use super::merge_commit::{
    MergeManifest, commit_merge, finish_merge, install_merged, merged_partition_name,
    read_merge_manifest, remove_merge_staged, rename_in_image_file, staged_name,
};
use super::merge_select::{MergeBounds, merge_groups};
use super::partition_read::{partition_corrupt, read_ts_partition};

/// The read path name partition reads and corruption reports carry.
const SITE: &str = "partition_merge";
/// The file a partition's commit point is.
const META_FILE: &str = "partition.meta";

/// What one merge pass did.
#[derive(Debug, Default, Clone, Copy)]
pub(in crate::data::executor) struct MergePass {
    /// Groups merged.
    pub merged: usize,
    /// Collections skipped because their database is over its maintenance
    /// budget.
    pub deferred: usize,
}

impl CoreLoop {
    /// Merge the partition groups of every timeseries collection on this
    /// core. `force` merges every group, including intervals that have not
    /// ended, and ignores the per-tick and maintenance budgets.
    pub(in crate::data::executor) fn merge_ts_partitions(&mut self, force: bool) -> MergePass {
        let mut pass = MergePass::default();
        let mut remaining = if force {
            usize::MAX
        } else {
            self.ts_tuning.merge_groups_per_tick
        };
        if remaining == 0 {
            return pass;
        }
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
            .unwrap_or(0);
        let budget = self.maintenance.maintenance_budget.clone();
        let keys: Vec<TsCollectionKey> = self.ts_registries.keys().cloned().collect();
        for key in keys {
            if remaining == 0 {
                break;
            }
            // The lease lives across the merge, so its drop records the
            // time the merge took against the database's budget.
            let _lease = match (force, budget.as_ref()) {
                (false, Some(tracker)) => match tracker.try_acquire(key.0, 0.0) {
                    Some(lease) => Some(lease),
                    None => {
                        pass.deferred += 1;
                        continue;
                    }
                },
                _ => None,
            };
            match self.merge_ts_collection(&key, now_ms, force, remaining) {
                Ok(merged) => {
                    pass.merged += merged;
                    remaining = remaining.saturating_sub(merged);
                }
                Err(e) => tracing::warn!(
                    core = self.core_id,
                    collection = %key.2,
                    error = %e,
                    "timeseries partition merge failed; the next maintenance pass retries"
                ),
            }
        }
        pass
    }

    /// Merge up to `limit` groups of one collection. Returns the number
    /// merged.
    fn merge_ts_collection(
        &mut self,
        key: &TsCollectionKey,
        now_ms: i64,
        force: bool,
        limit: usize,
    ) -> crate::Result<usize> {
        let ts_dir =
            super::paths::ts_collection_dir(&self.data_dir, key.0.as_u64(), key.1.as_u64(), &key.2);
        let bounds = MergeBounds {
            now_ms,
            quiet_ms: self.ts_tuning.merge_quiet_ms,
            max_rows: self.ts_tuning.merge_max_rows,
            bitemporal: self.is_bitemporal(key.0.as_u64(), key.1.as_u64(), &key.2),
            force,
        };
        // Most ticks find nothing to merge and touch no file.
        let has_groups = self
            .ts_registries
            .get(key)
            .is_some_and(|registry| !merge_groups(registry, bounds).is_empty());
        if !has_groups {
            return Ok(0);
        }
        // A committed merge finishes before a new one commits: a collection
        // holds one manifest. A committed flush installs first: its
        // partitions are not listed yet.
        self.roll_forward_ts_merge(key)?;
        if super::flush_commit::has_uninstalled_flush(&ts_dir)? {
            return Ok(0);
        }
        let groups = match self.ts_registries.get(key) {
            Some(registry) => merge_groups(registry, bounds),
            None => return Ok(0),
        };
        let mut merged = 0;
        for group in groups {
            if merged >= limit {
                break;
            }
            if self.merge_ts_group(key, &ts_dir, &group)? {
                merged += 1;
            }
        }
        Ok(merged)
    }

    /// Merge one group. `Ok(false)` when the group waits: a continuous
    /// aggregate record names some of its sources and not others, or the
    /// sources do not combine.
    fn merge_ts_group(
        &mut self,
        key: &TsCollectionKey,
        ts_dir: &Path,
        group: &[PartitionEntry],
    ) -> crate::Result<bool> {
        let (database_id, tenant_id, root) = (key.0.as_u64(), key.1.as_u64(), key.2.as_str());
        if !self.owns_cagg_root(database_id, root) {
            return Ok(false);
        }
        self.load_cagg_images(database_id, tenant_id, root);
        let sources: Vec<String> = group.iter().map(|e| e.dir_name.clone()).collect();
        if !self
            .continuous_agg_mgr
            .merge_admissible(database_id, tenant_id, root, &sources)
        {
            return Ok(false);
        }
        let Some((manifest, meta)) = stage_merge(
            ts_dir,
            group,
            sources,
            self.segment_keks.ts_segment_kek.as_ref(),
        )?
        else {
            return Ok(false);
        };
        commit_merge(ts_dir, &manifest)?;
        self.complete_ts_merge(key, ts_dir, &manifest, Some(meta))?;
        tracing::debug!(
            core = self.core_id,
            collection = root,
            merged = %manifest.installed,
            sources = manifest.sources.len(),
            "timeseries partitions merged"
        );
        Ok(true)
    }

    /// Finish the committed merge of `key`, if one is pending. Called before
    /// any work that lists or changes the collection's partitions.
    pub(in crate::data::executor) fn roll_forward_ts_merge(
        &mut self,
        key: &TsCollectionKey,
    ) -> crate::Result<()> {
        let ts_dir =
            super::paths::ts_collection_dir(&self.data_dir, key.0.as_u64(), key.1.as_u64(), &key.2);
        let Some(manifest) = read_merge_manifest(&ts_dir)? else {
            return Ok(());
        };
        self.complete_ts_merge(key, &ts_dir, &manifest, None)
    }

    /// Install a committed merge, rename its sources in the continuous
    /// aggregate records, write `cagg.state`, then remove the sources and the
    /// manifest. Each step is idempotent, so a merge that stopped part way
    /// finishes here. `meta` is the merged partition's meta when the caller
    /// holds it; otherwise it is read from disk.
    fn complete_ts_merge(
        &mut self,
        key: &TsCollectionKey,
        ts_dir: &Path,
        manifest: &MergeManifest,
        meta: Option<PartitionMeta>,
    ) -> crate::Result<()> {
        let (database_id, tenant_id, root) = (key.0.as_u64(), key.1.as_u64(), key.2.as_str());
        install_merged(ts_dir, manifest)?;
        let listed = self.ts_registries.get(key).is_some_and(|registry| {
            registry
                .iter()
                .any(|(_, entry)| entry.dir_name == manifest.installed)
        });
        let owns = self.owns_cagg_root(database_id, root);
        if !listed {
            let installed_dir = ts_dir.join(&manifest.installed);
            if !installed_dir.is_dir() {
                return Err(partition_corrupt(
                    &installed_dir,
                    "directory",
                    SITE,
                    "a committed merge names a merged partition that is missing",
                ));
            }
            let meta = match meta {
                Some(meta) => meta,
                None => ColumnarSegmentReader::read_meta(
                    &installed_dir,
                    self.segment_keks.ts_segment_kek.as_ref(),
                )
                .map_err(|e| partition_corrupt(&installed_dir, "meta", SITE, e))?,
            };
            if let Some(registry) = self.ts_registries.get_mut(key) {
                registry.install_merge(
                    PartitionEntry {
                        meta,
                        dir_name: manifest.installed.clone(),
                    },
                    &manifest.sources,
                );
            }
            if owns {
                self.load_cagg_images(database_id, tenant_id, root);
                if !self
                    .continuous_agg_mgr
                    .is_quarantined(database_id, tenant_id, root)
                {
                    self.continuous_agg_mgr.rename_merged(
                        database_id,
                        tenant_id,
                        root,
                        &manifest.sources,
                        &manifest.installed,
                    )?;
                }
            }
        }
        if owns {
            if self
                .continuous_agg_mgr
                .is_quarantined(database_id, tenant_id, root)
            {
                // The image is kept for repair: it takes the merged name on
                // disk, as boot recovery would give it.
                rename_in_image_file(ts_dir, manifest)?;
            } else {
                self.persist_cagg_image(database_id, tenant_id, root)?;
            }
        }
        finish_merge(ts_dir, manifest)
    }
}

/// Read `group`, combine its rows and stage the merged partition under
/// `ts_dir`. Returns the manifest that commits it and its meta, or `None`
/// when the sources do not combine.
fn stage_merge(
    ts_dir: &Path,
    group: &[PartitionEntry],
    sources: Vec<String>,
    kek: Option<&WalEncryptionKey>,
) -> crate::Result<Option<(MergeManifest, PartitionMeta)>> {
    // The schemas decide whether the group combines. Reading them first
    // keeps a group that never combines from being read whole every tick.
    let schemas = group
        .iter()
        .map(|entry| {
            let dir = ts_dir.join(&entry.dir_name);
            ColumnarSegmentReader::read_schema(&dir, kek)
                .map_err(|e| partition_corrupt(&dir, "schema", SITE, e))
        })
        .collect::<crate::Result<Vec<ColumnarSchema>>>()?;
    let schema_refs: Vec<&ColumnarSchema> = schemas.iter().collect();
    if let Some(reason) = schema_incompatibility(&schema_refs) {
        tracing::debug!(
            collection = %ts_dir.display(),
            reason,
            "timeseries partitions left unmerged"
        );
        return Ok(None);
    }
    let mut inputs = Vec::with_capacity(group.len());
    let mut stamp: Option<TsReplayStamp> = None;
    for entry in group {
        let dir = ts_dir.join(&entry.dir_name);
        let partition = read_ts_partition(&dir, SITE, kek)?;
        let columns = partition
            .columns
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| partition_corrupt(&dir, "column", SITE, "a column did not read"))?;
        inputs.push(MergeSource {
            schema: partition.schema,
            columns,
            symbol_dicts: partition.sym_dicts,
            meta: entry.meta.clone(),
        });
        if let Some(source_stamp) = read_ts_stamp(&dir)? {
            stamp = Some(match stamp {
                Some(held) => held.union(&source_stamp),
                None => source_stamp,
            });
        }
    }
    let merged = match merge_sources(inputs)? {
        MergeOutcome::Merged(merged) => merged,
        MergeOutcome::Incompatible(reason) => {
            tracing::debug!(
                collection = %ts_dir.display(),
                reason,
                "timeseries partitions left unmerged"
            );
            return Ok(None);
        }
    };

    // No manifest is present (the caller rolled any forward), so every
    // staged merge is the remains of one that never committed.
    remove_merge_staged(ts_dir)?;
    let installed = merged_partition_name(
        ts_dir,
        merged.drain.min_ts,
        merged.drain.max_ts,
        merged.last_flushed_wal_lsn,
    )?;
    let staged = staged_name(&installed);
    let staged_dir = ts_dir.join(&staged);
    std::fs::create_dir_all(&staged_dir).map_err(|e| storage(&staged_dir, "create", &e))?;
    // The stamp lands before `partition.meta`, the staged partition's commit
    // point, as a flush writes it. It names every record the sources' stamps
    // name, so the collection stamp does not change.
    if let Some(stamp) = stamp.as_ref() {
        write_ts_stamp(&staged_dir, stamp)?;
    }
    let mut meta = ColumnarSegmentWriter::new(ts_dir)
        .write_partition(
            &staged,
            &merged.drain.view(),
            merged.interval_ms,
            merged.last_flushed_wal_lsn,
            kek,
        )
        .map_err(|e| storage(&staged_dir, "write", &e))?;
    meta.state = PartitionState::Merged;
    // Every source of a group shares one TRUNCATE end (`merge_select`), and
    // the merged partition keeps it.
    meta.ended_at = group.first().and_then(|entry| entry.meta.ended_at);
    write_meta(&staged_dir, &meta, kek)?;
    Ok(Some((
        MergeManifest {
            staged,
            installed,
            sources,
        },
        meta,
    )))
}

/// Write `meta` as `dir`'s `partition.meta`, encrypted with `kek` when one
/// is installed, as the partition writer writes it.
fn write_meta(
    dir: &Path,
    meta: &PartitionMeta,
    kek: Option<&WalEncryptionKey>,
) -> crate::Result<()> {
    let json = sonic_rs::to_vec(meta).map_err(|e| crate::Error::Serialization {
        format: "json".into(),
        detail: format!("serialize {}: {e}", dir.join(META_FILE).display()),
    })?;
    let bytes = match kek {
        Some(key) => {
            encrypt_file(key, &json).map_err(|e| storage(dir, "encrypt the meta of", &e))?
        }
        None => json,
    };
    nodedb_wal::segment::atomic_write_fsync(dir, META_FILE, &bytes)
        .map_err(|e| storage(&dir.join(META_FILE), "write", &e))
}

fn storage(path: &Path, action: &str, detail: &dyn std::fmt::Display) -> crate::Error {
    crate::Error::Storage {
        engine: "timeseries".into(),
        detail: format!("partition merge: {action} {}: {detail}", path.display()),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{RefreshCut, TimeseriesSchema};

    use super::super::cagg_test_support::{
        COLLECTION, ROWS_A, ROWS_B, TENANT, flush, host_def, ingest, register, task_at, totals,
    };
    use super::super::merge_commit::{MERGE_MANIFEST_FILE, MERGE_STAGED_PREFIX};
    use super::*;
    use crate::bridge::envelope::Status;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::engine::document::store::CollectionConfig;
    use crate::engine::timeseries::continuous_agg::RefreshPolicy;
    use crate::types::{DatabaseId, TenantId};

    const SUM_AB: i64 = 11_111;
    /// An hour boundary, in milliseconds.
    const HOUR_BASE_MS: i64 = 1_699_999_200_000;
    const HOUR_MS: i64 = 3_600_000;

    fn key() -> TsCollectionKey {
        (
            DatabaseId::DEFAULT,
            TenantId::new(TENANT),
            COLLECTION.to_string(),
        )
    }

    /// Declare a one-hour partition interval, so late rows for an hour
    /// flush into a second partition of that hour.
    fn declare_hourly(core: &mut CoreLoop) {
        let mut config = CollectionConfig::new(COLLECTION);
        config.timeseries = Some(Box::new(TimeseriesSchema {
            time_key: "timestamp".into(),
            columns: Vec::new(),
            partition_interval: "1h".into(),
            last_value_cache: false,
        }));
        core.doc_configs.insert(key(), config);
        core.ts_tuning.merge_quiet_ms = 0;
    }

    fn ts_dir(core: &CoreLoop) -> std::path::PathBuf {
        super::super::paths::ts_collection_dir(&core.data_dir, 0, TENANT, COLLECTION)
    }

    fn entries(core: &CoreLoop) -> Vec<PartitionEntry> {
        core.ts_registries
            .get(&key())
            .map(|registry| registry.iter().map(|(_, e)| e.clone()).collect())
            .unwrap_or_default()
    }

    fn stored_rows(core: &CoreLoop) -> u64 {
        entries(core).iter().map(|e| e.meta.row_count).sum()
    }

    /// `ROWS_A` at LSN 1 and `ROWS_B` at LSN 2, each flushed: two
    /// partitions of one hour.
    fn two_partitions_of_one_hour(core: &mut CoreLoop) {
        assert_eq!(ingest(core, ROWS_A, 1), Status::Ok);
        flush(core);
        assert_eq!(ingest(core, ROWS_B, 2), Status::Ok);
        flush(core);
        assert_eq!(entries(core).len(), 2);
    }

    fn cut(id: u64) -> RefreshCut {
        RefreshCut {
            aggregate: "by_host".into(),
            incarnation: host_def("by_host", RefreshPolicy::Manual).incarnation,
            refresh_id: id,
            periodic: false,
        }
    }

    /// No staged merge and no manifest are left in the collection directory.
    fn assert_settled(core: &CoreLoop) {
        let dir = ts_dir(core);
        assert!(!dir.join(MERGE_MANIFEST_FILE).exists(), "manifest left");
        let staged = std::fs::read_dir(&dir)
            .expect("read collection dir")
            .flatten()
            .filter(|e| {
                e.file_name()
                    .to_str()
                    .is_some_and(|n| n.starts_with(MERGE_STAGED_PREFIX))
            })
            .count();
        assert_eq!(staged, 0, "staged merge left");
    }

    #[test]
    fn late_rows_for_an_hour_merge_into_one_partition() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        declare_hourly(&mut core);
        two_partitions_of_one_hour(&mut core);

        let pass = core.merge_ts_partitions(false);
        assert_eq!(pass.merged, 1);
        let merged = entries(&core);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].meta.state, PartitionState::Merged);
        assert_eq!(stored_rows(&core), 5);
        assert_settled(&core);

        let merged_dir = ts_dir(&core).join(&merged[0].dir_name);
        let partition = read_ts_partition(&merged_dir, "test", None).expect("reads");
        let time_idx = partition.schema.timestamp_idx;
        let Some(Some(crate::engine::timeseries::columnar_memtable::ColumnData::Timestamp(times))) =
            partition.columns.get(time_idx)
        else {
            panic!("time column");
        };
        assert_eq!(times.len(), 5);
        assert!(
            times.windows(2).all(|w| w[0] <= w[1]),
            "rows sorted by time"
        );
        let stamp = read_ts_stamp(&merged_dir).expect("stamp").expect("present");
        assert!(stamp.rows.skips(1) && stamp.rows.skips(2), "{stamp:?}");

        assert_eq!(core.merge_ts_partitions(false).merged, 0, "nothing left");
    }

    #[test]
    fn an_aggregate_is_unchanged_by_a_merge_and_a_restart() {
        let dir = tempfile::tempdir().expect("tempdir");
        {
            let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
            declare_hourly(&mut core);
            register(&mut core, host_def("by_host", RefreshPolicy::OnFlush));
            core.fold_pending_cagg_partitions(0);
            two_partitions_of_one_hour(&mut core);
            core.fold_pending_cagg_partitions(0);
            assert_eq!(totals(&core, "by_host"), (5, SUM_AB));

            assert_eq!(core.merge_ts_partitions(false).merged, 1);
            core.fold_pending_cagg_partitions(0);
            assert_eq!(
                totals(&core, "by_host"),
                (5, SUM_AB),
                "the merged partition is not folded again"
            );
        }
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        declare_hourly(&mut core);
        core.load_ts_registries().expect("load registries");
        register(&mut core, host_def("by_host", RefreshPolicy::OnFlush));
        core.fold_pending_cagg_partitions(0);
        core.fold_pending_cagg_partitions(0);
        assert_eq!(totals(&core, "by_host"), (5, SUM_AB));
        let reloaded = entries(&core);
        assert_eq!(reloaded.len(), 1);
        assert_eq!(reloaded[0].meta.state, PartitionState::Merged);
    }

    /// A manual aggregate folded the first partition at a cut and not the
    /// late one: the group waits until the next cut folds it.
    #[test]
    fn a_group_with_mixed_folded_state_waits_for_the_next_cut() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        declare_hourly(&mut core);
        register(&mut core, host_def("by_host", RefreshPolicy::Manual));
        assert_eq!(ingest(&mut core, ROWS_A, 1), Status::Ok);
        flush(&mut core);
        let resp = core.execute_cagg_refresh_cut(&task_at(3), COLLECTION, &cut(1));
        assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
        core.fold_pending_cagg_partitions(0);
        assert_eq!(totals(&core, "by_host"), (3, 111));
        assert_eq!(ingest(&mut core, ROWS_B, 4), Status::Ok);
        flush(&mut core);

        assert_eq!(
            core.merge_ts_partitions(false).merged,
            0,
            "mixed state waits"
        );
        assert_eq!(entries(&core).len(), 2);

        let resp = core.execute_cagg_refresh_cut(&task_at(5), COLLECTION, &cut(2));
        assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
        core.fold_pending_cagg_partitions(0);
        assert_eq!(core.merge_ts_partitions(false).merged, 1);
        core.fold_pending_cagg_partitions(0);
        assert_eq!(totals(&core, "by_host"), (5, SUM_AB));
    }

    /// Both sources pending in one cut merge, and the merged partition is
    /// what the cut folds.
    #[test]
    fn sources_pending_in_one_cut_merge_and_fold_once() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        declare_hourly(&mut core);
        register(&mut core, host_def("by_host", RefreshPolicy::Manual));
        two_partitions_of_one_hour(&mut core);
        let resp = core.execute_cagg_refresh_cut(&task_at(3), COLLECTION, &cut(1));
        assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);

        assert_eq!(core.merge_ts_partitions(false).merged, 1);
        core.fold_pending_cagg_partitions(0);
        core.fold_pending_cagg_partitions(0);
        assert_eq!(totals(&core, "by_host"), (5, SUM_AB));
    }

    /// A crash after each step of a merge: the reloaded collection holds
    /// every row once, its stamp still names both records, and the aggregate
    /// is not folded twice.
    #[test]
    fn a_crash_at_each_step_keeps_every_row_once() {
        for step in 0..=4 {
            let dir = tempfile::tempdir().expect("tempdir");
            let installed;
            {
                let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
                declare_hourly(&mut core);
                register(&mut core, host_def("by_host", RefreshPolicy::OnFlush));
                core.fold_pending_cagg_partitions(0);
                two_partitions_of_one_hour(&mut core);
                core.fold_pending_cagg_partitions(0);
                core.persist_dirty_cagg_images();

                let group = entries(&core);
                let sources: Vec<String> = group.iter().map(|e| e.dir_name.clone()).collect();
                let ts = ts_dir(&core);
                let (manifest, _meta) = stage_merge(&ts, &group, sources, None)
                    .expect("stage")
                    .expect("combines");
                installed = manifest.installed.clone();
                if step >= 1 {
                    commit_merge(&ts, &manifest).expect("commit");
                }
                if step >= 2 {
                    install_merged(&ts, &manifest).expect("install");
                }
                if step >= 3 {
                    rename_in_image_file(&ts, &manifest).expect("rename");
                }
                if step >= 4 {
                    for source in &manifest.sources {
                        std::fs::remove_dir_all(ts.join(source)).expect("remove source");
                    }
                }
            }
            let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
            declare_hourly(&mut core);
            core.load_ts_registries().expect("load registries");
            register(&mut core, host_def("by_host", RefreshPolicy::OnFlush));
            core.fold_pending_cagg_partitions(0);
            core.fold_pending_cagg_partitions(0);

            assert_eq!(stored_rows(&core), 5, "step {step}");
            let names: Vec<String> = entries(&core).into_iter().map(|e| e.dir_name).collect();
            if step == 0 {
                assert_eq!(names.len(), 2, "an uncommitted merge leaves its sources");
            } else {
                assert_eq!(names, vec![installed.clone()], "step {step}");
            }
            assert_eq!(totals(&core, "by_host"), (5, SUM_AB), "step {step}");
            assert!(core.ts_rows_named(&key(), 1) && core.ts_rows_named(&key(), 2));
            assert_settled(&core);
        }
    }

    #[test]
    fn an_encrypted_group_merges_with_the_segment_key() {
        let kek = WalEncryptionKey::from_bytes(&[0x42u8; 32]).expect("key");
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        core.set_ts_segment_kek(kek.clone());
        declare_hourly(&mut core);
        two_partitions_of_one_hour(&mut core);

        assert_eq!(core.merge_ts_partitions(false).merged, 1);
        let merged = entries(&core);
        let merged_dir = ts_dir(&core).join(&merged[0].dir_name);
        let partition = read_ts_partition(&merged_dir, "test", Some(&kek)).expect("reads with key");
        assert_eq!(partition.columns.len(), partition.schema.columns.len());
        assert!(read_ts_partition(&merged_dir, "test", None).is_err());
        let meta = ColumnarSegmentReader::read_meta(&merged_dir, Some(&kek)).expect("meta");
        assert_eq!(meta.state, PartitionState::Merged);
        assert_eq!(meta.row_count, 5);
    }

    /// One group merges per call under a budget of one.
    #[test]
    fn the_per_tick_budget_bounds_merges() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        declare_hourly(&mut core);
        core.ts_tuning.merge_groups_per_tick = 1;
        let mut lsn = 1;
        for hour in 0..3 {
            for offset in [0, 1] {
                let ms = HOUR_BASE_MS + hour * HOUR_MS + offset * 1_000;
                let line = format!("metrics,host=a value=1i {}\n", ms * 1_000_000);
                assert_eq!(ingest(&mut core, &line, lsn), Status::Ok);
                lsn += 1;
                flush(&mut core);
            }
        }
        assert_eq!(entries(&core).len(), 6);
        for expected_left in [5, 4, 3] {
            assert_eq!(core.merge_ts_partitions(false).merged, 1);
            assert_eq!(entries(&core).len(), expected_left);
        }
        assert_eq!(core.merge_ts_partitions(false).merged, 0);
        assert_eq!(stored_rows(&core), 6);
    }
}
