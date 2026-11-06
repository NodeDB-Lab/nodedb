// SPDX-License-Identifier: BUSL-1.1

//! `MetaOp::EnforceTimeseriesRetention`: drop a timeseries collection's
//! expired partitions.

use std::collections::HashSet;

use nodedb_physical::physical_plan::AggregateIncarnation;

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

use super::handlers::now_ms;

impl CoreLoop {
    /// `MetaOp::EnforceTimeseriesRetention`: drop partitions older than
    /// `max_age_ms`. Bitemporal collections use `max_system_ts` as the
    /// retention axis; non-bitemporal partitions fall through to `max_ts`.
    ///
    /// `aggregates` lists the continuous aggregates the catalog roots at
    /// `collection`. An expiring partition folds into each of them before it
    /// is removed, so the pass is refused, and nothing removed, while one of
    /// them is not registered on this core yet. The enforcement loop retries
    /// on its next pass.
    pub(in crate::data::executor::dispatch) fn meta_enforce_timeseries_retention(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        max_age_ms: i64,
        aggregates: &[AggregateIncarnation],
    ) -> Response {
        let unregistered = aggregates.iter().find(|agg| {
            !self.continuous_agg_mgr.is_registered_at(
                task.request.database_id.as_u64(),
                task.request.tenant_id.as_u64(),
                &agg.name,
                agg.incarnation,
            )
        });
        if let Some(agg) = unregistered {
            return self.response_error(
                task,
                crate::Error::ObjectNotInPrerequisiteState {
                    object: format!("continuous aggregate '{}'", agg.name),
                    detail: format!(
                        "retention of '{collection}' waits until continuous aggregate '{}' \
                         registers on this core; the next pass retries",
                        agg.name
                    ),
                },
            );
        }
        // A committed merge finishes first: until it does, its sources are
        // still on disk beside the merged partition.
        let merge_key = (
            task.request.database_id,
            task.request.tenant_id,
            collection.to_string(),
        );
        if let Err(e) = self.roll_forward_ts_merge(&merge_key) {
            return self.response_error(task, e);
        }
        let now_ms = now_ms();
        let cutoff = now_ms - max_age_ms;
        let mut deleted = 0usize;
        let ts_base = crate::data::executor::handlers::timeseries::paths::ts_collection_dir(
            &self.data_dir,
            task.request.database_id.as_u64(),
            task.request.tenant_id.as_u64(),
            collection,
        );

        let bitemporal = self.is_bitemporal(
            task.request.database_id.as_u64(),
            task.request.tenant_id.as_u64(),
            collection,
        );

        let ts_key = (
            task.request.database_id,
            task.request.tenant_id,
            collection.to_string(),
        );
        let expired: Vec<(
            crate::engine::timeseries::partition_registry::PartitionKey,
            String,
            i64,
        )> = self
            .ts_registries
            .get(&ts_key)
            .map(|registry| {
                registry
                    .iter()
                    .filter(|(_, e)| {
                        let axis_ts = if bitemporal && e.meta.max_system_ts > 0 {
                            nodedb_types::ordinal_to_ms(e.meta.max_system_ts)
                        } else {
                            e.meta.max_ts
                        };
                        axis_ts < cutoff
                            && e.meta.state != nodedb_types::timeseries::PartitionState::Deleted
                    })
                    .map(|(key, e)| (key.clone(), e.dir_name.clone(), e.meta.max_ts))
                    .collect()
            })
            .unwrap_or_default();
        // The collection directory's stamp must name every record of the
        // partitions removed below, or replay re-appends the ones the WAL
        // still holds. Nothing is removed until it does.
        if !expired.is_empty()
            && let Err(e) = self.persist_ts_collection_stamp(&ts_key)
        {
            return self.response_error(task, e);
        }
        // Every continuous aggregate rooted here holds the expiring
        // partitions, and its image says so on disk, before any is removed.
        // The image is then the only copy of those rows, so nothing is
        // removed until it is written.
        let database_id = task.request.database_id.as_u64();
        let tenant_id = task.request.tenant_id.as_u64();
        if !expired.is_empty() {
            let expiring: HashSet<String> = expired.iter().map(|(_, dir, _)| dir.clone()).collect();
            let dropped_through = expired
                .iter()
                .map(|(_, _, max_ts)| *max_ts)
                .max()
                .unwrap_or(i64::MIN);
            if let Err(e) = self.fold_expiring_partitions(
                database_id,
                tenant_id,
                collection,
                &expiring,
                dropped_through,
                now_ms,
            ) {
                return self.response_error(task, e);
            }
        }
        let mut removed: Vec<String> = Vec::new();
        if let Some(registry) = self.ts_registries.get_mut(&ts_key) {
            for (partition_key, dir_name, _) in expired {
                let partition_path = ts_base.join(&dir_name);
                if partition_path.exists()
                    && let Err(e) = std::fs::remove_dir_all(&partition_path)
                {
                    tracing::warn!(
                        path = %partition_path.display(),
                        error = %e,
                        "failed to delete expired partition"
                    );
                    continue;
                }
                registry.mark_deleted(&partition_key);
                removed.push(dir_name);
                deleted += 1;
            }

            if deleted > 0 {
                tracing::info!(
                    collection,
                    deleted,
                    max_age_ms,
                    "retention enforcement complete"
                );
            }
        }

        if !removed.is_empty() {
            self.release_ts_partitions(
                task.request.database_id,
                task.request.tenant_id,
                collection,
                &removed,
            );
            self.continuous_agg_mgr
                .forget_partitions(database_id, tenant_id, collection, &removed);
            self.persist_dirty_cagg_images();
        }

        // A last-value read below this ordinal finds the evicted series gone
        // and refuses rather than answer without them.
        let evict_ordinal = self.ts_batch_ordinal(
            crate::data::executor::handlers::timeseries::TimeseriesApplyMode::Immediate,
            now_ms,
        );
        if let Some(lvc) = self.ts_last_value_caches.get_mut(&ts_key) {
            let evicted = lvc.evict_older_than(cutoff, evict_ordinal);
            if !evicted.is_empty() {
                tracing::debug!(
                    collection,
                    evicted = evicted.len(),
                    "evicted stale LVC entries"
                );
                // The catalog keeps the evicted series. This pass runs at a
                // time local to each node, and the id a key resolves to
                // depends on every key the catalog holds, so forgetting one
                // here would let replicas give a later key different ids.
                // Only a replicated drop or purge removes a catalog.
            }
        }

        let payload = (deleted as u64).to_le_bytes().to_vec();
        self.response_with_payload(task, payload)
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::MetaOp;

    use crate::bridge::envelope::{PhysicalPlan, Status};
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::data::executor::handlers::timeseries::cagg_test_support::{
        COLLECTION, ROWS_A, ROWS_B, TENANT, flush, host_def, image_path, ingest, register, task_at,
        totals,
    };
    use crate::data::executor::task::ExecutionTask;
    use crate::engine::timeseries::continuous_agg::RefreshPolicy;
    use crate::types::{DatabaseId, TenantId, VShardId};

    fn retention_task() -> ExecutionTask {
        CoreLoop::replay_task(
            TenantId::new(TENANT),
            DatabaseId::DEFAULT,
            VShardId::new(0),
            PhysicalPlan::Meta(MetaOp::EnforceTimeseriesRetention {
                collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
                max_age_ms: 1,
                aggregates: Vec::new(),
            }),
            None,
        )
    }

    /// Live partitions of the collection.
    fn partitions(core: &CoreLoop) -> usize {
        core.ts_registries
            .get(&(
                DatabaseId::DEFAULT,
                TenantId::new(TENANT),
                COLLECTION.to_string(),
            ))
            .map(|r| {
                r.iter()
                    .filter(|(_, e)| {
                        e.meta.state != nodedb_types::timeseries::PartitionState::Deleted
                    })
                    .count()
            })
            .unwrap_or(0)
    }

    /// Evicting stale last values keeps every series id: a key ingested
    /// again resolves to the id it had.
    #[test]
    fn eviction_keeps_the_series_ids() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let key = (
            DatabaseId::DEFAULT,
            TenantId::new(TENANT),
            COLLECTION.to_string(),
        );
        core.ts_last_value_caches.insert(
            key.clone(),
            crate::engine::timeseries::last_value_cache::LastValueCache::new(),
        );
        assert_eq!(ingest(&mut core, ROWS_A, 1), Status::Ok);
        let held = core.ts_series_catalogs.get(&key).expect("catalog").clone();
        let resp = core.meta_enforce_timeseries_retention(&retention_task(), COLLECTION, 1, &[]);
        assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
        assert!(
            core.ts_last_value_caches
                .get(&key)
                .is_some_and(|cache| cache.is_empty()),
            "the stale values left the cache"
        );
        assert_eq!(core.ts_series_catalogs.get(&key), Some(&held));
    }

    #[test]
    fn history_survives_raw_retention_and_restart() {
        let dir = tempfile::tempdir().expect("tempdir");
        {
            let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
            for (i, policy) in [RefreshPolicy::OnFlush, RefreshPolicy::Manual]
                .into_iter()
                .enumerate()
            {
                register(&mut core, host_def(&format!("agg_{i}"), policy));
            }
            core.fold_pending_cagg_partitions(0);
            assert_eq!(ingest(&mut core, ROWS_A, 1), Status::Ok);
            flush(&mut core);
            let resp =
                core.meta_enforce_timeseries_retention(&retention_task(), COLLECTION, 1, &[]);
            assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
            assert_eq!(partitions(&core), 0, "retention removed the raw rows");
            // A Manual aggregate parks the expiring rows for its next cut.
            assert_eq!(totals(&core, "agg_0"), (3, 111));
            assert_eq!(totals(&core, "agg_1"), (0, 0));
        }
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        core.load_ts_registries().expect("load registries");
        for (i, policy) in [RefreshPolicy::OnFlush, RefreshPolicy::Manual]
            .into_iter()
            .enumerate()
        {
            register(&mut core, host_def(&format!("agg_{i}"), policy));
        }
        core.fold_pending_cagg_partitions(0);
        assert_eq!(totals(&core, "agg_0"), (3, 111));
        assert_eq!(totals(&core, "agg_1"), (0, 0));
        let cut = nodedb_physical::physical_plan::RefreshCut {
            aggregate: "agg_1".into(),
            incarnation: host_def("agg_1", RefreshPolicy::Manual).incarnation,
            refresh_id: 1,
            periodic: false,
        };
        let resp = core.execute_cagg_refresh_cut(&task_at(9), COLLECTION, &cut);
        assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
        assert_eq!(
            totals(&core, "agg_1"),
            (3, 111),
            "the parked rows merge at the cut"
        );
    }

    #[test]
    fn retention_refuses_when_image_write_fails() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        register(&mut core, host_def("by_host", RefreshPolicy::OnFlush));
        core.fold_pending_cagg_partitions(0);
        assert_eq!(ingest(&mut core, ROWS_A, 1), Status::Ok);
        flush(&mut core);
        // The registered aggregate's image is on disk. A directory in its
        // place makes the next write's rename fail.
        let image = image_path(dir.path());
        assert!(image.is_file(), "the image of the registered aggregate");
        std::fs::remove_file(&image).expect("remove the image");
        std::fs::create_dir_all(&image).expect("block the image path");

        let resp = core.meta_enforce_timeseries_retention(&retention_task(), COLLECTION, 1, &[]);
        assert_ne!(resp.status, Status::Ok);
        assert_eq!(partitions(&core), 1, "no partition is removed");
        assert_eq!(totals(&core, "by_host"), (3, 111));
    }

    /// The catalog roots an aggregate here that this core has not
    /// registered yet: retention removes nothing until it registers, so the
    /// aggregate never misses the expiring rows.
    #[test]
    fn retention_waits_for_every_catalog_aggregate_to_register() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        assert_eq!(ingest(&mut core, ROWS_A, 1), Status::Ok);
        flush(&mut core);
        let expected = [nodedb_physical::physical_plan::AggregateIncarnation {
            name: "by_host".into(),
            incarnation: host_def("by_host", RefreshPolicy::OnFlush).incarnation,
        }];

        let resp =
            core.meta_enforce_timeseries_retention(&retention_task(), COLLECTION, 1, &expected);
        assert_ne!(
            resp.status,
            Status::Ok,
            "the aggregate is not registered yet"
        );
        assert_eq!(partitions(&core), 1, "no partition is removed");

        register(&mut core, host_def("by_host", RefreshPolicy::OnFlush));
        let resp =
            core.meta_enforce_timeseries_retention(&retention_task(), COLLECTION, 1, &expected);
        assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
        assert_eq!(partitions(&core), 0);
        assert_eq!(
            totals(&core, "by_host"),
            (3, 111),
            "the expiring rows folded first"
        );
    }

    /// A merged partition expires like any other. The aggregate folded its
    /// sources and keeps their rows.
    #[test]
    fn retention_after_a_merge_keeps_the_aggregate() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let mut config = crate::engine::document::store::CollectionConfig::new(COLLECTION);
        config.timeseries = Some(Box::new(nodedb_physical::physical_plan::TimeseriesSchema {
            time_key: "timestamp".into(),
            columns: Vec::new(),
            partition_interval: "1h".into(),
            last_value_cache: false,
        }));
        core.doc_configs.insert(
            (
                DatabaseId::DEFAULT,
                TenantId::new(TENANT),
                COLLECTION.to_string(),
            ),
            config,
        );
        core.ts_tuning.merge_quiet_ms = 0;
        register(&mut core, host_def("by_host", RefreshPolicy::OnFlush));
        core.fold_pending_cagg_partitions(0);
        assert_eq!(ingest(&mut core, ROWS_A, 1), Status::Ok);
        flush(&mut core);
        assert_eq!(ingest(&mut core, ROWS_B, 2), Status::Ok);
        flush(&mut core);
        assert_eq!(core.merge_ts_partitions(false).merged, 1);
        assert_eq!(partitions(&core), 1);

        let resp = core.meta_enforce_timeseries_retention(&retention_task(), COLLECTION, 1, &[]);
        assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
        assert_eq!(partitions(&core), 0);
        assert_eq!(totals(&core, "by_host"), (5, 11_111));
    }

    /// The bitemporal audit purge removes timeseries partitions too, so it
    /// waits for the same aggregates as raw retention.
    #[test]
    fn temporal_purge_waits_for_every_catalog_aggregate_to_register() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        assert_eq!(ingest(&mut core, ROWS_A, 1), Status::Ok);
        flush(&mut core);
        let expected = vec![nodedb_physical::physical_plan::AggregateIncarnation {
            name: "by_host".into(),
            incarnation: host_def("by_host", RefreshPolicy::OnFlush).incarnation,
        }];
        // A cutoff past every partition: all of them expire.
        let cutoff_system_ms = i64::MAX;
        let task = CoreLoop::replay_task(
            TenantId::new(TENANT),
            DatabaseId::DEFAULT,
            VShardId::new(0),
            PhysicalPlan::Meta(MetaOp::TemporalPurgeColumnar {
                tenant_id: TENANT,
                collection: nodedb_types::QualifiedCollection::new(DatabaseId::DEFAULT, COLLECTION),
                cutoff_system_ms,
                aggregates: expected.clone(),
            }),
            None,
        );

        let resp = core.meta_temporal_purge_columnar(
            &task,
            TENANT,
            COLLECTION,
            cutoff_system_ms,
            &expected,
        );
        assert_ne!(
            resp.status,
            Status::Ok,
            "the aggregate is not registered yet"
        );
        assert_eq!(partitions(&core), 1, "no partition is removed");

        register(&mut core, host_def("by_host", RefreshPolicy::OnFlush));
        let resp = core.meta_temporal_purge_columnar(
            &task,
            TENANT,
            COLLECTION,
            cutoff_system_ms,
            &expected,
        );
        assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
        assert_eq!(partitions(&core), 0);
        assert_eq!(
            totals(&core, "by_host"),
            (3, 111),
            "the expiring rows folded first"
        );
    }
}
