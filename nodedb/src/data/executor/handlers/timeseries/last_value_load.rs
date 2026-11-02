// SPDX-License-Identifier: BUSL-1.1

//! Keeping a collection's last-value cache in step with its flag.
//!
//! `SET LAST_VALUE_CACHE` travels on the collection's registration. A
//! registration that turns the flag on builds the cache from the stored
//! rows: every current partition, then the memtable. Every ingest after
//! that updates it. A registration that turns the flag off drops it.
//!
//! The cache lives in memory only, and the rows it is built from are
//! durable. Boot registers every collection again, so a restart rebuilds
//! each enabled cache. A registration that lands before restart replay
//! builds from the partitions, and the replayed ingests update it. A Raft
//! snapshot install replaces the stored rows, so it rebuilds every enabled
//! cache of the tenant it installs.

use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::timeseries_checkpoint::stamp::TsCollectionKey;
use crate::engine::timeseries::last_value_rebuild::LastValueScan;
use crate::types::{DatabaseId, TenantId};

use super::partition_read::{partition_corrupt, read_ts_partition};
use super::paths::ts_collection_dir;

/// Read path named in partition corruption reports.
const REBUILD_SITE: &str = "last_value_rebuild";

impl CoreLoop {
    /// Whether the registration of `key` turns its last-value cache on.
    pub(in crate::data::executor) fn last_value_cache_enabled(
        &self,
        key: &TsCollectionKey,
    ) -> bool {
        self.doc_configs
            .get(key)
            .and_then(|config| config.timeseries.as_deref())
            .is_some_and(|schema| schema.last_value_cache)
    }

    /// Build the cache of `key` when `enabled` and none is held, and drop
    /// it when not `enabled`.
    pub(in crate::data::executor) fn sync_last_value_cache(
        &mut self,
        key: &TsCollectionKey,
        enabled: bool,
    ) -> crate::Result<()> {
        if !enabled {
            self.ts_last_value_caches.remove(key);
            return Ok(());
        }
        if self.ts_last_value_caches.contains_key(key) {
            return Ok(());
        }
        self.rebuild_last_value_cache(key)
    }

    /// Replace the cache of `key` with one built from its stored rows.
    pub(in crate::data::executor) fn rebuild_last_value_cache(
        &mut self,
        key: &TsCollectionKey,
    ) -> crate::Result<()> {
        let (database_id, tid, collection) = (key.0, key.1, key.2.as_str());
        self.ensure_ts_registry(tid, database_id, collection)?;
        let base = ts_collection_dir(
            &self.data_dir,
            database_id.as_u64(),
            tid.as_u64(),
            collection,
        );
        let dirs: Vec<String> = self
            .ts_registries
            .get(key)
            .map(|registry| {
                registry
                    .current_partitions()
                    .map(|entry| entry.dir_name.clone())
                    .collect()
            })
            .unwrap_or_default();

        let mut scan = LastValueScan::new();
        for dir in dirs {
            let part_dir = base.join(&dir);
            let part = read_ts_partition(
                &part_dir,
                REBUILD_SITE,
                self.segment_keks.ts_segment_kek.as_ref(),
            )?;
            let columns: Vec<_> = part
                .columns
                .into_iter()
                .collect::<Option<Vec<_>>>()
                .ok_or_else(|| {
                    partition_corrupt(
                        &part_dir,
                        "column",
                        REBUILD_SITE,
                        "a schema column did not read",
                    )
                })?;
            let row_count = columns
                .get(part.schema.timestamp_idx)
                .map_or(0, |column| column.len());
            scan.fold(&dir, &part.schema, &columns, row_count)?;
        }
        if let Some(memtable) = self.columnar_memtables.get(key) {
            let view = memtable.flush_view();
            scan.fold(
                "the memtable",
                view.schema,
                view.columns,
                usize::try_from(view.row_count).unwrap_or(usize::MAX),
            )?;
        }
        let now_ms = self.ingest_now_ms();
        let at = self.ts_batch_ordinal(super::TimeseriesApplyMode::Immediate, now_ms);
        let mut cache = scan.into_cache(at);
        cache.set_horizon(self.maintenance.version_purge.safe_horizon);
        self.ts_last_value_caches.insert(key.clone(), cache);
        Ok(())
    }

    /// Rebuild every enabled cache of `tid` in `database_id`: a snapshot
    /// install replaced the rows they were built from.
    pub(in crate::data::executor) fn rebuild_last_value_caches_of(
        &mut self,
        database_id: DatabaseId,
        tid: TenantId,
    ) -> crate::Result<()> {
        let enabled: Vec<TsCollectionKey> = self
            .doc_configs
            .iter()
            .filter(|(key, config)| {
                key.0 == database_id
                    && key.1 == tid
                    && config
                        .timeseries
                        .as_deref()
                        .is_some_and(|schema| schema.last_value_cache)
            })
            .map(|(key, _)| key.clone())
            .collect();
        for key in enabled {
            self.rebuild_last_value_cache(&key)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::cagg_test_support::{COLLECTION, TENANT, flush, ingest};
    use crate::bridge::envelope::Status;
    use crate::data::executor::core_loop::CoreLoop;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::types::{DatabaseId, TenantId};

    fn key() -> super::TsCollectionKey {
        (
            DatabaseId::DEFAULT,
            TenantId::new(TENANT),
            COLLECTION.to_string(),
        )
    }

    /// `(ts, value)` of every cached series, sorted.
    fn cached(core: &CoreLoop) -> Vec<(i64, f64)> {
        let mut values: Vec<(i64, f64)> = core
            .ts_last_value_caches
            .get(&key())
            .expect("cache")
            .all()
            .map(|(_, entry)| (entry.ts, entry.value))
            .collect();
        values.sort_by_key(|entry| entry.0);
        values
    }

    /// A cache built from flushed and memtable rows holds what live
    /// updates leave, and a disabled flag drops it.
    #[test]
    fn a_rebuilt_cache_matches_the_live_one() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        core.ts_last_value_caches.insert(
            key(),
            crate::engine::timeseries::last_value_cache::LastValueCache::new(),
        );
        let rows = "metrics,host=a value=1.5 1700000000000000000\n\
                    metrics,host=b value=7.5 1700000001000000000\n";
        assert_eq!(ingest(&mut core, rows, 1), Status::Ok);
        flush(&mut core);
        let later = "metrics,host=a value=2.5 1700000002000000000\n";
        assert_eq!(ingest(&mut core, later, 2), Status::Ok);
        let live = cached(&core);
        assert_eq!(
            live,
            vec![(1_700_000_001_000, 7.5), (1_700_000_002_000, 2.5)]
        );

        core.rebuild_last_value_cache(&key()).expect("rebuild");
        assert_eq!(cached(&core), live);

        core.sync_last_value_cache(&key(), false).expect("disable");
        assert!(!core.ts_last_value_caches.contains_key(&key()));
        core.sync_last_value_cache(&key(), true).expect("enable");
        assert_eq!(cached(&core), live);
    }

    /// A flush writes the series catalog, and a core that reloads the
    /// collection gives each key the id its flushed rows carry.
    #[test]
    fn a_flush_persists_the_ids_a_reload_resolves() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        let rows = "metrics,host=a value=1.5 1700000000000000000
";
        assert_eq!(ingest(&mut core, rows, 1), Status::Ok);
        flush(&mut core);
        let held = core
            .ts_series_catalogs
            .get(&key())
            .expect("catalog")
            .clone();
        let ts_dir = super::ts_collection_dir(dir.path(), 0, TENANT, COLLECTION);
        let stored =
            crate::data::executor::timeseries_checkpoint::series_catalog::read_ts_series_catalog(
                &ts_dir,
            )
            .expect("read")
            .expect("written by the flush");
        assert_eq!(stored, held);

        drop(core);
        let (mut reopened, _tx2, _rx2) = make_core_with_dir(dir.path());
        reopened
            .ensure_ts_registry(TenantId::new(TENANT), DatabaseId::DEFAULT, COLLECTION)
            .expect("load");
        assert_eq!(reopened.ts_series_catalogs.get(&key()), Some(&held));
    }
}
