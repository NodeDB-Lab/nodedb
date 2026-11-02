// SPDX-License-Identifier: BUSL-1.1

//! Purge of the partitions a timeseries `TRUNCATE` ended.
//!
//! A partition a TRUNCATE ended at `E` holds rows only a read below `E`
//! sees. A purge at horizon `H >= E` removes it: no read at or above the
//! horizon sees a row of it. A partition ended above the horizon, and every
//! current partition, stays.
//!
//! The end lives on the partition itself, so removing the partition removes
//! the end with it. No separate record of the truncate is left to hide
//! anything.

use crate::data::executor::core_loop::CoreLoop;
use crate::types::{DatabaseId, TenantId};

use super::truncate::remove_dir_tree;

impl CoreLoop {
    /// Remove every partition of the timeseries collection `collection`
    /// that a TRUNCATE ended at or below the ordinal `horizon`. Returns the
    /// number of partitions removed. A collection with no partition on
    /// this core purges nothing.
    ///
    /// The collection stamp lands on disk first, so it names the records of
    /// every removed partition and replay never appends them again. `Err`
    /// when the stamp or a partition directory does not write or remove;
    /// the partitions removed before it stay removed.
    ///
    /// No WAL record is appended here: the caller owns the audit record that
    /// covers the batch.
    pub(in crate::data::executor) fn purge_timeseries_truncated_below(
        &mut self,
        database_id: DatabaseId,
        tid: TenantId,
        collection: &str,
        horizon: i64,
    ) -> crate::Result<usize> {
        self.ensure_ts_registry(tid, database_id, collection)?;
        let key = (database_id, tid, collection.to_string());
        let purgeable: Vec<(
            crate::engine::timeseries::partition_registry::PartitionKey,
            String,
        )> = self
            .ts_registries
            .get(&key)
            .map(|registry| {
                registry
                    .iter()
                    .filter(|(_, entry)| {
                        entry.meta.state != nodedb_types::timeseries::PartitionState::Deleted
                            && entry.meta.ended_at.is_some_and(|end| end <= horizon)
                    })
                    .map(|(partition_key, entry)| (partition_key.clone(), entry.dir_name.clone()))
                    .collect()
            })
            .unwrap_or_default();
        if purgeable.is_empty() {
            return Ok(0);
        }
        self.persist_ts_collection_stamp(&key)?;
        let ts_dir = self.ts_dir_of(&key);
        let mut removed: Vec<String> = Vec::with_capacity(purgeable.len());
        let mut outcome = Ok(());
        for (partition_key, dir_name) in purgeable {
            if let Err(e) = remove_dir_tree(&ts_dir.join(&dir_name)) {
                outcome = Err(e);
                break;
            }
            if let Some(registry) = self.ts_registries.get_mut(&key) {
                registry.mark_deleted(&partition_key);
            }
            removed.push(dir_name);
        }
        if !removed.is_empty() {
            self.continuous_agg_mgr.forget_partitions(
                database_id.as_u64(),
                tid.as_u64(),
                collection,
                &removed,
            );
            self.persist_dirty_cagg_images();
            self.checkpoint_coordinator
                .mark_dirty("timeseries", removed.len());
        }
        outcome.map(|()| removed.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::envelope::Status;
    use crate::data::executor::core_loop::tests::make_core_with_dir;
    use crate::data::executor::handlers::timeseries::cagg_test_support::{
        COLLECTION, TENANT, flush, ingest, task_at,
    };

    fn key() -> (DatabaseId, TenantId, String) {
        (
            DatabaseId::DEFAULT,
            TenantId::new(TENANT),
            COLLECTION.to_string(),
        )
    }

    fn ended_partitions(core: &CoreLoop) -> Vec<Option<i64>> {
        core.ts_registries
            .get(&key())
            .map(|registry| {
                registry
                    .iter()
                    .filter(|(_, e)| {
                        e.meta.state != nodedb_types::timeseries::PartitionState::Deleted
                    })
                    .map(|(_, e)| e.meta.ended_at)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// A purge below the truncate's end keeps the ended partition. A purge
    /// at or above it removes the partition and its directory, and keeps
    /// every current partition.
    #[test]
    fn a_purge_removes_only_partitions_ended_at_or_below_the_horizon() {
        let dir = tempfile::tempdir().expect("tempdir");
        let (mut core, _tx, _rx) = make_core_with_dir(dir.path());
        assert_eq!(
            ingest(&mut core, "metrics,host=a value=1i 1000000000", 1),
            Status::Ok
        );
        let resp = core.execute_timeseries_truncate(&task_at(2), COLLECTION, None);
        assert_eq!(resp.status, Status::Ok, "{:?}", resp.error_code);
        let ended: Vec<i64> = ended_partitions(&core).into_iter().flatten().collect();
        assert_eq!(ended.len(), 1, "the truncate ended the flushed partition");
        let end = ended[0];
        assert_eq!(
            ingest(&mut core, "metrics,host=b value=2i 2000000000", 3),
            Status::Ok
        );
        flush(&mut core);

        let purged = core
            .purge_timeseries_truncated_below(
                DatabaseId::DEFAULT,
                TenantId::new(TENANT),
                COLLECTION,
                end - 1,
            )
            .expect("purge");
        assert_eq!(purged, 0, "a horizon below the end keeps the history");
        assert_eq!(ended_partitions(&core).len(), 2);

        let purged = core
            .purge_timeseries_truncated_below(
                DatabaseId::DEFAULT,
                TenantId::new(TENANT),
                COLLECTION,
                end,
            )
            .expect("purge");
        assert_eq!(purged, 1);
        assert_eq!(
            ended_partitions(&core),
            vec![None],
            "only the current partition stays"
        );
    }
}
