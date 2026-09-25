// SPDX-License-Identifier: BUSL-1.1

//! `MetaOp::EnforceTimeseriesRetention`: drop a timeseries collection's
//! expired partitions.

use crate::bridge::envelope::Response;
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::task::ExecutionTask;

use super::handlers::now_ms;

impl CoreLoop {
    /// `MetaOp::EnforceTimeseriesRetention`: drop partitions older than
    /// `max_age_ms`. Bitemporal collections use `max_system_ts` as the
    /// retention axis; non-bitemporal partitions fall through to `max_ts`.
    pub(in crate::data::executor::dispatch) fn meta_enforce_timeseries_retention(
        &mut self,
        task: &ExecutionTask,
        collection: &str,
        max_age_ms: i64,
    ) -> Response {
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
        let expired: Vec<(i64, String)> = self
            .ts_registries
            .get(&ts_key)
            .map(|registry| {
                registry
                    .iter()
                    .filter(|(_, e)| {
                        let axis_ts = if bitemporal && e.meta.max_system_ts > 0 {
                            e.meta.max_system_ts
                        } else {
                            e.meta.max_ts
                        };
                        axis_ts < cutoff
                            && e.meta.state != nodedb_types::timeseries::PartitionState::Deleted
                    })
                    .map(|(&start, e)| (start, e.dir_name.clone()))
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
        if let Some(registry) = self.ts_registries.get_mut(&ts_key) {
            for (start_ts, dir_name) in expired {
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
                registry.mark_deleted(start_ts);
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

        if let Some(lvc) = self.ts_last_value_caches.get_mut(&ts_key) {
            let evicted = lvc.evict_older_than(cutoff);
            if !evicted.is_empty() {
                tracing::debug!(
                    collection,
                    evicted = evicted.len(),
                    "evicted stale LVC entries"
                );
                // Drop the same series from the catalog. Leaving them behind
                // would let it accumulate every series the collection ever saw
                // while the cache it exists to serve has already released them.
                if let Some(catalog) = self.ts_series_catalogs.get_mut(&ts_key) {
                    for id in evicted {
                        catalog.forget(id);
                    }
                }
            }
        }

        let payload = (deleted as u64).to_le_bytes().to_vec();
        self.response_with_payload(task, payload)
    }
}
