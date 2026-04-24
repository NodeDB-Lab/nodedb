//! Retention-related Meta dispatch handlers.
//!
//! Houses the dispatch bodies for the retention, purge, and retention-adjacent
//! Meta ops so `dispatch/other.rs` stays under the per-file soft limit. Each
//! handler takes the `CoreLoop` via `&mut self` (or `&self` where the op is
//! read-only) and returns a completed `Response`.
//!
//! The three `TemporalPurge*` handlers implement the bitemporal audit-retention
//! contract: drop **superseded** versions older than a system-time cutoff
//! while preserving the single latest version of every logical row, so
//! "AS OF" reads beyond the cutoff still see a coherent terminal state.

use nodedb_types::TenantId;

use crate::bridge::envelope::{ErrorCode, Response};
use crate::data::executor::core_loop::CoreLoop;
use crate::data::executor::response_codec;
use crate::data::executor::task::ExecutionTask;

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
        let ts_base = self.data_dir.join("ts").join(collection);

        let bitemporal = self.is_bitemporal(task.request.tenant_id.as_u32(), collection);

        let ts_key = (task.request.tenant_id, collection.to_string());
        if let Some(registry) = self.ts_registries.get_mut(&ts_key) {
            let expired: Vec<(i64, String)> = registry
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
                .collect();

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
            if evicted > 0 {
                tracing::debug!(collection, evicted, "evicted stale LVC entries");
            }
        }

        let payload = (deleted as u64).to_le_bytes().to_vec();
        self.response_with_payload(task, payload)
    }

    /// `MetaOp::ApplyContinuousAggRetention`.
    pub(in crate::data::executor::dispatch) fn meta_apply_continuous_agg_retention(
        &mut self,
        task: &ExecutionTask,
    ) -> Response {
        let now_ms = now_ms();
        let removed = self.continuous_agg_mgr.apply_retention(now_ms);
        tracing::debug!(removed, "continuous aggregate retention applied");
        self.response_ok(task)
    }

    /// `MetaOp::QueryAggregateWatermark`.
    pub(in crate::data::executor::dispatch) fn meta_query_aggregate_watermark(
        &self,
        task: &ExecutionTask,
        aggregate_name: &str,
    ) -> Response {
        let wm = self
            .continuous_agg_mgr
            .get_watermark(aggregate_name)
            .cloned()
            .unwrap_or_default();
        match response_codec::encode_serde(&wm) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// `MetaOp::QueryLastValues`.
    pub(in crate::data::executor::dispatch) fn meta_query_last_values(
        &self,
        task: &ExecutionTask,
        collection: &str,
    ) -> Response {
        let lvc_key = (task.request.tenant_id, collection.to_string());
        let entries: Vec<(u64, i64, f64)> =
            if let Some(lvc) = self.ts_last_value_caches.get(&lvc_key) {
                lvc.all().map(|(id, e)| (id, e.ts, e.value)).collect()
            } else {
                Vec::new()
            };
        match response_codec::encode(&entries) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// `MetaOp::QueryLastValue`.
    pub(in crate::data::executor::dispatch) fn meta_query_last_value(
        &self,
        task: &ExecutionTask,
        collection: &str,
        series_id: u64,
    ) -> Response {
        let lvc_key = (task.request.tenant_id, collection.to_string());
        let entry: Option<(i64, f64)> = self
            .ts_last_value_caches
            .get(&lvc_key)
            .and_then(|lvc| lvc.get(series_id))
            .map(|e| (e.ts, e.value));
        match response_codec::encode(&entry) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: e.to_string(),
                },
            ),
        }
    }

    /// `MetaOp::TemporalPurgeEdgeStore`.
    pub(in crate::data::executor::dispatch) fn meta_temporal_purge_edge_store(
        &mut self,
        task: &ExecutionTask,
        tenant_id: u32,
        collection: &str,
        cutoff_system_ms: i64,
    ) -> Response {
        let tid = TenantId::new(tenant_id);
        match self
            .edge_store
            .purge_superseded_versions(tid, collection, cutoff_system_ms)
        {
            Ok(n) => {
                if n > 0 {
                    tracing::info!(
                        tenant = tenant_id,
                        collection,
                        purged = n,
                        cutoff_ms = cutoff_system_ms,
                        "edge_store: temporal purge complete"
                    );
                }
                let payload = (n as u64).to_le_bytes().to_vec();
                self.response_with_payload(task, payload)
            }
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("edge_store temporal purge: {e}"),
                },
            ),
        }
    }

    /// `MetaOp::TemporalPurgeDocumentStrict`.
    pub(in crate::data::executor::dispatch) fn meta_temporal_purge_document_strict(
        &mut self,
        task: &ExecutionTask,
        tenant_id: u32,
        collection: &str,
        cutoff_system_ms: i64,
    ) -> Response {
        match self.sparse.purge_superseded_document_versions(
            tenant_id,
            collection,
            cutoff_system_ms,
        ) {
            Ok((docs, idx)) => {
                if docs + idx > 0 {
                    tracing::info!(
                        tenant = tenant_id,
                        collection,
                        doc_versions_purged = docs,
                        index_versions_purged = idx,
                        cutoff_ms = cutoff_system_ms,
                        "document_strict: temporal purge complete"
                    );
                }
                let mut payload = Vec::with_capacity(16);
                payload.extend_from_slice(&(docs as u64).to_le_bytes());
                payload.extend_from_slice(&(idx as u64).to_le_bytes());
                self.response_with_payload(task, payload)
            }
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("document_strict temporal purge: {e}"),
                },
            ),
        }
    }

    /// `MetaOp::TemporalPurgeColumnar`: bitemporal audit purge for columnar
    /// collections, covering both the timeseries profile (partition-level)
    /// and the plain profile (row-level via segment delete bitmaps).
    ///
    /// Dispatch rule: a collection present in `ts_registries` is a
    /// timeseries-profile collection and goes through the shared
    /// [`CoreLoop::meta_enforce_timeseries_retention`] path, which already
    /// consults `max_system_ts` for bitemporal. A collection present in
    /// `columnar_engines` is a plain-profile collection and runs the
    /// segment-scanning row-version purge below.
    pub(in crate::data::executor::dispatch) fn meta_temporal_purge_columnar(
        &mut self,
        task: &ExecutionTask,
        tenant_id: u32,
        collection: &str,
        cutoff_system_ms: i64,
    ) -> Response {
        let tid = TenantId::new(tenant_id);
        let key = (tid, collection.to_string());

        // Timeseries profile: reuse partition-level retention with
        // max_age derived from the cutoff.
        if self.ts_registries.contains_key(&key) {
            let now = now_ms();
            let max_age_ms = (now - cutoff_system_ms).max(0);
            return self.meta_enforce_timeseries_retention(task, collection, max_age_ms);
        }

        // Plain columnar profile: row-level purge via delete bitmaps.
        match self.plain_columnar_purge(tid, collection, cutoff_system_ms) {
            Ok(n) => {
                if n > 0 {
                    tracing::info!(
                        tenant = tenant_id,
                        collection,
                        purged = n,
                        cutoff_ms = cutoff_system_ms,
                        "columnar (plain): temporal purge complete"
                    );
                }
                let payload = (n as u64).to_le_bytes().to_vec();
                self.response_with_payload(task, payload)
            }
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("columnar temporal purge: {e}"),
                },
            ),
        }
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_else(|e| {
            tracing::warn!("system clock before UNIX_EPOCH: {e}; using epoch as now");
            std::time::Duration::ZERO
        })
        .as_millis() as i64
}
