// SPDX-License-Identifier: BUSL-1.1

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
use nodedb_physical::physical_plan::MetaOp;

impl CoreLoop {
    /// Shared entry point for every `MetaOp::TemporalPurge*` variant.
    /// `other.rs` funnels all four here with a single arm so the match
    /// stays compact; each variant unwraps to its per-engine handler.
    /// Panics only if called with a non-purge `MetaOp`, which the caller
    /// guarantees via the outer `@` pattern.
    pub(in crate::data::executor::dispatch) fn dispatch_temporal_purge(
        &mut self,
        task: &ExecutionTask,
        op: &MetaOp,
    ) -> Response {
        match op {
            MetaOp::TemporalPurgeEdgeStore {
                tenant_id,
                collection,
                cutoff_system_ms,
            } => {
                self.meta_temporal_purge_edge_store(task, *tenant_id, collection, *cutoff_system_ms)
            }
            MetaOp::TemporalPurgeDocumentStrict {
                tenant_id,
                collection,
                cutoff_system_ms,
            } => self.meta_temporal_purge_document_strict(
                task,
                *tenant_id,
                collection,
                *cutoff_system_ms,
            ),
            MetaOp::TemporalPurgeColumnar {
                tenant_id,
                collection,
                cutoff_system_ms,
            } => self.meta_temporal_purge_columnar(task, *tenant_id, collection, *cutoff_system_ms),
            MetaOp::TemporalPurgeCrdt {
                tenant_id,
                collection,
                cutoff_system_ms,
            } => self.meta_temporal_purge_crdt(task, *tenant_id, collection, *cutoff_system_ms),
            MetaOp::TemporalPurgeArray {
                tenant_id,
                array_id,
                cutoff_system_ms,
            } => self.meta_temporal_purge_array(task, *tenant_id, array_id, *cutoff_system_ms),
            other => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("dispatch_temporal_purge: non-purge MetaOp {other:?}"),
                },
            ),
        }
    }

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
            .get_watermark(task.request.database_id.as_u64(), aggregate_name)
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
        let lvc_key = (
            task.request.database_id,
            task.request.tenant_id,
            collection.to_string(),
        );
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
        let lvc_key = (
            task.request.database_id,
            task.request.tenant_id,
            collection.to_string(),
        );
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
        tenant_id: u64,
        collection: &str,
        cutoff_system_ms: i64,
    ) -> Response {
        let tid = TenantId::new(tenant_id);
        let database_id = task.request.database_id.as_u64();
        match self.edge_store.purge_superseded_versions(
            database_id,
            tid,
            collection,
            cutoff_system_ms,
        ) {
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
        tenant_id: u64,
        collection: &str,
        cutoff_system_ms: i64,
    ) -> Response {
        match self.sparse.purge_superseded_document_versions(
            task.request.database_id.as_u64(),
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

    /// `MetaOp::TemporalPurgeCrdt`: drop archived row versions from the
    /// tenant's CRDT (Loro) engine for `collection` whose `_ts_system <
    /// cutoff_system_ms`. The live row is never touched. Returns a u64 LE
    /// count of archive entries deleted. A missing tenant CRDT engine is
    /// a no-op (returns 0) — the retention registry may outlive the
    /// engine when every bitemporal row has been purged.
    pub(in crate::data::executor::dispatch) fn meta_temporal_purge_crdt(
        &mut self,
        task: &ExecutionTask,
        tenant_id: u64,
        collection: &str,
        cutoff_system_ms: i64,
    ) -> Response {
        let tid = TenantId::new(tenant_id);
        let purged = match self.crdt_engines.get(&(task.request.database_id, tid)) {
            Some(engine) => match engine.purge_history_before(collection, cutoff_system_ms) {
                Ok(n) => n as u64,
                Err(e) => {
                    return self.response_error(
                        task,
                        ErrorCode::Internal {
                            detail: format!("crdt temporal purge: {e}"),
                        },
                    );
                }
            },
            None => 0,
        };
        if purged > 0 {
            tracing::info!(
                tenant = tenant_id,
                collection,
                purged,
                cutoff_ms = cutoff_system_ms,
                "crdt: temporal purge complete"
            );
        }
        let payload = purged.to_le_bytes().to_vec();
        self.response_with_payload(task, payload)
    }

    /// `MetaOp::TemporalPurgeArray`: drop superseded tile-versions older than
    /// `cutoff_system_ms` from the array engine. The latest version of each
    /// tile is always preserved so bitemporal AS-OF reads remain coherent.
    ///
    /// Returns an 8-byte little-endian u64 count of tile-versions dropped.
    pub(in crate::data::executor::dispatch) fn meta_temporal_purge_array(
        &mut self,
        task: &ExecutionTask,
        tenant_id: u64,
        array_id: &str,
        cutoff_system_ms: i64,
    ) -> Response {
        let tenant = TenantId::new(tenant_id);
        match self
            .array_engine
            .temporal_purge(tenant, array_id, cutoff_system_ms)
        {
            Ok(count) => {
                if count > 0 {
                    tracing::info!(
                        array = array_id,
                        purged = count,
                        cutoff_ms = cutoff_system_ms,
                        "array: temporal purge complete"
                    );
                }
                self.response_with_payload(task, count.to_le_bytes().to_vec())
            }
            Err(e) => self.response_error(
                task,
                ErrorCode::Internal {
                    detail: format!("array temporal purge: {e}"),
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
        tenant_id: u64,
        collection: &str,
        cutoff_system_ms: i64,
    ) -> Response {
        let tid = TenantId::new(tenant_id);
        let key = (task.request.database_id, tid, collection.to_string());

        // Timeseries profile: reuse partition-level retention with
        // max_age derived from the cutoff.
        if self.ts_registries.contains_key(&key) {
            let now = now_ms();
            let max_age_ms = (now - cutoff_system_ms).max(0);
            return self.meta_enforce_timeseries_retention(task, collection, max_age_ms);
        }

        // Plain columnar profile: row-level purge via delete bitmaps.
        match self.plain_columnar_purge(task.request.database_id, tid, collection, cutoff_system_ms)
        {
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
