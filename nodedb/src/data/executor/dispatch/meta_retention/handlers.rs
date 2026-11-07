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
use nodedb_physical::physical_plan::{AggregateIncarnation, MetaOp};

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
        // The cutoff is one decision, below a horizon its decider computed, so
        // every replica purges the same versions here.
        match op {
            MetaOp::TemporalPurgeEdgeStore {
                tenant_id,
                collection,
                cutoff_system_ms,
            } => self.meta_temporal_purge_edge_store(
                task,
                *tenant_id,
                collection.as_str(),
                *cutoff_system_ms,
            ),
            MetaOp::TemporalPurgeDocumentStrict {
                tenant_id,
                collection,
                cutoff_system_ms,
            } => self.meta_temporal_purge_document_strict(
                task,
                *tenant_id,
                collection.as_str(),
                *cutoff_system_ms,
            ),
            MetaOp::TemporalPurgeColumnar {
                tenant_id,
                collection,
                cutoff_system_ms,
                decided_ms,
                aggregates,
            } => self.meta_temporal_purge_columnar(
                task,
                *tenant_id,
                collection.as_str(),
                *cutoff_system_ms,
                *decided_ms,
                aggregates,
            ),
            MetaOp::TemporalPurgeCrdt {
                tenant_id,
                collection,
                cutoff_system_ms,
            } => self.meta_temporal_purge_crdt(
                task,
                *tenant_id,
                collection.as_str(),
                *cutoff_system_ms,
            ),
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

    /// `MetaOp::ApplyContinuousAggRetention`.
    pub(in crate::data::executor::dispatch) fn meta_apply_continuous_agg_retention(
        &mut self,
        task: &ExecutionTask,
        decided_ms: i64,
    ) -> Response {
        // A snapshot read below this ordinal finds the removed buckets gone
        // and refuses rather than answer without them. The ordinal comes from
        // the leader's clock read, so every replica records one rewrite.
        let ordinal = self.ts_batch_ordinal(
            crate::data::executor::handlers::timeseries::TimeseriesApplyMode::Replay,
            decided_ms,
        );
        let removed = self.continuous_agg_mgr.apply_retention(
            decided_ms,
            ordinal,
            task.request.database_id.as_u64(),
            task.request.tenant_id.as_u64(),
        );
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
            .get_watermark(
                task.request.database_id.as_u64(),
                task.request.tenant_id.as_u64(),
                aggregate_name,
            )
            .cloned()
            .unwrap_or_default();
        match response_codec::encode_serde(&wm) {
            Ok(payload) => self.response_with_payload(task, payload),
            Err(e) => self.response_error(task, ErrorCode::from(e)),
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
                    // A historical CSR at a cut the purge reaches read the
                    // removed versions.
                    self.historical_csr.invalidate_collection(
                        task.request.database_id,
                        tid,
                        collection,
                    );
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
            Err(e) => self.response_error(task, ErrorCode::from(e)),
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
        match self.purge_document_history(task, tenant_id, collection, cutoff_system_ms) {
            Ok((docs, idx)) => {
                let mut payload = Vec::with_capacity(16);
                payload.extend_from_slice(&(docs as u64).to_le_bytes());
                payload.extend_from_slice(&(idx as u64).to_le_bytes());
                self.response_with_payload(task, payload)
            }
            Err(e) => self.response_error(task, ErrorCode::from(e)),
        }
    }

    /// Remove the document versions and index entries of `collection` that no
    /// read at or above the cutoff sees. Returns `(versions, index entries)`.
    fn purge_document_history(
        &mut self,
        task: &ExecutionTask,
        tenant_id: u64,
        collection: &str,
        cutoff_system_ms: i64,
    ) -> crate::Result<(usize, usize)> {
        let (docs, idx) = self.sparse.purge_superseded_document_versions(
            task.request.database_id.as_u64(),
            tenant_id,
            collection,
            cutoff_system_ms,
        )?;
        if docs + idx > 0 {
            tracing::info!(
                tenant = tenant_id,
                collection,
                doc_versions_purged = docs,
                index_versions_purged = idx,
                cutoff_ms = cutoff_system_ms,
                "document history: temporal purge complete"
            );
            // The purged versions leave the tenant's history charge.
            if let Err(e) = self.recharge_document_history(
                task.request.database_id,
                crate::types::TenantId::new(tenant_id),
            ) {
                crate::diag::history_charge_stale(&e, "audit purge");
                tracing::warn!(
                    tenant = tenant_id,
                    collection,
                    error = %e,
                    "document history recharge after purge failed; the prior charge stays"
                );
            }
        }
        Ok((docs, idx))
    }

    /// `MetaOp::TemporalPurgeCrdt`: remove a CRDT collection's history below
    /// `cutoff_system_ms`. The history has two homes: the Loro archive, and
    /// the sparse row versions a write materializes, which `AS OF SYSTEM TIME`
    /// reads. The live row stays in both. Returns a u64 LE count of the entries
    /// removed. A tenant with no CRDT engine still has sparse versions to purge.
    pub(in crate::data::executor::dispatch) fn meta_temporal_purge_crdt(
        &mut self,
        task: &ExecutionTask,
        tenant_id: u64,
        collection: &str,
        cutoff_system_ms: i64,
    ) -> Response {
        let tid = TenantId::new(tenant_id);
        let archived = match self.crdt_engines.get(&(task.request.database_id, tid)) {
            Some(engine) => match engine.purge_history_before(collection, cutoff_system_ms) {
                Ok(n) => n as u64,
                Err(e) => {
                    return self.response_error(task, ErrorCode::from(e));
                }
            },
            None => 0,
        };
        let versions =
            match self.purge_document_history(task, tenant_id, collection, cutoff_system_ms) {
                Ok((docs, idx)) => (docs + idx) as u64,
                Err(e) => return self.response_error(task, ErrorCode::from(e)),
            };
        let purged = archived + versions;
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

    /// `MetaOp::TemporalPurgeArray`: purge the array's history below the
    /// last system-time ordinal of `cutoff_system_ms`
    /// (`ArrayEngine::purge_array_versions_below`). The version each cell
    /// shows at the cutoff stays, so every `AS OF` read at or after the
    /// cutoff answers as before.
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
        // An array no op has touched since boot is not open on this core, and
        // the engine purges nothing from an array it has not opened. Opened
        // from the catalog first; a dropped array has no entry and purges
        // nothing.
        let id =
            nodedb_array::types::ArrayId::in_database(tenant, task.request.database_id, array_id);
        if let Err(resp) = self.open_array_if_cataloged(task, &id) {
            return resp;
        }
        // The purge plans over segments, so a version still in the memtable
        // stays past its cutoff until a flush. A mirror holds its re-issued
        // cells there, and an array with few writes reaches no flush threshold.
        if self.array_engine.is_open(&id)
            && let Err(e) = self.flush_array(&id)
        {
            return self.response_error(task, ErrorCode::from(e));
        }
        // A later compaction's window ends here: it reads no local clock.
        let floor = self.array_retention_floors.entry(id).or_insert(i64::MIN);
        *floor = (*floor).max(cutoff_system_ms);
        match self.array_engine.purge_array_versions_below(
            tenant,
            task.request.database_id,
            array_id,
            nodedb_types::ms_to_ordinal_upper(cutoff_system_ms),
        ) {
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
    /// and the plain profile (row-level via row-version stamps).
    ///
    /// Dispatch rule: a collection present in `ts_registries` is a
    /// timeseries-profile collection and goes through the shared
    /// [`CoreLoop::meta_enforce_timeseries_retention`] path, which already
    /// consults `max_system_ts` for bitemporal. A collection present in
    /// `columnar_engines` is a plain-profile collection and runs the
    /// row-version purge below, which reads stamps, never a segment.
    ///
    /// `aggregates` lists the continuous aggregates the catalog roots at
    /// `collection`. The timeseries path removes nothing while one of them
    /// is not registered on this core.
    pub(in crate::data::executor::dispatch) fn meta_temporal_purge_columnar(
        &mut self,
        task: &ExecutionTask,
        tenant_id: u64,
        collection: &str,
        cutoff_system_ms: i64,
        decided_ms: i64,
        aggregates: &[AggregateIncarnation],
    ) -> Response {
        let tid = TenantId::new(tenant_id);
        let key = (task.request.database_id, tid, collection.to_string());

        // Timeseries profile: reuse partition-level retention with
        // max_age derived from the cutoff.
        if self.ts_registries.contains_key(&key) {
            let max_age_ms = decided_ms.saturating_sub(cutoff_system_ms);
            return self.meta_enforce_timeseries_retention(
                task, collection, max_age_ms, decided_ms, aggregates,
            );
        }

        // Plain columnar profile: row-version purge at the cutoff's ordinal.
        let n =
            self.plain_columnar_purge(task.request.database_id, tid, collection, cutoff_system_ms);
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
}

#[cfg(test)]
mod tests {
    use crate::data::executor::response_codec;
    use crate::engine::timeseries::continuous_agg::WatermarkState;

    /// The watermark the retention gate reads decodes from the payload this
    /// handler writes.
    #[test]
    fn watermark_payload_decodes() {
        let wm = WatermarkState {
            watermark_ts: 42,
            rows_aggregated: 3,
            last_refresh_ms: 9,
        };
        let payload = response_codec::encode_serde(&wm).expect("encode");
        let back: WatermarkState = response_codec::decode_payload(&payload).expect("decode");
        assert_eq!(back.watermark_ts, 42);
        assert_eq!(back.rows_aggregated, 3);
        assert_eq!(back.last_refresh_ms, 9);
    }
}
