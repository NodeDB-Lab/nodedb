// SPDX-License-Identifier: BUSL-1.1

//! ILP batch dispatch and adaptive rate estimation.

use sonic_rs;
use tracing::warn;

use crate::bridge::envelope::{Payload, PhysicalPlan, Response, Status};
use crate::control::gateway::GatewayErrorMap;
use crate::control::gateway::core::QueryContext;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, Lsn, RequestId, TenantId, TraceId, VShardId};
use nodedb_physical::physical_plan::TimeseriesOp;

/// EWMA-based rate estimator for adaptive ILP batch sizing.
pub(super) struct IlpRateEstimator {
    /// Smoothed rate in lines/second.
    rate: f64,
    /// EWMA smoothing factor (0.2 = responsive to recent changes).
    alpha: f64,
    /// Last measurement timestamp.
    last_ts: std::time::Instant,
}

impl IlpRateEstimator {
    pub(super) fn new() -> Self {
        Self {
            rate: 0.0,
            alpha: 0.2,
            last_ts: std::time::Instant::now(),
        }
    }

    /// Record that `lines` were flushed since the last call.
    pub(super) fn record(&mut self, lines: u64) {
        let now = std::time::Instant::now();
        let elapsed = now.duration_since(self.last_ts).as_secs_f64();
        self.last_ts = now;

        if elapsed > 0.0 {
            let instant_rate = lines as f64 / elapsed;
            if self.rate == 0.0 {
                self.rate = instant_rate;
            } else {
                self.rate = self.alpha * instant_rate + (1.0 - self.alpha) * self.rate;
            }
        }
    }

    /// Suggest (batch_size, window_ms) based on current rate.
    pub(super) fn suggest_batch_params(&self) -> (u64, u64) {
        if self.rate > 100_000.0 {
            // High rate: large batches, short window.
            (10_000, 10)
        } else if self.rate > 1_000.0 {
            // Medium rate: moderate batches.
            (1_000, 50)
        } else {
            // Low rate: small batches, long window.
            (100, 100)
        }
    }
}

/// Dispatch an ILP batch to the Data Plane with series-aware routing.
///
/// Groups lines by `(measurement, sorted_tags)` hash to route each series
/// to a deterministic core. This eliminates cross-core contention: each
/// core owns a subset of series.
///
/// For batches with a single measurement (common case), all lines go to
/// one dispatch — no overhead from grouping.
pub(super) async fn flush_ilp_batch(
    state: &SharedState,
    tenant_id: TenantId,
    batch: &str,
) -> crate::Result<u64> {
    // Quota enforcement — reject before WAL append or dispatch.
    state.check_tenant_quota(tenant_id)?;
    state.tenant_request_start(tenant_id);

    let result = flush_ilp_batch_inner(state, tenant_id, batch).await;
    state.tenant_request_end(tenant_id);
    result
}

/// Inner dispatch logic for ILP batch (separated for clean quota bookkeeping).
async fn flush_ilp_batch_inner(
    state: &SharedState,
    tenant_id: TenantId,
    batch: &str,
) -> crate::Result<u64> {
    // ILP protocol carries no database selector; all ops target DatabaseId::DEFAULT.
    // Fast path: extract collection from first line.
    let collection = batch
        .lines()
        .find(|l| !l.is_empty() && !l.starts_with('#'))
        .and_then(|l| l.split([',', ' ']).next())
        .unwrap_or("default_metrics")
        .to_string();

    // Route all ILP lines for a collection to the same vShard as the
    // collection-based scan uses. This ensures timeseries scans find
    // the memtable data on the correct Data Plane core.
    // Per-series sharding is deferred until the scan path supports
    // fan-out across multiple cores.
    let collection_vshard = VShardId::from_collection_in_database(DatabaseId::DEFAULT, &collection);
    let mut shard_batches: std::collections::HashMap<u32, String> =
        std::collections::HashMap::new();

    for line in batch.lines() {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let entry = shard_batches.entry(collection_vshard.as_u32()).or_default();
        entry.push_str(line);
        entry.push('\n');
    }

    let mut total_accepted = 0u64;

    for (shard_id, shard_batch) in &shard_batches {
        let vshard_id = VShardId::new(*shard_id);
        let payload_bytes = shard_batch.as_bytes().to_vec();

        // Append to WAL first — returns the assigned LSN for dedup tracking.
        let wal_lsn = crate::control::server::wal_dispatch::wal_append_timeseries(
            &state.wal,
            tenant_id,
            vshard_id,
            &collection,
            &payload_bytes,
            None,
            Some(&state.credentials),
        )?
        .map(|lsn| lsn.as_u64());

        let plan = PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: collection.clone(),
            payload: payload_bytes,
            format: "ilp".to_string(),
            wal_lsn,
            surrogates: Vec::new(),
            provenance: None,
        });

        let response = match state.gateway.as_ref() {
            Some(gw) => {
                let gw_ctx = QueryContext {
                    tenant_id,
                    trace_id: TraceId::generate(),
                    database_id: nodedb_types::id::DatabaseId::DEFAULT,
                    // ILP ingest is autocommit — no transaction context.
                    txn_id: None,
                };
                gw.execute(&gw_ctx, plan)
                    .await
                    .inspect_err(|err| {
                        let msg = GatewayErrorMap::to_resp(err);
                        warn!(
                            collection = %collection,
                            shard_id = shard_id,
                            error = %msg,
                            "ILP gateway dispatch error (batch dropped)"
                        );
                    })
                    .map(|payloads| {
                        let payload = payloads
                            .into_iter()
                            .next()
                            .map(Payload::from_vec)
                            .unwrap_or_else(Payload::empty);
                        Response {
                            request_id: RequestId::new(0),
                            status: Status::Ok,
                            attempt: 0,
                            partial: false,
                            payload,
                            watermark_lsn: Lsn::new(0),
                            error_code: None,
                        }
                    })?
            }
            None => {
                crate::control::server::dispatch_utils::dispatch_to_data_plane(
                    state,
                    tenant_id,
                    DatabaseId::DEFAULT,
                    vshard_id,
                    plan,
                    TraceId::ZERO,
                )
                .await?
            }
        };

        if !response.payload.is_empty()
            && let Ok(v) = sonic_rs::from_slice::<serde_json::Value>(&response.payload)
        {
            total_accepted += v.get("accepted").and_then(|a| a.as_u64()).unwrap_or(0);

            if let Some(schema_cols) = v.get("schema_columns").and_then(|s| s.as_array()) {
                let fields: Vec<(String, String)> = schema_cols
                    .iter()
                    .filter_map(|pair| {
                        let arr = pair.as_array()?;
                        Some((
                            arr.first()?.as_str()?.to_string(),
                            arr.get(1)?.as_str()?.to_string(),
                        ))
                    })
                    .collect();

                let catalog = state.credentials.catalog();
                if !fields.is_empty()
                    && let Ok(Some(mut coll)) =
                        catalog.get_collection(DatabaseId::DEFAULT, tenant_id.as_u64(), &collection)
                    && coll.fields != fields
                {
                    coll.fields = fields;
                    if let Err(e) = catalog.put_collection(DatabaseId::DEFAULT, &coll) {
                        tracing::warn!(
                            collection = %collection,
                            error = %e,
                            "failed to propagate ILP schema to catalog",
                        );
                    }
                }
            }
        }
    }

    Ok(total_accepted)
}
