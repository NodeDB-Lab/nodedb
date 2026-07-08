// SPDX-License-Identifier: BUSL-1.1

//! Single fan-out/gather primitive for coordinator-mediated data movement.
//!
//! `gather_all_cores` fans a child plan to every Data-Plane core in parallel
//! using `join_all`, collects per-core payloads, and merges them into two
//! complementary views:
//!
//! - `raw`: concatenated per-core payloads (multiple msgpack arrays back-to-back).
//!   Consumed by the sync layer and legacy raw-scan paths.
//! - `merged_array`: a single msgpack array containing every row element
//!   from all cores.  Consumed by the response path and by `ProviderScan`
//!   embedding in join inputs.
//!
//! `finalize_aggregate` runs the Arrow SIMD post-processing pass for
//! `Gather{as_aggregate: true}` plans.

use futures::future::join_all;
use std::time::{Duration, Instant};

use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Response, Status};
use crate::control::arrow_convert;
use crate::control::gateway::core::QueryContext;
use crate::control::server::payload_merge::{encode_msgpack_array, extract_msgpack_elements};
use crate::control::server::result_stream::ResultStream;
use crate::control::state::SharedState;
use crate::types::{
    DatabaseId, Lsn, ReadConsistency, RequestId, TenantId, TraceId, TxnId, VShardId,
};

/// Eagerly dispatch a plan to every local Data-Plane core, registering a tracker
/// receiver per core BEFORE returning so all cores have the request in flight
/// (true-parallelism prologue). `plan_for_core(core_id)` produces each core's
/// plan — pass `|_| plan.clone()` for an identical plan across cores, or scope it
/// per core (e.g. a graph-superstep's `owned_vshards`). Returns the per-core
/// `(core_id, receiver)` pairs in core_id order (0..num_cores); the caller
/// collects/merges as it sees fit.
///
/// Does NOT call `broadcast_call_count_increment()` — each caller is responsible
/// for its own observability increment.
///
/// `txn_id` is the originating session transaction id (if the dispatching
/// task ran inside a transaction block); it is threaded onto every per-core
/// `Request` so the Data-Plane scan handler can merge the transaction's
/// staging overlay (read-your-own-writes). Autocommit / non-transactional
/// callers pass `None`, which reproduces prior behaviour exactly.
pub(crate) fn eager_dispatch_to_all_cores(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    trace_id: TraceId,
    txn_id: Option<TxnId>,
    plan_for_core: impl Fn(usize) -> PhysicalPlan,
) -> crate::Result<
    Vec<(
        usize,
        tokio::sync::mpsc::Receiver<crate::bridge::envelope::Response>,
    )>,
> {
    let deadline_secs = state.tuning.network.default_deadline_secs;

    let num_cores = state
        .dispatcher
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .num_cores();

    let mut receivers = Vec::with_capacity(num_cores);
    for core_id in 0..num_cores {
        let request_id = state.next_request_id();
        let vshard_id = VShardId::new(core_id as u32);
        let request = Request {
            request_id,
            tenant_id,
            database_id,
            vshard_id,
            plan: plan_for_core(core_id),
            deadline: Instant::now() + Duration::from_secs(deadline_secs),
            priority: Priority::Normal,
            trace_id,
            consistency: ReadConsistency::Strong,
            idempotency_key: None,
            event_source: crate::event::EventSource::User,
            user_roles: Vec::new(),
            user_id: None,
            statement_digest: None,
            txn_id,
            wal_lsn: None,
        };

        let rx = state.tracker.register(request_id);
        state
            .dispatcher
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .dispatch_to_core(core_id, request)?;
        receivers.push((core_id, rx));
    }

    Ok(receivers)
}

/// Outcomes of a full fan-out/gather cycle across all Data-Plane cores.
pub struct GatherOutcome {
    /// Concatenated per-core payloads (multiple msgpack arrays back-to-back).
    /// Consumed by the sync layer and raw-scan paths.
    pub raw: Vec<u8>,
    /// Single merged msgpack array of all row elements.
    /// Consumed by the pgwire/native response path and `ProviderScan` embedding.
    pub merged_array: Vec<u8>,
    /// Maximum watermark LSN seen across all responding cores. Retained as the
    /// scalar fence value for Strong-consistency callers that need one LSN.
    pub watermark_lsn: Lsn,
    /// Per-shard watermark LSNs — one `(vshard, watermark_lsn)` per responding
    /// core, NOT collapsed to the max. The transaction read-set records one
    /// entry per participating shard from this, so a predicate read fanned over
    /// N cores is validated against each core's own version rather than a
    /// single global max.
    pub shard_watermarks: Vec<(VShardId, Lsn)>,
}

/// Fan `plan` to every Data-Plane core in parallel and gather the results.
///
/// All per-core sends are issued before any response is awaited (`join_all`).
/// `NotFound` errors from individual cores are treated as "no rows" (the
/// collection shard simply has no matching data on that core).  Any other
/// error status from a core is noted, but only surfaces as an error if no
/// rows were gathered at all — partial results from healthy cores are returned
/// as-is.
pub async fn gather_all_cores(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: PhysicalPlan,
    trace_id: TraceId,
    txn_id: Option<TxnId>,
) -> crate::Result<GatherOutcome> {
    // Track broadcast calls for observability (shared counter with broadcast.rs).
    crate::control::server::broadcast::broadcast_call_count_increment();

    let deadline_secs = state.tuning.network.default_deadline_secs;

    // Issue all per-core sends and collect receiver channels before awaiting.
    // This ensures every core has the request in flight before we block on any
    // of them, matching true parallelism semantics.
    let receivers =
        eager_dispatch_to_all_cores(state, tenant_id, database_id, trace_id, txn_id, |_| {
            plan.clone()
        })?;

    // Await all responses in parallel using join_all. Each core's scan result
    // may stream as several `Partial` frames before its terminal frame, so drain
    // and concatenate the full bounded response per core — taking only the first
    // frame would silently truncate that core's contribution to `stream_chunk_size`
    // rows.
    let deadline = Duration::from_secs(deadline_secs);
    let max_result_bytes = state.tuning.network.max_query_result_bytes as usize;
    let response_futures = receivers.into_iter().map(|(core_id, mut rx)| async move {
        let result = match tokio::time::timeout(
            deadline,
            crate::control::server::dispatch_utils::collect_bounded_response(
                &mut rx,
                max_result_bytes,
            ),
        )
        .await
        {
            Err(_) => Err(crate::Error::Dispatch {
                detail: format!("gather timeout on core {core_id}"),
            }),
            Ok(Ok(resp)) => Ok(resp),
            Ok(Err(crate::control::server::dispatch_utils::DispatchCollectError::OverBudget {
                bytes,
            })) => Err(crate::Error::ExecutionLimitExceeded {
                detail: format!(
                    "gather on core {core_id} exceeded max_query_result_bytes \
                     ({bytes} > {max_result_bytes} bytes)"
                ),
            }),
            Ok(Err(
                crate::control::server::dispatch_utils::DispatchCollectError::ChannelClosed,
            )) => Err(crate::Error::Dispatch {
                detail: format!("gather channel closed on core {core_id}"),
            }),
        };
        (core_id, result)
    });

    let results: Vec<(usize, crate::Result<Response>)> = join_all(response_futures).await;

    let mut raw = Vec::new();
    let mut all_elements: Vec<Vec<u8>> = Vec::new();
    let mut max_lsn = Lsn::ZERO;
    let mut shard_watermarks: Vec<(VShardId, Lsn)> = Vec::new();
    let mut had_error = false;
    let mut error_msg = String::new();

    for (core_id, result) in results {
        let resp = match result {
            Ok(r) => r,
            Err(e) => {
                had_error = true;
                error_msg = e.to_string();
                continue;
            }
        };

        if resp.status == Status::Error {
            if let Some(ref ec) = resp.error_code {
                match ec {
                    crate::bridge::envelope::ErrorCode::NotFound => continue,
                    _ => {
                        had_error = true;
                        error_msg = format!("{ec:?}");
                    }
                }
            }
            continue;
        }

        // Record this core's own watermark as a participating-shard version,
        // even when its payload is empty — an empty scan slice is still a
        // validatable observation at that shard's version (phantom safety).
        shard_watermarks.push((VShardId::new(core_id as u32), resp.watermark_lsn));

        if resp.watermark_lsn > max_lsn {
            max_lsn = resp.watermark_lsn;
        }

        if resp.payload.is_empty() {
            continue;
        }

        let payload_bytes: &[u8] = resp.payload.as_ref();
        raw.extend_from_slice(payload_bytes);
        all_elements.extend(extract_msgpack_elements(payload_bytes));
    }

    if had_error && all_elements.is_empty() && raw.is_empty() {
        return Err(crate::Error::Dispatch { detail: error_msg });
    }

    let merged_array = encode_msgpack_array(&all_elements);

    Ok(GatherOutcome {
        raw,
        merged_array,
        watermark_lsn: max_lsn,
        shard_watermarks,
    })
}

/// Streaming sibling of [`gather_all_cores`] for single-node fan-out.
///
/// Dispatches `plan` to every Data-Plane core eagerly (registering a tracker
/// receiver per core BEFORE returning, exactly like `gather_all_cores`'s
/// prologue), then returns a [`ResultStream`] that interleaves rows from all
/// cores as they arrive via `futures::stream::select_all`. Nothing is
/// materialized on the coordinator — each frame flows straight through.
///
/// NotFound tolerance matches `gather_all_cores`: a per-core terminal
/// `Status::Error` with `ErrorCode::NotFound` ends that core's stream cleanly
/// (the collection shard simply has no rows on that core) rather than failing
/// the whole stream. Any other error status propagates as a stream `Err`. This
/// is handled by passing `tolerate_not_found: true` to
/// [`stream_response_channel`], which centralizes the NotFound-vs-error
/// decision in the leaf adapter.
///
/// Unlike `gather_all_cores`, there is no per-core timeout wrapper here: the
/// caller (the pgwire framework polling the `QueryResponse` stream) owns the
/// connection-level deadline, and a streamed result has no single point at
/// which to apply a fan-out timeout without buffering. The request deadline in
/// each per-core `Request` envelope still bounds Data-Plane work.
pub fn gather_all_cores_stream(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: PhysicalPlan,
    trace_id: TraceId,
    txn_id: Option<TxnId>,
) -> crate::Result<ResultStream> {
    use crate::control::server::result_stream::stream_response_channel;

    // Track broadcast calls for observability (shared counter with broadcast.rs).
    crate::control::server::broadcast::broadcast_call_count_increment();

    let max_result_bytes = state.tuning.network.max_query_result_bytes as usize;

    // Eager dispatch: register a tracker receiver and dispatch to each core
    // BEFORE returning the stream, so every core has the request in flight
    // immediately (matching `gather_all_cores`'s true-parallelism prologue).
    let per_core: Vec<ResultStream> =
        eager_dispatch_to_all_cores(state, tenant_id, database_id, trace_id, txn_id, |_| {
            plan.clone()
        })?
        .into_iter()
        .map(|(_core_id, rx)| stream_response_channel(rx, max_result_bytes, true))
        .collect();

    Ok(Box::pin(futures::stream::select_all(per_core)))
}

/// Cluster-wide gather with routing awareness.
///
/// # Single-node mode
///
/// If `state.gateway` is `None` this degenerates to [`gather_all_cores`] with
/// unchanged behaviour.
///
/// # Cluster mode — single-vShard-homed sources (document, kv, columnar,
/// timeseries, spatial, vector, text)
///
/// Standard collections are *single-vShard-homed*: all rows for a collection
/// live on exactly one vShard determined by `vshard_for_collection(database_id,
/// &name)`.  The data-plane scan is **not** vshard-scoped, so broadcasting the
/// plan to every vShard via `Exchange{Gather}` causes the owning node to return
/// the full collection once per route that lands on it — 1 024× duplication.
///
/// For these sources the bare plan is routed through the gateway's normal
/// `route_plan` `other` arm, which sends it directly to the single owning
/// vShard (local or remote) and returns exactly the right rows.
///
/// # Cluster mode — cluster-partitioned sources (graph traversal, array)
///
/// Graph traversal ops and Array ops distribute data across vShards by node-id
/// or tile-id.  Cross-node gather for these sources requires a dedicated
/// scatter-gather path that does not yet exist.  To avoid producing wrong
/// results we fall back to the local `gather_all_cores` path.
///
/// TRACKED DEBT: cross-node gather for genuinely vShard-partitioned sources
/// (graph traversal / array) needs its own broadcast + vshard-scoped path.
/// The Exchange{Gather} broadcast approach is NOT correct for single-vShard-
/// homed collections and must not be reinstated for them.
pub async fn gather_all_vshards(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: PhysicalPlan,
    trace_id: TraceId,
    txn_id: Option<TxnId>,
) -> crate::Result<GatherOutcome> {
    let Some(gateway) = state.gateway.as_ref() else {
        // Single-node: delegate to the local fan-out path unchanged.
        return gather_all_cores(state, tenant_id, database_id, plan, trace_id, txn_id).await;
    };

    if nodedb_physical::physical_plan::plan_contains_cluster_partitioned_leaf(&plan) {
        // Graph node-id / array tile partitioning: cross-node gather via this
        // primitive is NOT yet correct (these engines have dedicated scatter-
        // gather paths). Fall back to the prior local fan to avoid introducing
        // wrong results.
        // TRACKED DEBT: cross-node gather for genuinely vShard-partitioned
        // sources (graph traversal / array) needs its own broadcast +
        // vshard-scoped path. Do not replace this fallback with Exchange{Gather}
        // broadcasting — that path is only correct for single-vShard-homed
        // collections.
        return gather_all_cores(state, tenant_id, database_id, plan, trace_id, txn_id).await;
    }

    // Single-vShard-homed source (document/kv/columnar/ts/spatial/vector/text):
    // the whole collection lives on ONE vShard. Route the BARE plan through the
    // gateway so route_plan's `other` arm sends it to that single owning vShard
    // (local or remote). Do NOT wrap in Exchange{Gather} — broadcasting would
    // duplicate rows because the data-plane scan is not vshard-scoped.
    let ctx = QueryContext {
        tenant_id,
        trace_id,
        database_id,
        txn_id,
    };

    // `Box::pin` breaks an async-fn recursion cycle: the gateway dispatches the
    // plan through `dispatch_to_data_plane`, which re-enters
    // `resolve_exchange_in_plan` → `resolve_exchange` → here. The cycle
    // terminates at runtime (the plan is Exchange-free, so the re-entrant
    // resolve is a no-op), but the future must be heap-indirected so its size
    // is finite.
    let payloads: Vec<Vec<u8>> =
        Box::pin(gateway.execute(&ctx, plan))
            .await
            .map_err(|e| crate::Error::Dispatch {
                detail: format!("cross-node gather via gateway: {e}"),
            })?;

    let mut all_elements: Vec<Vec<u8>> = Vec::new();
    let mut raw = Vec::new();
    for payload in &payloads {
        raw.extend_from_slice(payload);
        all_elements.extend(extract_msgpack_elements(payload));
    }

    let merged_array = encode_msgpack_array(&all_elements);

    Ok(GatherOutcome {
        raw,
        merged_array,
        // KNOWN LIMITATION: cross-node gather does not yet thread per-shard
        // watermark LSNs back through the gateway response, so Strong-consistency
        // LSN fencing degrades to pass-through on this path.  This is consistent
        // with existing gateway behavior (gateway.execute returns no LSN metadata).
        // The cross-node per-shard watermark folds into the gateway wire change
        // (co-located with cross-shard `txn_id`/`database_id` threading), so no
        // per-shard watermarks are surfaced here yet — a ZERO read version is
        // over-conservative (never unsafe) and nothing validates it until then.
        watermark_lsn: Lsn::ZERO,
        shard_watermarks: Vec::new(),
    })
}

/// Build the final aggregate payload for `Gather{as_aggregate: true}` plans.
///
/// Runs Arrow SIMD post-processing on the merged msgpack rows.  Returns the
/// merged array unchanged — the Arrow pass validates the merge and logs schema
/// information for observability; the payload itself is already in its final
/// form after the per-core partial-aggregate merge.
pub fn finalize_aggregate(merged_array: &[u8]) -> Vec<u8> {
    if let Some(batch) = arrow_convert::msgpack_rows_to_record_batch(merged_array) {
        tracing::trace!(
            rows = batch.num_rows(),
            columns = batch.num_columns(),
            "arrow aggregate post-processing: merged {} rows",
            batch.num_rows(),
        );
    }
    merged_array.to_vec()
}

/// Materialize a [`ResultStream`] into a synthetic successful Response.
///
/// Used by the non-pgwire `Resolved::Stream` consumers (native, internal
/// funnel, recursive resolve) that need the fully-collected result as one
/// merged-array `Response`, preserving their prior gather-then-return behaviour.
pub(crate) async fn stream_to_response(stream: ResultStream) -> crate::Result<Response> {
    let (merged, watermark_lsn) =
        crate::control::server::result_stream::materialize(stream).await?;
    Ok(outcome_to_response(merged, watermark_lsn))
}

/// Build a synthetic successful Response from a gathered merged-array payload.
pub(super) fn outcome_to_response(merged_array: Vec<u8>, watermark_lsn: Lsn) -> Response {
    Response {
        request_id: RequestId::new(0),
        status: Status::Ok,
        attempt: 1,
        partial: false,
        payload: crate::bridge::envelope::Payload::from_vec(merged_array),
        watermark_lsn,
        error_code: None,
    }
}
