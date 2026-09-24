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

use crate::bridge::envelope::{PhysicalPlan, Priority, Request, Response, Status};
use crate::control::arrow_convert;
use crate::control::gateway::core::QueryContext;
use crate::control::server::payload_merge::{encode_msgpack_array, extract_msgpack_elements};
use crate::control::server::result_stream::ResultStream;
use crate::control::server::shared::session::statement_deadline;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, Lsn, ReadConsistency, TenantId, TraceId, TxnId, VShardId};

pub(super) use super::response::outcome_to_response;
pub(crate) use super::response::stream_to_response;

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
///
/// Each entry is `(core_id, request_id, receiver)`. The request id travels
/// with the receiver so a collect that ends at the deadline can name the
/// request it cancelled.
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
        crate::types::RequestId,
        crate::control::ResponseReceiver,
    )>,
> {
    // Every core in this fan-out belongs to ONE statement, so all of them
    // carry that statement's deadline. Resolving per core would give each core
    // its own budget and leave the statement unbounded in aggregate.
    let deadline = statement_deadline(state.tuning.network.default_deadline_secs);

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
            deadline,
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
            resolved_now_ms: None,
            admission: crate::bridge::envelope::Admission::Exempt(
                crate::bridge::envelope::ExemptReason::Read,
            ),
        };

        let rx = state.tracker.register(request_id);
        state
            .dispatcher
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .dispatch_to_core(core_id, request)?;
        receivers.push((core_id, request_id, rx));
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
    /// Max per-collection read-version LSN across all responding cores. A read
    /// homes to exactly ONE core, so the non-owning cores report `Lsn::ZERO` and
    /// the owning core's value dominates the fold — yielding the scanned
    /// collection's `coll_write_lsn` at read time (the sound comparand for
    /// cross-shard OCC read validation, distinct from `watermark_lsn`).
    pub read_version_lsn: Lsn,
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
/// error status from a core fails the whole gather: a fan-out that lost one
/// core's contribution answers a different question than the one asked, so a
/// truncated row set must never reach the client as a success.
pub(crate) async fn gather_all_cores(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: PhysicalPlan,
    trace_id: TraceId,
    txn_id: Option<TxnId>,
) -> crate::Result<GatherOutcome> {
    // Track broadcast calls for observability (shared counter with broadcast.rs).
    crate::control::server::broadcast::broadcast_call_count_increment();

    let deadline = statement_deadline(state.tuning.network.default_deadline_secs);

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
    let max_result_bytes = state.tuning.network.max_query_result_bytes as usize;
    let response_futures = receivers
        .into_iter()
        .map(|(core_id, request_id, mut rx)| async move {
            let context = format!("gather on core {core_id}");
            let result = crate::control::server::dispatch_utils::collect_under_deadline(
                &mut rx,
                crate::control::server::dispatch_utils::DeadlineCollect {
                    request_id,
                    deadline,
                    max_result_bytes,
                    context: &context,
                },
            )
            .await;
            (core_id, result)
        });

    let results: Vec<(usize, crate::Result<Response>)> = join_all(response_futures).await;

    let mut raw = Vec::new();
    let mut all_elements: Vec<Vec<u8>> = Vec::new();
    let mut max_lsn = Lsn::ZERO;
    // Max-fold of the per-collection read-version across cores: a collection
    // homes to ONE core, so non-owning cores contribute `Lsn::ZERO` and the
    // owning core's value dominates. Kept as a single scalar (not per-core) —
    // the read plan targets one collection, so one non-zero value survives.
    let mut max_read_version = Lsn::ZERO;
    let mut shard_watermarks: Vec<(VShardId, Lsn)> = Vec::new();
    // First error seen across cores, kept as a TYPED `crate::Error` so a code
    // like `DivisionByZero` surfaces as SQLSTATE 22012 rather than collapsing
    // to a generic `Dispatch` (XX000).
    let mut first_error: Option<crate::Error> = None;

    for (core_id, result) in results {
        let resp = match result {
            Ok(r) => r,
            Err(e) => {
                if first_error.is_none() {
                    first_error = Some(e);
                }
                continue;
            }
        };

        if resp.status == Status::Error {
            if let Err(e) = crate::control::server::dispatch_utils::reject_data_plane_error(&resp)
                && first_error.is_none()
            {
                first_error = Some(e);
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
        if resp.read_version_lsn > max_read_version {
            max_read_version = resp.read_version_lsn;
        }

        if resp.payload.is_empty() {
            continue;
        }

        let payload_bytes: &[u8] = resp.payload.as_ref();
        raw.extend_from_slice(payload_bytes);
        all_elements.extend(extract_msgpack_elements(payload_bytes));
    }

    if let Some(err) = first_error {
        return Err(err);
    }

    let merged_array = encode_msgpack_array(&all_elements);

    Ok(GatherOutcome {
        raw,
        merged_array,
        watermark_lsn: max_lsn,
        read_version_lsn: max_read_version,
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
/// Consume an authorized scan before entering the internal all-core fan-out.
pub fn gather_all_cores_stream_authorized(
    state: &SharedState,
    authorized: crate::control::server::shared::authorization::AuthorizedTask,
    trace_id: TraceId,
) -> crate::Result<ResultStream> {
    let task = authorized.into_physical_task();
    gather_all_cores_stream(
        state,
        task.tenant_id,
        task.database_id,
        task.plan,
        trace_id,
        task.txn_id,
    )
}

pub(crate) fn gather_all_cores_stream(
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
        .map(|(_core_id, _request_id, rx)| stream_response_channel(rx, max_result_bytes, true))
        .collect();

    Ok(Box::pin(futures::stream::select_all(per_core)))
}

/// Cluster-wide gather with routing awareness.
///
/// # Single-node mode
///
/// If `state.gateway` is `None`, routing is delegated to
/// [`super::owning_core::gather_single_node`] (same shape-based routing).
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
pub(crate) async fn gather_all_vshards(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    plan: PhysicalPlan,
    trace_id: TraceId,
    txn_id: Option<TxnId>,
) -> crate::Result<GatherOutcome> {
    let Some(gateway) = state.gateway.get() else {
        // Single-node: route by plan shape (cluster-partitioned leaf → broadcast;
        // single-vShard-homed collection → its one owning core; else broadcast
        // fallback), mirroring the cluster branch below.
        return super::owning_core::gather_single_node(
            state,
            tenant_id,
            database_id,
            plan,
            trace_id,
            txn_id,
        )
        .await;
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
    // The gateway already fails with a typed `crate::Error` — a shard's
    // `Error::DataPlane` code included. Re-wrapping it in `Dispatch` would
    // rewrite every such verdict as SQLSTATE XX000, so it passes through.
    let (payloads, shard_watermarks, read_version_lsn): (Vec<Vec<u8>>, Vec<(VShardId, Lsn)>, Lsn) =
        Box::pin(gateway.execute_internal_with_watermarks(&ctx, plan)).await?;

    let mut all_elements: Vec<Vec<u8>> = Vec::new();
    let mut raw = Vec::new();
    for payload in &payloads {
        raw.extend_from_slice(payload);
        all_elements.extend(extract_msgpack_elements(payload));
    }

    let merged_array = encode_msgpack_array(&all_elements);

    // Fold the per-shard watermarks into a scalar fence the same way the local
    // `gather_all_cores` path does — max across participating shards — while
    // keeping the per-shard entries for the transaction read-set.
    let watermark_lsn = shard_watermarks
        .iter()
        .map(|(_, lsn)| *lsn)
        .max()
        .unwrap_or(Lsn::ZERO);

    Ok(GatherOutcome {
        raw,
        merged_array,
        watermark_lsn,
        read_version_lsn,
        shard_watermarks,
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
