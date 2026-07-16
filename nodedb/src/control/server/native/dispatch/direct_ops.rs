// SPDX-License-Identifier: BUSL-1.1

//! Direct Data Plane operation dispatch (PointGet, VectorSearch, Graph, etc.).

use nodedb_types::protocol::{NativeResponse, OpCode, TextFields};

use crate::bridge::envelope::{Payload, PhysicalPlan, Response, Status};
use crate::control::gateway::GatewayErrorMap;
use crate::control::gateway::core::QueryContext as GatewayQueryContext;
use crate::control::planner::calvin::{
    CrossShardTxnMode, DispatchClass, TxnDispatchPosition, classify_dispatch,
    dispatch_tasks_to_calvin,
};
use crate::control::server::response_shape::compose::{ShapeOutcome, shape_response_materialized};
use crate::control::server::response_shape::types::describe_plan;
use crate::control::server::shared::ddl::sqlstate::error_code_to_sqlstate;
use crate::control::server::shared::session::staging_gate::{
    InTxnRoute, StagingGateError, route_in_tx_write,
};
use crate::types::{Lsn, RequestId, TenantId, TraceId, TxnId, VShardId};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::super::super::dispatch_utils;
use super::{DispatchCtx, error_to_native, shape_error_to_native, to_native_columns_rows};

/// Dispatch a direct Data Plane operation by opcode.
pub(crate) async fn handle_direct_op(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    op: OpCode,
    fields: &TextFields,
) -> NativeResponse {
    let collection = fields
        .collection
        .as_deref()
        .unwrap_or("default")
        .to_lowercase();
    let vshard_key = fields.document_id.as_deref().unwrap_or(&collection);
    let vshard_id = ctx.vshard_for_key(vshard_key);
    let tenant_id = ctx.tenant_id();

    // Per-operation cap enforcement (vector dim, top_k, batch size, etc.).
    if let Err(e) = super::limits::check_op_limits(ctx.state, fields) {
        return NativeResponse::error(seq, "0A000", e.to_string());
    }

    // Quota enforcement — reject before planning or dispatch.
    if let Err(e) = ctx.state.check_tenant_quota(tenant_id) {
        return error_to_native(seq, &e);
    }

    let mut plan = match super::plan_builder::build_plan(ctx, op, fields, &collection) {
        Ok(p) => p,
        Err(e) => return NativeResponse::error(seq, "42601", e.to_string()),
    };

    // `INSERT ... SELECT` is orchestrated on the Control Plane (fresh, registered
    // surrogate per target row + atomic `BatchInsert`); it never reaches the
    // Data Plane as a single op.
    if let PhysicalPlan::Document(nodedb_physical::physical_plan::DocumentOp::InsertSelect {
        target_collection,
        source_collection,
        source_filters,
        source_limit,
    }) = &plan
    {
        ctx.state.tenant_request_start(tenant_id);
        let result = crate::control::insert_select::run_insert_select(
            ctx.state,
            tenant_id,
            ctx.database_id(),
            target_collection,
            source_collection,
            source_filters,
            *source_limit,
        )
        .await;
        ctx.state.tenant_request_end(tenant_id);
        return match result {
            Ok(resp) => data_plane_response_to_native(ctx, seq, &plan, &resp),
            Err(e) => error_to_native(seq, &e),
        };
    }

    // Autocommit `MERGE` is orchestrated on the Control Plane (fresh, registered
    // surrogate per NOT-MATCHED insert row + atomic apply); it never reaches the
    // Data Plane as a single op.
    if let PhysicalPlan::Document(nodedb_physical::physical_plan::DocumentOp::Merge {
        target_collection,
        source_collection,
        source_alias,
        target_join_col,
        source_join_col,
        clauses,
        returning: _,
        resolve_only: false,
        resolved_inserts: None,
        source_rows: _,
    }) = &plan
    {
        ctx.state.tenant_request_start(tenant_id);
        let result = crate::control::merge_orchestrator::run_merge(
            ctx.state,
            crate::control::merge_orchestrator::MergeArgs {
                tenant_id,
                database_id: ctx.database_id(),
                target_collection,
                source_collection,
                source_alias,
                target_join_col,
                source_join_col,
                clauses,
            },
        )
        .await;
        ctx.state.tenant_request_end(tenant_id);
        return match result {
            Ok(resp) => data_plane_response_to_native(ctx, seq, &plan, &resp),
            Err(e) => error_to_native(seq, &e),
        };
    }

    // Autocommit `UPDATE ... FROM <source>` is orchestrated on the Control Plane
    // (source scanned on its own core + shipped into the plan); it never reaches
    // the Data Plane as a single op reading a possibly-non-resident source.
    if let PhysicalPlan::Document(nodedb_physical::physical_plan::DocumentOp::UpdateFromJoin {
        target_collection,
        source_collection,
        source_alias,
        target_join_col,
        source_join_col,
        updates,
        target_filters,
        returning,
        resolve_only: false,
        source_rows: None,
    }) = &plan
    {
        ctx.state.tenant_request_start(tenant_id);
        let result = crate::control::update_from_join_orchestrator::run_update_from_join(
            ctx.state,
            crate::control::update_from_join_orchestrator::UpdateFromJoinArgs {
                tenant_id,
                database_id: ctx.database_id(),
                target_collection,
                source_collection,
                source_alias,
                target_join_col,
                source_join_col,
                updates,
                target_filters,
                returning: returning.as_ref(),
            },
        )
        .await;
        ctx.state.tenant_request_end(tenant_id);
        return match result {
            Ok(resp) => data_plane_response_to_native(ctx, seq, &plan, &resp),
            Err(e) => error_to_native(seq, &e),
        };
    }

    // Inject RLS filters from auth context (same as pgwire planner).
    if let Err(e) = crate::control::planner::rls_injection::inject_rls_for_single_plan(
        tenant_id.as_u64(),
        &mut plan,
        &ctx.state.rls,
        ctx.auth_context,
    ) {
        return NativeResponse::error(seq, "42501", e.to_string());
    }

    // Stamp the connection's active transaction id (as the SQL path's
    // `route_in_tx_write` does for in-transaction reads — see
    // `staging_gate.rs::route_in_tx_write`) so the Data Plane can resolve this
    // transaction's staging overlay for read-your-own-writes on direct-op
    // reads (PointGet / RangeScan / VectorSearch) and give direct-op writes
    // (KvBatchPut) a real transaction identity. `tx_id` is `None` outside a
    // transaction block, so autocommit behavior is unchanged.
    let txn_id = ctx.sessions.tx_id(ctx.peer_addr);

    // Implicit graph-edge extraction (pgwire / native-SQL parity): a schemaless
    // document carrying `_from`/`_to` is mirrored as a `GraphOp::EdgePut` task.
    // The common no-edge case leaves `tasks` at length 1 and runs the existing
    // single-dispatch path byte-identically below; an edge-bearing insert
    // augments the vec and routes through classify/Calvin like every other
    // write surface.
    let mut tasks = vec![PhysicalTask {
        tenant_id,
        vshard_id,
        database_id: ctx.database_id(),
        plan,
        post_set_op: PostSetOp::None,
        txn_id,
    }];
    if let Err(e) = crate::control::planner::implicit_edges::append_implicit_edge_tasks(
        ctx.state,
        &mut tasks,
        tenant_id,
        ctx.database_id(),
        TraceId::ZERO,
    )
    .await
    {
        return error_to_native(seq, &e);
    }

    if tasks.len() == 1
        && let Some(task) = tasks.pop()
    {
        // No-edge fast path — behaviorally identical to the pre-migration
        // single-plan dispatch. The local-path WAL append now lives inside
        // `dispatch_single_task` so it is shared with the single-shard edge loop.
        ctx.state.tenant_request_start(tenant_id);
        let result =
            dispatch_single_task(ctx, seq, tenant_id, vshard_id, task.plan, task.txn_id).await;
        ctx.state.tenant_request_end(tenant_id);
        return result;
    }

    // Edge-bearing insert: route the augmented task set the same way native SQL
    // does. A cross-shard set goes through the Calvin sequencer atomically (which
    // owns its own replicated durability); a single-shard set dispatches each
    // task sequentially (matching pgwire / native-SQL single-shard multi-task),
    // returning the document task's response. Local WAL durability for the
    // single-shard path is handled inside `dispatch_single_task`.
    ctx.state.tenant_request_start(tenant_id);
    // Autocommit direct-ops dispatch: no session read-set to widen with.
    let result = match classify_dispatch(&tasks, &std::collections::BTreeSet::new()) {
        DispatchClass::MultiShard { .. } => {
            match dispatch_tasks_to_calvin(
                ctx.state,
                &tasks,
                tenant_id,
                CrossShardTxnMode::Strict,
                TxnDispatchPosition::Autocommit,
                &[],
                None,
            )
            .await
            {
                // Edge-bearing INSERT: no RETURNING clause is possible here, so
                // the applied Response (if any) carries no rows — report one
                // row-affected per task.
                Ok(_apply) => {
                    let mut r = NativeResponse::ok(seq);
                    r.rows_affected = Some(tasks.len() as u64);
                    r
                }
                Err(e) => error_to_native(seq, &e),
            }
        }
        DispatchClass::SingleShard { .. } => {
            // The document task is first; its response is the one returned to
            // the caller. Edge tasks dispatch after it in order.
            let mut doc_response: Option<NativeResponse> = None;
            let mut error: Option<NativeResponse> = None;
            for task in tasks {
                let task_vshard = task.vshard_id;
                let task_txn_id = task.txn_id;
                let resp =
                    dispatch_single_task(ctx, seq, tenant_id, task_vshard, task.plan, task_txn_id)
                        .await;
                if resp.status == nodedb_types::protocol::ResponseStatus::Error {
                    error = Some(resp);
                    break;
                }
                if doc_response.is_none() {
                    doc_response = Some(resp);
                }
            }
            error
                .or(doc_response)
                .unwrap_or_else(|| NativeResponse::ok(seq))
        }
    };
    ctx.state.tenant_request_end(tenant_id);
    result
}

/// Dispatch a native `GraphMatch` op, unwrapping the DP `{rows, frontier}`
/// envelope into a bare rows array before native conversion.
///
/// MATCH responses are enveloped on the DP→CP hop (see
/// `data::executor::handlers::graph_match`). The native row decoder expects a
/// bare msgpack array, so this handler unwraps the envelope here. In B1
/// `cluster_mode` is always `false`, so the frontier is empty and the rows
/// payload is byte-identical to the prior bare-array native MATCH response.
/// (B2 will consume the frontier for cross-shard continuation.)
pub(crate) async fn handle_graph_match(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    fields: &TextFields,
) -> NativeResponse {
    let collection = fields
        .collection
        .as_deref()
        .unwrap_or("default")
        .to_lowercase();
    let vshard_key = fields.document_id.as_deref().unwrap_or(&collection);
    let vshard_id = ctx.vshard_for_key(vshard_key);
    let tenant_id = ctx.tenant_id();

    if let Err(e) = super::limits::check_op_limits(ctx.state, fields) {
        return NativeResponse::error(seq, "0A000", e.to_string());
    }
    if let Err(e) = ctx.state.check_tenant_quota(tenant_id) {
        return error_to_native(seq, &e);
    }

    let mut plan =
        match super::plan_builder::build_plan(ctx, OpCode::GraphMatch, fields, &collection) {
            Ok(p) => p,
            Err(e) => return NativeResponse::error(seq, "42601", e.to_string()),
        };
    if let Err(e) = crate::control::planner::rls_injection::inject_rls_for_single_plan(
        tenant_id.as_u64(),
        &mut plan,
        &ctx.state.rls,
        ctx.auth_context,
    ) {
        return NativeResponse::error(seq, "42501", e.to_string());
    }

    // Same rationale as `handle_direct_op`: stamp the active transaction id
    // (`None` outside a transaction block) so a MATCH issued inside a native
    // transaction resolves this connection's staging overlay identically to
    // every other direct-op read.
    let txn_id = ctx.sessions.tx_id(ctx.peer_addr);
    let plan_for_response = plan.clone();
    ctx.state.tenant_request_start(tenant_id);
    let raw = dispatch_single_task_raw(ctx, tenant_id, vshard_id, plan, txn_id).await;
    ctx.state.tenant_request_end(tenant_id);

    let resp = match raw {
        Ok(r) => r,
        Err(e) => return error_to_native(seq, &e),
    };

    // A MATCH issued inside a native transaction records a collection-scoped
    // predicate read at the shard's watermark, identical to every other read
    // seam. Single-shard direct op → one watermark, one entry.
    if (resp.status == Status::Ok
        || resp.error_code.as_deref() == Some(&crate::bridge::envelope::ErrorCode::NotFound))
        && ctx.sessions.transaction_state(ctx.peer_addr)
            == crate::control::server::shared::session::TransactionState::InBlock
    {
        crate::control::server::shared::session::record_read_set(
            ctx.state,
            ctx.sessions,
            ctx.peer_addr,
            ctx.tenant_id(),
            crate::control::server::shared::session::ReadCapture {
                plan: &plan_for_response,
                watermarks: &[(vshard_id, resp.watermark_lsn)],
                read_version_lsn: resp.read_version_lsn,
                found: resp.status == Status::Ok,
            },
        )
        .await;
    }

    if resp.status == Status::Error {
        return data_plane_response_to_native(ctx, seq, &plan_for_response, &resp);
    }

    // Unwrap the `{rows, frontier, resume}` envelope into a bare rows array. The
    // frontier is discarded here (B2 consumes it for cross-shard dispatch); the
    // resume cursor is likewise not acted on on this single-shard direct-op
    // path — the frame's `partial` flag already marks a truncated result.
    let unwrapped =
        match crate::control::server::graph_dispatch::unwrap_match_envelope(&resp.payload) {
            Ok(u) => Response {
                payload: u.rows_payload,
                ..resp
            },
            Err(e) => return error_to_native(seq, &e),
        };
    data_plane_response_to_native(ctx, seq, &plan_for_response, &unwrapped)
}

/// Dispatch one plan via the gateway (when wired) or the local SPSC path,
/// converting the Data-Plane response into a `NativeResponse`.
///
/// This is the exact single-plan dispatch the direct-op handler used before
/// implicit-edge extraction; it is factored out so the no-edge fast path and
/// the single-shard edge loop share one code path.
///
/// Routes through the same protocol-neutral in-transaction staging gate
/// (`route_in_tx_write`) the SQL-planned dispatch loops (`sql_loop.rs`,
/// pgwire's `execute_dml_hooks.rs`) already use. Outside a transaction block
/// this is a no-op passthrough (`InTxnRoute::Read` with the task unchanged),
/// so autocommit direct ops (including `KvBatchPut`) dispatch exactly as
/// before. Inside a transaction block, a stageable write (e.g. `KvBatchPut`)
/// is applied to the per-transaction overlay at statement time instead of
/// hitting durable storage directly -- fixing the atomicity gap where a
/// native direct-op write inside `BEGIN...COMMIT` used to commit immediately
/// and survive `ROLLBACK`. A non-stageable write is buffered for COMMIT-time
/// replay, matching the SQL path's deferral for the same plan shapes.
async fn dispatch_single_task(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    tenant_id: TenantId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
    txn_id: Option<TxnId>,
) -> NativeResponse {
    let task = PhysicalTask {
        tenant_id,
        vshard_id,
        database_id: ctx.database_id(),
        plan,
        post_set_op: PostSetOp::None,
        txn_id,
    };

    // Cloned before `route_in_tx_write` consumes `task`, so a staged write
    // whose outcome carries a real affected-count/computed-value payload
    // (e.g. `KvBatchPut`'s `{"inserted": n}`) can be shaped into the
    // response the same way the non-staged branch below shapes it.
    let plan_for_staged_response = task.plan.clone();

    let task = match route_in_tx_write(ctx.state, ctx.sessions, ctx.peer_addr, task, |stage_task| {
        dispatch_single_task_raw(
            ctx,
            stage_task.tenant_id,
            stage_task.vshard_id,
            stage_task.plan,
            stage_task.txn_id,
        )
    })
    .await
    {
        Ok(InTxnRoute::Read(routed_task)) => *routed_task,
        Ok(InTxnRoute::Buffered) => {
            let mut r = NativeResponse::ok(seq);
            r.rows_affected = Some(1);
            return r;
        }
        Ok(InTxnRoute::Staged(outcome)) => {
            let synthetic = Response {
                request_id: RequestId::new(0),
                status: Status::Ok,
                attempt: 0,
                partial: false,
                payload: Payload::from_vec(outcome.payload),
                watermark_lsn: Lsn::new(0),
                error_code: None,
                read_set_valid: None,
                read_version_lsn: crate::types::Lsn::ZERO,
                write_set: Vec::new(),
            };
            return data_plane_response_to_native(ctx, seq, &plan_for_staged_response, &synthetic);
        }
        Err(StagingGateError::Dispatch(e)) => return error_to_native(seq, &e),
        Err(StagingGateError::Rejected { code }) => {
            let (_, sqlstate, message) = match code {
                Some(code) => error_code_to_sqlstate(&code),
                None => ("ERROR", "XX000", "unknown data plane error".to_owned()),
            };
            return NativeResponse::error(seq, sqlstate, message);
        }
    };

    let plan_for_response = task.plan.clone();
    let task_vshard = task.vshard_id;
    match dispatch_single_task_raw(ctx, task.tenant_id, task.vshard_id, task.plan, task.txn_id)
        .await
    {
        Ok(resp) => {
            // Track direct-op reads (PointGet / RangeScan / VectorSearch / KV
            // Get) for conflict detection at the protocol-neutral layer, so
            // native direct-ops record identically to native SQL and pgwire.
            // Absent-key reads record too (a `NotFound` is a validatable phantom
            // observation). Direct ops are single-shard, so one watermark → one
            // entry.
            let records_read = resp.status == Status::Ok
                || resp.error_code.as_deref()
                    == Some(&crate::bridge::envelope::ErrorCode::NotFound);
            if records_read
                && ctx.sessions.transaction_state(ctx.peer_addr)
                    == crate::control::server::shared::session::TransactionState::InBlock
            {
                crate::control::server::shared::session::record_read_set(
                    ctx.state,
                    ctx.sessions,
                    ctx.peer_addr,
                    ctx.tenant_id(),
                    crate::control::server::shared::session::ReadCapture {
                        plan: &plan_for_response,
                        watermarks: &[(task_vshard, resp.watermark_lsn)],
                        read_version_lsn: resp.read_version_lsn,
                        found: resp.status == Status::Ok,
                    },
                )
                .await;
            }
            data_plane_response_to_native(ctx, seq, &plan_for_response, &resp)
        }
        Err(e) => error_to_native(seq, &e),
    }
}

/// Dispatch one plan via the gateway (when wired) or the local SPSC path and
/// return the raw Data-Plane [`Response`] without native conversion.
///
/// Factored out of [`dispatch_single_task`] so MATCH dispatch can unwrap the
/// `{rows, frontier}` envelope before native conversion while every other
/// direct op keeps its prior convert-in-place behaviour.
///
/// `txn_id` is the connection's active transaction id (`None` in autocommit),
/// threaded through to the Data Plane exactly like the native SQL path's
/// `dispatch_task_via_gateway` (see `sql_gateway.rs`) so direct-op reads can
/// resolve this transaction's staging overlay for read-your-own-writes.
async fn dispatch_single_task_raw(
    ctx: &DispatchCtx<'_>,
    tenant_id: TenantId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
    txn_id: Option<TxnId>,
) -> crate::Result<Response> {
    match ctx.state.gateway.as_ref() {
        Some(gw) => {
            let gw_ctx = GatewayQueryContext {
                tenant_id,
                trace_id: TraceId::generate(),
                database_id: ctx.database_id(),
                txn_id,
            };
            match gw.execute(&gw_ctx, plan).await {
                Ok(payloads) => Ok(gateway_payloads_to_response(payloads)),
                Err(e) => {
                    let (_code, msg) = GatewayErrorMap::to_native(&e);
                    Err(crate::Error::Dispatch { detail: msg })
                }
            }
        }
        None => {
            // Local SPSC path (single-node boot, before the gateway is wired):
            // the gateway would otherwise own WAL durability on the target node,
            // so the write must be appended locally. The append is performed
            // inside the dispatch core, under the write-admission guard and just
            // before the enqueue, so LSN order matches apply order. This covers
            // every local dispatch — the no-edge fast path AND each task of a
            // single-shard edge bundle — so an implicit edge written on the boot
            // path is durable. (Cross-shard bundles route via Calvin, which owns
            // its own replicated durability and never reaches this branch.)
            let database_id = ctx.database_id();
            dispatch_utils::dispatch_autocommit_write(
                ctx.state,
                dispatch_utils::AutocommitWrite {
                    tenant_id,
                    database_id,
                    vshard_id,
                    plan,
                    trace_id: TraceId::ZERO,
                    event_source: crate::event::EventSource::User,
                    txn_id,
                },
            )
            .await
        }
    }
}

/// Convert gateway `Vec<Vec<u8>>` payloads into a synthetic `Response`.
fn gateway_payloads_to_response(payloads: Vec<Vec<u8>>) -> Response {
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
        read_set_valid: None,
        read_version_lsn: crate::types::Lsn::ZERO,
        write_set: Vec::new(),
    }
}

/// Convert a raw Data-Plane [`Response`] into a [`NativeResponse`], shaping
/// a non-empty payload through the shared composed shaper.
///
/// Direct ops (`OpCode::PointGet`, `VectorSearch`, `GraphMatch`, ...) have no
/// SQL text, so there is no SELECT-list to project — `projection` is always
/// `None` here, matching pgwire's own direct-op (`{ }` field syntax / RESP)
/// handlers, which likewise never apply column projection.
fn data_plane_response_to_native(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    plan: &PhysicalPlan,
    resp: &Response,
) -> NativeResponse {
    if resp.status == Status::Error {
        let msg = if resp.payload.is_empty() {
            resp.error_code
                .as_ref()
                .map(|c| format!("{c:?}"))
                .unwrap_or_else(|| "unknown error".into())
        } else {
            String::from_utf8_lossy(&resp.payload).into_owned()
        };
        return NativeResponse::error(seq, "XX000", msg);
    }

    if resp.payload.is_empty() {
        let mut r = NativeResponse::ok(seq);
        r.watermark_lsn = resp.watermark_lsn.as_u64();
        return r;
    }

    let plan_kind = describe_plan(plan);
    match shape_response_materialized(
        &resp.payload,
        plan,
        plan_kind,
        None,
        ctx.state,
        ctx.database_id(),
        ctx.tenant_id(),
    ) {
        Ok(ShapeOutcome::Rows(shaped)) => {
            let (columns, rows) = to_native_columns_rows(&shaped);
            NativeResponse {
                seq,
                status: nodedb_types::protocol::ResponseStatus::Ok,
                columns: Some(columns),
                rows: Some(rows),
                rows_affected: None,
                watermark_lsn: resp.watermark_lsn.as_u64(),
                error: None,
                auth: None,
                warnings: shaped.notice.into_iter().collect(),
            }
        }
        Ok(ShapeOutcome::Passthrough) => {
            let mut r = NativeResponse::ok(seq);
            r.watermark_lsn = resp.watermark_lsn.as_u64();
            r
        }
        Err(e) => shape_error_to_native(seq, &e),
    }
}
