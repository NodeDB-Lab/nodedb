// SPDX-License-Identifier: BUSL-1.1

//! Native Graph MATCH dispatch and response-envelope handling.

use nodedb_types::protocol::{NativeResponse, OpCode, TextFields};

use crate::bridge::envelope::{Response, Status};
use crate::control::server::shared::metering::{PlanMeteringInfo, meter_dispatch};
use crate::control::server::shared::quota_admission::admit_quota_for_dispatch;

use super::raw_dispatch::dispatch_authorized_single_task;
use super::response::data_plane_response_to_native;
use super::{DispatchCtx, error_to_native, error_to_native_with_sqlstate};

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
    let tenant_id = ctx.tenant_id();

    if let Err(error) = super::limits::check_op_limits(ctx.state, fields) {
        return error_to_native_with_sqlstate(seq, "0A000", &error);
    }
    if let Err(error) = ctx.state.check_tenant_quota(tenant_id) {
        return error_to_native(seq, &error);
    }

    let mut plan =
        match super::plan_builder::build_plan(ctx, OpCode::GraphMatch, fields, &collection).await {
            Ok(plan) => plan,
            Err(error) => return error_to_native_with_sqlstate(seq, "42601", &error),
        };
    let vshard_id = ctx.task_vshard(&plan, fields.document_id.as_deref(), &collection);
    if let Err(error) = crate::control::planner::rls_injection::inject_rls_for_single_plan(
        tenant_id.as_u64(),
        ctx.database_id(),
        &mut plan,
        &ctx.state.rls,
        ctx.state.credentials.catalog(),
        ctx.auth_context(),
    ) {
        return error_to_native_with_sqlstate(seq, "42501", &error);
    }
    // Refuse what column redaction cannot cover: a MATCH returns graph
    // topology, which the result-path masking hook has no columns to rewrite.
    if let Err(error) = crate::control::planner::redaction_refusal::refuse_unredactable_plan(
        &plan,
        tenant_id,
        ctx.database_id(),
        ctx.auth_context(),
        &ctx.state.redaction,
    ) {
        return error_to_native_with_sqlstate(seq, "0A000", &error);
    }

    // Stamp the active transaction id so MATCH reads resolve this connection's
    // staging overlay identically to every other direct-op read.
    let txn_id = ctx.sessions.tx_id(ctx.peer_addr);
    let plan_for_response = plan.clone();
    // Extracted before `plan` moves into `dispatch_authorized_single_task`
    // below — metering needs the collection/engine shape after dispatch
    // succeeds, and only when metering is enabled (the default is disabled).
    let plan_metering_info = ctx
        .state
        .metering_config
        .enabled
        .then(|| PlanMeteringInfo::extract(&plan));

    // A spent hard quota refuses the task before it runs; the charge below is
    // on the success path and so can never refuse anything itself.
    if let Some(info) = &plan_metering_info
        && let Err(e) = admit_quota_for_dispatch(ctx.state, &ctx.scope, info)
    {
        return error_to_native_with_sqlstate(seq, "53400", &e);
    }
    let _request = ctx.state.tenant_request_guard(tenant_id);

    // In a cluster the pattern crosses every node's partitions: it runs
    // through the cross-shard MATCH scatter, as the SQL surface's MATCH does.
    if ctx.state.cluster_routing.is_some() {
        let native = match_across_shards(ctx, seq, vshard_id, &plan, txn_id).await;
        if native.status != nodedb_types::protocol::ResponseStatus::Error
            && let Some(info) = &plan_metering_info
        {
            let rows = native.rows.as_ref().map(|rows| rows.len() as u64);
            meter_dispatch(ctx.state, &ctx.scope, info, rows);
        }
        return native;
    }

    let raw = dispatch_authorized_single_task(ctx, tenant_id, vshard_id, plan, txn_id).await;

    let response = match raw {
        Ok(response) => response,
        Err(error) => return error_to_native(seq, &error),
    };

    // A MATCH issued inside a native transaction records the versions its
    // core reported, identical to every other read seam.
    if (response.status == Status::Ok
        || response.error_code.as_deref() == Some(&crate::bridge::envelope::ErrorCode::NotFound))
        && ctx.sessions.transaction_state(ctx.peer_addr)
            == crate::control::server::shared::session::TransactionState::InBlock
    {
        crate::control::server::shared::session::record_reads_for_response(
            ctx.state,
            ctx.sessions,
            ctx.peer_addr.into(),
            ctx.tenant_id(),
            crate::control::server::shared::session::ResponseReads {
                plan: &plan_for_response,
                read_versions: &response.read_versions,
                found: response.status == Status::Ok,
                distributed_reads: &[],
            },
        )
        .await;
    }

    if response.status == Status::Error {
        return data_plane_response_to_native(ctx, seq, &plan_for_response, &response);
    }

    // Unwrap the `{rows, frontier, resume}` envelope into a bare rows array.
    // This single-shard path does not consume frontier or resume metadata.
    let unwrapped =
        match crate::control::server::graph_dispatch::unwrap_match_envelope(&response.payload) {
            Ok(envelope) => Response {
                payload: envelope.rows_payload,
                ..response
            },
            Err(error) => return error_to_native(seq, &error),
        };
    let native = data_plane_response_to_native(ctx, seq, &plan_for_response, &unwrapped);
    // Metered only on the success path — `data_plane_response_to_native`
    // above already decoded the row envelope to shape the response, so
    // `native.rows` gives the real row count for free (no extra decode).
    if native.status != nodedb_types::protocol::ResponseStatus::Error
        && let Some(info) = &plan_metering_info
    {
        let rows = native.rows.as_ref().map(|rows| rows.len() as u64);
        meter_dispatch(ctx.state, &ctx.scope, info, rows);
    }
    native
}

/// Run a native MATCH across every node's partitions through
/// `graph_dispatch::scatter_match`. The rows come back as the bare array a
/// local MATCH yields once its envelope is unwrapped. A result the scatter
/// cannot finish is refused with `54001`, never returned partial. The
/// scatter notes every vShard it read, and the session loop records them into
/// the transaction read-set (`session::pending_shard_reads`).
async fn match_across_shards(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    vshard_id: crate::types::VShardId,
    plan: &crate::bridge::envelope::PhysicalPlan,
    txn_id: Option<crate::types::TxnId>,
) -> NativeResponse {
    let crate::bridge::envelope::PhysicalPlan::Graph(
        nodedb_physical::physical_plan::GraphOp::Match { query, .. },
    ) = plan
    else {
        return error_to_native(
            seq,
            &crate::Error::Internal {
                detail: "a native MATCH built a plan that is not a MATCH".into(),
            },
        );
    };
    let task = nodedb_physical::physical_task::PhysicalTask {
        tenant_id: ctx.tenant_id(),
        vshard_id,
        database_id: ctx.database_id(),
        plan: plan.clone(),
        post_set_op: nodedb_physical::physical_task::PostSetOp::None,
        txn_id,
    };
    if let Err(error) = super::sql_gateway::authorize_native_task(ctx, &task) {
        return error_to_native(seq, &error);
    }
    let outcome = crate::control::server::graph_dispatch::scatter_match(
        ctx.state,
        ctx.tenant_id(),
        ctx.database_id(),
        query.clone(),
        crate::control::gateway::dispatcher::statement_deadline_ms(ctx.state),
        crate::control::server::graph_dispatch::GraphRead {
            txn_id,
            // The native protocol has no read-consistency setting.
            linearizable: true,
        },
    )
    .await;
    match outcome {
        Ok(outcome) if outcome.partial => error_to_native_with_sqlstate(
            seq,
            "54001",
            &crate::Error::BadRequest {
                detail: crate::control::server::shared::ddl::neutral::match_ops::MATCH_INCOMPLETE_MESSAGE
                    .into(),
            },
        ),
        Ok(outcome) => data_plane_response_to_native(
            ctx,
            seq,
            plan,
            &crate::control::server::dispatch_utils::ok_payload_response(outcome.rows_payload),
        ),
        Err(error) => error_to_native(seq, &error),
    }
}
