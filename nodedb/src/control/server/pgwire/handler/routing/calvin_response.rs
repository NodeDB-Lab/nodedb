// SPDX-License-Identifier: BUSL-1.1

//! Response shaping for a completed Calvin batch.
//!
//! Calvin applies a whole batch atomically and deposits ONE applied response
//! for the transaction, so turning that batch into client-visible responses is
//! a different concern from planning a single statement into tasks — this file
//! owns the former, `planning.rs` the latter.

use pgwire::api::results::Tag;
use pgwire::error::{ErrorInfo, PgWireError};

use crate::control::server::response_shape::types::ShapedRows;
use crate::types::TenantId;
use nodedb_physical::physical_task::PhysicalTask;

/// Shared inputs for shaping one task of a completed Calvin batch.
pub(super) struct CalvinResponseCtx<'a> {
    /// The statement's announced output columns, when it announced any. A
    /// `RETURNING` write is held to them here exactly as the single-shard
    /// dispatch loop holds it, so the same statement renders the same row
    /// whichever route it took.
    pub(super) projection: Option<&'a crate::control::server::response_shape::schema::OutputSchema>,
    pub(super) state: &'a crate::control::state::SharedState,
    pub(super) tenant_id: TenantId,
    pub(super) database_id: crate::types::DatabaseId,
    /// The requester's resolved context; its roles drive column-level
    /// redaction of any RETURNING rows this batch surfaces.
    pub(super) auth: &'a crate::control::security::auth_context::AuthContext,
}

/// One Calvin task's contribution to the statement's response.
pub(super) enum CalvinTaskOutcome {
    /// RETURNING rows, to be folded into the statement's single result set.
    Rows(ShapedRows),
    /// A command tag, emitted as its own response.
    Tag(pgwire::api::results::Response),
}

/// Build the pgwire outcome for one task of a completed Calvin batch.
///
/// A task whose plan carries a RETURNING clause yields its rows as protocol-
/// neutral [`ShapedRows`] rather than an encoded response — the caller folds
/// every such task's rows into ONE result set for the statement, because a
/// multi-row write plans one task per row and an extended-query client reads a
/// RowDescription/DataRow sequence per task as several results for one
/// statement. Every other task (and a RETURNING task with no carried payload)
/// keeps the synthesised `Response::Execution` command tag.
pub(super) fn calvin_execution_response(
    task: &PhysicalTask,
    apply_resp: Option<&crate::bridge::envelope::Response>,
    ctx: CalvinResponseCtx<'_>,
) -> pgwire::error::PgWireResult<CalvinTaskOutcome> {
    use super::super::plan::{calvin_tag_for_plan, is_calvin_foldable};
    use crate::control::server::response_shape::compose::{
        ShapeOutcome, shape_response_materialized,
    };
    use crate::control::server::response_shape::redaction::QueryRedaction;
    use crate::control::server::response_shape::request::MaterializedShapeRequest;
    use crate::control::server::response_shape::types::{PlanKind, describe_plan};

    let CalvinResponseCtx {
        projection,
        state,
        tenant_id,
        database_id,
        auth,
    } = ctx;

    // RETURNING path: shape the applied payload into DATA-ROWs, exactly as the
    // non-Calvin dispatch loop does for a RETURNING write.
    let redaction = QueryRedaction::for_plan(tenant_id, auth, &task.plan);
    if let (PlanKind::ReturningRows, Some(resp)) = (describe_plan(&task.plan), apply_resp)
        && let Ok(ShapeOutcome::Rows(shaped)) =
            shape_response_materialized(MaterializedShapeRequest {
                payload: resp.payload.as_bytes(),
                plan: &task.plan,
                plan_kind: PlanKind::ReturningRows,
                projection,
                state,
                database_id,
                tenant_id,
                redaction: Some(redaction.ctx(&state.redaction)),
            })
    {
        return Ok(CalvinTaskOutcome::Rows(shaped));
    }

    // Plain (non-RETURNING) write: surface its ACTUAL affected count from the
    // payload — exactly as the non-Calvin write path does.
    //
    // Every primary-write participant deposits its applied `Response` before
    // proposing the completion ack (cross-node it rides back on the routed
    // submit's RPC reply), so a count-bearing plan ALWAYS has one here. If it
    // does not, the deposit path regressed: fail loudly rather than synthesise a
    // count, which is what made a delete of an absent row report a removed row.
    if let PlanKind::DmlResult(tag) = describe_plan(&task.plan) {
        let resp = apply_resp.ok_or_else(|| {
            PgWireError::UserError(Box::new(ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                format!(
                    "internal: Calvin {tag} completed with no applied response to read its \
                     affected-row count from"
                ),
            )))
        })?;
        return Ok(CalvinTaskOutcome::Tag(
            super::super::plan::payload_to_response(
                resp.payload.as_bytes(),
                describe_plan(&task.plan),
            )?
            .response,
        ));
    }

    let tag = if is_calvin_foldable(&task.plan) {
        calvin_tag_for_plan(&task.plan)?
    } else {
        Tag::new("OK")
    };
    Ok(CalvinTaskOutcome::Tag(
        pgwire::api::results::Response::Execution(tag),
    ))
}
