// SPDX-License-Identifier: BUSL-1.1

//! Lazy multi-frame SQL streaming for the native binary protocol.
//!
//! An eligible autocommit, single-task, unordered, multi-row SELECT — exactly
//! `Query(Exchange(Gather{as_aggregate:false}))` over a streamable scan — has
//! its rows pulled lazily off a [`ResultStream`] and written to the wire as a
//! sequence of `NativeResponse` frames, reusing the exact frame shape
//! `chunk_large_response` produces (columns on the first frame only, `Partial`
//! status on every frame but the last, terminal status + `rows_affected` on the
//! last). Existing clients reassemble it identically.
//!
//! The session loop owns the write stream, so the dispatch layer only *decides*
//! whether to stream and *opens* the stream; the per-batch frame emission runs
//! in the session loop (see `NativeSession::run`).

use nodedb_physical::physical_task::PostSetOp;
use nodedb_types::protocol::NativeResponse;

use crate::control::gateway::core::QueryContext;
use crate::control::server::exchange::gather::gather_all_cores_stream_authorized;
use crate::control::server::exchange::streamable::streamable_gather_child;
use crate::control::server::response_shape::redaction::QueryRedaction;
use crate::control::server::response_shape::schema::OutputSchema;
use crate::control::server::result_stream::ResultStream;
use crate::control::server::shared::session::TransactionState;

use super::DispatchCtx;

/// Outcome of dispatching a SQL statement on the native protocol.
///
/// The common case is a fully-formed [`NativeResponse`]. An eligible streamable
/// SELECT instead yields a [`SqlStream`] that the session loop drives frame by
/// frame against the connection's write stream.
pub(crate) enum SqlOutcome {
    /// A single materialized response — encoded/chunked by the session loop.
    Response(Box<NativeResponse>),
    /// A lazy row stream to be emitted as multiple frames.
    Stream(Box<SqlStream>),
}

impl SqlOutcome {
    /// Collapse to a `NativeResponse`. Only valid on the non-streaming path
    /// (callers that requested `allow_stream = false` never construct
    /// `Stream`); a `Stream` here is a programming error and is surfaced as a
    /// typed internal error rather than silently dropping the stream.
    pub(crate) fn into_response(self) -> NativeResponse {
        match self {
            SqlOutcome::Response(r) => *r,
            SqlOutcome::Stream(s) => crate::control::server::native::sqlstate_code::sqlstate_error(
                s.seq,
                "XX000",
                "internal error: SQL stream produced on a non-streaming path",
            ),
        }
    }
}

/// A lazy SQL row stream awaiting frame emission by the session loop.
pub(crate) struct SqlStream {
    /// Response sequence number echoed on every emitted frame.
    pub seq: u64,
    /// Global take-N across the whole union (`usize::MAX` = unlimited).
    pub limit: usize,
    /// The row-batch stream fanned out across cores / routes.
    pub stream: ResultStream,
    /// The statement's parsed SELECT-list projection, computed once up
    /// front. Streamable plans are always plain unordered scans (never a
    /// KV point-get or vector search — see `streamable_gather_child`), so
    /// each batch only needs decode + scan-envelope unwrap + this
    /// projection; no `apply_kv_wrap` / `translate_search_response` applies here.
    pub projection: Option<OutputSchema>,
    /// The statement's column-level redaction inputs, resolved ONCE here.
    /// Re-resolving them per batch would risk an early batch shipping rows a
    /// later one would have redacted.
    pub redaction: Option<QueryRedaction>,
    /// Descriptor leases acquired after planning. The session loop owns this
    /// stream, so retaining the scope here holds leases until final emission
    /// or connection teardown drops the stream.
    pub(crate) lease_scope: Option<crate::control::lease::QueryLeaseScope>,
}

impl SqlStream {
    /// Attach the descriptor leases for this stream exactly once.
    pub(crate) fn attach_lease_scope(
        &mut self,
        lease_scope: crate::control::lease::QueryLeaseScope,
    ) -> crate::Result<()> {
        if self.lease_scope.is_some() {
            return Err(crate::Error::Internal {
                detail: "SQL stream already owns a query lease scope".into(),
            });
        }
        self.lease_scope = Some(lease_scope);
        Ok(())
    }
}

/// If `task`-list is an eligible streamable SELECT, open its row stream.
///
/// Eligibility mirrors the pgwire `maybe_stream_select` predicate:
///   - single task (`tasks.len() == 1`),
///   - `post_set_op == PostSetOp::None`,
///   - no Control-Plane computed column in the projection (`cp_computed`
///     needs per-row session sequence access, which the per-batch shaper
///     does not carry),
///   - autocommit (not inside a `BEGIN..COMMIT` block), and
///   - the plan is `Query(Exchange(Gather{as_aggregate:false}))` over a
///     streamable unordered scan (via [`streamable_gather_child`]).
///
/// Returns `Ok(Some(stream))` when eligible, `Ok(None)` to fall back to the
/// materialized path, or `Err` if the stream could not be opened.
pub(crate) async fn try_open_sql_stream(
    ctx: &DispatchCtx<'_>,
    seq: u64,
    tasks: &[nodedb_physical::physical_task::PhysicalTask],
    database_id: crate::types::DatabaseId,
    output_schema: Option<&OutputSchema>,
) -> crate::Result<Option<SqlStream>> {
    let [task] = tasks else {
        return Ok(None);
    };
    if task.post_set_op != PostSetOp::None
        || output_schema.is_some_and(|s| !s.cp_computed.is_empty())
        || ctx.sessions.transaction_state(ctx.peer_addr) == TransactionState::InBlock
    {
        return Ok(None);
    }
    let Some((child_plan, limit)) = streamable_gather_child(&task.plan) else {
        return Ok(None);
    };

    let mut child_task = task.clone();
    child_task.plan = child_plan.clone();
    let tenant_id = child_task.tenant_id;
    let emitter =
        crate::control::security::audit::ArcAuditEmitter(std::sync::Arc::clone(&ctx.state.audit));
    let checked_child = match crate::control::server::shared::clone_write::intercept_and_authorize(
        crate::control::server::shared::clone_write::InterceptAndAuthorizeParams {
            state: ctx.state,
            task: child_task,
            identity: ctx.identity,
            tenant_id,
            permissions: &ctx.state.permissions,
            roles: &ctx.state.roles,
            emitter: &emitter,
        },
    )
    .await?
    {
        // A streamable child is always a plain unordered scan (see
        // `streamable_gather_child`), never a clone-write plan shape, so this
        // is unreachable in practice — surfaced as a typed error rather than
        // silently accommodated as either a stream or a materialized response.
        crate::control::server::shared::clone_write::CloneCheckedOutcome::Handled(_) => {
            return Err(crate::Error::Internal {
                detail: "clone-write hook unexpectedly intercepted a streamable scan".into(),
            });
        }
        crate::control::server::shared::clone_write::CloneCheckedOutcome::Proceed(checked) => {
            checked
        }
    };

    let gateway = ctx.state.gateway.get();
    let stream = if let Some(gw) = gateway {
        let gw_ctx = QueryContext {
            tenant_id: task.tenant_id,
            trace_id: crate::types::TraceId::ZERO,
            database_id,
            txn_id: task.txn_id,
        };
        gw.execute_stream(&gw_ctx, checked_child).await?
    } else {
        gather_all_cores_stream_authorized(
            ctx.state,
            checked_child.into_authorized(),
            crate::types::TraceId::ZERO,
        )?
    };

    Ok(Some(SqlStream {
        seq,
        limit,
        stream,
        projection: output_schema.cloned(),
        redaction: Some(QueryRedaction::for_plan(
            ctx.tenant_id(),
            ctx.auth_context(),
            &child_plan,
        )),
        lease_scope: None,
    }))
}
