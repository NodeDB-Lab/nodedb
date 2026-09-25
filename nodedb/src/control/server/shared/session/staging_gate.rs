// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral in-transaction write-routing gate.
//!
//! Decides, for a single physical task, whether it is:
//!
//! - a read, handed back for the caller's read dispatch;
//! - a write that applies now on the durable autocommit route: outside a
//!   transaction block, or a write a transaction cannot buffer;
//! - a write buffered for COMMIT-time replay ("OK" now, durable apply later);
//! - a stageable write applied to the per-transaction overlay now (real
//!   command tag and statement-time constraint errors), still buffered for
//!   COMMIT's durable replay.
//!
//! A write never comes back as a read, so no caller can send one down the
//! read route, which appends no WAL record.
//!
//! This is the shared seam every protocol's dispatch loop routes through
//! (pgwire SQL today; native and the DSL/UPSERT path in later units), so the
//! staging decision lives in exactly one place. No pgwire types are
//! referenced here — callers translate the neutral [`InTxnRoute`] outcome
//! into their own protocol's response type.

use std::future::Future;

use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Response, Status};
use crate::control::gateway::RouteDecision;
use crate::control::security::request_scope::RequestAuthScope;
use crate::control::server::shared::metering::{PlanMeteringInfo, meter_staged_write};
use crate::control::server::shared::quota_admission::admit_quota_for_dispatch;
use crate::control::server::shared::sql::staging_predicates::{
    is_stageable_write, require_affected_count, stageable_write_shape,
};
use crate::control::server::shared::write_admission::{plan_is_write, plan_requires_txn_buffering};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TxnId, VShardId};
use nodedb_physical::physical_plan::{ClusterArrayOp, CrdtOp, MetaOp};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::array_fanout_stage::stage_cluster_array_write;
use super::connection::SessionId;
use super::leader_forward::{forward_to_leader, resolve_leader};
use super::state::TransactionState;
use super::store::SessionStore;
use super::txn_expand::expand_for_buffering;
pub use crate::control::server::shared::sql::staging_predicates::StagedTagKind;

/// Outcome of routing a single task through the in-transaction staging gate.
pub enum InTxnRoute {
    /// Not a write. The task is handed back for the caller's read dispatch.
    /// Inside a transaction block it carries the transaction id, so the Data
    /// Plane reads this transaction's overlay (read-your-own-writes).
    Read(Box<PhysicalTask>),
    /// A write that applies now, on the caller's durable autocommit route:
    /// the session is outside a transaction block, or the write is one a
    /// transaction cannot buffer (index DDL, a Calvin-routed bulk write).
    ///
    /// The caller must dispatch it through the write funnel with
    /// `AppendHere`, or propose it through Raft in cluster mode
    /// (`dispatch_utils::dispatch_authorized_durable_write`). The read route
    /// appends no WAL record, and it refuses such a write.
    ///
    /// Inside a block the task carries the transaction id.
    Autocommit(Box<PhysicalTask>),
    /// A non-stageable write: buffered for COMMIT-time replay. The caller
    /// pushes an immediate "OK" tag.
    Buffered,
    /// A stageable write: applied to the per-transaction overlay now, with
    /// the real outcome available for a "command complete" tag. Also
    /// buffered (unchanged) for COMMIT's durable replay.
    Staged(StagedWriteOutcome),
}

/// The result of staging a write into the per-transaction overlay.
pub struct StagedWriteOutcome {
    pub kind: StagedTagKind,
    pub affected: usize,
    /// The stage handler's raw response payload, verbatim. Every staged
    /// write's response carries a payload here; only [`StagedTagKind::
    /// RawPayload`] outcomes (KV `Incr` / `IncrFloat` / `Cas` / `GetSet`,
    /// which return a computed value rather than an affected-row count) are
    /// expected to be forwarded to the client instead of being reduced to a
    /// tag + count.
    pub payload: Vec<u8>,
}

/// Session store + collision-free session identity, bundled so the
/// protocol-neutral DDL dispatch path (`dispatch` -> `try_dispatch` ->
/// `upsert_document` / `insert_document` -> `plan_and_dispatch`, plus the
/// `COPY FROM` bulk-import chain) can thread one state identity down to
/// [`route_in_tx_write`] without coupling storage to network provenance.
pub struct DmlTxnCtx<'a> {
    pub sessions: &'a SessionStore,
    pub session_id: SessionId,
}

/// An owned, session-less scope for callers with no BEGIN/COMMIT transaction
/// concept over their transport (stateless HTTP, autocommit test helpers).
///
/// It owns a fresh [`SessionStore`] and a private legacy session identity.
/// A fresh store reports [`TransactionState::Idle`] for that identity, so
/// [`route_in_tx_write`] answers `Read` for a read and `Autocommit` for a
/// write through a [`DmlTxnCtx`] borrowed from here. Keep the scope alive for
/// the duration of the dispatch call that borrows its [`ctx`](Self::ctx).
pub struct DetachedTxnScope {
    sessions: SessionStore,
    session_id: SessionId,
}

impl Default for DetachedTxnScope {
    fn default() -> Self {
        Self::new()
    }
}

impl DetachedTxnScope {
    /// Create an owned session-less scope.
    pub fn new() -> Self {
        Self {
            sessions: SessionStore::new(),
            session_id: SessionId::from(std::net::SocketAddr::from(([0, 0, 0, 0], 0))),
        }
    }

    /// Borrow a [`DmlTxnCtx`] pointing at this scope's owned store + session identity.
    pub fn ctx(&self) -> DmlTxnCtx<'_> {
        DmlTxnCtx {
            sessions: &self.sessions,
            session_id: self.session_id,
        }
    }
}

/// Error surfaced by [`route_in_tx_write`]. Kept distinct from
/// `crate::Error::DataPlane` (used elsewhere for data-plane errors that
/// arrive as a genuine `Err` from a dispatch call) because this variant
/// specifically represents a *successful* dispatch whose response carries a
/// logical failure (`Status::Error` + `error_code`) -- the same signal
/// `response_status_to_sqlstate` decodes today. Keeping the two separate
/// lets each protocol's caller reproduce its exact prior mapping: a real
/// dispatch `Err` maps through that protocol's generic error mapper, while a
/// staged-write rejection maps through the precise
/// `ErrorCode` -> wire-format mapping an inline status check applies.
pub enum StagingGateError {
    /// The dispatch closure itself returned an error.
    Dispatch(crate::Error),
    /// The dispatch succeeded, but the response reports a logical failure.
    /// `None` when the response carried no `error_code` (an "unknown data
    /// plane error" case).
    Rejected { code: Option<ErrorCode> },
}

/// Route a single physical task through the in-transaction staging gate.
///
/// `dispatch` is invoked ONLY for a stageable write, with a
/// `MetaOp::StageWrite` task wrapping the original plan; it must dispatch
/// that task and return the neutral `crate::Result<Response>` (i.e. the same
/// result a protocol's own single-task dispatch method produces, before any
/// protocol-specific error-to-wire mapping is applied). It is `Fn` because a
/// `ClusterArrayOp::{Put, Delete}` fans out into one staging dispatch per
/// owning vShard.
pub async fn route_in_tx_write<F, Fut>(
    state: &SharedState,
    sessions: &SessionStore,
    session_id: SessionId,
    mut task: PhysicalTask,
    dispatch: F,
) -> Result<InTxnRoute, StagingGateError>
where
    F: Fn(PhysicalTask) -> Fut,
    Fut: Future<Output = crate::Result<Response>>,
{
    if sessions.transaction_state(session_id) != TransactionState::InBlock {
        return Ok(if plan_is_write(&task.plan) {
            InTxnRoute::Autocommit(Box::new(task))
        } else {
            InTxnRoute::Read(Box::new(task))
        });
    }

    if matches!(
        &task.plan,
        PhysicalPlan::Crdt(CrdtOp::Apply { .. } | CrdtOp::ApplyAuthenticated { .. })
    ) {
        return Err(StagingGateError::Dispatch(
            crate::Error::CrdtApplyForbiddenInTransaction,
        ));
    }

    if !plan_requires_txn_buffering(&task.plan) {
        // Not buffered: it runs at the statement. Stamp the active transaction
        // id onto the task so the Data Plane can check this transaction's
        // staging overlay for read-your-own-writes on point lookups.
        task.txn_id = sessions.tx_id(session_id);
        return Ok(if plan_is_write(&task.plan) {
            InTxnRoute::Autocommit(Box::new(task))
        } else {
            InTxnRoute::Read(Box::new(task))
        });
    }

    // A distributed array write is a routing wrapper with no Data-Plane
    // handler: it is staged per owning vShard (each shard's own overlay, on
    // that shard's leader) and answers with the summed count, so the
    // cluster path gives the same statement-time contract as the
    // single-node `ArrayOp::{Put, Delete}` stage below.
    if matches!(
        &task.plan,
        PhysicalPlan::ClusterArray(ClusterArrayOp::Put { .. } | ClusterArrayOp::Delete { .. })
    ) {
        return Ok(InTxnRoute::Staged(
            stage_cluster_array_write(state, sessions, session_id, task, dispatch).await?,
        ));
    }

    // Point writes execute at STATEMENT time via the staging overlay (real
    // tag + statement-time constraint errors); the plan is still buffered so
    // COMMIT stays the sole durable apply. Other writes keep buffer + "OK",
    // reshaped first into the per-shard plans COMMIT can replay.
    if !is_stageable_write(&task.plan) {
        let buffered = expand_for_buffering(task).map_err(StagingGateError::Dispatch)?;
        for shard_task in buffered {
            sessions.buffer_write(session_id, shard_task);
        }
        return Ok(InTxnRoute::Buffered);
    }

    Ok(InTxnRoute::Staged(
        stage_write(state, sessions, session_id, task, dispatch).await?,
    ))
}

/// Wrap `plan` in a `MetaOp::StageWrite` task addressed to `vshard_id`,
/// carrying the transaction id. Shared by every staging-overlay dispatch: a
/// plain in-transaction write below, and `array_fanout_stage`'s per-shard
/// fan-out of a `ClusterArrayOp::{Put, Delete}`.
pub(super) fn wrap_stage_write(
    tenant_id: TenantId,
    vshard_id: VShardId,
    database_id: DatabaseId,
    txn_id: Option<TxnId>,
    plan: PhysicalPlan,
) -> PhysicalTask {
    PhysicalTask {
        tenant_id,
        vshard_id,
        database_id,
        plan: PhysicalPlan::Meta(MetaOp::StageWrite {
            plan: Box::new(plan),
        }),
        post_set_op: PostSetOp::None,
        txn_id,
    }
}

/// Stage a stageable write into the per-transaction overlay and classify its
/// outcome. Split out of [`route_in_tx_write`] to keep that function short.
///
/// Visible to the `session` module so the statement-time MERGE expander
/// ([`super::expander_stage`]) can stage each of the concrete point ops it
/// derives through the exact same overlay-dispatch + buffer path a plain
/// in-transaction point write uses — no separate staging code to drift.
pub(super) async fn stage_write<F, Fut>(
    state: &SharedState,
    sessions: &SessionStore,
    session_id: SessionId,
    task: PhysicalTask,
    dispatch: F,
) -> Result<StagedWriteOutcome, StagingGateError>
where
    F: FnOnce(PhysicalTask) -> Fut,
    Fut: Future<Output = crate::Result<Response>>,
{
    let plan = &task.plan;
    let Some(shape) = stageable_write_shape(plan) else {
        return Err(StagingGateError::Dispatch(crate::Error::Internal {
            detail: format!("stage_write requires a stageable write; got {plan:?}"),
        }));
    };

    let stage_task = wrap_stage_write(
        task.tenant_id,
        task.vshard_id,
        task.database_id,
        sessions.tx_id(session_id),
        task.plan.clone(),
    );

    // Resolved once and reused for both the admission check below and the
    // metering charge after dispatch, instead of looking up the identity and
    // rebuilding the scope twice for the same statement.
    let identity = sessions.identity(session_id);
    let scope = identity.as_ref().map(|identity| {
        RequestAuthScope::for_database(identity, state.auth_stores(), task.database_id)
    });

    // A spent hard quota refuses the staged write before it touches the
    // overlay. This mirrors the charge at the bottom of this function, which
    // is on the success path and so can never refuse anything itself — and
    // like that charge, gating here covers every `Staged` route at once
    // rather than being duplicated in each caller's dispatch closure.
    if state.metering_config.enabled
        && let Some(scope) = &scope
    {
        let info = PlanMeteringInfo::extract(&task.plan);
        admit_quota_for_dispatch(state, scope, &info).map_err(StagingGateError::Dispatch)?;
    }

    // Stage on the vShard's CURRENT leader. When this node leads the vShard (or
    // single-node), the existing local dispatch runs byte-identically; otherwise
    // the wrapped `StageWrite` is forwarded to the remote leader keyed by
    // `txn_id`, so the overlay is populated on the same node a later
    // read-your-own-writes read resolves to. `LeaderUnknown` fails closed inside
    // `forward_to_leader` (→ `Error::NotLeader`), never a local fallback.
    // The descriptor version set is computed from the INNER write (`task.plan`),
    // not the `Meta(StageWrite)` wrapper, so the leader's OCC check sees the
    // touched collection's version.
    let resp = match resolve_leader(&stage_task, state) {
        RouteDecision::Local => dispatch(stage_task)
            .await
            .map_err(StagingGateError::Dispatch)?,
        remote => forward_to_leader(state, remote, stage_task, &task.plan)
            .await
            .map_err(StagingGateError::Dispatch)?,
    };

    if resp.status == Status::Error {
        return Err(StagingGateError::Rejected {
            code: resp.error_code.as_deref().cloned(),
        });
    }

    // Metered here, once the staging dispatch above has already succeeded.
    // The per-transaction overlay write it just performed IS the real engine
    // work a `Staged` in-transaction write does — COMMIT only decides
    // whether that already-billed work becomes durable or is discarded by a
    // ROLLBACK, not whether it happened. Every `Staged` route funnels
    // through this one function — this file's own `route_in_tx_write` for a
    // plain in-transaction point write, and `expander_stage::
    // stage_and_aggregate`'s per-op staging for an in-transaction `MERGE` /
    // `UPDATE ... FROM` / `INSERT ... SELECT` — so metering here covers all
    // of them without duplicating the call in every caller's dispatch
    // closure. Compare the sibling non-stageable `Buffered` route in
    // `route_in_tx_write` above: that one performs no dispatch at all until
    // COMMIT, so it is metered there instead
    // (`session::commit::metering::meter_committed_buffered_writes`).
    //
    // `identity` is `None` only for a session that reached this point (inside
    // a transaction block, mid-write) with no identity ever recorded — not
    // reachable in practice, since every path that can enter `InBlock` state
    // authenticates first. Metering must never fail a request, so a missing
    // identity just skips the (impossible) charge rather than panicking.
    if let Some(scope) = &scope {
        meter_staged_write(state, scope, &task.plan, &resp);
    }

    let kind = shape.tag_kind(resp.payload.as_ref());

    // Every count-bearing stage handler answers with a real count
    // (`stage_count_response`), so a missing one means a staging handler stopped
    // reporting — surface it instead of assuming the statement touched a row.
    //
    // Two outcomes carry no count: `RawPayload` (the atomic KV ops `Incr` /
    // `IncrFloat` / `Cas` / `GetSet` / `Transfer` answer with a computed VALUE,
    // which the caller reads from `payload`) and `Truncate` (the tag is bare,
    // matching autocommit). `affected` is never rendered for either, so there
    // is nothing to require and nothing to assume.
    let affected = if matches!(kind, StagedTagKind::RawPayload | StagedTagKind::Truncate) {
        0
    } else {
        require_affected_count(resp.payload.as_ref()).map_err(StagingGateError::Dispatch)? as usize
    };
    let payload = resp.payload.as_ref().to_vec();

    // Durable path unchanged: still buffered, replayed at COMMIT.
    sessions.buffer_write(session_id, task);

    Ok(StagedWriteOutcome {
        kind,
        affected,
        payload,
    })
}
