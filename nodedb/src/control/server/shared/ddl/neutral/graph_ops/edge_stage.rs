// SPDX-License-Identifier: BUSL-1.1

//! In-transaction staging for graph-edge writes issued through the
//! `GRAPH ... EDGE` DSL.
//!
//! The `GRAPH INSERT EDGE` / `GRAPH DELETE EDGE` handlers dispatch a single
//! `GraphOp::EdgePut` / `EdgeDelete` directly to the Data Plane in autocommit.
//! Inside an explicit `BEGIN..COMMIT` block that direct dispatch would apply
//! the write DURABLY at statement time, so an in-transaction `MATCH` / `GRAPH
//! NEIGHBORS` would not observe it as staged (breaking read-your-own-writes)
//! and a ROLLBACK could not undo it. These helpers instead route the write
//! through the protocol-neutral staging gate
//! ([`route_in_tx_write`](crate::control::server::shared::session::staging_gate::route_in_tx_write)),
//! exactly like every other in-transaction point write: the Data Plane stages
//! the edge into the per-transaction `GraphTxnOverlay` (merged by Neighbors /
//! Hop for RYOW), the plan is buffered for COMMIT's durable replay, and
//! ROLLBACK drops the overlay.
//!
//! A SINGLE-HOME edge (both endpoints on one vShard, or single-node) stages
//! once into `vsrc`. A cross-shard (dual-home) edge is reachable from BOTH
//! endpoints, and each core merges only its OWN overlay on a read, so
//! [`stage_edge_dual_home`] stages the same edge into both the `vsrc` and `vdst`
//! overlays — read-your-own-writes works from either endpoint, and the touched
//! vShard set records both homes so ROLLBACK tears down both.

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::shared::session::DmlTxnCtx;
use crate::control::server::shared::session::staging_gate::{
    InTxnRoute, StagingGateError, route_in_tx_write,
};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId, VShardId};
use nodedb_physical::physical_plan::GraphOp;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::super::super::result::DdlError;
use super::edge::EdgeHomes;
use super::support::ddl_err;

/// Stage a graph-edge write into the active transaction's overlay on EVERY
/// vShard the edge homes to.
///
/// A single-home edge (both endpoints on one vShard, or single-node) stages
/// once, into `vsrc`. A cross-shard (dual-home) edge is reachable from BOTH
/// endpoints, and each Data-Plane core merges only its OWN transaction overlay
/// on a read — so a reverse/IN traversal that scatters to `from_key(dst)` would
/// never observe an edge staged only on `vsrc`. The dual-home case therefore
/// stages the SAME `GraphOp` into both the `vsrc` and `vdst` overlays, giving
/// read-your-own-writes from either endpoint. Each stage buffers its task, so
/// the session's touched-vShard set records both homes and ROLLBACK fans
/// `DropTxnOverlay` to both; COMMIT routes the buffered 2-vShard set through the
/// existing cross-shard commit path.
///
/// Caller invariant: the session is `InBlock`.
///
/// Returns the affected count the DSL caller renders in its command tag. A
/// dual-home edge stages the identical op into both endpoint overlays, so the
/// count is read from the `vsrc` home only — the canonical owner of logical
/// edge cardinality (mirrors `owns_logical_edge_stats` in the Data Plane).
pub(super) async fn stage_edge_dual_home(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    homes: EdgeHomes,
    op: GraphOp,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Result<u64, DdlError> {
    let EdgeHomes {
        vsrc,
        vdst,
        single_home,
    } = homes;

    if single_home {
        return stage_edge_write_in_txn(
            state,
            tenant_id,
            database_id,
            vsrc,
            PhysicalPlan::Graph(op),
            txn_ctx,
        )
        .await;
    }

    // Dual-home: stage the same edge into both endpoint overlays.
    let affected = stage_edge_write_in_txn(
        state,
        tenant_id,
        database_id,
        vsrc,
        PhysicalPlan::Graph(op.clone()),
        txn_ctx,
    )
    .await?;
    stage_edge_write_in_txn(
        state,
        tenant_id,
        database_id,
        vdst,
        PhysicalPlan::Graph(op),
        txn_ctx,
    )
    .await?;
    Ok(affected)
}

/// Stage a graph-edge write (`GraphOp::EdgePut` / `EdgeDelete`) into the active
/// transaction's overlay on ONE `vshard` through the neutral staging gate.
///
/// Caller invariant: the session is `InBlock` and `plan` is a stageable
/// `GraphOp` write. Returns the staged write's affected count once staged +
/// buffered; a staging rejection or dispatch failure maps to a [`DdlError`].
pub(super) async fn stage_edge_write_in_txn(
    state: &SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    vshard: VShardId,
    plan: PhysicalPlan,
    txn_ctx: &DmlTxnCtx<'_>,
) -> Result<u64, DdlError> {
    let task = PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        database_id,
        plan,
        post_set_op: PostSetOp::None,
        txn_id: None,
    };

    let routed = route_in_tx_write(
        state,
        txn_ctx.sessions,
        txn_ctx.session_id,
        task,
        |staged| {
            crate::control::server::dispatch_utils::dispatch_to_data_plane_with_txn(
                state,
                staged.tenant_id,
                staged.database_id,
                staged.vshard_id,
                staged.plan,
                TraceId::ZERO,
                staged.txn_id,
            )
        },
    )
    .await;

    match routed {
        Ok(InTxnRoute::Staged(outcome)) => Ok(outcome.affected as u64),
        // Edge writes are stageable (`is_stageable_write`), so inside a
        // transaction block the gate always returns `Staged`. `Read` (not in a
        // block) and `Buffered` (non-stageable write) cannot occur for a
        // caller that already checked `InBlock`; there is no affected count to
        // report for either, so treat them as a no-op tag rather than panicking.
        Ok(InTxnRoute::Read(_)) | Ok(InTxnRoute::Buffered) => Ok(0),
        Err(StagingGateError::Dispatch(e)) => Err(ddl_err("XX000", e.to_string())),
        Err(StagingGateError::Rejected { code }) => {
            let (_, sqlstate, message) = match code {
                Some(code) => {
                    crate::control::server::shared::ddl::sqlstate::error_code_to_sqlstate(&code)
                }
                None => ("ERROR", "XX000", "unknown data plane error".to_owned()),
            };
            Err(ddl_err(sqlstate, message))
        }
    }
}
