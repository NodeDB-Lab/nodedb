// SPDX-License-Identifier: BUSL-1.1

//! Submit a gate-rejected write through the deterministic Calvin scheduler.
//!
//! When [`admit`](super::admit) returns [`WriteAdmission::RouteToCalvin`](super::WriteAdmission),
//! the caller hands the single write here. A data-group leader's gate routes a
//! replicated write here too, through its proposer. Two shapes reach this
//! path, each routed through the existing Calvin entry point that builds a
//! VALID write set for it:
//!
//! - A **write with a static write set** whose key a pending commit holds (a
//!   row write, a collection-wide write, an edge on several homes): submitted
//!   as a single-vshard [`build_single_vshard_tx_class`] +
//!   [`submit_calvin_routed`]. A write on one vshard uses the single-vshard
//!   opt-in rather than the strict multi-vshard builder. The scheduler
//!   acquires its keys on the SAME lock table the gate probed, queues FIFO
//!   behind the holder, and applies it once released.
//! - A **predicate write** (`BulkUpdate` / `BulkDelete` on a SINGLE
//!   collection): its write set is not statically known, so it goes through
//!   [`dispatch_dependent_edge_recon`], which runs the pre-exec
//!   reconnaissance scan to discover the affected surrogates and commits the
//!   dependent Calvin transaction. Its dependent builder accepts a write set
//!   on one vshard, so a single collection sequences through the scheduler.
//!
//! Either way the applied [`Response`] (carrying any RETURNING rows) is returned
//! so the caller surfaces it in place of a fast dispatch.
//!
//! # Boxed future — breaks the async-recursion cycle
//!
//! The Calvin path this reaches (recon / routed submit) can, in turn, dispatch
//! writes back through the same autocommit funnel that called here — an async
//! recursion cycle whose future will otherwise be infinitely sized. Returning a
//! `Pin<Box<dyn Future>>` heap-boxes exactly one edge of that cycle, giving every
//! future in the strongly-connected group a finite size. The box is paid ONLY on
//! this cold routed path; the uncontended fast path never calls this function.

use std::future::Future;
use std::pin::Pin;

use crate::bridge::envelope::Response;
use crate::control::planner::calvin::{
    build_single_vshard_tx_class, dispatch_dependent_edge_recon, is_dependent_predicate,
    submit_calvin_routed,
};
use crate::control::state::SharedState;
use crate::event::EventSource;
use crate::types::{DatabaseId, RequestId, TenantId, VShardId};
use nodedb_physical::physical_plan::PhysicalPlan;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

/// Synthesize the bare `Ok` command-tag response for a Calvin-routed write
/// that carried no RETURNING rows (i.e. [`route_write_to_calvin`] resolved to
/// `None`). Every caller of `route_write_to_calvin` needs this same fallback,
/// so it lives here once instead of being reconstructed at each call site.
pub fn bare_ok_response(request_id: RequestId) -> Response {
    Response {
        request_id,
        status: crate::bridge::envelope::Status::Ok,
        attempt: 1,
        partial: false,
        payload: crate::bridge::envelope::Payload::from_vec(Vec::new()),
        watermark_lsn: crate::types::Lsn::ZERO,
        error_code: None,
        stage_vote: None,
        read_versions: crate::types::ReadVersions::new(),
        write_set: Vec::new(),
    }
}

/// Whether the scheduler applies `plan` exactly as its writer asked when the
/// gate routes it. A static write set carries `event_source`. The
/// reconnaissance of a predicate write applies it as a client write. No
/// route carries a restore id. A write the route cannot keep waits for its
/// keys at the gate instead.
pub fn calvin_route_keeps(plan: &PhysicalPlan, event_source: EventSource, restore_id: u64) -> bool {
    restore_id == 0 && (matches!(event_source, EventSource::User) || !is_dependent_predicate(plan))
}

/// Route one write to the deterministic scheduler and return the applied
/// `Response`. `None` for a plain write with no RETURNING rows — the caller then
/// synthesizes its normal command-tag response.
///
/// A static write set applies under `event_source` on every replica. The
/// gate routes a predicate write only when it is a client's (see
/// [`calvin_route_keeps`]).
///
/// Returns a boxed future (rather than an `async fn`) to break the async
/// recursion cycle described in the module docs.
pub fn route_write_to_calvin<'a>(
    shared: &'a SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
    event_source: EventSource,
) -> Pin<Box<dyn Future<Output = crate::Result<Option<Response>>> + Send + 'a>> {
    Box::pin(async move {
        let task = PhysicalTask {
            tenant_id,
            vshard_id,
            database_id,
            plan,
            post_set_op: PostSetOp::None,
            txn_id: None,
        };

        // Predicate writes have no statically-known write set: discover it via the
        // dependent reconnaissance path, which builds a valid dependent TxClass.
        //
        // This write reaches here ONLY because `admit` returned `RouteToCalvin`:
        // a pending commit already holds a key in the predicate's range. A
        // single-collection predicate targets a single vshard, which the recon
        // dispatch's dependent builder accepts.
        if is_dependent_predicate(&task.plan) {
            let recon =
                dispatch_dependent_edge_recon(shared, vec![task], tenant_id, database_id).await?;
            return Ok(recon.apply_result);
        }

        // A static write set: build a static TxClass and submit it.
        //
        // This write reaches here ONLY because a gate returned `RouteToCalvin`:
        // a pending commit already holds one of its keys. A write on a single
        // vshard must be built with the single-vshard opt-in — it sequences
        // through the scheduler to serialize on the SAME shared per-vShard
        // `LockManager` the holder is on, rather than being rejected as a
        // (spuriously) single-vshard multi-shard dispatch. The gate never
        // routes an append (timeseries ingest, columnar insert), so no
        // unresolved ingest reaches the builder.
        let mut tx_class =
            build_single_vshard_tx_class(std::slice::from_ref(&task), tenant_id, &[])?;
        tx_class.set_event_source(event_source.wal_code());
        submit_calvin_routed(shared, tx_class).await
    })
}
