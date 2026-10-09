// SPDX-License-Identifier: BUSL-1.1

//! Submit a gate-rejected write through the deterministic Calvin scheduler.
//!
//! When [`admit`](super::admit) returns [`WriteAdmission::RouteToCalvin`](super::WriteAdmission),
//! the caller hands the single write here. Two shapes reach this path, each
//! routed through the existing Calvin entry point that builds a VALID write set
//! for it:
//!
//! - A **point write** whose key a pending commit holds (Document / KV / Vector /
//!   single-home edge): submitted as a single-vshard
//!   [`build_single_vshard_tx_class`] + [`submit_calvin_routed`]. Because it
//!   targets one vshard, it uses the single-vshard opt-in rather than the strict
//!   multi-vshard builder. The scheduler acquires its key on the SAME lock
//!   table the gate probed, queues FIFO behind the holder, and applies it once
//!   released.
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
        read_version_lsn: crate::types::Lsn::ZERO,
        write_set: Vec::new(),
    }
}

/// Route one write to the deterministic scheduler and return the applied
/// `Response`. `None` for a plain write with no RETURNING rows — the caller then
/// synthesizes its normal command-tag response.
///
/// Returns a boxed future (rather than an `async fn`) to break the async
/// recursion cycle described in the module docs.
pub fn route_write_to_calvin<'a>(
    shared: &'a SharedState,
    tenant_id: TenantId,
    database_id: DatabaseId,
    vshard_id: VShardId,
    plan: PhysicalPlan,
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

        // Point write: its key is known, so build a static TxClass and submit it.
        //
        // This write reaches here ONLY because `admit` returned `RouteToCalvin`:
        // a pending commit already holds its key. A point write targets a single
        // vshard, so it must be built with the single-vshard opt-in — it sequences
        // through the scheduler to serialize on the SAME shared per-vShard
        // `LockManager` the holder is on, rather than being rejected as a
        // (spuriously) single-vshard multi-shard dispatch.
        // A timeseries ingest resolves to its rows here, before it is
        // sequenced: every replica resolves a sequenced write on its own.
        let resolved = crate::control::write_resolve::resolve_tasks_for_log(
            shared,
            std::slice::from_ref(&task),
        )
        .await?;
        let tasks = resolved.unwrap_or_else(|| vec![task]);
        // The submitter's resolve knows the rows the ingest stores and the
        // lines it rejected. The client reads both from this answer, not
        // from the scheduler's.
        let counts = match tasks.first() {
            Some(task) => crate::control::write_resolve::resolved_ingest_counts(&task.plan)?,
            None => None,
        };
        let tx_class = build_single_vshard_tx_class(&tasks, tenant_id, &[])?;
        let applied = submit_calvin_routed(shared, tx_class).await?;
        Ok(with_ingest_counts(applied, counts))
    })
}

/// `applied`, carrying the resolved ingest's counts when there are any. The
/// apply's counts win: its install rejected the rows that conflict with the
/// schema at its position. The resolve's `counts` answer only when the
/// apply's answer carries none. An error answer stays as the scheduler gave
/// it.
fn with_ingest_counts(applied: Option<Response>, counts: Option<Vec<u8>>) -> Option<Response> {
    let Some(counts) = counts else {
        return applied;
    };
    match applied {
        Some(response) if response.status != crate::bridge::envelope::Status::Ok => Some(response),
        Some(response)
            if crate::control::write_resolve::carries_applied_ingest_counts(
                response.payload.as_bytes(),
            ) =>
        {
            Some(response)
        }
        Some(response) => Some(Response {
            payload: crate::bridge::envelope::Payload::from_vec(counts),
            ..response
        }),
        None => {
            let mut response = bare_ok_response(RequestId::new(0));
            response.payload = crate::bridge::envelope::Payload::from_vec(counts);
            Some(response)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn counts(accepted: u64, rejected: u64) -> Vec<u8> {
        nodedb_types::json_to_msgpack(&serde_json::json!({
            "accepted": accepted,
            "rejected": rejected,
            "collection": "metrics",
        }))
        .expect("encode counts")
    }

    fn decoded(response: &Response) -> serde_json::Value {
        let json = crate::data::executor::response_codec::decode_payload_to_json(
            response.payload.as_bytes(),
        );
        sonic_rs::from_str(&json).expect("decode counts")
    }

    /// A Calvin-routed timeseries ingest answers the rejected count its
    /// submitter's resolve found, whether or not the scheduler answered.
    #[test]
    fn a_routed_ingest_answers_its_resolved_counts() {
        let bare = with_ingest_counts(None, Some(counts(2, 1))).expect("an answer");
        assert_eq!(decoded(&bare)["rejected"], serde_json::json!(1));

        let applied = with_ingest_counts(
            Some(bare_ok_response(RequestId::new(3))),
            Some(counts(2, 1)),
        )
        .expect("an answer");
        assert_eq!(applied.request_id, RequestId::new(3));
        assert_eq!(decoded(&applied)["accepted"], serde_json::json!(2));
        assert_eq!(decoded(&applied)["rejected"], serde_json::json!(1));
    }

    /// The Calvin completion hands back the flush's answer. When it carries
    /// the install's counts, the client gets the apply's count: here the
    /// install rejected one row the resolve accepted.
    #[test]
    fn a_routed_ingest_answers_the_apply_counts_when_the_flush_carries_them() {
        use crate::engine::timeseries::install_counts::{TsInstallCount, TsInstallCounts};
        let mut flushed = bare_ok_response(RequestId::new(4));
        flushed.payload = crate::bridge::envelope::Payload::from_vec(
            TsInstallCounts::new(vec![TsInstallCount {
                collection: "metrics".into(),
                accepted: 1,
                rejected: 2,
            }])
            .to_bytes()
            .expect("encode install counts"),
        );
        let applied = with_ingest_counts(Some(flushed), Some(counts(2, 1))).expect("an answer");
        assert_eq!(decoded(&applied)["accepted"], serde_json::json!(1));
        assert_eq!(decoded(&applied)["rejected"], serde_json::json!(2));
        assert_eq!(
            decoded(&applied)["collection"],
            serde_json::json!("metrics")
        );
    }

    /// A routed write that is not a resolved ingest keeps the scheduler's
    /// answer.
    #[test]
    fn a_routed_non_ingest_keeps_the_scheduler_answer() {
        assert!(with_ingest_counts(None, None).is_none());
    }
}
