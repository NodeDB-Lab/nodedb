// SPDX-License-Identifier: BUSL-1.1

//! Collect the Data Plane's response, classify the outcome, and run the
//! post-apply steps a successful write still owes: the post-apply redo, the
//! durable-at-ack barrier, DDL finalization, and the change-event publish.

use std::sync::Arc;
use std::time::Instant;

use crate::bridge::envelope::Status;
use crate::control::array_catalog::ddl::AuthorizedDdlTransition;
use crate::control::server::dispatch_utils::change_events::{WriteChangeSet, publish_change_set};
use crate::control::server::dispatch_utils::collect::{
    DispatchCollectError, collect_bounded_response,
};
use crate::control::server::dispatch_utils::durability_barrier::assert_durable_before_ack;
use crate::control::server::dispatch_utils::minted::{
    Collect, MintedRecords, OwnedResponse, OwnedWait, RecordOwner, await_response_owned,
};
use crate::control::server::dispatch_utils::submit_write::ambiguous_ddl::preserve_ambiguous_array_ddl;
use crate::control::server::wal_dispatch;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, Lsn, RequestId, TenantId, VShardId};

use super::super::params::SubmitOutcome;
use super::wal_append::rollback_on_err;

/// Everything the response phase needs, gathered from the admission, WAL
/// append, and dispatch phases that ran before it.
pub(super) struct ResponsePhaseInput {
    pub request_id: RequestId,
    pub rx: crate::control::ResponseReceiver,
    pub deadline: Instant,
    pub dispatch_started: Instant,
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    pub vshard_id: VShardId,
    pub wal_lsn: Option<Lsn>,
    pub appends_here: bool,
    /// The idempotency key every record this write appends carries (see
    /// `WalDurability::AppendHere`).
    pub apply_key: u64,
    /// The key a final refusal's abort marker carries, `0` when this write's
    /// refusals are not final. A final refusal is the proposal's outcome: the
    /// proposal ledger rebuilt at boot counts the key as applied.
    pub final_refusal_key: u64,
    pub post_apply: Option<String>,
    pub funnel_redo_engine: Option<&'static str>,
    pub change_set: Option<WriteChangeSet>,
    pub ddl_transition: AuthorizedDdlTransition,
    pub deferred_guards: super::dispatch::DeferredGuards,
    /// The records minted for this write, under their outcome-floor window.
    pub minted: Option<MintedRecords>,
}

/// Collect the response(s), classify the outcome, and run every step a
/// completed write still owes before the funnel returns.
///
/// For non-streaming queries, exactly one response arrives. For streaming
/// queries, multiple partial chunks arrive before the final. The partial
/// channel is bounded (see `RequestTracker::register`); here the *total* accumulated
/// payload is additionally capped so a runaway scan can't pin Control-Plane
/// RAM — any query whose combined result exceeds
/// `tuning.network.max_query_result_bytes` is cancelled with a typed
/// `ExecutionLimitExceeded` error.
pub(super) async fn collect_classify_and_finish(
    shared: &SharedState,
    max_result_bytes: usize,
    input: ResponsePhaseInput,
) -> crate::Result<SubmitOutcome> {
    let ResponsePhaseInput {
        request_id,
        rx,
        deadline,
        dispatch_started,
        tenant_id,
        database_id,
        vshard_id,
        wal_lsn,
        appends_here,
        apply_key,
        final_refusal_key,
        post_apply,
        funnel_redo_engine,
        change_set,
        ddl_transition,
        deferred_guards,
        minted,
    } = input;
    let owner = RecordOwner {
        tenant_id,
        database_id,
        vshard_id,
    };

    let vshard_u32 = vshard_id.as_u32();
    let observe = |shared: &SharedState| {
        let latency_us = dispatch_started.elapsed().as_micros().min(u64::MAX as u128) as u64;
        shared.per_vshard_metrics.observe(vshard_u32, latency_us);
    };

    // Wait to the same instant the envelope carries. The Data Plane normally
    // answers with `DeadlineExceeded` first; this bounds the wait when it is
    // inside a stage that carries no safe point yet. A write's records close
    // in a task this future does not own, so a caller dropped mid-wait still
    // closes them. A refusal that arrives after the deadline still cancels
    // them.
    let outcome = match minted {
        Some(minted) => {
            await_response_owned(
                OwnedWait {
                    wal: Arc::clone(&shared.wal),
                    owner,
                    final_refusal_key,
                    deadline,
                    collect: Collect::Merged { max_result_bytes },
                },
                rx,
                minted,
            )
            .await?
        }
        None => collect_unminted(shared, request_id, rx, deadline, max_result_bytes).await,
    };

    let response = match outcome {
        OwnedResponse::Answered { response, closed } => {
            if response.status != Status::Ok {
                let _ = ddl_transition.rollback(shared);
            }
            // A failed cancel holds the window and fails the write here.
            closed?;
            response
        }
        OwnedResponse::DeadlineExceeded => {
            observe(shared);
            // Dispatch completed, but the Data Plane may have applied CREATE
            // or ALTER before this deadline. Never roll that catalog state
            // back on an ambiguous post-enqueue outcome.
            preserve_ambiguous_array_ddl(shared, &ddl_transition);
            if !ddl_transition.preserves_on_ambiguous_apply() {
                let _ = ddl_transition.rollback(shared);
            }
            return Err(crate::Error::DeadlineExceeded { request_id });
        }
        OwnedResponse::OverBudget { bytes } => {
            observe(shared);
            // A partial response proves dispatch began but not whether an
            // Array DDL completed; preserve CREATE/ALTER and fail-stop.
            preserve_ambiguous_array_ddl(shared, &ddl_transition);
            if !ddl_transition.preserves_on_ambiguous_apply() {
                let _ = ddl_transition.rollback(shared);
            }
            return Err(crate::Error::ExecutionLimitExceeded {
                detail: format!(
                    "query result exceeded max_query_result_bytes \
                     ({bytes} > {max_result_bytes} bytes)"
                ),
            });
        }
        OwnedResponse::ChannelClosed => {
            observe(shared);
            // The producer can close after applying but before sending its
            // response. CREATE/ALTER must remain catalog-finalized here.
            preserve_ambiguous_array_ddl(shared, &ddl_transition);
            if !ddl_transition.preserves_on_ambiguous_apply() {
                let _ = ddl_transition.rollback(shared);
            }
            // A producer that stopped after the deadline stopped because the
            // statement ran out of time. Reporting the closure would hand the
            // client an internal error for its own timeout.
            if std::time::Instant::now() >= deadline {
                return Err(crate::Error::DeadlineExceeded { request_id });
            }
            return Err(crate::Error::Dispatch {
                detail: "response channel closed".into(),
            });
        }
    };

    // Mint the post-apply redo record while the guards are still held, then
    // release them. A PointUpdate whose collection carries a secondary vector
    // index returns its surrogate + post-image in `write_set`; without this
    // durable `Put` a WAL-only restart rebuilds the HNSW from the pre-update body
    // and resurrects the old embedding.
    let post_apply_lsn = if let Some(collection) = &post_apply
        && appends_here
        && response.status == Status::Ok
    {
        rollback_on_err(
            shared,
            &ddl_transition,
            wal_dispatch::append_write_set_redo(
                shared.wal.appender(apply_key),
                tenant_id,
                vshard_id,
                database_id,
                collection,
                &response.write_set,
            ),
        )?
    } else {
        None
    };
    drop(deferred_guards);

    // Durable-at-ack barrier: an acknowledged write must be WAL-fsync-durable
    // before this response (the client ack) returns. `WalAppender::append_*` only
    // buffers the record and mints its `Lsn`; without this barrier a `kill -9`
    // loses the buffered bytes, which is invisible for engines whose rows are
    // committed durably by redb but silently destroys every engine whose only
    // durability path is WAL replay: the KV hash tables, the HNSW graphs, the
    // columnar / timeseries memtables, the graph node labels, the CRDT states,
    // and the FTS index. `wal_lsn` is the forward write's LSN — minted above
    // under the admission guard for a write that owns its durability, or supplied
    // by a caller that appended upstream (procedural batch flush,
    // interactive-COMMIT transaction redo). `post_apply_lsn` covers the
    // post-apply redo appended just above. Both records are already buffered in
    // the shared WAL; one group-commit fsync coalesces concurrent writers (see
    // `WalManager::wait_durable`), and it runs here — after the admission guards
    // are released — so it never serializes same-key throughput. Reads / control
    // ops / trigger / staged-write dispatch carry no LSN and skip the barrier;
    // `durability_barrier` decides which of those skips are legitimate and makes
    // the rest loud instead of letting them ack a write nothing can recover.
    if response.status == Status::Ok {
        let durable_target = match (wal_lsn, post_apply_lsn) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        };
        match durable_target {
            Some(lsn) => {
                rollback_on_err(shared, &ddl_transition, shared.wal.wait_durable(lsn).await)?
            }
            // Nothing to fsync. Legitimate for most plans, but if this funnel
            // was the one required to mint the record, the ack below promises
            // durability the engine cannot deliver — silent until a `kill -9`
            // proves it, hence the check.
            None => assert_durable_before_ack(funnel_redo_engine),
        }
    }

    if response.status == Status::Ok {
        ddl_transition.finalize(shared)?;
    }

    // Publish change events for successful writes whose change feed this funnel
    // owns. `None` is a caller whose change feed is `Unowned` — see
    // [`super::super::params::ChangeFeedOwner`] for why the node that applies those
    // writes is not the node that publishes them.
    if response.status == Status::Ok {
        if let Some(change_set) = change_set {
            publish_change_set(shared, tenant_id, database_id, change_set, &response);
        }

        // Advance the tenant's observed write-HLC high-water on any successful
        // dispatch. Used by the RESTORE staleness gate. Advancing on every
        // success (not just writes) is intentionally conservative —
        // envelope.watermark is captured AFTER fan-out so it always dominates
        // the tenant_wm of a fresh backup.
        shared.advance_tenant_write_hlc(tenant_id.as_u64());
    }

    observe(shared);
    Ok(SubmitOutcome { response, wal_lsn })
}

/// Collect a response that carries no records, under the same deadline and
/// byte budget a write's owned wait applies.
async fn collect_unminted(
    shared: &SharedState,
    request_id: RequestId,
    mut rx: crate::control::ResponseReceiver,
    deadline: Instant,
    max_result_bytes: usize,
) -> OwnedResponse {
    let collected = tokio::time::timeout_at(
        tokio::time::Instant::from_std(deadline),
        collect_bounded_response(&mut rx, max_result_bytes),
    )
    .await;
    match collected {
        Ok(Ok(response)) => OwnedResponse::Answered {
            response,
            closed: Ok(()),
        },
        Ok(Err(DispatchCollectError::OverBudget { bytes })) => {
            shared.tracker.cancel(&request_id);
            OwnedResponse::OverBudget { bytes }
        }
        Ok(Err(DispatchCollectError::ChannelClosed)) => OwnedResponse::ChannelClosed,
        Err(_) => OwnedResponse::DeadlineExceeded,
    }
}
