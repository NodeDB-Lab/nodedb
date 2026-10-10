// SPDX-License-Identifier: BUSL-1.1

//! Collect the Data Plane's response, classify the outcome, and run the
//! post-apply steps a successful write still owes: the post-apply redo, the
//! durable-at-ack barrier, and the change-event publish.

use std::time::Instant;

use crate::bridge::envelope::Status;
use crate::control::server::dispatch_utils::change_events::PendingChanges;
use crate::control::server::dispatch_utils::durability_barrier::assert_durable_before_ack;
use crate::control::server::dispatch_utils::minted::OwnedResponse;
use crate::control::server::wal_dispatch;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, Lsn, RequestId, TenantId, VShardId};

use super::super::params::SubmitOutcome;
use super::answer::Answer;

/// Everything the response phase needs, gathered from the admission, WAL
/// append, and dispatch phases that ran before it.
pub(super) struct ResponsePhaseInput {
    pub request_id: RequestId,
    /// Where the response phase reads the answer.
    pub answer: Answer,
    /// The byte budget of the merged response.
    pub max_result_bytes: usize,
    pub deadline: Instant,
    pub dispatch_started: Instant,
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    pub vshard_id: VShardId,
    pub wal_lsn: Option<Lsn>,
    /// The origin of the write's record group, `Some` for a write this
    /// funnel journalled that stores rows its apply decides. Such a write owes
    /// the parts of its group. A write whose records live elsewhere, such as
    /// a WAL record replayed again, journals nothing here.
    pub group_origin: Option<wal_dispatch::GroupOrigin>,
    /// The commit instant every record of the write carries, when it was
    /// known before the append. See `AppendScope::commit_hlc`.
    pub upstream_commit_hlc: Option<u64>,
    /// The idempotency key every record this write appends carries (see
    /// `WalDurability::AppendHere`).
    pub apply_key: u64,
    /// The event source the write runs with. Its post-apply redo records
    /// carry it.
    pub event_source: crate::event::EventSource,
    pub post_apply: Option<String>,
    pub funnel_redo_engine: Option<&'static str>,
    pub change_set: Option<PendingChanges>,
    pub deferred_guards: super::dispatch::DeferredGuards,
    /// `Some` when the plan writes user data, so a success advances the
    /// tenant's observed write-HLC. Reads and system operations never do.
    pub user_write: Option<UserWriteMark>,
    /// Where a grouped write journals its parts when its final response
    /// arrives after the caller stopped waiting.
    pub late_parts: Option<super::late_parts::LateParts>,
}

/// The origin a successful user data write records on the tenant's observed
/// write-HLC.
pub(super) struct UserWriteMark {
    /// Which funnel path dispatched the write.
    pub site: &'static str,
    /// The collection the plan wrote, when it named one.
    pub collection: Option<String>,
    /// HLC wall time, in nanoseconds, at which the write committed.
    pub commit_hlc: u64,
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
    input: ResponsePhaseInput,
) -> crate::Result<SubmitOutcome> {
    let ResponsePhaseInput {
        request_id,
        answer,
        max_result_bytes,
        deadline,
        dispatch_started,
        tenant_id,
        database_id,
        vshard_id,
        wal_lsn,
        group_origin,
        upstream_commit_hlc,
        apply_key,
        event_source,
        post_apply,
        funnel_redo_engine,
        change_set,
        deferred_guards,
        user_write,
        late_parts,
    } = input;

    let vshard_u32 = vshard_id.as_u32();
    let observe = |shared: &SharedState| {
        let latency_us = dispatch_started.elapsed().as_micros().min(u64::MAX as u128) as u64;
        shared.per_vshard_metrics.observe(vshard_u32, latency_us);
    };

    // Wait to the same instant the envelope carries. The Data Plane normally
    // answers with `DeadlineExceeded` first; this bounds the wait when it is
    // inside a stage that carries no safe point yet. A write's records close
    // in the task the enqueue handed them to, so a caller dropped here still
    // closes them. A refusal that arrives after the deadline still cancels
    // them.
    let outcome = answer.wait().await?;

    let response = match outcome {
        OwnedResponse::Answered { response, closed } => {
            // A failed cancel holds the window and fails the write here.
            closed?;
            *response
        }
        OwnedResponse::DeadlineExceeded => {
            observe(shared);
            // The core can still apply the write: its guards stay held until
            // its parts are journalled.
            if let Some(late) = late_parts {
                late.spawn(deferred_guards);
            }
            return Err(crate::Error::DeadlineExceeded { request_id });
        }
        OwnedResponse::OverBudget { bytes } => {
            observe(shared);
            if let Some(late) = late_parts {
                late.spawn(deferred_guards);
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
            // A producer that stopped after the deadline stopped because the
            // statement ran out of time. Reporting the closure will hand the
            // client an internal error for its own timeout.
            if std::time::Instant::now() >= deadline {
                return Err(crate::Error::DeadlineExceeded { request_id });
            }
            return Err(crate::Error::Dispatch {
                detail: "response channel closed".into(),
            });
        }
    };

    // Journal every row the write reported, as the parts of its record
    // group, while its guards and its order fence are still held, then
    // release them. The rows' records then sit in the WAL in the order the
    // rows reached storage, so replay in LSN order rebuilds exactly the state
    // the core holds. A refusal that follows committed rows reports them too,
    // and they are journalled the same way. A refusal that applied nothing
    // has its origin cancelled by the window, so its group needs no part.
    let post_apply_lsn = if let Some(collection) = &post_apply
        && let Some(origin) = group_origin
        && (response.status == Status::Ok || !response.write_set.is_empty())
    {
        let appender = shared
            .wal
            .appender(apply_key)
            .with_event_source(event_source);
        let appender = match upstream_commit_hlc {
            Some(hlc) => appender.with_commit_hlc(hlc),
            None => appender,
        };
        wal_dispatch::append_group_parts(
            appender,
            wal_dispatch::WriteSetTarget {
                tenant_id,
                vshard_id,
                database_id,
                collection,
                origin,
            },
            &response.write_set,
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
    // post-apply redo appended right above. Both records are already buffered in
    // the shared WAL; one group-commit fsync coalesces concurrent writers (see
    // `WalManager::wait_durable`), and it runs here — after the admission guards
    // are released — so it never serializes same-key throughput. Reads / control
    // ops / trigger / staged-write dispatch carry no LSN and skip the barrier;
    // `durability_barrier` decides which of those skips are legitimate and makes
    // the rest loud instead of letting them ack a write nothing can recover.
    let restore_write = event_source == crate::event::EventSource::Restore;
    if response.status == Status::Ok {
        let durable_target = match (wal_lsn, post_apply_lsn) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        };
        match durable_target {
            Some(lsn) => shared.wal.wait_durable(lsn).await?,
            // Nothing to fsync. Legitimate for most plans, but if this funnel
            // was the one required to mint the record, the ack below promises
            // durability the engine cannot deliver — silent until a `kill -9`
            // proves it, hence the check.
            None => assert_durable_before_ack(funnel_redo_engine),
        }
    } else if let Some(lsn) = post_apply_lsn {
        // The refusal reports rows that stay committed: their records are
        // durable before the refusal returns.
        shared.wal.wait_durable(lsn).await?;
    }
    // The group's parts are durable, or its origin was cancelled: the core
    // drops the write set it stored.
    if post_apply.is_some()
        && let Some(origin) = group_origin
    {
        match shared.dispatcher.lock() {
            Ok(mut d) => d.note_write_set_settled(vshard_id, origin.lsn),
            Err(poisoned) => poisoned
                .into_inner()
                .note_write_set_settled(vshard_id, origin.lsn),
        }
    }

    // Stage for the apply loop the change events of a successful replicated
    // write. `None` is a write with no replicated entry — see
    // [`super::super::params::ChangeFeedOwner`].
    if response.status == Status::Ok {
        if let Some(change_set) = change_set {
            change_set.publish(shared, tenant_id, database_id, &response);
        }

        // Record the write's commit HLC on the tenant's observed high-water
        // before this response, the ack, returns. The RESTORE staleness gate
        // refuses an envelope older than the mark, so the mark is the instant
        // the write committed, never the instant this bookkeeping ran: a
        // backup taken after the ack then always carries a newer watermark.
        //
        // A write RESTORE re-issued raises its group's restore mark instead,
        // under its restore id. Its commit HLC still folds into this node's
        // clock, so a later backup here stamps a newer watermark.
        if let Some(mark) = &user_write {
            if restore_write {
                shared
                    .hlc_clock
                    .update(nodedb_types::Hlc::new(mark.commit_hlc, 0));
            } else {
                shared.advance_tenant_write_hlc(
                    tenant_id.as_u64(),
                    mark.commit_hlc,
                    mark.site,
                    mark.collection.as_deref(),
                );
            }
        }
    }

    observe(shared);
    Ok(SubmitOutcome { response })
}
