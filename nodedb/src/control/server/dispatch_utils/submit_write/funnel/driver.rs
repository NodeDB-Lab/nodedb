// SPDX-License-Identifier: BUSL-1.1

//! The funnel's orchestrator: runs admission, WAL append, dispatch, and
//! response classification in that fixed order for one write.

use crate::control::server::dispatch_utils::change_events::extract_write_change_set;
use crate::control::server::dispatch_utils::durability_barrier::funnel_minted_redo_engine;
use crate::control::server::dispatch_utils::minted::{MintedRecords, RecordOwner};
use crate::control::server::shared::session::statement_deadline;
use crate::control::server::shared::write_admission::{bare_ok_response, route_write_to_calvin};
use crate::control::server::wal_dispatch;
use crate::control::state::SharedState;

use super::super::params::{ChangeFeedOwner, SubmitOutcome, SubmitWrite, WalDurability};
use super::admission::{AdmissionOutcome, admit_write};
use super::dispatch::{DispatchTarget, dispatch_to_data_plane};
use super::response::{ResponsePhaseInput, collect_classify_and_finish};
use super::wal_append::authorize_and_append;

/// Admit, make durable, enqueue, collect, and publish one write.
///
/// See [`SubmitOutcome`] for what comes back.
pub(crate) async fn submit_write(
    shared: &SharedState,
    params: SubmitWrite,
) -> crate::Result<SubmitOutcome> {
    let SubmitWrite {
        tenant_id,
        database_id,
        vshard_id,
        plan,
        trace_id,
        event_source,
        txn_id,
        user_id,
        mut durability,
        ordering,
        change_feed,
    } = params;
    let owner = RecordOwner {
        tenant_id,
        database_id,
        vshard_id,
    };
    // Records the caller appended for this write, under their outcome-floor
    // window. Every path below closes the window.
    let caller_minted = durability.take_minted();

    // The running statement's deadline, pinned once at the session boundary and
    // shared by every request the statement fans out into. Used for both the
    // envelope the Data Plane enforces and the Control-Plane collect below, so
    // the two halves cannot disagree about when this statement expires.
    let deadline = statement_deadline(shared.tuning.network.default_deadline_secs);

    // Change metadata is derived from the plan HERE, before it is moved into
    // the request — the publish itself happens after apply, once the response
    // (which carries the event's LSN) exists, by which point the plan is gone.
    // Extraction is a pure match that clones out collection / document
    // identity, so a caller whose change feed is `Unowned` skips it rather than
    // allocating tuples nothing will read.
    let change_set = match change_feed {
        ChangeFeedOwner::Funnel => Some(extract_write_change_set(&plan, tenant_id)),
        ChangeFeedOwner::Unowned => None,
    };

    // Post-apply redo classification, computed before `plan` is moved (the
    // RouteToCalvin admit arm moves it). For a write whose autocommit WAL path
    // mints no redo of its own but whose effect must survive a WAL-only restart
    // (a document PointUpdate on a collection carrying a secondary vector
    // index), the durable redo is minted AFTER apply from the surrogate +
    // post-image the Data Plane returns in `Response::write_set`.
    // `Some(collection)` for such a write, else `None`.
    let post_apply = wal_dispatch::plan_post_apply_redo(&plan);
    let appends_here = matches!(&durability, WalDurability::AppendHere { .. });
    let apply_key = match &durability {
        WalDurability::AppendHere { apply_key, .. } => *apply_key,
        WalDurability::CallerSupplied { .. } => 0,
    };
    // A transaction redo's refusal is final: every replica reaches it at the
    // same log position against the same state. Its abort marker carries the
    // entry's key, so the proposal ledger counts the refusal as the entry's
    // outcome after a restart. Any other refused write keeps its entry
    // replayable, so its abort marker carries no key.
    let final_refusal_key = if matches!(
        plan,
        nodedb_physical::physical_plan::PhysicalPlan::Meta(
            nodedb_physical::physical_plan::MetaOp::ApplyTransactionRedo { .. }
        )
    ) {
        apply_key
    } else {
        0
    };

    // Durable-at-ack obligation, also computed before `plan` moves. `Some` only
    // for a write whose redo record THIS funnel is required to mint; a caller
    // that appended upstream (or declared durability owned elsewhere) is not
    // held to it, because the LSN it does or does not supply is its own
    // contract. See `durability_barrier` for why this is narrower than
    // "write-class plan with no LSN".
    let funnel_redo_engine = if appends_here {
        funnel_minted_redo_engine(&plan)
    } else {
        None
    };

    // Write-admission gate: every write-class plan whose ordering is not already
    // final passes here. On the Calvin route the deterministic scheduler applies
    // the write, emits its own WriteEvents, and owns durability (the sequenced
    // TxClass plus its own `CalvinApplied` WAL record), so no local WAL append or
    // enqueue happens. A plain write with no RETURNING rows yields `None`,
    // synthesized into a bare `Ok`.
    let (admission, admission_guard, order_guard) =
        match admit_write(shared, tenant_id, database_id, vshard_id, &plan, ordering).await {
            AdmissionOutcome::Proceed {
                admission,
                admission_guard,
                order_guard,
            } => (admission, admission_guard, order_guard),
            AdmissionOutcome::RouteToCalvin => {
                // The scheduler applies the write from its own records, so
                // the caller's records never apply.
                let superseded = caller_minted.map(|minted| {
                    minted.supersede(std::sync::Arc::clone(&shared.wal), owner, "calvin_route")
                });
                let routed =
                    route_write_to_calvin(shared, tenant_id, database_id, vshard_id, plan).await;
                if let Some(superseded) = superseded {
                    superseded.finish().await;
                }
                let routed = routed?;
                return Ok(SubmitOutcome {
                    response: routed
                        .unwrap_or_else(|| bare_ok_response(crate::types::RequestId::new(0))),
                    wal_lsn: None,
                });
            }
        };

    // A write that mints its own LSN opens its outcome-floor window before the
    // mint, and appends through it.
    let minted = match caller_minted {
        Some(minted) => Some(minted),
        None => appends_here.then(|| MintedRecords::open(&shared.outcome_floor)),
    };

    // Array DDL authorization + durability, under the admission guard,
    // immediately before the enqueue below.
    let wal_append_outcome =
        match authorize_and_append(shared, owner, plan, durability, minted.as_ref()) {
            Ok(outcome) => outcome,
            Err(error) => {
                // No record of this write reaches a core.
                if let Some(minted) = minted {
                    minted.cancel(&shared.wal, owner, 0).await?;
                }
                return Err(error);
            }
        };
    let ddl_transition = wal_append_outcome.ddl_transition;
    let plan = wal_append_outcome.plan;
    let wal_lsn = wal_append_outcome.wal_lsn;
    let resolved_now_ms = wal_append_outcome.resolved_now_ms;

    // Build the wire request and hand it to the Data-Plane dispatcher.
    let dispatched = dispatch_to_data_plane(
        shared,
        &ddl_transition,
        DispatchTarget {
            tenant_id,
            database_id,
            vshard_id,
            plan,
            deadline,
            trace_id,
            event_source,
            user_id,
            txn_id,
            wal_lsn,
            resolved_now_ms,
            admission,
        },
        admission_guard,
        order_guard,
        post_apply.is_some(),
    );
    let dispatch_outcome = match dispatched {
        Ok(outcome) => outcome,
        Err(error) => {
            // The dispatcher refused the request, so no core applied it. A
            // dispatch refusal depends on this node's load, so the markers
            // carry no proposal key.
            if let Some(minted) = minted {
                minted.cancel(&shared.wal, owner, 0).await?;
            }
            return Err(error);
        }
    };

    // Collect response(s), classify the outcome, and run the post-apply steps
    // a successful write still owes.
    let max_result_bytes = shared.tuning.network.max_query_result_bytes as usize;
    collect_classify_and_finish(
        shared,
        max_result_bytes,
        ResponsePhaseInput {
            request_id: dispatch_outcome.request_id,
            rx: dispatch_outcome.rx,
            deadline,
            dispatch_started: dispatch_outcome.dispatch_started,
            tenant_id,
            database_id,
            vshard_id,
            wal_lsn,
            appends_here,
            final_refusal_key,
            apply_key,
            post_apply,
            funnel_redo_engine,
            change_set,
            ddl_transition,
            deferred_guards: dispatch_outcome.deferred_guards,
            minted,
        },
    )
    .await
}
