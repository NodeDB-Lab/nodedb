// SPDX-License-Identifier: BUSL-1.1

//! The funnel's enqueue phase: runs admission, WAL append, and dispatch in
//! that fixed order for one write. [`super::pending::PendingWrite::finish`]
//! runs the response phase.

use std::sync::Arc;

use crate::bridge::dispatch::JournalGroup;
use crate::control::server::dispatch_utils::change_events::{
    PendingChanges, extract_write_change_set,
};
use crate::control::server::dispatch_utils::durability_barrier::funnel_minted_redo_engine;
use crate::control::server::dispatch_utils::minted::{
    Collect, MintedRecords, OwnedWait, RecordOwner, spawn_owned_wait,
};
use crate::control::server::shared::session::statement_deadline;
use crate::control::server::shared::write_admission::{
    bare_ok_response, calvin_route_keeps, order_row_write, route_write_to_calvin,
};
use crate::control::server::wal_dispatch;
use crate::control::state::SharedState;
use crate::engine::timeseries::resolved_ingest::TsDriftPolicy;

use super::super::params::{
    ChangeFeedOwner, SubmitOutcome, SubmitWrite, WalDurability, WriteOrdering,
};
use super::admission::{AdmissionOutcome, AdmissionTarget, admit_write};
use super::answer::Answer;
use super::dispatch::{DispatchTarget, HeldGuards, dispatch_to_data_plane};
use super::late_parts::LateParts;
use super::pending::PendingWrite;
use super::response::{ResponsePhaseInput, UserWriteMark};
use super::wal_append::{AppendScope, authorize_and_append};

/// Admit, make durable, and enqueue one write on its core.
///
/// The write is on its core's queue when this returns, so a caller that
/// enqueues writes one after another fixes their arrival order at the core.
/// [`PendingWrite::finish`] collects the outcome.
pub(crate) async fn enqueue_write(
    shared: &SharedState,
    params: SubmitWrite,
) -> crate::Result<PendingWrite> {
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
    // Only a user data write advances the tenant's observed write-HLC, which
    // the RESTORE staleness gate compares envelopes against. A schema install
    // such as a constraint set writes no row, so it records no mark. The mark
    // keeps the path and collection of the write, so a refused restore names it.
    let user_write_origin =
        crate::control::server::shared::write_admission::plan_writes_user_data(&plan).then(|| {
            let site = match &durability {
                WalDurability::AppendHere { apply_key: 0, .. } => "write funnel (autocommit)",
                WalDurability::AppendHere { .. } => "write funnel (replicated apply)",
                WalDurability::CallerSupplied { .. } => "write funnel (caller-appended)",
            };
            let collection = plan.named_collections().first().map(|c| (*c).to_owned());
            (site, collection)
        });
    // Records the caller appended for this write, under their outcome-floor
    // window. Every path below closes the window.
    let mut caller_minted = durability.take_minted();
    // A document row or edge write mints its records under its vShard's
    // write-order fence, which this funnel takes below, so WAL order equals
    // the order its rows reach storage. Records a caller appended before the
    // call were minted outside that fence.
    if caller_minted.is_some() && wal_dispatch::plan_post_apply_redo(&plan).is_some() {
        if let Some(minted) = caller_minted.take() {
            minted.cancel(&shared.wal, owner, 0).await?;
        }
        return Err(crate::Error::Internal {
            detail: format!(
                "internal invariant break: a caller appended the records of a row write on \
                 '{}' before the write funnel; a row write's records are appended inside the \
                 funnel, under its vShard's write-order fence",
                plan.collection().unwrap_or("<unknown>")
            ),
        });
    }

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
        // A staged write's rows are not committed: it yields no event yet.
        ChangeFeedOwner::LocalApply if txn_id.is_some() => None,
        // Every committed user write applies through a replicated entry, so a
        // write that yields change events on a route no other replica applies
        // is refused before any record of it lands.
        ChangeFeedOwner::LocalApply => {
            if !extract_write_change_set(&plan, tenant_id).is_empty() {
                if let Some(minted) = caller_minted {
                    minted.cancel(&shared.wal, owner, 0).await?;
                }
                return Err(
                    crate::control::change_stream::ChangeStreamError::UnreplicatedChange {
                        collection: plan
                            .named_collections()
                            .first()
                            .map(|collection| (*collection).to_owned())
                            .unwrap_or_default(),
                    }
                    .into(),
                );
            }
            None
        }
        ChangeFeedOwner::Replicated {
            group_id,
            log_index,
        } => Some(PendingChanges::staged(
            extract_write_change_set(&plan, tenant_id),
            group_id,
            log_index,
        )),
        ChangeFeedOwner::Unowned => None,
    };

    // Post-apply redo classification, computed before `plan` is moved (the
    // RouteToCalvin admit arm moves it). A document write whose stored rows
    // its apply decides journals them AFTER apply, from the surrogate +
    // post-image the Data Plane returns in `Response::write_set`.
    // `Some(collection)` for such a write, else `None`.
    let post_apply = wal_dispatch::plan_post_apply_redo(&plan);
    let appends_here = matches!(&durability, WalDurability::AppendHere { .. });
    // A write that appends its own records journals the rows its apply
    // decides as its record group. A write whose records live elsewhere,
    // such as a WAL record replayed again, journals nothing here.
    let groups = appends_here && post_apply.is_some();
    let apply_key = match &durability {
        WalDurability::AppendHere { apply_key, .. } => *apply_key,
        WalDurability::CallerSupplied { .. } => 0,
    };
    // A committed proposal applies in log order against the same state on
    // every replica, so a final refusal is its outcome everywhere. The abort
    // marker of a final refusal carries the proposal's key, so the proposal
    // ledger counts the refusal as the entry's outcome after a restart and a
    // redelivered copy is never applied. A write no proposal carries has key
    // `0`.
    let final_refusal_key = apply_key;
    // A write whose order is already final waits out a full dispatcher queue
    // rather than failing: a committed entry that fails for local load leaves
    // this replica without a write every other replica applied.
    let waits_for_capacity = matches!(ordering, WriteOrdering::AlreadyOrdered);
    // A committed entry carries the rows its proposer resolved. Resolving it
    // again here will let this replica accept other lines than its peers.
    if waits_for_capacity && crate::control::write_resolve::is_unresolved_ingest(&plan) {
        if let Some(minted) = caller_minted {
            minted.cancel(&shared.wal, owner, 0).await?;
        }
        return Err(crate::Error::Internal {
            detail: format!(
                "internal invariant break: a committed timeseries ingest on '{}' carries \
                 unresolved lines; its proposer resolves it before the entry exists",
                plan.collection().unwrap_or("<unknown>")
            ),
        });
    }

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
    // final passes here. On the Calvin route the deterministic scheduler owns
    // the write: the sequenced TxClass, then the stamped redo entry the data
    // group applies. No local WAL append or enqueue happens. A plain write with no RETURNING rows yields `None`,
    // synthesized into a bare `Ok`.
    let target = AdmissionTarget {
        tenant_id,
        database_id,
        vshard_id,
        plan: &plan,
        may_route: calvin_route_keeps(&plan, event_source, 0),
    };
    let admitted = match admit_write(shared, target, ordering, deadline).await {
        Ok(admitted) => admitted,
        Err(error) => {
            // No record of this write reaches a core.
            if let Some(minted) = caller_minted {
                minted.cancel(&shared.wal, owner, 0).await?;
            }
            return Err(error);
        }
    };
    let (admission, admission_guard, order_guard) = match admitted {
        AdmissionOutcome::Proceed {
            admission,
            admission_guard,
            order_guard,
        } => (admission, admission_guard, order_guard),
        AdmissionOutcome::RouteToCalvin => {
            // The scheduler journals the write in its own sequenced record.
            // The scheduler applies the write from its own records, so
            // the caller's records never apply.
            let superseded = caller_minted.map(|minted| {
                minted.supersede(std::sync::Arc::clone(&shared.wal), owner, "calvin_route")
            });
            let routed = route_write_to_calvin(
                shared,
                tenant_id,
                database_id,
                vshard_id,
                plan,
                event_source,
            )
            .await;
            if let Some(superseded) = superseded {
                superseded.finish().await;
            }
            let routed = routed?;
            return Ok(PendingWrite::done(SubmitOutcome {
                response: routed
                    .unwrap_or_else(|| bare_ok_response(crate::types::RequestId::new(0))),
            }));
        }
    };

    // Order this write's row records (document rows and edges) against every
    // other row write of its vShard, from before the append below through its
    // last post-apply record. Admission's guard, when it holds one, already
    // names the row. A staged write stores nothing until its COMMIT, which is
    // ordered then.
    let write_order = if txn_id.is_none() {
        order_row_write(
            shared,
            vshard_id,
            &plan,
            admission_guard.is_some() || order_guard.is_some(),
        )
        .await
    } else {
        crate::control::server::shared::write_admission::WriteOrder::default()
    };

    // A gate-admitted timeseries ingest resolves its rows on its core before
    // its record is appended, so the record logs exactly the rows the install
    // stores. The resolve queues on the core behind every write enqueued
    // before this one. Its install refuses a schema a concurrent write
    // changed, and the writer resolves again: the record is cancelled then.
    let plan = if appends_here
        && txn_id.is_none()
        && !waits_for_capacity
        && crate::control::write_resolve::is_unresolved_ingest(&plan)
    {
        crate::control::write_resolve::resolve_ingest_plan(
            shared,
            crate::control::write_resolve::WriteResolveContext {
                tenant_id,
                database_id,
            },
            vshard_id,
            &plan,
            TsDriftPolicy::Refuse,
        )
        .await?
    } else {
        plan
    };

    // The instant the write committed, which is the value its mark carries:
    // - a replicated entry carries its proposer's stamp;
    // - a caller that appended upstream committed before this call, so this
    //   instant bounds it from above;
    // - otherwise the append below is the commit, stamped once it lands.
    let upstream_commit_hlc = match &durability {
        WalDurability::AppendHere { commit_hlc, .. } => *commit_hlc,
        WalDurability::CallerSupplied { .. } => Some(shared.hlc_clock.now().wall_ns),
    };

    // A write that mints its own LSN opens its outcome-floor window before the
    // mint, and appends through it.
    let minted = match caller_minted {
        Some(minted) => Some(minted),
        None => appends_here.then(|| MintedRecords::open(&shared.outcome_floor)),
    };

    // Durability, under the admission guard, immediately before the enqueue
    // below.
    // The records carry the commit instant when it is known before the
    // append; otherwise each takes the node's HLC as it lands.
    let wal_append_outcome = match authorize_and_append(
        shared,
        plan,
        durability,
        AppendScope {
            owner,
            minted: minted.as_ref(),
            event_source,
            commit_hlc: upstream_commit_hlc,
            groups,
        },
    ) {
        Ok(outcome) => outcome,
        Err(error) => {
            // No record of this write reaches a core.
            if let Some(minted) = minted {
                minted.cancel(&shared.wal, owner, 0).await?;
            }
            return Err(error);
        }
    };
    let commit_hlc = upstream_commit_hlc.unwrap_or_else(|| shared.hlc_clock.now().wall_ns);
    // The write's events date by its commit HLC. A replicated write's is the
    // HLC its proposer stamped on the entry, which every replica shares:
    // every proposal path stamps one.
    let change_set = change_set.map(|changes| changes.committed_at(commit_hlc));
    let user_write = user_write_origin.map(|(site, collection)| UserWriteMark {
        site,
        collection,
        commit_hlc,
    });
    let plan = wal_append_outcome.plan;
    let wal_lsn = wal_append_outcome.wal_lsn;
    // The write's change events date by its commit HLC. Recorded before the
    // enqueue, so it exists before any event of the write reaches the Event
    // Plane.
    if let Some(lsn) = wal_lsn {
        shared
            .cdc_router
            .positions()
            .record_commit_hlc(lsn.as_u64(), commit_hlc);
    }
    let resolved_now_ms = wal_append_outcome.resolved_now_ms;
    let group_origin = wal_append_outcome.group_origin;
    // The core stores the write set of a grouped write beside its effects.
    let journal = match (&post_apply, group_origin) {
        (Some(collection), Some(origin)) => Some(JournalGroup {
            origin: origin.lsn,
            collection: collection.clone(),
            apply_key,
            commit_hlc: upstream_commit_hlc,
            change_position: wal_append_outcome.change_position,
        }),
        _ => None,
    };

    // A crash test parks one collection's logged write here: its LSN is
    // minted and no core holds it. A replicated write parks inside its apply
    // entry's enqueue, so no later entry of its Raft group starts until the
    // gate opens. A write of another group applies meanwhile, with a higher
    // LSN. The parked write also holds its own per-key admission guards.
    #[cfg(feature = "failpoints")]
    crate::control::fail_gate::before_dispatch(shared.node_id, &plan, wal_lsn).await;

    // Build the wire request and hand it to the Data-Plane dispatcher.
    let dispatched = dispatch_to_data_plane(
        shared,
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
            commit_hlc,
            // A write that applies a data-group entry versions its rows by
            // the entry's position, which every replica shares.
            entry_version: wal_append_outcome.change_position.map(|position| {
                nodedb_types::WriteVersion::logged(position.epoch, position.log_index)
            }),
            admission,
            journal,
        },
        HeldGuards {
            _admission: admission_guard,
            _order: order_guard,
            _write_order: write_order,
        },
        post_apply.is_some(),
        waits_for_capacity,
    )
    .await;
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

    // A core holds the request now. Its records move to the task that closes
    // them from the final response, with no await since the enqueue. A drop of this future's caller or of the
    // `PendingWrite` leaves those tasks running.
    let max_result_bytes = shared.tuning.network.max_query_result_bytes as usize;
    // A grouped write whose final response arrives late journals its parts
    // from it.
    let (late_tx, late_parts) = match (&post_apply, group_origin, &minted) {
        (Some(collection), Some(origin), Some(_)) => {
            let (tx, rx) = tokio::sync::oneshot::channel();
            let late = LateParts {
                response: rx,
                wal: Arc::clone(&shared.wal),
                tenant_id,
                vshard_id,
                database_id,
                collection: collection.clone(),
                origin,
                apply_key,
                event_source,
                commit_hlc: upstream_commit_hlc,
            };
            (Some(tx), Some(late))
        }
        _ => (None, None),
    };
    let answer = match minted {
        Some(minted) => Answer::Owned(spawn_owned_wait(
            OwnedWait {
                wal: Arc::clone(&shared.wal),
                owner,
                final_refusal_key,
                deadline,
                collect: Collect::Merged { max_result_bytes },
                late: late_tx,
            },
            dispatch_outcome.rx,
            minted,
        )),
        None => Answer::Unminted {
            rx: dispatch_outcome.rx,
            deadline,
            max_result_bytes,
        },
    };

    // The response phase collects the outcome and runs the post-apply steps a
    // successful write still owes.
    Ok(PendingWrite::dispatched(ResponsePhaseInput {
        request_id: dispatch_outcome.request_id,
        answer,
        max_result_bytes,
        deadline,
        dispatch_started: dispatch_outcome.dispatch_started,
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
        deferred_guards: dispatch_outcome.deferred_guards,
        user_write,
        late_parts,
    }))
}
