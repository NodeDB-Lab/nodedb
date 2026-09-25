// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral implicit-edge OLLP/Calvin reconnaissance dispatch.
//!
//! This is the session-UNAWARE core of the implicit-edge dependent-predicate
//! path, extracted from the pgwire `dispatch_calvin_multishard` OLLP branch so
//! the native protocol path can share one implementation. Two items live here:
//!
//! - [`plan_needs_implicit_edge_recon`] — the detection gate: given a task set,
//!   return the collection + database of the first dependent-predicate task
//!   (`BulkUpdate`/`BulkDelete`) whose target collection `has_implicit_edges`,
//!   else `None`. It does NOT check the not-in-txn-block or registry-available
//!   guards — those are per-protocol / session-state concerns and stay at the
//!   call sites.
//! - [`dispatch_dependent_edge_recon`] — the OLLP orchestration body: pre-exec
//!   recon scan → derive mirrored EdgeDelete/EdgePut tasks → atomic Calvin
//!   submit → OLLP drift-retry loop. It returns a protocol-neutral
//!   [`DependentReconOutcome`]; each protocol synthesises its own command tags
//!   from the original task list AFTER this returns `Ok`.

use crate::Error;
use crate::control::cluster::calvin::executor::ollp::error::OllpError;
use crate::control::planner::calvin::preexec::{PreexecScan, run_preexec_scan};
use crate::control::planner::calvin::tx_class::collection_name_from_plan;
use crate::control::planner::calvin::{
    DependentOutcome, DependentRetryArgs, build_dependent_tx_class,
    build_single_vshard_dependent_tx_class, is_dependent_predicate, predicate_class_for_filters,
    run_dependent_with_retry, submit_calvin_routed_assign,
};
use crate::control::planner::implicit_edges::{
    EdgeUpdateCtx, append_implicit_edge_delete_tasks, append_implicit_edge_update_tasks,
};
use crate::control::state::{CalvinApplyResult, SharedState};
use crate::types::{DatabaseId, TenantId, TraceId};
use nodedb_physical::physical_plan::OllpPredictedEdge;

use super::dependent_recon_plan::{inject_ollp_predicted_edges, inject_ollp_surrogates};
use super::dependent_recon_predicate::{
    EdgeLifecycle, classify_edge_lifecycle, extract_bulk_predicate_info,
};
use nodedb_physical::physical_task::PhysicalTask;

/// Protocol-neutral result of [`dispatch_dependent_edge_recon`].
///
/// The per-task command tags are synthesised by each protocol from the original
/// task list it already owns. When the dependent write carried a RETURNING
/// clause, `apply_result` carries the applied Data-Plane [`Response`] (with the
/// deleted/updated rows) that the scheduler deposited before the completion ack,
/// so the caller emits DATA-ROWs for the RETURNING task instead of a bare tag.
pub struct DependentReconOutcome {
    /// Number of tasks committed in the dependent Calvin transaction. Callers
    /// synthesise one command tag per task from their original task list.
    pub tasks_dispatched: u64,
    /// Applied Data-Plane response for the RETURNING doc write, if any. `None`
    /// for a plain (non-RETURNING) dependent write.
    pub apply_result: Option<crate::bridge::envelope::Response>,
}

/// Detect whether `tasks` carry a dependent predicate on an implicit-edge-
/// bearing collection, requiring the OLLP/Calvin recon path.
///
/// Returns `Some((collection, database_id))` of the FIRST dependent-predicate
/// task (`BulkUpdate`/`BulkDelete`) whose target collection has
/// `has_implicit_edges` set in the catalog, else `None`.
///
/// A genuine catalog READ error propagates as a typed [`crate::Error`]:
/// misrouting a delete on a real I/O fault would silently skip edge cleanup
/// (dangling edges). An ABSENT catalog (`None`) or absent collection row
/// (`Ok(None)`) is treated as non-edge-bearing and yields `None`.
///
/// This does NOT check the not-in-transaction-block or registry-available
/// guards — those differ per protocol / are session-state concerns and stay at
/// the call sites.
pub fn plan_needs_implicit_edge_recon(
    state: &SharedState,
    tasks: &[PhysicalTask],
    tenant_id: TenantId,
) -> crate::Result<Option<(String, DatabaseId)>> {
    let Some(dep_task) = tasks.iter().find(|t| is_dependent_predicate(&t.plan)) else {
        return Ok(None);
    };
    let coll = collection_name_from_plan(&dep_task.plan);
    let db = dep_task.database_id;
    let edge_bearing = {
        let catalog = state.credentials.catalog();
        catalog
            .get_collection(db, tenant_id.as_u64(), &coll)?
            .map(|c| c.has_implicit_edges)
            .unwrap_or(false)
    };
    if edge_bearing {
        Ok(Some((coll, db)))
    } else {
        Ok(None)
    }
}

/// Drive the implicit-edge OLLP/Calvin reconnaissance dispatch for `tasks`.
///
/// The coordinator owns the OLLP retry loop. This:
///
/// 1. Resolves the dependent (`BulkUpdate`/`BulkDelete`) task and its
///    implicit-edge lifecycle (`Delete` retracts mirrored edges; `Update`
///    reconciles them against the SET clause — overrides parsed ONCE here as
///    they are constant across retries).
/// 2. Runs an initial pre-execution reconnaissance scan to predict the matched
///    surrogate set + the implicit edges of any matched edge documents.
/// 3. Submits a Calvin transaction (routed to the sequencer-group leader via
///    `submit_calvin_routed_assign`) that mirrors the doc write together with
///    the derived EdgeDelete/EdgePut tasks, ATOMICALLY.
/// 4. On a POST-EXEC predicate-drift mismatch, re-scans (FRESH reconnaissance)
///    and resubmits, via [`run_dependent_with_retry`].
///
/// Returns a protocol-neutral [`DependentReconOutcome`]; the caller synthesises
/// its own per-task command tags from the original task list. All errors are
/// typed [`crate::Error`]; the caller maps them to its protocol's error shape.
///
/// `database_id` is supplied by the caller (it comes from the detection gate,
/// [`plan_needs_implicit_edge_recon`]) so it does not have to be re-derived.
///
/// `allow_single_vshard` selects the participant floor of the `TxClass` this
/// builds: `false` (the normal multi-shard OLLP callers — the pgwire and
/// native predicate-dispatch gates) uses the strict
/// [`build_dependent_tx_class`], which rejects a write set that collapses to
/// one vshard. `true` is the explicit opt-in used ONLY by the contended
/// single-collection predicate-write routing path
/// (`route_write_to_calvin`'s dependent-predicate branch, reached when the
/// write-admission gate returns `RouteToCalvin`): it uses
/// [`build_single_vshard_dependent_tx_class`] so a single-collection
/// `BulkUpdate`/`BulkDelete` that legitimately resolves to one vshard
/// sequences through the scheduler instead of being rejected.
pub async fn dispatch_authorized_dependent_edge_recon(
    state: &SharedState,
    authorized: crate::control::server::shared::authorization::AuthorizedTaskSet,
    identity: &crate::control::security::identity::AuthenticatedIdentity,
    tenant_id: TenantId,
    database_id: DatabaseId,
    allow_single_vshard: bool,
) -> crate::Result<DependentReconOutcome> {
    let tasks = authorized
        .into_tasks()
        .into_iter()
        .map(|task| task.into_physical_task())
        .collect();
    dispatch_dependent_edge_recon_inner(
        state,
        tasks,
        Some(identity),
        tenant_id,
        database_id,
        allow_single_vshard,
    )
    .await
}

pub(crate) async fn dispatch_dependent_edge_recon(
    state: &SharedState,
    tasks: Vec<PhysicalTask>,
    tenant_id: TenantId,
    database_id: DatabaseId,
    allow_single_vshard: bool,
) -> crate::Result<DependentReconOutcome> {
    dispatch_dependent_edge_recon_inner(
        state,
        tasks,
        None,
        tenant_id,
        database_id,
        allow_single_vshard,
    )
    .await
}

async fn dispatch_dependent_edge_recon_inner(
    state: &SharedState,
    tasks: Vec<PhysicalTask>,
    identity: Option<&crate::control::security::identity::AuthenticatedIdentity>,
    tenant_id: TenantId,
    database_id: DatabaseId,
    allow_single_vshard: bool,
) -> crate::Result<DependentReconOutcome> {
    let orchestrator = state.ollp_orchestrator.get();
    let registry = state
        .calvin_completion_registry
        .get()
        .ok_or(Error::SequencerUnavailable)?;

    // OLLP path: the coordinator owns the retry loop. `run_dependent_with_retry`
    // submits + awaits the assignment/completion via the local registry and, on
    // a post-exec predicate-drift mismatch, runs a FRESH pre-execution scan
    // (`rescan`) before resubmitting with the fresh prediction.
    let dep_task = tasks
        .iter()
        .find(|t| is_dependent_predicate(&t.plan))
        .ok_or_else(|| Error::Internal {
            detail: "dependent-edge recon dispatch invoked without a dependent-predicate task"
                .to_owned(),
        })?;

    let orc = orchestrator.ok_or(Error::SequencerUnavailable)?;
    // Hoisted across the retry loop so both `submit` and `rescan` can borrow them.
    let (dep_collection, dep_filter_bytes) = extract_bulk_predicate_info(&dep_task.plan);
    let pred_class = predicate_class_for_filters(&dep_filter_bytes, &dep_collection);

    let edge_mode = classify_edge_lifecycle(&dep_task.plan)?;

    // Initial reconnaissance — the first prediction the loop submits.
    let initial_predicted = run_preexec_scan(
        state,
        tenant_id,
        database_id,
        &dep_collection,
        dep_filter_bytes.clone(),
    )
    .await?;

    let timeout = std::time::Duration::from_secs(state.tuning.network.default_deadline_secs);
    let ollp_max_retries = orc.ollp_max_retries() as u32;

    // `submit`: build the TxClass with the loop-supplied prediction (NOT a
    // frozen clone), pass through this coordinator's circuit-breaker / tenant
    // budget gate, then ROUTE the inbox submit to the sequencer-group leader
    // via `submit_calvin_routed_assign` (returning the leader-assigned
    // `RoutedAssignment`). This lets a non-leader coordinator drive the
    // dependent (OLLP) cross-shard write to completion.
    let submit = |predicted: &PreexecScan| {
        let surrogates = predicted.surrogates.clone();
        let edges = predicted.edges.clone();
        let tasks = &tasks;
        let dep_collection = &dep_collection;
        let edge_mode = &edge_mode;
        async move {
            // Implicit-edge reconciliation: a matched edge document
            // (`_from`/`_to`) has an auto-created graph edge that must be
            // kept consistent in the SAME Calvin transaction, cross-shard-
            // correctly. For a DELETE we retract the edge; for an UPDATE we
            // diff the recon edge set against the SET-clause overrides and
            // emit the minimal EdgeDelete/EdgePut. These async tasks (each
            // endpoint surrogate resolved via the routed surrogate exchange)
            // are built BEFORE entering the sync tx_builder, then spliced
            // into the modified task set there.
            //
            // Content-drift TOCTOU (a concurrent UPDATE of a matched doc's
            // `_from`/`_to`/`_type`, or an edge appearing/disappearing among
            // the matched docs, between recon and execution) is closed below:
            // the recon edge set is carried into the plan as
            // `ollp_predicted_edges` and the data plane re-derives the ACTUAL
            // (pre-mutation) edge set from the matched docs, returning
            // `OllpRetryRequired` on any divergence BEFORE writing. The
            // existing retry loop then re-scans and re-derives fresh edges.
            //
            // `predicted_edges` mirrors the recon `edges` (which carry the
            // surrogate of each edge doc) into the plan-carried wire type.
            let predicted_edges: Vec<OllpPredictedEdge> = edges
                .iter()
                .map(|e| OllpPredictedEdge {
                    surrogate: e.surrogate,
                    from: e.from.clone(),
                    to: e.to.clone(),
                    label: e.label.clone(),
                })
                .collect();

            let mut edge_tasks: Vec<PhysicalTask> = Vec::new();
            match edge_mode {
                EdgeLifecycle::Delete => {
                    append_implicit_edge_delete_tasks(
                        state,
                        &mut edge_tasks,
                        tenant_id,
                        database_id,
                        TraceId::ZERO,
                        dep_collection,
                        &edges,
                    )
                    .await
                    .map_err(|e| OllpError::Terminal(Box::new(e)))?;
                }
                EdgeLifecycle::Update(overrides) => {
                    append_implicit_edge_update_tasks(
                        EdgeUpdateCtx {
                            state,
                            tenant_id,
                            database_id,
                            trace_id: TraceId::ZERO,
                            collection: dep_collection,
                        },
                        &mut edge_tasks,
                        &edges,
                        &surrogates,
                        overrides,
                    )
                    .await
                    .map_err(|e| OllpError::Terminal(Box::new(e)))?;
                }
            }

            let mut submission_tasks: Vec<PhysicalTask> = tasks.to_vec();
            submission_tasks.extend(edge_tasks);
            if let Some(identity) = identity {
                let emitter = crate::control::security::audit::ArcAuditEmitter(
                    std::sync::Arc::clone(&state.audit),
                );
                submission_tasks =
                    crate::control::server::shared::authorization::authorize_task_set(
                        identity,
                        &submission_tasks,
                        &state.permissions,
                        &state.roles,
                        &emitter,
                    )
                    .map_err(|e| OllpError::Terminal(Box::new(e.into())))?
                    .into_tasks()
                    .into_iter()
                    .map(|task| task.into_physical_task())
                    .collect();
            }

            orc.submit_with_retry_via(
                pred_class,
                tenant_id,
                || {
                    let modified_tasks: Vec<PhysicalTask> = submission_tasks
                        .iter()
                        .map(|t| {
                            let mut t = t.clone();
                            // `inject_ollp_surrogates` / `_predicted_edges`
                            // only touch the original BulkUpdate/BulkDelete
                            // doc tasks (no-ops on any other plan); the
                            // edge-delete tasks are appended AFTER, so they
                            // are untouched. The tx_builder may run more than
                            // once, so clone the predicted sets per task.
                            inject_ollp_surrogates(&mut t.plan, surrogates.clone());
                            inject_ollp_predicted_edges(&mut t.plan, predicted_edges.clone());
                            t
                        })
                        .collect();
                    let built = if allow_single_vshard {
                        build_single_vshard_dependent_tx_class(
                            &modified_tasks,
                            tenant_id,
                            dep_collection,
                            &surrogates,
                            &[],
                        )
                    } else {
                        build_dependent_tx_class(
                            &modified_tasks,
                            tenant_id,
                            dep_collection,
                            &surrogates,
                            &[],
                        )
                    };
                    built.map_err(|e| OllpError::Terminal(Box::new(e)))
                },
                // Retryable: a routed submit races a leader change. The cause
                // travels so exhaustion names it instead of claiming drift.
                |tx_class| async move {
                    submit_calvin_routed_assign(state, tx_class)
                        .await
                        .map_err(|e| OllpError::Retryable(Box::new(e)))
                },
            )
            .await
        }
    };

    // `rescan`: FRESH reconnaissance on each post-exec mismatch.
    let rescan = || {
        run_preexec_scan(
            state,
            tenant_id,
            database_id,
            &dep_collection,
            dep_filter_bytes.clone(),
        )
    };

    let completed_txn = match run_dependent_with_retry(DependentRetryArgs {
        registry,
        orchestrator: orc,
        predicate_class_hash: pred_class,
        timeout,
        ollp_max_retries,
        initial_predicted,
        submit,
        rescan,
    })
    .await?
    {
        DependentOutcome::Committed(txn_id) => txn_id,
        // The predicate matched no rows and nothing else in the batch writes,
        // so no entry was sequenced. The statement reports zero rows affected.
        DependentOutcome::NoOp => {
            return Ok(DependentReconOutcome {
                tasks_dispatched: 0,
                apply_result: None,
            });
        }
    };

    // A write to a permission-tree source is acknowledged only once it binds
    // every node. Tree sources live in the default database.
    let sources = state.authorization_fence.sources();
    let binds_authorization = database_id == crate::types::DatabaseId::DEFAULT
        && tasks.iter().any(|task| {
            task.plan
                .named_collections()
                .iter()
                .any(|collection| sources.is_source_collection(collection))
        });
    if binds_authorization {
        crate::control::security::auth_lease::calvin_write_barrier(state).await?;
    }

    // Completion fired: the scheduler deposited the applied Response (with any
    // RETURNING rows) into the sidecar before proposing the ack that woke the
    // retry loop, so the entry is present now if this write carried RETURNING.
    // Drain it (removing the entry) for the caller to shape into DATA-ROWs; a
    // `Conflict` (>1 RETURNING participant) fails loudly rather than returning a
    // partial cross-shard union.
    let drained = state
        .calvin
        .apply_results
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .remove(&completed_txn);
    let apply_result = match drained {
        Some(CalvinApplyResult::Single { response, .. }) => {
            // An installed txn whose reply failed to render deposits it as an
            // error for the statement.
            crate::control::server::dispatch_utils::reject_data_plane_error(&response)?;
            Some(response)
        }
        Some(CalvinApplyResult::Conflict) => {
            return Err(Error::Internal {
                detail: "multi-participant cross-shard RETURNING not supported".to_owned(),
            });
        }
        None => None,
    };

    Ok(DependentReconOutcome {
        tasks_dispatched: tasks.len() as u64,
        apply_result,
    })
}
