// SPDX-License-Identifier: BUSL-1.1

//! Control-Plane orchestrator for autocommit `MERGE`.
//!
//! A NOT-MATCHED insert row needs its OWN registered surrogate, and surrogate
//! registration is Control-Plane-only, so autocommit MERGE runs as a
//! TOCTOU-safe round trip: (0) ship the source rows (scanned on its own
//! core, since it may differ from the target's) into `source_rows`; (1)
//! resolve — the Data Plane classifies the merge read-only and returns
//! NOT-MATCHED rows; (2) assign a fresh registered surrogate per insert row;
//! (3) apply — the Data Plane re-derives the classification, verifies the
//! insert-key set still matches (`OllpRetryRequired` without writing on
//! drift), and applies every arm in one transaction.
//!
//! Resolve and apply are separate snapshots; concurrent drift between them
//! is caught by apply-time verification and retried (bounded; exhaustion
//! surfaces `OllpExhausted`) — the same predict-verify-retry OLLP uses.

use nodedb_types::{DatabaseId, TenantId};

use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Response, Status};
use crate::control::maintenance::clone_materializer::{dispatch_local, read_all_source_rows};
use crate::control::state::SharedState;
use nodedb_physical::physical_plan::document::merge_types::MergeClauseOp;
use nodedb_physical::physical_plan::{DocumentOp, ReturningSpec};

use super::resolve_arms::decode_resolve;
use crate::control::planner::materialized_sum::resolve_sum_targets_for_bodies;
use crate::control::target_identity::{
    assign_target_surrogate, bare_collection_name, resolve_target_pk,
};

/// Upper bound on resolve→apply retries under concurrent source/target drift.
/// Mirrors the OLLP dependent-read retry ceiling: a merge whose matched /
/// not-matched classification keeps changing every attempt is surfaced as
/// `OllpExhausted` rather than looping forever.
const MAX_MERGE_RETRIES: u32 = 10;

/// Bundled arguments for [`run_merge`], mirroring the fields of the intercepted
/// `DocumentOp::Merge` plan.
pub struct MergeArgs<'a> {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    pub target_collection: &'a str,
    pub source_collection: &'a str,
    pub source_alias: &'a str,
    pub target_join_col: &'a str,
    pub source_join_col: &'a str,
    pub clauses: &'a [MergeClauseOp],
    /// Projection for a `MERGE ... RETURNING`, attached by the RETURNING
    /// pre-processor. `None` selects the affected-count response.
    pub returning: Option<&'a ReturningSpec>,
    /// RLS read filters, carried onto the apply pass so `RETURNING` rows are
    /// gated as a `SELECT` by the same principal would be.
    pub rls_filters: &'a [u8],
    /// RLS write predicate, carried onto the apply pass which decides every
    /// arm's image against it. Separate from `rls_filters`: read vs write gate.
    pub rls_write_check: &'a nodedb_types::RlsWriteCheck,
    /// Declared `PRIMARY KEY` column of the target, `None` for none declared.
    /// Carried on both passes so the Data Plane's UPDATE-arm guard runs.
    pub declared_primary_key: Option<&'a str>,
}

/// Consume an authorized autocommit `MERGE` at the orchestration boundary.
pub async fn run_authorized_merge(
    state: &SharedState,
    authorized: crate::control::server::shared::authorization::AuthorizedTask,
) -> crate::Result<Response> {
    let task = authorized.into_physical_task();
    let PhysicalPlan::Document(DocumentOp::Merge {
        target_collection,
        source_collection,
        source_alias,
        target_join_col,
        source_join_col,
        clauses,
        resolved_inserts: None,
        source_rows: _,
        returning,
        rls_filters,
        rls_write_check,
        // Unresolved on the way in: the orchestrator's own RESOLVE pass is what
        // produces the join keys this is filled from.
        resolved_sum_targets: _,
        declared_primary_key,
    }) = task.plan
    else {
        return Err(crate::Error::BadRequest {
            detail: "authorized task is not an unresolved autocommit MERGE".into(),
        });
    };
    run_merge(
        state,
        MergeArgs {
            tenant_id: task.tenant_id,
            database_id: task.database_id,
            target_collection: target_collection.as_str(),
            source_collection: source_collection.as_str(),
            source_alias: &source_alias,
            target_join_col: &target_join_col,
            source_join_col: &source_join_col,
            clauses: &clauses,
            returning: returning.as_ref(),
            rls_filters: &rls_filters,
            rls_write_check: &rls_write_check,
            declared_primary_key: declared_primary_key.as_deref(),
        },
    )
    .await
}

/// Drive an autocommit `MERGE` from the Control Plane.
///
/// Returns the `{"affected": N}` (or RETURNING-rows) response the Data-Plane
/// merge handler produces, so the dispatch loops render the same command tag.
pub(crate) async fn run_merge(state: &SharedState, args: MergeArgs<'_>) -> crate::Result<Response> {
    let catalog = state.credentials.catalog();
    let target_bare = bare_collection_name(args.database_id, args.target_collection);
    let target = catalog
        .get_collection(args.database_id, args.tenant_id.as_u64(), &target_bare)?
        .ok_or_else(|| crate::Error::CollectionNotFound {
            tenant_id: args.tenant_id,
            collection: args.target_collection.to_string(),
        })?;
    let target_pk = resolve_target_pk(&target, "MERGE")?;

    let mut attempt: u32 = 0;
    loop {
        // Phase 0: read SOURCE on its own core (may differ from target's)
        // and ship raw rows into the plan. A fresh read per attempt keeps
        // resolve/apply on one consistent snapshot.
        let source_rows = read_all_source_rows(
            state,
            args.tenant_id,
            args.database_id,
            args.source_collection,
            None,
        )
        .await?;

        // Phase 1: resolve the NOT-MATCHED insert rows (read-only snapshot).
        let resolve_plan = merge_plan(&args, true, None, Some(source_rows.clone()), Vec::new());
        let resolve_resp = dispatch_local(
            state,
            args.tenant_id,
            args.database_id,
            args.target_collection,
            resolve_plan,
            None,
        )
        .await?;
        if resolve_resp.status != Status::Ok {
            return Ok(resolve_resp);
        }
        let arms = decode_resolve(&resolve_resp.payload)?;

        // Phase 2a: resolve materialized-sum targets from the arms just
        // classified (INSERT credits, DELETE debits, UPDATE the difference,
        // both sides on a join-key rewrite). Lookup-only: an unmatched join
        // value fails the statement. Drift is caught by apply's own
        // insert-key verification, same guard the surrogates rely on.
        let sum_bodies: Vec<&[u8]> = arms
            .updates
            .iter()
            .flat_map(|(_, _, body, old_body)| [body.as_slice(), old_body.as_slice()])
            .chain(arms.deletes.iter().map(|(_, _, body)| body.as_slice()))
            .chain(arms.inserts.iter().map(|(_, body)| body.as_slice()))
            .collect();
        let resolved_sum_targets = resolve_sum_targets_for_bodies(
            state,
            &sum_bodies,
            args.target_collection,
            args.tenant_id,
            args.database_id,
            crate::types::TraceId::ZERO,
        )
        .await?;

        let insert_rows = arms.inserts;

        // Phase 2: assign a fresh, registered surrogate per inserted row.
        let mut resolved: Vec<(String, u32)> = Vec::with_capacity(insert_rows.len());
        for (join_key, body) in &insert_rows {
            let surrogate = assign_target_surrogate(
                state,
                args.database_id,
                args.tenant_id,
                args.target_collection,
                &target_pk,
                body,
            )?;
            resolved.push((join_key.clone(), surrogate.as_u32()));
        }

        // Phase 3: atomic apply with the pre-assigned surrogates + drift verify.
        // The apply reuses THIS attempt's source snapshot so the DP re-derives
        // the classification from the same source the resolve saw.
        let apply_plan = merge_plan(
            &args,
            false,
            Some(resolved),
            Some(source_rows),
            resolved_sum_targets,
        );
        let apply_resp = dispatch_local(
            state,
            args.tenant_id,
            args.database_id,
            args.target_collection,
            apply_plan,
            None,
        )
        .await?;

        if apply_resp.error_code.as_deref() == Some(&ErrorCode::OllpRetryRequired) {
            attempt += 1;
            if attempt > MAX_MERGE_RETRIES {
                return Err(crate::Error::OllpExhausted {
                    retries: MAX_MERGE_RETRIES.min(u8::MAX as u32) as u8,
                    cause: crate::OllpExhaustedCause::PredicateDrift,
                });
            }
            // Concurrent drift: re-resolve (fresh phase 1) and retry. The
            // surrogates assigned this round are simply unused (harmless —
            // the counter is monotonic and gap-tolerant).
            continue;
        }

        // `dispatch_local` bypasses the funnel's post-apply redo minting, so
        // a vector-indexed target's write-set arrives unconsumed. Mint it
        // now — without it a WAL-only restart rebuilds the HNSW from
        // pre-merge records. No-op on non-vector targets.
        crate::control::server::wal_dispatch::mint_dispatch_local_redo(
            &state.wal,
            args.tenant_id,
            args.database_id,
            args.target_collection,
            &apply_resp,
        )?;
        return Ok(apply_resp);
    }
}

/// Build a `DocumentOp::Merge` physical plan for one orchestrator pass.
///
/// `source_rows` carries the RAW stored source rows scanned on the source's own
/// core (phase 0) so the Data Plane builds the join-map from the shipped bytes
/// rather than reading the source from the target core's local store.
fn merge_plan(
    args: &MergeArgs<'_>,
    resolve_only: bool,
    resolved_inserts: Option<Vec<(String, u32)>>,
    source_rows: Option<Vec<(String, Vec<u8>)>>,
    resolved_sum_targets: Vec<nodedb_physical::physical_plan::ResolvedSumTarget>,
) -> PhysicalPlan {
    let merge = DocumentOp::Merge {
        target_collection: nodedb_types::QualifiedCollection::from_stored(
            args.target_collection.to_string(),
        ),
        source_collection: nodedb_types::QualifiedCollection::from_stored(
            args.source_collection.to_string(),
        ),
        source_alias: args.source_alias.to_string(),
        target_join_col: args.target_join_col.to_string(),
        source_join_col: args.source_join_col.to_string(),
        clauses: args.clauses.to_vec(),
        // Only APPLY can project rows; RESOLVE's payload is the fixed
        // `(updates, deletes, inserts)` tuple `decode_resolve` expects.
        returning: if resolve_only {
            None
        } else {
            args.returning.cloned()
        },
        resolved_inserts,
        source_rows,
        rls_filters: args.rls_filters.to_vec(),
        // Carried on both passes (inert on RESOLVE) so a future writing
        // resolve cannot silently lose the gate.
        rls_write_check: args.rls_write_check.clone(),
        // Empty on RESOLVE (writes nothing); APPLY carries the resolution.
        resolved_sum_targets,
        declared_primary_key: args.declared_primary_key.map(str::to_string),
    };
    if resolve_only {
        PhysicalPlan::Document(DocumentOp::ResolveWrite(Box::new(merge)))
    } else {
        PhysicalPlan::Document(merge)
    }
}
