// SPDX-License-Identifier: BUSL-1.1

//! Control-Plane orchestrator for autocommit `UPDATE ... FROM <source>`.
//!
//! A remote source silently updates nothing unless shipped into `source_rows`
//! on the target's core. Never inserts. In-transaction resolves at statement
//! time instead — see `expand_staged_update_from_join`.
//!
//! The resolved apply plan lands on the target vShard's owner and every
//! replica through `orchestrated_write`; a target write policy is decided
//! here over the matched post-images, so the plan replicates decided.

use nodedb_types::{DatabaseId, TenantId};

use crate::bridge::envelope::{ErrorCode, PhysicalPlan, Response, Status};
use crate::control::maintenance::clone_materializer::{dispatch_local, read_all_source_rows};
use crate::control::orchestrated_write::{
    apply_orchestrated_write, decide_write_policy_over_images,
};
use crate::control::planner::materialized_sum::{
    resolve_sum_targets_for_bodies, source_drives_bindings,
};
use crate::control::state::SharedState;
use crate::control::update_from_join_orchestrator::expand_staged_update_from_join::decode_resolved_update_rows;
use nodedb_physical::physical_plan::{DocumentOp, ResolvedSumTarget, ReturningSpec, UpdateValue};

/// Attempts before a drifting materialized-sum resolution is reported rather
/// than retried forever. Mirrors the MERGE orchestrator's bound.
const MAX_UPDATE_FROM_JOIN_RETRIES: u32 = 8;

/// Bundled arguments for [`run_update_from_join`], mirroring the fields of the
/// intercepted `DocumentOp::UpdateFromJoin` plan.
pub struct UpdateFromJoinArgs<'a> {
    pub tenant_id: TenantId,
    pub database_id: DatabaseId,
    pub target_collection: &'a str,
    pub source_collection: &'a str,
    pub source_alias: &'a str,
    pub target_join_col: &'a str,
    pub source_join_col: &'a str,
    pub updates: &'a [(String, UpdateValue)],
    pub target_filters: &'a [u8],
    pub returning: Option<&'a ReturningSpec>,
    /// Target RLS read filters, gating rows a `RETURNING` update shows.
    pub rls_filters: &'a [u8],
    /// Target RLS write predicate, gating every matched row's post-image
    /// before writing. Separate from `rls_filters` (shown vs. written).
    pub rls_write_check: &'a nodedb_types::RlsWriteCheck,
    /// Declared `PRIMARY KEY` column of the target, `None` for none declared.
    /// Carried on both passes so the Data Plane's post-image guard runs.
    pub declared_primary_key: Option<&'a str>,
}

/// Consume an authorized autocommit `UPDATE ... FROM` at orchestration.
pub async fn run_authorized_update_from_join(
    state: &SharedState,
    authorized: crate::control::server::shared::authorization::AuthorizedTask,
) -> crate::Result<Response> {
    let task = authorized.into_physical_task();
    let PhysicalPlan::Document(DocumentOp::UpdateFromJoin {
        target_collection,
        source_collection,
        source_alias,
        target_join_col,
        source_join_col,
        updates,
        target_filters,
        returning,
        source_rows: _,
        rls_filters,
        rls_write_check,
        // Unresolved on the way in — resolved below before dispatch.
        resolved_sum_targets: _,
        declared_primary_key,
    }) = task.plan
    else {
        return Err(crate::Error::BadRequest {
            detail: "authorized task is not unresolved autocommit UPDATE ... FROM".into(),
        });
    };
    run_update_from_join(
        state,
        UpdateFromJoinArgs {
            tenant_id: task.tenant_id,
            database_id: task.database_id,
            target_collection: target_collection.as_str(),
            source_collection: source_collection.as_str(),
            source_alias: &source_alias,
            target_join_col: &target_join_col,
            source_join_col: &source_join_col,
            updates: &updates,
            target_filters: &target_filters,
            returning: returning.as_ref(),
            rls_filters: &rls_filters,
            rls_write_check: &rls_write_check,
            declared_primary_key: declared_primary_key.as_deref(),
        },
    )
    .await
}

/// Drive an autocommit `UPDATE ... FROM <source>` from the Control Plane.
/// Returns the same response a co-resident single-shard update would produce.
pub(crate) async fn run_update_from_join(
    state: &SharedState,
    args: UpdateFromJoinArgs<'_>,
) -> crate::Result<Response> {
    // Checked once: a target driving no materialized-sum binding, declaring
    // no period lock AND carrying no write policy skips the RESOLVE round
    // trip and retry loop entirely. A write policy needs the matched
    // post-images, which only RESOLVE produces.
    let drives_bindings = source_drives_bindings(
        state,
        args.target_collection,
        args.tenant_id,
        args.database_id,
    )?
    .is_some();
    let has_period_lock = crate::control::planner::period_lock::target_declares_period_lock(
        state,
        args.target_collection,
        args.tenant_id,
        args.database_id,
    )?;
    let needs_resolve = drives_bindings || has_period_lock || args.rls_write_check.has_predicate();

    let mut attempt: u32 = 0;
    loop {
        // Source vShard can map to a different core than the target's, so the
        // target-core dispatch below can't read it locally — scan and ship it.
        let source_rows = read_all_source_rows(
            state,
            args.tenant_id,
            args.database_id,
            args.source_collection,
            None,
        )
        .await?;

        let resolved_sum_targets = if needs_resolve {
            match resolve_matched_rows(state, &args, source_rows.clone()).await? {
                Some(resolved) => resolved,
                // RESOLVE pass failed on the Data Plane; its response is the answer.
                None => {
                    return Err(crate::Error::Dispatch {
                        detail: "UPDATE ... FROM target-row resolve pass failed".into(),
                    });
                }
            }
        } else {
            // No policy to decide: the gate admits every row, or refuses an
            // un-injected plan before it proposes.
            decide_write_policy_over_images(
                args.rls_write_check,
                std::iter::empty(),
                args.tenant_id,
                args.target_collection,
            )?;
            Vec::new()
        };

        let plan = PhysicalPlan::Document(DocumentOp::UpdateFromJoin {
            target_collection: nodedb_types::QualifiedCollection::from_stored(
                args.target_collection.to_string(),
            ),
            source_collection: nodedb_types::QualifiedCollection::from_stored(
                args.source_collection.to_string(),
            ),
            source_alias: args.source_alias.to_string(),
            target_join_col: args.target_join_col.to_string(),
            source_join_col: args.source_join_col.to_string(),
            updates: args.updates.to_vec(),
            target_filters: args.target_filters.to_vec(),
            returning: args.returning.cloned(),
            source_rows: Some(source_rows),
            rls_filters: args.rls_filters.to_vec(),
            // Decided above over the matched post-images; what replicates.
            rls_write_check: nodedb_types::RlsWriteCheck::decided_earlier_in_request(),
            resolved_sum_targets,
            declared_primary_key: args.declared_primary_key.map(str::to_string),
        });

        // Join-map now built from the shipped rows, so this lands correctly
        // regardless of where the source vShard lives — on the target's
        // owner and every replica.
        let resp = apply_orchestrated_write(
            state,
            args.tenant_id,
            args.database_id,
            args.target_collection,
            plan,
        )
        .await?;

        // A concurrent write moved the match set between RESOLVE and now, so the
        // write pass wrote nothing; re-resolve and re-dispatch to recover.
        if resp.error_code.as_deref() == Some(&ErrorCode::OllpRetryRequired) {
            attempt += 1;
            if attempt > MAX_UPDATE_FROM_JOIN_RETRIES {
                return Err(crate::Error::OllpExhausted {
                    retries: MAX_UPDATE_FROM_JOIN_RETRIES.min(u8::MAX as u32) as u8,
                    cause: crate::OllpExhaustedCause::PredicateDrift,
                });
            }
            continue;
        }

        return Ok(resp);
    }
}

/// Resolve the matched rows and everything the apply needs from them: the
/// target's write policy decided over every post-image, and the
/// materialized-sum AND period-lock targets (both images of every matched
/// row — a join-column rewrite debits one target and credits another; a
/// period-lock check may read either image). `None` means RESOLVE failed.
async fn resolve_matched_rows(
    state: &SharedState,
    args: &UpdateFromJoinArgs<'_>,
    source_rows: Vec<(String, Vec<u8>)>,
) -> crate::Result<Option<Vec<ResolvedSumTarget>>> {
    let resolve_plan = PhysicalPlan::Document(DocumentOp::ResolveWrite(Box::new(
        DocumentOp::UpdateFromJoin {
            target_collection: nodedb_types::QualifiedCollection::from_stored(
                args.target_collection.to_string(),
            ),
            source_collection: nodedb_types::QualifiedCollection::from_stored(
                args.source_collection.to_string(),
            ),
            source_alias: args.source_alias.to_string(),
            target_join_col: args.target_join_col.to_string(),
            source_join_col: args.source_join_col.to_string(),
            updates: args.updates.to_vec(),
            target_filters: args.target_filters.to_vec(),
            returning: None,
            source_rows: Some(source_rows),
            // Read-only: emits no rows, writes nothing, so neither policy gates here.
            rls_filters: Vec::new(),
            // Statement's injected write predicate, carried unchanged.
            rls_write_check: args.rls_write_check.clone(),
            // Folds no delta, so needs no resolution of its own.
            resolved_sum_targets: Vec::new(),
            declared_primary_key: args.declared_primary_key.map(str::to_string),
        },
    )));
    let resp = dispatch_local(
        state,
        args.tenant_id,
        args.database_id,
        args.target_collection,
        resolve_plan,
        None,
    )
    .await?;
    if resp.status != Status::Ok {
        return Ok(None);
    }

    let arms = decode_resolved_update_rows(&resp.payload)?;
    decide_write_policy_over_images(
        args.rls_write_check,
        arms.iter().map(|(_, _, body, _)| body.as_slice()),
        args.tenant_id,
        args.target_collection,
    )?;
    let bodies: Vec<&[u8]> = arms
        .iter()
        .flat_map(|(_, _, body, old_body)| [body.as_slice(), old_body.as_slice()])
        .collect();
    let mut resolved = resolve_sum_targets_for_bodies(
        state,
        &bodies,
        args.target_collection,
        args.tenant_id,
        args.database_id,
        crate::types::TraceId::ZERO,
    )
    .await?;
    resolved.extend(
        crate::control::planner::period_lock::resolve_period_lock_targets_for_bodies(
            state,
            &bodies,
            args.target_collection,
            args.tenant_id,
            args.database_id,
            crate::types::TraceId::ZERO,
        )
        .await?,
    );
    Ok(Some(resolved))
}
