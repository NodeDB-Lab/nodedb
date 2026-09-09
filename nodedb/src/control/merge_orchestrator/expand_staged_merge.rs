// SPDX-License-Identifier: BUSL-1.1

//! Resolve + emit the concrete point ops for one in-transaction `MERGE`.
//!
//! A transactional MERGE must not replay the raw `Merge` plan through the
//! legacy passthrough, which writes NOT-MATCHED inserts unsurrogated (never
//! indexed, lost on WAL-only restart, outside the COMMIT undo log). Instead
//! it's resolved at STATEMENT time: ship source rows, dispatch the shared
//! Data-Plane RESOLVE pass, assign each inserted row a fresh registered
//! surrogate (reusing the existing one for updates/deletes), then stage the
//! resulting point ops through the normal path — indexed, replicated,
//! undo-tracked, and visible to later statements immediately.
//!
//! The RESOLVE pass reads TARGET as base ∪ overlay via the threaded `txn_id`,
//! so a MERGE reuses the surrogate of a row a prior statement staged.
//! Mirrors [`super::orchestrator::run_merge`]'s autocommit identity
//! derivation via shared [`crate::control::target_identity`].

use nodedb_types::TenantId;

use crate::bridge::envelope::{PhysicalPlan, Status};
use crate::control::maintenance::clone_materializer::{dispatch_local, read_all_source_rows};
use crate::control::state::SharedState;
use crate::types::VShardId;
use nodedb_physical::physical_plan::DocumentOp;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

use super::resolve_arms::{ResolvedMergeArms, decode_resolve};
use crate::control::target_identity::{
    TargetPk, assign_target_surrogate, bare_collection_name, derive_document_id, require_surrogate,
    resolve_target_pk,
};

/// Resolve one in-transaction `DocumentOp::Merge` task into the concrete,
/// surrogate-carrying `PointInsert` / `PointPut` / `PointDelete` tasks its three
/// arms expand to.
///
/// `task.txn_id` must be the active transaction so the RESOLVE pass folds
/// earlier-staged rows. Emitted ops carry the same `txn_id` and a recomputed
/// target vShard (as the `INSERT ... SELECT` expander does). The caller
/// stages + buffers each returned op.
pub(crate) async fn resolve_and_emit_merge_ops(
    state: &SharedState,
    tenant_id: TenantId,
    task: &PhysicalTask,
) -> crate::Result<Vec<PhysicalTask>> {
    let PhysicalPlan::Document(DocumentOp::Merge {
        target_collection,
        rls_write_check,
        ..
    }) = &task.plan
    else {
        // Callers only pass a `Merge` task; a mismatch is a programmer error.
        return Err(crate::Error::PlanError {
            detail: "resolve_and_emit_merge_ops: non-MERGE task".into(),
        });
    };
    let target_collection = target_collection.clone();
    let rls_write_check = rls_write_check.clone();

    let arms = resolve_merge_arms(state, tenant_id, task).await?;

    // Gate every resolved arm on the target's write policy (post-image for
    // UPDATE/INSERT, pre-image for DELETE): this expansion rewrites the
    // statement past RLS injection, so without this check a governed MERGE
    // would launder into ungoverned point writes.
    if let nodedb_types::WriteGateDecision::Evaluate(predicate) = rls_write_check.decision() {
        let bodies = arms
            .updates
            .iter()
            .map(|(_, _, body, _)| body)
            .chain(arms.deletes.iter().map(|(_, _, body)| body))
            .chain(arms.inserts.iter().map(|(_, body)| body));
        for body in bodies {
            crate::control::security::rls::admit_compiled_write_image(
                predicate,
                body,
                tenant_id.as_u64(),
                target_collection.as_str(),
            )?;
        }
    }

    let catalog = state.credentials.catalog();
    let target_bare = bare_collection_name(task.database_id, target_collection.as_str());
    let target = catalog
        .get_collection(task.database_id, tenant_id.as_u64(), &target_bare)?
        .ok_or_else(|| crate::Error::CollectionNotFound {
            tenant_id,
            collection: target_collection.to_string(),
        })?;
    let target_pk = resolve_target_pk(&target, "MERGE")?;

    let vshard_id =
        VShardId::from_collection_in_database(task.database_id, target_collection.as_str());
    let mut out: Vec<PhysicalTask> = Vec::new();
    emit_arms(
        state,
        task,
        target_collection.as_str(),
        &target_pk,
        vshard_id,
        arms,
        &mut out,
    )?;
    Ok(out)
}

/// Ship the source rows and dispatch the shared Data-Plane RESOLVE pass for
/// one staged merge, decoding all three resolved arms. Never re-derives the
/// classification locally — `collect_merge_plan` is the single shared
/// classifier for both this path and autocommit `run_merge`.
async fn resolve_merge_arms(
    state: &SharedState,
    tenant_id: TenantId,
    task: &PhysicalTask,
) -> crate::Result<ResolvedMergeArms> {
    let PhysicalPlan::Document(DocumentOp::Merge {
        target_collection,
        source_collection,
        source_alias,
        target_join_col,
        source_join_col,
        clauses,
        rls_write_check,
        declared_primary_key,
        ..
    }) = &task.plan
    else {
        // Callers only pass a `Merge` task; a mismatch is a programmer error.
        return Err(crate::Error::PlanError {
            detail: "resolve_merge_arms: resolve on non-MERGE task".into(),
        });
    };

    // Phase 0: read the SOURCE where it lives (a different core than the
    // target's) and ship the rows into the plan; threading `txn_id` folds
    // the source's own staging overlay too.
    let source_rows = read_all_source_rows(
        state,
        tenant_id,
        task.database_id,
        source_collection.as_str(),
        task.txn_id,
    )
    .await?;

    // Phase 1: dispatch the read-only RESOLVE pass against the target's core.
    let resolve_plan =
        PhysicalPlan::Document(DocumentOp::ResolveWrite(Box::new(DocumentOp::Merge {
            target_collection: target_collection.clone(),
            source_collection: source_collection.clone(),
            source_alias: source_alias.clone(),
            target_join_col: target_join_col.clone(),
            source_join_col: source_join_col.clone(),
            clauses: clauses.clone(),
            returning: None,
            resolved_inserts: None,
            source_rows: Some(source_rows),
            // Read-only classification pass: writes nothing, so no gate applies.
            rls_filters: Vec::new(),
            // The statement's injected write predicate, unread here.
            rls_write_check: rls_write_check.clone(),
            // Writes nothing, so folds no sum delta; the emitted point ops
            // carry their own resolution.
            resolved_sum_targets: Vec::new(),
            declared_primary_key: declared_primary_key.clone(),
        })));
    // Passing `txn_id` lets the RESOLVE pass fold TARGET's staging overlay,
    // so a MERGE reuses a prior statement's row instead of duplicating it.
    let resolve_resp = dispatch_local(
        state,
        tenant_id,
        task.database_id,
        target_collection.as_str(),
        resolve_plan,
        task.txn_id,
    )
    .await?;
    if resolve_resp.status != Status::Ok {
        return Err(crate::Error::Dispatch {
            detail: format!(
                "in-transaction MERGE resolve failed: {:?}",
                resolve_resp.error_code
            ),
        });
    }
    decode_resolve(&resolve_resp.payload)
}

/// Rewrite the three resolved arms into concrete point-write tasks appended
/// to `out`. An UPDATE/DELETE arm with no registered surrogate is a hard
/// error — emitting a degraded raw op would reproduce the indexing /
/// durability defect this expansion fixes.
fn emit_arms(
    state: &SharedState,
    task: &PhysicalTask,
    target_collection: &str,
    target_pk: &TargetPk,
    vshard_id: VShardId,
    arms: ResolvedMergeArms,
    out: &mut Vec<PhysicalTask>,
) -> crate::Result<()> {
    for (_join_key, body) in arms.inserts {
        let surrogate = assign_target_surrogate(
            state,
            task.database_id,
            task.tenant_id,
            target_collection,
            target_pk,
            &body,
        )?;
        let document_id = derive_document_id(target_pk, &body, surrogate);
        out.push(point_task(
            task,
            vshard_id,
            PhysicalPlan::Document(DocumentOp::PointInsert {
                collection: nodedb_types::QualifiedCollection::from_stored(
                    target_collection.to_string(),
                ),
                document_id,
                value: body,
                if_absent: false,
                surrogate,
                // The MERGE itself owns the statement's projection; the ops it
                // expands into are internal writes that answer no client.
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
                deferred_sum_targets: Vec::new(),
            }),
        ));
    }

    for (doc_id, surrogate_u32, body, _old_body) in arms.updates {
        let surrogate = require_surrogate(surrogate_u32, &doc_id, "MERGE")?;
        let document_id = derive_document_id(target_pk, &body, surrogate);
        let pk_bytes = document_id.clone().into_bytes();
        out.push(point_task(
            task,
            vshard_id,
            PhysicalPlan::Document(DocumentOp::PointPut {
                collection: nodedb_types::QualifiedCollection::from_stored(
                    target_collection.to_string(),
                ),
                document_id,
                value: body,
                surrogate,
                pk_bytes,
                // See the insert arm above.
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: Vec::new(),
            }),
        ));
    }

    for (doc_id, surrogate_u32, body) in arms.deletes {
        let surrogate = require_surrogate(surrogate_u32, &doc_id, "MERGE")?;
        let document_id = derive_document_id(target_pk, &body, surrogate);
        let pk_bytes = document_id.clone().into_bytes();
        out.push(point_task(
            task,
            vshard_id,
            PhysicalPlan::Document(DocumentOp::PointDelete {
                collection: nodedb_types::QualifiedCollection::from_stored(
                    target_collection.to_string(),
                ),
                document_id,
                surrogate,
                pk_bytes,
                returning: None,
                rls_filters: Vec::new(),
                // Already decided against the merge's write predicate by
                // `admit_compiled_write_image` above; this op removes that
                // same row, so re-checking would re-run the same test.
                rls_write_check: nodedb_types::RlsWriteCheck::decided_earlier_in_request(),
                resolved_sum_targets: Vec::new(),
            }),
        ));
    }
    Ok(())
}

/// Build a concrete point-write task carrying the staged transaction's identity
/// (`txn_id`) so it commits inside the same COMMIT batch as its siblings.
fn point_task(task: &PhysicalTask, vshard_id: VShardId, plan: PhysicalPlan) -> PhysicalTask {
    PhysicalTask {
        tenant_id: task.tenant_id,
        vshard_id,
        database_id: task.database_id,
        plan,
        post_set_op: PostSetOp::None,
        txn_id: task.txn_id,
    }
}
