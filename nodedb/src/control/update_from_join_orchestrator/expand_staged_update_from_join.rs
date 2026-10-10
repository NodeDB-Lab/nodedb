// SPDX-License-Identifier: BUSL-1.1

//! Resolve + emit the concrete point ops for one in-transaction `UPDATE ... FROM <source>`.
//!
//! Must not replay the raw plan through the legacy Data-Plane passthrough, which
//! writes outside the COMMIT undo log. Resolves at statement time instead.

use nodedb_types::TenantId;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::maintenance::clone_materializer::{dispatch_local, read_all_source_rows};
use crate::control::state::SharedState;
use crate::control::target_identity::{
    bare_collection_name, derive_document_id, resolve_target_pk,
};
use crate::query::ResolvedUpdateRowWire;
use nodedb_physical::physical_plan::DocumentOp;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

/// Resolve one in-transaction `UpdateFromJoin` task into the concrete,
/// surrogate-carrying `PointPut` tasks its matched target rows expand to.
/// `task.txn_id` must be the active transaction. The ops carry no sum or
/// period-lock resolution: the caller runs the statement pass on them, then
/// stages + buffers each one.
pub(crate) async fn resolve_and_emit_update_from_join_ops(
    state: &SharedState,
    tenant_id: TenantId,
    task: &PhysicalTask,
) -> crate::Result<Vec<PhysicalTask>> {
    let PhysicalPlan::Document(DocumentOp::UpdateFromJoin {
        target_collection,
        rls_write_check,
        ..
    }) = &task.plan
    else {
        // Callers only pass an `UpdateFromJoin` task; a mismatch is a bug.
        return Err(crate::Error::PlanError {
            detail: "resolve_and_emit_update_from_join_ops: non-UPDATE-FROM task".into(),
        });
    };
    let target_collection = target_collection.clone();
    let rls_write_check = rls_write_check.clone();

    let resolved = resolve_update_rows(state, tenant_id, task).await?;

    // Gate every matched row on the target's write policy — without this, expanding
    // a governed update would launder it into ungoverned point writes.
    if let nodedb_types::WriteGateDecision::Evaluate(predicate) = rls_write_check.decision() {
        for (_, _, body, _) in &resolved {
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
    let target_pk = resolve_target_pk(&target, "UPDATE ... FROM")?;

    // Recomputed rather than reusing the staged task's vShard, keeping dispatch
    // classification honest, like the MERGE / INSERT SELECT expanders.
    let vshard_id = nodedb_types::CollectionKey::from_qualified_str(
        task.database_id,
        target_collection.as_str(),
    )?
    .vshard();

    let mut out: Vec<PhysicalTask> = Vec::with_capacity(resolved.len());
    for (_doc_id, surrogate_u32, body, _old_body) in resolved {
        // The wire surrogate is never absent: every matched `UPDATE ... FROM`
        // row is a storage-keyed row.
        let surrogate = nodedb_types::Surrogate::new(surrogate_u32);
        let document_id = derive_document_id(&target_pk, &body, surrogate);
        let pk_bytes = document_id.clone().into_bytes();
        out.push(PhysicalTask {
            tenant_id: task.tenant_id,
            vshard_id,
            database_id: task.database_id,
            plan: PhysicalPlan::Document(DocumentOp::PointPut {
                collection: target_collection.clone(),
                document_id,
                value: body,
                surrogate,
                pk_bytes,
                // The op owns the statement's projection; expanded puts answer no client.
                returning: None,
                rls_filters: Vec::new(),
                // Filled by the caller's sum and period-lock pass. It reads
                // the stored row, so a join-column rewrite debits the target
                // the row leaves as well as crediting the one it joins.
                resolved_sum_targets: Vec::new(),
            }),
            post_set_op: PostSetOp::None,
            txn_id: task.txn_id,
        });
    }
    Ok(out)
}

/// Ship source rows and dispatch the shared RESOLVE pass, decoding matched
/// target rows. Never re-derives the join locally — `collect_update_from_join_rows`
/// is the single shared classifier.
async fn resolve_update_rows(
    state: &SharedState,
    tenant_id: TenantId,
    task: &PhysicalTask,
) -> crate::Result<Vec<ResolvedUpdateRowWire>> {
    let PhysicalPlan::Document(DocumentOp::UpdateFromJoin {
        target_collection,
        source_collection,
        source_alias,
        target_join_col,
        source_join_col,
        updates,
        target_filters,
        rls_write_check,
        declared_primary_key,
        ..
    }) = &task.plan
    else {
        // Callers only pass an `UpdateFromJoin` task; a mismatch is a bug.
        return Err(crate::Error::PlanError {
            detail: "resolve_update_rows: resolve on non-UPDATE-FROM task".into(),
        });
    };

    // Read the SOURCE where it lives (vShard can differ from target's) and ship raw
    // rows into the plan. Threading `txn_id` folds the source's own staging overlay.
    let source_rows = read_all_source_rows(
        state,
        tenant_id,
        task.database_id,
        source_collection.as_str(),
        task.txn_id,
    )
    .await?;

    // Phase 1: dispatch the read-only RESOLVE pass against the target's core.
    let resolve_plan = PhysicalPlan::Document(DocumentOp::ResolveWrite(Box::new(
        DocumentOp::UpdateFromJoin {
            target_collection: target_collection.clone(),
            source_collection: source_collection.clone(),
            source_alias: source_alias.clone(),
            target_join_col: target_join_col.clone(),
            source_join_col: source_join_col.clone(),
            updates: updates.clone(),
            target_filters: target_filters.clone(),
            returning: None,
            source_rows: Some(source_rows),
            // Read-only: emits no rows, writes nothing, so neither policy gates here.
            rls_filters: Vec::new(),
            // Statement's injected write predicate, carried unchanged.
            rls_write_check: rls_write_check.clone(),
            // Writes nothing, so folds no materialized-sum delta.
            resolved_sum_targets: Vec::new(),
            declared_primary_key: declared_primary_key.clone(),
        },
    )));
    // Passing `txn_id` lets the target scan fold rows this transaction staged earlier.
    let resolve_resp = dispatch_local(
        state,
        tenant_id,
        task.database_id,
        target_collection.as_str(),
        resolve_plan,
        task.txn_id,
    )
    .await?;
    // A refused resolve keeps its Data-Plane code.
    let payload =
        crate::control::server::shared::response_payload::payload_or_typed_error(resolve_resp)?;
    decode_resolved_update_rows(&payload)
}

/// Decode the RESOLVE pass payload (a msgpack `Vec<ResolvedUpdateRowWire>`;
/// see `encode_resolved_update_rows`).
pub(crate) fn decode_resolved_update_rows(
    payload: &[u8],
) -> crate::Result<Vec<ResolvedUpdateRowWire>> {
    if payload.is_empty() {
        return Ok(Vec::new());
    }
    zerompk::from_msgpack(payload).map_err(|e| crate::Error::Serialization {
        format: "msgpack".into(),
        detail: format!("update-from-join resolve rows: {e}"),
    })
}
