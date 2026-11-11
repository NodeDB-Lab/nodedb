// SPDX-License-Identifier: BUSL-1.1

//! Resolve + emit the concrete point ops for one in-transaction
//! `INSERT ... SELECT`.
//!
//! Only an `INSERT ... SELECT` in a transaction reaches here: a client's
//! block, or the implicit transaction a statement whose `RETURNING` list can
//! fail runs in. Every other autocommit copy takes the orchestrator's paged
//! `BatchInsert` path. The plan is
//! resolved NOW, not buffered for COMMIT: the source reads base ∪ overlay,
//! each copied row gets its own fresh registered surrogate, and the
//! resulting `PointInsert` ops stage into the overlay via the normal
//! statement-time path — giving read-your-own-writes and a target-owned
//! `(collection, surrogate)→pk` binding for cross-engine lookups.
//!
//! Emits `PointInsert`, not `BatchInsert`: only `PointPut`/`PointInsert`/
//! `PointDelete` have an undo-tracked arm in transactional replay: a
//! `BatchInsert` here survives an atomic rollback (partial commit).

use nodedb_types::TenantId;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::insert_select::copy_rows::{assign_page_rows, resolve_copy_spec};
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::source_read::{SourceReadContext, read_source_rows};
use crate::control::state::SharedState;
use nodedb_physical::physical_plan::DocumentOp;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

/// Resolve one in-transaction `DocumentOp::InsertSelect` task into the
/// concrete, fresh-surrogate `PointInsert` tasks its copied rows expand to.
///
/// `task.txn_id` must be set to the active transaction so the source read
/// folds earlier-staged rows. The whole source is read before any row is
/// staged, so it is held to the scan byte budget and fails loudly past it.
/// Emitted ops carry the same `txn_id` and a recomputed target vShard (as the
/// MERGE / `UPDATE ... FROM` expanders do). The ops carry no sum or
/// period-lock resolution: the caller runs the statement pass on them, then
/// stages + buffers each one.
pub(crate) async fn resolve_and_emit_insert_select_ops(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    tenant_id: TenantId,
    task: &PhysicalTask,
) -> crate::Result<Vec<PhysicalTask>> {
    let PhysicalPlan::Document(DocumentOp::InsertSelect {
        target_collection,
        source,
        returning,
        rls_filters,
    }) = &task.plan
    else {
        // Callers only pass an `InsertSelect` task; a mismatch is a bug.
        return Err(crate::Error::PlanError {
            detail: "resolve_and_emit_insert_select_ops: non-INSERT-SELECT task".into(),
        });
    };

    let spec = resolve_copy_spec(
        state,
        tenant_id,
        task.database_id,
        target_collection.as_str(),
    )?;
    let ctx = SourceReadContext {
        state,
        identity,
        tenant_id,
        database_id: task.database_id,
        txn_id: task.txn_id,
    };
    // Read the source (base ∪ overlay via `task.txn_id`), then assign each
    // row a fresh, catalog-registered surrogate.
    let source_rows = read_source_rows(&ctx, source).await?;
    let rows = if source_rows.is_empty() {
        Vec::new()
    } else {
        assign_page_rows(
            state,
            tenant_id,
            task.database_id,
            target_collection.as_str(),
            &spec,
            source_rows,
        )
        .await?
    };

    // Recompute the target vShard (rather than reusing the staged task's)
    // to keep dispatch classification honest, as the MERGE expander does.
    let vshard_id = nodedb_types::CollectionKey::from_qualified_str(
        task.database_id,
        target_collection.as_str(),
    )?
    .vshard();

    let mut out: Vec<PhysicalTask> = Vec::with_capacity(rows.len());
    for (document_id, value, surrogate) in rows {
        out.push(PhysicalTask {
            tenant_id: task.tenant_id,
            vshard_id,
            database_id: task.database_id,
            plan: PhysicalPlan::Document(DocumentOp::PointInsert {
                collection: target_collection.clone(),
                document_id,
                value,
                if_absent: false,
                surrogate,
                // Each copied row answers the statement's `RETURNING` from
                // the row it stages, in copy order, under the target's read
                // policy.
                returning: returning.clone(),
                rls_filters: rls_filters.clone(),
                // Filled by the caller's sum and period-lock pass, the one
                // a plain INSERT takes.
                resolved_sum_targets: Vec::new(),
                deferred_sum_targets: Vec::new(),
                rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
            }),
            post_set_op: PostSetOp::None,
            txn_id: task.txn_id,
        });
    }
    Ok(out)
}
