// SPDX-License-Identifier: BUSL-1.1

//! Resolve + emit the concrete point ops for one in-transaction
//! `INSERT ... SELECT`.
//!
//! Only an in-transaction `INSERT ... SELECT` reaches here (autocommit uses
//! `run_insert_select`'s scan → `BatchInsert` path instead). The plan is
//! resolved NOW, not buffered for COMMIT: the source scans base ∪ overlay,
//! each copied row gets its own fresh registered surrogate, and the
//! resulting `PointInsert` ops stage into the overlay via the normal
//! statement-time path — giving read-your-own-writes and a target-owned
//! `(collection, surrogate)→pk` binding for cross-engine lookups.
//!
//! Emits `PointInsert`, not `BatchInsert`: only `PointPut`/`PointInsert`/
//! `PointDelete` have an undo-tracked arm in transactional replay: a
//! `BatchInsert` here would survive an atomic rollback (partial commit).

use nodedb_types::{DatabaseId, Surrogate, TenantId};

use crate::bridge::envelope::PhysicalPlan;
use crate::control::insert_select::copy_rows::{assign_page_rows, resolve_copy_spec};
use crate::control::maintenance::clone_materializer::scan_source_page;
use crate::control::state::SharedState;
use crate::types::{TxnId, VShardId};
use nodedb_physical::physical_plan::DocumentOp;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

/// Resolve one in-transaction `DocumentOp::InsertSelect` task into the
/// concrete, fresh-surrogate `PointInsert` tasks its copied rows expand to.
///
/// `task.txn_id` must be set to the active transaction so the scan folds
/// earlier-staged rows. Emitted ops carry the same `txn_id` and a recomputed
/// target vShard (as the MERGE / `UPDATE ... FROM` expanders do). The caller
/// stages + buffers each returned op.
pub(crate) async fn resolve_and_emit_insert_select_ops(
    state: &SharedState,
    tenant_id: TenantId,
    task: &PhysicalTask,
) -> crate::Result<Vec<PhysicalTask>> {
    let PhysicalPlan::Document(DocumentOp::InsertSelect {
        target_collection,
        source_collection,
        source_filters,
        source_limit,
        column_map,
    }) = &task.plan
    else {
        // Callers only pass an `InsertSelect` task; a mismatch is a bug.
        return Err(crate::Error::PlanError {
            detail: "resolve_and_emit_insert_select_ops: non-INSERT-SELECT task".into(),
        });
    };

    // Scan the source (base ∪ overlay via `task.txn_id`), assign each row a
    // fresh, catalog-registered surrogate.
    let rows = materialize_copy(
        state,
        MaterializeCopy {
            tenant_id,
            database_id: task.database_id,
            target_collection: target_collection.as_str(),
            source_collection: source_collection.as_str(),
            source_filters,
            source_limit: *source_limit,
            column_map,
            txn_id: task.txn_id,
        },
    )
    .await?;

    // Recompute the target vShard (rather than reusing the staged task's)
    // to keep dispatch classification honest, as the MERGE expander does.
    let vshard_id =
        VShardId::from_collection_in_database(task.database_id, target_collection.as_str());

    // Resolve materialized-sum targets: these ops stage directly, bypassing
    // statement-level resolution, so without this a bound target collection
    // would fold against an empty resolution.
    let sum_bodies: Vec<&[u8]> = rows.iter().map(|(_, value, _)| value.as_slice()).collect();
    let mut resolved_sum_targets =
        crate::control::planner::materialized_sum::resolve_sum_targets_for_bodies(
            state,
            &sum_bodies,
            target_collection.as_str(),
            tenant_id,
            task.database_id,
            crate::types::TraceId::ZERO,
        )
        .await?;
    // Period-lock target for the same rows, in the SAME slot — a target
    // collection under a period lock reads its reference row's surrogate off
    // `resolved_sum_targets` exactly like a materialized-sum fold does.
    resolved_sum_targets.extend(
        crate::control::planner::period_lock::resolve_period_lock_targets_for_bodies(
            state,
            &sum_bodies,
            target_collection.as_str(),
            tenant_id,
            task.database_id,
            crate::types::TraceId::ZERO,
        )
        .await?,
    );

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
                // Expanded internal writes answer no client — see the
                // orchestrator's paged batch insert.
                returning: None,
                rls_filters: Vec::new(),
                resolved_sum_targets: resolved_sum_targets.clone(),
                deferred_sum_targets: Vec::new(),
            }),
            post_set_op: PostSetOp::None,
            txn_id: task.txn_id,
        });
    }
    Ok(out)
}

/// Inputs for [`materialize_copy`], bundled to keep the copy pipeline within
/// argument limits.
struct MaterializeCopy<'a> {
    tenant_id: TenantId,
    database_id: DatabaseId,
    target_collection: &'a str,
    source_collection: &'a str,
    source_filters: &'a [u8],
    source_limit: usize,
    column_map: &'a [u8],
    txn_id: Option<TxnId>,
}

/// Scan the source page-by-page and produce the concrete target rows:
/// `(target_document_id, msgpack_value, fresh_surrogate)`, one per surviving
/// source row. Reuses the shared [`resolve_copy_spec`] / [`assign_page_rows`]
/// pipeline (scan → filter → assign) so filtering and identity derivation stay
/// identical to the autocommit path.
async fn materialize_copy(
    state: &SharedState,
    args: MaterializeCopy<'_>,
) -> crate::Result<Vec<(String, Vec<u8>, Surrogate)>> {
    let MaterializeCopy {
        tenant_id,
        database_id,
        target_collection,
        source_collection,
        source_filters,
        source_limit,
        column_map,
        txn_id,
    } = args;
    let spec = resolve_copy_spec(
        state,
        tenant_id,
        database_id,
        target_collection,
        source_filters,
        column_map,
    )?;

    let mut cursor: Vec<u8> = Vec::new();
    let mut remaining = source_limit;
    let mut rows: Vec<(String, Vec<u8>, Surrogate)> = Vec::new();

    while remaining > 0 {
        let (entries, next_cursor) = scan_source_page(
            state,
            tenant_id,
            database_id,
            source_collection,
            &cursor,
            None,
            txn_id,
        )
        .await?;

        let page = assign_page_rows(
            state,
            tenant_id,
            database_id,
            target_collection,
            &spec,
            entries,
            &mut remaining,
        )?;
        rows.extend(page);

        if next_cursor.is_empty() {
            break;
        }
        cursor = next_cursor;
    }

    Ok(rows)
}
