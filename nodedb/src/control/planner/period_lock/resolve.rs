// SPDX-License-Identifier: BUSL-1.1

//! Entry points: the per-plan pass wired into `plan_admission`, and the
//! body-driven seam the `MERGE` / `INSERT ... SELECT` / `UPDATE ... FROM`
//! orchestrators call directly.
//!
//! A period lock names its reference row by a VALUE — the write's period
//! column, e.g. `fiscal_period = "2024-Q1"` — read against the reference
//! table's declared primary key (`config.ref_pk`). Turning that value into
//! the reference row's storage key needs the pk → surrogate map, which lives
//! in the catalog redb — Control-Plane state the Data Plane never opens. So
//! the resolution happens here, at plan time, and travels on the plan in the
//! same `resolved_sum_targets` slot the materialized-sum resolution uses:
//! one `(target collection, value) -> surrogate` slot, one resolver on the
//! Data Plane ([`resolved_sum_surrogate`](nodedb_physical::physical_plan::resolved_sum_surrogate)),
//! for every cross-collection identity a write needs.
//!
//! Only the DML shapes the Data Plane's `check_period_lock` actually gates —
//! `PointPut`, `PointInsert`, `BatchInsert`, `Upsert`, `PointDelete`,
//! `PointUpdate`, `BulkUpdate`, `BulkDelete` — are resolved by
//! [`resolve_period_lock_targets`].
//!
//! `PointUpdate` checks BOTH images: the stored row it rewrites and the
//! post-image its assignments produce. A closed period must reject an edit
//! to a row it already holds, and must reject an edit that assigns the
//! period column into it. Both period values are resolved here, so
//! `check_period_lock` finds an entry for whichever image it reads.
//! `BulkUpdate` / `BulkDelete` name their rows by PREDICATE rather than by
//! body or key, so their resolution reads a Control-Plane reconnaissance
//! scan of that same predicate — mirroring the materialized-sum predicate
//! resolution in `materialized_sum::predicate`. A row whose period value
//! drifts between this scan and the Data-Plane apply resolves to no entry
//! and the write is refused as an unknown period — fail-closed, never a
//! silent admit.
//!
//! `MERGE`, `INSERT ... SELECT`, and `UPDATE ... FROM` are Control-Plane
//! orchestrated over several round trips and never reach the per-plan pass —
//! each resolves through [`resolve_period_lock_targets_for_bodies`] in its
//! own orchestrator instead.
//!
//! # Plane discipline
//!
//! Runs on the coordinator's Control Plane (Tokio). The routed lookup and
//! the reconnaissance reads cross the SPSC bridge exactly as `SELECT` does —
//! no storage I/O and no io_uring here.

use nodedb_physical::physical_plan::{DocumentOp, PhysicalPlan, ResolvedSumTarget};
use nodedb_physical::physical_task::PhysicalTask;

use super::gate::{period_lock_gated_collection, push_resolved};
use super::lookup::{PeriodLockScope, lookup_period_surrogate, strip_db_prefix};
use super::point_update::resolve_update_period_values;
use super::predicate::{PeriodLockEffect, resolve_predicate_period_values};
use super::singular::{resolve_batch_period_values, singular_period_value};
use crate::control::state::SharedState;
use crate::types::{DatabaseId, TenantId, TraceId};

/// Resolve the period-lock reference row for every write in `tasks` that a
/// period lock gates, appending the entry to the op's `resolved_sum_targets`
/// slot.
///
/// A period value the resolution cannot bind (no reference row named by that
/// value) adds no entry: `check_period_lock` treats an absent entry as an
/// unknown period and refuses the write, exactly as it treats a genuinely
/// closed one.
pub async fn resolve_period_lock_targets(
    state: &SharedState,
    tasks: &mut [PhysicalTask],
    tenant_id: TenantId,
    database_id: DatabaseId,
    trace_id: TraceId,
) -> crate::Result<()> {
    let catalog = state.credentials.catalog();
    let scope = PeriodLockScope {
        state,
        tenant_id,
        database_id,
        trace_id,
    };
    for task in tasks.iter_mut() {
        let PhysicalPlan::Document(op) = &mut task.plan else {
            continue;
        };
        let Some(collection) = period_lock_gated_collection(op) else {
            continue;
        };
        let collection = collection.to_string();
        let source = strip_db_prefix(database_id, &collection).to_string();
        let Some(def) = catalog
            .get_collection(database_id, tenant_id.as_u64(), &source)?
            .and_then(|coll| coll.period_lock)
        else {
            continue;
        };

        // `BatchInsert` carries a whole page of documents, each with its own
        // period value, rather than the single value every other gated shape
        // carries — resolved and pushed here, distinctly from the singular
        // path below.
        if let DocumentOp::BatchInsert {
            documents,
            resolved_sum_targets,
            ..
        } = op
        {
            let bodies: Vec<&[u8]> = documents.iter().map(|(_, v)| v.as_slice()).collect();
            let resolved =
                resolve_batch_period_values(state, &bodies, &def, tenant_id, database_id, trace_id)
                    .await?;
            resolved_sum_targets.extend(resolved);
            continue;
        }

        // `PointUpdate` carries field assignments rather than a whole row, and
        // it may rewrite the period column itself — both images need their own
        // resolution, distinctly from the single-value path below.
        if let DocumentOp::PointUpdate {
            document_id,
            surrogate,
            updates,
            resolved_sum_targets,
            ..
        } = op
        {
            let resolved = resolve_update_period_values(
                &scope,
                &collection,
                document_id,
                *surrogate,
                updates,
                &def,
            )
            .await?;
            resolved_sum_targets.extend(resolved);
            continue;
        }

        // `BulkUpdate` / `BulkDelete` name their rows by PREDICATE: the
        // Control Plane holds no body and no single key, so the resolution
        // reads a reconnaissance scan of the same predicate instead.
        if let DocumentOp::BulkUpdate {
            filters,
            updates,
            resolved_sum_targets,
            ..
        } = op
        {
            let resolved = resolve_predicate_period_values(
                &scope,
                &collection,
                filters.clone(),
                updates,
                PeriodLockEffect::Assign,
                &def,
            )
            .await?;
            resolved_sum_targets.extend(resolved);
            continue;
        }
        if let DocumentOp::BulkDelete {
            filters,
            resolved_sum_targets,
            ..
        } = op
        {
            let resolved = resolve_predicate_period_values(
                &scope,
                &collection,
                filters.clone(),
                &[],
                PeriodLockEffect::Remove,
                &def,
            )
            .await?;
            resolved_sum_targets.extend(resolved);
            continue;
        }

        let Some(period_key) =
            singular_period_value(state, op, &collection, &def, tenant_id, database_id).await?
        else {
            continue;
        };

        let Some(surrogate) = lookup_period_surrogate(
            state,
            &def.ref_table,
            &period_key,
            tenant_id,
            database_id,
            trace_id,
        )
        .await?
        else {
            // No reference row names this period — leave the slot unresolved
            // so `check_period_lock` reports the unknown-period refusal.
            continue;
        };

        push_resolved(
            op,
            ResolvedSumTarget::new(&def.ref_table, period_key, surrogate),
        );
    }
    Ok(())
}

/// Resolve the period-lock targets a page of row BODIES addresses, for a
/// caller that holds the bodies itself rather than a plan.
///
/// This is the seam `MERGE`, `INSERT ... SELECT`, and `UPDATE ... FROM`
/// orchestrators use: each resolves its own rows on the Control Plane and
/// re-issues concrete work through `dispatch_local`, which never passes
/// through [`resolve_period_lock_targets`]. Mirrors
/// [`resolve_sum_targets_for_bodies`](crate::control::planner::materialized_sum::resolve_sum_targets_for_bodies)
/// — call both and merge the results into one `resolved_sum_targets` vec,
/// since a page can owe both a materialized-sum target and a period lock.
///
/// Returns an empty vec — and issues no lookup at all — when the collection
/// declares no period lock.
pub async fn resolve_period_lock_targets_for_bodies(
    state: &SharedState,
    bodies: &[&[u8]],
    target_collection: &str,
    tenant_id: TenantId,
    database_id: DatabaseId,
    trace_id: TraceId,
) -> crate::Result<Vec<ResolvedSumTarget>> {
    let catalog = state.credentials.catalog();
    let source = strip_db_prefix(database_id, target_collection);
    let Some(def) = catalog
        .get_collection(database_id, tenant_id.as_u64(), source)?
        .and_then(|coll| coll.period_lock)
    else {
        return Ok(Vec::new());
    };
    resolve_batch_period_values(state, bodies, &def, tenant_id, database_id, trace_id).await
}

/// Whether `target_collection` declares a period lock.
///
/// The gate an orchestrator checks FIRST, alongside
/// [`source_drives_bindings`](crate::control::planner::materialized_sum::source_drives_bindings):
/// a target with neither a materialized-sum binding nor a period lock skips
/// the RESOLVE round trip its statement would otherwise pay for nothing.
pub fn target_declares_period_lock(
    state: &SharedState,
    target_collection: &str,
    tenant_id: TenantId,
    database_id: DatabaseId,
) -> crate::Result<bool> {
    let catalog = state.credentials.catalog();
    let source = strip_db_prefix(database_id, target_collection);
    Ok(catalog
        .get_collection(database_id, tenant_id.as_u64(), source)?
        .and_then(|coll| coll.period_lock)
        .is_some())
}
