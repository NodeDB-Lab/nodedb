// SPDX-License-Identifier: BUSL-1.1

//! Lower a `SqlPlan::UpdateFrom` to a `DocumentOp::UpdateFromJoin` physical task.
//!
//! The source plan lowers to the relation the orchestrator reads, and the
//! source alias comes from the plan. Assignments are converted with
//! table-qualified column references so the Data Plane can resolve `src.col`
//! against the merged `{target + "src.col": ...}` doc.

use nodedb_sql::types::{Filter, SqlExpr, SqlPlan};

use crate::bridge::envelope::PhysicalPlan;
use crate::types::TenantId;
use nodedb_physical::physical_plan::*;

use crate::control::planner::sql_plan_convert::convert::ConvertContext;
use crate::control::planner::sql_plan_convert::filter::serialize_filters;
use crate::control::planner::sql_plan_convert::value::assignments_to_update_values_qualified;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

/// Parameters for [`convert_update_from`], bundled to avoid an unwieldy
/// argument list. Fields borrow from the caller exactly as the individual
/// arguments did before this refactor — no new allocations.
pub(in crate::control::planner::sql_plan_convert) struct UpdateFromParams<'a> {
    pub collection: &'a str,
    pub source: &'a SqlPlan,
    /// The name assignments read source columns under (`<alias>.<col>`).
    pub source_alias: &'a str,
    pub target_join_col: &'a str,
    pub source_join_col: &'a str,
    pub assignments: &'a [(String, SqlExpr)],
    pub target_filters: &'a [Filter],
    pub returning: bool,
    pub tenant_id: TenantId,
    pub ctx: &'a ConvertContext,
}

pub(in crate::control::planner::sql_plan_convert) fn convert_update_from(
    params: UpdateFromParams<'_>,
) -> crate::Result<Vec<PhysicalTask>> {
    let UpdateFromParams {
        collection,
        source,
        source_alias,
        target_join_col,
        source_join_col,
        assignments,
        target_filters,
        returning: _returning,
        tenant_id,
        ctx,
    } = params;
    let collection_key = ctx.collection_key(collection);
    let qualified_collection = nodedb_types::QualifiedCollection::new(ctx.database_id, collection);
    // The source is any SELECT: its own filters (the WHERE conjuncts over
    // the source alone, or a CTE body's WHERE) and its read policy apply.
    // The whole source is read before the first write, so no collection is
    // written while it is read.
    let source = crate::control::planner::sql_plan_convert::row_source::lower_row_source(
        source,
        crate::control::planner::sql_plan_convert::row_source::ColumnMaps::PASSTHROUGH,
        None,
        tenant_id,
        ctx,
    )?;

    let updates = assignments_to_update_values_qualified(assignments)?;
    let target_filter_bytes = serialize_filters(target_filters)?;
    let vshard = collection_key.vshard();
    // A declared PRIMARY KEY implies NOT NULL; the Data Plane checks the
    // post-image against this name once the SET expressions are evaluated.
    let declared_primary_key = super::super::declared_primary_key_name(ctx, collection_key)?;

    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        database_id: ctx.database_id,
        plan: PhysicalPlan::Document(DocumentOp::UpdateFromJoin {
            target_collection: qualified_collection,
            source,
            source_alias: source_alias.to_string(),
            target_join_col: target_join_col.into(),
            source_join_col: source_join_col.into(),
            updates,
            target_filters: target_filter_bytes,
            returning: None,
            // The orchestrator reads `source` and ships its rows in; the
            // neutral plan carries none.
            source_rows: None,
            // Both filled in by the RLS injection pass, which runs after
            // conversion — the read filter gating `returning` and the write
            // predicate gating the persist are separate slots.
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            // Filled in by the materialized-sum resolution pass, which
            // recon-scans the target rows this join matches.
            resolved_sum_targets: Vec::new(),
            declared_primary_key,
            // The statement cut pass pins it to the block's snapshot inside
            // a transaction block.
            target_system_time: nodedb_types::SystemTimeScope::Current,
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }])
}
