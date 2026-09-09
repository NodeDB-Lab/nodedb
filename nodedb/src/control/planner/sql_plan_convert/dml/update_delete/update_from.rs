// SPDX-License-Identifier: BUSL-1.1

//! Lower a `SqlPlan::UpdateFrom` to a `DocumentOp::UpdateFromJoin` physical task.
//!
//! The source collection name and alias are extracted from the `source` plan.
//! Assignments are converted with table-qualified column references so the Data
//! Plane can resolve `src.col` against the merged `{target + "src.col": ...}` doc.

use nodedb_sql::types::{Filter, SqlExpr, SqlPlan};

use crate::bridge::envelope::PhysicalPlan;
use crate::types::TenantId;
use nodedb_physical::physical_plan::*;

use crate::control::planner::sql_plan_convert::convert::ConvertContext;
use crate::control::planner::sql_plan_convert::filter::serialize_filters;
use crate::control::planner::sql_plan_convert::value::assignments_to_update_values_qualified;
use crate::types::VShardId;
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

/// Parameters for [`convert_update_from`], bundled to avoid an unwieldy
/// argument list. Fields borrow from the caller exactly as the individual
/// arguments did before this refactor — no new allocations.
pub(in crate::control::planner::sql_plan_convert) struct UpdateFromParams<'a> {
    pub collection: &'a str,
    pub source: &'a SqlPlan,
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
        target_join_col,
        source_join_col,
        assignments,
        target_filters,
        returning: _returning,
        tenant_id,
        ctx,
    } = params;
    let coll_qualified = crate::control::planner::sql_plan_convert::convert::db_qualified(
        ctx.database_id,
        collection,
    );
    let qualified_collection = nodedb_types::QualifiedCollection::new(ctx.database_id, collection);
    let collection = coll_qualified.as_str();
    // Extract source collection name and alias from the source scan plan.
    let (source_collection, source_alias) = match source {
        SqlPlan::Scan {
            collection, alias, ..
        } => {
            let qualified = nodedb_types::QualifiedCollection::new(ctx.database_id, collection);
            let alias_str = alias.as_deref().unwrap_or(collection.as_str()).to_string();
            (qualified, alias_str)
        }
        SqlPlan::DocumentIndexLookup {
            collection, alias, ..
        } => {
            let qualified = nodedb_types::QualifiedCollection::new(ctx.database_id, collection);
            let alias_str = alias.as_deref().unwrap_or(collection.as_str()).to_string();
            (qualified, alias_str)
        }
        other => {
            return Err(crate::Error::PlanError {
                detail: format!("UpdateFrom source must be a Scan plan, got: {other:?}"),
            });
        }
    };

    let updates = assignments_to_update_values_qualified(assignments)?;
    let target_filter_bytes = serialize_filters(target_filters)?;
    let vshard = VShardId::from_collection_in_database(ctx.database_id, collection);
    // A declared PRIMARY KEY implies NOT NULL; the Data Plane checks the
    // post-image against this name once the SET expressions are evaluated.
    let declared_primary_key = super::super::declared_primary_key_name(ctx, collection)?;

    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        database_id: ctx.database_id,
        plan: PhysicalPlan::Document(DocumentOp::UpdateFromJoin {
            target_collection: qualified_collection,
            source_collection,
            source_alias,
            target_join_col: target_join_col.into(),
            source_join_col: source_join_col.into(),
            updates,
            target_filters: target_filter_bytes,
            returning: None,
            // The source rows are shipped in by the Control-Plane orchestrator
            // (cross-core source-ship); the neutral plan carries none.
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
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }])
}
