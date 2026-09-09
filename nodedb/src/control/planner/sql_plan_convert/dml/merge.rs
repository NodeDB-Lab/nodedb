// SPDX-License-Identifier: BUSL-1.1

//! Lower `SqlPlan::Merge` to `DocumentOp::Merge` physical task.

use nodedb_sql::types::{MergeClauseKind, MergePlanAction, MergePlanClause, SqlExpr, SqlPlan};

use crate::bridge::envelope::PhysicalPlan;
use crate::types::{TenantId, VShardId};
use nodedb_physical::physical_plan::DocumentOp;
use nodedb_physical::physical_plan::UpdateValue;
use nodedb_physical::physical_plan::document::merge_types::{
    MergeActionOp, MergeClauseKind as MergeClauseKindOp, MergeClauseOp,
};

use super::super::expr::sql_expr_to_bridge_expr_qualified;
use super::super::filter::serialize_filters;
use super::super::value::{assignments_to_update_values_qualified, sql_value_to_msgpack};
use nodedb_physical::physical_task::{PhysicalTask, PostSetOp};

/// Bundled arguments for [`convert_merge`].
pub(in super::super) struct ConvertMergeArgs<'a> {
    pub target: &'a str,
    pub source: &'a SqlPlan,
    pub target_join_col: &'a str,
    pub source_join_col: &'a str,
    pub source_alias: &'a str,
    pub clauses: &'a [MergePlanClause],
    pub returning: bool,
    pub tenant_id: TenantId,
    pub ctx: &'a super::super::convert::ConvertContext,
}

/// Lower a `SqlPlan::Merge` to a single `DocumentOp::Merge` physical task.
pub(in super::super) fn convert_merge(
    args: ConvertMergeArgs<'_>,
) -> crate::Result<Vec<PhysicalTask>> {
    let ConvertMergeArgs {
        target,
        source,
        target_join_col,
        source_join_col,
        source_alias,
        clauses,
        returning: _returning,
        tenant_id,
        ctx,
    } = args;
    let target_qualified = super::super::convert::db_qualified(ctx.database_id, target);
    let qualified_target = nodedb_types::QualifiedCollection::new(ctx.database_id, target);
    let target = target_qualified.as_str();
    // Extract source collection name from the source scan plan.
    let source_collection = match source {
        SqlPlan::Scan { collection, .. } => {
            nodedb_types::QualifiedCollection::new(ctx.database_id, collection)
        }
        SqlPlan::DocumentIndexLookup { collection, .. } => {
            nodedb_types::QualifiedCollection::new(ctx.database_id, collection)
        }
        other => {
            return Err(crate::Error::PlanError {
                detail: format!("Merge source must be a Scan plan, got: {other:?}"),
            });
        }
    };

    let clause_ops = clauses
        .iter()
        .map(convert_clause)
        .collect::<crate::Result<Vec<_>>>()?;

    let vshard = VShardId::from_collection_in_database(ctx.database_id, target);
    // A declared PRIMARY KEY implies NOT NULL; the Data Plane checks a MATCHED
    // or NOT-MATCHED-BY-SOURCE UPDATE arm's post-image against this name.
    let declared_primary_key = super::declared_primary_key_name(ctx, target)?;

    Ok(vec![PhysicalTask {
        tenant_id,
        vshard_id: vshard,
        database_id: ctx.database_id,
        plan: PhysicalPlan::Document(DocumentOp::Merge {
            target_collection: qualified_target,
            source_collection,
            source_alias: source_alias.into(),
            target_join_col: target_join_col.into(),
            source_join_col: source_join_col.into(),
            clauses: clause_ops,
            // Projected columns live only in raw SQL, attached as a
            // `ReturningSpec` by the RETURNING pre-processor after
            // conversion; same shape as sibling UPDATE/DELETE conversions.
            returning: None,
            // Autocommit MERGE is intercepted and driven by
            // `control::merge_orchestrator`, which re-issues this wrapped
            // in `ResolveWrite`. This neutral form never reaches the Data
            // Plane directly.
            resolved_inserts: None,
            // The source rows are shipped in by the Control-Plane orchestrator
            // (cross-core source-ship); the neutral plan carries none.
            source_rows: None,
            // Both filled in by the RLS injection pass, which runs after
            // conversion — the read filter gating `returning` and the write
            // predicate gating the persist are separate slots.
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            // Filled in by the merge orchestrator from its RESOLVE pass's arms;
            // the neutral plan has no classification to derive keys from.
            resolved_sum_targets: Vec::new(),
            declared_primary_key,
        }),
        post_set_op: PostSetOp::None,
        txn_id: None,
    }])
}

fn convert_clause(clause: &MergePlanClause) -> crate::Result<MergeClauseOp> {
    let kind = match clause.kind {
        MergeClauseKind::Matched => MergeClauseKindOp::Matched,
        MergeClauseKind::NotMatched => MergeClauseKindOp::NotMatched,
        MergeClauseKind::NotMatchedBySource => MergeClauseKindOp::NotMatchedBySource,
    };

    let extra_predicate = serialize_filters(&clause.extra_predicate)?;

    let action = convert_action(&clause.action)?;

    Ok(MergeClauseOp {
        kind,
        extra_predicate,
        action,
    })
}

fn convert_action(action: &MergePlanAction) -> crate::Result<MergeActionOp> {
    match action {
        MergePlanAction::Update { assignments } => {
            let updates = assignments_to_update_values_qualified(assignments)?;
            Ok(MergeActionOp::Update { updates })
        }
        MergePlanAction::Delete => Ok(MergeActionOp::Delete),
        MergePlanAction::Insert { columns, values } => {
            // Value expressions reference the source row (`s.col`, `s.qty * 2`),
            // so they qualify column names exactly like the UPDATE SET arm and
            // are evaluated against the qualified source document at apply time.
            let encoded: Vec<UpdateValue> = values
                .iter()
                .map(|expr| match expr {
                    SqlExpr::Literal(v) => UpdateValue::Literal(sql_value_to_msgpack(v)),
                    other => UpdateValue::Expr(sql_expr_to_bridge_expr_qualified(other)),
                })
                .collect();
            Ok(MergeActionOp::Insert {
                columns: columns.clone(),
                values: encoded,
            })
        }
        MergePlanAction::DoNothing => Ok(MergeActionOp::DoNothing),
    }
}
