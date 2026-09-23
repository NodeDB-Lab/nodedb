// SPDX-License-Identifier: BUSL-1.1

//! Permission-tree resolution for meta / maintenance operations.

use nodedb_physical::physical_plan::MetaOp;

use super::context::PermCtx;
use super::plan::walk;

/// Exhaustive over [`MetaOp`] so a new meta operation forces a decision
/// between filtering, refusing, and no-op.
pub(super) fn apply_meta(ctx: &PermCtx<'_>, op: &mut MetaOp) -> crate::Result<()> {
    match op {
        // Refuse: the last-value cache returns the most recently observed
        // `(timestamp, value)` of the collection's series — stored row
        // content, through a cache payload with no filter slot. A tree that
        // puts a series outside the caller's subtree must not let its latest
        // sample be read back.
        MetaOp::QueryLastValues { collection, .. } | MetaOp::QueryLastValue { collection, .. } => {
            ctx.refuse_if_tree(
                collection,
                "the last-value cache returns stored samples through a payload that carries no \
                 subtree filter",
            )
        }

        // Refuse: a byte-size estimate is derived from every stored row of the
        // collection, including the ones outside the subtree, and carries no
        // resource column to filter on. `name` is a bare collection name —
        // qualify it against this task's database before the tree lookup.
        MetaOp::QueryCollectionSize { name, .. } => ctx.refuse_if_tree(
            &nodedb_types::QualifiedCollection::new(ctx.database_id, name),
            "the size estimate is derived from every stored row, which the subtree filter cannot \
             be evaluated against",
        ),

        // Refuse: a tenant snapshot exports every document and index of the
        // tenant. It names no collection, so the narrow per-collection
        // question cannot be asked.
        MetaOp::CreateTenantSnapshot { .. } => ctx.refuse_if_any_tree(
            "a tenant snapshot exports every stored row of every collection, which the subtree \
             filter cannot be evaluated against",
        ),

        // Recurse: these carry nested physical plans, so a governed operation
        // buried in one is still resolved. Walking them keeps the invariant
        // that no plan reaches the Data Plane without this pass having seen
        // every node of it.
        MetaOp::TransactionBatch { plans, .. }
        | MetaOp::CalvinExecuteStatic { plans, .. }
        | MetaOp::CalvinExecuteActive { plans, .. }
        | MetaOp::RecordCalvinWriteVersions { plans, .. }
        | MetaOp::ResolveTxn { plans, .. } => {
            for plan in plans.iter_mut() {
                walk(ctx, plan)?;
            }
            Ok(())
        }

        MetaOp::StageWrite { plan } => walk(ctx, plan),

        // No-op: the Calvin passive participant reads keys chosen by the
        // deterministic scheduler for a transaction whose submitting statement
        // already went through this pass on the Control Plane that admitted
        // it, and its key list names no collection to key a second check on.
        MetaOp::CalvinExecutePassive { .. } => Ok(()),

        // No-op: durability, cancellation, snapshot install, purge, retention,
        // index and synonym maintenance, transaction-overlay bookkeeping, and
        // continuous-aggregate administration. None of these acts on rows on
        // behalf of a caller's statement — they are server-owned maintenance
        // and DDL, authorized as such rather than against a permission level.
        MetaOp::WalAppend { .. }
        | MetaOp::Cancel { .. }
        | MetaOp::CreateSnapshot
        | MetaOp::Compact
        | MetaOp::Checkpoint
        | MetaOp::RegisterContinuousAggregate { .. }
        | MetaOp::UnregisterContinuousAggregate { .. }
        | MetaOp::ListContinuousAggregates
        | MetaOp::ConvertCollection { .. }
        | MetaOp::RestoreTenantSnapshot { .. }
        | MetaOp::PurgeTenant { .. }
        | MetaOp::UnregisterCollection { .. }
        | MetaOp::UnregisterMaterializedView { .. }
        | MetaOp::EnforceTimeseriesRetention { .. }
        | MetaOp::TemporalPurgeEdgeStore { .. }
        | MetaOp::TemporalPurgeDocumentStrict { .. }
        | MetaOp::TemporalPurgeColumnar { .. }
        | MetaOp::TemporalPurgeCrdt { .. }
        | MetaOp::TemporalPurgeArray { .. }
        | MetaOp::AlterArray { .. }
        | MetaOp::ApplyContinuousAggRetention
        | MetaOp::QueryAggregateWatermark { .. }
        | MetaOp::RebuildIndex { .. }
        | MetaOp::PutSynonymGroup { .. }
        | MetaOp::DeleteSynonymGroup { .. }
        | MetaOp::RenameCollection { .. }
        | MetaOp::DropTxnOverlay { .. }
        | MetaOp::MarkSavepoint { .. }
        | MetaOp::RollbackToSavepoint { .. }
        | MetaOp::CalvinFlush { .. }
        | MetaOp::CalvinDrop { .. }
        | MetaOp::CalvinResolve { .. }
        | MetaOp::ApplyTransactionRedo { .. } => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{DocumentOp, MetaOp};

    use super::super::plan::test_support::{apply, assert_refused, cache_with_tree};
    use crate::bridge::envelope::PhysicalPlan;

    /// A plan nested in a transaction batch is still walked.
    #[test]
    fn transaction_batch_children_are_walked() {
        let cache = cache_with_tree("docs");
        let mut plan = PhysicalPlan::Meta(MetaOp::TransactionBatch {
            plans: vec![PhysicalPlan::Document(DocumentOp::IndexLookup {
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    "docs",
                ),
                path: "$.email".into(),
                value: "a@b.c".into(),
            })],
            txn_id: None,
        });
        assert_refused(apply(&mut plan, &cache), "docs");
    }

    /// A tenant snapshot exports every row, so any tree refuses it.
    #[test]
    fn tenant_snapshot_is_refused_while_any_tree_applies() {
        let cache = cache_with_tree("docs");
        let mut plan = PhysicalPlan::Meta(MetaOp::CreateTenantSnapshot { tenant_id: 1 });
        assert!(matches!(
            apply(&mut plan, &cache),
            Err(crate::Error::PlanError { .. })
        ));
    }
}
