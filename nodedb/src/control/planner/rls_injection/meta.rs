// SPDX-License-Identifier: BUSL-1.1

//! RLS resolution for meta / maintenance operations.

use nodedb_physical::physical_plan::MetaOp;

use super::context::RlsCtx;
use super::plan::walk;

/// Exhaustive over [`MetaOp`] so a new meta operation forces a decision
/// between injecting, refusing, and no-op.
pub(super) fn inject_meta(ctx: &RlsCtx<'_>, op: &mut MetaOp) -> crate::Result<()> {
    match op {
        // Refuse: the last-value cache returns the most recently observed
        // `(timestamp, value)` of the collection's series — stored row content,
        // through a cache payload with no row-filter slot. A policy that hides
        // a series' rows must not let its latest sample be read back.
        MetaOp::QueryLastValues { collection, .. } | MetaOp::QueryLastValue { collection, .. } => {
            ctx.refuse_if_policy(
                collection,
                "the last-value cache returns stored samples through a payload that carries no \
                 row filter",
            )
        }

        // Refuse: a byte-size estimate is derived from every stored row of the
        // collection, including the ones the policy hides, and carries no row
        // to filter. `name` is a bare collection name — qualify it against
        // this task's database before the policy lookup.
        MetaOp::QueryCollectionSize { name, .. } => ctx.refuse_if_policy(
            &nodedb_types::QualifiedCollection::new(ctx.database_id, name),
            "the size estimate is derived from every stored row, which the row filter cannot be \
             evaluated against",
        ),

        // Refuse: a tenant snapshot exports every document and index of the
        // tenant. It names no collection, so the narrow per-collection
        // question cannot be asked.
        MetaOp::CreateTenantSnapshot { .. } => ctx.refuse_if_any_policy(
            "a tenant snapshot exports every stored row of every collection, which the row filter \
             cannot be evaluated against",
        ),

        // Recurse: these carry nested physical plans. They hold write plans in
        // practice — so the walk finds nothing to inject — but walking them
        // keeps the invariant that no plan reaches the Data Plane without this
        // pass having seen every node of it.
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
        // continuous-aggregate administration. None of these returns stored
        // rows to a caller, and none writes a user row a policy predicate could
        // be evaluated against — they are authorized by the permission check
        // that precedes this pass.
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

    use super::super::plan::test_support::{assert_refused, inject, store_with_read_policy};
    use crate::bridge::envelope::PhysicalPlan;

    /// A plan nested in a transaction batch is still walked.
    #[test]
    fn transaction_batch_children_are_walked() {
        let store = store_with_read_policy("users");
        let mut plan = PhysicalPlan::Meta(MetaOp::TransactionBatch {
            plans: vec![PhysicalPlan::Document(DocumentOp::IndexLookup {
                collection: nodedb_types::QualifiedCollection::new(
                    nodedb_types::DatabaseId::DEFAULT,
                    "users",
                ),
                path: "$.email".into(),
                value: "a@b.c".into(),
            })],
            txn_id: None,
        });
        assert_refused(inject(&mut plan, &store), "users");
    }

    /// A tenant snapshot exports every row, so any read policy refuses it.
    #[test]
    fn tenant_snapshot_is_refused_while_any_policy_applies() {
        let store = store_with_read_policy("users");
        let mut plan = PhysicalPlan::Meta(MetaOp::CreateTenantSnapshot {
            tenant_id: 1,
            cut_watermark: None,
        });
        assert!(matches!(
            inject(&mut plan, &store),
            Err(crate::Error::PlanError { .. })
        ));
    }
}
