// SPDX-License-Identifier: BUSL-1.1

//! Footprints of the core's own ops and of cluster event reads.
//!
//! A meta op checkpoints, snapshots, purges, installs or stages across
//! collections, so each one is a core task. A cancel and a redo stream page
//! read touch no state an install writes.

use nodedb_physical::physical_plan::{ClusterEventOp, MetaOp};

use super::footprint::Footprint;

pub(super) fn meta_footprint(op: &MetaOp) -> Footprint {
    match op {
        MetaOp::Cancel { .. } | MetaOp::ReadRedoStreamPage { .. } => Footprint::Keys(Vec::new()),
        MetaOp::WalAppend { .. }
        | MetaOp::TransactionBatch { .. }
        | MetaOp::CreateSnapshot
        | MetaOp::Compact
        | MetaOp::Checkpoint
        | MetaOp::RegisterContinuousAggregate { .. }
        | MetaOp::InstallAggregateImage { .. }
        | MetaOp::UnregisterContinuousAggregate { .. }
        | MetaOp::ListContinuousAggregates
        | MetaOp::ContinuousAggregatesRegistered
        | MetaOp::AwaitContinuousAggregateRefresh { .. }
        | MetaOp::ConvertCollection { .. }
        | MetaOp::CreateTenantSnapshot { .. }
        | MetaOp::RestoreTenantSnapshot { .. }
        | MetaOp::PurgeDepartedVShards { .. }
        | MetaOp::PurgeTenant { .. }
        | MetaOp::ListTenantCollections { .. }
        | MetaOp::UnregisterCollection { .. }
        | MetaOp::UnregisterMaterializedView { .. }
        | MetaOp::QueryCollectionSize { .. }
        | MetaOp::VerifyHashChain { .. }
        | MetaOp::EnforceTimeseriesRetention { .. }
        | MetaOp::TemporalPurgeEdgeStore { .. }
        | MetaOp::TemporalPurgeDocumentStrict { .. }
        | MetaOp::TemporalPurgeColumnar { .. }
        | MetaOp::TemporalPurgeCrdt { .. }
        | MetaOp::TemporalPurgeArray { .. }
        | MetaOp::AlterArray { .. }
        | MetaOp::ApplyContinuousAggRetention { .. }
        | MetaOp::QueryAggregateWatermark { .. }
        | MetaOp::QueryLastValues { .. }
        | MetaOp::QueryLastValue { .. }
        | MetaOp::CalvinExecuteStatic { .. }
        | MetaOp::CalvinExecutePassive { .. }
        | MetaOp::CalvinExecuteActive { .. }
        | MetaOp::RebuildIndex { .. }
        | MetaOp::PutSynonymGroup { .. }
        | MetaOp::DeleteSynonymGroup { .. }
        | MetaOp::SetCustomCrsSet { .. }
        | MetaOp::StageWrite { .. }
        | MetaOp::DropTxnOverlay { .. }
        | MetaOp::MarkSavepoint { .. }
        | MetaOp::RollbackToSavepoint { .. }
        | MetaOp::CalvinDrop { .. }
        | MetaOp::ResolveTxnPart { .. }
        | MetaOp::CalvinResolve { .. }
        | MetaOp::ApplyTransactionRedo { .. }
        | MetaOp::RestoreRedo(_)
        | MetaOp::PurgeVersionsBelow { .. }
        | MetaOp::KvDueExpiries { .. }
        | MetaOp::RefreshGraphCount { .. }
        | MetaOp::QueryStorageBytes { .. } => Footprint::Core,
    }
}

/// A cluster event read of one database's binds or streams is a task of
/// that database. A tenant mark or metadata wait spans databases.
pub(super) fn cluster_event_footprint(op: &ClusterEventOp) -> Footprint {
    match op {
        ClusterEventOp::ConsumeStream { database_id, .. }
        | ClusterEventOp::SurrogateBinds { database_id, .. }
        | ClusterEventOp::SurrogateHolders { database_id, .. } => Footprint::Database(*database_id),
        ClusterEventOp::TenantWriteMarks { .. } | ClusterEventOp::MetadataApplied { .. } => {
            Footprint::Core
        }
    }
}
