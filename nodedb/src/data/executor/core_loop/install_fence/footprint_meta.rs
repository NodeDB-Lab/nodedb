// SPDX-License-Identifier: BUSL-1.1

//! Footprints of the core's own ops and of cluster event reads.
//!
//! A meta op checkpoints, snapshots, purges, installs or stages across
//! collections, so each one is a core task. A sampler, a checkpoint or a
//! version purge keeps no client order, so it waits for the install and
//! holds no later task. A cancel and a redo stream page read touch no state
//! an install writes.

use nodedb_physical::physical_plan::{ClusterEventOp, MetaOp};

use super::footprint::Footprint;

pub(super) fn meta_footprint(op: &MetaOp) -> Footprint {
    match op {
        MetaOp::Cancel { .. } | MetaOp::ReadRedoStreamPage { .. } => Footprint::Keys(Vec::new()),
        MetaOp::WalAppend { .. }
        | MetaOp::TransactionBatch { .. }
        | MetaOp::CreateSnapshot
        | MetaOp::Compact
        | MetaOp::RegisterContinuousAggregate { .. }
        | MetaOp::InstallAggregateImage { .. }
        | MetaOp::UnregisterContinuousAggregate { .. }
        | MetaOp::AwaitContinuousAggregateRefresh { .. }
        | MetaOp::ConvertCollection { .. }
        | MetaOp::CreateTenantSnapshot { .. }
        | MetaOp::RestoreTenantSnapshot { .. }
        | MetaOp::PurgeDepartedVShards { .. }
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
        | MetaOp::ApplyContinuousAggRetention { .. }
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
        // A client's read of catalog sizes, hash chains, aggregates or last
        // values keeps its order against the client's later writes. The
        // envelope names no connection, so each holds every later task.
        | MetaOp::ListTenantCollections { .. }
        | MetaOp::QueryCollectionSize { .. }
        | MetaOp::VerifyHashChain { .. }
        | MetaOp::QueryAggregateWatermark { .. }
        | MetaOp::QueryLastValues { .. }
        | MetaOp::QueryLastValue { .. }
        | MetaOp::ListContinuousAggregates
        | MetaOp::ContinuousAggregatesRegistered => Footprint::Core,
        // Work with no client order to keep, each correct after any later
        // write:
        // - the storage, graph and expiry samplers, each sent only by its
        //   Control-Plane pass, read gauges and due keys;
        // - a checkpoint publishes at least the state its stamps name, and a
        //   later write's effects are noted applied before any stamp names
        //   them;
        // - a purge below a horizon removes only versions ended below it,
        //   and a later write ends a version at an ordinal above it.
        MetaOp::KvDueExpiries { .. }
        | MetaOp::RefreshGraphCount { .. }
        | MetaOp::QueryStorageBytes { .. }
        | MetaOp::Checkpoint
        | MetaOp::PurgeVersionsBelow { .. } => Footprint::Background,
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
