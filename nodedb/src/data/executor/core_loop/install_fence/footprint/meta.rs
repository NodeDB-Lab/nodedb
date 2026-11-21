// SPDX-License-Identifier: BUSL-1.1

//! Footprints of the core's own ops and of cluster event reads.
//!
//! A meta op that drops, converts, purges or reindexes one named collection
//! keys that collection, and the graph of its tenant when it clears edges.
//! Every other meta op checkpoints, snapshots, purges, installs or stages
//! across collections, so it is a core task. A sampler, a checkpoint or a
//! version purge keeps no client order, so it waits for the install and
//! holds no later task. A cancel and a redo stream page read touch no state
//! an install writes.

use nodedb_physical::physical_plan::{ClusterEventOp, MetaOp};

use crate::types::{DatabaseId, TenantId};

use super::class::{Footprint, Scope};
use super::keys::{KeySet, write_of};

pub(super) fn meta_footprint(op: &MetaOp, scope: &Scope) -> Footprint {
    match op {
        MetaOp::Cancel { .. } | MetaOp::ReadRedoStreamPage { .. } => Footprint::Keys(Vec::new()),
        // A drop clears the collection's edges with its rows.
        MetaOp::UnregisterCollection {
            tenant_id, name, ..
        } => KeySet::new(scope)
            .requested(name)
            .graph_of(TenantId::new(*tenant_id))
            .write(),
        MetaOp::TemporalPurgeEdgeStore {
            tenant_id,
            collection,
            ..
        } => KeySet::new(scope)
            .collection(collection)
            .graph_of(TenantId::new(*tenant_id))
            .write(),
        // An array op names the array bare, in the request's database.
        MetaOp::AlterArray { array_id, .. } | MetaOp::TemporalPurgeArray { array_id, .. } => {
            KeySet::new(scope).requested(array_id).write()
        }
        // A refresh wait names the aggregate's root source collection, and
        // folds the cut before it answers.
        MetaOp::ConvertCollection { collection, .. }
        | MetaOp::RebuildIndex { collection, .. }
        | MetaOp::TemporalPurgeDocumentStrict { collection, .. }
        | MetaOp::TemporalPurgeColumnar { collection, .. }
        | MetaOp::TemporalPurgeCrdt { collection, .. }
        | MetaOp::EnforceTimeseriesRetention { collection, .. }
        | MetaOp::AwaitContinuousAggregateRefresh { collection, .. } => {
            write_of(collection, scope)
        }
        // A definition's source is a collection or another aggregate, and
        // only the core's aggregate graph names the root a chain reads. So
        // the registration is a task of the definition's whole database.
        MetaOp::RegisterContinuousAggregate { def } => {
            scope.whole_database_of(DatabaseId::new(def.database_id))
        }
        MetaOp::WalAppend { .. }
        | MetaOp::TransactionBatch { .. }
        | MetaOp::CreateSnapshot
        | MetaOp::Compact
        | MetaOp::InstallAggregateImage { .. }
        | MetaOp::UnregisterContinuousAggregate { .. }
        | MetaOp::CreateTenantSnapshot { .. }
        | MetaOp::RestoreTenantSnapshot { .. }
        | MetaOp::PurgeDepartedVShards { .. }
        | MetaOp::PurgeTenant { .. }
        | MetaOp::UnregisterMaterializedView { .. }
        | MetaOp::ApplyContinuousAggRetention { .. }
        | MetaOp::CalvinExecuteStatic { .. }
        | MetaOp::CalvinExecutePassive { .. }
        | MetaOp::CalvinExecuteActive { .. }
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
        //   Control-Plane pass, read gauges and due keys.
        // - a checkpoint publishes at least the state its stamps name, and a
        //   later write's effects are noted applied before any stamp names
        //   them.
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
/// that whole database. A tenant mark or metadata wait spans databases.
pub(super) fn cluster_event_footprint(op: &ClusterEventOp, scope: &Scope) -> Footprint {
    match op {
        ClusterEventOp::ConsumeStream { database_id, .. }
        | ClusterEventOp::SurrogateBinds { database_id, .. }
        | ClusterEventOp::SurrogateHolders { database_id, .. } => {
            scope.whole_database_of(*database_id)
        }
        ClusterEventOp::TenantWriteMarks { .. } | ClusterEventOp::MetadataApplied { .. } => {
            Footprint::Core
        }
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::PhysicalPlan;

    use super::super::keys::tests::{TENANT, coll, graph, key, scope, write};
    use super::super::plan::plan_footprint;
    use super::*;
    use crate::types::RequestId;

    fn class(op: MetaOp) -> Footprint {
        plan_footprint(&PhysicalPlan::Meta(op), &scope())
    }

    #[test]
    fn a_collection_drop_keys_its_collection_and_its_tenants_graph() {
        let drop = MetaOp::UnregisterCollection {
            tenant_id: TENANT.as_u64(),
            name: "a".into(),
            purge_lsn: 1,
            reclaim_l1_files: false,
        };
        assert_eq!(class(drop), write(vec![key("a"), graph()]));
    }

    #[test]
    fn a_retention_pass_keys_its_collection() {
        let retention = MetaOp::EnforceTimeseriesRetention {
            collection: coll("ts"),
            max_age_ms: 1,
            decided_ms: 1,
            aggregates: Vec::new(),
        };
        assert_eq!(class(retention), write(vec![key("ts")]));
    }

    #[test]
    fn an_array_purge_keys_the_array_in_the_requests_database() {
        let purge = MetaOp::TemporalPurgeArray {
            tenant_id: 0,
            array_id: "grid".into(),
            cutoff_system_ms: 1,
        };
        assert_eq!(class(purge), write(vec![key("grid")]));
    }

    #[test]
    fn meta_ops_are_core_except_cancel_stream_pages_and_background_passes() {
        assert_eq!(class(MetaOp::Compact), Footprint::Core);
        assert_eq!(class(MetaOp::Checkpoint), Footprint::Background);
        let cancel = MetaOp::Cancel {
            target_request_id: RequestId::new(1),
        };
        assert_eq!(class(cancel), Footprint::Keys(Vec::new()));
    }

    #[test]
    fn a_metadata_wait_is_core() {
        let wait = PhysicalPlan::ClusterEvent(ClusterEventOp::MetadataApplied { index: 1 });
        assert_eq!(plan_footprint(&wait, &scope()), Footprint::Core);
    }
}
