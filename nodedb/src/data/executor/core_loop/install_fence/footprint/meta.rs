// SPDX-License-Identifier: BUSL-1.1

//! Footprints of the core's own ops and of cluster event reads.
//!
//! A meta op that drops, converts, purges or reindexes one named collection
//! keys that collection, and the graph of its tenant when it clears edges.
//! A read of a named collection's size, hash chain or last values keys that
//! collection. An aggregate op keys the aggregate and the root collection
//! of its chain, which the core's aggregate graph names. A transaction's
//! staging, resolve, savepoint and overlay drop key its overlay (see
//! `txn`). Every other meta op checkpoints, snapshots, purges, installs or
//! lists across collections, so it is a core task. A sampler, a checkpoint
//! or a version purge keeps no client order, so it waits for the install
//! and holds no later task. A cancel and a redo stream page read touch no
//! state an install writes.

use nodedb_physical::physical_plan::{ClusterEventOp, MetaOp};

use crate::types::{DatabaseId, TenantId};

use super::class::{Footprint, Scope};
use super::keys::{KeySet, read_of, write_of};
use super::txn::{drop_overlay_footprint, resolve_footprint, stage_write_footprint};

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
        // A definition's source is a collection or another aggregate. The
        // core's aggregate graph names the root a chain reads.
        MetaOp::RegisterContinuousAggregate { def } => {
            let db = DatabaseId::new(def.database_id);
            let root = scope
                .registry
                .aggregate_source_root(db, scope.tenant, &def.source);
            aggregate_write(root, db, &def.name, scope)
        }
        MetaOp::UnregisterContinuousAggregate { name } => {
            let root = scope.registry.aggregate_root(scope.db, scope.tenant, name);
            aggregate_write(root, scope.db, name, scope)
        }
        // A watermark read reads the aggregate's state, which its root's
        // flushes fold into.
        MetaOp::QueryAggregateWatermark { aggregate_name } => {
            match scope
                .registry
                .aggregate_root(scope.db, scope.tenant, aggregate_name)
            {
                Some(root) => KeySet::new(scope)
                    .stored(&root)
                    .named_in(scope.db, aggregate_name)
                    .read(),
                None => scope.whole_database(),
            }
        }
        MetaOp::QueryCollectionSize { name, .. } => KeySet::new(scope).requested(name).read(),
        MetaOp::VerifyHashChain { collection }
        | MetaOp::QueryLastValues { collection, .. }
        | MetaOp::QueryLastValue { collection, .. } => read_of(collection, scope),
        MetaOp::StageWrite { plan } => stage_write_footprint(plan, scope),
        MetaOp::ResolveTxnPart { txn_id, plans, .. } => resolve_footprint(*txn_id, plans, scope),
        MetaOp::DropTxnOverlay { txn_id } => drop_overlay_footprint(*txn_id, scope),
        MetaOp::MarkSavepoint { txn_id, .. } | MetaOp::RollbackToSavepoint { txn_id, .. } => {
            KeySet::new(scope).txn_overlay(*txn_id).read()
        }
        MetaOp::WalAppend { .. }
        | MetaOp::TransactionBatch { .. }
        | MetaOp::CreateSnapshot
        | MetaOp::Compact
        | MetaOp::InstallAggregateImage { .. }
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
        | MetaOp::CalvinDrop { .. }
        | MetaOp::CalvinResolve { .. }
        | MetaOp::ApplyTransactionRedo { .. }
        | MetaOp::RestoreRedo(_)
        // A client's read of a tenant's collections or aggregates keeps its
        // order against the client's later writes. The envelope names no
        // connection, so each holds every later task.
        | MetaOp::ListTenantCollections { .. }
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

/// A write of the aggregate `name` of database `db` and of `root`, the
/// root collection its chain reads. A chain the core cannot resolve is a
/// task of the whole database `db`.
fn aggregate_write(root: Option<String>, db: DatabaseId, name: &str, scope: &Scope) -> Footprint {
    match root {
        Some(root) => KeySet::new(scope).stored(&root).named_in(db, name).write(),
        None => scope.whole_database_of(db),
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

    use super::super::keys::tests::{TENANT, coll, graph, key, scope, whole, write};
    use super::super::plan::plan_footprint;
    use super::super::registry::tests::{FakeRegistry, NO_REGISTRY};
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
    fn a_named_meta_read_keys_its_collection() {
        let size = MetaOp::QueryCollectionSize {
            tenant_id: TENANT.as_u64(),
            name: "b".into(),
        };
        assert_eq!(class(size), Footprint::Keys(vec![key("b")]));
        let chain = MetaOp::VerifyHashChain {
            collection: coll("ledger"),
        };
        assert_eq!(class(chain), Footprint::Keys(vec![key("ledger")]));
        let last = MetaOp::QueryLastValue {
            collection: coll("ts"),
            series_id: 1,
            system_time: nodedb_types::SystemTimeScope::Current,
        };
        assert_eq!(class(last), Footprint::Keys(vec![key("ts")]));
    }

    /// A registry whose aggregate `ts_1m` is rooted at `ts`.
    const AGGREGATES: FakeRegistry = FakeRegistry {
        aggregate: Some(("ts_1m", "ts")),
        ..NO_REGISTRY
    };

    fn with_aggregates(op: MetaOp) -> Footprint {
        let scope = Scope {
            registry: &AGGREGATES,
            ..scope()
        };
        plan_footprint(&PhysicalPlan::Meta(op), &scope)
    }

    #[test]
    fn an_aggregate_op_keys_the_aggregate_and_its_root() {
        let drop = MetaOp::UnregisterContinuousAggregate {
            name: "ts_1m".into(),
        };
        assert_eq!(with_aggregates(drop), write(vec![key("ts"), key("ts_1m")]));
        let watermark = MetaOp::QueryAggregateWatermark {
            aggregate_name: "ts_1m".into(),
        };
        assert_eq!(
            with_aggregates(watermark),
            Footprint::Keys(vec![key("ts"), key("ts_1m")])
        );
    }

    #[test]
    fn an_aggregate_the_core_does_not_register_is_its_whole_database() {
        let drop = MetaOp::UnregisterContinuousAggregate {
            name: "gone".into(),
        };
        assert_eq!(with_aggregates(drop), whole());
    }

    /// A core resolves a registration through its source chain.
    #[test]
    fn a_core_resolves_an_aggregate_of_an_aggregate_to_its_root() {
        let (mut core, _tx, _rx, _dir) = crate::data::executor::core_loop::tests::make_core();
        let def = |name: &str, source: &str| {
            crate::engine::timeseries::continuous_agg::test_support::exact_def(name, source, "1m")
        };
        core.continuous_agg_mgr
            .register(TENANT.as_u64(), def("ts_1m", "ts"));
        let scope = Scope {
            registry: &core,
            ..scope()
        };
        let register = MetaOp::RegisterContinuousAggregate {
            def: def("ts_1h", "ts_1m"),
        };
        assert_eq!(
            plan_footprint(&PhysicalPlan::Meta(register), &scope),
            write(vec![key("ts"), key("ts_1h")])
        );
    }

    #[test]
    fn a_metadata_wait_is_core() {
        let wait = PhysicalPlan::ClusterEvent(ClusterEventOp::MetadataApplied { index: 1 });
        assert_eq!(plan_footprint(&wait, &scope()), Footprint::Core);
    }
}
