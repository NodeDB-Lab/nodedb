// SPDX-License-Identifier: BUSL-1.1

//! Footprints of the core's own ops and of cluster event reads.
//!
//! A meta op that drops, converts, purges or reindexes one named collection
//! keys that collection, and the graph of its tenant when it clears edges.
//! A read of a named collection's size, hash chain or last values keys that
//! collection. An aggregate op keys the aggregate and the root collection
//! of its chain, which the core's aggregate graph names. A transaction's
//! staging, resolve, savepoint and overlay drop key its overlay (see
//! `txn`). A list of a tenant's collections or aggregates is an unkeyed
//! read: a client's holds its own stream, internal work holds the core.
//! A committed redo that holds its content inline keys what its install
//! writes and reads (see `redo`). A Calvin slice and a stream marker are
//! core tasks. Every other meta op checkpoints, snapshots, purges or
//! installs across collections, so it is a core task. A sampler, a checkpoint
//! or a version purge keeps no client order, so it waits for the install
//! and holds no later task. A cancel and a redo stream page read touch no
//! state an install writes.

use nodedb_physical::physical_plan::{ClusterEventOp, MetaOp};

use crate::types::{DatabaseId, TenantId};

use super::class::{Footprint, Scope};
use super::keys::{KeySet, read_of, write_of};
use super::redo::{RedoPlan, redo_footprint};
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
        | MetaOp::AwaitContinuousAggregateRefresh { collection, .. } => write_of(collection, scope),
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
        // A committed Calvin slice also ends its staged state.
        | MetaOp::ApplyTransactionRedo { calvin: Some(_), .. }
        | MetaOp::RestoreRedo(_) => Footprint::Core,
        MetaOp::ApplyTransactionRedo {
            redo,
            collections,
            sum_targets,
            calvin: None,
            reads,
            ..
        } => redo_footprint(
            &RedoPlan {
                redo,
                collections,
                sum_targets,
                reads,
            },
            scope,
        ),
        // A read of a tenant's collections or aggregates keeps its order
        // against later tasks of its own stream. A client's read holds its
        // own connection's later tasks. Internal work holds every later task.
        MetaOp::ListTenantCollections { .. }
        | MetaOp::ListContinuousAggregates
        | MetaOp::ContinuousAggregatesRegistered => scope.unkeyed_read(),
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
/// that whole database. A tenant mark or metadata wait spans databases, so
/// it is an unkeyed read of its stream.
pub(super) fn cluster_event_footprint(op: &ClusterEventOp, scope: &Scope) -> Footprint {
    match op {
        ClusterEventOp::ConsumeStream { database_id, .. }
        | ClusterEventOp::SurrogateBinds { database_id, .. }
        | ClusterEventOp::SurrogateHolders { database_id, .. } => {
            scope.whole_database_of(*database_id)
        }
        ClusterEventOp::TenantWriteMarks { .. } | ClusterEventOp::MetadataApplied { .. } => {
            scope.unkeyed_read()
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

    mod redo {
        use std::collections::BTreeSet;

        use nodedb_physical::physical_plan::{
            CalvinInstall, CalvinReplySpec, MetaOp, RedoOrigin, RedoSumTargets, ResolvedSumTarget,
        };
        use nodedb_types::Surrogate;
        use nodedb_types::calvin::{EngineTag, ReadKeyIdent, VersionedReadEntry};
        use nodedb_types::sync::wire::SyncProvenance;
        use nodedb_wal::record::RecordType;

        use super::super::super::class::Footprint;
        use super::super::super::keys::tests::{VSHARD, graph, key};
        use super::class;
        use crate::data::executor::core_loop::install_fence::FenceKey;
        use crate::data::executor::handlers::transaction::redo_apply::test_commit::doc_put_sub_record;
        use crate::wal::{RedoRecord, RedoStreamId, RedoStreamRef, RedoSubRecord};

        /// What a committed redo plan names beside its record.
        #[derive(Default)]
        struct Named {
            sum_targets: Vec<RedoSumTargets>,
            reads: Vec<VersionedReadEntry>,
            calvin: Option<CalvinInstall>,
        }

        fn record(ops: Vec<RedoSubRecord>) -> RedoRecord {
            RedoRecord {
                version: 1,
                ops,
                calvin_stamp: None,
                cross_shard_applied: None,
                row_sources: Vec::new(),
                publishes: Vec::new(),
                row_changes: Vec::new(),
                user_roles: Vec::new(),
                sum_targets: Vec::new(),
                pins: Default::default(),
            }
        }

        fn doc_put(collection: &str) -> RedoSubRecord {
            let body = nodedb_types::json_to_msgpack(&serde_json::json!({"n": 1})).expect("body");
            doc_put_sub_record(collection, "d1", &body, 1)
        }

        /// The install of the encoded `redo` into the stored `collections`,
        /// with what `named` names.
        fn plan(redo: Vec<u8>, collections: &[&str], named: Named) -> MetaOp {
            MetaOp::ApplyTransactionRedo {
                redo,
                collections: collections.iter().map(|name| name.to_string()).collect(),
                sum_targets: named.sum_targets,
                origin: RedoOrigin::Commit,
                calvin: named.calvin,
                reads: named.reads,
                snapshot_ordinal: None,
                pins: Default::default(),
            }
        }

        /// The install of `redo` into the stored `collections`.
        fn apply(redo: &RedoRecord, collections: &[&str]) -> MetaOp {
            let bytes = redo.to_bytes().expect("encode redo");
            plan(bytes, collections, Named::default())
        }

        /// The install of one document put into `a`, with what `named`
        /// names.
        fn put_into_a(named: Named) -> MetaOp {
            let bytes = record(vec![doc_put("a")]).to_bytes().expect("encode redo");
            plan(bytes, &["a"], named)
        }

        /// The keys a write footprint holds, on the request's vShard.
        fn written(footprint: Footprint) -> BTreeSet<FenceKey> {
            match footprint {
                Footprint::Write { keys, vshard } => {
                    assert_eq!(vshard, VSHARD, "the write carries the request's vShard");
                    keys.into_iter().collect()
                }
                other @ (Footprint::Keys(_)
                | Footprint::WholeDatabase { .. }
                | Footprint::Core
                | Footprint::Ordered
                | Footprint::Background) => panic!("a write footprint, got {other:?}"),
            }
        }

        fn read_of(collection: &str) -> VersionedReadEntry {
            VersionedReadEntry {
                engine: EngineTag::Document,
                collection: collection.into(),
                key: ReadKeyIdent::Predicate,
                home_vshard: None,
            }
        }

        #[test]
        fn a_whole_redo_keys_its_collections_sum_targets_and_reads() {
            let op = put_into_a(Named {
                sum_targets: vec![RedoSumTargets {
                    collection: "a".into(),
                    resolved: vec![ResolvedSumTarget::new("totals", "k", Surrogate::new(5))],
                }],
                reads: vec![read_of("r")],
                calvin: None,
            });
            assert_eq!(
                written(class(op)),
                BTreeSet::from([key("a"), key("totals"), key("r")])
            );
        }

        #[test]
        fn a_whole_redo_keys_the_graph_and_sync_stream_its_ops_write() {
            let edge = RedoSubRecord {
                record_type: RecordType::Put as u32,
                payload: zerompk::to_msgpack_vec(&crate::wal::EdgePutRedo {
                    collection: "knows".into(),
                    src_id: "a".into(),
                    label: "KNOWS".into(),
                    dst_id: "b".into(),
                    properties: Vec::new(),
                    src_surrogate: 1,
                    dst_surrogate: 2,
                    system_from: Some(5),
                    applied: None,
                    valid_from_ms: None,
                    valid_until_ms: None,
                })
                .expect("encode edge put"),
            };
            let provenance = SyncProvenance {
                producer_id: 7,
                epoch: 1,
                stream_id: 42,
                seq: 1,
            };
            let frame = RedoSubRecord {
                record_type: RecordType::VectorPut as u32,
                payload: crate::control::server::wal_dispatch::encode_vector_put_payload(
                    "v",
                    &[1.0, 2.0],
                    2,
                    "",
                    Surrogate::new(9),
                    Some(&provenance),
                )
                .expect("encode vector put"),
            };
            let op = apply(&record(vec![edge, frame]), &["knows", "v"]);
            assert_eq!(
                written(class(op)),
                BTreeSet::from([
                    key("knows"),
                    key("v"),
                    graph(),
                    FenceKey::SyncStream {
                        producer_id: 7,
                        stream_id: 42,
                    },
                ])
            );
        }

        #[test]
        fn a_calvin_slice_is_core() {
            let op = put_into_a(Named {
                calvin: Some(CalvinInstall {
                    epoch: 1,
                    position: 0,
                    epoch_system_ms: 0,
                    reply: CalvinReplySpec::Count(Vec::new()),
                    user_write: true,
                    mirror: false,
                }),
                ..Named::default()
            });
            assert_eq!(class(op), Footprint::Core);
        }

        #[test]
        fn a_stream_marker_is_core() {
            let reference = RedoStreamRef {
                stream: RedoStreamId::Session {
                    vshard: 1,
                    idempotency_key: 2,
                },
                len: 0,
                chunks: Vec::new(),
                publish_position: None,
                entry: None,
                stamp_floor: None,
            };
            let marker = reference.marker(None, None, Vec::new()).expect("marker");
            assert_eq!(class(apply(&marker, &["a"])), Footprint::Core);
        }

        #[test]
        fn an_undecodable_redo_is_core() {
            let op = plan(vec![0xff, 0x00, 0x13], &["a"], Named::default());
            assert_eq!(class(op), Footprint::Core);
        }

        #[test]
        fn a_malformed_collection_name_is_core() {
            let op = apply(&record(vec![doc_put("a")]), &["x/y/z"]);
            assert_eq!(class(op), Footprint::Core);
        }

        /// A read that names no collection reads its whole vShard.
        #[test]
        fn a_read_of_no_collection_is_core() {
            let op = put_into_a(Named {
                reads: vec![read_of("")],
                ..Named::default()
            });
            assert_eq!(class(op), Footprint::Core);
        }
    }

    #[test]
    fn an_internal_metadata_wait_is_core() {
        let wait = PhysicalPlan::ClusterEvent(ClusterEventOp::MetadataApplied { index: 1 });
        assert_eq!(plan_footprint(&wait, &scope()), Footprint::Core);
    }

    /// A client's unkeyed read orders against its own stream alone.
    #[test]
    fn a_client_unkeyed_read_is_ordered() {
        let connection =
            crate::control::server::shared::session::ConnectionId::new(1).expect("non-zero id");
        let client = Scope {
            stream: crate::bridge::envelope::RequestStream::client(1, connection),
            ..scope()
        };
        let reads = [
            PhysicalPlan::Meta(MetaOp::ListTenantCollections { tenant_id: 1 }),
            PhysicalPlan::Meta(MetaOp::ListContinuousAggregates),
            PhysicalPlan::Meta(MetaOp::ContinuousAggregatesRegistered),
            PhysicalPlan::ClusterEvent(ClusterEventOp::MetadataApplied { index: 1 }),
        ];
        for read in &reads {
            assert_eq!(plan_footprint(read, &client), Footprint::Ordered);
            assert_eq!(plan_footprint(read, &scope()), Footprint::Core);
        }
        assert_eq!(
            plan_footprint(&PhysicalPlan::Meta(MetaOp::Compact), &client),
            Footprint::Core,
            "a client's core op stays core"
        );
    }
}
