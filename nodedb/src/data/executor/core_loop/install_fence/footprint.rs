// SPDX-License-Identifier: BUSL-1.1

//! The state a task can touch, as the install fence sees it.
//!
//! Only a read of named collections gets `Keys`: a write gets the database
//! it writes, and so does every op this module cannot bound to named
//! collections. A misfiled write breaks the fence, so every match here is
//! exhaustive. A task of a database also names the vShard its events
//! carry, the task's own: its events advance that vShard's partition.

use nodedb_array::types::ArrayId;
use nodedb_physical::physical_plan::{
    ArrayOp, ClusterArrayOp, ColumnarOp, CrdtOp, DocumentOp, GraphOp, KvOp, PhysicalPlan, QueryOp,
    SpatialOp, TextOp, TimeseriesOp, VectorOp,
};
use nodedb_types::QualifiedCollection;

use crate::data::executor::task::ExecutionTask;
use crate::types::{DatabaseId, VShardId};

use super::footprint_meta::{cluster_event_footprint, meta_footprint};
use super::state::FenceKey;

/// The state a task can touch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::data::executor) enum Footprint {
    /// Reads of these keys only. Empty: the task touches no fenced state.
    Keys(Vec<FenceKey>),
    /// Any state of this database.
    Database(DatabaseId),
    /// Any state of database `db`, by a task whose events carry `vshard`.
    Write { db: DatabaseId, vshard: VShardId },
    /// Any state of the core.
    Core,
    /// Core work with no client order to keep, such as a sampler or a
    /// checkpoint. It waits while a fence is armed, so it never sees an
    /// install part way, and holds no later task.
    Background,
}

impl Footprint {
    /// The footprint of a task that does both.
    fn join(self, other: Footprint) -> Footprint {
        match (self, other) {
            (Footprint::Keys(mut a), Footprint::Keys(b)) => {
                a.extend(b);
                Footprint::Keys(a)
            }
            (Footprint::Keys(_), Footprint::Database(db))
            | (Footprint::Database(db), Footprint::Keys(_)) => Footprint::Database(db),
            (Footprint::Database(a), Footprint::Database(b)) if a == b => Footprint::Database(a),
            // A plan's parts never name a vShard: only `footprint` adds it.
            (Footprint::Database(_), Footprint::Database(_))
            | (Footprint::Write { .. }, _)
            | (_, Footprint::Write { .. })
            | (Footprint::Background, _)
            | (_, Footprint::Background)
            | (Footprint::Core, _)
            | (_, Footprint::Core) => Footprint::Core,
        }
    }
}

/// The footprint of `task`.
pub(in crate::data::executor) fn footprint(task: &ExecutionTask) -> Footprint {
    match plan_footprint(task.plan(), task.request.database_id) {
        Footprint::Database(db) => Footprint::Write {
            db,
            vshard: task.request.vshard_id,
        },
        footprint @ (Footprint::Keys(_)
        | Footprint::Write { .. }
        | Footprint::Core
        | Footprint::Background) => footprint,
    }
}

/// The footprint of `plan`, run for database `db`.
pub(super) fn plan_footprint(plan: &PhysicalPlan, db: DatabaseId) -> Footprint {
    match plan {
        PhysicalPlan::Document(op) => document_footprint(op, db),
        PhysicalPlan::Kv(op) => kv_footprint(op, db),
        PhysicalPlan::Vector(op) => vector_footprint(op, db),
        PhysicalPlan::Text(op) => text_footprint(op, db),
        PhysicalPlan::Columnar(op) => columnar_footprint(op, db),
        PhysicalPlan::Timeseries(op) => timeseries_footprint(op, db),
        PhysicalPlan::Spatial(op) => spatial_footprint(op, db),
        PhysicalPlan::Query(op) => query_footprint(op, db),
        PhysicalPlan::Graph(op) => graph_footprint(op, db),
        PhysicalPlan::Crdt(op) => crdt_footprint(op, db),
        PhysicalPlan::Array(op) => array_footprint(op),
        PhysicalPlan::ClusterArray(op) => cluster_array_footprint(op),
        PhysicalPlan::Meta(op) => meta_footprint(op),
        PhysicalPlan::ClusterEvent(op) => cluster_event_footprint(op),
    }
}

/// A read of `collection` alone, keyed by the database its stored name
/// names. A name that names no database is a task of `db`, and files a
/// report.
fn read_of(collection: &QualifiedCollection, db: DatabaseId) -> Footprint {
    match FenceKey::of_stored_collection(collection.as_str()) {
        Ok(key) => Footprint::Keys(vec![key]),
        Err(error) => {
            crate::diag::stored_collection_name_malformed(
                "fence_footprint: the read waits as a task of its database",
                collection.as_str(),
                &error,
            );
            Footprint::Database(db)
        }
    }
}

fn document_footprint(op: &DocumentOp, db: DatabaseId) -> Footprint {
    match op {
        DocumentOp::PointGet { collection, .. }
        | DocumentOp::Scan { collection, .. }
        | DocumentOp::RangeScan { collection, .. }
        | DocumentOp::IndexLookup { collection, .. }
        | DocumentOp::IndexedFetch { collection, .. }
        | DocumentOp::EstimateCount { collection, .. }
        | DocumentOp::MaterializeScan { collection, .. } => read_of(collection, db),
        DocumentOp::PointPut { .. }
        | DocumentOp::PointInsert { .. }
        | DocumentOp::PointDelete { .. }
        | DocumentOp::PointUpdate { .. }
        | DocumentOp::BatchInsert { .. }
        | DocumentOp::Register { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::BackfillIndex { .. }
        | DocumentOp::Truncate { .. }
        | DocumentOp::InsertSelect { .. }
        | DocumentOp::Upsert { .. }
        | DocumentOp::BatchUpsert { .. }
        | DocumentOp::UpdateFromJoin { .. }
        | DocumentOp::BulkUpdate { .. }
        | DocumentOp::BulkDelete { .. }
        | DocumentOp::Merge { .. }
        | DocumentOp::ApplyBalanceDelta { .. }
        | DocumentOp::ResolveWrite(_)
        | DocumentOp::ResolvedWrite { .. } => Footprint::Database(db),
    }
}

fn kv_footprint(op: &KvOp, db: DatabaseId) -> Footprint {
    match op {
        KvOp::Get { collection, .. }
        | KvOp::Scan { collection, .. }
        | KvOp::GetTtl { collection, .. }
        | KvOp::BatchGet { collection, .. }
        | KvOp::FieldGet { collection, .. }
        | KvOp::SortedIndexTxnRead { collection, .. }
        | KvOp::MaterializeScan { collection, .. } => read_of(collection, db),
        // A sorted-index read names an index, not the collection it reads.
        KvOp::SortedIndexRank { .. }
        | KvOp::SortedIndexTopK { .. }
        | KvOp::SortedIndexRange { .. }
        | KvOp::SortedIndexCount { .. }
        | KvOp::SortedIndexScore { .. }
        | KvOp::Put { .. }
        | KvOp::Insert { .. }
        | KvOp::InsertIfAbsent { .. }
        | KvOp::InsertOnConflictUpdate { .. }
        | KvOp::Delete { .. }
        | KvOp::Expire { .. }
        | KvOp::Persist { .. }
        | KvOp::BatchPut { .. }
        | KvOp::RegisterIndex { .. }
        | KvOp::DropIndex { .. }
        | KvOp::FieldSet { .. }
        | KvOp::Truncate { .. }
        | KvOp::Incr { .. }
        | KvOp::IncrFloat { .. }
        | KvOp::Cas { .. }
        | KvOp::GetSet { .. }
        | KvOp::Transfer { .. }
        | KvOp::TransferItem { .. }
        | KvOp::RegisterSortedIndex { .. }
        | KvOp::DropSortedIndex { .. }
        | KvOp::ResolveWrite(_)
        | KvOp::ResolvedWrite { .. }
        | KvOp::PredicateUpdate { .. }
        | KvOp::PredicateDelete { .. } => Footprint::Database(db),
    }
}

fn vector_footprint(op: &VectorOp, db: DatabaseId) -> Footprint {
    match op {
        // An inline prefilter plan runs on this core as part of the search.
        VectorOp::Search {
            collection,
            inline_prefilter_plan,
            ..
        } => match inline_prefilter_plan {
            Some(prefilter) => read_of(collection, db).join(plan_footprint(prefilter, db)),
            None => read_of(collection, db),
        },
        VectorOp::MultiSearch { collection, .. }
        | VectorOp::SparseSearch { collection, .. }
        | VectorOp::MultiVectorScoreSearch { collection, .. }
        | VectorOp::QueryStats { collection, .. } => read_of(collection, db),
        VectorOp::Insert { .. }
        | VectorOp::BatchInsert { .. }
        | VectorOp::Delete { .. }
        | VectorOp::DeleteBySurrogate { .. }
        | VectorOp::SetParams { .. }
        | VectorOp::DropIndex { .. }
        | VectorOp::Seal { .. }
        | VectorOp::CompactIndex { .. }
        | VectorOp::Rebuild { .. }
        | VectorOp::SparseInsert { .. }
        | VectorOp::SparseDelete { .. }
        | VectorOp::MultiVectorInsert { .. }
        | VectorOp::MultiVectorDelete { .. }
        | VectorOp::DirectUpsert { .. }
        | VectorOp::DirectInsert { .. }
        | VectorOp::DirectInsertIfAbsent { .. }
        | VectorOp::DirectBatchWrite { .. }
        | VectorOp::DirectDelete { .. }
        | VectorOp::DirectTruncate { .. }
        | VectorOp::DirectUpdate { .. }
        | VectorOp::ResolveDirectWrite(_)
        | VectorOp::ResolvedDirectWrite { .. } => Footprint::Database(db),
    }
}

fn text_footprint(op: &TextOp, db: DatabaseId) -> Footprint {
    match op {
        TextOp::Search { collection, .. }
        | TextOp::BM25ScoreScan { collection, .. }
        | TextOp::PhraseSearch { collection, .. }
        | TextOp::HybridSearch { collection, .. } => read_of(collection, db),
        // The triple search also walks the graph.
        TextOp::HybridSearchTriple { .. }
        | TextOp::FtsIndexDoc { .. }
        | TextOp::FtsDeleteDoc { .. }
        | TextOp::SetTextConfig { .. }
        | TextOp::RebuildIndex { .. } => Footprint::Database(db),
    }
}

fn columnar_footprint(op: &ColumnarOp, db: DatabaseId) -> Footprint {
    match op {
        ColumnarOp::Scan { collection, .. } | ColumnarOp::MaterializeScan { collection, .. } => {
            read_of(collection, db)
        }
        ColumnarOp::Insert { .. }
        | ColumnarOp::Update { .. }
        | ColumnarOp::Delete { .. }
        | ColumnarOp::ResolvedUpdate { .. }
        | ColumnarOp::ResolvedDelete { .. }
        | ColumnarOp::ResolveDml { .. }
        | ColumnarOp::Truncate { .. } => Footprint::Database(db),
    }
}

fn timeseries_footprint(op: &TimeseriesOp, db: DatabaseId) -> Footprint {
    match op {
        TimeseriesOp::Scan { collection, .. } => read_of(collection, db),
        // An aggregate read folds the aggregate's backlog before it answers.
        TimeseriesOp::ReadAggregate { .. }
        | TimeseriesOp::Ingest { .. }
        | TimeseriesOp::ResolveIngest(_)
        | TimeseriesOp::Truncate { .. }
        | TimeseriesOp::RefreshAggregate { .. }
        | TimeseriesOp::Partitions { .. } => Footprint::Database(db),
    }
}

fn spatial_footprint(op: &SpatialOp, db: DatabaseId) -> Footprint {
    match op {
        SpatialOp::Scan { collection, .. } => read_of(collection, db),
        SpatialOp::Insert { .. } | SpatialOp::Delete { .. } => Footprint::Database(db),
    }
}

fn query_footprint(op: &QueryOp, db: DatabaseId) -> Footprint {
    match op {
        QueryOp::Aggregate {
            collection, input, ..
        } => match input {
            Some(input) => read_of(collection, db).join(plan_footprint(input, db)),
            None => read_of(collection, db),
        },
        QueryOp::PartialAggregate { collection, .. } | QueryOp::FacetCounts { collection, .. } => {
            read_of(collection, db)
        }
        // Post-processing runs over its input alone.
        QueryOp::PostProcess { input, .. } => plan_footprint(input, db),
        QueryOp::Exchange(_)
        | QueryOp::ProviderScan { .. }
        | QueryOp::SetOp { .. }
        | QueryOp::PartialAggregateState { .. }
        | QueryOp::HashJoin { .. }
        | QueryOp::ShuffleJoinConsume { .. }
        | QueryOp::ShuffleAggregateConsume { .. }
        | QueryOp::NestedLoopJoin { .. }
        | QueryOp::SortMergeJoin { .. }
        | QueryOp::RecursiveScan { .. }
        | QueryOp::RecursiveValue { .. }
        | QueryOp::LateralTopK { .. }
        | QueryOp::LateralLoop { .. } => Footprint::Database(db),
    }
}

/// A graph op reads or writes the edge store and node labels of its tenant.
fn graph_footprint(op: &GraphOp, db: DatabaseId) -> Footprint {
    match op {
        GraphOp::EdgePut { .. }
        | GraphOp::EdgePutBatch { .. }
        | GraphOp::EdgeDelete { .. }
        | GraphOp::ResolveEdgeDelete(_)
        | GraphOp::EdgeDeleteBatch { .. }
        | GraphOp::Hop { .. }
        | GraphOp::Neighbors { .. }
        | GraphOp::NeighborsMulti { .. }
        | GraphOp::Path { .. }
        | GraphOp::Subgraph { .. }
        | GraphOp::RagFusion { .. }
        | GraphOp::Algo { .. }
        | GraphOp::Match { .. }
        | GraphOp::MatchContinuation { .. }
        | GraphOp::MatchVarLenResume { .. }
        | GraphOp::BspSuperstep(_)
        | GraphOp::WccSuperstep(_)
        | GraphOp::SetNodeLabels { .. }
        | GraphOp::RemoveNodeLabels { .. }
        | GraphOp::TemporalNeighbors { .. }
        | GraphOp::Stats { .. }
        | GraphOp::NodeEdgeGuard { .. }
        | GraphOp::NodePresenceGuard { .. }
        | GraphOp::TruncateEdges { .. }
        | GraphOp::NodePresenceRead { .. } => Footprint::Database(db),
    }
}

/// A CRDT read serves from a Loro document a redo install can write.
fn crdt_footprint(op: &CrdtOp, db: DatabaseId) -> Footprint {
    match op {
        CrdtOp::Read { .. }
        | CrdtOp::Apply { .. }
        | CrdtOp::ApplyAuthenticated { .. }
        | CrdtOp::ImportSnapshot { .. }
        | CrdtOp::SetConstraints { .. }
        | CrdtOp::DropConstraints { .. }
        | CrdtOp::ReadConstraints { .. }
        | CrdtOp::SetPolicy { .. }
        | CrdtOp::GetPolicy { .. }
        | CrdtOp::ReadAtVersion { .. }
        | CrdtOp::GetVersionVector { .. }
        | CrdtOp::ExportDelta { .. }
        | CrdtOp::RestoreToVersion { .. }
        | CrdtOp::CompactAtVersion { .. }
        | CrdtOp::ListInsert { .. }
        | CrdtOp::ListDelete { .. }
        | CrdtOp::ListMove { .. }
        | CrdtOp::DocUpsert { .. }
        | CrdtOp::DocDelete { .. }
        | CrdtOp::PreviewApply { .. } => Footprint::Database(db),
    }
}

/// An array op is a task of its array's database. An array read can read
/// tiles a redo install flushes.
fn array_footprint(op: &ArrayOp) -> Footprint {
    match op {
        ArrayOp::OpenArray { array_id, .. }
        | ArrayOp::Put { array_id, .. }
        | ArrayOp::Delete { array_id, .. }
        | ArrayOp::Slice { array_id, .. }
        | ArrayOp::Aggregate { array_id, .. }
        | ArrayOp::Flush { array_id, .. }
        | ArrayOp::Compact { array_id, .. }
        | ArrayOp::SurrogateBitmapScan { array_id, .. }
        | ArrayOp::DropArray { array_id }
        | ArrayOp::PurgeArrayDrop { array_id } => Footprint::Database(array_id.database_id),
        ArrayOp::RekeyArray { array_id, target } => arrays_footprint(array_id, target),
        ArrayOp::Elementwise { left, right, .. } => arrays_footprint(left, right),
    }
}

fn cluster_array_footprint(op: &ClusterArrayOp) -> Footprint {
    match op {
        ClusterArrayOp::Slice { array_id, .. }
        | ClusterArrayOp::Agg { array_id, .. }
        | ClusterArrayOp::Put { array_id, .. }
        | ClusterArrayOp::Delete { array_id, .. }
        | ClusterArrayOp::Flush { array_id, .. }
        | ClusterArrayOp::Compact { array_id, .. } => Footprint::Database(array_id.database_id),
        ClusterArrayOp::Elementwise { left, right, .. } => arrays_footprint(left, right),
    }
}

/// An op over two arrays touches the databases of both.
fn arrays_footprint(left: &ArrayId, right: &ArrayId) -> Footprint {
    Footprint::Database(left.database_id).join(Footprint::Database(right.database_id))
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{ClusterEventOp, MetaOp};

    use super::*;
    use crate::types::{RequestId, TenantId};

    fn coll(name: &str) -> QualifiedCollection {
        QualifiedCollection::new(DatabaseId::DEFAULT, name)
    }

    fn class(plan: PhysicalPlan) -> Footprint {
        plan_footprint(&plan, DatabaseId::DEFAULT)
    }

    fn read(name: &str) -> Footprint {
        read_of(&coll(name), DatabaseId::DEFAULT)
    }

    fn db() -> Footprint {
        Footprint::Database(DatabaseId::DEFAULT)
    }

    fn point_get(name: &str) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::PointGet {
            collection: coll(name),
            document_id: "d".into(),
            surrogate: None,
            pk_bytes: Vec::new(),
            rls_filters: Vec::new(),
            system_time: nodedb_types::SystemTimeScope::Current,
            valid_at_ms: None,
        })
    }

    fn array_id() -> nodedb_array::types::ArrayId {
        nodedb_array::types::ArrayId::new(TenantId::new(1), "arr")
    }

    #[test]
    fn document_reads_are_keys_and_writes_are_their_database() {
        assert_eq!(class(point_get("a")), read("a"));
        let put = PhysicalPlan::Document(DocumentOp::PointPut {
            collection: coll("a"),
            document_id: "d".into(),
            value: Vec::new(),
            surrogate: nodedb_types::Surrogate::new(1),
            pk_bytes: Vec::new(),
            returning: None,
            rls_filters: Vec::new(),
            resolved_sum_targets: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        });
        assert_eq!(class(put), db());
    }

    #[test]
    fn a_kv_get_is_a_read_of_its_collection() {
        let get = PhysicalPlan::Kv(KvOp::Get {
            collection: coll("k"),
            key: Vec::new(),
            rls_filters: Vec::new(),
            surrogate_ceiling: None,
            system_time: nodedb_types::SystemTimeScope::Current,
        });
        assert_eq!(class(get), read("k"));
    }

    #[test]
    fn a_vector_stats_read_is_a_read_of_its_collection() {
        let stats = PhysicalPlan::Vector(VectorOp::QueryStats {
            collection: coll("v"),
            field_name: "emb".into(),
        });
        assert_eq!(class(stats), read("v"));
    }

    #[test]
    fn a_text_config_change_is_its_database() {
        let config = PhysicalPlan::Text(TextOp::SetTextConfig {
            collection: coll("t"),
            analyzer_name: None,
            fuzzy_default: None,
        });
        assert_eq!(class(config), db());
    }

    #[test]
    fn a_columnar_page_read_is_a_read_of_its_collection() {
        let page = PhysicalPlan::Columnar(ColumnarOp::MaterializeScan {
            collection: coll("c"),
            cursor: Vec::new(),
            count: 1,
            system_as_of_ms: None,
        });
        assert_eq!(class(page), read("c"));
    }

    #[test]
    fn a_timeseries_truncate_is_its_database() {
        let truncate = PhysicalPlan::Timeseries(TimeseriesOp::Truncate {
            collection: coll("ts"),
            restart_identity: false,
        });
        assert_eq!(class(truncate), db());
    }

    #[test]
    fn a_spatial_delete_is_its_database() {
        let delete = PhysicalPlan::Spatial(SpatialOp::Delete {
            collection: coll("s"),
            field: "geom".into(),
            surrogate: None,
            provenance: None,
        });
        assert_eq!(class(delete), db());
    }

    #[test]
    fn post_processing_takes_the_footprint_of_its_input() {
        let post = PhysicalPlan::Query(QueryOp::PostProcess {
            input: Box::new(point_get("a")),
            filters: Vec::new(),
            projection: Vec::new(),
            computed_columns: Vec::new(),
            window_functions: Vec::new(),
            sort_keys: Vec::new(),
            limit: None,
            offset: 0,
            distinct: false,
        });
        assert_eq!(class(post), read("a"));
    }

    #[test]
    fn a_graph_read_is_its_database() {
        let stats = PhysicalPlan::Graph(GraphOp::Stats {
            collection: None,
            as_of: None,
        });
        assert_eq!(class(stats), db());
    }

    #[test]
    fn a_crdt_read_is_its_database() {
        let policy = PhysicalPlan::Crdt(CrdtOp::GetPolicy {
            collection: coll("cr"),
        });
        assert_eq!(class(policy), db());
    }

    #[test]
    fn array_ops_are_their_database() {
        let drop = PhysicalPlan::Array(ArrayOp::DropArray {
            array_id: array_id(),
        });
        assert_eq!(class(drop), db());
        let flush = PhysicalPlan::ClusterArray(ClusterArrayOp::Flush {
            array_id: array_id(),
            prefix_bits: 0,
        });
        assert_eq!(class(flush), db());
    }

    #[test]
    fn meta_ops_are_core_except_cancel_stream_pages_and_background_passes() {
        assert_eq!(class(PhysicalPlan::Meta(MetaOp::Compact)), Footprint::Core);
        assert_eq!(
            class(PhysicalPlan::Meta(MetaOp::Checkpoint)),
            Footprint::Background
        );
        let cancel = PhysicalPlan::Meta(MetaOp::Cancel {
            target_request_id: RequestId::new(1),
        });
        assert_eq!(class(cancel), Footprint::Keys(Vec::new()));
    }

    #[test]
    fn a_read_keyed_from_its_name_meets_a_fence_armed_from_install_collections() {
        let other = DatabaseId::new(7);
        let stored = QualifiedCollection::new(other, "users");
        // The install's collections carry the stored names it writes.
        let installed = vec![stored.as_str().to_string()];
        let keys = FenceKey::of_stored_collections(&installed).expect("stored names");
        let fence = super::super::InstallFence::new(keys.into_iter().collect());

        // A request of the default database still keys the read by the
        // database its collection names.
        let read = read_of(&stored, DatabaseId::DEFAULT);
        assert_eq!(
            read,
            Footprint::Keys(vec![FenceKey::Collection {
                db: other,
                name: stored.as_str().into(),
            }])
        );
        assert!(!fence.admits(&read));
        assert!(fence.admits(&read_of(&coll("users"), DatabaseId::DEFAULT)));
    }

    #[test]
    fn an_array_op_is_a_task_of_its_arrays_database() {
        let other =
            nodedb_array::types::ArrayId::in_database(TenantId::new(1), DatabaseId::new(7), "arr");
        let drop = PhysicalPlan::Array(ArrayOp::DropArray { array_id: other });
        assert_eq!(class(drop), Footprint::Database(DatabaseId::new(7)));
    }

    #[test]
    fn a_metadata_wait_is_core() {
        let wait = PhysicalPlan::ClusterEvent(ClusterEventOp::MetadataApplied { index: 1 });
        assert_eq!(class(wait), Footprint::Core);
    }

    #[test]
    fn a_join_takes_the_wider_class_and_two_databases_are_core() {
        let joined = read("a").join(Footprint::Database(DatabaseId::new(7)));
        assert_eq!(joined, Footprint::Database(DatabaseId::new(7)));
        let joined = db().join(Footprint::Database(DatabaseId::new(7)));
        assert_eq!(joined, Footprint::Core);
    }
}
