// SPDX-License-Identifier: BUSL-1.1

//! Exhaustive, compile-enforced routing oracle: `PhysicalPlan` → `PlanRouting`.
//!
//! The chokepoint the Calvin scheduler uses to decide whether a plan carried
//! in a `SequencedTxn` participates in this node's vshard. Exhaustive over
//! every `PhysicalPlan` and nested op variant — a new one is a compile error,
//! never a silent gap. Mirrors `required_permission`'s technique.

#![deny(clippy::wildcard_enum_match_arm)]

use nodedb_physical::physical_plan::{
    ArrayOp, ColumnarOp, CrdtOp, DocumentOp, GraphOp, KvOp, MetaOp, PhysicalPlan, TimeseriesOp,
    VectorOp,
};

use crate::types::{DatabaseId, RecordHomes, VShardId};
use nodedb_types::{CollectionKey, QualifiedCollection};

/// Where a `PhysicalPlan` routes for Calvin cross-shard scheduling purposes.
///
/// `Vshards` carries 1 (collection-homed) or 2 (dual-homed graph edge /
/// cross-collection) participants. `Unroutable` is a KNOWN, named gap — never
/// a silent empty vec — so a caller can log/abort with the precise reason
/// instead of a generic "non-routable" message.
pub(crate) enum PlanRouting {
    /// Collection- or key-homed write: participates in exactly these vshards.
    Vshards(Vec<VShardId>),
    /// This plan must never reach the Data Plane (handled entirely on the
    /// Control Plane, e.g. cluster-fanned-out array ops).
    ControlPlaneOnly,
    /// A read or DDL op that `is_write_plan` already excludes upstream. The
    /// arm exists purely for match exhaustiveness; seeing it inside a
    /// Calvin-scheduled write txn is itself a bug.
    NotAWrite,
    /// A write whose vshard cannot be determined from the plan alone. Named
    /// so the caller's error states WHY, not only that routing failed.
    Unroutable(&'static str),
}

/// Whether any read in `reads` homes to `vshard_id`. A read entry carries the
/// plan's database-qualified name. An entry that does not de-qualify homes
/// nowhere, so a txn whose only work is such a read fails loudly as one that
/// homes no local work.
pub(crate) fn homes_versioned_read(
    reads: &nodedb_types::calvin::VersionedReadSet,
    database_id: DatabaseId,
    vshard_id: u32,
) -> bool {
    reads
        .iter()
        .any(|entry| versioned_read_homes_on(entry, database_id, vshard_id))
}

/// Whether `vshard_id` validates `entry`: its `home_vshard` when set, else
/// its collection's vShard.
pub(crate) fn versioned_read_homes_on(
    entry: &nodedb_types::calvin::VersionedReadEntry,
    database_id: DatabaseId,
    vshard_id: u32,
) -> bool {
    match entry.home_vshard {
        Some(home) => home == vshard_id,
        None => CollectionKey::from_qualified_str(database_id, &entry.collection)
            .is_ok_and(|key| key.vshard().as_u32() == vshard_id),
    }
}

/// Route a collection-homed write to the vShard of its canonical key. The
/// plan carries the database-qualified name, de-qualified here.
fn collection_routing(database_id: DatabaseId, collection: &QualifiedCollection) -> PlanRouting {
    match CollectionKey::from_qualified(database_id, collection) {
        Ok(key) => PlanRouting::Vshards(vec![key.vshard()]),
        Err(_) => PlanRouting::Unroutable(
            "collection name lacks the qualifier of the transaction's database",
        ),
    }
}

#[cfg(test)]
fn collection_vshard(collection: &str) -> VShardId {
    CollectionKey::from_bare(DatabaseId::DEFAULT, collection).vshard()
}

/// Returns the routing decision for `plan`. Exhaustive over every
/// `PhysicalPlan` variant and every op nested inside it — adding a new
/// variant anywhere in this tree is a compile error, never a silent gap.
pub(crate) fn plan_vshard(plan: &PhysicalPlan) -> PlanRouting {
    plan_vshard_in_database(plan, DatabaseId::DEFAULT)
}

pub(crate) fn plan_vshard_in_database(plan: &PhysicalPlan, database_id: DatabaseId) -> PlanRouting {
    match plan {
        PhysicalPlan::Document(op) => document_routing(op, database_id),
        PhysicalPlan::Kv(op) => kv_routing(op, database_id),
        PhysicalPlan::Vector(op) => vector_routing(op, database_id),
        PhysicalPlan::Graph(op) => graph_routing(op),
        PhysicalPlan::Timeseries(op) => timeseries_routing(op, database_id),
        PhysicalPlan::Columnar(op) => columnar_routing(op, database_id),
        PhysicalPlan::Crdt(op) => crdt_routing(op, database_id),
        PhysicalPlan::Array(op) => array_routing(op),
        // Cluster-fanned-out array ops are handled entirely by the
        // Control-Plane `ArrayCoordinator` and never dispatched to the Data
        // Plane (see `data/executor/dispatch/visitor.rs`'s `unreachable!`).
        PhysicalPlan::ClusterArray(_) | PhysicalPlan::ClusterEvent(_) => {
            PlanRouting::ControlPlaneOnly
        }
        // Reads / query operators / metadata ops: `is_write_plan` already
        // excludes every variant of these four families upstream, except a
        // RESTORE batch.
        PhysicalPlan::Text(_) => PlanRouting::NotAWrite,
        PhysicalPlan::Spatial(_) => PlanRouting::NotAWrite,
        PhysicalPlan::Query(_) => PlanRouting::NotAWrite,
        // A RESTORE batch runs on the vShard it names.
        PhysicalPlan::Meta(MetaOp::RestoreRedo(batch)) => {
            PlanRouting::Vshards(vec![VShardId::new(batch.vshard)])
        }
        PhysicalPlan::Meta(_) => PlanRouting::NotAWrite,
    }
}

fn document_routing(op: &DocumentOp, database_id: DatabaseId) -> PlanRouting {
    match op {
        DocumentOp::PointPut { collection, .. }
        | DocumentOp::PointInsert { collection, .. }
        | DocumentOp::PointDelete { collection, .. }
        | DocumentOp::PointUpdate { collection, .. }
        | DocumentOp::BatchInsert { collection, .. }
        | DocumentOp::Upsert { collection, .. }
        | DocumentOp::BatchUpsert { collection, .. }
        | DocumentOp::BulkUpdate { collection, .. }
        | DocumentOp::BulkDelete { collection, .. }
        | DocumentOp::Truncate { collection, .. }
        // The balance write is homed on the TARGET collection it names, which
        // is the whole point of it being a task of its own: the source write it
        // was derived from homes elsewhere, and the pair is dual-homed by the
        // two tasks' own vshards rather than by one plan claiming both.
        | DocumentOp::ApplyBalanceDelta { collection, .. } => {
            collection_routing(database_id, collection)
        }
        // Never scheduled: the write-resolve orchestrator proposes it through
        // Raft directly, on the vshard of the collection it resolved.
        DocumentOp::ResolvedWrite { .. } => PlanRouting::Unroutable(
            "resolved governed write: proposed directly by the write-resolve orchestrator",
        ),
        DocumentOp::InsertSelect {
            target_collection, ..
        } => collection_routing(database_id, target_collection),
        // Both join the target with a DIFFERENT source collection; nothing on
        // the plan enforces the two live on the same vshard.
        DocumentOp::Merge { .. } | DocumentOp::UpdateFromJoin { .. } => PlanRouting::Unroutable(
            "cross-collection write: source/target co-location is not enforced",
        ),
        // Read-only: it reports what the wrapped write will do and mutates
        // nothing.
        DocumentOp::ResolveWrite(_)
        | DocumentOp::PointGet { .. }
        | DocumentOp::Scan { .. }
        | DocumentOp::RangeScan { .. }
        | DocumentOp::IndexLookup { .. }
        | DocumentOp::IndexedFetch { .. }
        | DocumentOp::EstimateCount { .. }
        | DocumentOp::MaterializeScan { .. }
        | DocumentOp::Register { .. }
        | DocumentOp::DropIndex { .. }
        | DocumentOp::BackfillIndex { .. } => PlanRouting::NotAWrite,
    }
}

fn kv_routing(op: &KvOp, database_id: DatabaseId) -> PlanRouting {
    match op {
        KvOp::Put { collection, .. }
        | KvOp::Insert { collection, .. }
        | KvOp::InsertIfAbsent { collection, .. }
        | KvOp::InsertOnConflictUpdate { collection, .. }
        | KvOp::Delete { collection, .. }
        | KvOp::BatchPut { collection, .. }
        | KvOp::Expire { collection, .. }
        | KvOp::Persist { collection, .. }
        | KvOp::FieldSet { collection, .. }
        | KvOp::Truncate { collection, .. }
        | KvOp::Incr { collection, .. }
        | KvOp::IncrFloat { collection, .. }
        | KvOp::Cas { collection, .. }
        | KvOp::GetSet { collection, .. }
        // Transfer moves value between two KEYS in the SAME collection field,
        // so it stays single-home unlike `TransferItem` below.
        | KvOp::Transfer { collection, .. }
        // Predicate DML touches only the collection it names, so it homes on
        // that collection's vshard like every other single-collection write.
        | KvOp::PredicateUpdate { collection, .. }
        | KvOp::PredicateDelete { collection, .. } => {
            collection_routing(database_id, collection)
        }
        // Source and dest are DIFFERENT collections; no co-location guarantee.
        KvOp::TransferItem { .. } => PlanRouting::Unroutable(
            "cross-collection write: source/target co-location is not enforced",
        ),
        // A resolved write carries per-mutation collections and can span two
        // (a resolved `TransferItem`), so the plan alone does not name one
        // home — same gap as `TransferItem` above.
        KvOp::ResolvedWrite { .. } => PlanRouting::Unroutable(
            "resolved KV write: mutations may span collections with no co-location guarantee",
        ),
        // Read-only: it reports what a write will do and mutates nothing.
        KvOp::ResolveWrite(_)
        | KvOp::Get { .. }
        | KvOp::Scan { .. }
        | KvOp::GetTtl { .. }
        | KvOp::BatchGet { .. }
        | KvOp::FieldGet { .. }
        | KvOp::MaterializeScan { .. }
        | KvOp::RegisterIndex { .. }
        | KvOp::DropIndex { .. }
        | KvOp::RegisterSortedIndex { .. }
        | KvOp::DropSortedIndex { .. }
        | KvOp::SortedIndexRank { .. }
        | KvOp::SortedIndexTopK { .. }
        | KvOp::SortedIndexRange { .. }
        | KvOp::SortedIndexCount { .. }
        | KvOp::SortedIndexScore { .. }
        | KvOp::SortedIndexTxnRead { .. } => PlanRouting::NotAWrite,
    }
}

fn vector_routing(op: &VectorOp, database_id: DatabaseId) -> PlanRouting {
    match op {
        VectorOp::Insert { collection, .. }
        | VectorOp::BatchInsert { collection, .. }
        | VectorOp::Delete { collection, .. }
        | VectorOp::DeleteBySurrogate { collection, .. }
        | VectorOp::SparseInsert { collection, .. }
        | VectorOp::SparseDelete { collection, .. }
        | VectorOp::MultiVectorInsert { collection, .. }
        | VectorOp::MultiVectorDelete { collection, .. }
        | VectorOp::DirectUpsert { collection, .. }
        | VectorOp::DirectInsert { collection, .. }
        | VectorOp::DirectInsertIfAbsent { collection, .. }
        | VectorOp::DirectBatchWrite { collection, .. }
        | VectorOp::DirectDelete { collection, .. }
        | VectorOp::DirectTruncate { collection, .. }
        | VectorOp::DirectUpdate { collection, .. } => collection_routing(database_id, collection),
        // Never scheduled: the write-resolve orchestrator proposes it through
        // Raft directly, on the vshard of the collection it resolved.
        VectorOp::ResolvedDirectWrite { .. } => PlanRouting::Unroutable(
            "resolved governed vector write: proposed directly by the write-resolve orchestrator",
        ),
        // Read-only: it reports what the wrapped write will do and mutates
        // nothing.
        VectorOp::ResolveDirectWrite(_)
        | VectorOp::Search { .. }
        | VectorOp::MultiSearch { .. }
        | VectorOp::SetParams { .. }
        | VectorOp::DropIndex { .. }
        | VectorOp::QueryStats { .. }
        | VectorOp::Seal { .. }
        | VectorOp::CompactIndex { .. }
        | VectorOp::Rebuild { .. }
        | VectorOp::SparseSearch { .. }
        | VectorOp::MultiVectorScoreSearch { .. } => PlanRouting::NotAWrite,
    }
}

fn graph_routing(op: &GraphOp) -> PlanRouting {
    match op {
        // Edge plans are key-homed (dual-homed across endpoints), not
        // collection-homed: route to from_key(src) ∪ from_key(dst).
        GraphOp::EdgePut { src_id, dst_id, .. } | GraphOp::EdgeDelete { src_id, dst_id, .. } => {
            PlanRouting::Vshards(RecordHomes::edge(src_id, dst_id).iter().collect())
        }
        // A batch is the union of its edges' homes, under the same key-homing
        // rule as the single-edge plans above.
        GraphOp::EdgePutBatch { edges } | GraphOp::EdgeDeleteBatch { edges } => {
            let mut vshards: Vec<VShardId> = Vec::new();
            for edge in edges {
                for vshard in RecordHomes::edge(&edge.src_id, &edge.dst_id).iter() {
                    if !vshards.contains(&vshard) {
                        vshards.push(vshard);
                    }
                }
            }
            if vshards.is_empty() {
                // An empty batch touches nothing; it is a no-op write, not a
                // routing gap. Naming it keeps the empty vec from ever again
                // meaning "unrecognized variant".
                PlanRouting::Unroutable("edge batch carries no edges")
            } else {
                PlanRouting::Vshards(vshards)
            }
        }
        // Node-label writes are key-homed on `node_id`, the same mechanism the
        // edge plans use for their endpoints.
        // A node delete's guard runs on the node's key home, which holds
        // every edge incident on the node.
        GraphOp::SetNodeLabels { node_id, .. }
        | GraphOp::RemoveNodeLabels { node_id, .. }
        | GraphOp::NodeEdgeGuard { node_id, .. } => {
            PlanRouting::Vshards(vec![VShardId::from_key(node_id.as_bytes())])
        }
        // A TRUNCATE's edge share and a CRDT delete's presence guard run on
        // the vShard they name.
        GraphOp::TruncateEdges { vshard, .. } | GraphOp::NodePresenceGuard { vshard, .. } => {
            PlanRouting::Vshards(vec![VShardId::new(*vshard)])
        }
        // Read-only: it decides the wrapped delete's policy and mutates nothing.
        GraphOp::ResolveEdgeDelete(_)
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
        | GraphOp::TemporalNeighbors { .. }
        | GraphOp::TemporalAlgorithm { .. }
        | GraphOp::Stats { .. }
        | GraphOp::NodePresenceRead { .. } => PlanRouting::NotAWrite,
    }
}

fn timeseries_routing(op: &TimeseriesOp, database_id: DatabaseId) -> PlanRouting {
    match op {
        TimeseriesOp::Ingest { collection, .. }
        | TimeseriesOp::Truncate { collection, .. }
        | TimeseriesOp::RefreshAggregate { collection, .. } => {
            collection_routing(database_id, collection)
        }
        // Read-only: it reports the lines the wrapped ingest will store and
        // mutates nothing. Partition maintenance changes no row.
        TimeseriesOp::ResolveIngest(_)
        | TimeseriesOp::Scan { .. }
        | TimeseriesOp::Partitions { .. }
        | TimeseriesOp::ReadAggregate { .. } => PlanRouting::NotAWrite,
    }
}

fn columnar_routing(op: &ColumnarOp, database_id: DatabaseId) -> PlanRouting {
    match op {
        ColumnarOp::Insert { collection, .. }
        | ColumnarOp::Update { collection, .. }
        | ColumnarOp::Delete { collection, .. }
        | ColumnarOp::ResolvedUpdate { collection, .. }
        | ColumnarOp::ResolvedDelete { collection, .. }
        | ColumnarOp::Truncate { collection, .. } => collection_routing(database_id, collection),
        ColumnarOp::Scan { .. }
        | ColumnarOp::MaterializeScan { .. }
        | ColumnarOp::ResolveDml { .. } => PlanRouting::NotAWrite,
    }
}

fn crdt_routing(op: &CrdtOp, database_id: DatabaseId) -> PlanRouting {
    match op {
        CrdtOp::Apply { collection, .. }
        | CrdtOp::ApplyAuthenticated { collection, .. }
        | CrdtOp::ListInsert { collection, .. }
        | CrdtOp::ListDelete { collection, .. }
        | CrdtOp::ListMove { collection, .. }
        | CrdtOp::DocUpsert { collection, .. }
        | CrdtOp::DocDelete { collection, .. }
        | CrdtOp::SetConstraints { collection, .. }
        | CrdtOp::DropConstraints { collection, .. }
        | CrdtOp::RestoreToVersion { collection, .. }
        | CrdtOp::ImportSnapshot { collection, .. } => collection_routing(database_id, collection),
        CrdtOp::Read { .. }
        | CrdtOp::PreviewApply { .. }
        | CrdtOp::ReadConstraints { .. }
        | CrdtOp::GetPolicy { .. }
        | CrdtOp::ReadAtVersion { .. }
        | CrdtOp::GetVersionVector { .. }
        | CrdtOp::ExportDelta { .. }
        | CrdtOp::SetPolicy { .. }
        | CrdtOp::CompactAtVersion { .. } => PlanRouting::NotAWrite,
    }
}

fn array_routing(op: &ArrayOp) -> PlanRouting {
    match op {
        // A cell write names the vShard its cells' tiles live on.
        ArrayOp::Put { vshard_id, .. } | ArrayOp::Delete { vshard_id, .. } => {
            PlanRouting::Vshards(vec![VShardId::new(*vshard_id)])
        }
        // A flush writes every tile of the array on a node, and a
        // transaction never stages one.
        ArrayOp::Flush { .. } => PlanRouting::Unroutable(
            "an array flush writes every tile of the array on a node, and runs outside a \
             transaction",
        ),
        ArrayOp::OpenArray { .. }
        | ArrayOp::Compact { .. }
        | ArrayOp::DropArray { .. }
        | ArrayOp::RekeyArray { .. }
        | ArrayOp::PurgeArrayDrop { .. }
        | ArrayOp::Slice { .. }
        | ArrayOp::Aggregate { .. }
        | ArrayOp::Elementwise { .. }
        | ArrayOp::SurrogateBitmapScan { .. } => PlanRouting::NotAWrite,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodedb_array::types::ArrayId;
    use nodedb_physical::physical_plan::{ClusterArrayOp, CrdtOp, GraphOp};
    use nodedb_types::{
        PayloadIndexKind, Surrogate, TenantId, VectorQuantization, VectorStorageDtype,
    };

    fn vshards_of(plan: &PhysicalPlan) -> Vec<u32> {
        match plan_vshard(plan) {
            PlanRouting::Vshards(v) => v.iter().map(|x| x.as_u32()).collect(),
            PlanRouting::ControlPlaneOnly | PlanRouting::NotAWrite | PlanRouting::Unroutable(_) => {
                panic!("expected Vshards routing")
            }
        }
    }

    /// Find two distinct string keys whose `from_key` vShards differ.
    fn two_distinct_key_vshards() -> (String, String, u32, u32) {
        let mut first: Option<(String, u32)> = None;
        for i in 0u32..2048 {
            let key = format!("node_{i}");
            let v = VShardId::from_key(key.as_bytes()).as_u32();
            if let Some((ref fkey, fv)) = first {
                if fv != v {
                    return (fkey.clone(), key, fv, v);
                }
            } else {
                first = Some((key, v));
            }
        }
        panic!("could not find two distinct-vshard keys in 2048 tries");
    }

    #[test]
    fn plan_vshard_routes_edge_to_both_endpoints() {
        let (src_id, dst_id, src_v, dst_v) = two_distinct_key_vshards();
        assert_ne!(src_v, dst_v);

        let plan = PhysicalPlan::Graph(GraphOp::EdgePut {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "follows"),
            src_id: src_id.clone(),
            label: "knows".to_owned(),
            dst_id: dst_id.clone(),
            properties: Vec::new(),
            src_surrogate: Surrogate::new(1),
            dst_surrogate: Surrogate::new(2),
        });

        let mut got = vshards_of(&plan);
        got.sort_unstable();
        let mut want = vec![src_v, dst_v];
        want.sort_unstable();
        assert_eq!(got, want, "edge plan routes to both from_key endpoints");
    }

    #[test]
    fn plan_vshard_routes_edge_batch_to_union_of_endpoints() {
        let (src_id, dst_id, src_v, dst_v) = two_distinct_key_vshards();
        assert_ne!(src_v, dst_v);

        let edge = |src: &str, dst: &str| nodedb_physical::physical_plan::BatchEdge {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "follows"),
            src_id: src.to_owned(),
            label: "knows".to_owned(),
            dst_id: dst.to_owned(),
            src_surrogate: Surrogate::new(1),
            dst_surrogate: Surrogate::new(2),
            write_gate: nodedb_types::RlsWriteCheck::NoPolicyApplies,
        };

        // Two edges over the same endpoint pair: the union must dedupe.
        let plan = PhysicalPlan::Graph(GraphOp::EdgePutBatch {
            edges: vec![edge(&src_id, &dst_id), edge(&src_id, &dst_id)],
        });

        let mut got = vshards_of(&plan);
        got.sort_unstable();
        let mut want = vec![src_v, dst_v];
        want.sort_unstable();
        assert_eq!(got, want, "edge batch routes to the union of its endpoints");
    }

    #[test]
    fn plan_vshard_routes_node_labels_by_node_key() {
        let (node_id, _, node_v, _) = two_distinct_key_vshards();

        for plan in [
            PhysicalPlan::Graph(GraphOp::SetNodeLabels {
                node_id: node_id.clone(),
                labels: vec!["Person".to_owned()],
            }),
            PhysicalPlan::Graph(GraphOp::RemoveNodeLabels {
                node_id: node_id.clone(),
                labels: vec!["Person".to_owned()],
            }),
        ] {
            assert_eq!(
                vshards_of(&plan),
                vec![node_v],
                "node-label write is key-homed on node_id"
            );
        }
    }

    #[test]
    fn plan_vshard_single_when_endpoints_collide() {
        // src == dst → a single deduped vShard.
        let key = "self".to_owned();
        let v = VShardId::from_key(key.as_bytes()).as_u32();
        let plan = PhysicalPlan::Graph(GraphOp::EdgePut {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "follows"),
            src_id: key.clone(),
            label: "knows".to_owned(),
            dst_id: key,
            properties: Vec::new(),
            src_surrogate: Surrogate::new(1),
            dst_surrogate: Surrogate::new(1),
        });
        assert_eq!(vshards_of(&plan), vec![v]);
    }

    #[test]
    fn plan_vshard_routes_crdt_list_ops_to_collection_vshard() {
        let list_ops = [
            (
                "ListInsert",
                PhysicalPlan::Crdt(CrdtOp::ListInsert {
                    collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
                    document_id: "id1".to_owned(),
                    list_path: "blocks".to_owned(),
                    index: 0,
                    fields_json: "{}".to_owned(),
                    surrogate: Surrogate::new(1),
                }),
            ),
            (
                "ListDelete",
                PhysicalPlan::Crdt(CrdtOp::ListDelete {
                    collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
                    document_id: "id1".to_owned(),
                    list_path: "blocks".to_owned(),
                    index: 0,
                    surrogate: Surrogate::new(1),
                }),
            ),
            (
                "ListMove",
                PhysicalPlan::Crdt(CrdtOp::ListMove {
                    collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
                    document_id: "id1".to_owned(),
                    list_path: "blocks".to_owned(),
                    from_index: 0,
                    to_index: 1,
                    surrogate: Surrogate::new(1),
                }),
            ),
        ];

        for (name, plan) in &list_ops {
            assert!(
                matches!(plan_vshard(plan), PlanRouting::Vshards(_)),
                "{name} must be routable"
            );
        }
    }

    #[test]
    fn document_truncate_routes_to_collection_vshard() {
        let plan = PhysicalPlan::Document(DocumentOp::Truncate {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            restart_identity: false,
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        });
        let want = collection_vshard("docs").as_u32();
        assert_eq!(vshards_of(&plan), vec![want]);
    }

    #[test]
    fn collection_routing_preserves_database_scope() {
        let db = DatabaseId::new(7);
        let collection = (0..2048)
            .map(|i| format!("db_scoped_{i}"))
            .find(|name| {
                CollectionKey::from_bare(DatabaseId::DEFAULT, name).vshard()
                    != CollectionKey::from_bare(db, name).vshard()
            })
            .expect("collection whose home differs by database");
        let plan = PhysicalPlan::Document(DocumentOp::Truncate {
            collection: QualifiedCollection::new(db, &collection),
            restart_identity: false,
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        });
        // The plan carries the qualified name; the home is the bare key's.
        let expected = CollectionKey::from_bare(db, &collection).vshard();
        match plan_vshard_in_database(&plan, db) {
            PlanRouting::Vshards(actual) => assert_eq!(actual, vec![expected]),
            PlanRouting::ControlPlaneOnly | PlanRouting::NotAWrite | PlanRouting::Unroutable(_) => {
                panic!("document truncate must be database-scoped")
            }
        }
    }

    #[test]
    fn columnar_update_and_delete_route_to_collection_vshard() {
        let want = collection_vshard("metrics").as_u32();

        let update = PhysicalPlan::Columnar(ColumnarOp::Update {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "metrics"),
            filters: Vec::new(),
            updates: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        });
        assert_eq!(vshards_of(&update), vec![want]);

        let delete = PhysicalPlan::Columnar(ColumnarOp::Delete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "metrics"),
            filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        });
        assert_eq!(vshards_of(&delete), vec![want]);
    }

    #[test]
    fn vector_direct_upsert_and_multi_vector_delete_route_to_collection_vshard() {
        let want = collection_vshard("vecs").as_u32();

        let direct_upsert = PhysicalPlan::Vector(VectorOp::DirectUpsert {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "vecs"),
            field: "emb".to_owned(),
            surrogate: Surrogate::new(3),
            pk_bytes: Vec::new(),
            vector: vec![0.5, 0.6],
            payload: vec![1, 2, 3],
            quantization: VectorQuantization::None,
            storage_dtype: VectorStorageDtype::F32,
            payload_indexes: vec![("tenant_id".to_owned(), PayloadIndexKind::Equality)],
            returning: None,
            rls_filters: Vec::new(),
            on_conflict_updates: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::decided_earlier_in_request(),
        });
        assert_eq!(vshards_of(&direct_upsert), vec![want]);

        let multi_vector_delete = PhysicalPlan::Vector(VectorOp::MultiVectorDelete {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "vecs"),
            field_name: "colbert".to_owned(),
            document_surrogate: Surrogate::new(2),
        });
        assert_eq!(vshards_of(&multi_vector_delete), vec![want]);
    }

    #[test]
    fn crdt_set_drop_constraints_and_restore_route_to_collection_vshard() {
        let want = collection_vshard("docs").as_u32();

        let set_constraints = PhysicalPlan::Crdt(CrdtOp::SetConstraints {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            constraint_version: 1,
            constraints: Vec::new(),
        });
        assert_eq!(vshards_of(&set_constraints), vec![want]);

        let drop_constraints = PhysicalPlan::Crdt(CrdtOp::DropConstraints {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            constraint_version: 1,
        });
        assert_eq!(vshards_of(&drop_constraints), vec![want]);

        let restore = PhysicalPlan::Crdt(CrdtOp::RestoreToVersion {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            document_id: "id1".to_owned(),
            target_version_json: "{}".to_owned(),
            surrogate: Surrogate::new(1),
        });
        assert_eq!(vshards_of(&restore), vec![want]);
    }

    #[test]
    fn cluster_array_routes_control_plane_only() {
        let plan = PhysicalPlan::ClusterArray(ClusterArrayOp::Put {
            array_id: ArrayId::new(TenantId::new(1), "genome"),
            array_id_msgpack: Vec::new(),
            cells: Vec::new(),
            wal_lsn: 0,
            prefix_bits: 8,
        });
        assert!(matches!(plan_vshard(&plan), PlanRouting::ControlPlaneOnly));
    }

    #[test]
    fn document_merge_is_unroutable() {
        let plan = PhysicalPlan::Document(DocumentOp::Merge {
            target_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            source: nodedb_physical::physical_plan::RowSource::whole_collection(
                QualifiedCollection::new(DatabaseId::DEFAULT, "staging"),
            ),
            source_alias: "s".to_owned(),
            target_join_col: "id".to_owned(),
            source_join_col: "id".to_owned(),
            clauses: Vec::new(),
            returning: None,
            resolved_inserts: None,
            resolved_insert_identities: Vec::new(),
            source_rows: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        });
        assert!(matches!(plan_vshard(&plan), PlanRouting::Unroutable(_)));
    }

    #[test]
    fn array_cell_writes_route_to_their_tile_vshard() {
        let put = PhysicalPlan::Array(ArrayOp::Put {
            array_id: ArrayId::new(TenantId::new(1), "genome"),
            cells_msgpack: Vec::new(),
            wal_lsn: 0,
            provenance: None,
            vshard_id: 77,
        });
        assert_eq!(vshards_of(&put), vec![77]);
        let delete = PhysicalPlan::Array(ArrayOp::Delete {
            array_id: ArrayId::new(TenantId::new(1), "genome"),
            coords_msgpack: Vec::new(),
            wal_lsn: 0,
            provenance: None,
            vshard_id: 12,
        });
        assert_eq!(vshards_of(&delete), vec![12]);
        let flush = PhysicalPlan::Array(ArrayOp::Flush {
            array_id: ArrayId::new(TenantId::new(1), "genome"),
            wal_lsn: 0,
        });
        assert!(matches!(plan_vshard(&flush), PlanRouting::Unroutable(_)));
    }

    #[test]
    fn document_update_from_join_is_unroutable() {
        let plan = PhysicalPlan::Document(DocumentOp::UpdateFromJoin {
            target_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "docs"),
            source: nodedb_physical::physical_plan::RowSource::whole_collection(
                QualifiedCollection::new(DatabaseId::DEFAULT, "staging"),
            ),
            source_alias: "s".to_owned(),
            target_join_col: "id".to_owned(),
            source_join_col: "id".to_owned(),
            updates: Vec::new(),
            target_filters: Vec::new(),
            returning: None,
            source_rows: None,
            rls_filters: Vec::new(),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            resolved_sum_targets: Vec::new(),
            declared_primary_key: None,
        });
        assert!(matches!(plan_vshard(&plan), PlanRouting::Unroutable(_)));
    }

    #[test]
    fn kv_transfer_item_is_unroutable() {
        let plan = PhysicalPlan::Kv(KvOp::TransferItem {
            source_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "inbox"),
            dest_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "archive"),
            item_key: vec![1, 2, 3],
            dest_key: vec![4, 5, 6],
            surrogate: Surrogate::new(1),
            source_rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
            dest_rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        });
        assert!(matches!(plan_vshard(&plan), PlanRouting::Unroutable(_)));
    }

    #[test]
    fn empty_edge_batch_is_unroutable_not_silently_empty() {
        let plan = PhysicalPlan::Graph(GraphOp::EdgePutBatch { edges: Vec::new() });
        assert!(
            matches!(plan_vshard(&plan), PlanRouting::Unroutable(_)),
            "an empty edge batch must be a named Unroutable, never a silent empty vshard list"
        );
    }
}
