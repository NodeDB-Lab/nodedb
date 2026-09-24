use crate::bridge::envelope::PhysicalPlan;
use crate::control::gateway::version_set::touched_collections;
use crate::control::security::identity::{Permission, required_permission};

use super::AuthorizationRequirement;
use super::order::requirement_order;
use super::query::collect_query_requirements;

/// Return every authorization requirement for `plan`. General plan permission
/// applies to single-resource plans; multi-resource sources require `Read`, targets `Write`.
pub fn plan_requirements(plan: &PhysicalPlan) -> Vec<AuthorizationRequirement> {
    let mut requirements = Vec::new();
    collect_requirements(plan, &mut requirements);
    requirements.sort_by(requirement_order);
    requirements.dedup();
    requirements
}

fn collect_requirements(plan: &PhysicalPlan, out: &mut Vec<AuthorizationRequirement>) {
    use nodedb_physical::physical_plan::{CrdtOp, DocumentOp, GraphOp, KvOp, MetaOp, SpatialOp};

    let mut pending = vec![plan];
    while let Some(plan) = pending.pop() {
        let initial_len = out.len();
        match plan {
            PhysicalPlan::Document(DocumentOp::InsertSelect {
                target_collection,
                source_collection,
                ..
            })
            | PhysicalPlan::Document(DocumentOp::UpdateFromJoin {
                target_collection,
                source_collection,
                ..
            })
            | PhysicalPlan::Document(DocumentOp::Merge {
                target_collection,
                source_collection,
                ..
            }) => {
                out.push(AuthorizationRequirement::collection(
                    target_collection.as_str(),
                    Permission::Write,
                ));
                out.push(AuthorizationRequirement::collection(
                    source_collection.as_str(),
                    Permission::Read,
                ));
            }
            PhysicalPlan::Kv(KvOp::TransferItem {
                source_collection,
                dest_collection,
                ..
            }) => {
                out.push(AuthorizationRequirement::collection(
                    source_collection.as_str(),
                    Permission::Read,
                ));
                out.push(AuthorizationRequirement::collection(
                    dest_collection.as_str(),
                    Permission::Write,
                ));
            }
            PhysicalPlan::Query(_) | PhysicalPlan::Vector(_) => {
                if !collect_query_requirements(plan, &mut pending, out) {
                    add_general_requirements(plan, out);
                }
            }
            PhysicalPlan::Spatial(SpatialOp::Insert { collection, .. })
            | PhysicalPlan::Spatial(SpatialOp::Delete { collection, .. })
            | PhysicalPlan::Crdt(
                CrdtOp::ImportSnapshot { collection, .. }
                | CrdtOp::SetConstraints { collection, .. }
                | CrdtOp::DropConstraints { collection, .. }
                | CrdtOp::ReadConstraints { collection, .. }
                | CrdtOp::PreviewApply { collection, .. }
                | CrdtOp::GetVersionVector { collection, .. }
                | CrdtOp::ExportDelta { collection, .. }
                | CrdtOp::CompactAtVersion { collection, .. },
            ) => add_collection_requirement(collection.as_str(), required_permission(plan), out),
            PhysicalPlan::Graph(GraphOp::EdgePut { collection, .. })
            | PhysicalPlan::Graph(GraphOp::EdgeDelete { collection, .. }) => {
                add_collection_requirement(collection.as_str(), required_permission(plan), out);
            }
            PhysicalPlan::Graph(GraphOp::EdgePutBatch { edges })
            | PhysicalPlan::Graph(GraphOp::EdgeDeleteBatch { edges }) => {
                let permission = required_permission(plan);
                for edge in edges {
                    add_collection_requirement(edge.collection.as_str(), permission, out);
                }
            }
            PhysicalPlan::Meta(
                MetaOp::ConvertCollection { collection, .. }
                | MetaOp::EnforceTimeseriesRetention { collection, .. }
                | MetaOp::TemporalPurgeEdgeStore { collection, .. }
                | MetaOp::TemporalPurgeDocumentStrict { collection, .. }
                | MetaOp::TemporalPurgeColumnar { collection, .. }
                | MetaOp::TemporalPurgeCrdt { collection, .. }
                | MetaOp::QueryLastValues { collection }
                | MetaOp::QueryLastValue { collection, .. }
                | MetaOp::RebuildIndex { collection, .. },
            ) => add_collection_requirement(collection.as_str(), required_permission(plan), out),
            PhysicalPlan::Meta(
                MetaOp::UnregisterCollection { name, .. }
                | MetaOp::UnregisterMaterializedView { name, .. }
                | MetaOp::QueryCollectionSize { name, .. },
            ) => add_collection_requirement(name, required_permission(plan), out),
            PhysicalPlan::Meta(MetaOp::RenameCollection {
                old_collection,
                new_collection,
                ..
            }) => {
                let permission = required_permission(plan);
                add_collection_requirement(old_collection.as_str(), permission, out);
                add_collection_requirement(new_collection.as_str(), permission, out);
            }
            PhysicalPlan::Meta(MetaOp::TransactionBatch { plans, .. })
            | PhysicalPlan::Meta(MetaOp::ResolveTxn { plans, .. })
            | PhysicalPlan::Meta(MetaOp::RecordCalvinWriteVersions { plans, .. })
            | PhysicalPlan::Meta(MetaOp::CalvinExecuteStatic { plans, .. })
            | PhysicalPlan::Meta(MetaOp::CalvinExecuteActive { plans, .. }) => {
                for nested in plans {
                    pending.push(nested);
                }
            }
            PhysicalPlan::Meta(MetaOp::StageWrite { plan: nested }) => {
                pending.push(nested);
            }
            PhysicalPlan::Kv(KvOp::Transfer { collection, .. }) => {
                out.push(AuthorizationRequirement::collection(
                    collection.as_str(),
                    Permission::Write,
                ));
            }
            // Read-only probe over exactly the rows the wrapped write reads.
            PhysicalPlan::Document(DocumentOp::ResolveWrite(inner)) => match inner.as_ref() {
                DocumentOp::UpdateFromJoin {
                    target_collection,
                    source_collection,
                    ..
                }
                | DocumentOp::Merge {
                    target_collection,
                    source_collection,
                    ..
                } => {
                    add_collection_requirement(target_collection.as_str(), Permission::Read, out);
                    add_collection_requirement(source_collection.as_str(), Permission::Read, out);
                }
                // Point/bulk writes read exactly the collection they'd write.
                DocumentOp::PointUpdate { collection, .. }
                | DocumentOp::PointDelete { collection, .. }
                | DocumentOp::Upsert { collection, .. }
                | DocumentOp::BulkUpdate { collection, .. }
                | DocumentOp::BulkDelete { collection, .. } => {
                    add_collection_requirement(collection.as_str(), Permission::Read, out);
                }
                // No other op is ever wrapped; an empty set falls back to a tenant-wide
                // check below — fail-closed, not a bypass. Enumerated so a new op is a compile error.
                DocumentOp::PointGet { .. }
                | DocumentOp::PointPut { .. }
                | DocumentOp::PointInsert { .. }
                | DocumentOp::Scan { .. }
                | DocumentOp::BatchInsert { .. }
                | DocumentOp::RangeScan { .. }
                | DocumentOp::Register { .. }
                | DocumentOp::IndexLookup { .. }
                | DocumentOp::IndexedFetch { .. }
                | DocumentOp::DropIndex { .. }
                | DocumentOp::BackfillIndex { .. }
                | DocumentOp::Truncate { .. }
                | DocumentOp::EstimateCount { .. }
                | DocumentOp::InsertSelect { .. }
                | DocumentOp::MaterializeScan { .. }
                | DocumentOp::ApplyBalanceDelta { .. }
                | DocumentOp::ResolveWrite(_)
                | DocumentOp::ResolvedWrite { .. } => {}
            },
            // Per-mutation: `PhysicalPlan::collection` reports none of the rows touched.
            PhysicalPlan::Document(DocumentOp::ResolvedWrite { mutations, .. }) => {
                for mutation in mutations {
                    add_collection_requirement(mutation.collection().as_str(), Permission::Write, out);
                }
            }
            // Read-only probe over exactly the rows the wrapped write reads.
            PhysicalPlan::Kv(KvOp::ResolveWrite(inner)) => {
                for collection in kv_op_collections(inner) {
                    add_collection_requirement(collection, Permission::Read, out);
                }
            }
            // Per-mutation: a resolved `TransferItem` spans two collections,
            // neither reported by `KvOp::collection`.
            PhysicalPlan::Kv(KvOp::ResolvedWrite { mutations, .. }) => {
                for mutation in mutations {
                    add_collection_requirement(mutation.collection().as_str(), Permission::Write, out);
                }
            }
            PhysicalPlan::Kv(
                KvOp::Get { .. }
                | KvOp::Put { .. }
                | KvOp::Insert { .. }
                | KvOp::InsertIfAbsent { .. }
                | KvOp::InsertOnConflictUpdate { .. }
                | KvOp::Delete { .. }
                | KvOp::Scan { .. }
                | KvOp::Expire { .. }
                | KvOp::Persist { .. }
                | KvOp::GetTtl { .. }
                | KvOp::BatchGet { .. }
                | KvOp::BatchPut { .. }
                | KvOp::RegisterIndex { .. }
                | KvOp::DropIndex { .. }
                | KvOp::FieldGet { .. }
                | KvOp::FieldSet { .. }
                | KvOp::Truncate { .. }
                | KvOp::Incr { .. }
                | KvOp::IncrFloat { .. }
                | KvOp::Cas { .. }
                | KvOp::GetSet { .. }
                | KvOp::RegisterSortedIndex { .. }
                | KvOp::DropSortedIndex { .. }
                | KvOp::SortedIndexRank { .. }
                | KvOp::SortedIndexTopK { .. }
                | KvOp::SortedIndexRange { .. }
                | KvOp::SortedIndexCount { .. }
                | KvOp::SortedIndexScore { .. }
                | KvOp::SortedIndexTxnRead { .. }
                | KvOp::MaterializeScan { .. }
                // One collection each — the general extractor reads `KvOp::collection`.
                | KvOp::PredicateUpdate { .. }
                | KvOp::PredicateDelete { .. },
            ) => add_general_requirements(plan, out),
            PhysicalPlan::Document(_)
            | PhysicalPlan::Graph(_)
            | PhysicalPlan::Text(_)
            | PhysicalPlan::Columnar(_)
            | PhysicalPlan::Timeseries(_)
            | PhysicalPlan::Spatial(_)
            | PhysicalPlan::Crdt(_)
            | PhysicalPlan::Meta(_)
            | PhysicalPlan::Array(_)
            | PhysicalPlan::ClusterArray(_)
            | PhysicalPlan::ClusterEvent(_) => add_general_requirements(plan, out),
        }

        // A wrapper delegates its resource boundary to nested plans — its own missing
        // collection must not add a tenant-wide requirement when a descendant names one.
        if out.len() == initial_len && requires_tenant_fallback(plan) {
            out.push(AuthorizationRequirement::tenant(required_permission(plan)));
        }
    }
}

fn requires_tenant_fallback(plan: &PhysicalPlan) -> bool {
    use nodedb_physical::physical_plan::{MetaOp, QueryOp};

    match plan {
        PhysicalPlan::Query(QueryOp::Exchange(_))
        | PhysicalPlan::Meta(MetaOp::StageWrite { .. }) => false,
        PhysicalPlan::Meta(
            MetaOp::TransactionBatch { plans, .. }
            | MetaOp::ResolveTxn { plans, .. }
            | MetaOp::RecordCalvinWriteVersions { plans, .. }
            | MetaOp::CalvinExecuteStatic { plans, .. }
            | MetaOp::CalvinExecuteActive { plans, .. },
        ) => plans.is_empty(),
        PhysicalPlan::Vector(_)
        | PhysicalPlan::Graph(_)
        | PhysicalPlan::Document(_)
        | PhysicalPlan::Kv(_)
        | PhysicalPlan::Text(_)
        | PhysicalPlan::Columnar(_)
        | PhysicalPlan::Timeseries(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Crdt(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::Array(_)
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => true,
    }
}

/// Every collection a KV op names. `TransferItem` names two, `ResolvedWrite`
/// one per mutation; enumerated, not wildcarded, so a new op forces a decision here.
fn kv_op_collections(op: &nodedb_physical::physical_plan::KvOp) -> Vec<&str> {
    use nodedb_physical::physical_plan::KvOp;
    match op {
        KvOp::TransferItem {
            source_collection,
            dest_collection,
            ..
        } => vec![source_collection.as_str(), dest_collection.as_str()],
        KvOp::ResolvedWrite { mutations, .. } => {
            mutations.iter().map(|m| m.collection().as_str()).collect()
        }
        KvOp::ResolveWrite(inner) => kv_op_collections(inner),
        other @ (KvOp::Get { .. }
        | KvOp::Put { .. }
        | KvOp::Insert { .. }
        | KvOp::InsertIfAbsent { .. }
        | KvOp::InsertOnConflictUpdate { .. }
        | KvOp::Delete { .. }
        | KvOp::Scan { .. }
        | KvOp::Expire { .. }
        | KvOp::Persist { .. }
        | KvOp::GetTtl { .. }
        | KvOp::BatchGet { .. }
        | KvOp::BatchPut { .. }
        | KvOp::RegisterIndex { .. }
        | KvOp::DropIndex { .. }
        | KvOp::FieldGet { .. }
        | KvOp::FieldSet { .. }
        | KvOp::Truncate { .. }
        | KvOp::Incr { .. }
        | KvOp::IncrFloat { .. }
        | KvOp::Cas { .. }
        | KvOp::GetSet { .. }
        | KvOp::Transfer { .. }
        | KvOp::RegisterSortedIndex { .. }
        | KvOp::DropSortedIndex { .. }
        | KvOp::SortedIndexRank { .. }
        | KvOp::SortedIndexTopK { .. }
        | KvOp::SortedIndexRange { .. }
        | KvOp::SortedIndexCount { .. }
        | KvOp::SortedIndexScore { .. }
        | KvOp::SortedIndexTxnRead { .. }
        | KvOp::PredicateUpdate { .. }
        | KvOp::PredicateDelete { .. }
        | KvOp::MaterializeScan { .. }) => other.collection().into_iter().collect(),
    }
}

pub(super) fn add_general_requirements(
    plan: &PhysicalPlan,
    out: &mut Vec<AuthorizationRequirement>,
) {
    let permission = required_permission(plan);
    for collection in touched_collections(plan) {
        out.push(AuthorizationRequirement::collection(collection, permission));
    }
}

pub(super) fn add_collection_requirement(
    collection: &str,
    permission: Permission,
    out: &mut Vec<AuthorizationRequirement>,
) {
    if !collection.is_empty() {
        out.push(AuthorizationRequirement::collection(collection, permission));
    }
}

pub(super) fn add_read(collection: &str, out: &mut Vec<AuthorizationRequirement>) {
    add_collection_requirement(collection, Permission::Read, out);
}
