use crate::bridge::envelope::PhysicalPlan;
use crate::control::gateway::version_set::touched_collections;
use crate::control::security::identity::{Permission, required_permission};

use super::AuthorizationRequirement;
use super::order::requirement_order;
use super::query::collect_query_requirements;

/// Return every authorization requirement for `plan`.
///
/// The general plan permission applies to ordinary single-resource plans. The
/// multi-resource cases below intentionally override it so sources require
/// `Read` while targets require `Write`. Nested query inputs are traversed
/// iteratively in addition to their named collection fields.
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
                    target_collection,
                    Permission::Write,
                ));
                out.push(AuthorizationRequirement::collection(
                    source_collection,
                    Permission::Read,
                ));
            }
            PhysicalPlan::Kv(KvOp::TransferItem {
                source_collection,
                dest_collection,
                ..
            }) => {
                out.push(AuthorizationRequirement::collection(
                    source_collection,
                    Permission::Read,
                ));
                out.push(AuthorizationRequirement::collection(
                    dest_collection,
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
                | CrdtOp::GetVersionVector { collection, .. }
                | CrdtOp::ExportDelta { collection, .. }
                | CrdtOp::CompactAtVersion { collection, .. },
            ) => add_collection_requirement(collection, required_permission(plan), out),
            PhysicalPlan::Graph(GraphOp::EdgePut { collection, .. })
            | PhysicalPlan::Graph(GraphOp::EdgeDelete { collection, .. }) => {
                add_collection_requirement(collection, required_permission(plan), out);
            }
            PhysicalPlan::Graph(GraphOp::EdgePutBatch { edges })
            | PhysicalPlan::Graph(GraphOp::EdgeDeleteBatch { edges }) => {
                let permission = required_permission(plan);
                for edge in edges {
                    add_collection_requirement(&edge.collection, permission, out);
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
            ) => add_collection_requirement(collection, required_permission(plan), out),
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
                add_collection_requirement(old_collection, permission, out);
                add_collection_requirement(new_collection, permission, out);
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
                    collection,
                    Permission::Write,
                ));
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
                | KvOp::MaterializeScan { .. },
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
            | PhysicalPlan::ClusterArray(_) => add_general_requirements(plan, out),
        }

        // A wrapper delegates its resource boundary to its nested plans. Its
        // own missing collection must not add a tenant-wide requirement when a
        // descendant names the protected resource; a genuinely resource-less
        // descendant still receives this fail-closed fallback when visited.
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
        | PhysicalPlan::ClusterArray(_) => true,
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
