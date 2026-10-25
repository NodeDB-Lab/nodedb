// SPDX-License-Identifier: BUSL-1.1

//! The collection a relational, maintenance, or cluster-event operation
//! reads or writes rows of.
//!
//! Each function matches its operation family variant by variant. A variant
//! that reads or writes rows of one named collection reports it.

use nodedb_physical::physical_plan::{ClusterEventOp, MetaOp, QueryOp};

use super::collection::extract_collection;

pub(super) fn query_collection(op: &QueryOp) -> Option<&str> {
    match op {
        QueryOp::Aggregate { collection, .. }
        | QueryOp::PartialAggregate { collection, .. }
        // Scans the named collection like `PartialAggregate`. `collection`
        // stays populated even when `input` is set.
        | QueryOp::PartialAggregateState { collection, .. }
        | QueryOp::FacetCounts { collection, .. }
        | QueryOp::RecursiveScan { collection, .. } => Some(collection.as_str()),
        // A join reports its left side. Callers that need both sides walk
        // the join themselves.
        QueryOp::HashJoin {
            left_collection, ..
        }
        | QueryOp::NestedLoopJoin {
            left_collection, ..
        }
        | QueryOp::SortMergeJoin {
            left_collection, ..
        } => Some(left_collection.as_str()),
        // A lateral join reports its outer side, as a join reports its left
        // side. An outer side that names no collection leaves the inner one.
        QueryOp::LateralTopK {
            outer_plan,
            inner_collection,
            ..
        }
        | QueryOp::LateralLoop {
            outer_plan,
            inner_collection,
            ..
        } => extract_collection(outer_plan).or(Some(inner_collection.as_str())),
        QueryOp::Exchange(op) => extract_collection(&op.child),
        QueryOp::PostProcess { input, .. } => extract_collection(input),
        // The first branch that names a collection, the same way a join
        // reports its left side. Callers that need every branch walk the
        // inputs themselves.
        QueryOp::SetOp { inputs, .. } => inputs.iter().find_map(extract_collection),
        // A catalog or constant source: no user collection.
        QueryOp::ProviderScan { .. } => None,
        // These consume rows staged under a shuffle path, not a collection.
        QueryOp::ShuffleJoinConsume { .. } | QueryOp::ShuffleAggregateConsume { .. } => None,
        // A recursive value computes rows from expressions and reads none.
        QueryOp::RecursiveValue { .. } => None,
    }
}

pub(super) fn meta_collection(op: &MetaOp) -> Option<&str> {
    match op {
        // Each reads or rewrites the rows of the named collection.
        MetaOp::ConvertCollection { collection, .. }
        | MetaOp::VerifyHashChain { collection }
        | MetaOp::EnforceTimeseriesRetention { collection, .. }
        | MetaOp::TemporalPurgeEdgeStore { collection, .. }
        | MetaOp::TemporalPurgeDocumentStrict { collection, .. }
        | MetaOp::TemporalPurgeColumnar { collection, .. }
        | MetaOp::TemporalPurgeCrdt { collection, .. }
        | MetaOp::QueryLastValues { collection }
        | MetaOp::QueryLastValue { collection, .. }
        | MetaOp::RebuildIndex { collection, .. } => Some(collection.as_str()),
        // The staged plan is the write verbatim.
        MetaOp::StageWrite { plan } => extract_collection(plan),
        // Node-wide maintenance names no collection.
        MetaOp::WalAppend { .. }
        | MetaOp::Cancel { .. }
        | MetaOp::CreateSnapshot
        | MetaOp::Compact
        | MetaOp::Checkpoint
        | MetaOp::ApplyContinuousAggRetention => None,
        // These carry several plans or collections. A redo install names
        // its collections through `PhysicalPlan::named_collections`.
        MetaOp::TransactionBatch { .. }
        | MetaOp::CalvinExecuteStatic { .. }
        | MetaOp::CalvinExecutePassive { .. }
        | MetaOp::CalvinExecuteActive { .. }
        | MetaOp::ResolveTxn { .. }
        | MetaOp::ApplyTransactionRedo { .. }
        | MetaOp::RestoreRedo(_) => None,
        // Tenant-wide: every collection of the tenant.
        MetaOp::CreateTenantSnapshot { .. }
        | MetaOp::RestoreTenantSnapshot { .. }
        | MetaOp::PurgeTenant { .. } => None,
        // Every collection homed on vShards this node left, in every tenant.
        MetaOp::PurgeDepartedVShards { .. } => None,
        // The continuous-aggregate registry: no row is read or written.
        MetaOp::RegisterContinuousAggregate { .. }
        | MetaOp::UnregisterContinuousAggregate { .. }
        | MetaOp::ListContinuousAggregates
        | MetaOp::ContinuousAggregatesRegistered
        | MetaOp::AwaitContinuousAggregateRefresh { .. }
        | MetaOp::QueryAggregateWatermark { .. } => None,
        // `name` is a bare catalog name, not a qualified collection.
        MetaOp::UnregisterCollection { .. }
        | MetaOp::UnregisterMaterializedView { .. }
        | MetaOp::QueryCollectionSize { .. } => None,
        // `array_id` is the retention registry's array key, not a qualified
        // collection. `AlterArray` changes array config and no cell.
        MetaOp::TemporalPurgeArray { .. } | MetaOp::AlterArray { .. } => None,
        // The tenant synonym registry: no collection.
        MetaOp::PutSynonymGroup { .. } | MetaOp::DeleteSynonymGroup { .. } => None,
        // The catalog-wide user-defined CRS set: no collection.
        MetaOp::SetCustomCrsSet { .. } => None,
        // Transaction control and version probes name no collection.
        MetaOp::DropTxnOverlay { .. }
        | MetaOp::MarkSavepoint { .. }
        | MetaOp::RollbackToSavepoint { .. }
        | MetaOp::CalvinDrop { .. }
        | MetaOp::CalvinResolve { .. }
        | MetaOp::HomeVersions { .. } => None,
    }
}

pub(super) fn cluster_event_collection(op: &ClusterEventOp) -> Option<&str> {
    match op {
        // A change-stream consumer reads a stream, not a collection.
        ClusterEventOp::ConsumeStream { .. } => None,
        // Surrogate binds can span several collections.
        ClusterEventOp::SurrogateBinds { .. } => None,
        // Tenant-wide or cluster bookkeeping.
        ClusterEventOp::TenantWriteMarks { .. }
        | ClusterEventOp::MetadataApplied { .. }
        | ClusterEventOp::SurrogateHolders { .. } => None,
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{DocumentOp, StorageMode};
    use nodedb_types::{DatabaseId, QualifiedCollection};

    use crate::bridge::envelope::PhysicalPlan;

    use super::*;

    fn docs() -> QualifiedCollection {
        QualifiedCollection::new(DatabaseId::DEFAULT, "docs")
    }

    fn qualified(name: &str) -> String {
        QualifiedCollection::new(DatabaseId::DEFAULT, name)
            .as_str()
            .to_string()
    }

    /// Assert every plan reports `expected`.
    fn assert_names(plans: Vec<PhysicalPlan>, expected: &str) {
        for plan in plans {
            assert_eq!(extract_collection(&plan), Some(expected), "{plan:?}");
        }
    }

    fn estimate(collection: &str) -> PhysicalPlan {
        PhysicalPlan::Document(DocumentOp::EstimateCount {
            collection: QualifiedCollection::new(DatabaseId::DEFAULT, collection),
            field: "id".into(),
        })
    }

    fn provider_scan() -> PhysicalPlan {
        PhysicalPlan::Query(QueryOp::ProviderScan {
            provider: None,
            rows: Vec::new(),
            sources: Vec::new(),
            filters: Vec::new(),
            projection: Vec::new(),
            computed_columns: Vec::new(),
            window_functions: Vec::new(),
            sort_keys: Vec::new(),
            limit: None,
            offset: 0,
            distinct: false,
        })
    }

    fn lateral_top_k(outer: PhysicalPlan) -> PhysicalPlan {
        PhysicalPlan::Query(QueryOp::LateralTopK {
            outer_plan: Box::new(outer),
            outer_alias: "o".into(),
            inner_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "inner"),
            inner_filters: Vec::new(),
            inner_order_by: Vec::new(),
            inner_limit: 1,
            correlation_keys: Vec::new(),
            lateral_alias: "l".into(),
            projection: Vec::new(),
            left_join: false,
        })
    }

    fn lateral_loop(outer: PhysicalPlan) -> PhysicalPlan {
        PhysicalPlan::Query(QueryOp::LateralLoop {
            outer_plan: Box::new(outer),
            outer_alias: "o".into(),
            inner_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "inner"),
            inner_filters: Vec::new(),
            correlation_predicates: Vec::new(),
            lateral_alias: "l".into(),
            projection: Vec::new(),
            left_join: false,
            outer_row_cap: 0,
        })
    }

    #[test]
    fn query_row_ops_name_their_collection() {
        assert_names(
            vec![
                PhysicalPlan::Query(QueryOp::SortMergeJoin {
                    left_collection: docs(),
                    right_collection: QualifiedCollection::new(DatabaseId::DEFAULT, "r"),
                    on: Vec::new(),
                    join_type: "inner".into(),
                    limit: 0,
                    pre_sorted: false,
                    left_rls_filters: Vec::new(),
                    right_rls_filters: Vec::new(),
                    left_input: None,
                    right_input: None,
                }),
                PhysicalPlan::Query(QueryOp::RecursiveScan {
                    collection: docs(),
                    base_filters: Vec::new(),
                    recursive_filters: Vec::new(),
                    join_link: None,
                    max_iterations: 1,
                    distinct: false,
                    limit: 0,
                }),
            ],
            docs().as_str(),
        );
    }

    /// A lateral join reports its outer side, else its inner collection.
    #[test]
    fn a_lateral_join_names_its_outer_then_its_inner_collection() {
        assert_names(
            vec![
                lateral_top_k(estimate("docs")),
                lateral_loop(estimate("docs")),
            ],
            &qualified("docs"),
        );
        assert_names(
            vec![
                lateral_top_k(provider_scan()),
                lateral_loop(provider_scan()),
            ],
            &qualified("inner"),
        );
    }

    #[test]
    fn meta_row_ops_name_their_collection() {
        assert_names(
            vec![
                PhysicalPlan::Meta(MetaOp::ConvertCollection {
                    collection: docs(),
                    target_type: "kv".into(),
                    schema_json: "{}".into(),
                    source_storage_mode: StorageMode::Schemaless,
                }),
                PhysicalPlan::Meta(MetaOp::VerifyHashChain { collection: docs() }),
                PhysicalPlan::Meta(MetaOp::EnforceTimeseriesRetention {
                    collection: docs(),
                    max_age_ms: 0,
                    aggregates: Vec::new(),
                }),
                PhysicalPlan::Meta(MetaOp::TemporalPurgeEdgeStore {
                    tenant_id: 1,
                    collection: docs(),
                    cutoff_system_ms: 0,
                }),
                PhysicalPlan::Meta(MetaOp::TemporalPurgeDocumentStrict {
                    tenant_id: 1,
                    collection: docs(),
                    cutoff_system_ms: 0,
                }),
                PhysicalPlan::Meta(MetaOp::TemporalPurgeColumnar {
                    tenant_id: 1,
                    collection: docs(),
                    cutoff_system_ms: 0,
                    aggregates: Vec::new(),
                }),
                PhysicalPlan::Meta(MetaOp::TemporalPurgeCrdt {
                    tenant_id: 1,
                    collection: docs(),
                    cutoff_system_ms: 0,
                }),
                PhysicalPlan::Meta(MetaOp::QueryLastValues { collection: docs() }),
                PhysicalPlan::Meta(MetaOp::QueryLastValue {
                    collection: docs(),
                    series_id: 7,
                }),
                PhysicalPlan::Meta(MetaOp::RebuildIndex {
                    collection: docs(),
                    index_name: None,
                    concurrent: false,
                }),
                PhysicalPlan::Meta(MetaOp::StageWrite {
                    plan: Box::new(estimate("docs")),
                }),
            ],
            docs().as_str(),
        );
    }
}
