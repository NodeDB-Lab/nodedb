// SPDX-License-Identifier: BUSL-1.1

//! Footprints of vector, text, columnar, timeseries and spatial ops.
//!
//! Each write keys its collection. A sync frame, a write with sync
//! provenance, also keys the high-water mark of its stream.

use nodedb_physical::physical_plan::{ColumnarOp, SpatialOp, TextOp, TimeseriesOp, VectorOp};
use nodedb_types::QualifiedCollection;
use nodedb_types::sync::wire::SyncProvenance;

use super::class::{Footprint, Scope};
use super::keys::{KeySet, read_of, write_of};
use super::plan::plan_footprint;

/// A write of `collection`, and of the stream mark a sync frame advances.
fn sync_write(
    collection: &QualifiedCollection,
    provenance: &Option<SyncProvenance>,
    scope: &Scope,
) -> Footprint {
    KeySet::new(scope)
        .collection(collection)
        .sync(provenance.as_ref())
        .write()
}

pub(super) fn vector_footprint(op: &VectorOp, scope: &Scope) -> Footprint {
    match op {
        // An inline prefilter plan runs on this core as part of the search.
        VectorOp::Search {
            collection,
            inline_prefilter_plan,
            ..
        } => match inline_prefilter_plan {
            Some(prefilter) => read_of(collection, scope).join(plan_footprint(prefilter, scope)),
            None => read_of(collection, scope),
        },
        VectorOp::MultiSearch { collection, .. }
        | VectorOp::SparseSearch { collection, .. }
        | VectorOp::MultiVectorScoreSearch { collection, .. }
        | VectorOp::QueryStats { collection, .. } => read_of(collection, scope),
        VectorOp::Insert {
            collection,
            provenance,
            ..
        }
        | VectorOp::DeleteBySurrogate {
            collection,
            provenance,
            ..
        } => sync_write(collection, provenance, scope),
        VectorOp::BatchInsert { collection, .. }
        | VectorOp::Delete { collection, .. }
        | VectorOp::SetParams { collection, .. }
        | VectorOp::DropIndex { collection, .. }
        | VectorOp::Seal { collection, .. }
        | VectorOp::CompactIndex { collection, .. }
        | VectorOp::Rebuild { collection, .. }
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
        | VectorOp::DirectUpdate { collection, .. }
        | VectorOp::ResolvedDirectWrite { collection, .. } => write_of(collection, scope),
        // The resolve pass reads what the wrapped write writes.
        VectorOp::ResolveDirectWrite(inner) => vector_footprint(inner, scope),
    }
}

pub(super) fn text_footprint(op: &TextOp, scope: &Scope) -> Footprint {
    match op {
        TextOp::Search { collection, .. }
        | TextOp::BM25ScoreScan { collection, .. }
        | TextOp::PhraseSearch { collection, .. }
        | TextOp::HybridSearch { collection, .. } => read_of(collection, scope),
        // The triple search also walks the graph.
        TextOp::HybridSearchTriple { collection, .. } => {
            KeySet::new(scope).collection(collection).graph().read()
        }
        TextOp::FtsIndexDoc {
            collection,
            provenance,
            ..
        }
        | TextOp::FtsDeleteDoc {
            collection,
            provenance,
            ..
        } => sync_write(collection, provenance, scope),
        TextOp::SetTextConfig { collection, .. } | TextOp::RebuildIndex { collection, .. } => {
            write_of(collection, scope)
        }
    }
}

pub(super) fn columnar_footprint(op: &ColumnarOp, scope: &Scope) -> Footprint {
    match op {
        ColumnarOp::Scan { collection, .. } | ColumnarOp::MaterializeScan { collection, .. } => {
            read_of(collection, scope)
        }
        ColumnarOp::Insert {
            collection,
            provenance,
            ..
        } => sync_write(collection, provenance, scope),
        ColumnarOp::Update { collection, .. }
        | ColumnarOp::Delete { collection, .. }
        | ColumnarOp::ResolvedUpdate { collection, .. }
        | ColumnarOp::ResolvedDelete { collection, .. }
        | ColumnarOp::ResolveDml { collection, .. }
        | ColumnarOp::Truncate { collection, .. } => write_of(collection, scope),
    }
}

pub(super) fn timeseries_footprint(op: &TimeseriesOp, scope: &Scope) -> Footprint {
    match op {
        TimeseriesOp::Scan { collection, .. } => read_of(collection, scope),
        TimeseriesOp::Ingest {
            collection,
            provenance,
            ..
        } => sync_write(collection, provenance, scope),
        // The resolve pass reads what the wrapped ingest writes.
        TimeseriesOp::ResolveIngest(resolve) => timeseries_footprint(&resolve.ingest, scope),
        // Each aggregate op names its aggregate's root source collection. An
        // aggregate read folds the aggregate's backlog before it answers.
        TimeseriesOp::Truncate { collection, .. }
        | TimeseriesOp::RefreshAggregate { collection, .. }
        | TimeseriesOp::Partitions { collection, .. }
        | TimeseriesOp::ReadAggregate { collection, .. } => write_of(collection, scope),
    }
}

pub(super) fn spatial_footprint(op: &SpatialOp, scope: &Scope) -> Footprint {
    match op {
        SpatialOp::Scan { collection, .. } => read_of(collection, scope),
        SpatialOp::Insert {
            collection,
            provenance,
            ..
        }
        | SpatialOp::Delete {
            collection,
            provenance,
            ..
        } => sync_write(collection, provenance, scope),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{ColumnarInsertIntent, PhysicalPlan};
    use nodedb_types::{RlsWriteCheck, Surrogate};

    use super::super::keys::tests::{coll, graph, key, scope, write};
    use super::*;

    fn class(plan: PhysicalPlan) -> Footprint {
        plan_footprint(&plan, &scope())
    }

    fn provenance() -> Option<SyncProvenance> {
        Some(SyncProvenance {
            producer_id: 1,
            epoch: 1,
            stream_id: 1,
            seq: 1,
        })
    }

    /// The stream key of [`provenance`].
    fn stream() -> crate::data::executor::core_loop::install_fence::FenceKey {
        crate::data::executor::core_loop::install_fence::FenceKey::SyncStream {
            producer_id: 1,
            stream_id: 1,
        }
    }

    fn vector_insert(provenance: Option<SyncProvenance>) -> PhysicalPlan {
        PhysicalPlan::Vector(VectorOp::Insert {
            collection: coll("v"),
            vector: vec![0.0],
            dim: 1,
            field_name: "emb".into(),
            surrogate: Surrogate::new(1),
            pk_bytes: None,
            provenance,
        })
    }

    #[test]
    fn a_vector_insert_keys_its_collection_and_a_sync_frame_its_stream() {
        assert_eq!(class(vector_insert(None)), write(vec![key("v")]));
        assert_eq!(
            class(vector_insert(provenance())),
            write(vec![key("v"), stream()])
        );
    }

    #[test]
    fn a_vector_stats_read_is_a_read_of_its_collection() {
        let stats = PhysicalPlan::Vector(VectorOp::QueryStats {
            collection: coll("v"),
            field_name: "emb".into(),
        });
        assert_eq!(class(stats), Footprint::Keys(vec![key("v")]));
    }

    #[test]
    fn text_writes_key_their_collection() {
        let index = PhysicalPlan::Text(TextOp::FtsIndexDoc {
            collection: coll("t"),
            surrogate: Surrogate::new(1),
            fields: Vec::new(),
            provenance: None,
        });
        assert_eq!(class(index), write(vec![key("t")]));
        let config = PhysicalPlan::Text(TextOp::SetTextConfig {
            collection: coll("t"),
            analyzer_name: None,
            fuzzy_default: None,
        });
        assert_eq!(class(config), write(vec![key("t")]));
        let delete = PhysicalPlan::Text(TextOp::FtsDeleteDoc {
            collection: coll("t"),
            surrogate: None,
            provenance: provenance(),
        });
        assert_eq!(class(delete), write(vec![key("t"), stream()]));
    }

    #[test]
    fn a_triple_search_reads_its_collection_and_the_graph() {
        let search = PhysicalPlan::Text(TextOp::HybridSearchTriple {
            collection: coll("t"),
            vector_field: "emb".into(),
            query_vector: Vec::new(),
            text_field: None,
            query_text: String::new(),
            filters: Vec::new(),
            graph_seed_id: "n".into(),
            graph_depth: 1,
            graph_edge_label: None,
            top_k: 1,
            ef_search: 1,
            mode: nodedb_types::text_search::QueryMode::default(),
            fuzzy: false,
            rrf_k: (1.0, 1.0, 1.0),
            filter_bitmap: None,
            rls_filters: Vec::new(),
            score_alias: None,
            system_time: nodedb_types::SystemTimeScope::Current,
        });
        assert_eq!(class(search), Footprint::Keys(vec![key("t"), graph()]));
    }

    #[test]
    fn a_columnar_insert_keys_its_collection() {
        let insert = PhysicalPlan::Columnar(ColumnarOp::Insert {
            collection: coll("c"),
            payload: Vec::new(),
            format: "msgpack".into(),
            intent: ColumnarInsertIntent::Insert,
            on_conflict_updates: Vec::new(),
            surrogates: Vec::new(),
            schema_bytes: Vec::new(),
            provenance: None,
            wal_lsn: None,
            system_from: None,
            restored_versions: Vec::new(),
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: Vec::new(),
        });
        assert_eq!(class(insert), write(vec![key("c")]));
    }

    #[test]
    fn a_columnar_page_read_is_a_read_of_its_collection() {
        let page = PhysicalPlan::Columnar(ColumnarOp::MaterializeScan {
            collection: coll("c"),
            cursor: Vec::new(),
            count: 1,
            system_as_of_ms: None,
        });
        assert_eq!(class(page), Footprint::Keys(vec![key("c")]));
    }

    #[test]
    fn timeseries_writes_and_aggregate_reads_key_their_collection() {
        let truncate = PhysicalPlan::Timeseries(TimeseriesOp::Truncate {
            collection: coll("ts"),
            restart_identity: false,
        });
        assert_eq!(class(truncate), write(vec![key("ts")]));
        let aggregate = PhysicalPlan::Timeseries(TimeseriesOp::ReadAggregate {
            collection: coll("ts"),
            aggregate: "ts_1m".into(),
            system_time: nodedb_types::SystemTimeScope::Current,
        });
        assert_eq!(class(aggregate), write(vec![key("ts")]));
        let ingest = PhysicalPlan::Timeseries(TimeseriesOp::Ingest {
            collection: coll("ts"),
            payload: Vec::new(),
            format: "ilp".into(),
            wal_lsn: None,
            surrogates: Vec::new(),
            provenance: None,
            rls_write_check: RlsWriteCheck::NoPolicyApplies,
            returning: None,
            rls_filters: Vec::new(),
        });
        assert_eq!(class(ingest), write(vec![key("ts")]));
    }

    #[test]
    fn a_spatial_delete_keys_its_collection() {
        let delete = PhysicalPlan::Spatial(SpatialOp::Delete {
            collection: coll("s"),
            field: "geom".into(),
            surrogate: None,
            provenance: None,
        });
        assert_eq!(class(delete), write(vec![key("s")]));
    }
}
