// SPDX-License-Identifier: BUSL-1.1

//! Protocol-neutral physical-plan classification helpers shared by every
//! server entrypoint (pgwire, native, http) and the transaction orchestrator.

use crate::bridge::envelope::PhysicalPlan;
use crate::control::server::shared::session::read_set::{EngineTag, ReadKey};
use crate::types::KeyRepr;
use nodedb_physical::physical_plan::{
    ColumnarOp, DocumentOp, GraphOp, KvOp, MetaOp, QueryOp, SpatialOp, TextOp, TimeseriesOp,
    VectorOp,
};

/// Extract the collection name from a physical plan (if applicable).
pub(crate) fn extract_collection(plan: &PhysicalPlan) -> Option<&str> {
    match plan {
        PhysicalPlan::Document(DocumentOp::PointGet { collection, .. })
        | PhysicalPlan::Vector(VectorOp::Search { collection, .. })
        | PhysicalPlan::Document(DocumentOp::RangeScan { collection, .. })
        | PhysicalPlan::Vector(VectorOp::Insert { collection, .. })
        | PhysicalPlan::Vector(VectorOp::BatchInsert { collection, .. })
        | PhysicalPlan::Vector(VectorOp::MultiSearch { collection, .. })
        // A vector-primary collection stores its row here and nowhere else, so this
        // op is the only source a RETURNING projection can key a redaction policy on.
        | PhysicalPlan::Vector(VectorOp::DirectUpsert { collection, .. })
        | PhysicalPlan::Vector(VectorOp::Delete { collection, .. })
        | PhysicalPlan::Document(DocumentOp::BatchInsert { collection, .. })
        | PhysicalPlan::Document(DocumentOp::PointPut { collection, .. })
        | PhysicalPlan::Document(DocumentOp::PointInsert { collection, .. })
        | PhysicalPlan::Document(DocumentOp::PointDelete { collection, .. })
        | PhysicalPlan::Document(DocumentOp::PointUpdate { collection, .. })
        | PhysicalPlan::Document(DocumentOp::Scan { collection, .. })
        | PhysicalPlan::Query(QueryOp::Aggregate { collection, .. })
        | PhysicalPlan::Query(QueryOp::HashJoin {
            left_collection: collection,
            ..
        })
        | PhysicalPlan::Query(QueryOp::NestedLoopJoin {
            left_collection: collection,
            ..
        })
        | PhysicalPlan::Graph(GraphOp::RagFusion { collection, .. })
        | PhysicalPlan::Vector(VectorOp::SetParams { collection, .. })
        | PhysicalPlan::Text(TextOp::Search { collection, .. })
        | PhysicalPlan::Text(TextOp::PhraseSearch { collection, .. })
        | PhysicalPlan::Text(TextOp::HybridSearch { collection, .. })
        | PhysicalPlan::Text(TextOp::HybridSearchTriple { collection, .. })
        | PhysicalPlan::Text(TextOp::BM25ScoreScan { collection, .. })
        | PhysicalPlan::Text(TextOp::FtsIndexDoc { collection, .. })
        | PhysicalPlan::Text(TextOp::FtsDeleteDoc { collection, .. })
        | PhysicalPlan::Text(TextOp::SetTextConfig { collection, .. })
        | PhysicalPlan::Query(QueryOp::PartialAggregate { collection, .. })
        // Scans the named collection like `PartialAggregate`; `collection` stays
        // populated even when `input` is set.
        | PhysicalPlan::Query(QueryOp::PartialAggregateState { collection, .. })
        | PhysicalPlan::Query(QueryOp::FacetCounts { collection, .. })
        | PhysicalPlan::Document(DocumentOp::BulkUpdate { collection, .. })
        | PhysicalPlan::Document(DocumentOp::BulkDelete { collection, .. })
        | PhysicalPlan::Document(DocumentOp::Upsert { collection, .. })
        | PhysicalPlan::Document(DocumentOp::InsertSelect {
            target_collection: collection,
            ..
        })
        // The source is read, but every written/RETURNING row belongs to the
        // target — that's the collection whose policies and write version apply.
        | PhysicalPlan::Document(DocumentOp::Merge {
            target_collection: collection,
            ..
        })
        | PhysicalPlan::Document(DocumentOp::UpdateFromJoin {
            target_collection: collection,
            ..
        })
        | PhysicalPlan::Document(DocumentOp::Truncate { collection, .. })
        | PhysicalPlan::Document(DocumentOp::EstimateCount { collection, .. })
        | PhysicalPlan::Columnar(ColumnarOp::Scan { collection, .. })
        | PhysicalPlan::Columnar(ColumnarOp::Insert { collection, .. })
        | PhysicalPlan::Columnar(ColumnarOp::Update { collection, .. })
        | PhysicalPlan::Columnar(ColumnarOp::Delete { collection, .. })
        | PhysicalPlan::Columnar(ColumnarOp::ResolvedUpdate { collection, .. })
        | PhysicalPlan::Columnar(ColumnarOp::ResolvedDelete { collection, .. })
        | PhysicalPlan::Timeseries(TimeseriesOp::Scan { collection, .. })
        | PhysicalPlan::Timeseries(TimeseriesOp::Ingest { collection, .. })
        | PhysicalPlan::Spatial(SpatialOp::Scan { collection, .. })
        | PhysicalPlan::Document(DocumentOp::Register { collection, .. })
        | PhysicalPlan::Document(DocumentOp::IndexLookup { collection, .. })
        | PhysicalPlan::Document(DocumentOp::IndexedFetch { collection, .. })
        | PhysicalPlan::Document(DocumentOp::DropIndex { collection, .. }) => {
            Some(collection.as_str())
        }
        PhysicalPlan::Graph(GraphOp::EdgePut { .. })
        | PhysicalPlan::Graph(GraphOp::EdgeDelete { .. })
        | PhysicalPlan::Graph(GraphOp::ResolveEdgeDelete(_))
        | PhysicalPlan::Graph(GraphOp::Hop { .. })
        | PhysicalPlan::Graph(GraphOp::Neighbors { .. })
        | PhysicalPlan::Graph(GraphOp::Path { .. })
        | PhysicalPlan::Graph(GraphOp::Subgraph { .. })
        | PhysicalPlan::Meta(MetaOp::WalAppend { .. })
        | PhysicalPlan::Meta(MetaOp::Cancel { .. })
        | PhysicalPlan::Meta(MetaOp::TransactionBatch { .. })
        | PhysicalPlan::Meta(MetaOp::CreateSnapshot)
        | PhysicalPlan::Meta(MetaOp::Compact)
        | PhysicalPlan::Meta(MetaOp::Checkpoint)
        | PhysicalPlan::Graph(GraphOp::Algo { .. })
        | PhysicalPlan::Graph(GraphOp::Match { .. })
        | PhysicalPlan::Graph(GraphOp::MatchContinuation { .. })
        | PhysicalPlan::Graph(GraphOp::MatchVarLenResume { .. })
        | PhysicalPlan::Graph(GraphOp::BspSuperstep(_))
        | PhysicalPlan::Graph(GraphOp::WccSuperstep(_)) => None,
        // Exchange: recurse into the child plan to extract the collection.
        PhysicalPlan::Query(QueryOp::Exchange(op)) => extract_collection(&op.child),
        // PostProcess: recurse into the materialized child.
        PhysicalPlan::Query(QueryOp::PostProcess { input, .. }) => extract_collection(input),
        // SetOp: the first branch that names a collection, the same way a
        // join reports its left side. Callers that need every branch walk
        // the inputs themselves.
        PhysicalPlan::Query(QueryOp::SetOp { inputs, .. }) => {
            inputs.iter().find_map(extract_collection)
        }
        // ProviderScan is a catalog/constant source — no user collection.
        PhysicalPlan::Query(QueryOp::ProviderScan { .. }) => None,
        // KV ops carry their own collection (sorted-index-only ops return None).
        PhysicalPlan::Kv(op) => op.collection(),
        // Every CRDT op is scoped to exactly one collection's Loro document,
        // so all 20 variants carry a `collection` and the accessor is total.
        // Reporting `None` for any of them would silently drop RLS injection,
        // redaction refusal, clone read/write interception, read-set tracking
        // and metering for that op.
        PhysicalPlan::Crdt(op) => Some(op.collection().as_str()),
        // Read-only resolve wrapper: it reports the wrapped ingest's collection.
        PhysicalPlan::Timeseries(TimeseriesOp::ResolveIngest(inner)) => match inner.as_ref() {
            TimeseriesOp::Scan { collection, .. } | TimeseriesOp::Ingest { collection, .. } => {
                Some(collection.as_str())
            }
            TimeseriesOp::ResolveIngest(_) => None,
        },
        // Remaining ops carry no extractable collection. Exhaustive so a new
        // variant forces a decision rather than silently returning None.
        PhysicalPlan::Document(_)
        | PhysicalPlan::Vector(_)
        | PhysicalPlan::Graph(_)
        | PhysicalPlan::Columnar(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::Array(_)
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => None,
    }
}

/// Every collection a plan's work must be metered against, first-seen order,
/// no duplicates. [`extract_collection`] answers "which one collection" and
/// reports `None` for a `ResolvedWrite`/`TransferItem`, which can span two.
pub(crate) fn metered_collections(plan: &PhysicalPlan) -> Vec<String> {
    let mut out = Vec::new();
    if let PhysicalPlan::Kv(KvOp::TransferItem {
        source_collection,
        dest_collection,
        ..
    }) = plan
    {
        push_distinct(&mut out, source_collection.as_str());
        push_distinct(&mut out, dest_collection.as_str());
        return out;
    }
    if let PhysicalPlan::Kv(KvOp::ResolvedWrite { mutations, .. }) = plan {
        for mutation in mutations {
            push_distinct(&mut out, mutation.collection().as_str());
        }
        return out;
    }
    if let PhysicalPlan::Document(DocumentOp::ResolvedWrite { mutations, .. }) = plan {
        for mutation in mutations {
            push_distinct(&mut out, mutation.collection().as_str());
        }
        return out;
    }
    // Every other plan identifies one collection at most.
    out.extend(extract_collection(plan).map(str::to_string));
    out
}

/// Append `collection` unless it is already present. The list holds at most a
/// handful of names, so a linear scan beats building a set.
fn push_distinct(out: &mut Vec<String>, collection: &str) {
    if !out.iter().any(|held| held == collection) {
        out.push(collection.to_string());
    }
}

/// Classify which peer engine a plan targets. Total over the top-level
/// [`PhysicalPlan`] variants (one-to-one with [`EngineTag`]) so a new engine
/// forces an explicit decision rather than a silent default.
pub(crate) fn plan_engine(plan: &PhysicalPlan) -> EngineTag {
    match plan {
        PhysicalPlan::Vector(_) => EngineTag::Vector,
        PhysicalPlan::Graph(_) => EngineTag::Graph,
        PhysicalPlan::Document(_) => EngineTag::Document,
        PhysicalPlan::Kv(_) => EngineTag::Kv,
        PhysicalPlan::Text(_) => EngineTag::Text,
        PhysicalPlan::Columnar(_) => EngineTag::Columnar,
        PhysicalPlan::Timeseries(_) => EngineTag::Timeseries,
        PhysicalPlan::Spatial(_) => EngineTag::Spatial,
        PhysicalPlan::Crdt(_) => EngineTag::Crdt,
        PhysicalPlan::Query(_) => EngineTag::Query,
        PhysicalPlan::Meta(_) => EngineTag::Meta,
        PhysicalPlan::Array(_) => EngineTag::Array,
        PhysicalPlan::ClusterArray(_) => EngineTag::ClusterArray,
        PhysicalPlan::ClusterEvent(_) => EngineTag::Meta,
    }
}

/// Classify a read plan's observed identity for the transaction read-set. A
/// `PointGet` miss degrades to [`ReadKey::Predicate`] (can't catch a phantom
/// INSERT); a KV miss keeps `Point` — its key IS the future write's key.
pub(crate) fn read_key_of(plan: &PhysicalPlan, found: bool) -> ReadKey {
    match plan {
        PhysicalPlan::Document(DocumentOp::PointGet { surrogate, .. }) => {
            if found {
                ReadKey::Point {
                    repr: KeyRepr::Surrogate(surrogate.as_u32()),
                }
            } else {
                ReadKey::Predicate
            }
        }
        PhysicalPlan::Kv(KvOp::Get { key, .. }) | PhysicalPlan::Kv(KvOp::FieldGet { key, .. }) => {
            ReadKey::Point {
                repr: KeyRepr::KvKey(key.clone().into_boxed_slice()),
            }
        }
        // Observation is confined to the indexed dimension; `filters` (the residual
        // compound predicate) is ignored — validating the indexed dimension is sound.
        PhysicalPlan::Document(
            DocumentOp::IndexedFetch { path, value, .. }
            | DocumentOp::IndexLookup { path, value, .. },
        ) => ReadKey::IndexEq {
            field: path.clone(),
            value: value.clone(),
        },
        // Bound bytes interpreted as UTF-8, same as the scan. One-sided ranges
        // leave the absent bound `None`.
        PhysicalPlan::Document(DocumentOp::RangeScan {
            field,
            lower,
            upper,
            ..
        }) => ReadKey::IndexRange {
            field: field.clone(),
            lo: lower
                .as_ref()
                .map(|b| String::from_utf8_lossy(b).into_owned()),
            hi: upper
                .as_ref()
                .map(|b| String::from_utf8_lossy(b).into_owned()),
        },
        PhysicalPlan::Document(_)
        | PhysicalPlan::Kv(_)
        | PhysicalPlan::Vector(_)
        | PhysicalPlan::Graph(_)
        | PhysicalPlan::Text(_)
        | PhysicalPlan::Columnar(_)
        | PhysicalPlan::Timeseries(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Crdt(_)
        | PhysicalPlan::Query(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::Array(_)
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => ReadKey::Predicate,
    }
}
