// SPDX-License-Identifier: BUSL-1.1

//! Plan rewriting from a target-database read into the equivalent source-database
//! read at the effective source LSN.

use nodedb_types::DatabaseId;
use nodedb_types::TenantId;

use crate::control::state::SharedState;
use nodedb_physical::physical_plan::{
    ColumnarOp, DocumentOp, ExchangeOp, KvOp, PhysicalPlan, QueryOp, TimeseriesOp,
};
use nodedb_types::SystemTimeScope;

use super::refusal::{SourceRewrite, plan_reads_cloned_collection, refuse_clone_read_shape};

/// Compute the source-side system-time selection for a clone scan rewrite.
///
/// Snapshot clones read the source at a fixed point-in-time (`effective_ms`),
/// so they always collapse to an `AsOf` (or the plan's own selection when the
/// clone carries no ceiling). `AllVersions` (audit log) does not compose with a
/// snapshot clone — the request is rejected with a typed error rather than
/// silently picking an arbitrary snapshot.
fn rewrite_system_time(
    effective_ms: Option<i64>,
    plan_scope: SystemTimeScope,
) -> crate::Result<SystemTimeScope> {
    if matches!(plan_scope, SystemTimeScope::AllVersions) {
        return Err(crate::Error::PlanError {
            detail: "AS OF SYSTEM TIME NULL (all-versions) cannot be read through a \
                     snapshot clone; query the source database directly"
                .into(),
        });
    }
    match effective_ms {
        Some(ms) => Ok(SystemTimeScope::AsOf(ms)),
        None => Ok(plan_scope),
    }
}

/// Per-call inputs for [`rewrite_plan_for_source`].
///
/// Bundled into a struct so the function stays under the clippy
/// `too_many_arguments` cap as snapshot-isolation knobs are added.
pub struct RewriteForSourceParams<'a> {
    pub plan: &'a PhysicalPlan,
    pub target_db_id: DatabaseId,
    pub source_db_id: DatabaseId,
    pub tenant_id: TenantId,
    pub target_coll: &'a str,
    pub source_coll: &'a str,
    /// Effective source system-time-ms for `AS OF` rewrites (Document /
    /// Columnar / Timeseries scans).  `None` leaves any pre-existing
    /// `system_as_of_ms` on the plan untouched.
    pub effective_source_ms: Option<i64>,
    /// Source surrogate high-water captured at clone-create time.
    /// Threaded into rewritten KV plans so the source-side scan/get
    /// filters out bindings allocated AFTER the clone's AS-OF
    /// (snapshot isolation for the lazy KV read path).
    pub kv_surrogate_ceiling: Option<u32>,
    pub state: &'a SharedState,
}

/// Rewrite a `PhysicalPlan` to target the source database and collection at
/// the effective source LSN.
///
/// `state` resolves the source surrogate for `DocumentOp::PointGet` rewrites —
/// the target surrogate is not valid in the source database, so the lookup runs
/// read-only against the source-qualified collection.
///
/// A read that names the cloned collection but has no rewrite is refused with a
/// typed error; see [`SourceRewrite`].
pub fn rewrite_plan_for_source(params: RewriteForSourceParams<'_>) -> crate::Result<SourceRewrite> {
    let RewriteForSourceParams {
        plan,
        target_db_id,
        source_db_id,
        tenant_id,
        target_coll,
        source_coll,
        effective_source_ms,
        kv_surrogate_ceiling,
        state,
    } = params;
    let target_qualified = nodedb_types::QualifiedCollection::new(target_db_id, target_coll);
    let source_qualified = nodedb_types::QualifiedCollection::new(source_db_id, source_coll);

    match plan {
        // Structural wrappers, matched before the engine arms: the converter
        // wraps every sharded read in `Exchange{Gather}` / `PostProcess`.
        // Recursing and re-wrapping with the same mode makes the source-side
        // task fan and gather exactly like the target-side one.
        PhysicalPlan::Query(QueryOp::Exchange(op)) => {
            let rewritten = rewrite_plan_for_source(RewriteForSourceParams {
                plan: &op.child,
                target_db_id,
                source_db_id,
                tenant_id,
                target_coll,
                source_coll,
                effective_source_ms,
                kv_surrogate_ceiling,
                state,
            })?;
            Ok(match rewritten {
                SourceRewrite::Task(child) => {
                    SourceRewrite::task(PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
                        child,
                        mode: op.mode.clone(),
                    })))
                }
                SourceRewrite::NoSourceTask => SourceRewrite::NoSourceTask,
            })
        }

        PhysicalPlan::Query(QueryOp::PostProcess {
            input,
            filters,
            projection,
            computed_columns,
            sort_keys,
            limit,
            offset,
            distinct,
        }) => {
            let rewritten = rewrite_plan_for_source(RewriteForSourceParams {
                plan: input,
                target_db_id,
                source_db_id,
                tenant_id,
                target_coll,
                source_coll,
                effective_source_ms,
                kv_surrogate_ceiling,
                state,
            })?;
            Ok(match rewritten {
                SourceRewrite::Task(child) => {
                    SourceRewrite::task(PhysicalPlan::Query(QueryOp::PostProcess {
                        input: child,
                        filters: filters.clone(),
                        projection: projection.clone(),
                        computed_columns: computed_columns.clone(),
                        sort_keys: sort_keys.clone(),
                        limit: *limit,
                        offset: *offset,
                        distinct: *distinct,
                    }))
                }
                SourceRewrite::NoSourceTask => SourceRewrite::NoSourceTask,
            })
        }

        PhysicalPlan::Document(DocumentOp::Scan {
            collection,
            limit,
            offset,
            sort_keys,
            filters,
            distinct,
            projection,
            computed_columns,
            window_functions,
            system_time,
            valid_at_ms,
            prefilter,
        }) if collection == &target_qualified => {
            let system_time = rewrite_system_time(effective_source_ms, *system_time)?;
            Ok(SourceRewrite::task(PhysicalPlan::Document(
                DocumentOp::Scan {
                    collection: source_qualified,
                    limit: *limit,
                    offset: *offset,
                    sort_keys: sort_keys.clone(),
                    filters: filters.clone(),
                    distinct: *distinct,
                    projection: projection.clone(),
                    computed_columns: computed_columns.clone(),
                    window_functions: window_functions.clone(),
                    system_time,
                    valid_at_ms: *valid_at_ms,
                    prefilter: prefilter.clone(),
                },
            )))
        }

        PhysicalPlan::Document(DocumentOp::PointGet {
            collection,
            document_id,
            surrogate: _,
            pk_bytes,
            rls_filters,
            system_time,
            valid_at_ms,
        }) if collection == &target_qualified => {
            // The target surrogate is invalid in the source database — each
            // maintains its own pk→surrogate mapping. No binding means the
            // row never existed there; skip rather than use a sentinel.
            // Lookup errors are also treated as "skip" (visible in the
            // assigner's own metrics/logs instead).
            let system_time = rewrite_system_time(effective_source_ms, *system_time)?;
            let Some(source_surrogate) = state
                .surrogate_assigner
                .lookup(source_db_id, tenant_id, source_qualified.as_str(), pk_bytes)
                .ok()
                .flatten()
            else {
                return Ok(SourceRewrite::NoSourceTask);
            };
            Ok(SourceRewrite::task(PhysicalPlan::Document(
                DocumentOp::PointGet {
                    collection: source_qualified,
                    document_id: document_id.clone(),
                    surrogate: source_surrogate,
                    pk_bytes: pk_bytes.clone(),
                    rls_filters: rls_filters.clone(),
                    system_time,
                    valid_at_ms: *valid_at_ms,
                },
            )))
        }

        PhysicalPlan::Document(DocumentOp::IndexedFetch {
            collection,
            path,
            value,
            filters,
            projection,
            limit,
            offset,
        }) if collection == &target_qualified => Ok(SourceRewrite::task(PhysicalPlan::Document(
            DocumentOp::IndexedFetch {
                collection: source_qualified,
                path: path.clone(),
                value: value.clone(),
                filters: filters.clone(),
                projection: projection.clone(),
                limit: *limit,
                offset: *offset,
            },
        ))),

        PhysicalPlan::Kv(KvOp::Scan {
            collection,
            cursor,
            count,
            filters,
            match_pattern,
            sort_keys,
            // The original target-side scan never carries a ceiling
            // (clones-of-clones still funnel through here per-level);
            // the resolver overrides it for source delegation below.
            surrogate_ceiling: _,
        }) if collection == &target_qualified => {
            Ok(SourceRewrite::task(PhysicalPlan::Kv(KvOp::Scan {
                collection: source_qualified,
                cursor: cursor.clone(),
                count: *count,
                filters: filters.clone(),
                match_pattern: match_pattern.clone(),
                sort_keys: sort_keys.clone(),
                surrogate_ceiling: kv_surrogate_ceiling,
            })))
        }

        PhysicalPlan::Kv(KvOp::Get {
            collection,
            key,
            rls_filters,
            surrogate_ceiling: _,
        }) if collection == &target_qualified => {
            Ok(SourceRewrite::task(PhysicalPlan::Kv(KvOp::Get {
                collection: source_qualified,
                key: key.clone(),
                rls_filters: rls_filters.clone(),
                surrogate_ceiling: kv_surrogate_ceiling,
            })))
        }

        PhysicalPlan::Columnar(ColumnarOp::Scan {
            collection,
            projection,
            limit,
            filters,
            rls_filters,
            sort_keys,
            system_time,
            valid_at_ms,
            prefilter,
            computed_columns,
        }) if collection == &target_qualified => {
            let system_time = rewrite_system_time(effective_source_ms, *system_time)?;
            Ok(SourceRewrite::task(PhysicalPlan::Columnar(
                ColumnarOp::Scan {
                    collection: source_qualified,
                    projection: projection.clone(),
                    limit: *limit,
                    filters: filters.clone(),
                    rls_filters: rls_filters.clone(),
                    sort_keys: sort_keys.clone(),
                    system_time,
                    valid_at_ms: *valid_at_ms,
                    prefilter: prefilter.clone(),
                    computed_columns: computed_columns.clone(),
                },
            )))
        }

        // A bucketing or aggregating timeseries scan is the same unsound
        // concatenation as `Query::Aggregate`: target and source payloads are
        // appended, so a bucket present on both sides comes back twice and
        // every sum/avg over the union is wrong. Only a plain scan reads
        // through; the aggregating form is refused.
        PhysicalPlan::Timeseries(TimeseriesOp::Scan {
            collection,
            bucket_interval_ms,
            group_by,
            aggregates,
            ..
        }) if collection == &target_qualified
            && (!group_by.is_empty() || !aggregates.is_empty() || *bucket_interval_ms != 0) =>
        {
            Err(refuse_clone_read_shape(plan, target_coll))
        }

        PhysicalPlan::Timeseries(TimeseriesOp::Scan {
            collection,
            time_range,
            projection,
            limit,
            filters,
            sort_keys,
            bucket_interval_ms,
            group_by,
            aggregates,
            gap_fill,
            computed_columns,
            rls_filters,
            system_time,
            valid_at_ms,
        }) if collection == &target_qualified => {
            let system_time = rewrite_system_time(effective_source_ms, *system_time)?;
            Ok(SourceRewrite::task(PhysicalPlan::Timeseries(
                TimeseriesOp::Scan {
                    collection: source_qualified,
                    time_range: *time_range,
                    projection: projection.clone(),
                    limit: *limit,
                    filters: filters.clone(),
                    sort_keys: sort_keys.clone(),
                    bucket_interval_ms: *bucket_interval_ms,
                    group_by: group_by.clone(),
                    aggregates: aggregates.clone(),
                    gap_fill: gap_fill.clone(),
                    computed_columns: computed_columns.clone(),
                    rls_filters: rls_filters.clone(),
                    system_time,
                    valid_at_ms: *valid_at_ms,
                },
            )))
        }

        // DEFAULT: refuse any READ naming the cloned collection (aggregates,
        // joins, vector/text/graph/spatial searches — no proven rewrite);
        // allow everything else through untouched. Every top-level variant
        // is enumerated so a new engine forces a decision here.
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
        | PhysicalPlan::ClusterEvent(_) => {
            if plan_reads_cloned_collection(plan, target_qualified.as_str()) {
                return Err(refuse_clone_read_shape(plan, target_coll));
            }
            Ok(SourceRewrite::NoSourceTask)
        }
    }
}

/// Strip the `"<db_id>/"` prefix added by `db_qualified()`, returning the
/// bare collection name.  If the collection was stored without a prefix
/// (default database, id == 0), the string is returned as-is.
pub(super) fn strip_db_prefix(db_id: DatabaseId, qualified: &str) -> &str {
    if db_id == DatabaseId::DEFAULT {
        return qualified;
    }
    let prefix = format!("{}/", db_id.as_u64());
    if let Some(stripped) = qualified.strip_prefix(prefix.as_str()) {
        stripped
    } else {
        qualified
    }
}

#[cfg(test)]
mod tests {
    use crate::control::server::shared::plan_util::extract_collection;
    use nodedb_graph::{Direction, GraphTraversalOptions};
    use nodedb_physical::physical_plan::{
        ColumnarOp, DocumentOp, ExchangeMode, ExchangeOp, GraphOp, PhysicalPlan, QueryOp, TextOp,
        VectorOp,
    };
    use nodedb_types::QualifiedCollection;
    use nodedb_types::SystemTimeScope;
    use nodedb_types::vector_distance::DistanceMetric;

    const COLL: &str = "7/users";

    /// Wrap a plan the way the converter wraps every sharded source.
    fn gather(plan: PhysicalPlan) -> PhysicalPlan {
        PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            child: Box::new(plan),
            mode: ExchangeMode::Gather {
                as_aggregate: false,
            },
        }))
    }

    /// One representative plan per collection-carrying variant accepted by
    /// `PhysicalPlan::is_sharded_source`. Graph traversal ops are sharded
    /// sources too but carry no collection (keyed by node/edge label
    /// instead); `RagFusion` is the one graph op that names one, and it's
    /// covered here.
    fn sharded_source_plans() -> Vec<(&'static str, PhysicalPlan)> {
        vec![
            (
                "document_scan",
                PhysicalPlan::Document(DocumentOp::Scan {
                    collection: QualifiedCollection::from_stored(COLL.to_string()),
                    limit: 10,
                    offset: 0,
                    sort_keys: Vec::new(),
                    filters: Vec::new(),
                    distinct: false,
                    projection: Vec::new(),
                    computed_columns: Vec::new(),
                    window_functions: Vec::new(),
                    system_time: SystemTimeScope::default(),
                    valid_at_ms: None,
                    prefilter: None,
                }),
            ),
            (
                "columnar_scan",
                PhysicalPlan::Columnar(ColumnarOp::Scan {
                    collection: QualifiedCollection::from_stored(COLL.to_string()),
                    projection: Vec::new(),
                    limit: 10,
                    filters: Vec::new(),
                    rls_filters: Vec::new(),
                    sort_keys: Vec::new(),
                    system_time: SystemTimeScope::default(),
                    valid_at_ms: None,
                    prefilter: None,
                    computed_columns: Vec::new(),
                }),
            ),
            (
                "partial_aggregate",
                PhysicalPlan::Query(QueryOp::PartialAggregate {
                    collection: QualifiedCollection::from_stored(COLL.to_string()),
                    group_by: Vec::new(),
                    aggregates: Vec::new(),
                    filters: Vec::new(),
                }),
            ),
            (
                "partial_aggregate_state",
                PhysicalPlan::Query(QueryOp::PartialAggregateState {
                    collection: QualifiedCollection::from_stored(COLL.to_string()),
                    input: None,
                    group_by: Vec::new(),
                    aggregates: Vec::new(),
                    filters: Vec::new(),
                }),
            ),
            (
                "vector_search",
                PhysicalPlan::Vector(VectorOp::Search {
                    collection: QualifiedCollection::from_stored(COLL.to_string()),
                    query_vector: vec![0.0, 1.0],
                    top_k: 4,
                    ef_search: 16,
                    metric: DistanceMetric::L2,
                    filter_bitmap: None,
                    field_name: "emb".to_string(),
                    rls_filters: Vec::new(),
                    inline_prefilter_plan: None,
                    ann_options: nodedb_types::VectorAnnOptions::default(),
                    skip_payload_fetch: false,
                    payload_filters: Vec::new(),
                }),
            ),
            (
                "text_search",
                PhysicalPlan::Text(TextOp::Search {
                    collection: QualifiedCollection::from_stored(COLL.to_string()),
                    query: "hello".to_string(),
                    top_k: 4,
                    fuzzy: false,
                    prefilter: None,
                    rls_filters: Vec::new(),
                }),
            ),
            (
                "text_bm25_score_scan",
                PhysicalPlan::Text(TextOp::BM25ScoreScan {
                    collection: QualifiedCollection::from_stored(COLL.to_string()),
                    query: "hello".to_string(),
                    score_alias: "score".to_string(),
                    fuzzy: false,
                }),
            ),
            (
                "text_hybrid_search",
                PhysicalPlan::Text(TextOp::HybridSearch {
                    collection: QualifiedCollection::from_stored(COLL.to_string()),
                    query_vector: vec![0.0, 1.0],
                    query_text: "hello".to_string(),
                    top_k: 4,
                    ef_search: 16,
                    fuzzy: false,
                    vector_weight: 0.5,
                    filter_bitmap: None,
                    rls_filters: Vec::new(),
                    score_alias: None,
                }),
            ),
            (
                "text_hybrid_search_triple",
                PhysicalPlan::Text(TextOp::HybridSearchTriple {
                    collection: QualifiedCollection::from_stored(COLL.to_string()),
                    query_vector: vec![0.0, 1.0],
                    query_text: "hello".to_string(),
                    graph_seed_id: "n1".to_string(),
                    graph_depth: 1,
                    graph_edge_label: None,
                    top_k: 4,
                    ef_search: 16,
                    fuzzy: false,
                    rrf_k: (60.0, 60.0, 60.0),
                    filter_bitmap: None,
                    rls_filters: Vec::new(),
                    score_alias: None,
                }),
            ),
            (
                "graph_rag_fusion",
                PhysicalPlan::Graph(GraphOp::RagFusion {
                    collection: QualifiedCollection::from_stored(COLL.to_string()),
                    query_vector: vec![0.0, 1.0],
                    vector_top_k: 4,
                    edge_label: None,
                    direction: Direction::Out,
                    expansion_depth: 1,
                    final_top_k: 4,
                    rrf_k: (60.0, 60.0),
                    rrf_k_triple: None,
                    vector_field: "emb".to_string(),
                    options: GraphTraversalOptions::default(),
                    bm25_query: None,
                    bm25_field: None,
                }),
            ),
        ]
    }

    /// Every sharded source the converter wraps in `Exchange{Gather}` must
    /// still be reachable by the collection extractor the clone resolver runs
    /// on the first task. When it is not, the resolver reads a cloned
    /// collection as "not a clone" and the query silently returns zero source
    /// rows.
    #[test]
    fn clone_resolver_sees_through_the_converter_wrapper() {
        for (name, plan) in sharded_source_plans() {
            assert!(
                plan.is_sharded_source(),
                "{name}: plan is no longer a sharded source — update this list"
            );
            assert_eq!(
                extract_collection(&plan),
                Some(COLL),
                "{name}: bare plan must expose its collection"
            );
            assert_eq!(
                extract_collection(&gather(plan)),
                Some(COLL),
                "{name}: wrapped plan must expose its collection"
            );
        }
    }

    /// The default arm refuses a plan when it is classified `Read` AND its
    /// collection is extractable. Both inputs must hold for every sharded
    /// source, or an unrewritable read would fall through to `NoSourceTask` and
    /// answer from the target alone.
    #[test]
    fn every_sharded_source_is_a_classified_read() {
        for (name, plan) in sharded_source_plans() {
            assert_eq!(
                crate::control::security::identity::required_permission(&plan),
                crate::control::security::identity::Permission::Read,
                "{name}: a sharded source must classify as a read"
            );
            assert_eq!(
                extract_collection(&gather(plan)),
                Some(COLL),
                "{name}: the refusal check must see the collection"
            );
        }
    }

    /// `PostProcess` is the other wrapper the converter puts over a
    /// materialized subquery body.
    #[test]
    fn clone_resolver_sees_through_post_process() {
        for (name, plan) in sharded_source_plans() {
            let wrapped = PhysicalPlan::Query(QueryOp::PostProcess {
                input: Box::new(gather(plan)),
                filters: Vec::new(),
                projection: Vec::new(),
                sort_keys: Vec::new(),
                limit: None,
                offset: 0,
                distinct: false,
            });
            assert_eq!(
                extract_collection(&wrapped),
                Some(COLL),
                "{name}: post-processed plan must expose its collection"
            );
        }
    }
}
