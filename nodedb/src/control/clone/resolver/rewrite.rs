// SPDX-License-Identifier: BUSL-1.1

//! Plan rewriting from a target-database read into the equivalent source-database
//! read at the effective source LSN.

use nodedb_types::DatabaseId;
use nodedb_types::QualifiedCollection;
use nodedb_types::TenantId;

use crate::control::state::SharedState;
use nodedb_physical::physical_plan::{ExchangeOp, PhysicalPlan, QueryOp, SetOpKind};
use nodedb_types::SystemTimeScope;

use super::refusal::{SourceRewrite, plan_reads_cloned_collection, refuse_clone_read_shape};
use super::rewrite_engine::{rewrite_columnar, rewrite_document, rewrite_kv, rewrite_timeseries};

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
pub async fn rewrite_plan_for_source(
    params: RewriteForSourceParams<'_>,
) -> crate::Result<SourceRewrite> {
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
    let ctx = RewriteCtx {
        source_db_id,
        tenant_id,
        target_coll,
        source_coll,
        target_qualified: QualifiedCollection::new(target_db_id, target_coll),
        source_qualified: QualifiedCollection::new(source_db_id, source_coll),
        effective_source_ms,
        kv_surrogate_ceiling,
        state,
    };
    rewrite(plan, &ctx).await
}

/// The inputs every rewrite stage shares.
pub(super) struct RewriteCtx<'a> {
    pub source_db_id: DatabaseId,
    pub tenant_id: TenantId,
    pub target_coll: &'a str,
    pub source_coll: &'a str,
    pub target_qualified: QualifiedCollection,
    pub source_qualified: QualifiedCollection,
    pub effective_source_ms: Option<i64>,
    pub kv_surrogate_ceiling: Option<u32>,
    pub state: &'a SharedState,
}

impl RewriteCtx<'_> {
    /// Whether `collection` is the cloned collection.
    pub(super) fn is_target(&self, collection: &QualifiedCollection) -> bool {
        collection == &self.target_qualified
    }

    /// The source-side system-time selection for a plan's own selection.
    pub(super) fn system_time(
        &self,
        plan_scope: SystemTimeScope,
    ) -> crate::Result<SystemTimeScope> {
        rewrite_system_time(self.effective_source_ms, plan_scope)
    }
}

/// Rewrite one plan node through the stage for its plan family.
async fn rewrite(plan: &PhysicalPlan, ctx: &RewriteCtx<'_>) -> crate::Result<SourceRewrite> {
    match plan {
        PhysicalPlan::Query(op) => rewrite_query(plan, op, ctx).await,
        PhysicalPlan::Document(op) => rewrite_document(plan, op, ctx).await,
        PhysicalPlan::Kv(op) => rewrite_kv(plan, op, ctx),
        PhysicalPlan::Columnar(op) => rewrite_columnar(plan, op, ctx),
        PhysicalPlan::Timeseries(op) => rewrite_timeseries(plan, op, ctx),
        // No proven rewrite for these families. Every top-level variant is
        // enumerated so a new engine forces a decision here.
        PhysicalPlan::Vector(_)
        | PhysicalPlan::Graph(_)
        | PhysicalPlan::Text(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Crdt(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::Array(_)
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => refuse_or_skip(plan, ctx),
    }
}

/// The default for a plan with no rewrite. A read that names the cloned
/// collection (aggregates, joins, vector/text/graph/spatial searches) is
/// refused. Everything else passes through untouched.
pub(super) fn refuse_or_skip(
    plan: &PhysicalPlan,
    ctx: &RewriteCtx<'_>,
) -> crate::Result<SourceRewrite> {
    if plan_reads_cloned_collection(plan, ctx.target_qualified.as_str()) {
        return Err(refuse_clone_read_shape(plan, ctx.target_coll));
    }
    Ok(SourceRewrite::NoSourceTask)
}

/// Put a rewritten child back under the wrapper node it came from.
fn rewrap(
    rewritten: SourceRewrite,
    wrap: impl FnOnce(Box<PhysicalPlan>) -> PhysicalPlan,
) -> SourceRewrite {
    match rewritten {
        SourceRewrite::Task(child) => SourceRewrite::task(wrap(child)),
        SourceRewrite::NoSourceTask => SourceRewrite::NoSourceTask,
    }
}

/// Query plans. The structural wrappers recurse into their inputs. Every
/// other query op takes the default.
async fn rewrite_query(
    plan: &PhysicalPlan,
    op: &QueryOp,
    ctx: &RewriteCtx<'_>,
) -> crate::Result<SourceRewrite> {
    match op {
        // The converter wraps every sharded read in `Exchange{Gather}` /
        // `PostProcess`. Recursing and re-wrapping with the same mode makes
        // the source-side task fan and gather exactly like the target-side
        // one.
        QueryOp::Exchange(exchange) => {
            let rewritten = Box::pin(rewrite(&exchange.child, ctx)).await?;
            Ok(rewrap(rewritten, |child| {
                PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
                    child,
                    mode: exchange.mode.clone(),
                }))
            }))
        }
        QueryOp::PostProcess {
            input,
            filters,
            projection,
            computed_columns,
            window_functions,
            sort_keys,
            limit,
            offset,
            distinct,
        } => {
            let rewritten = Box::pin(rewrite(input, ctx)).await?;
            Ok(rewrap(rewritten, |child| {
                PhysicalPlan::Query(QueryOp::PostProcess {
                    input: child,
                    filters: filters.clone(),
                    projection: projection.clone(),
                    computed_columns: computed_columns.clone(),
                    window_functions: window_functions.clone(),
                    sort_keys: sort_keys.clone(),
                    limit: *limit,
                    offset: *offset,
                    distinct: *distinct,
                })
            }))
        }
        QueryOp::SetOp { inputs, op } => rewrite_set_op(inputs, op, ctx).await,
        _ => refuse_or_skip(plan, ctx),
    }
}

/// Rewrite every branch of a set operation.
///
/// Branches that do not read the cloned collection yield no source task and
/// are dropped, so the source-side node carries only the rows the target side
/// is missing. That is sound for `UNION ALL` (the merge appends). Any other
/// kind dedups or subtracts by exact row match against the target rows, which
/// is unsound across an unmaterialized clone. It is refused the same way the
/// task-level `post_set_op` refusal does.
async fn rewrite_set_op(
    inputs: &[PhysicalPlan],
    op: &SetOpKind,
    ctx: &RewriteCtx<'_>,
) -> crate::Result<SourceRewrite> {
    let mut rewritten_inputs = Vec::with_capacity(inputs.len());
    for input in inputs {
        if let SourceRewrite::Task(child) = Box::pin(rewrite(input, ctx)).await? {
            rewritten_inputs.push(*child);
        }
    }
    if rewritten_inputs.is_empty() {
        return Ok(SourceRewrite::NoSourceTask);
    }
    match op {
        SetOpKind::UnionAll => Ok(SourceRewrite::task(PhysicalPlan::Query(QueryOp::SetOp {
            inputs: rewritten_inputs,
            op: SetOpKind::UnionAll,
        }))),
        SetOpKind::UnionDistinct
        | SetOpKind::Intersect
        | SetOpKind::IntersectAll
        | SetOpKind::Except
        | SetOpKind::ExceptAll => Err(crate::Error::PlanError {
            detail: format!(
                "a set operation over '{}' cannot be read through an \
                 unmaterialized clone; run ALTER DATABASE <clone> MATERIALIZE first",
                ctx.target_coll
            ),
        }),
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
                    field: None,
                    query: "hello".to_string(),
                    top_k: 4,
                    mode: nodedb_types::text_search::QueryMode::And,
                    fuzzy: false,
                    prefilter: None,
                    filters: Vec::new(),
                    rls_filters: Vec::new(),
                    scores: Vec::new(),
                }),
            ),
            (
                "text_bm25_score_scan",
                PhysicalPlan::Text(TextOp::BM25ScoreScan {
                    collection: QualifiedCollection::from_stored(COLL.to_string()),
                    filters: Vec::new(),
                    rls_filters: Vec::new(),
                    scores: vec![nodedb_physical::physical_plan::TextScoreSpec {
                        field: None,
                        query: "hello".to_string(),
                        mode: nodedb_types::text_search::QueryMode::And,
                        fuzzy: false,
                        alias: "score".to_string(),
                    }],
                    bound: None,
                }),
            ),
            (
                "text_hybrid_search",
                PhysicalPlan::Text(TextOp::HybridSearch {
                    collection: QualifiedCollection::from_stored(COLL.to_string()),
                    vector_field: String::new(),
                    query_vector: vec![0.0, 1.0],
                    text_field: None,
                    query_text: "hello".to_string(),
                    filters: Vec::new(),
                    top_k: 4,
                    ef_search: 16,
                    mode: nodedb_types::text_search::QueryMode::And,
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
                    vector_field: String::new(),
                    query_vector: vec![0.0, 1.0],
                    text_field: None,
                    query_text: "hello".to_string(),
                    filters: Vec::new(),
                    graph_seed_id: "n1".to_string(),
                    graph_depth: 1,
                    graph_edge_label: None,
                    top_k: 4,
                    ef_search: 16,
                    mode: nodedb_types::text_search::QueryMode::And,
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
                    stage: nodedb_physical::physical_plan::RagStage::Local,
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
    /// source, or an unrewritable read falls through to `NoSourceTask` and
    /// answers from the target alone.
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
                computed_columns: Vec::new(),
                window_functions: Vec::new(),
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
