// SPDX-License-Identifier: BUSL-1.1

//! Fail-closed refusal of plans whose results column redaction cannot cover.
//!
//! Column redaction is applied to the decoded result rows in the Control Plane
//! (see `server::response_shape`), which works for anything that returns the
//! stored columns themselves. Two plan shapes return something else:
//!
//! - An **aggregate** returns a scalar computed in the Data Plane over the
//!   unredacted stored values. The disclosure has already happened by the time
//!   a mask could be applied to the scalar.
//! - A **graph traversal or pattern match** returns topology — node ids and
//!   edge labels — with no columns for a rule to rewrite.
//!
//! Neither can be masked, so both are refused while a rule exists for the
//! requester's roles, per column and per collection. This runs in the Control
//! Plane before dispatch, next to `rls_injection::inject_rls`, which resolves
//! the same two shapes the same way.

use nodedb_physical::physical_plan::{QueryOp, VectorOp};
use nodedb_physical::physical_task::PhysicalTask;

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::auth_context::AuthContext;
use crate::control::security::redaction::RedactionStore;
use crate::types::TenantId;

use super::aggregate::{JoinSide, refuse_aggregates, refuse_facet_fields, refuse_join_aggregates};
use super::graph::{refuse_graph_op, refuse_match_scoped};
use super::lookup::RefusalCtx;

/// Refuse `plan` when it reads something a redaction policy protects but the
/// result-path masking hook cannot rewrite.
///
/// Returns `Err(crate::Error::PlanError)` — the same typed refusal RLS raises
/// for the operation types it cannot filter.
pub fn refuse_unredactable_plan(
    plan: &PhysicalPlan,
    tenant_id: TenantId,
    database_id: nodedb_types::DatabaseId,
    auth: &AuthContext,
    store: &RedactionStore,
) -> crate::Result<()> {
    // A policy is keyed on a role. An identity holding none can match no
    // policy, so nothing about this plan is unredactable.
    if auth.roles.is_empty() {
        return Ok(());
    }
    let ctx = RefusalCtx {
        store,
        tenant_id: tenant_id.as_u64(),
        roles: &auth.roles,
        database_id,
    };
    walk(plan, &ctx)
}

/// Refuse a graph traversal of `collection` that column redaction cannot cover.
///
/// The `GRAPH TRAVERSE` / `NEIGHBORS` / `PATH` / `SUBGRAPH` DDL family reaches
/// the Data Plane through a broadcast that never builds a single dispatch plan
/// for this pass to inspect, so its shared authorization seam names the
/// collection directly — exactly as it does for the RLS refusal beside it.
pub fn refuse_unredactable_graph_collection(
    collection: &str,
    database_id: nodedb_types::DatabaseId,
    tenant_id: TenantId,
    auth: &AuthContext,
    store: &RedactionStore,
) -> crate::Result<()> {
    if auth.roles.is_empty() {
        return Ok(());
    }
    super::graph::refuse_traversal(
        &RefusalCtx {
            store,
            tenant_id: tenant_id.as_u64(),
            roles: &auth.roles,
            database_id,
        },
        &nodedb_types::QualifiedCollection::new(database_id, collection),
    )
}

/// Refuse a serialized MATCH pattern that column redaction cannot cover.
///
/// The SQL `MATCH` path dispatches its query bytes directly — broadcast to
/// every core, or scattered across owners — instead of handing a plan node to
/// a single dispatch seam, so it has no [`PhysicalPlan`] to pass to
/// [`refuse_unredactable_plan`]. The identical refusal is applied to the
/// query itself here, before either dispatch shape runs.
pub fn refuse_unredactable_graph_match(
    query: &[u8],
    tenant_id: TenantId,
    database_id: nodedb_types::DatabaseId,
    auth: &AuthContext,
    store: &RedactionStore,
) -> crate::Result<()> {
    if auth.roles.is_empty() {
        return Ok(());
    }
    super::graph::refuse_match(
        &RefusalCtx {
            store,
            tenant_id: tenant_id.as_u64(),
            roles: &auth.roles,
            database_id,
        },
        query,
    )
}

/// Refuse a MATCH already parsed into its `collection` scope, without
/// re-decoding a serialized query the caller already holds unpacked.
///
/// Identical fail-closed behavior to [`refuse_unredactable_graph_match`]: a
/// caller that already has the parsed `MatchQuery` (rather than only its
/// encoded bytes) uses this to skip the decode round-trip.
pub fn refuse_unredactable_graph_match_scoped(
    collection: Option<&str>,
    tenant_id: TenantId,
    database_id: nodedb_types::DatabaseId,
    auth: &AuthContext,
    store: &RedactionStore,
) -> crate::Result<()> {
    if auth.roles.is_empty() {
        return Ok(());
    }
    refuse_match_scoped(
        &RefusalCtx {
            store,
            tenant_id: tenant_id.as_u64(),
            roles: &auth.roles,
            database_id,
        },
        collection,
    )
}

/// Refuse any plan in `tasks`, each under its own task's tenant.
///
/// The task-slice twin of [`refuse_unredactable_plan`], mirroring
/// `rls_injection::inject_rls`.
pub fn refuse_unredactable_tasks(
    tasks: &[PhysicalTask],
    auth: &AuthContext,
    store: &RedactionStore,
) -> crate::Result<()> {
    for task in tasks {
        refuse_unredactable_plan(&task.plan, task.tenant_id, task.database_id, auth, store)?;
    }
    Ok(())
}

/// Walk one plan, refusing at the first unredactable read.
///
/// Exhaustive over [`PhysicalPlan`] so a new engine forces a decision here.
fn walk(plan: &PhysicalPlan, ctx: &RefusalCtx<'_>) -> crate::Result<()> {
    match plan {
        PhysicalPlan::Query(op) => walk_query(op, ctx),
        // A graph op embeds no sub-plan: it is refused, or not, on its own.
        PhysicalPlan::Graph(op) => refuse_graph_op(op, ctx),
        // A vector search can carry a resolved sub-plan as its prefilter; the
        // rows it produces come from that plan, so it is walked like any other
        // embedded child.
        PhysicalPlan::Vector(VectorOp::Search {
            inline_prefilter_plan: Some(child),
            ..
        }) => walk(child, ctx),
        // Every remaining op returns stored columns (which the result-path
        // hook masks), a write acknowledgement, or maintenance metadata — and
        // none of them embeds a sub-plan. The inner wildcards keep this arm
        // total over each engine's own operations while the outer match stays
        // exhaustive over `PhysicalPlan`.
        PhysicalPlan::Vector(_)
        | PhysicalPlan::Document(_)
        | PhysicalPlan::Kv(_)
        | PhysicalPlan::Text(_)
        | PhysicalPlan::Columnar(_)
        | PhysicalPlan::Timeseries(_)
        | PhysicalPlan::Spatial(_)
        | PhysicalPlan::Crdt(_)
        | PhysicalPlan::Meta(_)
        | PhysicalPlan::Array(_)
        | PhysicalPlan::ClusterArray(_)
        | PhysicalPlan::ClusterEvent(_) => Ok(()),
    }
}

/// Walk one relational op, refusing its own aggregates and then its children.
///
/// Exhaustive over [`QueryOp`] so a new relational op forces a decision about
/// both halves: whether it aggregates, and whether it carries a sub-plan that
/// an aggregate could hide under.
fn walk_query(op: &QueryOp, ctx: &RefusalCtx<'_>) -> crate::Result<()> {
    match op {
        QueryOp::Exchange(exchange) => walk(&exchange.child, ctx),

        QueryOp::PostProcess { input, .. } => walk(input, ctx),

        QueryOp::Aggregate {
            collection,
            input,
            aggregates,
            sub_aggregates,
            ..
        } => {
            // `collection` stays populated whether the rows come from the
            // named collection or from `input`, so both aggregate lists are
            // resolved against it either way.
            refuse_aggregates(ctx, collection, aggregates)?;
            refuse_aggregates(ctx, collection, sub_aggregates)?;
            match input {
                Some(child) => walk(child, ctx),
                None => Ok(()),
            }
        }

        QueryOp::PartialAggregate {
            collection,
            aggregates,
            ..
        } => refuse_aggregates(ctx, collection, aggregates),

        QueryOp::PartialAggregateState {
            collection,
            input,
            aggregates,
            ..
        } => {
            refuse_aggregates(ctx, collection, aggregates)?;
            match input {
                Some(child) => walk(child, ctx),
                None => Ok(()),
            }
        }

        QueryOp::HashJoin {
            left_collection,
            right_collection,
            left_alias,
            right_alias,
            post_aggregates,
            left_input,
            right_input,
            left_bitmap,
            right_bitmap,
            ..
        } => {
            let left = JoinSide {
                alias: left_alias.as_deref(),
                collection: left_collection.as_str(),
            };
            let right = JoinSide {
                alias: right_alias.as_deref(),
                collection: right_collection.as_str(),
            };
            refuse_join_aggregates(ctx, &left, &right, post_aggregates)?;
            for child in [left_input, right_input, left_bitmap, right_bitmap]
                .into_iter()
                .flatten()
            {
                walk(child, ctx)?;
            }
            Ok(())
        }

        QueryOp::FacetCounts {
            collection, fields, ..
        } => refuse_facet_fields(ctx, collection, fields),

        QueryOp::LateralTopK { outer_plan, .. } | QueryOp::LateralLoop { outer_plan, .. } => {
            walk(outer_plan, ctx)
        }

        // The shuffle consumers merge partial state staged on this node by
        // producer plans (`PartialAggregateState` for the aggregate side,
        // per-side scans for the join side). Every producer went through this
        // pass on the Control Plane that dispatched it, so a consumer for a
        // redacted aggregate is never reached — and the staged frame files name
        // no collection to key a second check on.
        QueryOp::ShuffleAggregateConsume { .. } | QueryOp::ShuffleJoinConsume { .. } => Ok(()),

        // No aggregate, no embedded sub-plan: these return stored columns that
        // the result-path masking hook rewrites (`ProviderScan` returns
        // coordinator-materialized catalog or constant rows; the recursive and
        // non-hash join ops scan their named collections), so there is nothing
        // unredactable to refuse.
        QueryOp::ProviderScan { .. }
        | QueryOp::NestedLoopJoin { .. }
        | QueryOp::SortMergeJoin { .. }
        | QueryOp::RecursiveScan { .. }
        | QueryOp::RecursiveValue { .. } => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_physical::physical_plan::{
        AggregateSpec, DocumentOp, ExchangeMode, ExchangeOp, GraphOp, GroupKeySpec,
    };

    use crate::control::security::redaction::{
        RedactionMode, RedactionPolicy, RedactionRule, RedactionStore,
    };

    use super::*;

    const TENANT: u64 = 1;

    fn store_with_rule(collection: &str, role: &str, field: &str) -> RedactionStore {
        let store = RedactionStore::new();
        store.create_policy(RedactionPolicy {
            name: format!("{collection}_{role}_{field}"),
            tenant_id: TENANT,
            collection: collection.into(),
            display_collection: collection.into(),
            for_role: role.into(),
            rules: vec![RedactionRule {
                field: field.into(),
                mode: RedactionMode::Mask("***".into()),
            }],
        });
        store
    }

    /// An authenticated session holding exactly `role`.
    ///
    /// The role list is overwritten after construction because a redaction
    /// policy is keyed on an arbitrary role NAME, which the built-in `Role`
    /// enum cannot express.
    fn auth_with_role(role: &str) -> AuthContext {
        use crate::control::security::identity::{
            AuthMethod, AuthenticatedIdentity, DatabaseSet, Role,
        };

        let identity = AuthenticatedIdentity::new_regular(
            42,
            "alice",
            TenantId::new(TENANT),
            AuthMethod::ScramSha256,
            vec![Role::ReadWrite],
            None,
            DatabaseSet::Some(smallvec::smallvec![nodedb_types::id::DatabaseId::DEFAULT]),
        );
        let mut auth = AuthContext::from_identity(&identity, "s_test".into());
        auth.roles = vec![role.to_string()];
        auth
    }

    fn agg_spec(function: &str, field: &str) -> AggregateSpec {
        AggregateSpec {
            function: function.into(),
            alias: format!("{function}_{field}"),
            user_alias: None,
            field: field.into(),
            expr: None,
        }
    }

    fn aggregate_plan(collection: &str, specs: Vec<AggregateSpec>) -> PhysicalPlan {
        PhysicalPlan::Query(QueryOp::Aggregate {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                collection,
            ),
            input: None,
            group_by: Vec::<GroupKeySpec>::new(),
            aggregates: specs,
            filters: Vec::new(),
            having: Vec::new(),
            limit: 0,
            sub_group_by: Vec::new(),
            sub_aggregates: Vec::new(),
            grouping_sets: Vec::new(),
            sort_keys: Vec::new(),
        })
    }

    fn check(plan: &PhysicalPlan, store: &RedactionStore, role: &str) -> crate::Result<()> {
        refuse_unredactable_plan(
            plan,
            TenantId::new(TENANT),
            nodedb_types::id::DatabaseId::DEFAULT,
            &auth_with_role(role),
            store,
        )
    }

    fn assert_refused(result: crate::Result<()>) {
        match result {
            Err(crate::Error::PlanError { .. }) => {}
            other => panic!("expected PlanError refusal, got {other:?}"),
        }
    }

    /// `MIN(<redacted col>)` is computed over the stored values, so it is
    /// refused for a role the policy names.
    #[test]
    fn aggregate_over_redacted_column_is_refused() {
        let store = store_with_rule("users", "support", "ssn");
        let plan = aggregate_plan("users", vec![agg_spec("min", "ssn")]);
        assert_refused(check(&plan, &store, "support"));
    }

    /// The same aggregate for a role the policy does not name still runs.
    #[test]
    fn aggregate_is_allowed_for_a_role_without_the_rule() {
        let store = store_with_rule("users", "support", "ssn");
        let plan = aggregate_plan("users", vec![agg_spec("min", "ssn")]);
        assert!(check(&plan, &store, "analyst").is_ok());
    }

    /// The refusal is per column: a rule on `ssn` must not block `MAX(age)`.
    #[test]
    fn aggregate_over_an_unruled_column_is_allowed() {
        let store = store_with_rule("users", "support", "ssn");
        let plan = aggregate_plan("users", vec![agg_spec("max", "age")]);
        assert!(check(&plan, &store, "support").is_ok());
    }

    /// `COUNT(*)` reads no column value, so it is never refused.
    #[test]
    fn count_star_is_allowed_on_a_redacted_collection() {
        let store = store_with_rule("users", "support", "ssn");
        let plan = aggregate_plan("users", vec![agg_spec("count", "*")]);
        assert!(check(&plan, &store, "support").is_ok());
    }

    /// An aggregate whose argument is an expression still reads the column
    /// the expression references.
    #[test]
    fn aggregate_over_an_expression_reading_the_column_is_refused() {
        let store = store_with_rule("users", "support", "ssn");
        let mut spec = agg_spec("max", "*");
        spec.expr = Some(nodedb_query::expr::SqlExpr::Function {
            name: "length".into(),
            args: vec![nodedb_query::expr::SqlExpr::Column("ssn".into())],
        });
        let plan = aggregate_plan("users", vec![spec]);
        assert_refused(check(&plan, &store, "support"));
    }

    /// An aggregate buried under `Exchange` is still caught — the converter
    /// wraps every sharded aggregate before this pass runs.
    #[test]
    fn aggregate_under_exchange_is_refused() {
        let store = store_with_rule("users", "support", "ssn");
        let plan = PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            child: Box::new(aggregate_plan("users", vec![agg_spec("min", "ssn")])),
            mode: ExchangeMode::Gather { as_aggregate: true },
        }));
        assert_refused(check(&plan, &store, "support"));
    }

    /// …and under `PostProcess`, the subquery-body wrapper.
    #[test]
    fn aggregate_under_post_process_is_refused() {
        let store = store_with_rule("users", "support", "ssn");
        let plan = PhysicalPlan::Query(QueryOp::PostProcess {
            input: Box::new(aggregate_plan("users", vec![agg_spec("min", "ssn")])),
            filters: Vec::new(),
            projection: Vec::new(),
            computed_columns: Vec::new(),
            window_functions: Vec::new(),
            sort_keys: Vec::new(),
            limit: None,
            offset: 0,
            distinct: false,
        });
        assert_refused(check(&plan, &store, "support"));
    }

    fn hash_join(
        post_aggregates: Vec<(String, String)>,
        left_input: Option<PhysicalPlan>,
    ) -> PhysicalPlan {
        PhysicalPlan::Query(QueryOp::HashJoin {
            left_collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "users",
            ),
            right_collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "orders",
            ),
            left_alias: Some("u".into()),
            right_alias: Some("o".into()),
            on: Vec::new(),
            join_type: "inner".into(),
            limit: 0,
            post_group_by: Vec::new(),
            post_aggregates,
            projection: Vec::new(),
            computed_projection: Vec::new(),
            join_filters: Vec::new(),
            post_filters: Vec::new(),
            left_input: left_input.map(Box::new),
            right_input: None,
            left_bitmap: None,
            right_bitmap: None,
            left_rls_filters: Vec::new(),
            right_rls_filters: Vec::new(),
            left_scan_filters: Vec::new(),
            right_scan_filters: Vec::new(),
        })
    }

    /// An aggregate hidden in a join's child plan is walked into.
    #[test]
    fn aggregate_under_a_join_child_is_refused() {
        let store = store_with_rule("users", "support", "ssn");
        let plan = hash_join(
            Vec::new(),
            Some(aggregate_plan("users", vec![agg_spec("min", "ssn")])),
        );
        assert_refused(check(&plan, &store, "support"));
    }

    /// A post-join aggregate qualified with a side's alias resolves to that
    /// side's collection.
    #[test]
    fn qualified_post_join_aggregate_over_redacted_column_is_refused() {
        let store = store_with_rule("users", "support", "ssn");
        let plan = hash_join(vec![("min".into(), "u.ssn".into())], None);
        assert_refused(check(&plan, &store, "support"));
    }

    /// …and one naming the other side's unruled column still runs.
    #[test]
    fn qualified_post_join_aggregate_over_the_other_side_is_allowed() {
        let store = store_with_rule("users", "support", "ssn");
        let plan = hash_join(vec![("sum".into(), "o.total".into())], None);
        assert!(check(&plan, &store, "support").is_ok());
    }

    /// A traversal over a collection with a rule for the caller's role is
    /// refused: it returns topology, which redaction cannot rewrite.
    #[test]
    fn graph_traversal_over_redacted_collection_is_refused() {
        let store = store_with_rule("users", "support", "ssn");
        let plan = PhysicalPlan::Graph(GraphOp::Neighbors {
            collection: Some(nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "users",
            )),
            node_id: "n1".into(),
            edge_label: None,
            direction: nodedb_types::graph::Direction::Out,
            rls_filters: Vec::new(),
        });
        assert_refused(check(&plan, &store, "support"));
        assert!(check(&plan, &store, "analyst").is_ok());
    }

    /// A MATCH scoped to a collection with a rule is refused; the same pattern
    /// scoped to an unruled collection runs.
    #[test]
    fn graph_match_is_refused_per_scoped_collection() {
        use crate::engine::graph::pattern::ast::MatchQuery;

        let store = store_with_rule("users", "support", "ssn");
        let mut query = MatchQuery {
            clauses: Vec::new(),
            where_predicates: Vec::new(),
            return_columns: Vec::new(),
            distinct: false,
            limit: None,
            order_by: Vec::new(),
            collection: Some("users".into()),
        };
        let refused = PhysicalPlan::Graph(GraphOp::Match {
            query: zerompk::to_msgpack_vec(&query).expect("encode match query"),
            frontier_bitmap: None,
            cluster_mode: false,
        });
        assert_refused(check(&refused, &store, "support"));

        query.collection = Some("orders".into());
        let allowed = PhysicalPlan::Graph(GraphOp::Match {
            query: zerompk::to_msgpack_vec(&query).expect("encode match query"),
            frontier_bitmap: None,
            cluster_mode: false,
        });
        assert!(check(&allowed, &store, "support").is_ok());
    }

    /// A MATCH naming no collection may traverse anything the tenant holds, so
    /// it is refused whenever the role holds a rule — and allowed when it
    /// holds none.
    #[test]
    fn unscoped_graph_match_falls_back_to_the_tenant_wide_question() {
        use crate::engine::graph::pattern::ast::MatchQuery;

        let store = store_with_rule("users", "support", "ssn");
        let query = MatchQuery {
            clauses: Vec::new(),
            where_predicates: Vec::new(),
            return_columns: Vec::new(),
            distinct: false,
            limit: None,
            order_by: Vec::new(),
            collection: None,
        };
        let plan = PhysicalPlan::Graph(GraphOp::Match {
            query: zerompk::to_msgpack_vec(&query).expect("encode match query"),
            frontier_bitmap: None,
            cluster_mode: false,
        });
        assert_refused(check(&plan, &store, "support"));
        assert!(check(&plan, &store, "analyst").is_ok());
    }

    /// A plan with no aggregate and no graph read is untouched — the ordinary
    /// SELECT path stays on the masking hook.
    #[test]
    fn a_plain_scan_is_never_refused() {
        let store = store_with_rule("users", "support", "ssn");
        let plan = PhysicalPlan::Document(DocumentOp::EstimateCount {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "users",
            ),
            field: "id".into(),
        });
        assert!(check(&plan, &store, "support").is_ok());
    }
}
