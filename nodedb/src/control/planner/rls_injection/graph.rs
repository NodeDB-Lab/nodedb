// SPDX-License-Identifier: BUSL-1.1

//! RLS resolution for graph-overlay operations.
//!
//! No graph read carries a row-filter slot: traversal returns topology,
//! algorithms return per-node scalars, pattern match returns bindings, RAG
//! fusion returns fused rows. Each refuses while a read policy restricts
//! the collection, rather than return rows the policy hides — including
//! algorithm/stats/RAG-fusion shapes the redaction pass permits, since RLS
//! restricts the row set itself, not only column values.

use nodedb_physical::physical_plan::GraphOp;

use super::context::RlsCtx;

const TRAVERSAL_REASON: &str =
    "a traversal returns graph topology, which the row filter cannot be evaluated against";

const EDGE_BATCH_REASON: &str = "a batched edge write is applied with empty properties, so it carries no row image for the \
     policy to be evaluated against";

const ALGORITHM_REASON: &str = "an algorithm returns per-node scalars computed over every edge, which the row filter cannot \
     be evaluated against";

/// Exhaustive over [`GraphOp`] so a new graph operation forces a decision
/// between injecting, refusing, and no-op.
pub(super) fn inject_graph(ctx: &RlsCtx<'_>, op: &mut GraphOp) -> crate::Result<()> {
    match op {
        // Refuse: discloses topology, not row bodies. `None` is a
        // tree-index walk with no collection mapping — no policy to consult.
        GraphOp::Hop { collection, .. }
        | GraphOp::Neighbors { collection, .. }
        | GraphOp::NeighborsMulti { collection, .. }
        | GraphOp::Path { collection, .. }
        | GraphOp::Subgraph { collection, .. } => match collection.as_ref() {
            Some(collection) => ctx.refuse_if_policy(collection, TRAVERSAL_REASON),
            None => Ok(()),
        },

        // Refuse: same shape as `Neighbors`; always names its collection.
        GraphOp::TemporalNeighbors { collection, .. } => {
            ctx.refuse_if_policy(collection, TRAVERSAL_REASON)
        }

        // Refuse: returns bindings with no filter slot; its own `WHERE`
        // can probe a hidden row's field one predicate at a time.
        GraphOp::Match { query, .. }
        | GraphOp::MatchContinuation { query, .. }
        | GraphOp::MatchVarLenResume { query, .. } => refuse_match(ctx, query),

        // Refuse: runs over the whole CSR, returns scalars derived from
        // every row (including hidden ones), no row to filter. `params.collection`
        // is bare (from `nodedb-graph`) — qualify before the lookup.
        GraphOp::Algo { params, .. } | GraphOp::TemporalAlgorithm { params, .. } => ctx
            .refuse_if_policy(
                &nodedb_types::QualifiedCollection::new(ctx.database_id, &params.collection),
                ALGORITHM_REASON,
            ),

        // Refuse: same algorithm, one round at a time.
        GraphOp::BspSuperstep(plan) => ctx.refuse_if_policy(
            &nodedb_types::QualifiedCollection::new(ctx.database_id, &plan.params.collection),
            ALGORITHM_REASON,
        ),
        GraphOp::WccSuperstep(plan) => ctx.refuse_if_policy(
            &nodedb_types::QualifiedCollection::new(ctx.database_id, &plan.params.collection),
            ALGORITHM_REASON,
        ),

        // Refuse: the fusion envelope has no filter slot and no sub-plan
        // to recurse into — hidden rows will be ranked and returned.
        GraphOp::RagFusion { collection, .. } => ctx.refuse_if_policy(
            collection,
            "fusion returns ranked document rows through a fused response shape that carries no \
             row filter",
        ),

        // Refuse: counters over the collection's edges, no row to filter.
        // `None` spans every collection, so no narrow question can be asked.
        GraphOp::Stats { collection, .. } => match collection.as_ref() {
            Some(collection) => ctx.refuse_if_policy(
                collection,
                "graph statistics are counters over the collection's edges, which the row filter \
                 cannot be evaluated against",
            ),
            None => ctx.refuse_if_any_policy(
                "graph statistics report counters for every collection holding edges, which the \
                 row filter cannot be evaluated against",
            ),
        },

        // Admit: `PROPERTIES` is the post-image, decided at plan time like a
        // document insert. No `PROPERTIES` = no field to test = denied.
        //
        // The mirrored `_from`/`_to` edge is NOT decided here: appended
        // after this pass, its source document write already admitted it.
        GraphOp::EdgePut {
            collection,
            properties,
            ..
        } => ctx.admit_write_property_image(collection, properties),

        // Ship the predicate: a delete carries no image; the stored property
        // object is readable only where the tombstone is written.
        GraphOp::EdgeDelete {
            collection,
            rls_write_check,
            ..
        } => ctx.set_write_check(collection, rls_write_check),

        // Recurse: the wrapped delete's own slot is the one the policy fills.
        GraphOp::ResolveEdgeDelete(inner) => inject_graph(ctx, inner),

        // Refuse: every edge in a batch applies with empty properties, so
        // there's no image to evaluate even though each names its collection.
        GraphOp::EdgePutBatch { .. } | GraphOp::EdgeDeleteBatch { .. } => {
            ctx.refuse_if_any_write_policy(EDGE_BATCH_REASON)
        }
        GraphOp::SetNodeLabels { .. } | GraphOp::RemoveNodeLabels { .. } => ctx
            .refuse_if_any_write_policy(
                "a node-label write is keyed on a node id that names no collection, and it carries \
                 no row body for the policy to be evaluated against",
            ),

        // Admit: these derive from a document delete or TRUNCATE the policy
        // on the same collection already decided. The guards write nothing,
        // and each edge a TRUNCATE removes goes with the rows it removes.
        // A delete's planner reads which ids are stored, for that delete.
        GraphOp::NodeEdgeGuard { .. }
        | GraphOp::NodePresenceGuard { .. }
        | GraphOp::TruncateEdges { .. }
        | GraphOp::NodePresenceRead { .. } => Ok(()),
    }
}

/// Refuse a pattern match whose target collection carries a read policy.
/// A query naming no collection, or one that fails to decode, falls back
/// to the tenant-wide question.
fn refuse_match(ctx: &RlsCtx<'_>, query: &[u8]) -> crate::Result<()> {
    let decoded: Result<crate::engine::graph::pattern::ast::MatchQuery, _> =
        zerompk::from_msgpack(query);
    match decoded.ok().and_then(|query| query.collection) {
        Some(collection) => ctx.refuse_if_policy(
            &nodedb_types::QualifiedCollection::new(ctx.database_id, &collection),
            "a pattern match returns bindings over graph topology, which the row filter cannot be \
             evaluated against",
        ),
        None => ctx.refuse_if_any_policy(
            "a pattern match returns bindings over graph topology, which the row filter cannot be \
             evaluated against, and the pattern's scope cannot be narrowed to an unrestricted \
             collection",
        ),
    }
}

#[cfg(test)]
mod tests {
    use nodedb_graph::{AlgoParams, GraphAlgorithm};
    use nodedb_physical::physical_plan::GraphOp;

    use super::super::plan::test_support::{
        assert_refused, inject, inject_without_policy, store_with_read_policy,
        store_with_write_policy,
    };
    use crate::bridge::envelope::PhysicalPlan;
    use crate::engine::graph::pattern::ast::MatchQuery;

    fn algo_plan(collection: &str) -> PhysicalPlan {
        PhysicalPlan::Graph(GraphOp::Algo {
            algorithm: GraphAlgorithm::PageRank,
            params: AlgoParams {
                collection: collection.into(),
                edge_label: None,
                damping: None,
                max_iterations: None,
                tolerance: None,
                source_node: None,
                sample_size: None,
                direction: None,
                resolution: None,
                mode: None,
                personalization_vector: None,
            },
            stage: nodedb_physical::physical_plan::AlgoStage::Local,
        })
    }

    fn match_plan(collection: Option<&str>) -> PhysicalPlan {
        let query = MatchQuery {
            clauses: Vec::new(),
            where_predicates: Vec::new(),
            return_columns: Vec::new(),
            distinct: false,
            limit: None,
            order_by: Vec::new(),
            collection: collection.map(str::to_string),
        };
        PhysicalPlan::Graph(GraphOp::Match {
            query: zerompk::to_msgpack_vec(&query).expect("encode match query"),
            frontier_bitmap: None,
            cluster_mode: false,
        })
    }

    /// A pattern match scoped to a policed collection is refused.
    #[test]
    fn scoped_match_is_refused_under_a_read_policy() {
        let store = store_with_read_policy("users");
        let mut plan = match_plan(Some("users"));
        assert_refused(inject(&mut plan, &store), "users");
    }

    /// …and the same pattern scoped elsewhere still runs.
    #[test]
    fn match_on_an_unpoliced_collection_runs() {
        let store = store_with_read_policy("users");
        let mut plan = match_plan(Some("orders"));
        assert!(inject(&mut plan, &store).is_ok());
    }

    /// An unscoped match can traverse anything the tenant holds, so it falls
    /// back to the tenant-wide question.
    #[test]
    fn unscoped_match_falls_back_to_the_tenant_wide_question() {
        let store = store_with_read_policy("users");
        let mut plan = match_plan(None);
        assert!(matches!(
            inject(&mut plan, &store),
            Err(crate::Error::PlanError { .. })
        ));
    }

    /// With no policy at all, both match shapes run untouched.
    #[test]
    fn match_without_a_policy_is_untouched() {
        for collection in [Some("users"), None] {
            let mut plan = match_plan(collection);
            let before = plan.clone();
            assert!(inject_without_policy(&mut plan).is_ok());
            assert_eq!(plan, before);
        }
    }

    /// An `EdgePut` whose properties are `properties_json` stored as plain
    /// MessagePack. An empty string stores no properties.
    fn edge_put(collection: &str, properties_json: &str) -> PhysicalPlan {
        let properties = if properties_json.is_empty() {
            Vec::new()
        } else {
            let json: serde_json::Value =
                sonic_rs::from_str(properties_json).expect("test properties are JSON");
            nodedb_types::json_msgpack::json_to_msgpack(&json).expect("encode properties")
        };
        PhysicalPlan::Graph(GraphOp::EdgePut {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                collection,
            ),
            src_id: "a".into(),
            label: "knows".into(),
            dst_id: "b".into(),
            properties,
            src_surrogate: nodedb_types::Surrogate::new(1),
            dst_surrogate: nodedb_types::Surrogate::new(2),
        })
    }

    fn edge_delete(collection: &str) -> PhysicalPlan {
        PhysicalPlan::Graph(GraphOp::EdgeDelete {
            collection: nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                collection,
            ),
            src_id: "a".into(),
            label: "knows".into(),
            dst_id: "b".into(),
            src_surrogate: nodedb_types::Surrogate::new(1),
            dst_surrogate: nodedb_types::Surrogate::new(2),
            rls_write_check: nodedb_types::RlsWriteCheck::pending_injection(),
        })
    }

    /// The `PROPERTIES` clause is the edge's row image, so a conforming one is
    /// admitted at plan time.
    #[test]
    fn conforming_edge_put_is_admitted() {
        let store = store_with_write_policy("users");
        let mut plan = edge_put("users", r#"{"owner_id":"42"}"#);
        assert!(inject(&mut plan, &store).is_ok());
    }

    /// …and one whose properties violate the policy fails the statement rather
    /// than being persisted unchecked.
    #[test]
    fn violating_edge_put_is_rejected() {
        let store = store_with_write_policy("users");
        let mut plan = edge_put("users", r#"{"owner_id":"99"}"#);
        assert!(matches!(
            inject(&mut plan, &store),
            Err(crate::Error::RejectedAuthz { .. })
        ));
    }

    /// An edge with no `PROPERTIES` carries no field the predicate can test, so
    /// it is denied rather than admitted by omission.
    #[test]
    fn edge_put_without_properties_is_denied_under_a_write_policy() {
        let store = store_with_write_policy("users");
        let mut plan = edge_put("users", "");
        assert!(matches!(
            inject(&mut plan, &store),
            Err(crate::Error::RejectedAuthz { .. })
        ));
    }

    /// With no write policy the same edge insert runs untouched, whatever its
    /// properties are.
    #[test]
    fn edge_put_without_a_policy_is_untouched() {
        let mut plan = edge_put("users", "");
        let before = plan.clone();
        assert!(inject_without_policy(&mut plan).is_ok());
        assert_eq!(plan, before);
    }

    /// A delete's image is the stored property object, so the compiled
    /// predicate ships to the Data Plane instead of the plan being refused.
    #[test]
    fn edge_delete_carries_the_write_check() {
        let store = store_with_write_policy("users");
        let mut plan = edge_delete("users");
        assert!(inject(&mut plan, &store).is_ok());
        match &plan {
            PhysicalPlan::Graph(GraphOp::EdgeDelete {
                rls_write_check, ..
            }) => assert!(
                rls_write_check.has_predicate(),
                "a governed edge delete must ship the compiled predicate"
            ),
            other => panic!("expected an EdgeDelete plan, got {other:?}"),
        }
    }

    /// …and an ungoverned collection ships an empty check, which admits
    /// everything and costs the Data Plane no pre-image read.
    #[test]
    fn edge_delete_without_a_policy_records_that_injection_ran() {
        let mut plan = edge_delete("users");
        let before = plan.clone();
        assert!(inject_without_policy(&mut plan).is_ok());

        // Stamps `NoPolicyApplies` so the gate can tell the pass ran.
        assert_eq!(
            plan.rls_write_checks(),
            vec![&nodedb_types::RlsWriteCheck::NoPolicyApplies]
        );

        let mut normalized = plan.clone();
        super::super::plan::test_support::reset_write_check(&mut normalized);
        assert_eq!(normalized, before, "nothing else may change");
    }

    /// A graph algorithm runs over every edge of the collection.
    #[test]
    fn algo_is_refused_under_a_read_policy() {
        let store = store_with_read_policy("users");
        let mut plan = algo_plan("users");
        assert_refused(inject(&mut plan, &store), "users");
    }

    /// …and is untouched when no policy applies.
    #[test]
    fn algo_without_a_policy_is_untouched() {
        let mut plan = algo_plan("users");
        let before = plan.clone();
        assert!(inject_without_policy(&mut plan).is_ok());
        assert_eq!(plan, before);
    }

    /// Collection-scoped graph stats count edges of rows the policy hides.
    #[test]
    fn scoped_stats_is_refused_under_a_read_policy() {
        let store = store_with_read_policy("users");
        let mut plan = PhysicalPlan::Graph(GraphOp::Stats {
            collection: Some(nodedb_types::QualifiedCollection::new(
                nodedb_types::DatabaseId::DEFAULT,
                "users",
            )),
            as_of: None,
        });
        assert_refused(inject(&mut plan, &store), "users");
    }

    /// Tenant-wide stats cannot be narrowed, so any read policy refuses them.
    #[test]
    fn unscoped_stats_is_refused_while_any_policy_applies() {
        let store = store_with_read_policy("users");
        let mut plan = PhysicalPlan::Graph(GraphOp::Stats {
            collection: None,
            as_of: None,
        });
        assert!(matches!(
            inject(&mut plan, &store),
            Err(crate::Error::PlanError { .. })
        ));
    }

    /// …and run normally when the tenant has no read policy.
    #[test]
    fn unscoped_stats_without_a_policy_is_untouched() {
        let mut plan = PhysicalPlan::Graph(GraphOp::Stats {
            collection: None,
            as_of: None,
        });
        let before = plan.clone();
        assert!(inject_without_policy(&mut plan).is_ok());
        assert_eq!(plan, before);
    }
}
