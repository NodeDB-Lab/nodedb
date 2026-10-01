// SPDX-License-Identifier: Apache-2.0

//! SELECT-body search triggers and post-processing order.

use crate::error::Result;
use crate::functions::registry::FunctionRegistry;
use crate::planner::select::limit::apply_limit;
use crate::planner::select::order_by::{apply_order_by, try_hybrid_from_projection};
use crate::planner::select::query_tail::QueryTail;
use crate::planner::select::select_stmt::{has_aggregation, plan_select};
use crate::temporal::TemporalScope;
use crate::types::{Projection, SqlCatalog, SqlPlan};
use sqlparser::ast::{Query, Select};

pub(super) fn plan_select_query(
    query: &Query,
    select: &Select,
    catalog: &dyn SqlCatalog,
    functions: &FunctionRegistry,
    temporal: TemporalScope,
    statement_output: bool,
) -> Result<SqlPlan> {
    // ORDER BY / LIMIT belong to the query, not to its SELECT body,
    // but the scan planner needs them to pick an access path that can
    // honour them — so they travel down with the SELECT.
    let tail = QueryTail {
        order_by: query.order_by.as_ref(),
        limit_clause: &query.limit_clause,
        fetch: query.fetch.as_ref(),
    };
    let planned = plan_select(
        select,
        catalog,
        functions,
        temporal,
        &tail,
        statement_output,
    )?;
    let scope = planned.scope;
    let mut plan = planned.plan;
    // Snapshot the projection before ORDER BY transforms the plan,
    // in case `apply_order_by` converts a Scan into VectorSearch.
    let pre_order_by_projection: Option<Vec<Projection>> = match &plan {
        SqlPlan::Scan { projection, .. } => Some(projection.clone()),
        _ => None,
    };
    let pre_order_by_collection: Option<String> = match &plan {
        SqlPlan::Scan { collection, .. } => Some(collection.clone()),
        _ => None,
    };
    if let Some(order_by) = &query.order_by {
        plan = apply_order_by(&plan, order_by, functions, &select.projection, &scope)?;
    }
    // Fall back to a SELECT-projection scan for hybrid-search and
    // text-search triggers. The `SELECT id, rrf_score(...) AS score
    // FROM c WHERE ... LIMIT N` shape has no ORDER BY, so
    // `apply_order_by` cannot fire. The same applies to
    // `SELECT id, bm25_score(field, term) FROM c ORDER BY id` where
    // ORDER BY does not contain a search trigger.
    //
    // Also fires when the plan is already `TextSearch` (set by the
    // WHERE `text_match(...)` path) and the SELECT list additionally
    // contains `bm25_score(...)` — in that case we attach the
    // `score_alias` so the executor knows to inject the score column.
    //
    // `apply_order_by` may have wrapped a search plan in a
    // post-processing tail to carry the sort, so the upgrade inspects
    // the body and is re-wrapped in place — otherwise the score column
    // the SELECT list asked for would never be attached.
    let upgrade = {
        let leaf = match &plan {
            SqlPlan::Subquery { input, .. } => input.as_ref(),
            other => other,
        };
        if matches!(leaf, SqlPlan::Scan { .. } | SqlPlan::TextSearch { .. }) {
            try_hybrid_from_projection(leaf, &select.projection, functions)?
        } else {
            None
        }
    };
    if let Some(upgraded_leaf) = upgrade {
        plan = match plan {
            SqlPlan::Subquery {
                filters,
                projection,
                window_functions,
                sort_keys,
                offset,
                distinct,
                limit,
                ..
            } => SqlPlan::Subquery {
                input: Box::new(upgraded_leaf),
                filters,
                projection,
                window_functions,
                sort_keys,
                offset,
                distinct,
                limit,
            },
            _ => upgraded_leaf,
        };
    }
    super::payload::apply_vector_payload(
        &mut plan,
        catalog,
        pre_order_by_projection.as_deref(),
        pre_order_by_collection.as_deref(),
    )?;
    let plan = apply_limit(plan, &tail)?;
    // ORDER BY and LIMIT sit on the aggregate by now, so the wrap
    // only restates the output columns around it.
    crate::planner::aggregate_cp_wrap::wrap_aggregate_cp_items(
        plan,
        &select.projection,
        has_aggregation(select, functions),
        functions,
        &scope,
    )
}

#[cfg(test)]
mod tests {
    use super::super::fixtures::plan_select_sql;
    use crate::types::*;
    #[test]
    fn order_by_vector_distance_with_array_join_fuses_into_vector_search() {
        let plan = plan_select_sql(
            "SELECT v.id FROM embeddings v \
             JOIN ARRAY_SLICE('genome', '{chrom: [1, 1], pos: [0, 50000]}') AS s \
               ON v.id = s.qual \
             ORDER BY vector_distance(v.embedding, [1.0, 0.0, 0.0]) \
             LIMIT 10",
        );

        let SqlPlan::VectorSearch {
            collection,
            top_k,
            array_prefilter,
            ..
        } = plan
        else {
            panic!("expected fused VectorSearch plan");
        };
        assert_eq!(collection, "embeddings");
        assert_eq!(top_k, 10);
        let prefilter = array_prefilter.expect("array_prefilter must be set on fused plan");
        assert_eq!(prefilter.array_name, "genome");
        assert_eq!(prefilter.slice.dim_ranges.len(), 2);
    }

    #[test]
    fn vector_distance_two_args_produces_default_ann_options() {
        let plan = plan_select_sql(
            "SELECT id FROM embeddings ORDER BY vector_distance(embedding, [1.0, 0.0]) LIMIT 5",
        );
        let SqlPlan::VectorSearch { ann_options, .. } = plan else {
            panic!("expected VectorSearch plan");
        };
        assert_eq!(ann_options, VectorAnnOptions::default());
    }

    #[test]
    fn order_by_sparse_score_desc_routes_to_sparse_search() {
        // `ORDER BY sparse_score(field, '{dim: weight, ...}') DESC LIMIT k` must
        // route to `SqlPlan::SparseSearch` exactly as `vector_distance(...)` routes
        // to `SqlPlan::VectorSearch`. The query literal is parsed into sorted
        // `(dimension, weight)` entries and `top_k` tracks the LIMIT.
        let plan = plan_select_sql(
            "SELECT id FROM embeddings \
             ORDER BY sparse_score(terms, '{3: 1.0, 7: 0.5}') DESC LIMIT 5",
        );
        let SqlPlan::SparseSearch {
            collection,
            field,
            query_entries,
            top_k,
            ..
        } = plan
        else {
            panic!("expected SparseSearch plan");
        };
        assert_eq!(collection, "embeddings");
        assert_eq!(field, "terms");
        assert_eq!(top_k, 5);
        assert_eq!(query_entries, vec![(3, 1.0), (7, 0.5)]);
    }

    #[test]
    fn vector_distance_named_args_parses_ann_options() {
        let plan = plan_select_sql(
            "SELECT id FROM embeddings ORDER BY vector_distance(embedding, [1.0, 0.0], quantization => 'rabitq', oversample => 3) LIMIT 5",
        );
        let SqlPlan::VectorSearch {
            ann_options,
            ef_search,
            top_k,
            ..
        } = plan
        else {
            panic!("expected VectorSearch plan");
        };
        assert_eq!(ann_options.quantization, Some(VectorQuantization::RaBitQ));
        assert_eq!(ann_options.oversample, Some(3));
        // ef_search falls back to top_k * 2 (no ef_search_override supplied).
        assert_eq!(ef_search, top_k * 2);
    }

    #[test]
    fn vector_distance_ef_search_override_applied() {
        let plan = plan_select_sql(
            "SELECT id FROM embeddings ORDER BY vector_distance(embedding, [1.0], ef_search => 150) LIMIT 5",
        );
        let SqlPlan::VectorSearch { ef_search, .. } = plan else {
            panic!("expected VectorSearch plan");
        };
        assert_eq!(ef_search, 150);
    }

    #[test]
    fn arrow_distance_operator_yields_l2_metric() {
        // The <-> operator rewrites to vector_distance(...) via the preprocessor.
        // Use the function form here since sqlparser handles bracket-array syntax.
        let plan = plan_select_sql(
            "SELECT id FROM embeddings ORDER BY vector_distance(embedding, [1.0, 0.0, 0.0]) LIMIT 5",
        );
        let SqlPlan::VectorSearch { metric, .. } = plan else {
            panic!("expected VectorSearch plan");
        };
        assert_eq!(metric, DistanceMetric::L2);
    }

    #[test]
    fn cosine_distance_operator_yields_cosine_metric() {
        // The <=> operator rewrites to vector_cosine_distance(...).
        let plan = plan_select_sql(
            "SELECT id FROM embeddings ORDER BY vector_cosine_distance(embedding, [1.0, 0.0, 0.0]) LIMIT 5",
        );
        let SqlPlan::VectorSearch { metric, .. } = plan else {
            panic!("expected VectorSearch plan");
        };
        assert_eq!(metric, DistanceMetric::Cosine);
    }

    #[test]
    fn neg_inner_product_operator_yields_inner_product_metric() {
        // The <#> operator rewrites to vector_neg_inner_product(...).
        let plan = plan_select_sql(
            "SELECT id FROM embeddings ORDER BY vector_neg_inner_product(embedding, [1.0, 0.0, 0.0]) LIMIT 5",
        );
        let SqlPlan::VectorSearch { metric, .. } = plan else {
            panic!("expected VectorSearch plan");
        };
        assert_eq!(metric, DistanceMetric::InnerProduct);
    }

    #[test]
    fn vector_distance_function_yields_l2_metric() {
        let plan = plan_select_sql(
            "SELECT id FROM embeddings ORDER BY vector_distance(embedding, [1.0, 0.0]) LIMIT 5",
        );
        let SqlPlan::VectorSearch { metric, .. } = plan else {
            panic!("expected VectorSearch plan");
        };
        assert_eq!(metric, DistanceMetric::L2);
    }

    #[test]
    fn vector_cosine_distance_function_yields_cosine_metric() {
        let plan = plan_select_sql(
            "SELECT id FROM embeddings ORDER BY vector_cosine_distance(embedding, [1.0, 0.0]) LIMIT 5",
        );
        let SqlPlan::VectorSearch { metric, .. } = plan else {
            panic!("expected VectorSearch plan");
        };
        assert_eq!(metric, DistanceMetric::Cosine);
    }

    #[test]
    fn vector_neg_inner_product_function_yields_inner_product_metric() {
        let plan = plan_select_sql(
            "SELECT id FROM embeddings ORDER BY vector_neg_inner_product(embedding, [1.0, 0.0]) LIMIT 5",
        );
        let SqlPlan::VectorSearch { metric, .. } = plan else {
            panic!("expected VectorSearch plan");
        };
        assert_eq!(metric, DistanceMetric::InnerProduct);
    }
}
