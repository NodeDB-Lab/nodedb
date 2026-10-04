// SPDX-License-Identifier: Apache-2.0

//! SELECT-body search triggers and post-processing order.

use super::payload::apply_vector_payload;
use super::pk_prefilter::apply_vector_pk_prefilter;
use crate::error::Result;
use crate::functions::registry::FunctionRegistry;
use crate::planner::aggregate_cp_wrap::wrap_aggregate_cp_items;
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
    // contains `bm25_score(...)` — in that case each call joins the
    // plan as a score column the executor injects.
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
        match scope.single_table() {
            Some(table) if matches!(leaf, SqlPlan::Scan { .. } | SqlPlan::TextSearch(_)) => {
                try_hybrid_from_projection(leaf, &select.projection, functions, table)?
            }
            _ => None,
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
    apply_vector_pk_prefilter(&mut plan, catalog)?;
    apply_vector_payload(
        &mut plan,
        catalog,
        pre_order_by_projection.as_deref(),
        pre_order_by_collection.as_deref(),
    )?;
    let plan = apply_limit(plan, &tail)?;
    // ORDER BY and LIMIT sit on the aggregate by now, so the wrap
    // only restates the output columns around it.
    wrap_aggregate_cp_items(
        plan,
        &select.projection,
        has_aggregation(select, functions),
        functions,
        &scope,
    )
}

#[cfg(test)]
mod tests {
    use super::super::fixtures::{plan_select_sql, try_plan_select_sql};
    use crate::error::SqlError;
    use crate::types::*;
    use nodedb_types::text_search::{QueryMode, TextColumnFault, TextSearchParams};
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
    fn a_lone_column_argument_matches_no_vector_distance_signature() {
        let err = try_plan_select_sql(
            "SELECT id FROM embeddings ORDER BY vector_distance(embedding) LIMIT 5",
        )
        .unwrap_err();
        assert_eq!(
            err,
            SqlError::UndefinedFunction {
                name: "vector_distance".into()
            }
        );
        // The one-argument query-vector form still plans a search.
        let plan = plan_select_sql(
            "SELECT id FROM embeddings ORDER BY vector_distance(ARRAY[1.0, 0.0]) LIMIT 5",
        );
        assert!(matches!(plan, SqlPlan::VectorSearch { .. }));
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

    /// The text plan of `plan`, under at most one post-processing tail.
    fn text_plan(plan: &SqlPlan) -> &TextSearchPlan {
        match plan {
            SqlPlan::TextSearch(search) => search,
            SqlPlan::Subquery { input, .. } => match input.as_ref() {
                SqlPlan::TextSearch(search) => search,
                other => panic!("expected TextSearch under the tail, got {other:?}"),
            },
            other => panic!("expected TextSearch, got {other:?}"),
        }
    }

    /// The field and top-k of a `Match` shape.
    fn match_shape(search: &TextSearchPlan) -> (Option<&str>, Option<usize>) {
        match &search.shape {
            TextSearchShape::Match { field, top_k, .. } => (field.as_deref(), *top_k),
            TextSearchShape::ScoreScan => panic!("expected a Match shape"),
        }
    }

    #[test]
    fn where_text_match_scopes_to_the_column_and_returns_every_match() {
        let plan = plan_select_sql("SELECT id FROM docs WHERE text_match(title, 'rust')");
        let search = text_plan(&plan);
        assert_eq!(match_shape(search), (Some("title"), None));
        assert!(search.scores.is_empty());
        assert!(search.filters.is_empty());
    }

    #[test]
    fn where_text_match_star_reads_the_whole_document() {
        let plan = plan_select_sql("SELECT id FROM docs WHERE text_match(*, 'rust')");
        assert_eq!(match_shape(text_plan(&plan)), (None, None));
        let plan = plan_select_sql("SELECT id FROM docs WHERE text_match(docs.*, 'rust')");
        assert_eq!(match_shape(text_plan(&plan)), (None, None));
    }

    #[test]
    fn where_text_match_limit_is_the_top_k() {
        let plan = plan_select_sql("SELECT id FROM docs WHERE text_match(body, 'rust') LIMIT 3");
        assert_eq!(match_shape(text_plan(&plan)), (Some("body"), Some(3)));
    }

    #[test]
    fn a_score_beside_a_match_keeps_the_match_shape() {
        let plan = plan_select_sql(
            "SELECT id, bm25_score(title, 'x') AS s FROM docs WHERE text_match(body, 'y')",
        );
        let search = text_plan(&plan);
        assert_eq!(match_shape(search), (Some("body"), None));
        assert_eq!(
            search.scores,
            vec![TextScoreColumn {
                field: Some("title".into()),
                query: "x".into(),
                mode: QueryMode::Or,
                fuzzy: false,
                alias: "s".into(),
            }]
        );
    }

    #[test]
    fn every_sibling_predicate_restricts_the_match() {
        let plan = plan_select_sql(
            "SELECT id FROM docs WHERE text_match(body, 'w') AND tag = 'a' AND n > 1 LIMIT 3",
        );
        let search = text_plan(&plan);
        assert_eq!(match_shape(search), (Some("body"), Some(3)));
        assert_eq!(search.filters.len(), 2);
    }

    #[test]
    fn a_score_without_a_match_scans_the_filtered_rows_in_order() {
        let plan = plan_select_sql(
            "SELECT id, bm25_score(body, 'x') FROM docs WHERE tag = 'a' ORDER BY id LIMIT 2",
        );
        let SqlPlan::Subquery {
            sort_keys, limit, ..
        } = &plan
        else {
            panic!("expected a post-processing tail, got {plan:?}");
        };
        assert_eq!(*limit, Some(2));
        assert_eq!(sort_keys.len(), 1);
        let search = text_plan(&plan);
        assert!(matches!(search.shape, TextSearchShape::ScoreScan));
        assert_eq!(search.filters.len(), 1);
        assert_eq!(search.scores.len(), 1);
        assert_eq!(search.scores[0].field.as_deref(), Some("body"));
    }

    #[test]
    fn order_by_score_sorts_by_the_score_column() {
        let plan =
            plan_select_sql("SELECT id FROM docs ORDER BY bm25_score(body, 'x') DESC LIMIT 5");
        let SqlPlan::Subquery {
            sort_keys, limit, ..
        } = &plan
        else {
            panic!("expected a post-processing tail, got {plan:?}");
        };
        assert_eq!(*limit, Some(5));
        let search = text_plan(&plan);
        assert!(matches!(search.shape, TextSearchShape::ScoreScan));
        let alias = &search.scores[0].alias;
        assert!(matches!(
            &sort_keys[0].expr,
            SqlExpr::Column { table: None, name } if name == alias
        ));
        assert!(!sort_keys[0].ascending);
        // A null score takes the default NULL placement of a DESC key: first.
        assert!(sort_keys[0].nulls_first);
    }

    #[test]
    fn order_by_score_honors_an_explicit_nulls_clause() {
        let plan = plan_select_sql(
            "SELECT id FROM docs ORDER BY bm25_score(body, 'x') DESC NULLS LAST LIMIT 5",
        );
        let SqlPlan::Subquery { sort_keys, .. } = &plan else {
            panic!("expected a post-processing tail, got {plan:?}");
        };
        assert!(!sort_keys[0].ascending);
        assert!(!sort_keys[0].nulls_first);

        let plan = plan_select_sql("SELECT id FROM docs ORDER BY bm25_score(body, 'x') LIMIT 5");
        let SqlPlan::Subquery { sort_keys, .. } = &plan else {
            panic!("expected a post-processing tail, got {plan:?}");
        };
        assert!(sort_keys[0].ascending);
        assert!(!sort_keys[0].nulls_first);
    }

    #[test]
    fn order_by_score_alias_reuses_the_select_alias() {
        let plan = plan_select_sql(
            "SELECT id, bm25_score(body, 'x') AS score FROM docs \
             WHERE text_match(body, 'x') ORDER BY score DESC LIMIT 20",
        );
        let search = text_plan(&plan);
        assert_eq!(match_shape(search), (Some("body"), None));
        assert_eq!(search.scores.len(), 1);
        assert_eq!(search.scores[0].alias, "score");
    }

    #[test]
    fn order_by_qualifier_of_the_table_names_its_column() {
        let plan = plan_select_sql("SELECT d.id FROM docs d ORDER BY bm25_score(d.body, 'x')");
        assert_eq!(text_plan(&plan).scores[0].field.as_deref(), Some("body"));
    }

    #[test]
    fn order_by_vector_qualifier_of_the_table_names_its_column() {
        let plan = plan_select_sql(
            "SELECT e.id FROM embeddings e ORDER BY vector_distance(e.embedding, [1.0, 0.0]) LIMIT 5",
        );
        let SqlPlan::VectorSearch { field, .. } = plan else {
            panic!("expected VectorSearch plan");
        };
        assert_eq!(field, "embedding");
    }

    #[test]
    fn order_by_foreign_qualifier_is_an_unknown_table() {
        let err = try_plan_select_sql("SELECT id FROM docs ORDER BY bm25_score(zz.body, 'x')")
            .unwrap_err();
        assert_eq!(err, SqlError::UnknownTable { name: "zz".into() });
    }

    #[test]
    fn a_literal_is_not_a_text_column() {
        let err =
            try_plan_select_sql("SELECT id FROM docs WHERE text_match('lit', 'x')").unwrap_err();
        assert!(matches!(
            err,
            SqlError::TextColumn {
                fault: TextColumnFault::NotAColumn,
                ..
            }
        ));
    }

    #[test]
    fn strict_columns_are_checked_for_text() {
        let plan = plan_select_sql("SELECT id FROM articles WHERE text_match(title, 'x')");
        assert_eq!(match_shape(text_plan(&plan)), (Some("title"), None));

        let err = try_plan_select_sql("SELECT id FROM articles WHERE text_match(views, 'x')")
            .unwrap_err();
        assert!(matches!(
            err,
            SqlError::TextColumn {
                fault: TextColumnFault::NotText { .. },
                ..
            }
        ));

        let err = try_plan_select_sql("SELECT id FROM articles WHERE text_match(ghost, 'x')")
            .unwrap_err();
        assert!(matches!(
            err,
            SqlError::TextColumn {
                ref collection,
                fault: TextColumnFault::Undeclared,
                ..
            } if collection == "articles"
        ));
    }

    #[test]
    fn a_text_match_with_one_argument_is_an_arity_error() {
        let err = try_plan_select_sql("SELECT id FROM docs WHERE text_match(body)").unwrap_err();
        assert!(matches!(err, SqlError::Arity { .. }));
    }

    #[test]
    fn distinct_over_a_score_scan_is_refused() {
        let err =
            try_plan_select_sql("SELECT DISTINCT id, bm25_score(body, 'x') FROM docs").unwrap_err();
        assert!(matches!(err, SqlError::Unsupported { .. }));
    }

    #[test]
    fn aggregation_over_a_where_match_is_refused() {
        let err = try_plan_select_sql("SELECT count(*) FROM docs WHERE text_match(body, 'x')")
            .unwrap_err();
        assert!(matches!(err, SqlError::Unsupported { .. }));
    }

    #[test]
    fn hybrid_carries_both_columns_and_the_filters() {
        let plan = plan_select_sql(
            "SELECT id, rrf_score(vector_distance(emb, [1.0, 0.0]), bm25_score(title, 'rust')) \
             AS s FROM docs WHERE tag = 'a' LIMIT 5",
        );
        let SqlPlan::HybridSearch(hybrid) = plan else {
            panic!("expected HybridSearch, got {plan:?}");
        };
        assert_eq!(hybrid.vector_field, "emb");
        assert_eq!(hybrid.text_field.as_deref(), Some("title"));
        assert_eq!(hybrid.query_text, "rust");
        assert_eq!(hybrid.filters.len(), 1);
        assert_eq!(hybrid.top_k, 5);
    }

    /// The mode and fuzzy flag of a `Match` shape.
    fn match_options(search: &TextSearchPlan) -> (QueryMode, bool) {
        match &search.shape {
            TextSearchShape::Match { query, mode, .. } => (*mode, query.is_fuzzy()),
            TextSearchShape::ScoreScan => panic!("expected a Match shape"),
        }
    }

    /// The `Unsupported` detail of a planning error.
    fn unsupported_detail(sql: &str) -> String {
        match try_plan_select_sql(sql).unwrap_err() {
            SqlError::Unsupported { detail } => detail,
            other => panic!("expected Unsupported, got {other:?}"),
        }
    }

    #[test]
    fn text_match_without_options_runs_the_trait_default() {
        let defaults = TextSearchParams::default();
        let plan = plan_select_sql("SELECT id FROM docs WHERE text_match(body, 'rust db')");
        assert_eq!(
            match_options(text_plan(&plan)),
            (defaults.mode, defaults.fuzzy)
        );
        let plan = plan_select_sql("SELECT id FROM docs ORDER BY bm25_score(body, 'x') LIMIT 5");
        let score = &text_plan(&plan).scores[0];
        assert_eq!((score.mode, score.fuzzy), (defaults.mode, defaults.fuzzy));
    }

    #[test]
    fn text_match_mode_option_reaches_the_plan() {
        let plan =
            plan_select_sql("SELECT id FROM docs WHERE text_match(body, 'rust db', mode => 'and')");
        assert_eq!(match_options(text_plan(&plan)), (QueryMode::And, false));
        let plan =
            plan_select_sql("SELECT id FROM docs WHERE text_match(body, 'rust db', mode => 'or')");
        assert_eq!(match_options(text_plan(&plan)), (QueryMode::Or, false));
    }

    #[test]
    fn text_match_fuzzy_option_reaches_the_plan() {
        let plan =
            plan_select_sql("SELECT id FROM docs WHERE text_match(body, 'databse', fuzzy => true)");
        assert_eq!(match_options(text_plan(&plan)), (QueryMode::Or, true));
        let plan = plan_select_sql(
            "SELECT id FROM docs WHERE text_match(body, 'databse', fuzzy => false, mode => 'and')",
        );
        assert_eq!(match_options(text_plan(&plan)), (QueryMode::And, false));
    }

    #[test]
    fn bm25_score_options_reach_the_score_column() {
        let plan = plan_select_sql(
            "SELECT id FROM docs \
             ORDER BY bm25_score(body, 'x y', mode => 'and', fuzzy => true) DESC LIMIT 5",
        );
        let score = &text_plan(&plan).scores[0];
        assert_eq!((score.mode, score.fuzzy), (QueryMode::And, true));
        let plan = plan_select_sql(
            "SELECT id, bm25_score(title, 'x', fuzzy => true) AS s FROM docs \
             WHERE text_match(body, 'y', mode => 'and')",
        );
        let search = text_plan(&plan);
        assert_eq!(match_options(search), (QueryMode::And, false));
        assert_eq!(
            (search.scores[0].mode, search.scores[0].fuzzy),
            (QueryMode::Or, true)
        );
    }

    #[test]
    fn hybrid_text_leg_takes_the_bm25_options() {
        let plan = plan_select_sql(
            "SELECT id, rrf_score(vector_distance(emb, [1.0, 0.0]), \
             bm25_score(title, 'rust', mode => 'and', fuzzy => true)) AS s FROM docs LIMIT 5",
        );
        let SqlPlan::HybridSearch(hybrid) = plan else {
            panic!("expected HybridSearch, got {plan:?}");
        };
        assert_eq!((hybrid.mode, hybrid.fuzzy), (QueryMode::And, true));
    }

    #[test]
    fn an_unknown_text_option_is_refused() {
        let detail =
            unsupported_detail("SELECT id FROM docs WHERE text_match(body, 'x', boost => 2)");
        assert!(
            detail.contains("unknown text-search option 'boost'"),
            "{detail}"
        );
        let detail = unsupported_detail(
            "SELECT id FROM docs ORDER BY bm25_score(body, 'x', slop => 1) LIMIT 5",
        );
        assert!(
            detail.contains("unknown text-search option 'slop'"),
            "{detail}"
        );
    }

    #[test]
    fn an_equals_text_option_is_refused() {
        let detail =
            unsupported_detail("SELECT id FROM docs WHERE text_match(body, 'x', mode = 'or')");
        assert!(detail.contains("use '=>'"), "{detail}");
    }

    #[test]
    fn a_positional_third_text_argument_is_refused() {
        let detail = unsupported_detail("SELECT id FROM docs WHERE text_match(body, 'x', 'fuzzy')");
        assert!(detail.contains("third positional argument"), "{detail}");
        let detail =
            unsupported_detail("SELECT id FROM docs WHERE text_match(body, 'x', { fuzzy: true })");
        assert!(detail.contains("third positional argument"), "{detail}");
    }

    #[test]
    fn options_on_a_phrase_query_are_refused() {
        let detail = unsupported_detail(
            "SELECT id FROM docs WHERE text_match(body, '\"quick fox\"', fuzzy => true)",
        );
        assert!(detail.contains("phrase"), "{detail}");
        let plan = plan_select_sql("SELECT id FROM docs WHERE text_match(body, '\"quick fox\"')");
        assert!(matches!(
            &text_plan(&plan).shape,
            TextSearchShape::Match {
                query: crate::fts_types::FtsQuery::Phrase(_),
                ..
            }
        ));
    }

    #[test]
    fn hybrid_without_a_vector_leg_is_an_error() {
        let err = try_plan_select_sql(
            "SELECT id, rrf_score(1, bm25_score(title, 'rust')) AS s FROM docs LIMIT 5",
        )
        .unwrap_err();
        assert!(matches!(err, SqlError::InvalidFunction { .. }));
    }
}
