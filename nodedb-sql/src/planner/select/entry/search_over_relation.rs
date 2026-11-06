// SPDX-License-Identifier: Apache-2.0

//! A search leaf over a CTE or derived table is refused at plan time. It has
//! no body to inline, and it must not read a collection that shares the name.

use super::fixtures::{plan_select_sql, try_plan_select_sql};
use crate::error::SqlError;
use crate::types::*;

const RRF: &str = "rrf_score(vector_distance(emb, [1.0, 0.0]), bm25_score(title, 'rust'))";

fn assert_refused(sql: &str, relation: &str) {
    match try_plan_select_sql(sql) {
        Err(SqlError::Unsupported { detail }) => {
            assert!(
                detail.contains(&format!("'{relation}' is a CTE")),
                "{sql}: {detail}"
            );
        }
        other => panic!("{sql}: expected the refusal, got {other:?}"),
    }
}

/// `SELECT ... FROM c <tail>` over the CTE `c`.
fn over_cte(tail: &str) -> String {
    format!("WITH c AS (SELECT * FROM docs) SELECT id FROM c {tail}")
}

#[test]
fn text_search_over_a_cte_is_refused() {
    assert_refused(&over_cte("WHERE text_match(body, 'x')"), "c");
    assert_refused(
        &over_cte("ORDER BY bm25_score(body, 'x') DESC LIMIT 5"),
        "c",
    );
    assert_refused(
        "WITH c AS (SELECT * FROM docs) SELECT id, bm25_score(body, 'x') AS s FROM c",
        "c",
    );
}

#[test]
fn vector_search_over_a_cte_is_refused() {
    assert_refused(
        &over_cte("ORDER BY vector_distance(emb, [1.0, 0.0]) LIMIT 5"),
        "c",
    );
}

#[test]
fn sparse_search_over_a_cte_is_refused() {
    assert_refused(
        &over_cte("ORDER BY sparse_score(terms, '{3: 1.0}') DESC LIMIT 5"),
        "c",
    );
}

#[test]
fn spatial_scan_over_a_cte_is_refused() {
    assert_refused(
        &over_cte("WHERE ST_DWithin(location, ST_Point(-122.4, 37.8), 1000)"),
        "c",
    );
}

#[test]
fn hybrid_search_over_a_cte_is_refused() {
    assert_refused(
        &format!("WITH c AS (SELECT * FROM docs) SELECT id, {RRF} AS s FROM c LIMIT 5"),
        "c",
    );
}

#[test]
fn triple_hybrid_search_over_a_cte_is_refused() {
    assert_refused(
        "WITH c AS (SELECT * FROM docs) \
         SELECT id, rrf_score(vector_distance(emb, [1.0, 0.0]), bm25_score(title, 'rust'), \
         graph_score(id, 'seed')) AS s FROM c LIMIT 5",
        "c",
    );
}

/// A CTE that shadows a stored collection names the CTE, never the collection.
/// Reading the collection drops the CTE's filter.
#[test]
fn a_cte_that_shadows_a_collection_is_refused() {
    assert_refused(
        "WITH docs AS (SELECT * FROM docs WHERE tag = 'a') \
         SELECT id FROM docs WHERE text_match(body, 'x')",
        "docs",
    );
}

#[test]
fn a_search_over_a_derived_table_is_refused() {
    assert_refused(
        "SELECT id FROM (SELECT * FROM docs) d WHERE text_match(body, 'x')",
        "d",
    );
}

/// A search inside a CTE body reads the stored collection. An outer read of
/// the CTE scans it, and that scan inlines.
#[test]
fn a_search_inside_a_cte_body_still_plans() {
    let plan = plan_select_sql(
        "WITH docs AS (SELECT id FROM docs WHERE text_match(body, 'x')) SELECT id FROM docs",
    );
    let SqlPlan::Cte(CtePlan { definitions, .. }) = plan else {
        panic!("expected Cte, got {plan:?}");
    };
    assert!(
        matches!(
            definitions[0].1,
            SqlPlan::TextSearch(_) | SqlPlan::Subquery { .. }
        ),
        "{:?}",
        definitions[0].1
    );
}
