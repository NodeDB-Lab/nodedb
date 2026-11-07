// SPDX-License-Identifier: Apache-2.0

//! A CTE around a LATERAL join: the outer side reads it, the inner side of a
//! top-k or loop join refuses it.

use super::fixtures::{plan_select_sql, try_plan_select_sql};
use crate::error::SqlError;
use crate::planner::cte::inline_cte;
use crate::types::*;

/// The outer plan of `sql` with its one CTE inlined.
fn inlined(sql: &str) -> SqlPlan {
    let SqlPlan::Cte(CtePlan { definitions, outer }) = plan_select_sql(sql) else {
        panic!("expected a CTE plan for {sql}");
    };
    let (name, body) = &definitions[0];
    inline_cte(&outer, name, body)
}

fn scans_users(plan: &SqlPlan) -> bool {
    matches!(plan, SqlPlan::Scan { collection, .. } if collection == "users")
}

#[test]
fn a_top_k_outer_side_reads_the_cte_body() {
    let plan = inlined(
        "WITH c AS (SELECT * FROM users) \
         SELECT c.id FROM c, LATERAL (\
             SELECT o.id FROM orders o WHERE o.user_id = c.id ORDER BY o.score DESC LIMIT 1\
         ) best",
    );
    let SqlPlan::LateralTopK(top_k) = plan else {
        panic!("expected LateralTopK, got {plan:?}");
    };
    assert!(scans_users(&top_k.outer), "{:?}", top_k.outer);
}

#[test]
fn a_loop_outer_side_reads_the_cte_body() {
    let plan = inlined(
        "WITH c AS (SELECT * FROM users) \
         SELECT c.id FROM c JOIN LATERAL (\
             SELECT o.id FROM orders o WHERE o.log_time > c.created_at\
         ) recent ON true",
    );
    let SqlPlan::LateralLoop(lateral_loop) = plan else {
        panic!("expected LateralLoop, got {plan:?}");
    };
    assert!(scans_users(&lateral_loop.outer), "{:?}", lateral_loop.outer);
}

#[test]
fn a_cte_inner_side_is_refused() {
    for sql in [
        "WITH c AS (SELECT * FROM orders) \
         SELECT u.id FROM users u, LATERAL (\
             SELECT c.id FROM c WHERE c.user_id = u.id ORDER BY c.score DESC LIMIT 1\
         ) best",
        "WITH c AS (SELECT * FROM orders) \
         SELECT u.id FROM users u JOIN LATERAL (\
             SELECT c.id FROM c WHERE c.log_time > u.created_at\
         ) recent ON true",
    ] {
        match try_plan_select_sql(sql) {
            Err(SqlError::Unsupported { detail }) => {
                assert!(detail.contains("'c'"), "{detail}");
            }
            other => panic!("{sql}: expected the CTE refusal, got {other:?}"),
        }
    }
}

/// An equi-correlated LATERAL with no LIMIT plans to a regular join, whose
/// sides inline a CTE, so it stays allowed.
#[test]
fn an_equi_join_inner_side_keeps_reading_a_cte() {
    let plan = inlined(
        "WITH c AS (SELECT * FROM orders) \
         SELECT u.id FROM users u, LATERAL (SELECT c.id FROM c WHERE c.user_id = u.id) best",
    );
    assert!(matches!(plan, SqlPlan::Join { .. }), "{plan:?}");
}
