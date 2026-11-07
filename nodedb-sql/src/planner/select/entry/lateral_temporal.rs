// SPDX-License-Identifier: Apache-2.0

//! A LATERAL join reads one cut. The statement's `AS OF` reaches the inner
//! side of a top-k or loop join, and a scope the inner scan cannot read is
//! refused.

use nodedb_types::SystemTimeScope;

use super::fixtures::{plan_select_sql, try_plan_select_sql};
use crate::error::SqlError;
use crate::types::*;

const TOP_K: &str = "SELECT u.id FROM users u, LATERAL (\
     SELECT o.id FROM orders o WHERE o.user_id = u.id ORDER BY o.score DESC LIMIT 1\
     ) best";

const LOOP: &str = "SELECT u.id FROM users u JOIN LATERAL (\
     SELECT o.id FROM orders o WHERE o.log_time > u.created_at\
     ) recent ON true";

fn outer_scope(outer: &SqlPlan) -> SystemTimeScope {
    let SqlPlan::Scan { temporal, .. } = outer else {
        panic!("expected an outer Scan, got {outer:?}");
    };
    temporal.system_time
}

#[test]
fn a_top_k_carries_the_statement_system_time() {
    let plan = plan_select_sql(&format!("{TOP_K} AS OF SYSTEM TIME 5"));
    let SqlPlan::LateralTopK(top_k) = plan else {
        panic!("expected LateralTopK, got {plan:?}");
    };
    assert_eq!(top_k.temporal.system_time, SystemTimeScope::AsOf(5));
    assert_eq!(outer_scope(&top_k.outer), SystemTimeScope::AsOf(5));
}

#[test]
fn a_loop_inner_scan_carries_the_statement_system_time() {
    let plan = plan_select_sql(&format!("{LOOP} AS OF SYSTEM TIME 5"));
    let SqlPlan::LateralLoop(lateral_loop) = plan else {
        panic!("expected LateralLoop, got {plan:?}");
    };
    assert_eq!(outer_scope(&lateral_loop.inner), SystemTimeScope::AsOf(5));
    assert_eq!(outer_scope(&lateral_loop.outer), SystemTimeScope::AsOf(5));
}

#[test]
fn every_versions_is_refused_on_both_shapes() {
    for shape in [TOP_K, LOOP] {
        let sql = format!("{shape} AS OF SYSTEM TIME NULL");
        match try_plan_select_sql(&sql) {
            Err(SqlError::Unsupported { detail }) => {
                assert!(detail.contains("orders"), "{detail}");
            }
            other => panic!("{sql}: expected the refusal, got {other:?}"),
        }
    }
}

/// The inner scan reads no valid time, so the qualifier would reach the outer
/// side only. The outer collection is bitemporal, so the outer side accepts it.
#[test]
fn a_valid_time_is_refused() {
    let sql = "SELECT u.id FROM tagged_embeddings_bitemporal u, LATERAL (\
         SELECT o.id FROM orders o WHERE o.user_id = u.id ORDER BY o.score DESC LIMIT 1\
         ) best AS OF VALID TIME 5";
    assert!(
        matches!(try_plan_select_sql(sql), Err(SqlError::Unsupported { .. })),
        "{sql}"
    );
}

/// The inner side runs as a document scan, which finds no row of another
/// engine at any cut.
#[test]
fn a_non_document_inner_side_is_refused() {
    let sql = "SELECT u.id FROM users u, LATERAL (\
         SELECT p.key FROM user_prefs p WHERE p.key = u.id ORDER BY p.key LIMIT 1\
         ) best";
    match try_plan_select_sql(sql) {
        Err(SqlError::Unsupported { detail }) => {
            assert!(detail.contains("user_prefs"), "{detail}");
        }
        other => panic!("expected the engine refusal, got {other:?}"),
    }
}
