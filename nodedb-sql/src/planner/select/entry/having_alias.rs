// SPDX-License-Identifier: Apache-2.0

//! A SELECT-list alias binds in HAVING at any expression depth.

use super::fixtures::plan_select_sql;
use crate::types::*;

/// The HAVING filters of `SELECT ... HAVING <having>` over `orders`, rendered.
fn having_of(having: &str) -> String {
    let sql = format!(
        "SELECT user_id, SUM(amount) AS total FROM orders GROUP BY user_id HAVING {having}"
    );
    let plan = plan_select_sql(&sql);
    let SqlPlan::Aggregate { having, .. } = plan else {
        panic!("expected Aggregate, got {plan:?}");
    };
    format!("{having:?}")
}

#[test]
fn an_alias_inside_an_expression_addresses_the_computed_column() {
    for having in [
        "total > 0",
        "ROUND(total, 1) > 0",
        "CASE WHEN total > 0 THEN 1 ELSE 0 END = 1",
        "CAST(total AS BIGINT) > 0",
        "total IN (1, 2)",
    ] {
        let rendered = having_of(having);
        assert!(rendered.contains("sum(amount)"), "{having}: {rendered}");
        assert!(!rendered.contains("total"), "{having}: {rendered}");
    }
}

/// An aggregate argument names an input column. An alias of the same name
/// does not replace it, or `SUM(amount)` reads as `SUM(SUM(amount))`.
#[test]
fn an_aggregate_argument_never_binds_to_an_alias() {
    let plan = plan_select_sql(
        "SELECT user_id, SUM(amount) AS amount FROM orders GROUP BY user_id \
         HAVING SUM(amount) > 0",
    );
    let SqlPlan::Aggregate {
        aggregates, having, ..
    } = plan
    else {
        panic!("expected Aggregate, got {plan:?}");
    };
    assert_eq!(aggregates.len(), 1, "HAVING reuses the selected aggregate");
    assert!(format!("{having:?}").contains("sum(amount)"), "{having:?}");
}
