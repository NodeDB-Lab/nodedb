// SPDX-License-Identifier: BUSL-1.1

//! A large `INSERT ... ON CONFLICT DO NOTHING` page on the source of a
//! cross-shard materialized sum.
//!
//! The plan reads the stored row of every page row before it settles the
//! balance. The page must answer inside the statement deadline, and the balance
//! must equal the source sum afterwards.
//!
//! The fixture starts with `e0` = 10 and `e1` = 5 on `acc`, balance 15.

use super::sum_cell_support::{balance, source_sum, sum_fixture};

/// Rows in the page. A read cut per row misses the 30 s statement deadline at
/// this size.
const PAGE_ROWS: usize = 1_000;

/// A page of `PAGE_ROWS` new keys, each with amount 1.
fn page_sql() -> String {
    let values: Vec<String> = (0..PAGE_ROWS)
        .map(|i| format!("('sp{i}', 'acc', '1')"))
        .collect();
    format!(
        "INSERT INTO ptv_post (id, account_id, amount) VALUES {} ON CONFLICT DO NOTHING",
        values.join(", ")
    )
}

/// A page of new keys lands inside the statement deadline. A page that takes a
/// read cut per row answers `57014` instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_large_skip_page_lands_inside_the_deadline_and_the_balance_follows() {
    let server = sum_fixture().await;
    if let Err(e) = server.exec(&page_sql()).await {
        panic!("the {PAGE_ROWS}-row skip page applies: {e}");
    }
    let expected = 15 + PAGE_ROWS as i64;
    assert_eq!(source_sum(&server).await, expected, "source sum");
    assert_eq!(
        balance(&server, "acc").await,
        expected.to_string(),
        "balance follows the source sum"
    );
}
