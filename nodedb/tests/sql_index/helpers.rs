//! Shared helpers for the `sql_index_naming` test binary.

#![allow(dead_code)] // Not every sub-file uses every helper.

use super::common::pgwire_harness::TestServer;

/// Return all rows of EXPLAIN <sql> concatenated, lowercased.
///
/// Used to assert that the chosen physical plan references an index lookup
/// rather than a full scan when a WHERE predicate lands on an indexed field.
pub async fn explain_lower(server: &TestServer, sql: &str) -> String {
    let rows = server
        .query_text(&format!("EXPLAIN {sql}"))
        .await
        .expect("EXPLAIN must succeed");
    rows.join("\n").to_lowercase()
}

/// Return all rows of EXPLAIN <sql> concatenated, preserving case.
///
/// The pgwire EXPLAIN handler emits the `PhysicalPlan` via `{:?}` debug
/// format, so structural fields like `filters: [..]`, `projection: [..]`,
/// `limit: N`, etc. appear verbatim. Plan-shape assertions use this
/// form; `explain_lower` is only for case-insensitive plan-variant
/// name checks.
pub async fn explain_raw(server: &TestServer, sql: &str) -> String {
    let rows = server
        .query_text(&format!("EXPLAIN {sql}"))
        .await
        .expect("EXPLAIN must succeed");
    rows.join("\n")
}
