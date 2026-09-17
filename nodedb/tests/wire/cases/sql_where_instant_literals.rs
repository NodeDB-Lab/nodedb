// SPDX-License-Identifier: BUSL-1.1

//! A literal compared against a declared `TIMESTAMP`/`TIMESTAMPTZ` column
//! follows one rule on every engine and every path: a string literal parses
//! as a datetime, a numeric literal is epoch milliseconds, and any other
//! literal kind is a typed error. The comparison then compares instants, and
//! `SELECT` renders the column as `2020-03-05T10:00:00.000000Z` style
//! ISO-8601.

use crate::harness::TestServer;

/// `2020-03-05 10:00:00` as a bare datetime literal. Its epoch-millisecond
/// form is `1583402400000`, which the predicate tables below spell out
/// directly in each SQL clause.
const EARLY: &str = "2020-03-05 10:00:00";
/// `EARLY` rendered by a `SELECT` of a `TIMESTAMP` column.
const EARLY_ISO: &str = "2020-03-05T10:00:00.000000Z";

/// A predicate against `at`, and the row ids it must select, in `ORDER BY
/// id` order.
struct Predicate {
    label: &'static str,
    clause: &'static str,
    expected: &'static [&'static str],
}

/// One family per operator, expressed with a numeric-ms literal. Callers
/// substitute the literal for a string form to cover the other literal kind.
const NUMERIC_PREDICATES: &[Predicate] = &[
    Predicate {
        label: "= ms",
        clause: "at = 1583402400000",
        expected: &["r2_at"],
    },
    Predicate {
        label: "> ms",
        clause: "at > 1583402400000",
        expected: &["r3_after"],
    },
    Predicate {
        label: ">= ms",
        clause: "at >= 1583402400000",
        expected: &["r2_at", "r3_after"],
    },
    Predicate {
        label: "< ms",
        clause: "at < 1583402400000",
        expected: &["r1_before"],
    },
    Predicate {
        label: "<= ms",
        clause: "at <= 1583402400000",
        expected: &["r1_before", "r2_at"],
    },
    Predicate {
        label: "BETWEEN ms",
        clause: "at BETWEEN 1583402400000 AND 1583406000000",
        expected: &["r2_at", "r3_after"],
    },
    Predicate {
        label: "IN ms",
        clause: "at IN (1583402400000, 1583406000000)",
        expected: &["r2_at", "r3_after"],
    },
];

/// The same seven predicates with a space-separated datetime string literal.
const STRING_SPACE_PREDICATES: &[Predicate] = &[
    Predicate {
        label: "= string",
        clause: "at = '2020-03-05 10:00:00'",
        expected: &["r2_at"],
    },
    Predicate {
        label: "> string",
        clause: "at > '2020-03-05 10:00:00'",
        expected: &["r3_after"],
    },
    Predicate {
        label: ">= string",
        clause: "at >= '2020-03-05 10:00:00'",
        expected: &["r2_at", "r3_after"],
    },
    Predicate {
        label: "< string",
        clause: "at < '2020-03-05 10:00:00'",
        expected: &["r1_before"],
    },
    Predicate {
        label: "<= string",
        clause: "at <= '2020-03-05 10:00:00'",
        expected: &["r1_before", "r2_at"],
    },
    Predicate {
        label: "BETWEEN string",
        clause: "at BETWEEN '2020-03-05 10:00:00' AND '2020-03-05 11:00:00'",
        expected: &["r2_at", "r3_after"],
    },
    Predicate {
        label: "IN string",
        clause: "at IN ('2020-03-05 10:00:00', '2020-03-05 11:00:00')",
        expected: &["r2_at", "r3_after"],
    },
];

/// The same seven predicates with an ISO-8601 `T`/`Z` string literal.
const STRING_ISO_PREDICATES: &[Predicate] = &[
    Predicate {
        label: "= iso",
        clause: "at = '2020-03-05T10:00:00Z'",
        expected: &["r2_at"],
    },
    Predicate {
        label: "> iso",
        clause: "at > '2020-03-05T10:00:00Z'",
        expected: &["r3_after"],
    },
    Predicate {
        label: ">= iso",
        clause: "at >= '2020-03-05T10:00:00Z'",
        expected: &["r2_at", "r3_after"],
    },
    Predicate {
        label: "< iso",
        clause: "at < '2020-03-05T10:00:00Z'",
        expected: &["r1_before"],
    },
    Predicate {
        label: "<= iso",
        clause: "at <= '2020-03-05T10:00:00Z'",
        expected: &["r1_before", "r2_at"],
    },
    Predicate {
        label: "BETWEEN iso",
        clause: "at BETWEEN '2020-03-05T10:00:00Z' AND '2020-03-05T11:00:00Z'",
        expected: &["r2_at", "r3_after"],
    },
    Predicate {
        label: "IN iso",
        clause: "at IN ('2020-03-05T10:00:00Z', '2020-03-05T11:00:00Z')",
        expected: &["r2_at", "r3_after"],
    },
];

/// Create `name` on `engine` with `(id TEXT PRIMARY KEY, at TIMESTAMP, v
/// FLOAT)` and seed `r1_before`/`r2_at`/`r3_after` an hour apart around `EARLY`.
/// `engine` is `None` for a schemaless document collection (no `WITH`
/// clause).
async fn seed(server: &TestServer, name: &str, engine: Option<&str>) {
    let with_clause = match engine {
        Some(e) => format!(" WITH (engine='{e}')"),
        None => String::new(),
    };
    server
        .exec(&format!(
            "CREATE COLLECTION {name} (id TEXT PRIMARY KEY, at TIMESTAMP, v FLOAT){with_clause}"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {name}: {e}"));
    server
        .exec(&format!(
            "INSERT INTO {name} (id, at, v) VALUES \
             ('r1_before', '2020-03-05 09:00:00', 1.0), \
             ('r2_at', '{EARLY}', 2.0), \
             ('r3_after', '2020-03-05 11:00:00', 3.0)"
        ))
        .await
        .unwrap_or_else(|e| panic!("insert into {name}: {e}"));
}

/// Create a timeseries collection `(at TIMESTAMP TIME_KEY, id TEXT, v FLOAT)
/// WITH (engine='timeseries')` and seed the same three rows.
async fn seed_timeseries(server: &TestServer, name: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {name} (at TIMESTAMP TIME_KEY, id TEXT, v FLOAT) \
             WITH (engine='timeseries')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {name}: {e}"));
    server
        .exec(&format!(
            "INSERT INTO {name} (at, id, v) VALUES \
             ('2020-03-05 09:00:00', 'r1_before', 1.0), \
             ('{EARLY}', 'r2_at', 2.0), \
             ('2020-03-05 11:00:00', 'r3_after', 3.0)"
        ))
        .await
        .unwrap_or_else(|e| panic!("insert into {name}: {e}"));
}

/// Run every predicate in `table` against `collection` and assert the row
/// ids each one selects, each assertion message naming the predicate.
async fn assert_predicates(server: &TestServer, collection: &str, table: &[Predicate]) {
    for p in table {
        let rows = server
            .query_text(&format!(
                "SELECT id FROM {collection} WHERE {} ORDER BY id",
                p.clause
            ))
            .await
            .unwrap_or_else(|e| panic!("{collection} [{}]: {e}", p.label));
        assert_eq!(
            rows, p.expected,
            "{collection} [{}]: expected {:?}, got {rows:?}",
            p.label, p.expected
        );
    }
}

/// Run the full literal-kind suite (numeric ms, space-separated string,
/// ISO-8601 string) against `collection`, plus the typed-error case for a
/// non-instant literal kind.
async fn assert_full_suite(server: &TestServer, collection: &str) {
    assert_predicates(server, collection, NUMERIC_PREDICATES).await;
    assert_predicates(server, collection, STRING_SPACE_PREDICATES).await;
    assert_predicates(server, collection, STRING_ISO_PREDICATES).await;
    server
        .expect_error(
            &format!("SELECT id FROM {collection} WHERE at = true"),
            "at",
        )
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn document_strict_instant_literal_predicates() {
    let server = TestServer::start().await;
    let name = "swi_strict";
    seed(&server, name, Some("document_strict")).await;
    assert_full_suite(&server, name).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn schemaless_document_instant_literal_predicates() {
    let server = TestServer::start().await;
    let name = "swi_schemaless";
    seed(&server, name, None).await;
    assert_full_suite(&server, name).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_instant_literal_predicates() {
    let server = TestServer::start().await;
    let name = "swi_kv";
    seed(&server, name, Some("kv")).await;
    assert_full_suite(&server, name).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_instant_literal_predicates() {
    let server = TestServer::start().await;
    let name = "swi_columnar";
    seed(&server, name, Some("columnar")).await;
    assert_full_suite(&server, name).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timeseries_instant_literal_predicates() {
    let server = TestServer::start().await;
    let name = "swi_timeseries";
    seed_timeseries(&server, name).await;
    assert_full_suite(&server, name).await;
}

/// Rows flushed to a timeseries partition (a low memtable budget forces the
/// flush) follow the same instant-literal rule as an unflushed memtable read.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timeseries_instant_literal_predicates_after_flush() {
    let server = TestServer::start_with_timeseries_memtable_budget(1).await;
    let name = "swi_ts_flushed";
    seed_timeseries(&server, name).await;
    assert_full_suite(&server, name).await;
}

/// Rows on a flushed columnar segment follow the same instant-literal rule
/// as rows still in the write buffer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_instant_literal_predicates_after_flush() {
    let server = TestServer::start_with_columnar_flush_threshold(2).await;
    let name = "swi_columnar_flushed";
    seed(&server, name, Some("columnar")).await;
    assert_full_suite(&server, name).await;
}

/// `UPDATE ... WHERE at > <ms>` and `DELETE ... WHERE at < <ms>` scope by the
/// same instant the read path selects by, for every DML-capable engine.
async fn assert_update_delete_scoped_by_instant(server: &TestServer, collection: &str) {
    server
        .exec(&format!(
            "UPDATE {collection} SET v = 9 WHERE at > 1583402400000"
        ))
        .await
        .unwrap_or_else(|e| panic!("update {collection}: {e}"));
    let updated = server
        .query_text(&format!("SELECT id FROM {collection} WHERE v = 9"))
        .await
        .unwrap_or_else(|e| panic!("select updated {collection}: {e}"));
    assert_eq!(
        updated,
        vec!["r3_after".to_string()],
        "UPDATE ... WHERE at > <ms> must touch only r3_after, got {updated:?}"
    );

    server
        .exec(&format!(
            "DELETE FROM {collection} WHERE at < 1583402400000"
        ))
        .await
        .unwrap_or_else(|e| panic!("delete {collection}: {e}"));
    let remaining = server
        .query_text(&format!("SELECT id FROM {collection} ORDER BY id"))
        .await
        .unwrap_or_else(|e| panic!("select remaining {collection}: {e}"));
    assert_eq!(
        remaining,
        vec!["r2_at".to_string(), "r3_after".to_string()],
        "DELETE ... WHERE at < <ms> must remove only r1_before, got {remaining:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn document_strict_update_delete_scoped_by_instant_literal() {
    let server = TestServer::start().await;
    let name = "swi_strict_dml";
    seed(&server, name, Some("document_strict")).await;
    assert_update_delete_scoped_by_instant(&server, name).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn columnar_update_delete_scoped_by_instant_literal() {
    let server = TestServer::start().await;
    let name = "swi_columnar_dml";
    seed(&server, name, Some("columnar")).await;
    assert_update_delete_scoped_by_instant(&server, name).await;
}

/// A JOIN `ON` clause comparing a timeseries time key against a numeric-ms
/// literal follows the same rule as a top-level `WHERE`: the literal is an
/// instant, so the predicate scopes the join to rows after `EARLY`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn join_on_clause_with_instant_ms_literal_scopes_the_match() {
    let server = TestServer::start().await;
    let ts_name = "swi_join_ts";
    let strict_name = "swi_join_strict";
    seed_timeseries(&server, ts_name).await;
    seed(&server, strict_name, Some("document_strict")).await;

    let joined = server
        .query_text(&format!(
            "SELECT a.id FROM {ts_name} a JOIN {strict_name} b \
             ON a.id = b.id AND a.at > 1583402400000"
        ))
        .await
        .unwrap_or_else(|e| panic!("join {ts_name}/{strict_name}: {e}"));
    assert_eq!(
        joined,
        vec!["r3_after".to_string()],
        "the ON clause's ms literal must scope the join to rows after EARLY: {joined:?}"
    );
}

/// A `SELECT` of the declared `at` column renders `EARLY` as ISO-8601 UTC,
/// independent of which literal kind selected the row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn selecting_the_matched_row_renders_iso8601() {
    let server = TestServer::start().await;
    let name = "swi_render";
    seed(&server, name, Some("document_strict")).await;

    let rows = server
        .query_text(&format!("SELECT at FROM {name} WHERE at = 1583402400000"))
        .await
        .unwrap_or_else(|e| panic!("select at from {name}: {e}"));
    assert_eq!(
        rows,
        vec![EARLY_ISO.to_string()],
        "SELECT of a TIMESTAMP column must render ISO-8601 UTC: {rows:?}"
    );
}
