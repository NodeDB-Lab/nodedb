// SPDX-License-Identifier: BUSL-1.1

//! `NOT` takes a boolean operand only, as in PostgreSQL, on every document
//! and columnar engine.
//!
//! - `NOT` on a number fails with SQLSTATE `42804` (datatype_mismatch). It
//!   never answers the negated number.
//! - `NOT` on a boolean answers its complement, and `NOT NULL` is NULL.
//! - Unary minus still negates a number.

use crate::harness::TestServer;

/// One table per engine, each with the same two rows.
const TABLES: [(&str, &str); 3] = [
    (
        "nb_strict",
        "(id STRING PRIMARY KEY, n INT, flag BOOL) WITH (engine='document_strict')",
    ),
    (
        "nb_schemaless",
        "(id STRING PRIMARY KEY, n INT, flag BOOL) WITH (engine='document_schemaless')",
    ),
    (
        "nb_columnar",
        "(id TEXT, n INT, flag BOOL) WITH (engine='columnar')",
    ),
];

async fn seeded() -> TestServer {
    let srv = TestServer::start().await;
    for (table, shape) in TABLES {
        srv.exec(&format!("CREATE COLLECTION {table} {shape}"))
            .await
            .unwrap_or_else(|e| panic!("create {table}: {e}"));
        srv.exec(&format!(
            "INSERT INTO {table} (id, n, flag) VALUES ('a', 1, true), ('b', 2, false)"
        ))
        .await
        .unwrap_or_else(|e| panic!("insert {table}: {e}"));
    }
    srv
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn not_on_a_number_is_42804_on_every_engine() {
    let srv = seeded().await;
    for (table, _) in TABLES {
        for sql in [
            "SELECT NOT 5".to_string(),
            format!("SELECT NOT 5 FROM {table} WHERE id = 'a'"),
            format!("SELECT id FROM {table} WHERE NOT n"),
        ] {
            let error = srv
                .query_rows(&sql)
                .await
                .expect_err("NOT on a number must fail");
            assert!(
                error.contains("42804") && error.contains("argument of NOT must be type boolean"),
                "{table}: {sql}: expected 42804, got: {error}"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn not_on_a_boolean_or_null_is_unchanged_on_every_engine() {
    let srv = seeded().await;
    for (table, _) in TABLES {
        let rows = srv
            .client
            .query(
                &format!(
                    "SELECT NOT flag, NOT true, NOT false, NOT NULL FROM {table} WHERE id = $1"
                ),
                &[&"a"],
            )
            .await
            .unwrap_or_else(|e| panic!("{table}: NOT on booleans and NULL must succeed: {e}"));
        assert_eq!(rows.len(), 1, "{table}");
        let cells: Vec<Option<bool>> = (0..4).map(|i| rows[0].get(i)).collect();
        assert_eq!(
            cells,
            vec![Some(false), Some(false), Some(true), None],
            "{table}"
        );

        for predicate in ["NOT (n = 1)", "NOT flag"] {
            let rows = srv
                .query_rows(&format!(
                    "SELECT id FROM {table} WHERE {predicate} ORDER BY id"
                ))
                .await
                .unwrap_or_else(|e| panic!("{table}: {predicate}: {e}"));
            assert_eq!(rows, vec![vec!["b".to_string()]], "{table}: {predicate}");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unary_minus_still_negates_on_every_engine() {
    let srv = seeded().await;
    for (table, _) in TABLES {
        let rows = srv
            .query_rows(&format!("SELECT -n FROM {table} WHERE id = 'b'"))
            .await
            .unwrap_or_else(|e| panic!("{table}: {e}"));
        assert_eq!(rows, vec![vec!["-2".to_string()]], "{table}");
    }
}
