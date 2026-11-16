// SPDX-License-Identifier: BUSL-1.1

//! A cloned vector-primary collection reads its source columns, before and
//! after the materializer copies its rows.

use std::sync::atomic::AtomicBool;

use nodedb::control::maintenance::clone_materializer::run_scheduled_sweep;
use nodedb_test_support::pgwire_harness::TestServer;

const ROWS: &str = "SELECT id, tag, emb FROM vp_clone ORDER BY id";
const FILTERED: &str = "SELECT id FROM vp_clone WHERE tag = 'first' \
                        ORDER BY vector_distance(emb, ARRAY[0.0,1.0,0.0]) LIMIT 10";

/// Create the source database with two rows, and clone it.
async fn cloned_source(server: &TestServer) {
    for sql in [
        "CREATE DATABASE vp_clone_src",
        "USE DATABASE vp_clone_src",
        "CREATE COLLECTION vp_clone (id TEXT PRIMARY KEY, tag TEXT, emb VECTOR(3)) \
         WITH (engine='vector', primary='vector', vector_field='emb', dim=3, \
         payload_indexes=['tag'])",
        "INSERT INTO vp_clone (id, tag, emb) VALUES ('v1', 'first', ARRAY[1.0,0.0,0.0])",
        "INSERT INTO vp_clone (id, tag, emb) VALUES ('v2', 'second', ARRAY[0.0,1.0,0.0])",
        "USE DATABASE default",
        "CLONE DATABASE vp_clone_copy FROM vp_clone_src",
        "USE DATABASE vp_clone_copy",
    ] {
        server
            .exec(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
}

const WRITES: [&str; 3] = [
    "UPSERT INTO vp_clone (id, tag, emb) VALUES ('v1', 'changed', ARRAY[1.0,0.0,0.0])",
    "INSERT INTO vp_clone (id, tag, emb) VALUES ('v3', 'third', ARRAY[0.0,0.0,1.0])",
    "DELETE FROM vp_clone WHERE id = 'v2'",
];

/// A vector write into the clone before its rows are copied is refused,
/// naming the vector engine. Once the copy ran, the same writes apply.
#[tokio::test(flavor = "multi_thread")]
async fn a_clone_takes_vector_writes_once_its_rows_are_copied() {
    let server = TestServer::start().await;
    cloned_source(&server).await;
    let source_rows = server.query_rows(ROWS).await.expect("clone rows");
    for sql in WRITES {
        let error = server
            .exec(sql)
            .await
            .expect_err("a vector write before the copy is refused");
        assert!(
            error.contains("(engine 'vector') has no copy-on-write support")
                && error.contains("55006"),
            "{sql}: {error}"
        );
    }
    assert_eq!(
        server
            .query_rows(ROWS)
            .await
            .expect("clone rows after the refusals"),
        source_rows,
        "a refused write changes nothing"
    );

    let cancel = AtomicBool::new(false);
    run_scheduled_sweep(&server.shared, server.shared.credentials.catalog(), &cancel)
        .await
        .expect("run_scheduled_sweep");
    for sql in WRITES {
        server
            .exec(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    let expected = vec![
        vec![
            "v1".to_string(),
            "changed".to_string(),
            "[1.0,0.0,0.0]".to_string(),
        ],
        vec![
            "v3".to_string(),
            "third".to_string(),
            "[0.0,0.0,1.0]".to_string(),
        ],
    ];
    assert_eq!(
        server
            .query_rows(ROWS)
            .await
            .expect("clone rows after the writes"),
        expected,
        "the copied clone applies every write"
    );
    assert_eq!(
        server
            .query_rows("SELECT id FROM vp_clone WHERE tag = 'changed'")
            .await
            .expect("filtered read"),
        vec![vec!["v1".to_string()]]
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cloned_vector_primary_row_reads_its_source_columns() {
    let server = TestServer::start().await;
    for sql in [
        "CREATE DATABASE vp_clone_src",
        "USE DATABASE vp_clone_src",
        "CREATE COLLECTION vp_clone (id TEXT PRIMARY KEY, tag TEXT, emb VECTOR(3)) \
         WITH (engine='vector', primary='vector', vector_field='emb', dim=3, \
         payload_indexes=['tag'])",
        "INSERT INTO vp_clone (id, tag, emb) VALUES ('v1', 'first', ARRAY[1.0,0.0,0.0])",
        "INSERT INTO vp_clone (id, tag, emb) VALUES ('v2', 'second', ARRAY[0.0,1.0,0.0])",
    ] {
        server
            .exec(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    let expected = server.query_rows(ROWS).await.expect("source rows");
    assert_eq!(expected.len(), 2, "{expected:?}");
    assert_eq!(expected[0][0], "v1", "{expected:?}");
    let expected_filtered = server.query_rows(FILTERED).await.expect("source search");
    assert_eq!(expected_filtered, vec![vec!["v1".to_string()]]);

    for sql in [
        "USE DATABASE default",
        "CLONE DATABASE vp_clone_copy FROM vp_clone_src",
    ] {
        server
            .exec(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    let cancel = AtomicBool::new(false);
    run_scheduled_sweep(&server.shared, server.shared.credentials.catalog(), &cancel)
        .await
        .expect("run_scheduled_sweep");

    server
        .exec("USE DATABASE vp_clone_copy")
        .await
        .expect("USE vp_clone_copy");
    assert_eq!(
        server.query_rows(ROWS).await.expect("cloned rows"),
        expected,
        "the cloned rows read the source columns"
    );
    assert_eq!(
        server.query_rows(FILTERED).await.expect("cloned search"),
        expected_filtered,
        "a filtered search on the clone finds the source rows"
    );
}
