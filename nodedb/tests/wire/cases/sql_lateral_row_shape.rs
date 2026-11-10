// SPDX-License-Identifier: BUSL-1.1

//! LATERAL reads each outer row under the row shape its outer plan produces.
//! A user column named `data` is a value like any other, never a scan
//! envelope.

use crate::harness::TestServer;

async fn setup(server: &TestServer) {
    for sql in [
        "CREATE COLLECTION lrs_outer (id TEXT PRIMARY KEY, data TEXT, uid TEXT, min_score INT) \
         WITH (engine='document_strict')",
        "CREATE COLLECTION lrs_events (id TEXT PRIMARY KEY, uid TEXT, score INT) \
         WITH (engine='document_strict')",
        "INSERT INTO lrs_outer (id, data, uid, min_score) VALUES ('o1', 'alpha', 'u1', 15)",
        "INSERT INTO lrs_outer (id, data, uid, min_score) VALUES ('o2', 'beta', 'u2', 5)",
        "INSERT INTO lrs_events (id, uid, score) VALUES ('e1', 'u1', 10)",
        "INSERT INTO lrs_events (id, uid, score) VALUES ('e2', 'u1', 30)",
        "INSERT INTO lrs_events (id, uid, score) VALUES ('e3', 'u2', 20)",
    ] {
        server
            .exec(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
}

fn sorted(mut rows: Vec<Vec<String>>) -> Vec<Vec<String>> {
    rows.sort();
    rows
}

fn row(cells: &[&str]) -> Vec<String> {
    cells.iter().map(|c| c.to_string()).collect()
}

/// Top-k LATERAL correlates on `uid` and projects the outer `data` column.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn top_k_lateral_keeps_an_outer_column_named_data() {
    let server = TestServer::start().await;
    setup(&server).await;

    let rows = server
        .query_rows(
            "SELECT o.id, o.data, best.id \
             FROM lrs_outer o, \
                  LATERAL (\
                      SELECT e.id FROM lrs_events e \
                      WHERE e.uid = o.uid \
                      ORDER BY e.score DESC \
                      LIMIT 1\
                  ) best",
        )
        .await
        .expect("top-k LATERAL over an outer column named data");
    assert_eq!(
        sorted(rows),
        vec![row(&["o1", "alpha", "e2"]), row(&["o2", "beta", "e3"])]
    );
}

/// A non-equi LATERAL binds the outer row's columns, and `data` stays a value.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn loop_lateral_keeps_an_outer_column_named_data() {
    let server = TestServer::start().await;
    setup(&server).await;

    let rows = server
        .query_rows(
            "SELECT o.data, x.id \
             FROM lrs_outer o, \
                  LATERAL (\
                      SELECT e.id FROM lrs_events e \
                      WHERE e.uid = o.uid AND e.score > o.min_score\
                  ) x",
        )
        .await
        .expect("loop LATERAL over an outer column named data");
    assert_eq!(
        sorted(rows),
        vec![row(&["alpha", "e2"]), row(&["beta", "e3"])]
    );
}
