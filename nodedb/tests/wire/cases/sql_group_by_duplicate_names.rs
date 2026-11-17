// SPDX-License-Identifier: BUSL-1.1

//! An aggregate whose SELECT list repeats an output name keeps every cell,
//! as a plain SELECT does: `SELECT a AS x, b AS x` is legal SQL.

use crate::harness::TestServer;

async fn setup(server: &TestServer) {
    for sql in [
        "CREATE COLLECTION gbd_items (id TEXT PRIMARY KEY, a TEXT, b TEXT) \
         WITH (engine='document_strict')",
        "INSERT INTO gbd_items (id, a, b) VALUES ('i1', 'a1', 'b1')",
        "INSERT INTO gbd_items (id, a, b) VALUES ('i2', 'a1', 'b1')",
        "INSERT INTO gbd_items (id, a, b) VALUES ('i3', 'a2', 'b2')",
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

/// Two GROUP BY keys under one alias keep their own cells.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_group_keys_under_one_alias_keep_their_cells() {
    let server = TestServer::start().await;
    setup(&server).await;

    let rows = server
        .query_rows("SELECT a AS x, b AS x, count(*) FROM gbd_items GROUP BY a, b")
        .await
        .expect("a repeated GROUP BY alias");
    assert_eq!(
        sorted(rows),
        vec![row(&["a1", "b1", "2"]), row(&["a2", "b2", "1"])]
    );
}

/// A GROUP BY key aliased to an aggregate's name keeps its own cell.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_group_key_aliased_to_an_aggregate_name_keeps_its_cell() {
    let server = TestServer::start().await;
    setup(&server).await;

    let rows = server
        .query_rows("SELECT a AS n, count(*) AS n FROM gbd_items GROUP BY a")
        .await
        .expect("a GROUP BY alias equal to an aggregate alias");
    assert_eq!(sorted(rows), vec![row(&["a1", "2"]), row(&["a2", "1"])]);
}
