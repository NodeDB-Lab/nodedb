// SPDX-License-Identifier: BUSL-1.1

//! One HTTP request that holds several predicate writes.
//!
//! The statements of a request plan as one task set. A later statement reads
//! the rows the earlier ones wrote, as in a transaction block, so the whole
//! request applies as one transaction.

use nodedb_types::id::DatabaseId;

use crate::harness::TestServer;

/// Data Plane cores the server runs.
const CORES: usize = 2;

fn core_of(name: &str) -> usize {
    nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, name)
        .vshard()
        .as_u32() as usize
        % CORES
}

/// Two collection names that hash to different cores.
fn collections_on_two_cores() -> (String, String) {
    let first = "ps_one".to_owned();
    let second = (0..)
        .map(|n| format!("ps_two_{n}"))
        .find(|name| core_of(name) != core_of(&first))
        .expect("a name on the other core");
    (first, second)
}

async fn exec(server: &TestServer, sql: &str) {
    server
        .exec(sql)
        .await
        .unwrap_or_else(|e| panic!("run {sql}: {e}"));
}

async fn column(server: &TestServer, sql: &str) -> Vec<String> {
    server
        .query_text(sql)
        .await
        .unwrap_or_else(|e| panic!("read {sql}: {e}"))
}

/// A schemaless collection with one edge written into it, so its predicate
/// writes carry edge tasks.
async fn edge_bearing(server: &TestServer, name: &str) {
    exec(server, &format!("CREATE COLLECTION {name}")).await;
    for id in ["a", "b"] {
        exec(
            server,
            &format!("UPSERT INTO {name} {{ id: '{id}', n: 1, tag: 'x' }}"),
        )
        .await;
    }
    exec(
        server,
        &format!("GRAPH INSERT EDGE IN '{name}' FROM 'a' TO 'b' TYPE 'l'"),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_increments_of_one_row_in_one_request_both_apply() {
    let server = TestServer::start().await;
    edge_bearing(&server, "ps_inc").await;

    server
        .http_exec(
            "UPDATE ps_inc SET n = n + 1 WHERE id = 'a'; \
         UPDATE ps_inc SET n = n + 1 WHERE id = 'a'",
        )
        .await;

    assert_eq!(
        column(&server, "SELECT n FROM ps_inc WHERE id = 'a'").await,
        ["3"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_update_matches_the_value_the_first_wrote() {
    let server = TestServer::start().await;
    exec(&server, "CREATE COLLECTION ps_dep").await;
    exec(&server, "UPSERT INTO ps_dep { id: 'a', n: 1 }").await;
    exec(&server, "UPSERT INTO ps_dep { id: 'b', n: 5 }").await;
    exec(
        &server,
        "GRAPH INSERT EDGE IN 'ps_dep' FROM 'a' TO 'b' TYPE 'l'",
    )
    .await;

    server
        .http_exec("UPDATE ps_dep SET n = 2 WHERE n = 1; UPDATE ps_dep SET n = 3 WHERE n = 2")
        .await;

    assert_eq!(
        column(&server, "SELECT n FROM ps_dep WHERE id = 'a'").await,
        ["3"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_later_update_matches_the_tag_an_earlier_one_wrote() {
    let server = TestServer::start().await;
    edge_bearing(&server, "ps_chain").await;

    server
        .http_exec(
            "UPDATE ps_chain SET tag = 'moved' WHERE id = 'a'; \
         UPDATE ps_chain SET n = 7 WHERE tag = 'moved'",
        )
        .await;

    assert_eq!(
        column(&server, "SELECT id FROM ps_chain WHERE n = 7").await,
        ["a"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_third_statement_reads_the_first_across_two_vshards() {
    let server = TestServer::start_multicores(CORES).await;
    let (one, two) = collections_on_two_cores();
    for collection in [&one, &two] {
        exec(
            &server,
            &format!(
                "CREATE COLLECTION {collection} (id TEXT PRIMARY KEY, v TEXT) \
                 WITH (engine='document_strict')"
            ),
        )
        .await;
        exec(
            &server,
            &format!("INSERT INTO {collection} (id, v) VALUES ('r1', 'v1'), ('r2', 'keep')"),
        )
        .await;
    }

    server
        .http_exec(&format!(
            "UPDATE {one} SET v = 'v2' WHERE v = 'v1'; \
             UPDATE {two} SET v = 'w2' WHERE v = 'v1'; \
             UPDATE {one} SET v = 'v3' WHERE v = 'v2'"
        ))
        .await;

    assert_eq!(
        column(&server, &format!("SELECT v FROM {one} ORDER BY id")).await,
        ["v3", "keep"]
    );
    assert_eq!(
        column(&server, &format!("SELECT v FROM {two} ORDER BY id")).await,
        ["w2", "keep"]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_predicate_reads_the_point_write_before_it_across_two_vshards() {
    let server = TestServer::start_multicores(CORES).await;
    let (one, two) = collections_on_two_cores();
    for collection in [&one, &two] {
        exec(
            &server,
            &format!(
                "CREATE COLLECTION {collection} (id TEXT PRIMARY KEY, n INT) \
                 WITH (engine='document_strict')"
            ),
        )
        .await;
    }
    exec(
        &server,
        &format!("INSERT INTO {one} (id, n) VALUES ('x', 0)"),
    )
    .await;
    exec(
        &server,
        &format!("INSERT INTO {two} (id, n) VALUES ('y', 0)"),
    )
    .await;

    server
        .http_exec(&format!(
            "UPDATE {one} SET n = 1 WHERE id = 'x'; \
             UPDATE {one} SET n = 2 WHERE n = 1; \
             UPDATE {two} SET n = 9 WHERE id = 'y'"
        ))
        .await;

    assert_eq!(
        column(&server, &format!("SELECT n FROM {one} WHERE id = 'x'")).await,
        ["2"]
    );
    assert_eq!(
        column(&server, &format!("SELECT n FROM {two} WHERE id = 'y'")).await,
        ["9"]
    );
}
