// SPDX-License-Identifier: BUSL-1.1

//! A vector search with an equality on a secondary-indexed field ranks only
//! the rows that field selects.

use nodedb_test_support::pgwire_harness::TestServer;

async fn seed(server: &TestServer, indexed: bool) {
    server
        .exec(
            "CREATE COLLECTION memo (\
               id STRING PRIMARY KEY, \
               agent_id STRING NOT NULL, \
               embedding VECTOR(4)) WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec("CREATE VECTOR INDEX idx_memo ON memo METRIC l2 DIM 4")
        .await
        .unwrap();
    if indexed {
        server
            .exec("CREATE INDEX ON memo FIELDS agent_id")
            .await
            .unwrap();
    }
    // The three rows nearest the query belong to agent b. Agent a owns the
    // rows farther out, so a search that ranks all rows first and filters
    // after cuts them off.
    for (id, agent, vector) in [
        ("b1", "b", "1.00, 0.00, 0.00, 0.00"),
        ("b2", "b", "0.99, 0.01, 0.00, 0.00"),
        ("b3", "b", "0.98, 0.02, 0.00, 0.00"),
        ("a1", "a", "0.50, 0.50, 0.00, 0.00"),
        ("a2", "a", "0.40, 0.60, 0.00, 0.00"),
        ("a3", "a", "0.00, 1.00, 0.00, 0.00"),
    ] {
        server
            .exec(&format!(
                "INSERT INTO memo (id, agent_id, embedding) VALUES ('{id}', '{agent}', ARRAY[{vector}])"
            ))
            .await
            .unwrap();
    }
}

const QUERY: &str = "SELECT id FROM memo WHERE agent_id = 'a' \
                     AND embedding <-> ARRAY[1.0, 0.0, 0.0, 0.0] LIMIT 2";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_indexed_equality_ranks_only_its_own_rows() {
    let server = TestServer::start().await;
    seed(&server, true).await;
    let ids = server.query_text(QUERY).await.unwrap();
    assert_eq!(ids, vec!["a1".to_string(), "a2".to_string()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_indexed_and_unindexed_searches_agree() {
    let indexed = TestServer::start().await;
    seed(&indexed, true).await;
    let plain = TestServer::start().await;
    seed(&plain, false).await;
    assert_eq!(
        indexed.query_text(QUERY).await.unwrap(),
        plain.query_text(QUERY).await.unwrap()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_value_no_row_holds_returns_nothing() {
    let server = TestServer::start().await;
    seed(&server, true).await;
    let ids = server
        .query_text(
            "SELECT id FROM memo WHERE agent_id = 'zzz' \
             AND embedding <-> ARRAY[1.0, 0.0, 0.0, 0.0] LIMIT 2",
        )
        .await
        .unwrap();
    assert!(ids.is_empty(), "{ids:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_row_written_after_the_index_is_found() {
    let server = TestServer::start().await;
    seed(&server, true).await;
    server
        .exec(
            "INSERT INTO memo (id, agent_id, embedding) \
             VALUES ('a0', 'a', ARRAY[0.97, 0.03, 0.00, 0.00])",
        )
        .await
        .unwrap();
    let ids = server.query_text(QUERY).await.unwrap();
    assert_eq!(ids, vec!["a0".to_string(), "a1".to_string()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_row_updated_out_of_the_value_is_not_returned() {
    let server = TestServer::start().await;
    seed(&server, true).await;
    server
        .exec("UPDATE memo SET agent_id = 'b' WHERE id = 'a1'")
        .await
        .unwrap();
    let ids = server.query_text(QUERY).await.unwrap();
    assert_eq!(ids, vec!["a2".to_string(), "a3".to_string()]);
}

/// Inside a block, the indexed equality selects its rows at the block's
/// snapshot. A row another session moves off the value after the snapshot
/// still ranks, and a row it moves onto the value does not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_block_selects_the_rows_of_its_snapshot() {
    const PASSWORD: &str = "prefilter-writer-7";
    let server = TestServer::start().await;
    seed(&server, true).await;
    server
        .exec(&format!(
            "CREATE USER prefilter_writer WITH PASSWORD '{PASSWORD}' ROLE readwrite"
        ))
        .await
        .unwrap();
    let (writer, _connection) = server
        .connect_as("prefilter_writer", PASSWORD)
        .await
        .unwrap();

    server.exec("BEGIN").await.unwrap();
    let before = server.query_text(QUERY).await.unwrap();
    assert_eq!(before, vec!["a1".to_string(), "a2".to_string()]);
    writer
        .simple_query("UPDATE memo SET agent_id = 'b' WHERE id = 'a1'")
        .await
        .unwrap();
    writer
        .simple_query("UPDATE memo SET agent_id = 'a' WHERE id = 'b1'")
        .await
        .unwrap();
    let inside = server.query_text(QUERY).await.unwrap();
    assert_eq!(inside, before, "the block reads its snapshot");
    server.exec("COMMIT").await.unwrap();

    let after = server.query_text(QUERY).await.unwrap();
    assert_eq!(after, vec!["b1".to_string(), "a2".to_string()]);
}
