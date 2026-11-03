// SPDX-License-Identifier: BUSL-1.1

//! A read nested in a transaction block's search joins the block's read
//! set. A concurrent commit that changes what the nested read matched fails
//! the block's COMMIT with `40001`.
//!
//! - An indexed equality prefilter reads the collection's secondary index.
//! - An `ARRAY_SLICE` prefilter reads the array's cells on every vShard.
//!
//! A concurrent write the nested read never observed leaves the COMMIT
//! alone. Each block also writes a row, so its COMMIT validates its reads.

use crate::harness::TestServer;

const PASSWORD: &str = "txn-nested-reads-5";

/// A search whose indexed equality on `agent_id` lowers to an index
/// lookup prefilter.
const INDEX_QUERY: &str = "SELECT id FROM nested_memo WHERE agent_id = 'a' \
                           AND embedding <-> ARRAY[1.0, 0.0, 0.0, 0.0] LIMIT 2";

/// A search fused with an `ARRAY_SLICE` prefilter over `nested_genome`.
const ARRAY_QUERY: &str = "SELECT id FROM nested_genes \
                           JOIN ARRAY_SLICE('nested_genome', '{chrom: [0, 0], pos: [0, 4]}') AS s \
                             ON id = s.qual \
                           ORDER BY vector_distance(embedding, [5.5, 0.0, 0.0]) \
                           LIMIT 10";

fn sqlstate(error: &tokio_postgres::Error) -> Option<String> {
    error.as_db_error().map(|e| e.code().code().to_string())
}

async fn exec(client: &tokio_postgres::Client, sql: &str) {
    client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// A second connection, as a user that can read and write.
async fn second_session(
    server: &TestServer,
    user: &str,
) -> (tokio_postgres::Client, tokio::task::JoinHandle<()>) {
    server
        .exec(&format!("CREATE USER {user} PASSWORD '{PASSWORD}'"))
        .await
        .expect("CREATE USER");
    server
        .exec(&format!("GRANT ROLE readwrite TO {user}"))
        .await
        .expect("GRANT ROLE");
    server
        .connect_as(user, PASSWORD)
        .await
        .expect("second connection")
}

/// The collection a block writes, and the one an unrelated rival writes.
async fn side_collections(server: &TestServer) {
    for collection in ["nested_side", "nested_unrelated"] {
        server
            .exec(&format!("CREATE COLLECTION {collection}"))
            .await
            .unwrap_or_else(|e| panic!("CREATE COLLECTION {collection}: {e}"));
    }
}

/// A strict collection with a vector index and a secondary index on
/// `agent_id`. Rows `a1`, `a2` belong to agent `a`, `b1` to agent `b`.
async fn seed_index(server: &TestServer) {
    for sql in [
        "CREATE COLLECTION nested_memo (\
           id STRING PRIMARY KEY, \
           agent_id STRING NOT NULL, \
           embedding VECTOR(4)) WITH (engine='document_strict')",
        "CREATE VECTOR INDEX idx_nested_memo ON nested_memo METRIC l2 DIM 4",
        "CREATE INDEX ON nested_memo FIELDS agent_id",
        "INSERT INTO nested_memo (id, agent_id, embedding) \
         VALUES ('a1', 'a', ARRAY[0.50, 0.50, 0.00, 0.00])",
        "INSERT INTO nested_memo (id, agent_id, embedding) \
         VALUES ('a2', 'a', ARRAY[0.40, 0.60, 0.00, 0.00])",
        "INSERT INTO nested_memo (id, agent_id, embedding) \
         VALUES ('b1', 'b', ARRAY[1.00, 0.00, 0.00, 0.00])",
    ] {
        server
            .exec(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
    }
    side_collections(server).await;
}

/// A document collection with a vector index, and two arrays: the one the
/// search slices and one it never reads.
async fn seed_array(server: &TestServer) {
    server
        .exec("CREATE COLLECTION nested_genes TYPE document")
        .await
        .expect("CREATE COLLECTION nested_genes");
    server
        .exec("CREATE VECTOR INDEX idx_nested_genes ON nested_genes (embedding) METRIC l2 DIM 3")
        .await
        .expect("CREATE VECTOR INDEX");
    for i in 0..4 {
        server
            .exec(&format!(
                "INSERT INTO nested_genes (id, embedding) VALUES ('g{i}', ARRAY[{}.0, 0.0, 0.0])",
                i + 1
            ))
            .await
            .unwrap_or_else(|e| panic!("INSERT g{i}: {e}"));
    }
    for array in ["nested_genome", "nested_unread_genome"] {
        server
            .exec(&format!(
                "CREATE ARRAY {array} \
                 DIMS (chrom INT64 [0..1], pos INT64 [0..4]) \
                 ATTRS (qual FLOAT64) \
                 TILE_EXTENTS (1, 1)"
            ))
            .await
            .unwrap_or_else(|e| panic!("CREATE ARRAY {array}: {e}"));
        server
            .exec(&format!(
                "INSERT INTO ARRAY {array} COORDS (0, 0) VALUES (1.0), COORDS (0, 1) VALUES (2.0)"
            ))
            .await
            .unwrap_or_else(|e| panic!("seed {array}: {e}"));
    }
    side_collections(server).await;
}

/// Run `query` in a block on the server's client, let `rival` commit
/// `rival_write`, write a row in the block, and return the COMMIT outcome.
async fn block_with_rival(
    server: &TestServer,
    rival: &tokio_postgres::Client,
    query: &str,
    rival_write: &str,
) -> Result<Vec<tokio_postgres::SimpleQueryMessage>, tokio_postgres::Error> {
    let a = &server.client;
    exec(a, "BEGIN").await;
    a.simple_query(query)
        .await
        .unwrap_or_else(|e| panic!("search in the block: {e}"));
    // The rival commits after the block's snapshot.
    exec(rival, rival_write).await;
    exec(a, "INSERT INTO nested_side (id, n) VALUES ('s', 1)").await;
    a.simple_query("COMMIT").await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_into_an_index_prefilter_value_aborts_the_block() {
    let server = TestServer::start().await;
    seed_index(&server).await;
    let (b, handle) = second_session(&server, "nested_index_b").await;

    let outcome = block_with_rival(
        &server,
        &b,
        INDEX_QUERY,
        "UPDATE nested_memo SET agent_id = 'a' WHERE id = 'b1'",
    )
    .await;
    let error = outcome.expect_err("the rival moved a row into the prefilter's value");
    assert_eq!(sqlstate(&error).as_deref(), Some("40001"), "{error:?}");

    drop(b);
    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_the_index_prefilter_never_read_commits() {
    let server = TestServer::start().await;
    seed_index(&server).await;
    let (b, handle) = second_session(&server, "nested_index_ctl").await;

    block_with_rival(
        &server,
        &b,
        INDEX_QUERY,
        "INSERT INTO nested_unrelated (id, n) VALUES ('u', 1)",
    )
    .await
    .expect("the rival wrote nothing the block read");

    drop(b);
    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cell_write_into_an_array_prefilter_aborts_the_block() {
    let server = TestServer::start().await;
    seed_array(&server).await;
    let (b, handle) = second_session(&server, "nested_array_b").await;

    let outcome = block_with_rival(
        &server,
        &b,
        ARRAY_QUERY,
        "INSERT INTO ARRAY nested_genome COORDS (0, 3) VALUES (4.0)",
    )
    .await;
    let error = outcome.expect_err("the rival wrote a cell inside the prefilter's slice");
    assert_eq!(sqlstate(&error).as_deref(), Some("40001"), "{error:?}");

    drop(b);
    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cell_write_to_an_array_the_prefilter_never_read_commits() {
    let server = TestServer::start().await;
    seed_array(&server).await;
    let (b, handle) = second_session(&server, "nested_array_ctl").await;

    block_with_rival(
        &server,
        &b,
        ARRAY_QUERY,
        "INSERT INTO ARRAY nested_unread_genome COORDS (0, 3) VALUES (4.0)",
    )
    .await
    .expect("the rival wrote an array the block never read");

    drop(b);
    handle.abort();
}
