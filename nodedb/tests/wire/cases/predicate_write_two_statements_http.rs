// SPDX-License-Identifier: BUSL-1.1

//! One HTTP request that holds a predicate `UPDATE` of each of two
//! collections on different Data Plane cores.
//!
//! The request plans as one task set that spans two vShards, so it commits
//! through Calvin. Each predicate write must apply to the rows its own
//! predicate matched, never to the rows the other one matched.

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
    let first = "pw_one".to_owned();
    let second = (0..)
        .map(|n| format!("pw_two_{n}"))
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

async fn values(server: &TestServer, collection: &str) -> Vec<String> {
    server
        .query_text(&format!("SELECT v FROM {collection} ORDER BY id"))
        .await
        .unwrap_or_else(|e| panic!("read {collection}: {e}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_predicate_updates_in_one_http_request_each_touch_their_own_rows() {
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
    }
    for (collection, tag) in [(&one, "one"), (&two, "two")] {
        exec(
            &server,
            &format!("INSERT INTO {collection} (id, v) VALUES ('r1', '{tag}-old'), ('r2', 'keep')"),
        )
        .await;
    }

    let sql = format!(
        "UPDATE {one} SET v = 'one-new' WHERE v = 'one-old'; \
         UPDATE {two} SET v = 'two-new' WHERE v = 'two-old'"
    );
    server.http_exec(&sql).await;

    assert_eq!(values(&server, &one).await, ["one-new", "keep"]);
    assert_eq!(values(&server, &two).await, ["two-new", "keep"]);
}
