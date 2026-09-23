// SPDX-License-Identifier: BUSL-1.1

//! A row an explicit pgwire transaction commits is visible to a PK point
//! lookup and a filtered aggregate, on the writing connection.
//!
//! The collection is chosen so its vShard lives on a core other than core 0.
//! A write staged on the wrong core leaves the owning core's overlay empty,
//! so COMMIT would resolve and install nothing there.

use nodedb_types::id::{DatabaseId, VShardId};

use crate::harness::TestServer;

const NUM_CORES: usize = 4;

fn collection_on_nonzero_core(prefix: &str) -> String {
    (0..64u32)
        .map(|i| format!("{prefix}_{i}"))
        .find(|name| {
            let vshard = VShardId::from_collection_in_database(DatabaseId::DEFAULT, name).as_u32();
            !(vshard as usize).is_multiple_of(NUM_CORES)
        })
        .expect("a candidate collection hashes off core 0")
}

async fn assert_committed_row_visible(server: &TestServer, coll: &str) {
    server.exec("BEGIN").await.unwrap();
    server
        .exec(&format!(
            "INSERT INTO {coll} (id, name) VALUES ('a1', 'alpha')"
        ))
        .await
        .unwrap();
    server.exec("COMMIT").await.unwrap();

    let point = server
        .query_text(&format!("SELECT id FROM {coll} WHERE id = 'a1'"))
        .await
        .unwrap();
    assert_eq!(point, vec!["a1"], "PK point lookup on {coll}");

    let count = server
        .query_text(&format!("SELECT count(*) FROM {coll} WHERE name = 'alpha'"))
        .await
        .unwrap();
    assert_eq!(count, vec!["1"], "filtered count on {coll}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pgwire_committed_strict_row_is_visible_to_a_point_lookup_on_a_nonzero_core() {
    let server = TestServer::start_multicores(NUM_CORES).await;
    let coll = collection_on_nonzero_core("pg_commit_vis_strict");
    server
        .exec(&format!(
            "CREATE COLLECTION {coll} (id STRING PRIMARY KEY, name STRING) \
             WITH (engine='document_strict')"
        ))
        .await
        .unwrap();

    assert_committed_row_visible(&server, &coll).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pgwire_committed_schemaless_row_is_visible_to_a_point_lookup_on_a_nonzero_core() {
    let server = TestServer::start_multicores(NUM_CORES).await;
    let coll = collection_on_nonzero_core("pg_commit_vis_doc");
    server
        .exec(&format!(
            "CREATE COLLECTION {coll} (id STRING PRIMARY KEY, name STRING) \
             WITH (engine='document_schemaless')"
        ))
        .await
        .unwrap();

    assert_committed_row_visible(&server, &coll).await;
}
