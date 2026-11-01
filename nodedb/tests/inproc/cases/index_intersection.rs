// SPDX-License-Identifier: BUSL-1.1

//! A read with equalities on several secondary-indexed fields returns the
//! rows every equality selects, however many rows one field holds.

use nodedb_test_support::pgwire_harness::TestServer;

async fn seed(server: &TestServer, indexed: bool) {
    server
        .exec(
            "CREATE COLLECTION turns (\
               id STRING PRIMARY KEY, \
               agent_id STRING NOT NULL, \
               session_id STRING NOT NULL) WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    if indexed {
        server
            .exec("CREATE INDEX ON turns FIELDS agent_id")
            .await
            .unwrap();
        server
            .exec("CREATE INDEX ON turns FIELDS session_id")
            .await
            .unwrap();
    }
    // More rows than the old per-value cap of 1000 index entries.
    for chunk in (0..1100).collect::<Vec<_>>().chunks(100) {
        let rows: Vec<String> = chunk
            .iter()
            .map(|n| {
                let session = if n % 550 == 0 { "s-rare" } else { "s-common" };
                format!("('t{n:04}', 'a1', '{session}')")
            })
            .collect();
        server
            .exec(&format!(
                "INSERT INTO turns (id, agent_id, session_id) VALUES {}",
                rows.join(", ")
            ))
            .await
            .unwrap();
    }
    server
        .exec("INSERT INTO turns (id, agent_id, session_id) VALUES ('other', 'a2', 's-rare')")
        .await
        .unwrap();
}

const BOTH: &str =
    "SELECT id FROM turns WHERE agent_id = 'a1' AND session_id = 's-rare' ORDER BY id";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_indexed_equalities_return_the_shared_rows() {
    let server = TestServer::start().await;
    seed(&server, true).await;
    let ids = server.query_text(BOTH).await.unwrap();
    assert_eq!(ids, vec!["t0000".to_string(), "t0550".to_string()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn indexed_and_unindexed_reads_agree() {
    let indexed = TestServer::start().await;
    seed(&indexed, true).await;
    let plain = TestServer::start().await;
    seed(&plain, false).await;
    assert_eq!(
        indexed.query_text(BOTH).await.unwrap(),
        plain.query_text(BOTH).await.unwrap()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_value_with_over_a_thousand_rows_is_read_whole() {
    let server = TestServer::start().await;
    seed(&server, true).await;
    let ids = server
        .query_text("SELECT id FROM turns WHERE agent_id = 'a1'")
        .await
        .unwrap();
    assert_eq!(ids.len(), 1100);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_row_moved_onto_both_values_in_a_transaction_is_found() {
    let server = TestServer::start().await;
    seed(&server, true).await;
    server.exec("BEGIN").await.unwrap();
    server
        .exec("UPDATE turns SET session_id = 's-rare' WHERE id = 't0001'")
        .await
        .unwrap();
    let ids = server.query_text(BOTH).await.unwrap();
    server.exec("ROLLBACK").await.unwrap();
    assert_eq!(
        ids,
        vec![
            "t0000".to_string(),
            "t0001".to_string(),
            "t0550".to_string()
        ]
    );
}
