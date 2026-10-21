// SPDX-License-Identifier: BUSL-1.1

//! Node identity and node labels on a graph read `AS OF SYSTEM TIME`.
//!
//! A historical MATCH takes each node's surrogate from the edge version its
//! cut resolves to, and each node's labels from the node-label version at
//! the cut. A node deleted after the cut keeps its properties and labels
//! there, across a restart too.

use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::harness::TestServer;

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after the epoch")
        .as_millis() as i64
}

/// Let the wall clock move past the last write's millisecond.
async fn tick() {
    tokio::time::sleep(Duration::from_millis(50)).await;
}

const MATCH_ADMIN: &str = "MATCH (a:Person)-[:knows]->(b) IN 'gbn_people'";

async fn match_admins(server: &TestServer, as_of: Option<i64>) -> Vec<String> {
    let cut = as_of.map_or(String::new(), |ms| format!(" AS OF SYSTEM TIME {ms}"));
    server
        .query_text(&format!(
            "{MATCH_ADMIN}{cut} WHERE a.role = 'admin' RETURN a"
        ))
        .await
        .expect("match admins")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_deleted_after_the_cut_keeps_its_labels_and_properties_there() {
    let server = TestServer::start().await;
    for sql in [
        "CREATE COLLECTION gbn_people WITH (engine='document_schemaless', bitemporal=true)",
        "INSERT INTO gbn_people (id, role) VALUES ('alice', 'admin')",
        "INSERT INTO gbn_people (id, role) VALUES ('bob', 'user')",
        "GRAPH INSERT EDGE IN 'gbn_people' FROM 'alice' TO 'bob' TYPE 'knows'",
        "GRAPH LABEL 'alice' AS 'Person'",
    ] {
        server.exec(sql).await.expect(sql);
    }
    tick().await;
    let cut = now_ms();
    tick().await;
    for sql in [
        "DELETE FROM gbn_people WHERE id = 'alice'",
        "GRAPH UNLABEL 'alice' AS 'Person'",
    ] {
        server.exec(sql).await.expect(sql);
    }

    assert_eq!(
        match_admins(&server, Some(cut)).await,
        vec!["alice".to_string()],
        "as of the cut alice is a Person with role admin"
    );
    assert!(
        match_admins(&server, None).await.is_empty(),
        "alice is gone now"
    );

    // A label set after the cut is not a label at the cut.
    server
        .exec("GRAPH LABEL 'bob' AS 'Person'")
        .await
        .expect("label bob");
    let bob_at_cut = server
        .query_text(&format!(
            "MATCH (a)-[:knows]->(b:Person) IN 'gbn_people' AS OF SYSTEM TIME {cut} RETURN b"
        ))
        .await
        .expect("match bob as a Person at the cut");
    assert!(
        bob_at_cut.is_empty(),
        "bob had no label at the cut: {bob_at_cut:?}"
    );

    let (server, dir) = server.take_dir();
    server.graceful_shutdown().await;
    let (server, _dir) = TestServer::open_on_path(dir).await;

    assert_eq!(
        match_admins(&server, Some(cut)).await,
        vec!["alice".to_string()],
        "the cut answers the same after a restart"
    );
    assert!(match_admins(&server, None).await.is_empty());
    let bob_at_cut = server
        .query_text(&format!(
            "MATCH (a)-[:knows]->(b:Person) IN 'gbn_people' AS OF SYSTEM TIME {cut} RETURN b"
        ))
        .await
        .expect("match bob as a Person at the cut after a restart");
    assert!(bob_at_cut.is_empty(), "{bob_at_cut:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_future_cut_on_a_single_node_reads_every_write_before_it() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION gbn_cut")
        .await
        .expect("create");
    let future = now_ms() + 3_600_000;
    for dst in ["b", "c", "d"] {
        server
            .exec(&format!(
                "GRAPH INSERT EDGE IN 'gbn_cut' FROM 'a' TO '{dst}' TYPE 'l'"
            ))
            .await
            .expect("insert edge");
        // The read follows the write with no pause: the cut must cover a
        // version stamped in the same millisecond.
        let read = server
            .query_text(&format!(
                "GRAPH NEIGHBORS IN 'gbn_cut' OF 'a' AS OF SYSTEM TIME {future}"
            ))
            .await
            .expect("neighbors at a future cut")
            .join("");
        assert!(
            read.contains(&format!("\"{dst}\"")),
            "the edge to {dst} written before the read is under its cut: {read}"
        );
    }
}
