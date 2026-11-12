// SPDX-License-Identifier: BUSL-1.1

//! Event Plane consumers in a database other than the default one.
//!
//! Outside the default database a write event names its collection as
//! storage keys it: `{database_id}/{name}`. Triggers, event definitions and
//! change streams are registered under the bare catalog name. Each consumer
//! must look its registry up by the bare name, or nothing in a non-default
//! database fires.
//!
//! A graph edge write emits its event on the edge collection, qualified the
//! same way, so a change stream on an edge collection receives it under the
//! bare name. An array write emits no Event Plane event: its change event
//! goes to the Control Plane change bus, which names the array bare.

use std::time::Duration;

use nodedb_test_support::pgwire_harness::TestServer;

async fn exec(server: &TestServer, sql: &str) {
    server
        .exec(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// Poll `sql` until it returns at least `want` rows, or time out. The Event
/// Plane consumes write events asynchronously.
async fn await_rows(server: &TestServer, sql: &str, want: usize) -> Vec<String> {
    let mut rows = Vec::new();
    for _ in 0..100 {
        rows = server.query_text(sql).await.unwrap_or_default();
        if rows.len() >= want {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    rows
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn after_trigger_fires_in_a_non_default_database() {
    let (server, _database) = TestServer::with_database("ev_trigger_db").await;
    exec(&server, "CREATE COLLECTION trig_src").await;
    exec(&server, "CREATE COLLECTION trig_log").await;
    exec(
        &server,
        "CREATE TRIGGER trig_on_ins AFTER INSERT ON trig_src FOR EACH ROW \
         BEGIN INSERT INTO trig_log (marker) VALUES ('fired'); END;",
    )
    .await;

    exec(&server, "INSERT INTO trig_src (id, v) VALUES ('r1', 1)").await;

    let rows = await_rows(&server, "SELECT marker FROM trig_log", 1).await;
    assert_eq!(
        rows.len(),
        1,
        "the AFTER trigger did not fire in a non-default database: {rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn define_event_fires_in_a_non_default_database() {
    let (server, _database) = TestServer::with_database("ev_define_db").await;
    exec(&server, "CREATE COLLECTION ev_src").await;
    exec(&server, "CREATE COLLECTION ev_log").await;
    exec(
        &server,
        "DEFINE EVENT ev_logged ON ev_src WHEN INSERT \
         THEN INSERT INTO ev_log (id) VALUES ($document_id)",
    )
    .await;

    exec(&server, "INSERT INTO ev_src (id, v) VALUES ('e1', 1)").await;

    let rows = await_rows(&server, "SELECT id FROM ev_log", 1).await;
    assert_eq!(
        rows.len(),
        1,
        "the DEFINE EVENT action did not run in a non-default database: {rows:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn change_stream_routes_and_names_the_bare_collection_in_a_non_default_database() {
    let (server, _database) = TestServer::with_database("ev_cdc_db").await;
    exec(&server, "CREATE COLLECTION cdc_src").await;
    exec(&server, "CREATE CHANGE STREAM cdc_changes ON cdc_src").await;
    exec(&server, "CREATE CONSUMER GROUP cdc_readers ON cdc_changes").await;

    exec(&server, "INSERT INTO cdc_src (id, v) VALUES ('c1', 1)").await;

    let sql = "SELECT * FROM STREAM cdc_changes CONSUMER GROUP cdc_readers LIMIT 10";
    let mut events = Vec::new();
    for _ in 0..100 {
        events = server.query_named_rows(sql).await.unwrap_or_default();
        if !events.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        events.len(),
        1,
        "the change stream received no event in a non-default database"
    );
    assert_eq!(
        events[0].get("collection").map(String::as_str),
        Some("cdc_src"),
        "the change event must name the collection as the stream does"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graph_edge_event_routes_by_the_bare_collection_in_a_non_default_database() {
    let (server, _database) = TestServer::with_database("ev_edge_db").await;
    exec(&server, "CREATE COLLECTION edge_src").await;
    exec(&server, "CREATE CHANGE STREAM edge_changes ON edge_src").await;
    exec(
        &server,
        "CREATE CONSUMER GROUP edge_readers ON edge_changes",
    )
    .await;

    exec(
        &server,
        "GRAPH INSERT EDGE IN 'edge_src' FROM 'alice' TO 'bob' TYPE 'knows'",
    )
    .await;

    let sql = "SELECT * FROM STREAM edge_changes CONSUMER GROUP edge_readers LIMIT 10";
    let mut events = Vec::new();
    for _ in 0..100 {
        events = server.query_named_rows(sql).await.unwrap_or_default();
        if !events.is_empty() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        !events.is_empty(),
        "the change stream received no graph edge event in a non-default database"
    );
    assert!(
        events
            .iter()
            .all(|event| event.get("collection").map(String::as_str) == Some("edge_src")),
        "a graph edge event must name its collection bare: {events:?}"
    );
}
