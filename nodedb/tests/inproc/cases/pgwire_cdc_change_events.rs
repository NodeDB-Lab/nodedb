// SPDX-License-Identifier: BUSL-1.1

//! pgwire SQL DML must raise Control-Plane CDC change events.
//!
//! ## The bug this guards against
//!
//! Every pgwire SQL write funnelled through `dispatch_local` submitted with no
//! change-feed owner, so `publish_change_event` was never reached from the SQL
//! path at all. `ChangeStream` subscribers — HTTP `/cdc`, WS-RPC live queries,
//! `SHOW CHANGES` — observed NOTHING from SQL DML, for every engine. Only the
//! internal autocommit funnel published, and no user SQL statement reaches it.
//!
//! This is distinct from the Event-Plane CDC / AFTER-trigger path, which is fed
//! by the Data Plane's `emit_write_event` and worked throughout: two separate
//! streams, only one of which was broken.
//!
//! ## Test shape
//!
//! Subscribe to the Control-Plane change stream BEFORE writing, then run
//! INSERT / UPDATE / DELETE over a real `tokio_postgres` client and assert one
//! event per statement, in statement order, with the right operation kind.
//! Single node: `cluster_transport` is `None`, so no NOTIFY broadcast is
//! involved and every event observed here must be published locally by the
//! write funnel, or the first `recv_filtered` times out.

use std::time::Duration;

use nodedb::control::change_stream::{ChangeEvent, ChangeOperation, Subscription};
use nodedb_test_support::pgwire_harness::TestServer;

/// Await the next event matching the subscription's filters, failing the test
/// rather than hanging if the write path published nothing (the bug).
async fn next_event(sub: &mut Subscription, what: &str) -> ChangeEvent {
    match tokio::time::timeout(Duration::from_secs(5), sub.recv_filtered()).await {
        Ok(Ok(event)) => event,
        Ok(Err(e)) => panic!("change stream closed while awaiting the {what} event: {e}"),
        Err(_) => panic!(
            "no Control-Plane change event for the {what}: pgwire SQL DML \
             published nothing to the change stream"
        ),
    }
}

#[tokio::test]
async fn pgwire_sql_dml_publishes_change_events() {
    let server = TestServer::start().await;

    server
        .exec("CREATE COLLECTION cdc_orders TYPE DOCUMENT (id STRING, status STRING)")
        .await
        .expect("CREATE COLLECTION cdc_orders");

    // Subscribe BEFORE any write: the stream is a broadcast bus with no replay
    // for events published before a receiver existed.
    let mut sub = server
        .shared
        .change_stream
        .subscribe(Some("cdc_orders".into()), None);

    server
        .exec("INSERT INTO cdc_orders (id, status) VALUES ('o1', 'new')")
        .await
        .expect("INSERT");
    server
        .exec("UPDATE cdc_orders SET status = 'shipped' WHERE id = 'o1'")
        .await
        .expect("UPDATE");
    server
        .exec("DELETE FROM cdc_orders WHERE id = 'o1'")
        .await
        .expect("DELETE");

    // One event per statement, in statement order. The event's document
    // identity is per-plan-variant and covered by the extractor's own unit
    // tests; what this test locks is that the SQL path publishes at all, for
    // the operation the statement performed.
    for (expected_op, what) in [
        (ChangeOperation::Insert, "INSERT"),
        (ChangeOperation::Update, "UPDATE"),
        (ChangeOperation::Delete, "DELETE"),
    ] {
        let event = next_event(&mut sub, what).await;
        assert_eq!(
            event.collection, "cdc_orders",
            "{what}: event published for the wrong collection"
        );
        assert_eq!(
            event.operation, expected_op,
            "{what}: change event carries the wrong operation kind"
        );
        assert!(
            !event.document_id.as_str().is_empty(),
            "{what}: change event carries no document identity"
        );
    }

    server.graceful_shutdown().await;
}
