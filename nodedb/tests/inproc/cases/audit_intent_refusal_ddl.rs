// SPDX-License-Identifier: BUSL-1.1

//! A statement whose first effect precedes its catalog entry refuses on a failed record.
//!
//! The catalog chokepoints record too late for these, so each handler records first.
//! Each test covers one path shape: refused under a failing audit WAL, nothing landed,
//! and the same statement succeeds once the WAL is back.

use nodedb::types::DatabaseId;
use nodedb_test_support::pgwire_harness::TestServer;
use nodedb_types::CloneStatus;

use super::audit_intent_refusal::{assert_refused, exec, intent_rows, refuse_audit, restore_audit};

fn database_id(server: &TestServer, name: &str) -> DatabaseId {
    server
        .shared
        .credentials
        .catalog()
        .get_database_id_by_name(name)
        .expect("catalog lookup")
        .expect("the database exists")
}

/// `ALTER DATABASE ... MATERIALIZE` copies nothing behind a failed record.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_record_leaves_a_clone_shadowed() {
    let server = TestServer::start().await;
    for sql in [
        "CREATE DATABASE refused_src",
        "USE DATABASE refused_src",
        "CREATE COLLECTION events (key STRING PRIMARY KEY, payload STRING) WITH (engine='kv')",
        "INSERT INTO events (key, payload) VALUES ('e1', 'data')",
        "USE DATABASE default",
        "CLONE DATABASE refused_clone FROM refused_src",
    ] {
        exec(&server, sql).await;
    }
    let materialize = "ALTER DATABASE refused_clone MATERIALIZE";
    let clone_status = || {
        let collections = server
            .shared
            .credentials
            .catalog()
            .load_all_collections(database_id(&server, "refused_clone"))
            .expect("catalog read");
        collections
            .into_iter()
            .find(|collection| collection.name == "events")
            .expect("the clone holds the collection")
            .clone_status
    };

    refuse_audit(&server);
    assert_refused(&server, materialize).await;
    assert!(
        matches!(clone_status(), CloneStatus::Shadowed),
        "a refused materialize copies nothing"
    );

    restore_audit(&server);
    exec(&server, materialize).await;
    assert!(!matches!(clone_status(), CloneStatus::Shadowed));
}

/// `DROP DATABASE ... FORCE` materializes no dependent clone behind a failed record.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_record_leaves_dependent_clones_of_a_forced_drop_shadowed() {
    let server = TestServer::start().await;
    for sql in [
        "CREATE DATABASE drop_src",
        "USE DATABASE drop_src",
        "CREATE COLLECTION events (key STRING PRIMARY KEY, payload STRING) WITH (engine='kv')",
        "USE DATABASE default",
        "CLONE DATABASE drop_clone FROM drop_src",
    ] {
        exec(&server, sql).await;
    }

    refuse_audit(&server);
    assert_refused(&server, "DROP DATABASE drop_src FORCE").await;

    let collections = server
        .shared
        .credentials
        .catalog()
        .load_all_collections(database_id(&server, "drop_clone"))
        .expect("catalog read");
    assert!(
        collections
            .iter()
            .all(|collection| matches!(collection.clone_status, CloneStatus::Shadowed)),
        "a refused drop copies nothing into its clones"
    );
    database_id(&server, "drop_src");
}

/// A refused `MOVE TENANT` writes no journal entry and moves no collection.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_record_refuses_a_tenant_move() {
    let server = TestServer::start().await;
    for sql in [
        "CREATE DATABASE mv_src",
        "USE DATABASE mv_src",
        "CREATE TENANT refused_mv ID 10",
        "CREATE COLLECTION orders (order_id STRING PRIMARY KEY, amount STRING) WITH (engine='kv')",
        "INSERT INTO orders (order_id, amount) VALUES ('o1', '100')",
        "USE DATABASE default",
        "CREATE DATABASE mv_tgt",
        "USE DATABASE mv_tgt",
        "CREATE COLLECTION orders (order_id STRING PRIMARY KEY, amount STRING) WITH (engine='kv')",
        "USE DATABASE default",
    ] {
        exec(&server, sql).await;
    }
    let move_tenant = "MOVE TENANT refused_mv FROM mv_src TO mv_tgt";
    let catalog = server.shared.credentials.catalog();

    refuse_audit(&server);
    assert_refused(&server, move_tenant).await;
    assert!(
        catalog
            .move_tenant_journal_scan_all()
            .expect("read the journal")
            .is_empty(),
        "a refused move writes no journal entry"
    );
    assert!(
        catalog
            .load_all_collections(database_id(&server, "mv_src"))
            .expect("catalog read")
            .iter()
            .any(|collection| collection.name == "orders" && collection.is_active),
        "a refused move leaves the source collection"
    );

    restore_audit(&server);
    exec(&server, move_tenant).await;
}

/// A sorted index keeps its tree and its records behind a failed record.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_record_refuses_engine_first_index_changes() {
    let server = TestServer::start().await;
    exec(
        &server,
        "CREATE COLLECTION board (id STRING PRIMARY KEY, score INT) WITH (engine='kv')",
    )
    .await;
    exec(&server, "INSERT INTO board (id, score) VALUES ('p1', 10)").await;
    let create = "CREATE SORTED INDEX board_idx ON board (score DESC) KEY id";
    let topk = "SELECT * FROM TOPK(board_idx, 10)";

    refuse_audit(&server);
    assert_refused(&server, create).await;
    assert!(
        server.query_rows(topk).await.unwrap_or_default().is_empty(),
        "a refused create builds no tree"
    );

    restore_audit(&server);
    exec(&server, create).await;

    refuse_audit(&server);
    assert_refused(&server, "DROP SORTED INDEX board_idx").await;
    assert_refused(&server, "DROP INDEX board_idx").await;
    let rows = server.query_rows(topk).await.expect("the tree survives");
    assert_eq!(rows.len(), 1, "a refused drop keeps the tree: {rows:?}");

    restore_audit(&server);
    exec(&server, "DROP SORTED INDEX board_idx").await;
}

/// A re-CREATE over a soft-deleted collection purges nothing behind a failed record.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_record_keeps_the_soft_deleted_collection_a_create_replaces() {
    let server = TestServer::start().await;
    for sql in [
        "CREATE COLLECTION recreated (id TEXT PRIMARY KEY, name TEXT) \
         WITH (engine='document_strict')",
        "INSERT INTO recreated (id, name) VALUES ('kept_row', 'kept')",
        "DROP COLLECTION recreated",
    ] {
        exec(&server, sql).await;
    }

    refuse_audit(&server);
    assert_refused(
        &server,
        "CREATE COLLECTION recreated (id TEXT PRIMARY KEY, name TEXT) \
         WITH (engine='document_strict')",
    )
    .await;

    restore_audit(&server);
    exec(&server, "UNDROP COLLECTION recreated").await;
    let rows = server
        .query_text("SELECT id FROM recreated")
        .await
        .expect("read the undropped collection");
    assert!(
        format!("{rows:?}").contains("kept_row"),
        "the old rows survive: {rows:?}"
    );
}

/// Statements on an in-memory store refuse, and a lockdown still runs during the outage.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_record_refuses_an_unlock_and_a_delegation_but_not_a_lockdown() {
    let server = TestServer::start().await;
    let delegate = "DELEGATE AUTH USER 'bob' AS AUTH USER 'alice' SCOPES 'read' REASON 'test'";

    refuse_audit(&server);
    assert_refused(&server, delegate).await;
    assert!(
        server
            .shared
            .impersonation
            .delegated_scopes("bob")
            .is_empty()
    );

    // A lockdown takes two admins. This approval is the first, and the statement is the second.
    server
        .shared
        .emergency
        .submit_two_party_approval("EMERGENCY LOCKDOWN", "first_admin");
    exec(&server, "EMERGENCY LOCKDOWN REASON 'outage'").await;
    assert!(server.shared.emergency.is_locked_down());
    assert_refused(&server, "EMERGENCY UNLOCK").await;
    assert!(
        server.shared.emergency.is_locked_down(),
        "a refused unlock keeps the lockdown"
    );

    restore_audit(&server);
    exec(&server, "EMERGENCY UNLOCK").await;
    assert!(!server.shared.emergency.is_locked_down());
    exec(&server, delegate).await;
    assert_eq!(
        server.shared.impersonation.delegated_scopes("bob"),
        ["read"]
    );
}

/// A refused tenant override leaves the session on its identity's tenant.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_record_refuses_a_session_tenant_override() {
    let server = TestServer::start().await;
    exec(&server, "CREATE TENANT refused_override ID 11").await;
    let set_tenant = "SET TENANT = 'refused_override'";

    refuse_audit(&server);
    assert_refused(&server, set_tenant).await;
    let before = intent_rows(&server, "tenant switched");

    restore_audit(&server);
    exec(&server, set_tenant).await;
    assert_eq!(intent_rows(&server, "tenant switched"), before + 1);
}
