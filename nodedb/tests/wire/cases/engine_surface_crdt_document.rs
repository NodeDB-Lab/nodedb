// SPDX-License-Identifier: BUSL-1.1

//! Engine surface tests for "CRDT via SQL" through the real pgwire server.
//!
//! A document collection created with `WITH (crdt='true')` routes its SQL DML
//! through the CRDT (Loro) engine. PK-targeted INSERT / UPSERT (full replace) /
//! UPDATE SET (literal RHS) / DELETE lower to `CrdtOp::DocUpsert` / `DocDelete`,
//! and their effects are materialized into the document store so ordinary
//! `SELECT` reads observe them. `UPDATE`/`DELETE ... RETURNING` project the
//! updated / deleted row exactly like the non-CRDT `PointUpdate` / `PointDelete`
//! path. Every unsupported shape — predicate (non-PK) UPDATE/DELETE and
//! non-literal UPDATE RHS — is rejected with a typed error rather than silently
//! falling through to a non-CRDT path (which would bypass CRDT convergence).
//!
//! These cases prove the round trip end to end: the Data Plane CRDT write must
//! materialize into the readable store, partial UPDATE must be
//! last-writer-wins per field (untouched fields survive), and DELETE must
//! remove the row from current-state reads.

use crate::harness::TestServer;

/// INSERT by primary key into a `crdt='true'` collection must materialize into
/// the readable document store — a plain `SELECT` sees every inserted row.
#[tokio::test]
async fn crdt_insert_and_select_round_trips() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE TABLE crdt_notes (id TEXT PRIMARY KEY, title TEXT, body TEXT) \
         WITH (crdt='true')",
    )
    .await
    .unwrap();

    srv.exec("INSERT INTO crdt_notes (id, title, body) VALUES ('a', 't1', 'b1')")
        .await
        .unwrap();
    srv.exec("INSERT INTO crdt_notes (id, title, body) VALUES ('b', 't2', 'b2')")
        .await
        .unwrap();

    let rows = srv
        .query_rows("SELECT id, title FROM crdt_notes ORDER BY id")
        .await
        .unwrap();
    let pairs: Vec<(&str, &str)> = rows
        .iter()
        .map(|r| (r[0].as_str(), r[1].as_str()))
        .collect();

    assert_eq!(
        pairs,
        vec![("a", "t1"), ("b", "t2")],
        "CRDT INSERT must materialize into the readable store; got {pairs:?}"
    );
}

/// A partial `UPDATE SET` (literal RHS) on a CRDT collection is
/// last-writer-wins PER FIELD: the touched field changes and every untouched
/// field survives. This is the load-bearing CRDT merge assertion.
#[tokio::test]
async fn crdt_update_set_merges_fields() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE TABLE crdt_notes (id TEXT PRIMARY KEY, title TEXT, body TEXT) \
         WITH (crdt='true')",
    )
    .await
    .unwrap();

    srv.exec("INSERT INTO crdt_notes (id, title, body) VALUES ('a', 't1', 'b1')")
        .await
        .unwrap();

    srv.exec("UPDATE crdt_notes SET title='t2' WHERE id='a'")
        .await
        .unwrap();

    let rows = srv
        .query_rows("SELECT id, title, body FROM crdt_notes WHERE id='a'")
        .await
        .unwrap();

    assert_eq!(
        rows.len(),
        1,
        "exactly one row must remain after partial UPDATE; got {rows:?}"
    );
    assert_eq!(
        rows[0][1].as_str(),
        "t2",
        "the touched field `title` must reflect the new value; got {:?}",
        rows[0]
    );
    assert_eq!(
        rows[0][2].as_str(),
        "b1",
        "the untouched field `body` must survive the partial UPDATE (LWW-per-field); got {:?}",
        rows[0]
    );
}

/// A PK-targeted `DELETE` on a CRDT collection removes the row from
/// current-state reads (tombstone + sparse-store removal).
#[tokio::test]
async fn crdt_delete_removes_row() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE TABLE crdt_notes (id TEXT PRIMARY KEY, title TEXT, body TEXT) \
         WITH (crdt='true')",
    )
    .await
    .unwrap();

    srv.exec("INSERT INTO crdt_notes (id, title, body) VALUES ('a', 't1', 'b1')")
        .await
        .unwrap();

    srv.exec("DELETE FROM crdt_notes WHERE id='a'")
        .await
        .unwrap();

    let rows = srv
        .query_rows("SELECT id FROM crdt_notes WHERE id='a'")
        .await
        .unwrap();

    assert!(
        rows.is_empty(),
        "DELETE must remove the row from current-state reads; got {rows:?}"
    );
}

/// A full-replace `UPSERT` onto an existing primary key on a CRDT collection
/// replaces the row wholesale (CRDT last-writer-wins full replace).
#[tokio::test]
async fn crdt_upsert_replaces() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE TABLE crdt_notes (id TEXT PRIMARY KEY, title TEXT, body TEXT) \
         WITH (crdt='true')",
    )
    .await
    .unwrap();

    srv.exec("INSERT INTO crdt_notes (id, title, body) VALUES ('a', 't1', 'b1')")
        .await
        .unwrap();

    // Full-replace UPSERT form (no ON CONFLICT clause) — routes to
    // `CrdtOp::DocUpsert` full replace on a CRDT collection.
    srv.exec("UPSERT INTO crdt_notes (id, title, body) VALUES ('a', 't9', 'b9')")
        .await
        .unwrap();

    let rows = srv
        .query_rows("SELECT id, title, body FROM crdt_notes WHERE id='a'")
        .await
        .unwrap();

    assert_eq!(
        rows.len(),
        1,
        "UPSERT on an existing PK must not duplicate the row; got {rows:?}"
    );
    assert_eq!(
        (rows[0][1].as_str(), rows[0][2].as_str()),
        ("t9", "b9"),
        "UPSERT must replace all fields of the existing row; got {:?}",
        rows[0]
    );
}

/// A predicate (non-primary-key) `UPDATE` on a CRDT collection must be
/// rejected — there is no silent fallthrough to a non-CRDT path.
#[tokio::test]
async fn predicate_update_on_crdt_rejected() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE TABLE crdt_notes (id TEXT PRIMARY KEY, title TEXT, body TEXT) \
         WITH (crdt='true')",
    )
    .await
    .unwrap();

    srv.exec("INSERT INTO crdt_notes (id, title, body) VALUES ('a', 't1', 'b1')")
        .await
        .unwrap();

    srv.expect_error(
        "UPDATE crdt_notes SET title='x' WHERE title='t1'",
        "predicate (non-primary-key) UPDATE on CRDT collection",
    )
    .await;
}

/// A predicate (non-primary-key) `DELETE` on a CRDT collection must be
/// rejected — there is no silent fallthrough to a non-CRDT path.
#[tokio::test]
async fn predicate_delete_on_crdt_rejected() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE TABLE crdt_notes (id TEXT PRIMARY KEY, title TEXT, body TEXT) \
         WITH (crdt='true')",
    )
    .await
    .unwrap();

    srv.exec("INSERT INTO crdt_notes (id, title, body) VALUES ('a', 't1', 'b1')")
        .await
        .unwrap();

    srv.expect_error(
        "DELETE FROM crdt_notes WHERE title='t1'",
        "predicate (non-primary-key) DELETE on CRDT collection",
    )
    .await;
}

/// `UPDATE ... RETURNING` on a CRDT collection projects the updated row,
/// mirroring the non-CRDT `PointUpdate` RETURNING path.
#[tokio::test]
async fn crdt_update_returning_projects_updated_row() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE TABLE crdt_notes (id TEXT PRIMARY KEY, title TEXT, body TEXT) \
         WITH (crdt='true')",
    )
    .await
    .unwrap();

    srv.exec("INSERT INTO crdt_notes (id, title, body) VALUES ('a', 't1', 'b1')")
        .await
        .unwrap();

    let rows = srv
        .query_rows("UPDATE crdt_notes SET title='t2' WHERE id='a' RETURNING id, title")
        .await
        .unwrap();

    assert_eq!(
        rows.len(),
        1,
        "UPDATE ... RETURNING must project exactly the updated row; got {rows:?}"
    );
    assert_eq!(
        (rows[0][0].as_str(), rows[0][1].as_str()),
        ("a", "t2"),
        "UPDATE ... RETURNING must reflect the post-update field values; got {:?}",
        rows[0]
    );
}

/// `DELETE ... RETURNING` on a CRDT collection projects the deleted row,
/// mirroring the non-CRDT `PointDelete` RETURNING path.
#[tokio::test]
async fn crdt_delete_returning_projects_deleted_row() {
    let srv = TestServer::start().await;
    srv.exec(
        "CREATE TABLE crdt_notes (id TEXT PRIMARY KEY, title TEXT, body TEXT) \
         WITH (crdt='true')",
    )
    .await
    .unwrap();

    srv.exec("INSERT INTO crdt_notes (id, title, body) VALUES ('a', 't1', 'b1')")
        .await
        .unwrap();

    let rows = srv
        .query_rows("DELETE FROM crdt_notes WHERE id='a' RETURNING id")
        .await
        .unwrap();

    assert_eq!(
        rows.len(),
        1,
        "DELETE ... RETURNING must project exactly the deleted row; got {rows:?}"
    );
    assert_eq!(
        rows[0][0].as_str(),
        "a",
        "DELETE ... RETURNING must return the deleted row's id; got {:?}",
        rows[0]
    );

    let remaining = srv
        .query_rows("SELECT id FROM crdt_notes WHERE id='a'")
        .await
        .unwrap();
    assert!(
        remaining.is_empty(),
        "DELETE ... RETURNING must still remove the row; got {remaining:?}"
    );
}

/// The CRDT gate is read from a catalog keyed by the BARE collection name, so a
/// non-default database must refuse a predicate UPDATE exactly as `default`
/// does — mirrors `predicate_update_on_crdt_rejected`.
#[tokio::test]
async fn predicate_update_on_crdt_rejected_in_non_default_database() {
    let (srv, _db) = TestServer::with_database("crdt_pred_upd_scope").await;
    srv.exec(
        "CREATE TABLE crdt_notes_nd (id TEXT PRIMARY KEY, title TEXT, body TEXT) \
         WITH (crdt='true')",
    )
    .await
    .unwrap();

    srv.exec("INSERT INTO crdt_notes_nd (id, title, body) VALUES ('a', 't1', 'b1')")
        .await
        .unwrap();

    srv.expect_error(
        "UPDATE crdt_notes_nd SET title='x' WHERE title='t1'",
        "predicate (non-primary-key) UPDATE on CRDT collection",
    )
    .await;
}

/// The DELETE side of the same gate — mirrors
/// `predicate_delete_on_crdt_rejected`.
#[tokio::test]
async fn predicate_delete_on_crdt_rejected_in_non_default_database() {
    let (srv, _db) = TestServer::with_database("crdt_pred_del_scope").await;
    srv.exec(
        "CREATE TABLE crdt_notes_nd (id TEXT PRIMARY KEY, title TEXT, body TEXT) \
         WITH (crdt='true')",
    )
    .await
    .unwrap();

    srv.exec("INSERT INTO crdt_notes_nd (id, title, body) VALUES ('a', 't1', 'b1')")
        .await
        .unwrap();

    srv.expect_error(
        "DELETE FROM crdt_notes_nd WHERE title='t1'",
        "predicate (non-primary-key) DELETE on CRDT collection",
    )
    .await;
}

/// An explicit `ON CONFLICT DO UPDATE SET` on a CRDT collection is refused —
/// CRDT convergence IS the LWW full replace, so the caller's merge clause has
/// nowhere to run. The refusal rides the same catalog gate, so a non-default
/// database must refuse it too.
///
/// `INSERT ... ON CONFLICT DO UPDATE SET` is the form that reaches the upsert
/// converter with the clause attached; the `UPSERT INTO` form is consumed and
/// rebuilt by the protocol-neutral collection DML parser before planning.
#[tokio::test]
async fn upsert_on_conflict_do_update_on_crdt_rejected_in_non_default_database() {
    let (srv, _db) = TestServer::with_database("crdt_upsert_scope").await;
    srv.exec(
        "CREATE TABLE crdt_notes_nd (id TEXT PRIMARY KEY, title TEXT, body TEXT) \
         WITH (crdt='true')",
    )
    .await
    .unwrap();

    srv.exec("INSERT INTO crdt_notes_nd (id, title, body) VALUES ('a', 't1', 'b1')")
        .await
        .unwrap();

    srv.expect_error(
        "INSERT INTO crdt_notes_nd (id, title, body) VALUES ('a', 't9', 'b9') \
         ON CONFLICT (id) DO UPDATE SET title = 't9'",
        "UPSERT with ON CONFLICT DO UPDATE on CRDT collection",
    )
    .await;
}
