// SPDX-License-Identifier: BUSL-1.1

//! Affected-row counts on autocommit DML must be REPORTED by the write that
//! ran, never assumed by the caller that dispatched it.
//!
//! A client reads the count out of the pgwire `CommandComplete` tag (`DELETE n`
//! / `UPDATE n`) and uses it to answer "did my statement touch a row?" — an ORM
//! turns it directly into the boolean its `delete` / `update` returns. That
//! answer is only usable when the count is derived from the actual mutation
//! outcome, so every case below asserts the count against the observable state
//! (`SELECT count(*)`) rather than against the plan's expectation.
//!
//! The counts covered here span every point-write engine surface that can
//! legitimately match nothing: schemaless document, strict document, key-value,
//! CRDT documents, `DELETE ... RETURNING`, and the predicate (non-PK) path.

use crate::harness::TestServer;
use tokio_postgres::SimpleQueryMessage;

/// Affected-row count carried by the first `CommandComplete` in a simple-query
/// response (PostgreSQL's `INSERT 0 N` / `UPDATE N` / `DELETE N` count).
fn command_count(msgs: &[SimpleQueryMessage]) -> Option<u64> {
    msgs.iter().find_map(|m| match m {
        SimpleQueryMessage::CommandComplete(n) => Some(*n),
        _ => None,
    })
}

/// Run `sql` and return the affected-row count from its command tag.
async fn affected(server: &TestServer, sql: &str) -> u64 {
    let msgs = server
        .client
        .simple_query(sql)
        .await
        .unwrap_or_else(|e| panic!("statement should succeed: {sql}: {e}"));
    command_count(&msgs).unwrap_or_else(|| panic!("no CommandComplete tag for: {sql}"))
}

/// Number of rows a `SELECT count(*)` reports — the observable state the
/// affected count must agree with.
async fn live_rows(server: &TestServer, sql: &str) -> u64 {
    let rows = server
        .query_text(sql)
        .await
        .unwrap_or_else(|e| panic!("count query should succeed: {sql}: {e}"));
    rows.first()
        .unwrap_or_else(|| panic!("count query returned no row: {sql}"))
        .parse()
        .unwrap_or_else(|e| panic!("count query returned a non-integer: {sql}: {e}"))
}

/// Re-deleting a primary key that was already deleted must report `0`: the row
/// is gone, so the statement removed nothing. The surrogate assigned to that
/// primary key survives the delete by design (surrogate identity is stable
/// across a delete + re-insert), so a resolvable surrogate must NOT be read as
/// evidence that a row was there to remove.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn redelete_of_deleted_primary_key_reports_zero() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION del_probe (id TEXT PRIMARY KEY, v INT)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO del_probe (id, v) VALUES ('a', 1)")
        .await
        .unwrap();

    let first = affected(&server, "DELETE FROM del_probe WHERE id = 'a'").await;
    assert_eq!(first, 1, "the delete that removed the row must report 1");

    let second = affected(&server, "DELETE FROM del_probe WHERE id = 'a'").await;
    assert_eq!(
        second, 0,
        "re-deleting an already-deleted primary key removes nothing and must report 0"
    );

    // The count must agree with the observable state, not merely be small: the
    // row is genuinely absent, which is exactly what makes a reported 1 a lie.
    assert_eq!(
        live_rows(&server, "SELECT count(*) FROM del_probe WHERE id = 'a'").await,
        0,
        "the row must really be absent after the first delete"
    );
}

/// A primary key that has never existed reports `0`. This is the path clients
/// can already rely on; it is asserted here so a fix to the re-delete count
/// cannot regress it into reporting 1 for a never-existing key either.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_of_never_existing_primary_key_reports_zero() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION del_probe (id TEXT PRIMARY KEY, v INT)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO del_probe (id, v) VALUES ('a', 1)")
        .await
        .unwrap();

    let count = affected(&server, "DELETE FROM del_probe WHERE id = 'never_existed'").await;
    assert_eq!(
        count, 0,
        "deleting a primary key that never existed must report 0"
    );
    assert_eq!(
        live_rows(&server, "SELECT count(*) FROM del_probe").await,
        1,
        "a delete that matched nothing must not remove the row that does exist"
    );
}

/// The count must follow the row through a full insert → delete → re-insert →
/// delete cycle: `1, 0, 1, 0`. A count that latches on after the first delete
/// keeps reporting a hit for the rest of the collection's lifetime.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_count_tracks_reinsert_delete_cycle() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION del_probe (id TEXT PRIMARY KEY, v INT)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO del_probe (id, v) VALUES ('a', 1)")
        .await
        .unwrap();

    let mut counts = Vec::new();
    counts.push(affected(&server, "DELETE FROM del_probe WHERE id = 'a'").await);
    counts.push(affected(&server, "DELETE FROM del_probe WHERE id = 'a'").await);
    server
        .exec("INSERT INTO del_probe (id, v) VALUES ('a', 2)")
        .await
        .unwrap();
    counts.push(affected(&server, "DELETE FROM del_probe WHERE id = 'a'").await);
    counts.push(affected(&server, "DELETE FROM del_probe WHERE id = 'a'").await);

    assert_eq!(
        counts,
        vec![1, 0, 1, 0],
        "the delete count must follow the row across a re-insert, not latch on"
    );
}

/// An `UPDATE` targeting a previously-deleted primary key reports `0`. Same
/// surrogate-survives-delete situation as the re-delete, so the update count
/// must come from the row read, not from the plan resolving a surrogate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn update_of_deleted_primary_key_reports_zero() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION del_probe (id TEXT PRIMARY KEY, v INT)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO del_probe (id, v) VALUES ('a', 1)")
        .await
        .unwrap();
    server
        .exec("DELETE FROM del_probe WHERE id = 'a'")
        .await
        .unwrap();

    let count = affected(&server, "UPDATE del_probe SET v = 9 WHERE id = 'a'").await;
    assert_eq!(
        count, 0,
        "updating an already-deleted primary key changes nothing and must report 0"
    );
    assert_eq!(
        live_rows(&server, "SELECT count(*) FROM del_probe WHERE id = 'a'").await,
        0,
        "the update must not resurrect the deleted row"
    );
}

/// The strict-document storage mode shares the point-delete path, so it shares
/// the count contract: a re-delete reports `0`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn strict_redelete_of_deleted_primary_key_reports_zero() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION del_strict (id TEXT NOT NULL PRIMARY KEY, v INT) \
             WITH (engine='document_strict')",
        )
        .await
        .unwrap();
    server
        .exec("INSERT INTO del_strict (id, v) VALUES ('a', 1)")
        .await
        .unwrap();

    assert_eq!(
        affected(&server, "DELETE FROM del_strict WHERE id = 'a'").await,
        1,
        "the delete that removed the row must report 1"
    );
    assert_eq!(
        affected(&server, "DELETE FROM del_strict WHERE id = 'a'").await,
        0,
        "re-deleting an already-deleted strict-document row must report 0"
    );
}

/// A key-value delete that removed the key reports `1`. The KV engine already
/// counts the keys it removed; the count has to survive the trip back to the
/// client rather than being replaced by the plan's response classification.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_delete_of_existing_key_reports_one() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION kv_probe (key TEXT PRIMARY KEY, n INT) WITH (engine='kv')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO kv_probe (key, n) VALUES ('a', 1)")
        .await
        .unwrap();

    let count = affected(&server, "DELETE FROM kv_probe WHERE key = 'a'").await;
    assert_eq!(count, 1, "the KV delete that removed the key must report 1");
    assert_eq!(
        live_rows(&server, "SELECT count(*) FROM kv_probe WHERE key = 'a'").await,
        0,
        "the KV key must really be absent, so the reported count is the honest one"
    );
}

/// Re-deleting an already-removed key-value key reports `0`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_redelete_of_deleted_key_reports_zero() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION kv_probe (key TEXT PRIMARY KEY, n INT) WITH (engine='kv')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO kv_probe (key, n) VALUES ('a', 1)")
        .await
        .unwrap();
    server
        .exec("DELETE FROM kv_probe WHERE key = 'a'")
        .await
        .unwrap();

    let count = affected(&server, "DELETE FROM kv_probe WHERE key = 'a'").await;
    assert_eq!(
        count, 0,
        "re-deleting an already-deleted KV key must report 0"
    );
}

/// A multi-key KV delete reports how many of the targeted keys existed. This is
/// the same contract read from the other side: a count synthesised per
/// statement cannot represent "2 of the 3 keys were there".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_multi_key_delete_reports_matched_key_count() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION kv_probe (key TEXT PRIMARY KEY, n INT) WITH (engine='kv')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO kv_probe (key, n) VALUES ('a', 1)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO kv_probe (key, n) VALUES ('b', 2)")
        .await
        .unwrap();

    let count = affected(
        &server,
        "DELETE FROM kv_probe WHERE key IN ('a', 'b', 'missing')",
    )
    .await;
    assert_eq!(
        count, 2,
        "a 3-key KV delete where 2 keys exist must report 2"
    );
    assert_eq!(
        live_rows(&server, "SELECT count(*) FROM kv_probe").await,
        0,
        "both existing keys must be gone"
    );
}

/// A key-value `INSERT` of a new key writes exactly one row, so its command tag
/// must report `1`. A kv write that answers with no affected count renders as a
/// bare `OK` tag, which the client's tag parser reads as `0` rows — the count is
/// the only signal distinguishing "wrote" from "matched nothing".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_insert_of_new_key_reports_one() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION kv_probe (key TEXT PRIMARY KEY, n INT) WITH (engine='kv')")
        .await
        .unwrap();

    let count = affected(&server, "INSERT INTO kv_probe (key, n) VALUES ('a', 1)").await;
    assert_eq!(
        count, 1,
        "a KV INSERT that wrote one key must report 1, not a bare tag the client reads as 0"
    );
    assert_eq!(
        live_rows(&server, "SELECT count(*) FROM kv_probe WHERE key = 'a'").await,
        1,
        "the key must really be present, so the reported count is the honest one"
    );
}

/// A key-value `UPSERT` overwrites or writes exactly one key, so its tag must
/// report `1` for the same reason: the PUT path always writes the key.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_upsert_of_new_key_reports_one() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION kv_probe (key TEXT PRIMARY KEY, n INT) WITH (engine='kv')")
        .await
        .unwrap();

    let count = affected(&server, "UPSERT INTO kv_probe (key, n) VALUES ('a', 1)").await;
    assert_eq!(
        count, 1,
        "a KV UPSERT that wrote one key must report 1, not a bare tag the client reads as 0"
    );
    assert_eq!(
        live_rows(&server, "SELECT count(*) FROM kv_probe WHERE key = 'a'").await,
        1,
        "the upserted key must really be present"
    );
}

/// A CRDT document collection routes its PK-targeted delete through the CRDT
/// engine, which shares the count contract: a delete that removed the row
/// reports `1`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crdt_delete_of_existing_row_reports_one() {
    let server = TestServer::start().await;
    server
        .exec("CREATE TABLE crdt_probe (id TEXT PRIMARY KEY, v INT) WITH (crdt='true')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO crdt_probe (id, v) VALUES ('a', 1)")
        .await
        .unwrap();

    let count = affected(&server, "DELETE FROM crdt_probe WHERE id = 'a'").await;
    assert_eq!(
        count, 1,
        "the CRDT delete that removed the row must report 1"
    );
    assert_eq!(
        live_rows(&server, "SELECT count(*) FROM crdt_probe WHERE id = 'a'").await,
        0,
        "the CRDT row must really be absent, so the reported count is the honest one"
    );
}

/// Re-deleting an already-deleted CRDT row reports `0`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crdt_redelete_of_deleted_primary_key_reports_zero() {
    let server = TestServer::start().await;
    server
        .exec("CREATE TABLE crdt_probe (id TEXT PRIMARY KEY, v INT) WITH (crdt='true')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO crdt_probe (id, v) VALUES ('a', 1)")
        .await
        .unwrap();
    server
        .exec("DELETE FROM crdt_probe WHERE id = 'a'")
        .await
        .unwrap();

    let count = affected(&server, "DELETE FROM crdt_probe WHERE id = 'a'").await;
    assert_eq!(
        count, 0,
        "re-deleting an already-deleted CRDT row must report 0"
    );
}

/// `DELETE ... RETURNING` on an already-deleted primary key returns no rows,
/// and its command tag must agree — a tag claiming 1 alongside zero returned
/// rows is self-contradictory in the same response.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delete_returning_of_deleted_primary_key_reports_zero_rows() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION del_probe (id TEXT PRIMARY KEY, v INT)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO del_probe (id, v) VALUES ('a', 1)")
        .await
        .unwrap();
    server
        .exec("DELETE FROM del_probe WHERE id = 'a'")
        .await
        .unwrap();

    let msgs = server
        .client
        .simple_query("DELETE FROM del_probe WHERE id = 'a' RETURNING id, v")
        .await
        .expect("DELETE RETURNING on an absent row should succeed");

    let returned = msgs
        .iter()
        .filter(|m| matches!(m, SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(
        returned, 0,
        "DELETE RETURNING must project no rows when nothing was deleted"
    );
    assert_eq!(
        command_count(&msgs),
        Some(0),
        "the DELETE RETURNING tag must agree with the zero rows it returned"
    );
}

/// A predicate (non-primary-key) `DELETE` that matches nothing reports `0`.
/// Same contract on the bulk path, and the load-bearing guard against the
/// opposite failure: a predicate delete must never report (or remove) more
/// than it matched.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn predicate_delete_matching_nothing_reports_zero() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION del_probe (id TEXT PRIMARY KEY, v INT)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO del_probe (id, v) VALUES ('a', 1)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO del_probe (id, v) VALUES ('b', 2)")
        .await
        .unwrap();

    let count = affected(&server, "DELETE FROM del_probe WHERE v = 99").await;
    assert_eq!(count, 0, "a predicate delete matching no row must report 0");
    assert_eq!(
        live_rows(&server, "SELECT count(*) FROM del_probe").await,
        2,
        "a predicate delete matching nothing must leave every row in place"
    );
}

/// `INSERT ... ON CONFLICT DO NOTHING` onto an existing primary key inserts
/// nothing and reports `0`. Same shape of claim as the delete: the statement
/// succeeds either way, so the count is the only thing distinguishing "inserted"
/// from "skipped" — and a client uses it for exactly that.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn insert_on_conflict_do_nothing_reports_zero_rows() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION del_probe (id TEXT PRIMARY KEY, v INT)")
        .await
        .unwrap();
    server
        .exec("INSERT INTO del_probe (id, v) VALUES ('a', 1)")
        .await
        .unwrap();

    let inserted = affected(
        &server,
        "INSERT INTO del_probe (id, v) VALUES ('b', 2) ON CONFLICT DO NOTHING",
    )
    .await;
    assert_eq!(inserted, 1, "an insert of a new key must report 1");

    let skipped = affected(
        &server,
        "INSERT INTO del_probe (id, v) VALUES ('a', 99) ON CONFLICT DO NOTHING",
    )
    .await;
    assert_eq!(
        skipped, 0,
        "ON CONFLICT DO NOTHING on an existing key inserts nothing and must report 0"
    );

    let v = server
        .query_text("SELECT v FROM del_probe WHERE id = 'a'")
        .await
        .unwrap();
    assert_eq!(
        v,
        vec!["1"],
        "the skipped insert must not overwrite the row"
    );
}

/// The key-value engine's `ON CONFLICT DO NOTHING` shares the contract: a
/// skipped insert reports `0`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_insert_on_conflict_do_nothing_reports_zero_rows() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION kv_probe (key TEXT PRIMARY KEY, n INT) WITH (engine='kv')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO kv_probe (key, n) VALUES ('a', 1)")
        .await
        .unwrap();

    let skipped = affected(
        &server,
        "INSERT INTO kv_probe (key, n) VALUES ('a', 99) ON CONFLICT DO NOTHING",
    )
    .await;
    assert_eq!(
        skipped, 0,
        "ON CONFLICT DO NOTHING on an existing KV key must report 0"
    );

    let n = server
        .query_text("SELECT n FROM kv_probe WHERE key = 'a'")
        .await
        .unwrap();
    assert_eq!(
        n,
        vec!["1"],
        "the skipped insert must not overwrite the key"
    );
}
