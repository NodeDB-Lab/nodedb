// SPDX-License-Identifier: BUSL-1.1

//! `UPDATE ... RETURNING` and `DELETE ... RETURNING` on the key-value engine.
//!
//! An update returns the STORED post-image (the merged row as a `SELECT`
//! shows it), a delete returns the pre-image of every row it removed, and a
//! statement that matched nothing returns zero rows rather than a count. The
//! keyed form (`WHERE <pk> = ...`) and the predicate form (`WHERE <field> =
//! ...`) go through different `KvOp`s and are pinned separately.

use crate::harness::TestServer;

/// Rows of `sql`, each row's columns joined by `|`.
async fn rows(server: &TestServer, sql: &str) -> Vec<String> {
    server
        .query_rows(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .into_iter()
        .map(|r| r.join("|"))
        .collect()
}

/// A KV collection holding three rows: two owned by `alice`, one by `bob`.
async fn seed(server: &TestServer, collection: &str) {
    server
        .exec(&format!(
            "CREATE COLLECTION {collection} \
             (id TEXT PRIMARY KEY, owner TEXT, n INT) \
             WITH (engine='kv')"
        ))
        .await
        .unwrap_or_else(|e| panic!("create {collection}: {e}"));
    for (id, owner, n) in [("r1", "alice", 1), ("r2", "alice", 2), ("r3", "bob", 3)] {
        server
            .exec(&format!(
                "INSERT INTO {collection} (id, owner, n) VALUES ('{id}', '{owner}', {n})"
            ))
            .await
            .unwrap_or_else(|e| panic!("seed {collection}/{id}: {e}"));
    }
}

/// A keyed UPDATE returns the post-image: the assigned value, the untouched
/// columns, and the key, exactly as a `SELECT` of that key reports them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_keyed_update_returning_ships_the_post_image() {
    let server = TestServer::start().await;
    seed(&server, "kv_ret_upd_key").await;

    let returned = server
        .query_named_rows("UPDATE kv_ret_upd_key SET n = 99 WHERE id = 'r1' RETURNING *")
        .await
        .expect("KV keyed UPDATE RETURNING * must return the updated row");

    assert_eq!(returned.len(), 1, "one updated row: {returned:?}");
    assert_eq!(returned[0].get("id").map(String::as_str), Some("r1"));
    assert_eq!(returned[0].get("owner").map(String::as_str), Some("alice"));
    assert_eq!(returned[0].get("n").map(String::as_str), Some("99"));
    assert_eq!(
        rows(
            &server,
            "SELECT id, owner, n FROM kv_ret_upd_key WHERE id = 'r1'"
        )
        .await,
        vec!["r1|alice|99".to_string()],
        "RETURNING must report the row a SELECT sees"
    );
}

/// A keyed UPDATE against a key that was never inserted reports `UPDATE 0`
/// and creates no row — RESP `HSET` semantics (create on absent) do not
/// leak into SQL `UPDATE`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keyed_update_on_absent_key_affects_zero_rows() {
    let server = TestServer::start().await;
    seed(&server, "kv_upd_absent").await;

    let messages = server
        .client
        .simple_query("UPDATE kv_upd_absent SET n = 5 WHERE id = 'missing'")
        .await
        .expect("a keyed UPDATE on an absent key must succeed");
    let mut count = None;
    for message in messages {
        if let tokio_postgres::SimpleQueryMessage::CommandComplete(n) = message {
            count = Some(n);
        }
    }
    assert_eq!(
        count,
        Some(0),
        "an absent keyed UPDATE must report UPDATE 0, not create the row"
    );
    assert_eq!(
        rows(&server, "SELECT id FROM kv_upd_absent WHERE id = 'missing'").await,
        Vec::<String>::new(),
        "the absent key must not be created"
    );
}

/// A keyed `UPDATE ... RETURNING` against a key that was never inserted
/// returns zero rows, never a post-image materialized from nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keyed_update_returning_on_absent_key_returns_no_rows() {
    let server = TestServer::start().await;
    seed(&server, "kv_upd_absent_ret").await;

    let returned = server
        .query_rows("UPDATE kv_upd_absent_ret SET n = 5 WHERE id = 'missing' RETURNING *")
        .await
        .expect("a keyed UPDATE RETURNING on an absent key must succeed");
    assert!(
        returned.is_empty(),
        "an absent keyed UPDATE RETURNING must return no rows: {returned:?}"
    );
    assert_eq!(
        rows(
            &server,
            "SELECT id FROM kv_upd_absent_ret WHERE id = 'missing'"
        )
        .await,
        Vec::<String>::new(),
        "the absent key must not be created"
    );
}

/// A predicate UPDATE returns one post-image per matched row, and only the
/// matched rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_predicate_update_returning_ships_every_matched_post_image() {
    let server = TestServer::start().await;
    seed(&server, "kv_ret_upd_pred").await;

    let mut returned = rows(
        &server,
        "UPDATE kv_ret_upd_pred SET n = 7 WHERE owner = 'alice' RETURNING id, owner, n",
    )
    .await;
    returned.sort();
    assert_eq!(
        returned,
        vec!["r1|alice|7".to_string(), "r2|alice|7".to_string()],
        "both of alice's rows come back with the new value"
    );
    assert_eq!(
        rows(&server, "SELECT id, n FROM kv_ret_upd_pred ORDER BY id").await,
        vec!["r1|7".to_string(), "r2|7".to_string(), "r3|3".to_string()],
        "bob's row is untouched"
    );
}

/// A keyed DELETE returns the pre-image of the row it removed, and the row is
/// gone afterwards.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_keyed_delete_returning_ships_the_pre_image() {
    let server = TestServer::start().await;
    seed(&server, "kv_ret_del_key").await;

    let returned = server
        .query_named_rows("DELETE FROM kv_ret_del_key WHERE id = 'r3' RETURNING *")
        .await
        .expect("KV keyed DELETE RETURNING * must return the removed row");

    assert_eq!(returned.len(), 1, "one removed row: {returned:?}");
    assert_eq!(returned[0].get("id").map(String::as_str), Some("r3"));
    assert_eq!(returned[0].get("owner").map(String::as_str), Some("bob"));
    assert_eq!(returned[0].get("n").map(String::as_str), Some("3"));
    assert_eq!(
        rows(&server, "SELECT id FROM kv_ret_del_key ORDER BY id").await,
        vec!["r1".to_string(), "r2".to_string()],
        "the returned row must no longer be stored"
    );
}

/// A predicate DELETE returns the pre-image of every row it removed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_predicate_delete_returning_ships_every_removed_pre_image() {
    let server = TestServer::start().await;
    seed(&server, "kv_ret_del_pred").await;

    let mut returned = rows(
        &server,
        "DELETE FROM kv_ret_del_pred WHERE owner = 'alice' RETURNING id, n",
    )
    .await;
    returned.sort();
    assert_eq!(
        returned,
        vec!["r1|1".to_string(), "r2|2".to_string()],
        "both of alice's rows come back as they were stored"
    );
    assert_eq!(
        rows(&server, "SELECT id FROM kv_ret_del_pred").await,
        vec!["r3".to_string()],
        "only bob's row survives"
    );
}

/// An UPDATE that matches no row returns zero rows: never a count payload,
/// which the RETURNING renderer cannot decode, and never an error.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_update_returning_with_zero_matches_returns_zero_rows() {
    let server = TestServer::start().await;
    seed(&server, "kv_ret_upd_none").await;

    let returned = server
        .query_rows("UPDATE kv_ret_upd_none SET n = 0 WHERE owner = 'nobody' RETURNING *")
        .await
        .expect("a zero-match UPDATE RETURNING must succeed");
    assert!(returned.is_empty(), "no row matched: {returned:?}");
    assert_eq!(
        rows(&server, "SELECT id, n FROM kv_ret_upd_none ORDER BY id").await,
        vec!["r1|1".to_string(), "r2|2".to_string(), "r3|3".to_string()],
        "nothing was written"
    );
}

/// A DELETE that matches no row returns zero rows, keyed and predicate alike.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_delete_returning_with_zero_matches_returns_zero_rows() {
    let server = TestServer::start().await;
    seed(&server, "kv_ret_del_none").await;

    for sql in [
        "DELETE FROM kv_ret_del_none WHERE id = 'missing' RETURNING *",
        "DELETE FROM kv_ret_del_none WHERE owner = 'nobody' RETURNING *",
    ] {
        let returned = server
            .query_rows(sql)
            .await
            .unwrap_or_else(|e| panic!("a zero-match DELETE RETURNING must succeed: {sql}: {e}"));
        assert!(
            returned.is_empty(),
            "no row matched for {sql}: {returned:?}"
        );
    }
    assert_eq!(
        rows(&server, "SELECT id FROM kv_ret_del_none ORDER BY id").await,
        vec!["r1".to_string(), "r2".to_string(), "r3".to_string()],
        "nothing was removed"
    );
}

/// A RETURNING expression evaluates against the removed row's pre-image, and
/// only the requested column is shipped.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_delete_returning_expression_evaluates_on_the_pre_image() {
    let server = TestServer::start().await;
    seed(&server, "kv_ret_del_expr").await;

    let msgs = server
        .client
        .simple_query("DELETE FROM kv_ret_del_expr WHERE id = 'r2' RETURNING n * 2 AS d")
        .await
        .expect("an expression-only RETURNING must succeed");
    let row = msgs
        .iter()
        .find_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .expect("one returned row");
    assert_eq!(row.len(), 1, "only the requested column is shipped");
    assert_eq!(row.columns()[0].name(), "d");
    assert_eq!(row.get(0), Some("4"));
}
