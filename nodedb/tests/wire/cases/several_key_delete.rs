// SPDX-License-Identifier: BUSL-1.1

//! A `DELETE` naming several keys lowers to one predicate delete. It answers
//! `RETURNING` with each row's before-image, judges the write policy per row,
//! and obeys append-only and legal hold as a point delete does.

use crate::harness::TestServer;

const PASSWORD: &str = "several-key-secret-58";

async fn exec(server: &TestServer, sql: &str) {
    server
        .exec(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

async fn ids(server: &TestServer, collection: &str) -> Vec<String> {
    let mut rows: Vec<String> = server
        .query_rows(&format!("SELECT id FROM {collection}"))
        .await
        .unwrap_or_else(|e| panic!("read {collection}: {e}"))
        .into_iter()
        .map(|row| row.join(","))
        .collect();
    rows.sort();
    rows
}

async fn seed_three(server: &TestServer, collection: &str) {
    exec(
        server,
        &format!("CREATE COLLECTION {collection} (id TEXT PRIMARY KEY, n INT)"),
    )
    .await;
    exec(
        server,
        &format!("INSERT INTO {collection} (id, n) VALUES ('a', 1), ('b', 2), ('c', 3)"),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn returning_star_answers_each_before_image() {
    let server = TestServer::start().await;
    seed_three(&server, "skd_ret").await;

    let mut rows: Vec<String> = server
        .query_rows("DELETE FROM skd_ret WHERE id IN ('a', 'b') RETURNING *")
        .await
        .expect("DELETE RETURNING *")
        .into_iter()
        .map(|row| row.join(","))
        .collect();
    rows.sort();

    assert_eq!(rows.len(), 2, "one before-image per key: {rows:?}");
    assert!(rows[0].contains('a') && rows[0].contains('1'), "{rows:?}");
    assert!(rows[1].contains('b') && rows[1].contains('2'), "{rows:?}");
    assert_eq!(ids(&server, "skd_ret").await, ["c"]);
}

async fn create_user(server: &TestServer, user: &str) {
    exec(server, &format!("CREATE USER {user} PASSWORD '{PASSWORD}'")).await;
    exec(server, &format!("GRANT ROLE readwrite TO {user}")).await;
}

async fn seed_owned(server: &TestServer, collection: &str, user: &str) {
    exec(
        server,
        &format!("CREATE COLLECTION {collection} (id TEXT PRIMARY KEY, owner TEXT)"),
    )
    .await;
    for (id, owner) in [("m1", user), ("m2", user), ("t1", "alice")] {
        exec(
            server,
            &format!("INSERT INTO {collection} (id, owner) VALUES ('{id}', '{owner}')"),
        )
        .await;
    }
    create_user(server, user).await;
}

async fn run_as(server: &TestServer, user: &str, sql: &str) -> Result<Vec<String>, String> {
    let (client, handle) = server
        .connect_as(user, PASSWORD)
        .await
        .unwrap_or_else(|e| panic!("connect as {user}: {e}"));
    let result = client.simple_query(sql).await.map_err(|e| e.to_string());
    drop(client);
    handle.abort();
    result.map(|messages| {
        messages
            .into_iter()
            .filter_map(|message| match message {
                tokio_postgres::SimpleQueryMessage::Row(row) => Some(
                    (0..row.len())
                        .map(|i| row.get(i).unwrap_or("").to_string())
                        .collect::<Vec<_>>()
                        .join("|"),
                ),
                _ => None,
            })
            .collect()
    })
}

/// A write policy judges each removed row. A key set holding a row outside
/// the policy fails whole and removes nothing. A key set of admitted rows
/// removes them all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_policy_judges_each_row_of_a_several_key_delete() {
    let server = TestServer::start().await;
    let user = "skd_write_user";
    seed_owned(&server, "skd_write", user).await;
    exec(
        &server,
        "CREATE RLS POLICY skd_write_owner ON skd_write FOR WRITE \
         USING (owner = $auth.username)",
    )
    .await;

    let refused = run_as(
        &server,
        user,
        "DELETE FROM skd_write WHERE id IN ('m1', 't1')",
    )
    .await;
    assert!(
        refused.is_err(),
        "a row outside the policy refuses the delete"
    );
    assert_eq!(ids(&server, "skd_write").await, ["m1", "m2", "t1"]);

    run_as(
        &server,
        user,
        "DELETE FROM skd_write WHERE id IN ('m1', 'm2')",
    )
    .await
    .expect("admitted rows delete");
    assert_eq!(ids(&server, "skd_write").await, ["t1"]);
}

/// A read policy hides rows from `RETURNING` only. The delete removes every
/// named row.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_read_policy_hides_rows_from_a_several_key_delete_returning() {
    let server = TestServer::start().await;
    let user = "skd_read_user";
    seed_owned(&server, "skd_read", user).await;
    exec(
        &server,
        "CREATE RLS POLICY skd_read_owner ON skd_read FOR READ \
         USING (owner = $auth.username)",
    )
    .await;

    let mut shown = run_as(
        &server,
        user,
        "DELETE FROM skd_read WHERE id IN ('m1', 't1') RETURNING id",
    )
    .await
    .expect("delete returning");
    shown.sort();
    assert_eq!(shown, ["m1"], "only the readable row is shown back");
    assert_eq!(ids(&server, "skd_read").await, ["m2"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_append_only_collection_refuses_a_several_key_delete() {
    let server = TestServer::start().await;
    seed_three(&server, "skd_append").await;
    exec(&server, "ALTER COLLECTION skd_append SET APPEND_ONLY").await;

    let result = server
        .exec("DELETE FROM skd_append WHERE id IN ('a', 'b')")
        .await;
    assert!(result.is_err(), "append-only refuses the delete");
    assert_eq!(ids(&server, "skd_append").await, ["a", "b", "c"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_legal_hold_refuses_key_set_and_predicate_deletes() {
    let server = TestServer::start().await;
    seed_three(&server, "skd_hold").await;
    exec(
        &server,
        "ALTER COLLECTION skd_hold SET LEGAL_HOLD = TRUE TAG 'litigation'",
    )
    .await;

    for sql in [
        "DELETE FROM skd_hold WHERE id IN ('a', 'b')",
        "DELETE FROM skd_hold WHERE n >= 1",
    ] {
        assert!(server.exec(sql).await.is_err(), "{sql} must be refused");
        assert_eq!(ids(&server, "skd_hold").await, ["a", "b", "c"], "{sql}");
    }
}
