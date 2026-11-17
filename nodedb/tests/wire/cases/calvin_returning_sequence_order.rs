// SPDX-License-Identifier: BUSL-1.1

//! The statements of one request run in order, so a later `RETURNING` list
//! sees every sequence change an earlier one made, as in PostgreSQL. Here
//! the two statements write `ONE` and `TWO`, which land on different
//! vShards, so they commit as one Calvin txn with two participants. The
//! second participant evaluates its list only after the first one's
//! changes reach it, over native SQL and over HTTP alike.

use nodedb_test_support::native_harness::{open_trust_session, send_sql};
use nodedb_types::Value;
use nodedb_types::id::DatabaseId;
use nodedb_types::protocol::NativeResponse;

use crate::harness::TestServer;

const ONE: &str = "cso_one";

fn vshard_of(name: &str) -> u32 {
    nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, name)
        .vshard()
        .as_u32()
}

/// A collection name on another vShard than `ONE`.
fn two() -> String {
    (0..)
        .map(|n| format!("cso_two_{n}"))
        .find(|name| vshard_of(name) != vshard_of(ONE))
        .expect("a name on another vShard")
}

/// Two strict collections on two vShards, each with rows `r1` and `r2`,
/// and the sequence `s`.
async fn fixture() -> (TestServer, String) {
    let server = TestServer::start_multicores(2).await;
    let two = two();
    for collection in [ONE, two.as_str()] {
        server
            .exec(&format!(
                "CREATE COLLECTION {collection} (id TEXT PRIMARY KEY, v TEXT) \
                 WITH (engine='document_strict')"
            ))
            .await
            .expect("create the collection");
        server
            .exec(&format!(
                "INSERT INTO {collection} (id, v) VALUES ('r1', 'a'), ('r2', 'b')"
            ))
            .await
            .expect("seed the collection");
    }
    server.exec("CREATE SEQUENCE s").await.expect("create s");
    (server, two)
}

/// The `(id, n)` rows `sql` answers over native SQL, in a fresh session.
async fn native_rows(server: &TestServer, sql: &str) -> Vec<(String, i64)> {
    let mut stream = open_trust_session(server.native_port, "nodedb").await;
    let response = send_sql(&mut stream, 2, sql).await;
    assert!(response.error.is_none(), "{sql}: {:?}", response.error);
    let rows = response
        .rows
        .unwrap_or_else(|| panic!("{sql}: rows in the answer"));
    rows.into_iter()
        .map(|row| match row.as_slice() {
            [Value::String(id), Value::Integer(n)] => (id.clone(), *n),
            other => panic!("{sql}: an (id, n) row, got {other:?}"),
        })
        .collect()
}

/// The `(id, n)` rows `sql` answers over HTTP, which keeps no session.
async fn http_rows(server: &TestServer, sql: &str) -> Vec<(String, i64)> {
    let (status, body) = server.http_query(sql).await;
    assert_eq!(status, 200, "{sql}: {body}");
    let answer: serde_json::Value = serde_json::from_str(&body).expect("a JSON answer");
    answer["rows"]
        .as_array()
        .unwrap_or_else(|| panic!("rows in {body}"))
        .iter()
        .map(|row| {
            let id = row["id"]
                .as_str()
                .unwrap_or_else(|| panic!("an id in {body}"));
            let n = row["n"]
                .as_i64()
                .unwrap_or_else(|| panic!("an n in {body}"));
            (id.to_owned(), n)
        })
        .collect()
}

/// The `n` values of the rows at `take`, ascending.
fn values(rows: &[(String, i64)], take: std::ops::Range<usize>) -> Vec<i64> {
    let mut values: Vec<i64> = rows[take].iter().map(|(_, n)| *n).collect();
    values.sort_unstable();
    values
}

fn currval_plus_one_sql(two: &str) -> String {
    format!(
        "UPDATE {ONE} SET v = 'x' WHERE id = 'r1' RETURNING id, nextval('s') AS n; \
         UPDATE {two} SET v = 'x' WHERE id = 'r2' RETURNING id, currval('s') + 1 AS n"
    )
}

/// `currval('s') + 1` in the second statement reads the first statement's
/// `nextval('s')`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_later_currval_expression_sees_an_earlier_nextval_over_native() {
    let (server, two) = fixture().await;
    let rows = native_rows(&server, &currval_plus_one_sql(&two)).await;
    assert_eq!(rows, [("r1".to_owned(), 1), ("r2".to_owned(), 2)]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_later_currval_expression_sees_an_earlier_nextval_over_http() {
    let (server, two) = fixture().await;
    let rows = http_rows(&server, &currval_plus_one_sql(&two)).await;
    assert_eq!(rows, [("r1".to_owned(), 1), ("r2".to_owned(), 2)]);
}

fn two_nextvals_sql(two: &str) -> String {
    format!(
        "UPDATE {ONE} SET v = 'x' WHERE id IN ('r1', 'r2') RETURNING id, nextval('s') AS n; \
         UPDATE {two} SET v = 'x' WHERE id IN ('r1', 'r2') RETURNING id, nextval('s') AS n"
    )
}

/// A later statement that draws `nextval` itself draws after the earlier
/// statement's draws: every value is distinct, and the later ones follow.
fn assert_ordered_draws(rows: &[(String, i64)]) {
    assert_eq!(rows.len(), 4, "{rows:?}");
    assert_eq!(values(rows, 0..2), [1, 2], "the first statement: {rows:?}");
    assert_eq!(values(rows, 2..4), [3, 4], "the second statement: {rows:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_later_nextval_follows_the_earlier_draws_over_native() {
    let (server, two) = fixture().await;
    assert_ordered_draws(&native_rows(&server, &two_nextvals_sql(&two)).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_later_nextval_follows_the_earlier_draws_over_http() {
    let (server, two) = fixture().await;
    assert_ordered_draws(&http_rows(&server, &two_nextvals_sql(&two)).await);
}

fn setval_then_currval_sql(two: &str) -> String {
    format!(
        "UPDATE {ONE} SET v = 'x' WHERE id = 'r1' RETURNING id, setval('s', 100) AS n; \
         UPDATE {two} SET v = 'x' WHERE id = 'r2' RETURNING id, currval('s') AS n"
    )
}

/// The second statement's `currval` reads the first statement's `setval`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_later_currval_sees_an_earlier_setval_over_native() {
    let (server, two) = fixture().await;
    let rows = native_rows(&server, &setval_then_currval_sql(&two)).await;
    assert_eq!(rows, [("r1".to_owned(), 100), ("r2".to_owned(), 100)]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_later_currval_sees_an_earlier_setval_over_http() {
    let (server, two) = fixture().await;
    let rows = http_rows(&server, &setval_then_currval_sql(&two)).await;
    assert_eq!(rows, [("r1".to_owned(), 100), ("r2".to_owned(), 100)]);
}

fn empty_first_statement_sql(two: &str) -> String {
    format!(
        "UPDATE {ONE} SET v = 'x' WHERE id = 'none' RETURNING id, nextval('s') AS n; \
         UPDATE {two} SET v = 'x' WHERE id = 'r1' RETURNING id, currval('s') AS n"
    )
}

/// The value `two`'s row `r1` holds.
async fn r1_value(server: &TestServer, two: &str) -> Vec<String> {
    server
        .query_text(&format!("SELECT v FROM {two} WHERE id = 'r1'"))
        .await
        .expect("read r1")
}

/// In a fresh session whose first statement matched no row, `currval` is
/// not yet defined: the request fails with SQLSTATE 55000 and writes
/// nothing, as PostgreSQL does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn currval_after_an_empty_statement_fails_in_a_fresh_session_over_native() {
    let (server, two) = fixture().await;
    let mut stream = open_trust_session(server.native_port, "nodedb").await;
    let sql = empty_first_statement_sql(&two);
    let response: NativeResponse = send_sql(&mut stream, 2, &sql).await;
    let error = response
        .error
        .unwrap_or_else(|| panic!("currval is not yet defined: {sql}"));
    assert_eq!(error.code, "55000", "{}", error.message);
    assert_eq!(r1_value(&server, &two).await, ["a"], "nothing is written");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn currval_after_an_empty_statement_fails_in_a_fresh_session_over_http() {
    let (server, two) = fixture().await;
    let (status, body) = server.http_query(&empty_first_statement_sql(&two)).await;
    assert_ne!(status, 200, "{body}");
    assert!(
        body.contains("is not yet defined in this session"),
        "the 55000 message: {body}"
    );
    assert_eq!(r1_value(&server, &two).await, ["a"], "nothing is written");
}
