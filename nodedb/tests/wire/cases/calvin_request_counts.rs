// SPDX-License-Identifier: BUSL-1.1

//! The count of a request that commits through one Calvin transaction.
//!
//! Each slice adds the counts of its own plans, and the coordinator adds its
//! slices'. So a write that matched no row adds nothing, whichever position it
//! holds, and a real write is never hidden or counted twice.
//!
//! `ONE` and `TWO` land on different vShards, so a request over both commits
//! through Calvin with two slices. A write of a cross-shard sum source commits
//! through Calvin too, so two writes of `ptv_post` share one slice.

use nodedb::control::planner::calvin::settling_writes::SEVERAL_RETURNING_REFUSAL;
use nodedb_test_support::native_harness::{open_trust_session, send_sql};
use nodedb_types::id::DatabaseId;

use super::materialized_sum_cross_shard_expanders::{balance, sum_fixture, sum_fixture_on};
use crate::harness::TestServer;

/// Data Plane cores the server runs.
const CORES: usize = 2;
const ONE: &str = "cc_one";

fn vshard_of(name: &str) -> u32 {
    nodedb_types::CollectionKey::from_bare(DatabaseId::DEFAULT, name)
        .vshard()
        .as_u32()
}

/// A collection name on another vShard than `ONE`.
fn two() -> String {
    (0..)
        .map(|n| format!("cc_two_{n}"))
        .find(|name| vshard_of(name) != vshard_of(ONE))
        .expect("a name on the other vShard")
}

/// Two strict collections on two vShards, each with rows `r1` and `r2`.
async fn fixture() -> (TestServer, String) {
    let server = TestServer::start_multicores(CORES).await;
    let other = two();
    for collection in [ONE, other.as_str()] {
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
    (server, other)
}

fn update(collection: &str, id: &str) -> String {
    format!("UPDATE {collection} SET v = 'x' WHERE id = '{id}'")
}

/// The `affected` count `sql` answers over HTTP.
async fn http_affected(server: &TestServer, sql: &str) -> u64 {
    let (status, body) = server.http_query(sql).await;
    assert_eq!(status, 200, "{sql}: {body}");
    let answer: serde_json::Value = serde_json::from_str(&body).expect("a JSON answer");
    answer["rows"][0]["affected"]
        .as_u64()
        .unwrap_or_else(|| panic!("{sql}: an affected count in {body}"))
}

/// The command and the count `sql` answers over native SQL.
async fn native_answer(server: &TestServer, sql: &str) -> (Option<String>, Option<u64>) {
    let mut stream = open_trust_session(server.native_port, "nodedb").await;
    let response = send_sql(&mut stream, 2, sql).await;
    assert!(response.error.is_none(), "{sql}: {:?}", response.error);
    (response.command, response.rows_affected)
}

async fn values(server: &TestServer, collection: &str) -> Vec<String> {
    server
        .query_text(&format!("SELECT v FROM {collection} ORDER BY id"))
        .await
        .expect("read the collection")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_two_slices_with_a_second_write_that_matches_nothing_count_one() {
    let (server, other) = fixture().await;
    let sql = format!("{}; {}", update(ONE, "r1"), update(&other, "absent"));

    assert_eq!(http_affected(&server, &sql).await, 1);
    assert_eq!(values(&server, ONE).await, ["x", "b"]);
    assert_eq!(values(&server, &other).await, ["a", "b"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_two_slices_with_a_first_write_that_matches_nothing_count_one() {
    let (server, other) = fixture().await;
    let sql = format!("{}; {}", update(ONE, "absent"), update(&other, "r1"));

    assert_eq!(http_affected(&server, &sql).await, 1);
    assert_eq!(values(&server, &other).await, ["x", "b"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_two_slices_with_two_real_writes_count_two() {
    let (server, other) = fixture().await;
    let sql = format!("{}; {}", update(ONE, "r1"), update(&other, "r1"));

    assert_eq!(http_affected(&server, &sql).await, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_two_slices_with_a_write_that_matches_nothing_count_one() {
    let (server, other) = fixture().await;
    let sql = format!("{}; {}", update(ONE, "r1"), update(&other, "absent"));

    assert_eq!(
        native_answer(&server, &sql).await,
        (Some("UPDATE".to_owned()), Some(1))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_two_slices_with_two_real_writes_count_two() {
    let (server, other) = fixture().await;
    let sql = format!("{}; {}", update(ONE, "r1"), update(&other, "r2"));

    assert_eq!(
        native_answer(&server, &sql).await,
        (Some("UPDATE".to_owned()), Some(2))
    );
}

/// A real `UPDATE` beside a `DELETE` that matches nothing, on two slices.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_real_write_beside_a_no_op_of_another_verb_counts_one() {
    let (server, other) = fixture().await;
    let sql = format!(
        "{}; DELETE FROM {other} WHERE id = 'absent'",
        update(ONE, "r1")
    );

    assert_eq!(http_affected(&server, &sql).await, 1);
    assert_eq!(
        native_answer(
            &server,
            &format!(
                "{}; DELETE FROM {other} WHERE id = 'absent'",
                update(ONE, "r2")
            )
        )
        .await,
        (Some("MIXED".to_owned()), Some(1))
    );
    assert_eq!(values(&server, ONE).await, ["x", "x"]);
}

/// A timeseries collection on another vShard than `ONE`, with `ONE` seeded.
async fn timeseries_fixture() -> (TestServer, String) {
    let (server, _) = fixture().await;
    let series = (0..)
        .map(|n| format!("cc_ts_{n}"))
        .find(|name| vshard_of(name) != vshard_of(ONE))
        .expect("a name on the other vShard");
    server
        .exec(&format!(
            "CREATE COLLECTION {series} \
             COLUMNS (id TEXT, ts BIGINT TIME_KEY, v INT) \
             WITH (engine='timeseries')"
        ))
        .await
        .expect("create the timeseries collection");
    (server, series)
}

fn ingest(series: &str) -> String {
    format!("INSERT INTO {series} (id, ts, v) VALUES ('s1', 1000, 1), ('s2', 2000, 2)")
}

/// A plain write beside a timeseries install on another slice adds its count
/// to the rows the install stored.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_timeseries_install_beside_a_plain_write_counts_both() {
    let (server, series) = timeseries_fixture().await;
    let sql = format!("{}; {}", ingest(&series), update(ONE, "r1"));

    assert_eq!(http_affected(&server, &sql).await, 3);
    assert_eq!(values(&server, ONE).await, ["x", "b"]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_timeseries_install_beside_a_no_op_counts_the_install() {
    let (server, series) = timeseries_fixture().await;
    let sql = format!("{}; {}", ingest(&series), update(ONE, "absent"));

    assert_eq!(
        native_answer(&server, &sql).await,
        (Some("INSERT".to_owned()), Some(2))
    );
}

/// Two point updates of one Calvin slice. A balance task ships beside them and
/// adds no count.
const SAME_SLICE_NO_OP: &str = "UPDATE ptv_post SET amount = '1' WHERE id = 'e0'; \
     UPDATE ptv_post SET amount = '2' WHERE id = 'absent'";
const SAME_SLICE_NO_OP_FIRST: &str = "UPDATE ptv_post SET amount = '2' WHERE id = 'absent'; \
     UPDATE ptv_post SET amount = '1' WHERE id = 'e0'";
const SAME_SLICE_REAL: &str = "UPDATE ptv_post SET amount = '1' WHERE id = 'e0'; \
     UPDATE ptv_post SET amount = '2' WHERE id = 'e1'";
const SAME_SLICE_MIXED: &str = "UPDATE ptv_post SET amount = '1' WHERE id = 'e0'; \
     DELETE FROM ptv_post WHERE id = 'e1'";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_one_slice_with_a_write_that_matches_nothing_counts_one() {
    let server = sum_fixture().await;

    assert_eq!(http_affected(&server, SAME_SLICE_NO_OP).await, 1);
    assert_eq!(balance(&server, "acc").await, "6", "1 + 5");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_one_slice_with_a_first_write_that_matches_nothing_counts_one() {
    let server = sum_fixture().await;

    assert_eq!(http_affected(&server, SAME_SLICE_NO_OP_FIRST).await, 1);
    assert_eq!(balance(&server, "acc").await, "6", "1 + 5");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_one_slice_counts_each_write_once() {
    let server = sum_fixture().await;

    assert_eq!(
        native_answer(&server, SAME_SLICE_REAL).await,
        (Some("UPDATE".to_owned()), Some(2))
    );
    assert_eq!(balance(&server, "acc").await, "3", "1 + 2");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_one_slice_of_two_verbs_answers_mixed_with_the_sum() {
    let server = sum_fixture().await;

    assert_eq!(
        native_answer(&server, SAME_SLICE_MIXED).await,
        (Some("MIXED".to_owned()), Some(2))
    );
    assert_eq!(balance(&server, "acc").await, "1", "e1 is gone");
}

/// Each write of a request owns its own `RETURNING` clause. One transaction
/// answers one set of rows, so two writes that each carry one are refused
/// whole, over two slices or one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn returning_on_several_calvin_tasks_is_refused_whole() {
    let (server, other) = fixture().await;
    let sql = format!(
        "{} RETURNING v; {} RETURNING v",
        update(ONE, "r1"),
        update(&other, "r1")
    );

    let (status, body) = server.http_query(&sql).await;

    assert_eq!(status, 501, "feature not supported: {body}");
    assert!(body.contains(SEVERAL_RETURNING_REFUSAL), "{body}");
    assert_eq!(values(&server, ONE).await, ["a", "b"]);
    assert_eq!(values(&server, &other).await, ["a", "b"]);
}

/// One `RETURNING` write of a cross-shard sum source answers its own rows, and
/// the balance task beside it adds nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_returning_write_on_calvin_answers_its_rows() {
    let server = sum_fixture().await;

    let (status, body) = server
        .http_query("UPDATE ptv_post SET amount = '1' WHERE id = 'e0' RETURNING id, amount")
        .await;

    assert_eq!(status, 200, "{body}");
    let answer: serde_json::Value = serde_json::from_str(&body).expect("a JSON answer");
    assert_eq!(answer["rows"].as_array().map(Vec::len), Some(1), "{body}");
    assert_eq!(answer["rows"][0]["id"], "e0", "{body}");
    assert_eq!(balance(&server, "acc").await, "6", "1 + 5");
}

/// The amount posting `e0` holds.
async fn e0_amount(server: &TestServer) -> Vec<String> {
    server
        .query_text("SELECT amount FROM ptv_post WHERE id = 'e0'")
        .await
        .expect("read posting e0")
}

/// A `RETURNING` list that fails on a Calvin write refuses the write with
/// its own SQLSTATE before the participant votes. The divisor reads a row
/// column, so only the participant's evaluation of the written row can
/// raise it. The row keeps its value.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn returning_error_on_calvin_leaves_the_row() {
    let server = sum_fixture().await;
    server
        .exec("ALTER TABLE ptv_post ADD COLUMN n INT NOT NULL DEFAULT 0")
        .await
        .expect("add column n");

    server
        .expect_error(
            "UPDATE ptv_post SET amount = '1', n = 5 WHERE id = 'e0' RETURNING 10 / (n - n)",
            "SQLSTATE 22012",
        )
        .await;

    assert_eq!(e0_amount(&server).await, ["10"]);
    assert_eq!(
        server
            .query_text("SELECT n FROM ptv_post WHERE id = 'e0'")
            .await
            .expect("read n"),
        ["0"]
    );
    assert_eq!(balance(&server, "acc").await, "15", "10 + 5");
}

/// A Calvin write whose `RETURNING` response holds more rows than
/// `max_returning_rows` is refused before the participant votes. The row
/// keeps its value.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn returning_over_limit_on_calvin_leaves_the_row() {
    // The sum fixture runs on as many cores as this file's fixture.
    let server =
        sum_fixture_on(TestServer::start_multicores_with_max_returning_rows(CORES, 0).await).await;

    // The SQLSTATE and message a direct statement past an execution limit
    // reports: `54000` with the limit's own text.
    server
        .expect_error(
            "UPDATE ptv_post SET amount = '1' WHERE id = 'e0' RETURNING id, amount",
            "execution limit exceeded: RETURNING rows 1 exceeds limit 0 (SQLSTATE 54000)",
        )
        .await;

    assert_eq!(e0_amount(&server).await, ["10"]);
    assert_eq!(balance(&server, "acc").await, "15", "10 + 5");
}

/// A Calvin write's `RETURNING` list draws each `nextval` once per row: the
/// participant's leader evaluates it before the vote, and the client gets
/// those rows. A later `nextval` follows them, and the session's `currval`
/// is the last value the rows drew.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn returning_nextval_on_calvin_is_once_per_row() {
    let server = sum_fixture().await;
    server
        .exec("CREATE SEQUENCE cc_returning_seq")
        .await
        .expect("create the sequence");

    let rows = server
        .query_named_rows(
            "UPDATE ptv_post SET amount = '1' WHERE id IN ('e0', 'e1') \
             RETURNING id, nextval('cc_returning_seq') AS n",
        )
        .await
        .expect("the update answers its rows");

    let mut drawn: Vec<i64> = rows
        .iter()
        .map(|row| {
            row.get("n")
                .and_then(|n| n.parse().ok())
                .unwrap_or_else(|| panic!("a drawn value in {row:?}"))
        })
        .collect();
    drawn.sort_unstable();
    assert_eq!(drawn, vec![1, 2], "one value per row: {rows:?}");
    let mut ids: Vec<&str> = rows
        .iter()
        .filter_map(|row| row.get("id").map(String::as_str))
        .collect();
    ids.sort_unstable();
    assert_eq!(ids, ["e0", "e1"]);
    assert_eq!(
        server
            .query_text("SELECT currval('cc_returning_seq')")
            .await
            .expect("currval"),
        ["2"],
        "the session's currval is the last value the rows drew"
    );
    assert_eq!(
        server
            .query_text("SELECT nextval('cc_returning_seq')")
            .await
            .expect("nextval"),
        ["3"],
        "the write drew each value once"
    );
}
