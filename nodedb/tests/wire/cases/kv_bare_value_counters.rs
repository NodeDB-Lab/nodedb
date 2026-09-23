// SPDX-License-Identifier: BUSL-1.1

//! Counter atomics on a KV row stored as raw bytes.
//!
//! A single-`value` SQL insert and RESP `SET` store the value as a byte
//! string: an integer is its decimal text. `INCR`, `INCRBY`, `DECR`, and
//! `INCRBYFLOAT` read that text by the Redis rules and store the result as
//! decimal text, so RESP `GET` and a SQL `SELECT value` read the new number
//! back. A value that does not parse, and a result out of range, answer the
//! Redis error text over RESP and a data-exception SQLSTATE over pgwire.

use crate::harness::TestServer;
use crate::harness::resp_client::Reply;

const COLLECTION: &str = "kvcount";

async fn create_bare_value_collection(server: &TestServer) {
    server
        .exec(&format!(
            "CREATE COLLECTION {COLLECTION} (key STRING PRIMARY KEY, value STRING) \
             WITH (engine='kv')"
        ))
        .await
        .unwrap();
}

async fn value_of(server: &TestServer, key: &str) -> String {
    let rows = server
        .query_text(&format!(
            "SELECT value FROM {COLLECTION} WHERE key = '{key}'"
        ))
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        1,
        "expected exactly one row for {key}, got {rows:?}"
    );
    rows[0].clone()
}

/// Parse the JSON payload `SELECT KV_*(...)` returns as its single text column.
fn json_of(rows: &[String]) -> serde_json::Value {
    serde_json::from_str(&rows[0]).expect("KV_* result must be JSON")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resp_incr_on_set_decimal_text_counts_from_the_stored_number() {
    let server = TestServer::start().await;
    create_bare_value_collection(&server).await;
    let mut resp = server.resp_session("kvcount_incr_user", COLLECTION).await;

    assert_eq!(
        resp.cmd(&["SET", "k", "5"]).await,
        Reply::Simple("OK".into())
    );
    assert_eq!(
        resp.cmd(&["INCR", "k"]).await,
        Reply::Integer(6),
        "INCR reads the stored text \"5\" as the number 5"
    );
    assert_eq!(
        resp.cmd(&["GET", "k"]).await,
        Reply::Bulk(Some("6".into())),
        "INCR stores the result as decimal text"
    );

    assert_eq!(
        resp.cmd(&["SET", "big", "41"]).await,
        Reply::Simple("OK".into())
    );
    assert_eq!(resp.cmd(&["INCRBY", "big", "1"]).await, Reply::Integer(42));
    assert_eq!(resp.cmd(&["DECR", "big"]).await, Reply::Integer(41));
    assert_eq!(
        resp.cmd(&["GET", "big"]).await,
        Reply::Bulk(Some("41".into()))
    );
    assert_eq!(value_of(&server, "big").await, "41");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resp_incr_on_text_that_is_not_an_integer_answers_the_redis_error() {
    let server = TestServer::start().await;
    create_bare_value_collection(&server).await;
    let mut resp = server.resp_session("kvcount_nan_user", COLLECTION).await;

    assert_eq!(
        resp.cmd(&["SET", "k", "abc"]).await,
        Reply::Simple("OK".into())
    );
    assert_eq!(
        resp.cmd(&["INCR", "k"]).await,
        Reply::Error("ERR value is not an integer or out of range".into())
    );
    assert_eq!(
        resp.cmd(&["INCRBYFLOAT", "k", "1"]).await,
        Reply::Error("ERR value is not a valid float".into())
    );
    assert_eq!(
        resp.cmd(&["GET", "k"]).await,
        Reply::Bulk(Some("abc".into())),
        "a refused increment leaves the value unchanged"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resp_incrby_past_the_i64_range_answers_the_overflow_error() {
    let server = TestServer::start().await;
    create_bare_value_collection(&server).await;
    let mut resp = server.resp_session("kvcount_ovf_user", COLLECTION).await;

    let max = i64::MAX.to_string();
    assert_eq!(
        resp.cmd(&["SET", "k", &max]).await,
        Reply::Simple("OK".into())
    );
    assert_eq!(
        resp.cmd(&["INCRBY", "k", "1"]).await,
        Reply::Error("ERR increment or decrement would overflow".into())
    );
    assert_eq!(resp.cmd(&["GET", "k"]).await, Reply::Bulk(Some(max)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resp_incrbyfloat_on_decimal_text_stores_decimal_text() {
    let server = TestServer::start().await;
    create_bare_value_collection(&server).await;
    let mut resp = server.resp_session("kvcount_float_user", COLLECTION).await;

    assert_eq!(
        resp.cmd(&["SET", "k", "1.5"]).await,
        Reply::Simple("OK".into())
    );
    assert_eq!(
        resp.cmd(&["INCRBYFLOAT", "k", "1"]).await,
        Reply::Bulk(Some("2.5".into()))
    );
    assert_eq!(
        resp.cmd(&["GET", "k"]).await,
        Reply::Bulk(Some("2.5".into()))
    );
    assert_eq!(value_of(&server, "k").await, "2.5");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resp_incrbyfloat_adds_decimal_text_exactly_like_redis() {
    let server = TestServer::start().await;
    create_bare_value_collection(&server).await;
    let mut resp = server.resp_session("kvcount_exact_user", COLLECTION).await;

    for (key, stored, delta, expected) in [
        ("a", "0.1", "0.2", "0.3"),
        ("b", "10.5", "0.1", "10.6"),
        ("c", "5.0e3", "200", "5200"),
        ("d", "3.0", "0", "3"),
        ("e", "-1.5", "1.5", "0"),
    ] {
        assert_eq!(
            resp.cmd(&["SET", key, stored]).await,
            Reply::Simple("OK".into())
        );
        assert_eq!(
            resp.cmd(&["INCRBYFLOAT", key, delta]).await,
            Reply::Bulk(Some(expected.into())),
            "{stored} + {delta}"
        );
        assert_eq!(
            resp.cmd(&["GET", key]).await,
            Reply::Bulk(Some(expected.into())),
            "{stored} + {delta} is stored as the reply text"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_kv_incr_on_a_single_value_row_counts_from_the_stored_number() {
    let server = TestServer::start().await;
    create_bare_value_collection(&server).await;

    server
        .exec(&format!(
            "INSERT INTO {COLLECTION} (key, value) VALUES ('k', '5')"
        ))
        .await
        .unwrap();
    let rows = server
        .query_text(&format!("SELECT KV_INCR('{COLLECTION}', 'k', 1)"))
        .await
        .unwrap();
    assert_eq!(json_of(&rows)["value"], 6);
    assert_eq!(value_of(&server, "k").await, "6");

    let mut resp = server.resp_session("kvcount_sql_user", COLLECTION).await;
    assert_eq!(
        resp.cmd(&["GET", "k"]).await,
        Reply::Bulk(Some("6".into())),
        "KV_INCR stores decimal text, the same bytes RESP INCR stores"
    );

    server
        .exec(&format!(
            "INSERT INTO {COLLECTION} (key, value) VALUES ('f', '1.5')"
        ))
        .await
        .unwrap();
    let rows = server
        .query_text(&format!("SELECT KV_INCR_FLOAT('{COLLECTION}', 'f', 1)"))
        .await
        .unwrap();
    assert_eq!(json_of(&rows)["value"], 2.5);
    assert_eq!(json_of(&rows)["text"], "2.5");
    assert_eq!(value_of(&server, "f").await, "2.5");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sql_kv_incr_faults_answer_data_exception_sqlstates() {
    let server = TestServer::start().await;
    create_bare_value_collection(&server).await;

    server
        .exec(&format!(
            "INSERT INTO {COLLECTION} (key, value) VALUES ('name', 'abc')"
        ))
        .await
        .unwrap();
    server
        .expect_error(
            &format!("SELECT KV_INCR('{COLLECTION}', 'name', 1)"),
            "SQLSTATE 22P02",
        )
        .await;
    server
        .expect_error(
            &format!("SELECT KV_INCR_FLOAT('{COLLECTION}', 'name', 1)"),
            "SQLSTATE 22P02",
        )
        .await;

    server
        .exec(&format!(
            "INSERT INTO {COLLECTION} (key, value) VALUES ('max', '{}')",
            i64::MAX
        ))
        .await
        .unwrap();
    server
        .expect_error(
            &format!("SELECT KV_INCR('{COLLECTION}', 'max', 1)"),
            "SQLSTATE 22003",
        )
        .await;
    assert_eq!(value_of(&server, "max").await, i64::MAX.to_string());
}
