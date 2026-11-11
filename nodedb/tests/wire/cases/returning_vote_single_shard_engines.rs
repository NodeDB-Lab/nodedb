// SPDX-License-Identifier: BUSL-1.1

//! Per engine, a failing `RETURNING` list on a one-vShard insert stores
//! nothing over native and HTTP. A list that cannot fail keeps the direct
//! path, and a list that commits through Calvin answers as the direct path
//! answers.

use std::collections::BTreeSet;

use nodedb_test_support::native_harness::send_sql;
use nodedb_types::Value;

use super::returning_vote_single_shard::{http_error, native, native_error, rows_of};
use crate::harness::TestServer;

/// One engine's collection, a failing insert into it, and a read of it.
/// Each SQL holds a `{c}` for the collection and the insert a `{k}` for a
/// fresh key.
pub(super) struct Engine {
    pub(super) engine: &'static str,
    pub(super) ddl: &'static str,
    pub(super) insert: &'static str,
    pub(super) read: &'static str,
}

pub(super) const ENGINES: &[Engine] = &[
    Engine {
        engine: "document_schemaless",
        ddl: "CREATE COLLECTION {c} (id TEXT PRIMARY KEY, n INT)",
        insert: "INSERT INTO {c} (id, n) VALUES ('{k}', 5) RETURNING 1 / (n - n)",
        read: "SELECT id FROM {c}",
    },
    Engine {
        engine: "document_strict",
        ddl: "CREATE COLLECTION {c} (id TEXT PRIMARY KEY, n INT) WITH (engine='document_strict')",
        insert: "INSERT INTO {c} (id, n) VALUES ('{k}', 5) RETURNING 1 / (n - n)",
        read: "SELECT id FROM {c}",
    },
    Engine {
        engine: "kv",
        ddl: "CREATE COLLECTION {c} (id TEXT PRIMARY KEY, n INT) WITH (engine='kv')",
        insert: "INSERT INTO {c} (id, n) VALUES ('{k}', 5) RETURNING 1 / (n - n)",
        read: "SELECT id FROM {c}",
    },
    Engine {
        engine: "columnar",
        ddl: "CREATE COLLECTION {c} (id TEXT PRIMARY KEY, n INT) WITH (engine='columnar')",
        insert: "INSERT INTO {c} (id, n) VALUES ('{k}', 5) RETURNING 1 / (n - n)",
        read: "SELECT id FROM {c}",
    },
    Engine {
        engine: "timeseries",
        ddl: "CREATE COLLECTION {c} (ts TIMESTAMP TIME_KEY, n INT) WITH (engine='timeseries')",
        insert: "INSERT INTO {c} (ts, n) VALUES ({k}, 5) RETURNING 1 / (n - n)",
        read: "SELECT n FROM {c}",
    },
    Engine {
        engine: "vector",
        ddl: "CREATE COLLECTION {c} (id STRING PRIMARY KEY, vec VECTOR(3), n INT) \
              WITH (engine='vector', primary='vector', vector_field='vec', dim=3)",
        insert: "INSERT INTO {c} (id, vec, n) VALUES ('{k}', ARRAY[1.0, 0.0, 0.0], 5) \
                 RETURNING 1 / (n - n)",
        read: "SELECT id FROM {c}",
    },
];

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failing_list_stores_nothing_on_every_engine() {
    let server = TestServer::start().await;
    let mut stream = native(&server).await;
    for (index, engine) in ENGINES.iter().enumerate() {
        let collection = format!("rv_engine_{index}");
        let bind = |sql: &str, key: &str| sql.replace("{c}", &collection).replace("{k}", key);
        server
            .exec(&bind(engine.ddl, ""))
            .await
            .unwrap_or_else(|e| panic!("{}: create: {e}", engine.engine));

        let native_insert = bind(engine.insert, "1000");
        let seq = 2 + u64::try_from(index).expect("an index");
        let (code, message) = native_error(&mut stream, seq, &native_insert).await;
        assert_eq!(code, "22012", "{}: {message}", engine.engine);
        http_error(&server, &bind(engine.insert, "2000"), "division by zero").await;

        assert!(
            rows_of(&server, &bind(engine.read, "")).await.is_empty(),
            "{}: the refusals stored nothing",
            engine.engine
        );
    }
}

/// The `admitted` count of Calvin transactions the sequencer reports on
/// `/metrics`.
pub(super) async fn admitted_txns(server: &TestServer) -> u64 {
    let body = reqwest::Client::new()
        .get(format!("http://127.0.0.1:{}/metrics", server.http_port))
        .send()
        .await
        .unwrap_or_else(|e| panic!("GET /metrics: {e}"))
        .text()
        .await
        .unwrap_or_default();
    body.lines()
        .find_map(|line| {
            line.strip_prefix("nodedb_sequencer_admitted_txns_total{outcome=\"admitted\"} ")
                .and_then(|value| value.trim().parse().ok())
        })
        .unwrap_or_else(|| panic!("no admitted sequencer count in /metrics: {body}"))
}

/// `RETURNING id` on a point insert cannot fail, so no Calvin transaction
/// commits it. A computed item can fail, so its insert commits through
/// Calvin.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_list_that_cannot_fail_keeps_the_direct_path() {
    let server = TestServer::start().await;
    server
        .exec("CREATE COLLECTION rv_direct (id TEXT PRIMARY KEY, n INT)")
        .await
        .expect("create rv_direct");
    let mut stream = native(&server).await;

    let before = admitted_txns(&server).await;
    let response = send_sql(
        &mut stream,
        2,
        "INSERT INTO rv_direct (id, n) VALUES ('a', 1) RETURNING id",
    )
    .await;
    assert!(response.error.is_none(), "{:?}", response.error);
    let (status, body) = server
        .http_query("INSERT INTO rv_direct (id, n) VALUES ('b', 1) RETURNING id")
        .await;
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        admitted_txns(&server).await,
        before,
        "no Calvin transaction committed a list that cannot fail"
    );

    let response = send_sql(
        &mut stream,
        3,
        "INSERT INTO rv_direct (id, n) VALUES ('c', 1) RETURNING id, n * 2",
    )
    .await;
    assert!(response.error.is_none(), "{:?}", response.error);
    assert!(
        admitted_txns(&server).await > before,
        "a list that can fail commits through Calvin"
    );
    assert_eq!(
        rows_of(&server, "SELECT id FROM rv_direct").await,
        ["a", "b", "c"]
    );
}

/// `RETURNING id AS k, v::text` commits through Calvin over native and HTTP.
/// It answers the names and values pgwire answers for the same list.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_list_on_calvin_answers_as_the_direct_path() {
    let server = TestServer::start().await;
    server
        .exec(
            "CREATE COLLECTION rv_parity (id TEXT PRIMARY KEY, v INT) \
             WITH (engine='document_strict')",
        )
        .await
        .expect("create rv_parity");
    let list = "RETURNING id AS k, v::text";

    let reference = server
        .query_named_rows(&format!(
            "INSERT INTO rv_parity (id, v) VALUES ('p0', 7) {list}"
        ))
        .await
        .expect("the pgwire insert answers");
    let [reference] = reference.as_slice() else {
        panic!("one pgwire row: {reference:?}");
    };
    assert_eq!(reference.get("k").map(String::as_str), Some("p0"));
    let cast_name = reference
        .keys()
        .find(|name| name.as_str() != "k")
        .cloned()
        .unwrap_or_else(|| panic!("two columns: {reference:?}"));
    assert_eq!(reference.get(&cast_name).map(String::as_str), Some("7"));

    let mut stream = native(&server).await;
    let response = send_sql(
        &mut stream,
        2,
        &format!("INSERT INTO rv_parity (id, v) VALUES ('p1', 7) {list}"),
    )
    .await;
    assert!(response.error.is_none(), "{:?}", response.error);
    assert_eq!(
        response.columns.as_deref(),
        Some(["k".to_owned(), cast_name.clone()].as_slice())
    );
    assert_eq!(
        response.rows.as_deref(),
        Some([vec![Value::String("p1".into()), Value::String("7".into())]].as_slice())
    );

    let (status, body) = server
        .http_query(&format!(
            "INSERT INTO rv_parity (id, v) VALUES ('p2', 7) {list}"
        ))
        .await;
    assert_eq!(status, 200, "{body}");
    let answer: serde_json::Value = serde_json::from_str(&body).expect("a JSON answer");
    let row = answer["rows"][0]
        .as_object()
        .unwrap_or_else(|| panic!("one row: {body}"));
    let names: BTreeSet<&str> = row.keys().map(String::as_str).collect();
    let expected: BTreeSet<&str> = reference.keys().map(String::as_str).collect();
    assert_eq!(names, expected, "{body}");
    assert_eq!(row["k"], "p2", "{body}");
    assert_eq!(row[cast_name.as_str()], "7", "{body}");
}
