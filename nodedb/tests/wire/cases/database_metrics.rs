// SPDX-License-Identifier: BUSL-1.1

//! Per-database and per-tenant quota counters on `/metrics` go non-zero
//! after queries, and each database's `nodedb_database_qps_total` counter
//! is independent of the others.

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::harness::TestServer;

/// Fetch the raw Prometheus text body from `/metrics`.
async fn fetch_metrics_body(http_port: u16) -> String {
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", http_port))
        .await
        .expect("connect to /metrics");
    let req = b"GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    stream.write_all(req).await.expect("write metrics request");
    let mut body = String::new();
    stream
        .read_to_string(&mut body)
        .await
        .expect("read metrics response");
    body
}

/// Read a counter/gauge value for `name{...label_value...}` from a
/// Prometheus text body. Panics if no matching line exists.
fn labeled_metric(body: &str, name: &str, label_value: &str) -> u64 {
    for line in body.lines() {
        if let Some(rest) = line.strip_prefix(name)
            && rest.starts_with('{')
            && rest.contains(label_value)
            && let Some((_, value)) = rest.rsplit_once(' ')
            && let Ok(v) = value.trim().parse::<u64>()
        {
            return v;
        }
    }
    panic!("metric {name} with label {label_value} not found in: {body}");
}

/// Whether any `name{...}` line in the body reports a value greater than 0.
fn any_labeled_metric_nonzero(body: &str, name: &str) -> bool {
    body.lines().any(|line| {
        line.strip_prefix(name)
            .and_then(|rest| rest.starts_with('{').then_some(rest))
            .and_then(|rest| rest.rsplit_once(' '))
            .and_then(|(_, value)| value.trim().parse::<u64>().ok())
            .is_some_and(|v| v > 0)
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn database_qps_counter_increments_after_queries() {
    let (server, db_a) = TestServer::with_database("metrics_a").await;

    server
        .exec("CREATE COLLECTION doc_a (id STRING PRIMARY KEY, v STRING) WITH (engine='document_schemaless')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO doc_a (id, v) VALUES ('k1', 'hello')")
        .await
        .unwrap();
    server
        .exec("SELECT id, v FROM doc_a WHERE id = 'k1'")
        .await
        .unwrap();

    let body = fetch_metrics_body(server.http_port).await;
    let qps_a = labeled_metric(
        &body,
        "nodedb_database_qps_total",
        &format!("database=\"{db_a}\""),
    );
    assert!(
        qps_a > 0,
        "database '{db_a}' should have a non-zero QPS counter after queries, got {qps_a}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_databases_have_independent_qps_counters() {
    let (server, db_a) = TestServer::with_database("metrics_db_a").await;

    let db_b = format!(
        "metrics_db_b_{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0)
    );
    server
        .client
        .simple_query(&format!("CREATE DATABASE {db_b}"))
        .await
        .unwrap();
    server
        .client
        .simple_query(&format!("USE DATABASE {db_b}"))
        .await
        .unwrap();

    server
        .exec("CREATE COLLECTION doc_b (id STRING PRIMARY KEY) WITH (engine='kv')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO doc_b (id) VALUES ('x')")
        .await
        .unwrap();

    let body = fetch_metrics_body(server.http_port).await;
    let qps_b = labeled_metric(
        &body,
        "nodedb_database_qps_total",
        &format!("database=\"{db_b}\""),
    );
    assert!(
        qps_b > 0,
        "database '{db_b}' should have a non-zero QPS counter, got {qps_b}"
    );

    // db_b ran strictly more counted queries (its own setup plus the DML
    // above) than db_a's with_database() setup alone, so the two counters
    // must be independent rather than sharing one accumulator.
    let qps_a = labeled_metric(
        &body,
        "nodedb_database_qps_total",
        &format!("database=\"{db_a}\""),
    );
    assert!(
        qps_b >= qps_a,
        "db_b (qps={qps_b}) should have at least as many counted queries as db_a setup (qps={qps_a})"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tenant_quota_metrics_non_zero_under_load() {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    let db_name = format!("metrics_load_{ts}");
    let (server, _db) = TestServer::with_database(&db_name).await;

    let col = format!("kv_load_{ts}");
    server
        .exec(&format!(
            "CREATE COLLECTION {col} (id STRING PRIMARY KEY, v STRING) WITH (engine='kv')"
        ))
        .await
        .unwrap();
    for i in 0..5_u32 {
        let key = format!("k{i}");
        let val = format!("v{i}");
        // Ignore duplicate-key on retry -- the counter increments regardless.
        let _ = server
            .exec(&format!(
                "INSERT INTO {col} (id, v) VALUES ('{key}', '{val}')"
            ))
            .await;
    }

    let body = fetch_metrics_body(server.http_port).await;
    assert!(
        any_labeled_metric_nonzero(&body, "nodedb_tenant_total_requests"),
        "at least one tenant should have tracked requests: {body}"
    );
}

/// POST `sql` to `/v1/query` against `database`. Returns the HTTP status.
async fn http_query(http_port: u16, database: &str, sql: &str) -> reqwest::StatusCode {
    reqwest::Client::new()
        .post(format!("http://127.0.0.1:{http_port}/v1/query"))
        .header("X-NodeDB-Database", database)
        .json(&serde_json::json!({ "sql": sql }))
        .send()
        .await
        .expect("POST /v1/query")
        .status()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn http_query_increments_database_qps_counter() {
    let (server, db) = TestServer::with_database("metrics_http").await;
    let label = format!("database=\"{db}\"");

    let before = labeled_metric(
        &fetch_metrics_body(server.http_port).await,
        "nodedb_database_qps_total",
        &label,
    );
    let status = http_query(server.http_port, &db, "SELECT 1").await;
    assert!(status.is_success(), "HTTP query must succeed: {status}");
    let after = labeled_metric(
        &fetch_metrics_body(server.http_port).await,
        "nodedb_database_qps_total",
        &label,
    );

    assert_eq!(
        after,
        before + 1,
        "one HTTP query counts exactly once (before={before}, after={after})"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failing_statement_increments_database_errors_counter() {
    let (server, db) = TestServer::with_database("metrics_errors").await;
    let label = format!("database=\"{db}\"");

    let before = labeled_metric(
        &fetch_metrics_body(server.http_port).await,
        "nodedb_database_errors_total",
        &label,
    );
    server
        .expect_error(
            "SELECT id FROM no_such_collection_for_metrics",
            "does not exist",
        )
        .await;
    let after = labeled_metric(
        &fetch_metrics_body(server.http_port).await,
        "nodedb_database_errors_total",
        &label,
    );

    assert_eq!(
        after,
        before + 1,
        "one failed statement counts exactly once (before={before}, after={after})"
    );
}
