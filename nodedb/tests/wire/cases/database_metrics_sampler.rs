// SPDX-License-Identifier: BUSL-1.1

//! The per-database metric sampler fills the gauges that have an owning
//! source. Today that is bridge queue depth, read from the dispatch WFQ: a
//! database created after boot must gain a depth line within one sampling
//! interval, and the bootstrapped default database must have one too.

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::harness::TestServer;

/// Longest the test waits for one sampler interval plus scheduling slack.
const WAIT_SECS: u64 = 25;

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

/// Value of `name{...label_value...}`, or `None` when the line is absent.
fn labeled_metric_value(body: &str, name: &str, label_value: &str) -> Option<u64> {
    body.lines().find_map(|line| {
        let rest = line.strip_prefix(name)?;
        if !rest.starts_with('{') || !rest.contains(label_value) {
            return None;
        }
        let (_, value) = rest.rsplit_once(' ')?;
        value.trim().parse::<u64>().ok()
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sampler_publishes_queue_depth() {
    let (server, db) = TestServer::with_database("metrics_sampler").await;
    server
        .exec("CREATE COLLECTION sampler_docs (id STRING PRIMARY KEY, v STRING) WITH (engine='document_schemaless')")
        .await
        .unwrap();
    server
        .exec("INSERT INTO sampler_docs (id, v) VALUES ('k1', 'v')")
        .await
        .unwrap();

    let label = format!("database=\"{db}\"");
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(WAIT_SECS);
    loop {
        let body = fetch_metrics_body(server.http_port).await;
        let depth = labeled_metric_value(&body, "nodedb_database_bridge_queue_depth", &label);
        let default_depth = labeled_metric_value(
            &body,
            "nodedb_database_bridge_queue_depth",
            "database=\"default\"",
        );
        if depth.is_some() && default_depth.is_some() {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "sampler did not publish queue depth within {WAIT_SECS}s: \
             depth={depth:?} default_depth={default_depth:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}

/// A capped database gains a live connection count after a client binds to
/// it. The uncapped database must never publish a fabricated value: either it
/// has no permit entry and no line, or it has an entry with a measured zero.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sampler_publishes_capped_connections_and_skips_uncapped() {
    let server = TestServer::start().await;
    let suffix = std::process::id();
    let capped = format!("sampler_capped_{suffix}");
    let uncapped = format!("sampler_uncapped_{suffix}");
    server
        .exec(&format!("CREATE DATABASE {capped}"))
        .await
        .unwrap();
    server
        .exec(&format!(
            "ALTER DATABASE {capped} SET QUOTA (max_connections = 4)"
        ))
        .await
        .unwrap();
    server
        .exec(&format!("CREATE DATABASE {uncapped}"))
        .await
        .unwrap();

    // The cap must be visible in the usage report before the connection
    // opens: the admission registry is seeded from the applied quota, and a
    // connection admitted before that seeding holds no permit.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(WAIT_SECS);
    loop {
        let visible = server
            .query_named_rows(&format!("SHOW DATABASE USAGE FOR {capped}"))
            .await
            .unwrap_or_default()
            .into_iter()
            .any(|row| {
                row.get("quota_name").map(String::as_str) == Some("max_connections")
                    && row.get("limit").map(String::as_str) == Some("4")
            });
        if visible {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the max_connections cap never became visible in SHOW DATABASE USAGE"
        );
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }

    let (client, handle) = server
        .connect_as_database("nodedb", "nodedb", &capped)
        .await
        .expect("a connection to a capped database must be admitted");

    let capped_label = format!("database=\"{capped}\"");
    let uncapped_label = format!("database=\"{uncapped}\"");
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(WAIT_SECS);
    loop {
        let body = fetch_metrics_body(server.http_port).await;
        let live = labeled_metric_value(&body, "nodedb_database_connections", &capped_label);
        if live.is_some_and(|v| v >= 1) {
            if let Some(uncapped) =
                labeled_metric_value(&body, "nodedb_database_connections", &uncapped_label)
            {
                assert_eq!(
                    uncapped, 0,
                    "an uncapped database without a permit must publish a measured zero, \
                     never a fabricated count"
                );
            }
            break;
        }
        let published: Vec<&str> = body
            .lines()
            .filter(|line| line.starts_with("nodedb_database_connections{"))
            .collect();
        let usage_current = server
            .query_named_rows(&format!("SHOW DATABASE USAGE FOR {capped}"))
            .await
            .unwrap_or_default()
            .into_iter()
            .find(|row| row.get("quota_name").map(String::as_str) == Some("max_connections"))
            .and_then(|row| row.get("current").cloned())
            .unwrap_or_else(|| "<no row>".to_string());
        assert!(
            tokio::time::Instant::now() < deadline,
            "sampler did not publish a capped connection count within {WAIT_SECS}s: \
             live={live:?} published={published:?} usage_current={usage_current:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }

    drop(client);
    let _ = handle.await;
}
