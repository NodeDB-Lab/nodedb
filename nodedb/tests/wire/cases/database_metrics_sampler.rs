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
