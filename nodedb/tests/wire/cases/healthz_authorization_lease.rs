// SPDX-License-Identifier: BUSL-1.1

//! A fresh single-node server boots through its first authorization lease.
//!
//! The binary runs its default single-node cluster, which is the metadata
//! leader and its only lease holder. Boot opens the gateway only once the
//! node holds a lease, so a fresh boot must reach ready, and `/healthz`
//! must report the lease valid.

use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::harness::TestServer;

/// Longest a fresh boot may take to reach ready.
const BOOT_BOUND: Duration = Duration::from_secs(30);

async fn fetch_healthz(http_port: u16) -> String {
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", http_port))
        .await
        .expect("connect to /healthz");
    let req = b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    stream.write_all(req).await.expect("write healthz request");
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .await
        .expect("read healthz response");
    response
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fresh_single_node_boot_reaches_ready_holding_a_valid_lease() {
    let started = Instant::now();
    // `start()` returns only after `/healthz` answered 200.
    let server = TestServer::start().await;
    assert!(
        started.elapsed() < BOOT_BOUND,
        "a fresh boot took {:?} to reach ready",
        started.elapsed()
    );

    let response = fetch_healthz(server.http_port).await;
    assert!(
        response.starts_with("HTTP/1.1 200"),
        "/healthz must be 200 on a booted node: {response}"
    );
    assert!(
        response.contains("\"authorization_lease\":\"valid\""),
        "/healthz must report the authorization lease valid: {response}"
    );

    // A permission-checked statement plans at once.
    server
        .exec("CREATE COLLECTION lease_boot_probe")
        .await
        .expect("CREATE COLLECTION after boot");
    server
        .exec("SELECT * FROM lease_boot_probe")
        .await
        .expect("the first read after boot must not be refused");
}
